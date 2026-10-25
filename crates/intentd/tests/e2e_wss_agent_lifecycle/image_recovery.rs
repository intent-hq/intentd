//! Recovery of a retained image batch rejected by the former aggregate limit.

use super::*;
use base64::Engine as _;

const IMAGE_COUNT: usize = 14;
const PROMPT_BUDGET: usize = 8 * 1024 * 1024;
const FAILED_CONTENT: &str = "Inspect the retained image batch";
const FOLLOW_UP: &str = "Continue with a text-only follow-up";

/// Observe the complete terminal lifecycle, not just RPC acceptance.
async fn await_success<S>(ws: &mut WebSocketStream<S>, agent_id: &str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    let (mut ends, mut idle, mut settled) = (0, false, false);
    while ends == 0 || !idle || !settled {
        let frame = wss_event_opt_until(ws, deadline)
            .await
            .expect("image recovery turn completed");
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => panic!("recovery failed: {event}"),
            Some("agent:stream:end") => ends += 1,
            Some("agent:idle") => idle = true,
            Some("agent:status-changed") if event["data"]["status"] == "idle" => settled = true,
            _ => {}
        }
    }
    assert_eq!(ends, 1, "one terminal event per successful turn");
}

fn read_prompts(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .expect("read provider prompts")
        .lines()
        .map(|line| serde_json::from_str(line).expect("provider prompt JSON"))
        .collect()
}

#[intent_test_macros::daemon_test]
async fn retained_oversized_image_batch_retries_and_allows_text_follow_up_over_wss() {
    use intent_core::{now_iso, AgentId, AgentSession, AgentStatus, WorkspaceId};
    use intent_store::{AgentQueueRow, AttachmentRecord, Store};

    let Some(script) = gate("retained image batch recovery E2E") else {
        return;
    };
    // Each original is below the per-image dimensions/bytes limit. The old
    // preparation therefore passed each one through and rejected their sum.
    let mut state = 0x1234_5678_u32;
    let pixels = image::RgbImage::from_fn(512, 512, |_, _| {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        let [r, g, b, _] = state.to_le_bytes();
        image::Rgb([r, g, b])
    });
    let mut encoded = std::io::Cursor::new(Vec::new());
    pixels
        .write_to(&mut encoded, image::ImageFormat::Png)
        .unwrap();
    let original = encoded.into_inner();
    let original_hash = Sha256::digest(&original);
    let original_base64_len = base64::engine::general_purpose::STANDARD
        .encode(&original)
        .len();
    assert!(original.len() < 3 * 1024 * 1024);
    assert!(original_base64_len * IMAGE_COUNT > PROMPT_BUDGET);

    let data_dir = temp_data_dir();
    let workspace_id = WorkspaceId::new();
    let agent_id = AgentId::from(format!("agent-{}", uuid::Uuid::new_v4()));
    let turn_id = uuid::Uuid::now_v7().to_string();
    let queue_id = uuid::Uuid::now_v7().to_string();
    let root = data_dir.path().join("ws-root");
    std::fs::create_dir_all(root.join(".intent/attachments")).unwrap();
    let mut references = Vec::new();
    let mut originals = Vec::new();
    {
        let store = Store::open(&data_dir.path().join("intentd.db"))
            .await
            .unwrap();
        let mut workspace = workspace_seed(&workspace_id);
        workspace.worktree_path = Some(root.to_string_lossy().into_owned());
        store.insert_workspace(&workspace).await.unwrap();
        let ts = now_iso();
        let error = "invalid params: Image 14: the prepared images together exceed the 8 MiB base64 prompt budget. Crop or export a smaller PNG/JPEG, or send fewer images.";
        let session: AgentSession = serde_json::from_value(json!({
            "id": agent_id.0, "workspaceId": workspace_id.0,
            "name": "Image recovery", "nameExplicitlySet": true,
            "model": "default", "provider": "mock",
            "systemPrompt": "Inspect the supplied images.",
            "status": "error", "isActive": false,
            "createdAt": ts, "updatedAt": ts,
            "stopReason": error, "stopReasonTimestamp": ts,
        }))
        .unwrap();
        assert_eq!(session.status, AgentStatus::Error);
        store.insert_agent_session(&session).await.unwrap();
        for index in 0..IMAGE_COUNT {
            let attachment_id = uuid::Uuid::new_v4().to_string();
            let file_name = format!("image-{index}.png");
            let stored_path = format!(".intent/attachments/{file_name}");
            let path = root.join(&stored_path);
            std::fs::write(&path, &original).unwrap();
            store
                .insert_attachment(&AttachmentRecord {
                    id: attachment_id.clone(),
                    workspace_id: workspace_id.clone(),
                    file_name,
                    mime_type: Some("image/png".to_string()),
                    size: i64::try_from(original.len()).unwrap(),
                    uploaded_at: ts.clone(),
                    stored_path,
                })
                .await
                .unwrap();
            references.push(json!({"type": "image", "attachmentId": attachment_id}));
            originals.push(path);
        }
        let mut blocks = vec![json!({"type": "text", "text": FAILED_CONTENT})];
        blocks.extend(references.clone());
        store
            .append_agent_message_with_id(&agent_id, &turn_id, "user", &json!(blocks), None, &ts)
            .await
            .unwrap();
        // This is the durable terminal-failure payload, not a newly queued
        // submission. Keep the original transcript row, references and turn id.
        store
            .replace_agent_queue(
                &agent_id,
                &[AgentQueueRow {
                    id: queue_id.clone(),
                    agent_id: agent_id.clone(),
                    position: 0,
                    created_at: ts.clone(),
                    turn_id: turn_id.clone(),
                    payload: json!({
                        "id": queue_id, "turnId": turn_id, "content": FAILED_CONTENT,
                        "imageBlocks": references, "queuedAt": ts,
                        "persisted": true, "requeuedAfterFailure": true, "userOrigin": true,
                    }),
                }],
            )
            .await
            .unwrap();
        store.close().await;
    }

    let prompt_log = data_dir.path().join("prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let behavior = json!({"response": "seen", "loadSession": true}).to_string();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_PROMPT_LOG", prompt_log_str.as_str()),
    ];
    let _daemon = Daemon {
        child: spawn_serve(data_dir.path(), "both", &env),
    };
    let socket = data_dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut sub = connect_ws(port, cfg.clone()).await;
    let subscribed = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "workspaceId": workspace_id.0, "eventTypes": ["agent:*"]
        }),
    )
    .await;
    assert!(subscribed["subscriptionId"].is_string());
    let mut rpc = connect_ws(port, cfg).await;
    let session = wss_rpc(
        &mut rpc,
        2,
        "agent.getSession",
        json!({"agentId": agent_id.0}),
    )
    .await;
    assert_eq!(session["session"]["status"], "error");
    let queue = wss_rpc(
        &mut rpc,
        3,
        "agent.getQueue",
        json!({"agentId": agent_id.0}),
    )
    .await;
    assert_eq!(queue["queue"].as_array().unwrap().len(), 1);
    assert_eq!(queue["queue"][0]["turnId"], turn_id);
    assert_eq!(queue["queue"][0]["requeuedAfterFailure"], true);
    assert_eq!(queue["queue"][0]["imageBlocks"], json!(references));

    let retry = wss_rpc(
        &mut rpc,
        4,
        "agent.retry",
        json!({
            "workspaceId": workspace_id.0, "agentId": agent_id.0,
        }),
    )
    .await;
    assert_eq!(retry["ok"], true);
    assert_eq!(retry["redriven"], true);
    assert_eq!(retry["turnId"], turn_id);
    await_success(&mut sub, &agent_id.0).await;
    let prompts = read_prompts(&prompt_log);
    assert_eq!(prompts.len(), 1, "one delivery of the retained batch");
    let images: Vec<_> = prompts[0]["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|block| block["type"] == "image")
        .collect();
    assert_eq!(
        images.len(),
        IMAGE_COUNT,
        "every retained image is delivered"
    );
    let total: usize = images
        .iter()
        .map(|image| image["data"].as_str().unwrap().len())
        .sum();
    assert!(
        total <= PROMPT_BUDGET,
        "prepared batch exceeds aggregate budget: {total}"
    );
    for image in images {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(image["data"].as_str().unwrap())
            .unwrap();
        assert!(bytes.len() <= 3 * 1024 * 1024);
        let format = image::guess_format(&bytes).unwrap();
        assert_eq!(image["mimeType"], format.to_mime_type());
        let decoded = image::load_from_memory(&bytes).unwrap();
        assert!(decoded.width() <= 2000 && decoded.height() <= 2000);
    }
    let queue = wss_rpc(
        &mut rpc,
        5,
        "agent.getQueue",
        json!({"agentId": agent_id.0}),
    )
    .await;
    assert!(queue["queue"].as_array().unwrap().is_empty());

    let sent = wss_rpc(
        &mut rpc,
        6,
        "agent.sendMessage",
        json!({
            "workspaceId": workspace_id.0, "agentId": agent_id.0, "content": FOLLOW_UP,
        }),
    )
    .await;
    assert_eq!(sent["success"], true);
    await_success(&mut sub, &agent_id.0).await;
    let prompts = read_prompts(&prompt_log);
    assert_eq!(
        prompts.len(),
        2,
        "no failed batch drains after the follow-up"
    );
    assert!(prompts[1]["text"].as_str().unwrap().contains(FOLLOW_UP));
    assert!(prompts[1]["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .all(|block| block["type"] != "image"));
    let session = wss_rpc(
        &mut rpc,
        7,
        "agent.getSession",
        json!({"agentId": agent_id.0}),
    )
    .await;
    assert_eq!(session["session"]["status"], "idle");
    let conversation = wss_rpc(
        &mut rpc,
        8,
        "agent.getConversation",
        json!({"agentId": agent_id.0}),
    )
    .await;
    let messages = conversation["messages"]
        .as_array()
        .expect("conversation messages");
    let original_rows: Vec<_> = messages.iter().filter(|row| row["id"] == turn_id).collect();
    assert_eq!(original_rows.len(), 1, "retry keeps the original user row");
    assert_eq!(
        messages.iter().filter(|row| row["role"] == "user").count(),
        2
    );
    let original_images: Vec<_> = original_rows[0]["contentBlocks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|block| block["type"] == "image")
        .cloned()
        .map(|mut block| {
            let object = block.as_object_mut().unwrap();
            object.remove("id");
            object.remove("width");
            object.remove("height");
            block
        })
        .collect();
    assert_eq!(
        original_images, references,
        "transcript keeps original references"
    );
    for path in originals {
        assert_eq!(
            Sha256::digest(std::fs::read(path).unwrap()),
            original_hash,
            "original bytes unchanged"
        );
    }
}
