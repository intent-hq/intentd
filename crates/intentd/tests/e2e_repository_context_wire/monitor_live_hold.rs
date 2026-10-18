//! Live internal history notification through the actual private socket writer.
use super::*;

pub(super) async fn exercise(
    h: &Harness,
    store: &Store,
    agent: &intent_core::AgentId,
    control: &mut Client,
    live: (&mut Client, &Value),
    case: (bool, bool, bool),
) {
    let (wss, replace_source, unchanged) = case;
    let (doc, subscription_id) = live;
    let barrier = h.dir.path().join("private-write-hold");
    wait_marker(&barrier.join("entered")).await;
    assert_eq!(
        std::fs::read_to_string(barrier.join("entered")).unwrap(),
        if wss {
            "wss-before-flush"
        } else {
            "uds-before-write"
        }
    );
    assert!(!doc
        .notices
        .iter()
        .any(|frame| frame.to_string().contains("pr_monitor_wake")));
    let original = store.get_agent_messages(agent, None).await.unwrap();
    let original_json = serde_json::to_value(&original).unwrap();
    let private_rows: Vec<_> = original_json
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["metadata"]["type"] == "pr_monitor_wake")
        .collect();
    assert_eq!(private_rows.len(), 1);
    let private_id = private_rows[0]["id"].as_str().unwrap();
    let monitors = store.list_active_pr_monitors_by_agent(agent).await.unwrap();
    if replace_source {
        success(&control.rpc("agent.replaceMessages", json!({"agentId":agent,"workspaceId":h.workspace,
            "messages":[{"role":"user","contentBlocks":[{"type":"text","text":"replacement ordinary live history"}],"timestamp":"2026-10-10T00:00:00Z"}]})).await);
    } else if !unchanged {
        authorize(control, "pat-second").await;
        authorize(control, "stored-pat").await;
    }
    success(
        &control
            .rpc(
                "settings.update",
                json!({"changes":[{"path":"git.autoCommit","value":true}]}),
            )
            .await,
    );
    std::fs::write(barrier.join("release"), b"release live notification").unwrap();
    let result = if unchanged {
        let frame = push(doc, subscription_id).await;
        assert_eq!(frame["params"]["seq"], 1);
        assert_eq!(frame["params"]["kind"], "snapshot");
        assert!(frame.to_string().contains(private_id));
        success(
            &doc.rpc("settings.get", json!({"path":"git.autoCommit"}))
                .await,
        );
        while !doc
            .notices
            .iter()
            .any(|frame| frame.to_string().contains("settings:changed"))
        {
            let frame = doc.next().await;
            doc.notices.push(frame);
        }
        assert!(!barrier.join("discarded-public.json").exists());
        json!({"private":frame,"laterPublic":doc.notices,"socketUsable":true})
    } else {
        let mut received = Vec::new();
        timeout(Duration::from_secs(15), async {
            match &mut doc.socket {
                Socket::Uds(stream) => {
                    stream.read_to_end(&mut received).await.unwrap();
                }
                Socket::Ws(socket) => {
                    while let Some(frame) = socket.next().await {
                        match frame {
                            Ok(Message::Text(text)) => received.extend_from_slice(text.as_bytes()),
                            Ok(Message::Close(_)) | Err(_) => break,
                            _ => {}
                        }
                    }
                }
            }
        })
        .await
        .unwrap();
        assert!(
            received.is_empty(),
            "retired live output escaped: {}",
            String::from_utf8_lossy(&received)
        );
        wait_marker(&barrier.join("discarded-public.json")).await;
        let discarded: Vec<String> =
            serde_json::from_slice(&std::fs::read(barrier.join("discarded-public.json")).unwrap())
                .unwrap();
        assert_eq!(
            discarded
                .iter()
                .filter(|frame| frame.contains("settings:changed"))
                .count(),
            1
        );
        let mut fresh = connect(h, wss).await;
        success(&fresh.rpc("client.hello",json!({"clientId":"native-live-chat","connectionRole":"review-monitor","capabilities":{"reviewMonitoring":1}})).await);
        let history = complete_history(&mut fresh, agent, "snapshot").await;
        assert_ne!(
            history["subscribe"]["result"]["subscriptionId"],
            *subscription_id
        );
        if replace_source {
            assert!(history
                .to_string()
                .contains("replacement ordinary live history"));
            assert!(!history.to_string().contains(private_id));
        } else {
            assert!(history.to_string().contains(private_id));
        }
        json!({"postRetirementTextBytes":received.len(),"discardedPublic":discarded,"fresh":history})
    };
    if !replace_source {
        assert_eq!(
            store.get_agent_messages(agent, None).await.unwrap(),
            original
        );
    }
    assert_eq!(
        serde_json::to_value(store.list_active_pr_monitors_by_agent(agent).await.unwrap()).unwrap(),
        serde_json::to_value(monitors).unwrap()
    );
    if let Some(dir) = std::env::var_os("INTENT_MONITOR_FIXTURE_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(Path::new(&dir).join(format!("live-held-{}-{}.json",if wss {"wss"} else {"uds"},if unchanged {"unchanged"} else if replace_source {"source"} else {"account"})),serde_json::to_vec_pretty(&json!({"wss":wss,"replaceSource":replace_source,"unchanged":unchanged,"originalMessageId":private_id,"result":result})).unwrap()).unwrap();
    }
}

#[tokio::test]
#[ignore = "requires verified installed native runtime and actual ACP database"]
async fn native_live_uds_held_account() {
    native_daemon_case(
        true,
        true,
        false,
        false,
        false,
        false,
        Some((false, false, false)),
    )
    .await;
}
#[tokio::test]
#[ignore = "requires verified installed native runtime and actual ACP database"]
async fn native_live_wss_held_account() {
    native_daemon_case(
        true,
        true,
        false,
        false,
        false,
        false,
        Some((true, false, false)),
    )
    .await;
}
#[tokio::test]
#[ignore = "requires verified installed native runtime and actual ACP database"]
async fn native_live_uds_held_source() {
    native_daemon_case(
        true,
        true,
        false,
        false,
        false,
        false,
        Some((false, true, false)),
    )
    .await;
}
#[tokio::test]
#[ignore = "requires verified installed native runtime and actual ACP database"]
async fn native_live_wss_held_source() {
    native_daemon_case(
        true,
        true,
        false,
        false,
        false,
        false,
        Some((true, true, false)),
    )
    .await;
}
#[tokio::test]
#[ignore = "requires verified installed native runtime and actual ACP database"]
async fn native_live_uds_held_unchanged() {
    native_daemon_case(
        true,
        true,
        false,
        false,
        false,
        false,
        Some((false, false, true)),
    )
    .await;
}
#[tokio::test]
#[ignore = "requires verified installed native runtime and actual ACP database"]
async fn native_live_wss_held_unchanged() {
    native_daemon_case(
        true,
        true,
        false,
        false,
        false,
        false,
        Some((true, false, true)),
    )
    .await;
}
