//! End-to-end UDS slice test: seed via the store, then drive the daemon as a
//! JSON-RPC client over a temp Unix-domain socket (§5.7 DoD).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    now_iso, Config, ContentType, Note, NoteId, NoteVisibility, Workspace, WorkspaceActivity,
    WorkspaceApi, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

fn seed_workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "Seed WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts.clone(),
        last_activity: None,
        tags: vec!["seed".to_string()],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
    }
}

fn seed_note(ws: &WorkspaceId) -> Note {
    let ts = now_iso();
    Note {
        id: NoteId::from("note-seed"),
        workspace_id: ws.clone(),
        title: "Spec".to_string(),
        content: "# Seed".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: true,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        task: None,
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    }
}

async fn send(socket: &Path, frame: &str) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    write_half.write_all(frame.as_bytes()).await.expect("write");
    write_half.write_all(b"\n").await.expect("write nl");
    write_half.flush().await.expect("flush");
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read");
    serde_json::from_str(line.trim()).expect("valid json")
}

/// Drive several frames over ONE connection (so the per-connection `client_id`
/// binding from `client.hello` persists across them, §16), collecting the
/// ordered responses.
async fn send_session(socket: &Path, frames: &[&str]) -> Vec<Value> {
    let stream = UnixStream::connect(socket).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut out = Vec::new();
    for frame in frames {
        write_half.write_all(frame.as_bytes()).await.expect("write");
        write_half.write_all(b"\n").await.expect("write nl");
        write_half.flush().await.expect("flush");
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("read");
        out.push(serde_json::from_str(line.trim()).expect("valid json"));
    }
    out
}

#[tokio::test]
async fn uds_slice_end_to_end() {
    // Use a short base path: macOS caps UDS paths at ~104 bytes (SUN_LEN) and
    // `temp_dir()` resolves to a long `/var/folders/...` path.
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = Path::new("/tmp").join(format!("intentd-it-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("INTENTD_DATA_DIR", &dir);
    let config = Config::resolve().expect("resolve config");

    let ws_id = WorkspaceId::from("ws-seed");
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store
            .insert_workspace(&seed_workspace(&ws_id))
            .await
            .expect("seed ws");
        store
            .insert_note(&seed_note(&ws_id))
            .await
            .expect("seed note");
    }

    let store = Store::open(&config.db_path).await.expect("reopen store");
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_uds(services, bus, &socket, None, async move {
            let _ = rx.await;
        })
        .await
        .expect("serve");
    });

    // Wait for the socket to appear.
    for _ in 0..50 {
        if config.socket_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // (a) workspace.list
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.list"}"#,
    )
    .await;
    assert!(resp["result"].is_object(), "result must be an object");
    let wss = resp["result"]["workspaces"]
        .as_array()
        .expect("workspaces array");
    assert!(wss.iter().any(|w| w["id"] == json!("ws-seed")));

    // (b) note.list with the seeded workspaceId
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":2,"method":"note.list","params":{"workspaceId":"ws-seed"}}"#,
    )
    .await;
    assert!(resp["result"].is_object(), "result must be an object");
    let notes = resp["result"]["notes"].as_array().expect("notes array");
    assert!(notes.iter().any(|n| n["id"] == json!("note-seed")));

    // (c) malformed request (jsonrpc != "2.0") → -32600
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"1.0","id":3,"method":"workspace.list"}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32600));

    // (d) unknown method as a request → -32601
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":4,"method":"does.notExist"}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32601));

    // (e) workspace.* CRUD lifecycle: create → get → update → archive →
    //     unarchive → dismissAttention → delete (PROTOCOL §5.1).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":5,"method":"workspace.create","params":{"title":"Lifecycle WS"}}"#,
    )
    .await;
    let new_id = resp["result"]["workspace"]["id"]
        .as_str()
        .expect("created id")
        .to_string();
    assert_eq!(resp["result"]["workspace"]["title"], json!("Lifecycle WS"));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"workspace.get","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["workspace"]["id"], json!(new_id));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"workspace.update","params":{{"workspaceId":"{new_id}","title":"Renamed"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["workspace"]["title"], json!("Renamed"));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":8,"method":"workspace.archive","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":9,"method":"workspace.unarchive","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":10,"method":"workspace.dismissAttention","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["workspace"]["attention"], json!("none"));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":11,"method":"workspace.delete","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));

    // (f) get after delete → -32602 "Workspace not found".
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":12,"method":"workspace.get","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(resp["error"]["message"], json!("Workspace not found"));

    // (g) note.create on the seeded workspace, with a checkbox line so the
    //     task.* lifecycle has something to edit. Asserts the camelCase Note
    //     wire shape the iOS client expects (PROTOCOL §5.2).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":13,"method":"note.create","params":{"workspaceId":"ws-seed","title":"Task Note","content":"- [ ] Do the thing"}}"#,
    )
    .await;
    let note = &resp["result"]["note"];
    let task_note_id = note["id"].as_str().expect("note id").to_string();
    assert_eq!(note["title"], json!("Task Note"));
    assert_eq!(note["workspaceId"], json!("ws-seed"));
    assert_eq!(note["contentType"], json!("markdown"));
    for key in [
        "workspaceId",
        "contentType",
        "isPinned",
        "isArchived",
        "isDefault",
        "createdAt",
        "rev",
        "updatedAt",
    ] {
        assert!(note.get(key).is_some(), "Note must carry camelCase `{key}`");
    }
    assert!(
        note.get("workspace_id").is_none() && note.get("content_type").is_none(),
        "Note must not leak snake_case keys"
    );

    // (h) task.update the checkbox line → camelCase TaskUpdateResult.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":14,"method":"task.update","params":{{"workspaceId":"ws-seed","noteId":"{task_note_id}","line":1,"status":"done"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["lineNumber"], json!(1));
    assert_eq!(resp["result"]["status"], json!("done"));
    assert!(
        resp["result"].get("line_number").is_none(),
        "task.update must not leak snake_case keys"
    );

    // (i) task.markAsTask then task.updateNoteStatus → snake_case status words.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":15,"method":"task.markAsTask","params":{{"workspaceId":"ws-seed","noteId":"{task_note_id}","status":"not_started"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["status"], json!("not_started"));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":16,"method":"task.updateNoteStatus","params":{{"workspaceId":"ws-seed","noteId":"{task_note_id}","status":"in_progress"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["status"], json!("in_progress"));
    assert_eq!(
        resp["result"]["note"]["task"]["status"],
        json!("in_progress")
    );

    // (j) comment.add anchored to the seeded note's "# Seed" content
    //     (PROTOCOL §5.3). Asserts the camelCase CommentAddResult shape.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":17,"method":"comment.add","params":{"workspaceId":"ws-seed","noteId":"note-seed","searchContext":"Seed","commentTarget":"Seed","comment":"first comment"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));
    assert_eq!(resp["result"]["anchored"], json!(true));
    let comment_id = resp["result"]["commentId"]
        .as_str()
        .expect("commentId")
        .to_string();
    assert_eq!(resp["result"]["location"]["anchoredText"], json!("Seed"));
    assert!(
        resp["result"]["location"].get("anchored_text").is_none(),
        "comment.add must not leak snake_case keys"
    );

    // (k) comment.list with includeComments → camelCase thread summary + nested
    //     CommentWire (`type`, `authorType`, `createdAt`).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":18,"method":"comment.list","params":{"workspaceId":"ws-seed","noteId":"note-seed","includeComments":true}}"#,
    )
    .await;
    assert_eq!(resp["result"]["totalThreads"], json!(1));
    assert_eq!(resp["result"]["totalComments"], json!(1));
    let thread = &resp["result"]["threads"][0];
    assert_eq!(thread["threadId"], json!(comment_id));
    assert_eq!(thread["commentCount"], json!(1));
    let listed = &thread["comments"][0];
    assert_eq!(listed["type"], json!("comment"));
    assert_eq!(listed["authorType"], json!("agent"));
    assert!(
        listed.get("author_type").is_none() && listed.get("kind").is_none(),
        "CommentWire must use camelCase `authorType` and `type`"
    );

    // (l) comment.respond with a suggestion → nested `suggestionDiff`.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":19,"method":"comment.respond","params":{{"workspaceId":"ws-seed","noteId":"note-seed","threadId":"{comment_id}","comment":"try this","type":"suggestion","suggestionOriginal":"Seed","suggestionProposed":"Sprout"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));
    let reply = &resp["result"]["comment"];
    assert_eq!(reply["type"], json!("suggestion"));
    assert_eq!(reply["suggestionDiff"]["original"], json!("Seed"));
    assert_eq!(reply["suggestionDiff"]["proposed"], json!("Sprout"));
    assert_eq!(resp["result"]["thread"]["threadId"], json!(comment_id));
    assert_eq!(resp["result"]["thread"]["totalComments"], json!(2));

    // (m) comment.getThread → root + replies with camelCase fields.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":20,"method":"comment.getThread","params":{{"workspaceId":"ws-seed","noteId":"note-seed","threadId":"{comment_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["threadId"], json!(comment_id));
    assert_eq!(resp["result"]["rootComment"]["id"], json!(comment_id));
    assert_eq!(resp["result"]["totalComments"], json!(2));
    assert_eq!(resp["result"]["replies"][0]["type"], json!("suggestion"));

    // (n) comment.delete → success envelope.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":21,"method":"comment.delete","params":{{"workspaceId":"ws-seed","noteId":"note-seed","commentId":"{comment_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));

    // (o) agent.getModels (no workspaceId) → non-empty model catalog.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":22,"method":"agent.getModels"}"#,
    )
    .await;
    let models = resp["result"]["models"].as_array().expect("models array");
    assert!(!models.is_empty());

    // (p) agent.create → { agent: { id, name } } on the seeded workspace.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":23,"method":"agent.create","params":{"workspaceId":"ws-seed","name":"E2E Agent","model":"auggie:sonnet4.5"}}"#,
    )
    .await;
    let agent_id = resp["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    assert_eq!(resp["result"]["agent"]["name"], json!("E2E Agent"));

    // (q) agent.list → AgentLite projection with messageCount, no messages key.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":24,"method":"agent.list","params":{"workspaceId":"ws-seed"}}"#,
    )
    .await;
    let agents = resp["result"]["agents"].as_array().expect("agents array");
    let listed = agents
        .iter()
        .find(|a| a["id"] == json!(agent_id))
        .expect("created agent listed");
    assert_eq!(listed["messageCount"], json!(0));
    assert!(
        listed.get("messages").is_none() && listed.get("systemPrompt").is_none(),
        "AgentLite must strip messages/systemPrompt"
    );

    // (r) agent.sendMessage (agent idle) → delivered, queued:false.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":25,"method":"agent.sendMessage","params":{{"workspaceId":"ws-seed","agentId":"{agent_id}","content":"Run the tests","messageId":"m1"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));
    assert_eq!(resp["result"]["queued"], json!(false));
    assert_eq!(resp["result"]["messageId"], json!("m1"));

    // (s) agent.getConversation → the persisted user message.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":26,"method":"agent.getConversation","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["agentId"], json!(agent_id));
    assert_eq!(resp["result"]["totalMessages"], json!(1));
    let message = &resp["result"]["messages"][0];
    assert_eq!(message["role"], json!("user"));
    // Wire shape matches TS `AgentMessage`: `contentBlocks` + `timestamp`,
    // never `content`/`createdAt` (so the iOS conversation view renders).
    assert!(message["contentBlocks"].is_array());
    assert!(message["timestamp"].is_string());
    assert!(message.get("content").is_none());
    assert!(message.get("createdAt").is_none());

    // (t) agent.get unknown → -32602 "Agent not found".
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":27,"method":"agent.get","params":{"agentId":"agent-00000000-0000-0000-0000-000000000000"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(resp["error"]["message"], json!("Agent not found"));

    // (u) agent.stop → { success: true }, then agent.delete.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":28,"method":"agent.stop","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":29,"method":"agent.delete","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));

    // (v) client.hello establishes a logical clientId + `server` block (§5.17);
    //     drafts.* then round-trip on the SAME connection (the `client_id`
    //     binding is per-connection, §16).
    let sess = send_session(
        &config.socket_path,
        &[
            r#"{"jsonrpc":"2.0","id":30,"method":"client.hello","params":{"clientId":"cli-e2e","name":"E2E","capabilities":{"forward":true}}}"#,
            r#"{"jsonrpc":"2.0","id":31,"method":"drafts.set","params":{"workspaceId":"ws-seed","agentId":"agent-e2e","text":"draft body"}}"#,
            r#"{"jsonrpc":"2.0","id":32,"method":"drafts.get","params":{"workspaceId":"ws-seed","agentId":"agent-e2e"}}"#,
        ],
    )
    .await;
    assert_eq!(sess[0]["result"]["clientId"], json!("cli-e2e"));
    assert_eq!(sess[0]["result"]["server"]["locality"], json!("local"));
    assert!(sess[0]["result"]["server"]["osArch"]
        .as_str()
        .unwrap()
        .contains('/'));
    assert_eq!(sess[1]["result"]["ok"], json!(true));
    assert!(sess[1]["result"]["updatedAt"].is_string());
    assert_eq!(sess[2]["result"]["text"], json!("draft body"));

    // (w) a fresh connection re-presenting the same clientId restores the draft.
    let sess = send_session(
        &config.socket_path,
        &[
            r#"{"jsonrpc":"2.0","id":33,"method":"client.hello","params":{"clientId":"cli-e2e"}}"#,
            r#"{"jsonrpc":"2.0","id":34,"method":"drafts.get","params":{"workspaceId":"ws-seed","agentId":"agent-e2e"}}"#,
        ],
    )
    .await;
    assert_eq!(sess[0]["result"]["clientId"], json!("cli-e2e"));
    assert_eq!(
        sess[1]["result"]["text"],
        json!("draft body"),
        "reconnect restores the draft"
    );

    // (x) an anonymous connection (no hello) never sees another client's draft,
    //     round-trips its own, and an empty set clears it.
    let sess = send_session(
        &config.socket_path,
        &[
            r#"{"jsonrpc":"2.0","id":35,"method":"drafts.get","params":{"workspaceId":"ws-seed","agentId":"agent-e2e"}}"#,
            r#"{"jsonrpc":"2.0","id":36,"method":"drafts.set","params":{"workspaceId":"ws-seed","agentId":"agent-e2e","text":"anon"}}"#,
            r#"{"jsonrpc":"2.0","id":37,"method":"drafts.get","params":{"workspaceId":"ws-seed","agentId":"agent-e2e"}}"#,
            r#"{"jsonrpc":"2.0","id":38,"method":"drafts.set","params":{"workspaceId":"ws-seed","agentId":"agent-e2e","text":""}}"#,
            r#"{"jsonrpc":"2.0","id":39,"method":"drafts.get","params":{"workspaceId":"ws-seed","agentId":"agent-e2e"}}"#,
        ],
    )
    .await;
    assert_eq!(
        sess[0]["result"],
        Value::Null,
        "anonymous sees no other client's draft"
    );
    assert_eq!(sess[1]["result"]["ok"], json!(true));
    assert_eq!(sess[2]["result"]["text"], json!("anon"));
    assert_eq!(sess[3]["result"]["ok"], json!(true));
    assert!(
        sess[3]["result"].get("updatedAt").is_none(),
        "empty set is a clear"
    );
    assert_eq!(sess[4]["result"], Value::Null, "the draft was cleared");

    let _ = tx.send(());
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);
}
