//! End-to-end UDS slice test: seed via the store, then drive the daemon as a
//! JSON-RPC client over a temp Unix-domain socket (§5.7 `DoD`).

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    now_iso, Config, ContentType, Note, NoteId, NoteMetadata, NoteVisibility, Workspace,
    WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
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
        status_image_asset_id: None,
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
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

fn seed_note(ws: &WorkspaceId) -> Note {
    let ts = now_iso();
    Note {
        id: NoteId::from("note-seed"),
        workspace_id: ws.clone(),
        title: "Seed Note".to_string(),
        content: "# Seed".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: true,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
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
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(ws_root.path().to_path_buf())
            // Keep the (y) github.* section hermetic: `github.connect` must
            // deterministically fail fast (port 0 is never a valid
            // destination), never touch the real github.com.
            .with_github_login_base_uri("http://127.0.0.1:0"),
    );
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
    // §5.1: archive/unarchive return the updated `workspace` record (no
    // `{success:true}`) so callers do not need a follow-up `workspace.get`.
    assert_eq!(resp["result"]["workspace"]["id"], json!(new_id.as_str()));
    assert_eq!(resp["result"]["workspace"]["archived"], json!(true));
    assert_eq!(resp["result"]["workspace"]["status"], json!("Archived"));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":9,"method":"workspace.unarchive","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["workspace"]["id"], json!(new_id.as_str()));
    assert_eq!(resp["result"]["workspace"]["archived"], json!(false));
    assert_eq!(resp["result"]["workspace"]["status"], json!("Active"));

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

    // (f2) workspace.getTokenUsage on the seeded workspace returns the default
    //      (empty, lastScanAt: null) snapshot before any scan has run (§5.23).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":120,"method":"workspace.getTokenUsage","params":{"workspaceId":"ws-seed"}}"#,
    )
    .await;
    let tu = &resp["result"]["tokenUsage"];
    assert!(tu.is_object(), "tokenUsage must be an object");
    assert_eq!(tu["byAgentId"], json!({}));
    assert_eq!(tu["byModel"], json!({}));
    assert_eq!(
        tu["totals"],
        json!({ "inputTokens": 0, "outputTokens": 0, "cacheReadTokens": 0, "cacheCreationTokens": 0 })
    );
    assert_eq!(tu["lastScanAt"], Value::Null);

    // (f3) getTokenUsage on the deleted workspace → -32602 "Workspace not found".
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":121,"method":"workspace.getTokenUsage","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(resp["error"]["message"], json!("Workspace not found"));

    // (f4) workspace.getSetupScript on the seeded workspace returns the default
    //      (empty `script`, `updatedAt: 0`, no `projectType`/`generatedBy`) record
    //      before any save (§5.25).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":130,"method":"workspace.getSetupScript","params":{"workspaceId":"ws-seed"}}"#,
    )
    .await;
    let s = &resp["result"]["setupScript"];
    assert!(s.is_object(), "setupScript must be an object");
    assert_eq!(s["script"], json!(""));
    assert_eq!(s["updatedAt"], json!(0));
    assert_eq!(s.get("projectType"), None);
    assert_eq!(s.get("generatedBy"), None);

    // (f5) saveSetupScript now requires a repository path (PR #223: repo config
    //      sole source). Without worktreePath or repositoryPath → -32602.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":131,"method":"workspace.saveSetupScript","params":{"workspaceId":"ws-seed","script":"echo hello"}}"#,
    )
    .await;
    assert_eq!(
        resp["error"]["code"],
        json!(-32602),
        "saveSetupScript without repository path should return InvalidParams"
    );

    // (f6) getSetupScript still returns the empty default (no repo config to read).

    // (f7) saveSetupScript with a missing `script` param → -32602.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":133,"method":"workspace.saveSetupScript","params":{"workspaceId":"ws-seed"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (f8) detectProjectType on a workspace whose worktree holds a Cargo.toml →
    //      "rust"; generateSetupScript drafts a `cargo fetch` script stamped
    //      `generatedBy:"agent"` with the detected `projectType` (§5.25).
    let manifest_dir = dir.join("rust-ws");
    std::fs::create_dir_all(&manifest_dir).unwrap();
    std::fs::write(manifest_dir.join("Cargo.toml"), "[package]\nname=\"x\"\n").unwrap();
    let wt = manifest_dir.to_string_lossy().to_string();
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":134,"method":"workspace.create","params":{{"title":"Rust WS","worktreePath":"{wt}"}}}}"#
        ),
    )
    .await;
    let rust_id = resp["result"]["workspace"]["id"]
        .as_str()
        .expect("rust ws id")
        .to_string();
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":135,"method":"workspace.detectProjectType","params":{{"workspaceId":"{rust_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["projectType"], json!("rust"));
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":136,"method":"workspace.generateSetupScript","params":{{"workspaceId":"{rust_id}"}}}}"#
        ),
    )
    .await;
    let s = &resp["result"]["setupScript"];
    assert_eq!(s["projectType"], json!("rust"));
    assert_eq!(s["generatedBy"], json!("agent"));
    assert!(s["script"]
        .as_str()
        .expect("script")
        .contains("cargo fetch"));

    // (f9) detectProjectType on a workspace with no worktree manifest → null.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":137,"method":"workspace.detectProjectType","params":{"workspaceId":"ws-seed"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["projectType"], Value::Null);

    // (f10) getSetupScript on the deleted workspace → -32602 "Workspace not found".
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":138,"method":"workspace.getSetupScript","params":{{"workspaceId":"{new_id}"}}}}"#
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
        resp["result"]["note"]["metadata"]["task"]["status"],
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
    assert!(
        resp["result"]["noteRev"].is_i64(),
        "comment.add must echo the post-rewrite noteRev (monorepo#638): {}",
        resp["result"]
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

    // (o) agent.getModels (no workspaceId) → a models array (possibly empty
    // when no auggie CLI is available — no static fallback catalog remains).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":22,"method":"agent.getModels"}"#,
    )
    .await;
    resp["result"]["models"].as_array().expect("models array");

    // (o′) models.list (§5.30) → rich catalog + `source` tag; rows present
    // carry the id/name/provider triple.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":93,"method":"models.list"}"#,
    )
    .await;
    let models = resp["result"]["models"].as_array().expect("models array");
    for m in models {
        assert!(m["id"].is_string());
        assert!(m["name"].is_string());
        assert!(m["provider"].is_string());
    }
    let source = resp["result"]["source"].as_str().expect("source");
    assert!(source == "auggie" || source == "static", "source: {source}");

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
    // No client id was supplied, so `appMessageId` stays off the wire
    // (backward compatible — PROTOCOL §5.5).
    assert!(message.get("appMessageId").is_none());

    // (s0) agent.sendMessage with `userAppMessageId` → the id folds into the
    // row metadata and round-trips as `appMessageId` on the conversation read
    // (activates the FE dedup guard, PROTOCOL §5.5).
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":251,"method":"agent.sendMessage","params":{{"workspaceId":"ws-seed","agentId":"{agent_id}","content":"tagged send","messageId":"m2","userAppMessageId":"app-msg-e2e-1"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));
    assert_eq!(resp["result"]["messageId"], json!("m2"));
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":252,"method":"agent.getConversation","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    let tagged = resp["result"]["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| m["id"] == json!("m2"))
        .expect("tagged user row present");
    assert_eq!(tagged["appMessageId"], json!("app-msg-e2e-1"));
    assert_eq!(
        tagged["metadata"]["userAppMessageId"],
        json!("app-msg-e2e-1")
    );

    // (s0a) An oversized `userAppMessageId` is rejected at the router → -32602.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":253,"method":"agent.sendMessage","params":{{"workspaceId":"ws-seed","agentId":"{agent_id}","content":"too big","userAppMessageId":"{big_id}"}}}}"#,
            big_id = "x".repeat(300)
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (s0b) agent.sendQueuedMessageNow → the store-only fallback atomically
    // dequeues the queued entry and persists it under the ENTRY id, which the
    // result's `messageId` echoes (PROTOCOL §5.5).
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":254,"method":"agent.queueMessage","params":{{"agentId":"{agent_id}","content":"queued then sent now"}}}}"#
        ),
    )
    .await;
    let queued_id = resp["result"]["queuedMessage"]["id"]
        .as_str()
        .expect("queued entry id")
        .to_string();
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":255,"method":"agent.sendQueuedMessageNow","params":{{"workspaceId":"ws-seed","agentId":"{agent_id}","messageId":"{queued_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));
    assert_eq!(resp["result"]["queued"], json!(false));
    assert_eq!(resp["result"]["messageId"], json!(queued_id));
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":256,"method":"agent.getConversation","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    let sent_now = resp["result"]["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| m["id"] == json!(queued_id))
        .expect("dequeued user row present under the entry id");
    assert_eq!(sent_now["role"], json!("user"));

    // (s0c) agent.sendQueuedMessageNow with an unknown entry id → -32602 with
    // NO side effects (deliberately NOT idempotent, PROTOCOL §5.5).
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":257,"method":"agent.sendQueuedMessageNow","params":{{"workspaceId":"ws-seed","agentId":"{agent_id}","messageId":"no-such-entry"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (s1a) agent.getSession → full `AgentSession` (superset of AgentLite):
    // `messages` is present as an array (the field AgentLite strips). Confirms
    // the C1d/C1e loadAgent rehydration RPC returns the full shape (§5.5).
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":2401,"method":"agent.getSession","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    let session = &resp["result"]["session"];
    assert_eq!(session["id"], json!(agent_id));
    assert!(session["messages"].is_array());
    assert!(session["name"].is_string());

    // (s1b) agent.getSession unknown → -32602 "Agent not found".
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":2402,"method":"agent.getSession","params":{"agentId":"agent-00000000-0000-0000-0000-000000000000"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(resp["error"]["message"], json!("Agent not found"));

    // (s1c) agent.update patches `systemPrompt` + `isBackground`; the round
    // trip through agent.getSession proves the patch persisted.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":2403,"method":"agent.update","params":{{"agentId":"{agent_id}","changes":{{"systemPrompt":"be helpful","isBackground":true}}}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));
    assert_eq!(resp["result"]["agent"]["id"], json!(agent_id));
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":2404,"method":"agent.getSession","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(
        resp["result"]["session"]["systemPrompt"],
        json!("be helpful")
    );
    assert_eq!(resp["result"]["session"]["isBackground"], json!(true));

    // (s1d) agent.update rejects unknown field → -32602.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":2405,"method":"agent.update","params":{{"agentId":"{agent_id}","changes":{{"nope":"x"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (s1e) agent.appendMessage + agent.replaceMessages: append a user
    // message, then atomically swap the transcript with two fresh entries at
    // seq 0/1. Row ids are minted by the store so callers cannot smuggle
    // stale ids across the swap (§5.5).
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":2406,"method":"agent.appendMessage","params":{{"agentId":"{agent_id}","role":"user","contentBlocks":[{{"type":"text","text":"wake"}}]}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));
    assert_eq!(resp["result"]["message"]["role"], json!("user"));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":2407,"method":"agent.replaceMessages","params":{{"agentId":"{agent_id}","messages":[{{"role":"user","contentBlocks":[{{"type":"text","text":"edit"}}]}},{{"role":"assistant","contentBlocks":[{{"type":"text","text":"ok"}}]}}]}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));
    let swapped = resp["result"]["messages"].as_array().expect("messages");
    assert_eq!(swapped.len(), 2);
    assert_eq!(swapped[0]["seq"], json!(0));
    assert_eq!(swapped[1]["seq"], json!(1));

    // (s2) agent.getSessionStats → `{ stats: SessionStats }`. With auggie
    // unavailable in CI the counts derive from the transcript (one persisted
    // user message), and `creditsUsed` serializes as explicit `null` (§5.24
    // `number|null`) rather than being fabricated or omitted.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":261,"method":"agent.getSessionStats","params":{{"sessionId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    let stats = &resp["result"]["stats"];
    assert!(stats["messageCount"].is_number());
    assert!(stats["toolCount"].is_number());
    assert!(stats["creditsUsed"].is_null());

    // (s3) agent.getSessionStats unknown → -32602 "Session not found".
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":262,"method":"agent.getSessionStats","params":{"sessionId":"agent-00000000-0000-0000-0000-000000000000"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(resp["error"]["message"], json!("Session not found"));

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

    // (y) github.* browse / auth / identity (PROTOCOL §5.27), token-absent path.
    //     `connect` starts a real device flow, but the login host is pinned to
    //     an invalid port-0 address above, so it fails with a graceful domain
    //     error (never a hang, never live network); `cancelAuth` / `revoke` are
    //     idempotent no-ops with nothing in flight; `authStatus` validates the
    //     env PAT and degrades gracefully to a well-formed
    //     `{ isConfigured: bool, ..., deviceFlow: null }` (the single
    //     `GET /user` probe is swallowed on failure, so this never asserts on
    //     live network).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":40,"method":"github.connect","params":{}}"#,
    )
    .await;
    assert_eq!(
        resp["error"]["code"],
        json!(-32603),
        "connect against an invalid login host is a graceful Internal error"
    );

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":41,"method":"github.cancelAuth","params":{}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(
        resp["result"]["cancelled"],
        json!(false),
        "nothing in flight — cancel is an idempotent no-op"
    );

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":43,"method":"github.revoke","params":{}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":42,"method":"github.authStatus","params":{}}"#,
    )
    .await;
    assert!(resp["result"].is_object(), "authStatus result is an object");
    assert!(
        resp["result"]["isConfigured"].is_boolean(),
        "isConfigured is a boolean"
    );
    assert_eq!(resp["result"]["oauthUrl"], json!(""));
    assert_eq!(resp["result"]["deviceFlow"], Value::Null);
    // 🔒 The PAT is never echoed over the wire.
    assert!(resp["result"].get("token").is_none());

    let _ = tx.send(());
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);
}
