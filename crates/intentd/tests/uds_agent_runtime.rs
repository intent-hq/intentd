//! Over-the-wire ACP runtime E2E (spec §13.2 / M3.10): drive the REAL `intentd
//! serve` daemon over its UDS transport — NOT a library-level `AgentManager`
//! built in-test — and prove the `agent.*` RPC handlers orchestrate the live
//! spawn/turn/MCP loop.
//!
//! The DB is pre-seeded with a workspace + target note, then the daemon process
//! is launched with the mock ACP provider. A UDS client subscribes to events,
//! calls `agent.create` + `agent.sendMessage` (model `mock:default`), and we
//! assert the daemon-spawned child reached the per-agent workspace MCP server
//! via the generated `--mcp-config` (mutating the note), and that the tool's
//! `note:updated` domain event and the terminal `agent:stream:end` arrive over
//! the transport.
//!
//! Gated by `node` + the mock script (the CI ACP gate); skips cleanly otherwise.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use intent_core::{
    now_iso, NoteCreate, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
    WorkspaceId, WorkspaceStatus,
};
use intent_services::Services;
use intent_store::Store;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;
use tokio::time::timeout;
use uuid::Uuid;

const MARKER: &str = "MCP_TOOL_MARKER_otw_e2e";

fn workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "OTW".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: None,
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
        archived: false,
        archived_at: None,
    }
}

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    for _ in 0..200 {
        if let Ok(s) = UnixStream::connect(socket).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("could not connect to {}", socket.display());
}

async fn send(write_half: &mut (impl AsyncWriteExt + Unpin), frame: Value) {
    let line = serde_json::to_string(&frame).unwrap();
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>, secs: u64) -> Value {
    let mut line = String::new();
    let n = timeout(Duration::from_secs(secs), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

/// Issue one request on a response-only connection and return its `result`.
async fn rpc(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    send(
        write_half,
        json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }),
    )
    .await;
    let resp = read_json(reader, 5).await;
    assert_eq!(resp["id"], id, "response id mismatch for {method}");
    assert!(resp.get("error").is_none(), "rpc {method} errored: {resp}");
    resp["result"].clone()
}

#[tokio::test]
async fn daemon_drives_agent_turn_and_mcp_tool_call_over_uds() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping over-the-wire agent E2E: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping over-the-wire agent E2E: script not found at {script}");
        return;
    }

    // Pre-seed the daemon's DB with a workspace + target note (the daemon opens
    // this same data dir on launch). We close the store before launching so the
    // daemon process gets a clean handle.
    // Keep the dir name short: the UDS path must fit within SUN_LEN (~104B).
    let data_dir =
        std::env::temp_dir().join(format!("itd-{}", &Uuid::new_v4().simple().to_string()[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let db_path = data_dir.join("intentd.db");
    let ws = WorkspaceId::new();
    let note_id = {
        let store = Store::open(&db_path).await.expect("open store");
        let services = Services::new(store.clone());
        store
            .insert_workspace(&workspace(&ws))
            .await
            .expect("insert ws");
        let note = services
            .create_note(
                ws.clone(),
                NoteCreate {
                    title: "Target".into(),
                    content: Some("# Target\n".into()),
                    tags: None,
                    parent_id: None,
                },
            )
            .await
            .expect("create note");
        note.id.0
    };

    let behavior = json!({
        "toolCall": { "name": "add_to_note_workspace-mcp", "arguments": { "noteId": note_id, "content": MARKER } },
        "response": "added via mcp over the wire",
    })
    .to_string();

    // Launch the REAL daemon. It reads MOCK_AGENT_SCRIPT_PATH/MOCK_AGENT_BEHAVIOR
    // when resolving the `mock` provider for the agent and forwards the behavior
    // to the spawned child; its own `intentd` binary backs the MCP bridge.
    let log_path = data_dir.join("daemon.log");
    let log = std::fs::File::create(&log_path).expect("create daemon log");
    let child = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("serve")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("MOCK_AGENT_SCRIPT_PATH", &script)
        .env("MOCK_AGENT_BEHAVIOR", &behavior)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn intentd serve");
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    if timeout(Duration::from_secs(10), async {
        loop {
            if UnixStream::connect(&socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_err()
    {
        let logs = std::fs::read_to_string(&log_path).unwrap_or_default();
        panic!(
            "daemon never listened on {}\n--- daemon log ---\n{logs}",
            socket.display()
        );
    }

    // Subscriber connection (events.event notifications) — established BEFORE the
    // turn so no events are missed.
    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = BufReader::new(sub_read);
    send(
        &mut sub_write,
        json!({ "jsonrpc": "2.0", "id": 1, "method": "events.subscribe",
            "params": { "eventTypes": ["agent:*", "note:*"], "workspaceId": ws.0 } }),
    )
    .await;
    let sub_resp = read_json(&mut sub_reader, 5).await;
    assert!(
        sub_resp["result"]["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC connection (responses only) — create the agent and send a message.
    let (rpc_read, mut rpc_write) = connect_retry(&socket).await.into_split();
    let mut rpc_reader = BufReader::new(rpc_read);
    let created = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        10,
        "agent.create",
        json!({ "workspaceId": ws.0, "name": "OTW", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws.0, "agentId": agent_id, "content": "please add" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Collect events until the terminal stream:end. The daemon-spawned child's
    // MCP tool call fires `note:updated`; the turn streams a chunk + one end.
    let mut saw_note_updated = false;
    let mut saw_stream_end = false;
    let mut saw_chunk = false;
    for _ in 0..50 {
        let frame = read_json(&mut sub_reader, 30).await;
        if frame["method"] != "events.event" {
            continue;
        }
        match frame["params"]["event"]["type"].as_str() {
            Some("note:updated") => saw_note_updated = true,
            Some("agent:stream:chunk") => saw_chunk = true,
            Some("agent:stream:end") => {
                saw_stream_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_note_updated,
        "tool's note:updated domain event received over the transport"
    );
    assert!(saw_chunk, "at least one agent:stream:chunk received");
    assert!(saw_stream_end, "terminal agent:stream:end received");

    // BE state changed via the daemon-spawned child's real MCP tool call.
    let note = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "note.get",
        json!({ "workspaceId": ws.0, "noteId": note_id }),
    )
    .await;
    assert!(
        note["note"]["content"]
            .as_str()
            .unwrap_or_default()
            .contains(MARKER)
            || note["content"]
                .as_str()
                .unwrap_or_default()
                .contains(MARKER),
        "note mutated by the daemon-spawned MCP tool call: {note}"
    );
}
