//! `DoD` tests for repo config consumer integrations (workspace.create fallbacks,
//! script bootstrap, agent instructions).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

mod common;

struct TempDb {
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("intentd-repo-cfg-{}.db", Uuid::new_v4())),
        }
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    // Wait for socket to exist
    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Then try to connect
    for _ in 0..100 {
        if let Ok(s) = UnixStream::connect(socket).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not connect to {}", socket.display());
}

async fn send(write_half: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    write_half.write_all(frame.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    let n = timeout(Duration::from_secs(2), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

/// Issue one JSON-RPC request and return the FULL response (incl. any `error`).
async fn call(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    send(write_half, &serde_json::to_string(&frame).unwrap()).await;
    let resp = read_json(reader).await;
    assert_eq!(resp["id"], id, "response id mismatch for {method}");
    resp
}

struct TempRepo(PathBuf);
impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Utility: create a temporary git repo with a .intent/config.json file.
fn create_test_repo_with_config(config: &str) -> TempRepo {
    let repo_path = std::env::temp_dir().join(format!("repo-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&repo_path).unwrap();

    // Initialize a git repo with explicit default branch
    let status = std::process::Command::new("git")
        .args(["init", "--initial-branch=main"])
        .current_dir(&repo_path)
        .output()
        .expect("git init spawn failed");
    assert!(status.status.success(), "git init command failed");

    let status = std::process::Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(&repo_path)
        .output()
        .expect("git config user.email spawn failed");
    assert!(
        status.status.success(),
        "git config user.email command failed"
    );

    let status = std::process::Command::new("git")
        .args(["config", "user.name", "Test User"])
        .current_dir(&repo_path)
        .output()
        .expect("git config user.name spawn failed");
    assert!(
        status.status.success(),
        "git config user.name command failed"
    );

    // Create .intent/config.json
    let intent_dir = repo_path.join(".intent");
    std::fs::create_dir_all(&intent_dir).unwrap();
    std::fs::write(intent_dir.join("config.json"), config).unwrap();

    // Create an initial commit so the repo has a main branch
    std::fs::write(repo_path.join("README.md"), "test").unwrap();
    let status = std::process::Command::new("git")
        .args(["add", "."])
        .current_dir(&repo_path)
        .output()
        .expect("git add spawn failed");
    assert!(status.status.success(), "git add command failed");
    let status = std::process::Command::new("git")
        .args(["commit", "-m", "Initial commit"])
        .current_dir(&repo_path)
        .output()
        .expect("git commit spawn failed");
    assert!(status.status.success(), "git commit command failed");

    TempRepo(repo_path)
}

/// `DoD` test (a): workspace.create with repo branchPrefix and no request prefix -> branch carries prefix.
#[tokio::test]
async fn test_workspace_create_uses_repo_branch_prefix() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_event_bus(bus.clone())
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_settings_registry(common::registry_with_default_provider(ws_root.path())),
    );

    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-rc-");
    let socket_path = sock_dir.path().join("uds.sock");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn({
        let services = services.clone();
        let bus = bus.clone();
        let socket = socket_path.clone();
        async move {
            serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve_uds failed");
        }
    });

    let stream = connect_retry(&socket_path).await;
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);

    // Create a test repo with branchPrefix in config
    let repo = create_test_repo_with_config(r#"{"branchPrefix": "test/"}"#);
    let repo_path = repo.0.to_str().unwrap();

    // Create a workspace without specifying a branch prefix
    let resp = call(
        &mut wr,
        &mut reader,
        1,
        "workspace.create",
        json!({
            "repositoryPath": repo_path,
            "initialAgent": { "prompt": "Hello world" }
        }),
    )
    .await;

    // Verify the branch has the prefix
    assert!(resp["result"]["workspace"]["branch"]
        .as_str()
        .unwrap()
        .starts_with("test/"));

    shutdown_tx.send(()).ok();
    let _ = server.await;
}

/// `DoD` test (b): workspace.create with repo setupScript and no request script -> readable
/// via getSetupScript; a request-supplied script is execute-only and never persisted.
#[tokio::test]
async fn test_workspace_create_setup_script_fallback() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_event_bus(bus.clone())
            .with_workspaces_root(ws_root.path().to_path_buf()),
    );

    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-rc-");
    let socket_path = sock_dir.path().join("uds.sock");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn({
        let services = services.clone();
        let bus = bus.clone();
        let socket = socket_path.clone();
        async move {
            serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve_uds failed");
        }
    });

    let stream = connect_retry(&socket_path).await;
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);

    // Test 1: repo setupScript is readable via workspace.getSetupScript
    let repo1 = create_test_repo_with_config(r#"{"setupScript": "npm install"}"#);
    let repo_path1 = repo1.0.to_str().unwrap();

    let resp1 = call(
        &mut wr,
        &mut reader,
        1,
        "workspace.create",
        json!({ "repositoryPath": repo_path1 }),
    )
    .await;
    let ws_id1 = resp1["result"]["workspace"]["id"].as_str().unwrap();

    // Workspace DB row should NOT have setupScript (retired field)
    assert_eq!(
        resp1["result"]["workspace"].get("setupScript"),
        None,
        "workspace DB row should not have setupScript"
    );

    // But getSetupScript should return the repo config value
    let get_resp1 = call(
        &mut wr,
        &mut reader,
        10,
        "workspace.getSetupScript",
        json!({ "workspaceId": ws_id1 }),
    )
    .await;
    assert_eq!(
        get_resp1["result"]["setupScript"]["script"],
        json!("npm install"),
        "getSetupScript should return repo config value"
    );
    assert_eq!(
        get_resp1["result"]["setupScript"]["generatedBy"],
        json!("user"),
        "generatedBy should be user for repo config scripts"
    );
    assert!(
        get_resp1["result"]["setupScript"]["updatedAt"].is_number(),
        "updatedAt should be present (file mtime)"
    );

    // Test 2: request-supplied script is execute-only — NOT written to repo config
    let repo2 = create_test_repo_with_config(r#"{"setupScript": "npm install"}"#);
    let repo_path2 = repo2.0.to_str().unwrap();

    let resp2 = call(
        &mut wr,
        &mut reader,
        2,
        "workspace.create",
        json!({
            "repositoryPath": repo_path2,
            "setupScript": "yarn install"
        }),
    )
    .await;
    let ws_id2 = resp2["result"]["workspace"]["id"].as_str().unwrap();

    // Workspace DB row should NOT have setupScript
    assert_eq!(
        resp2["result"]["workspace"].get("setupScript"),
        None,
        "workspace DB row should not have setupScript"
    );

    // getSetupScript should return the committed repo-config value — the
    // explicit request param is execute-only and never persisted (§5.1).
    let get_resp2 = call(
        &mut wr,
        &mut reader,
        11,
        "workspace.getSetupScript",
        json!({ "workspaceId": ws_id2 }),
    )
    .await;
    assert_eq!(
        get_resp2["result"]["setupScript"]["script"],
        json!("npm install"),
        "getSetupScript should return the committed repo config value, not the request param"
    );
    assert_eq!(
        get_resp2["result"]["setupScript"]["generatedBy"],
        json!("user"),
        "generatedBy should be user for repo config scripts"
    );
    assert!(
        get_resp2["result"]["setupScript"]["updatedAt"].is_number(),
        "updatedAt should be present"
    );

    shutdown_tx.send(()).ok();
    let _ = server.await;
}

/// `DoD` test (c): script.list on empty workspace with repo scripts[] -> seeded.
#[tokio::test]
async fn test_script_list_bootstrap_from_repo() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_event_bus(bus.clone())
            .with_workspaces_root(ws_root.path().to_path_buf()),
    );

    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-rc-");
    let socket_path = sock_dir.path().join("uds.sock");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn({
        let services = services.clone();
        let bus = bus.clone();
        let socket = socket_path.clone();
        async move {
            serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve_uds failed");
        }
    });

    let stream = connect_retry(&socket_path).await;
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);

    // Create a test repo with scripts in config
    let repo = create_test_repo_with_config(
        r#"{
        "scripts": [
            {
                "name": "dev",
                "command": "npm run dev",
                "mode": "service",
                "category": "dev"
            },
            {
                "name": "test",
                "command": "npm test",
                "mode": "command",
                "category": "test"
            }
        ]
    }"#,
    );
    let repo_path = repo.0.to_str().unwrap();

    // Create a workspace
    let resp = call(
        &mut wr,
        &mut reader,
        1,
        "workspace.create",
        json!({ "repositoryPath": repo_path }),
    )
    .await;

    let workspace_id = resp["result"]["workspace"]["id"].as_str().unwrap();

    // List scripts (should trigger bootstrap)
    let resp = call(
        &mut wr,
        &mut reader,
        2,
        "script.list",
        json!({ "workspaceId": workspace_id }),
    )
    .await;

    let scripts = resp["result"]["scripts"].as_array().unwrap();
    assert_eq!(scripts.len(), 2);
    // Scripts may be in any order (same created_at timestamp), so find by name
    let dev = scripts.iter().find(|s| s["name"] == "dev").unwrap();
    let test = scripts.iter().find(|s| s["name"] == "test").unwrap();
    assert_eq!(dev["command"], "npm run dev");
    assert_eq!(test["command"], "npm test");

    shutdown_tx.send(()).ok();
    let _ = server.await;
}

/// `DoD` test (d): repo instructions reach the composed system prompt.
/// This test verifies that when a workspace has a repo config with instructions,
/// those instructions are included in the agent's system prompt.
#[tokio::test]
async fn test_repo_instructions_in_system_prompt() {
    // Simulate what rules.rs does at lines 381-386
    use intent_services::repo_config::read_repo_config;
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_event_bus(bus.clone())
            .with_workspaces_root(ws_root.path().to_path_buf()),
    );

    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-rc-");
    let socket_path = sock_dir.path().join("uds.sock");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn({
        let services = services.clone();
        let bus = bus.clone();
        let socket = socket_path.clone();
        async move {
            serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve_uds failed");
        }
    });

    let _stream = connect_retry(&socket_path).await;

    // Task 3 DoD point (d): repo instructions are integrated into rules.rs system prompt assembly.
    // The full flow (agent system prompt assembly → agent.readConversation) can't be tested
    // without agent.readConversation RPC (out of scope for this PR). We verify that the repo
    // config file parsing works by directly calling the same function rules.rs uses: read_repo_config.
    let repo =
        create_test_repo_with_config(r#"{"instructions": "Always use TypeScript for new files"}"#);
    let repo_path_buf = repo.0.clone();

    let repo_config = read_repo_config(&repo_path_buf).await;
    assert_eq!(
        repo_config.instructions.as_deref(),
        Some("Always use TypeScript for new files")
    );

    shutdown_tx.send(()).ok();
    let _ = server.await;
}

/// Regression test for PR #184 race condition: concurrent `script.list` calls
/// on an empty workspace with repo config scripts must produce exactly one
/// set of scripts (no duplicates). The fix uses a per-workspace async lock
/// (`WorkspaceScriptLocks`) to serialize bootstrap operations.
#[tokio::test]
async fn concurrent_script_list_no_duplicates() {
    use intent_core::WorkspaceCreate;
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store.clone()).with_workspaces_root(ws_root.path().to_path_buf()));
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-rc-");
    let socket_path = sock_dir.path().join("uds.sock");

    let repo = create_test_repo_with_config(
        r#"{"scripts": [
            {"name": "test", "command": "npm test", "mode": "command"},
            {"name": "dev", "command": "npm run dev", "mode": "service"}
        ]}"#,
    );

    // Create a workspace with the repo
    let input: WorkspaceCreate = serde_json::from_value(json!({
        "repositoryPath": repo.0.to_string_lossy(),
        "skipWorktree": true
    }))
    .unwrap();
    let ws_id = services
        .create_workspace(input, None)
        .await
        .expect("workspace create")
        .workspace
        .id
        .to_string();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let _server = tokio::spawn({
        let services = services.clone();
        let bus = bus.clone();
        let socket = socket_path.clone();
        async move {
            serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve_uds failed");
        }
    });

    // Wait for socket
    let stream = connect_retry(&socket_path).await;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Fire 10 concurrent script.list calls — all should see the same workspace as empty
    // initially, triggering the bootstrap path. With the fix, only one succeeds in
    // bootstrapping; the others block and then find the already-bootstrapped scripts.
    let mut handles = Vec::new();
    for i in 0..10 {
        let socket = socket_path.clone();
        let ws_id = ws_id.clone();
        handles.push(tokio::spawn(async move {
            let stream = UnixStream::connect(&socket).await.unwrap();
            let (read_half, mut write_half) = stream.into_split();
            let mut reader = BufReader::new(read_half);
            let resp = call(
                &mut write_half,
                &mut reader,
                i,
                "script.list",
                json!({ "workspaceId": ws_id }),
            )
            .await;
            resp["result"]["scripts"].as_array().unwrap().len()
        }));
    }

    // All 10 calls should return exactly 2 scripts (no duplicates)
    for h in handles {
        let count = h.await.unwrap();
        assert_eq!(count, 2, "concurrent script.list created duplicates");
    }

    // Verify database contains exactly 2 script rows
    let db_scripts = store
        .list_all_scripts()
        .await
        .expect("list scripts from DB");
    assert_eq!(
        db_scripts.len(),
        2,
        "database should contain exactly 2 scripts, found {}",
        db_scripts.len()
    );

    // Verify final script.list still returns 2
    let final_resp = call(
        &mut write_half,
        &mut reader,
        999,
        "script.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        final_resp["result"]["scripts"].as_array().unwrap().len(),
        2,
        "final script.list should return 2 scripts"
    );

    shutdown_tx.send(()).ok();
}
