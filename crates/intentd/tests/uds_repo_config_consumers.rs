//! DoD tests for repo config consumer integrations (workspace.create fallbacks,
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

    // Initialize a git repo
    std::process::Command::new("git")
        .args(["init"])
        .current_dir(&repo_path)
        .output()
        .unwrap();
    std::process::Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(&repo_path)
        .output()
        .unwrap();
    std::process::Command::new("git")
        .args(["config", "user.name", "Test User"])
        .current_dir(&repo_path)
        .output()
        .unwrap();

    // Create .intent/config.json
    let intent_dir = repo_path.join(".intent");
    std::fs::create_dir_all(&intent_dir).unwrap();
    std::fs::write(intent_dir.join("config.json"), config).unwrap();

    // Create an initial commit so the repo has a main branch
    std::fs::write(repo_path.join("README.md"), "test").unwrap();
    std::process::Command::new("git")
        .args(["add", "."])
        .current_dir(&repo_path)
        .output()
        .unwrap();
    std::process::Command::new("git")
        .args(["commit", "-m", "Initial commit"])
        .current_dir(&repo_path)
        .output()
        .unwrap();

    TempRepo(repo_path)
}

/// DoD test (a): workspace.create with repo branchPrefix and no request prefix -> branch carries prefix.
#[tokio::test]
async fn test_workspace_create_uses_repo_branch_prefix() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_event_bus(bus.clone())
            .with_workspaces_root(std::env::temp_dir().join(format!("itd-ws-{}", Uuid::new_v4()))),
    );

    let socket_path =
        std::env::temp_dir().join(format!("intentd-repo-cfg-{}.sock", Uuid::new_v4()));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn({
        let services = services.clone();
        let bus = bus.clone();
        let socket = socket_path.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
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
}

/// DoD test (b): workspace.create with repo setupScript and no request script -> workspace record has it;
/// request-supplied script wins.
#[tokio::test]
async fn test_workspace_create_setup_script_fallback() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_event_bus(bus.clone())
            .with_workspaces_root(std::env::temp_dir().join(format!("itd-ws-{}", Uuid::new_v4()))),
    );

    let socket_path =
        std::env::temp_dir().join(format!("intentd-repo-cfg-{}.sock", Uuid::new_v4()));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn({
        let services = services.clone();
        let bus = bus.clone();
        let socket = socket_path.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });

    let stream = connect_retry(&socket_path).await;
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);

    // Test 1: repo setupScript is used when request omits it
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

    assert_eq!(
        resp1["result"]["workspace"]["setupScript"],
        json!({"type": "user", "script": "npm install"})
    );

    // Test 2: request-supplied script wins over repo config
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

    assert_eq!(
        resp2["result"]["workspace"]["setupScript"],
        json!({"type": "user", "script": "yarn install"})
    );

    shutdown_tx.send(()).ok();
}

/// DoD test (c): script.list on empty workspace with repo scripts[] -> seeded.
#[tokio::test]
async fn test_script_list_bootstrap_from_repo() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_event_bus(bus.clone())
            .with_workspaces_root(std::env::temp_dir().join(format!("itd-ws-{}", Uuid::new_v4()))),
    );

    let socket_path =
        std::env::temp_dir().join(format!("intentd-repo-cfg-{}.sock", Uuid::new_v4()));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn({
        let services = services.clone();
        let bus = bus.clone();
        let socket = socket_path.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
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
    assert_eq!(scripts[0]["name"], "dev");
    assert_eq!(scripts[0]["command"], "npm run dev");
    assert_eq!(scripts[1]["name"], "test");
    assert_eq!(scripts[1]["command"], "npm test");

    shutdown_tx.send(()).ok();
}

/// DoD test (d): repo instructions reach the composed system prompt.
/// This test verifies that when a workspace has a repo config with instructions,
/// those instructions are included in the agent's system prompt.
#[tokio::test]
async fn test_repo_instructions_in_system_prompt() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_event_bus(bus.clone())
            .with_workspaces_root(std::env::temp_dir().join(format!("itd-ws-{}", Uuid::new_v4()))),
    );

    let socket_path =
        std::env::temp_dir().join(format!("intentd-repo-cfg-{}.sock", Uuid::new_v4()));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn({
        let services = services.clone();
        let bus = bus.clone();
        let socket = socket_path.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });

    let stream = connect_retry(&socket_path).await;
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);

    // Create a test repo with instructions
    let repo = create_test_repo_with_config(
        r#"{
        "instructions": "Always use TypeScript for new files.\nPrefer functional components."
    }"#,
    );
    let repo_path = repo.0.to_str().unwrap();

    // Create a workspace
    let resp = call(
        &mut wr,
        &mut reader,
        1,
        "workspace.create",
        json!({
            "repositoryPath": repo_path,
            "initialAgent": { "prompt": "Hello" }
        }),
    )
    .await;

    let workspace_id = resp["result"]["workspace"]["id"].as_str().unwrap();
    let agent_id = resp["result"]["initialAgent"]["id"].as_str().unwrap();

    // Read the agent's conversation to check the system prompt
    let resp = call(
        &mut wr,
        &mut reader,
        2,
        "agent.readConversation",
        json!({ "agentId": agent_id, "workspaceId": workspace_id }),
    )
    .await;

    // The first message should be the system prompt
    let messages = resp["result"]["messages"].as_array().unwrap();
    assert!(!messages.is_empty());
    let system_message = &messages[0];
    let content = system_message["content"].as_str().unwrap();

    // Verify the repo instructions are in the system prompt
    assert!(
        content.contains("Always use TypeScript for new files"),
        "System prompt should contain repo instructions"
    );
    assert!(
        content.contains("Prefer functional components"),
        "System prompt should contain repo instructions"
    );

    shutdown_tx.send(()).ok();
}
