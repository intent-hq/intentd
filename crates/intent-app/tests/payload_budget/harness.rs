//! Boots the large fixture behind a real UDS listener and speaks JSON-RPC
//! to it, so every method goes through the same `conn` + router path a
//! client uses.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_services::test_support::large_state::{build_large_state, LargeState, LargeStateScale};
use intent_services::{EventBus, Services};
use serde_json::{json, Value};

use crate::rows::Ctx;
use crate::seed::Seeded;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;

/// The fixture plus the listener serving it. Dropping it stops the server.
pub struct Daemon {
    pub state: LargeState,
    pub services: Services,
    pub socket: PathBuf,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    _sock_dir: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Build the fixture at `scale`.
pub async fn fixture(scale: &LargeStateScale) -> LargeState {
    build_large_state(scale).await
}

/// Compose the feature crates onto the fixture's services and serve them
/// on a fresh socket.
pub async fn serve(state: LargeState) -> Daemon {
    let bus = EventBus::new(state.store.clone());
    let services = intent_app::compose(state.services.clone())
        .with_event_bus(bus.clone())
        .with_source_control(Arc::new(crate::proxies::OversizedForge::default()))
        .with_linear_engine(Arc::new(crate::proxies::OversizedLinear))
        .with_sentry_engine(Arc::new(crate::proxies::OversizedSentry));
    // What the daemon does at startup: load the stored scripts.
    intent_core::with_caller(intent_core::Caller::Daemon, services.hydrate_scripts())
        .await
        .expect("hydrate scripts");
    // Short path under /tmp: UDS paths are length-capped (macOS SUN_LEN).
    let sock_dir = tempfile::Builder::new()
        .prefix("itd-pb-")
        .tempdir_in("/tmp")
        .expect("socket dir");
    let socket = sock_dir.path().join("s.sock");
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let api: Arc<dyn intent_core::WorkspaceApi> = Arc::new(services.clone());
    intent_core::spawn_daemon({
        let socket = socket.clone();
        async move {
            let _ = intent_transport::serve_uds(api, bus, &socket, None, async {
                let _ = rx.await;
            })
            .await;
        }
    });
    Daemon {
        state,
        services,
        socket,
        shutdown: Some(tx),
        _sock_dir: sock_dir,
    }
}

/// The ids the sweep rows aim at, resolving the few that need a read.
pub async fn ctx<'a>(client: &mut Client, daemon: &Daemon, seeded: &'a Seeded) -> Ctx<'a> {
    let ids = &daemon.state.ids;
    let page = client
        .call(
            "agent.getConversation",
            &json!({ "agentId": ids.big_agent.0, "limit": 1, "aroundIndex": 60 }),
        )
        .await;
    let message_id = page.result()["messages"][0]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("conversation page: {}", page.frame))
        .to_string();
    // The fixture's assistant messages sit at odd indices; block 3 of each
    // is a long tool_result (`m<index>:3`).
    let tool = client
        .call(
            "agent.getConversation",
            &json!({ "agentId": ids.big_agent.0, "limit": 1, "aroundIndex": 1 }),
        )
        .await;
    let tool_message_id = tool.result()["messages"][0]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("conversation page: {}", tool.frame))
        .to_string();
    Ctx {
        tool_message_id,
        tool_block_id: "m1:3".into(),
        ws: ids.big_workspace.0.clone(),
        agent: ids.big_agent.0.clone(),
        note: ids.big_note.0.clone(),
        versioned_note: ids.versioned_note.0.clone(),
        task_note: ids.task_note.0.clone(),
        thread: ids.comment_thread.clone(),
        conversation: ids.discussion_conversation.clone(),
        script: ids.script.clone(),
        monitor: "large-state-monitor-0000".into(),
        worktree: daemon.state.worktree.display().to_string(),
        specialist: "large-specialist-0".into(),
        message_id,
        owner_principal: ids.owner_principal.0.clone(),
        seeded,
    }
}

/// One JSON-RPC connection.
pub struct Client {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    next_id: i64,
}

/// One reply: the decoded frame and its size on the wire (the line, without
/// the newline).
pub struct Reply {
    pub frame: Value,
    pub bytes: usize,
}

impl Reply {
    pub fn error(&self) -> Option<&Value> {
        self.frame.get("error")
    }
    pub fn result(&self) -> &Value {
        &self.frame["result"]
    }
}

/// Per-frame read timeout; generous because debug builds serialize
/// multi-megabyte offenders slowly. Scaled for slow (coverage) runners.
fn frame_timeout() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(120))
}

impl Client {
    pub async fn connect(daemon: &Daemon) -> Self {
        Self::connect_socket(&daemon.socket).await
    }

    /// Connect to the daemon listening on `socket`, retrying while the
    /// listener binds.
    pub async fn connect_socket(socket: &std::path::Path) -> Self {
        let mut last = None;
        for _ in 0..200 {
            match UnixStream::connect(socket).await {
                Ok(stream) => {
                    let (read, writer) = stream.into_split();
                    return Self {
                        reader: BufReader::new(read),
                        writer,
                        next_id: 1,
                    };
                }
                Err(e) => last = Some(e),
            }
            // timing-guard: retry interval while the listener binds its socket; there is no bind event to await
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("connect {}: {last:?}", socket.display());
    }

    /// Next frame on the connection.
    pub async fn read_frame(&mut self) -> Reply {
        let mut line = String::new();
        let n = tokio::time::timeout(frame_timeout(), self.reader.read_line(&mut line))
            .await
            .expect("timed out waiting for a frame")
            .expect("read frame");
        assert!(n > 0, "connection closed");
        let line = line.trim_end_matches('\n');
        Reply {
            bytes: line.len(),
            frame: serde_json::from_str(line).expect("frame is JSON"),
        }
    }

    /// Send one request and return its response, skipping any notification
    /// that arrives first.
    pub async fn call(&mut self, method: &str, params: &Value) -> Reply {
        let id = self.next_id;
        self.next_id += 1;
        let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let mut raw = frame.to_string();
        raw.push('\n');
        self.writer.write_all(raw.as_bytes()).await.expect("write");
        self.writer.flush().await.expect("flush");
        loop {
            let reply = self.read_frame().await;
            if reply.frame.get("id") == Some(&json!(id)) {
                return reply;
            }
        }
    }
}
