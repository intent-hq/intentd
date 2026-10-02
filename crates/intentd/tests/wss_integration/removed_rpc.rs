//! The approved route retirement must agree on owner WSS and local UDS.

use super::*;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;

const REMOVED_METHODS: [&str; 10] = [
    "git.diff",
    "git.log",
    "pr.status",
    "file-tracking.getLineStats",
    "metrics.getWorkspaceStats",
    "metrics.getAllWorkspaceStats",
    "metrics.clearAgentStats",
    "forward.create",
    "forward.list",
    "forward.close",
];

#[intent_test_macros::daemon_test]
async fn approved_ten_rpc_names_are_absent_over_uds_and_owner_wss() {
    let srv = start(WsOptions::default()).await;
    let socket = srv.dir.path().join("removed-rpc.sock");
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let (api, bus, sock) = (srv.api.clone(), srv.bus.clone(), socket.clone());
    let uds = intent_core::spawn_daemon(async move {
        serve_uds(api, bus, &sock, None, async move {
            let _ = stopped.await;
        })
        .await
        .expect("serve isolated UDS");
    });
    let stream = tokio::time::timeout(Duration::from_secs(10), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            poll.tick().await;
            if let Ok(stream) = UnixStream::connect(&socket).await {
                break stream;
            }
        }
    })
    .await
    .expect("isolated UDS ready");
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let mut failures = Vec::new();
    for (id, method) in REMOVED_METHODS.iter().enumerate() {
        let frame = json!({"jsonrpc":"2.0", "id":id, "method":method, "params":{}}).to_string();
        let wss = wss_call(srv.port, srv.cfg.clone(), &frame).await;
        write
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        read.read_line(&mut line).await.unwrap();
        let local: Value = serde_json::from_str(&line).unwrap();
        for (transport, reply) in [("owner WSS", wss), ("UDS", local)] {
            assert_eq!(reply["jsonrpc"], "2.0");
            assert_eq!(reply["id"], id);
            if reply["error"]["code"] != -32601 || reply.get("result").is_some() {
                failures.push(format!("{transport} {method}: {reply}"));
            }
        }
    }
    drop(write);
    drop(read);
    let _ = stop.send(());
    uds.await.unwrap();
    srv.ws.stop().await;
    assert!(
        failures.is_empty(),
        "retired routes still dispatch:\n{}",
        failures.join("\n")
    );
}
