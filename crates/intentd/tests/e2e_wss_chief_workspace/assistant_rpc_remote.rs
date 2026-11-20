//! The Assistant's `ws.rpc` on remote backends (Phase 2): two fake owner
//! desktop apps report backends over pinned TLS (`backends.report`) and
//! serve the `backends.relayCall` reverse RPC; the Assistant lists, reads,
//! classifies (D6), and approves a multi-backend batch through the real
//! ACP → MCP bridge.

use super::{
    await_uds, client_config, common, connect_ws, disable_toon_output, gate, spawn_serve,
    temp_data_dir, wss_rpc_envelope, Daemon, CHIEF_WORKSPACE_ID, TOKEN,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

type Ws = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;
type Seen = Arc<Mutex<Vec<Value>>>;

const REMOTE_CODE: &str = r"
    const b = await ws.rpc.backends();
    const info = await ws.rpc.info({ backend: 'fp:AA:BB' });
    const methods = await ws.rpc.methods({ backend: 'fp:AA:BB', prefix: 'remote.' });
    const list = await ws.rpc.call('workspace.list', {}, { backend: 'fp:AA:BB' });
    const lie = await ws.rpc.call('workspace.archive', { workspaceId: 'w' }, { backend: 'fp:AA:BB' });
    const remoteOnly = await ws.rpc.call('remote.only', { x: 1 }, { backend: 'fp:AA:BB' });
    const forbidden = await ws.rpc.call('settings.update', {}, { backend: 'fp:AA:BB' });
    const unknown = await ws.rpc.call('system.status', {}, { backend: 'fp:ZZ' });
    return { rpcRemote: true, b, info, methods, list, lie, remoteOnly, forbidden, unknown };
";

const OFFLINE_CODE: &str = r"
    const b = await ws.rpc.backends();
    const home = await ws.rpc.call('workspace.list', {}, { backend: 'addr:home.example:8843' });
    const studio = await ws.rpc.call('workspace.list', {}, { backend: 'fp:AA:BB' });
    const local = await ws.rpc.call('system.status');
    const refused = await ws.rpc.batch([
        { method: 'workspace.markSeen', params: { workspaceId: 'x' } },
        { method: 'workspace.archive', params: { workspaceId: 'y' }, backend: 'addr:home.example:8843' },
    ]);
    return { rpcOffline: true, b, home, studio, local: local.ok, refused };
";

const BATCH_CODE: &str = r"
    const list = await ws.rpc.call('workspace.list', {});
    const id = list.result.workspaces.find(w => w.title === 'Remote batch local').id;
    const pending = await ws.rpc.batch([
        { method: 'workspace.markSeen', params: { workspaceId: id } },
        { method: 'workspace.archive', params: { workspaceId: 'remote-ws' }, backend: 'fp:AA:BB' },
        { method: 'workspace.markSeen', params: { workspaceId: id } },
    ], { title: 'Mixed batch' });
    return { rpcBatchRemote: true, pending };
";

const RECONNECT_CODE: &str = r"
    const home = await ws.rpc.call('workspace.list', {}, { backend: 'addr:home.example:8843' });
    return { rpcReconnect: true, home };
";

async fn start(data_dir: &Path) -> (Daemon, u16, std::sync::Arc<rustls::ClientConfig>) {
    let script = gate("Assistant remote ws.rpc").expect("mock ACP agent");
    let rule = |keyword: &str, code: &str| {
        json!({
            "ifPromptContains": keyword,
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "Exercise remote ws.rpc" },
            },
            "response": "remote ws.rpc checked",
            "emitToolBlocks": true,
        })
    };
    let behavior = json!({
        "response": "ok",
        "rules": [
            rule("RPC_REMOTE", REMOTE_CODE),
            rule("RPC_OFFLINE", OFFLINE_CODE),
            rule("RPC_MIXED", BATCH_CODE),
            rule("RPC_RECONNECT", RECONNECT_CODE),
        ],
    })
    .to_string();
    let daemon = Daemon {
        child: spawn_serve(
            data_dir,
            "both",
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("MOCK_AGENT_SCRIPT_PATH", &script),
                ("MOCK_AGENT_BEHAVIOR", &behavior),
            ],
        ),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    disable_toon_output(&socket).await;
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("WSS port")).expect("port fits u16");
    let cfg = client_config(
        status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint"),
    );
    (daemon, port, cfg)
}

async fn rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    let response = wss_rpc_envelope(ws, id, method, params).await;
    assert!(response.get("error").is_none(), "{method}: {response}");
    response["result"].clone()
}

/// A fake owner desktop app: hello with `backendRelay`, report `backends`,
/// then answer `backends.relayCall` until dropped. Every relayed request is
/// recorded in `seen`. `workspace.archive` makes the app vanish mid-call.
async fn fake_app(
    port: u16,
    cfg: std::sync::Arc<rustls::ClientConfig>,
    client_id: &str,
    device: &str,
    backends: Value,
    seen: Seen,
) -> tokio::task::JoinHandle<()> {
    let mut ws = connect_ws(port, cfg).await;
    rpc(
        &mut ws,
        1,
        "client.hello",
        json!({
            "clientId": client_id, "name": "Intent", "prettyHostname": device,
            "capabilities": { "backendRelay": 1 },
        }),
    )
    .await;
    let report = rpc(
        &mut ws,
        2,
        "backends.report",
        json!({ "backends": backends }),
    )
    .await;
    assert!(report["accepted"].as_u64().is_some(), "{report}");
    tokio::spawn(async move {
        while let Some(Ok(message)) = ws.next().await {
            let Message::Text(text) = message else {
                continue;
            };
            let frame: Value = serde_json::from_str(&text).expect("json frame");
            if frame["method"] != "backends.relayCall" {
                continue;
            }
            seen.lock().unwrap().push(frame["params"].clone());
            let method = frame["params"]["method"].as_str().unwrap_or_default();
            let result = match method {
                "workspace.archive" => return,
                "system.status" => {
                    json!({ "result": { "version": "9.9.9", "protocolVersion": "18.0" } })
                }
                "agent.rpcMethods" => json!({ "result": { "methods": [
                    { "method": "workspace.archive", "class": "read" },
                    { "method": "remote.only", "class": "read" },
                ] } }),
                "workspace.list" => {
                    json!({ "result": { "workspaces": [{ "title": "Remote WS" }] } })
                }
                "remote.only" => json!({ "result": { "echo": frame["params"]["params"].clone() } }),
                _ => json!({ "error": { "code": -32601, "message": "Method not found" } }),
            };
            let reply = json!({ "jsonrpc": "2.0", "id": frame["id"], "result": result });
            if ws
                .send(Message::Text(reply.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    })
}

async fn run_tool(ws: &mut Ws, id: i64, agent_id: &str, prompt: &str, key: &str) -> Value {
    let sent = rpc(
        ws,
        id,
        "agent.sendMessage",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID, "agentId": agent_id, "content": prompt }),
    )
    .await;
    assert_eq!(sent["success"], true);
    let mut checked = std::collections::HashSet::new();
    let mut next = id + 1;
    for _ in 0..120 {
        next += 1;
        let conversation = rpc(
            ws,
            next,
            "agent.getConversation",
            json!({ "agentId": agent_id }),
        )
        .await;
        let blocks: Vec<(String, String)> = conversation["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|m| {
                m["contentBlocks"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|b| b["type"] == "tool_result")
                    .filter_map(|b| {
                        Some((m["id"].as_str()?.to_string(), b["id"].as_str()?.to_string()))
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        for (message_id, block_id) in blocks {
            if !checked.insert((message_id.clone(), block_id.clone())) {
                continue;
            }
            next += 1;
            let full = rpc(
                ws,
                next,
                "agent.getMessageBlock",
                json!({ "agentId": agent_id, "messageId": message_id, "blockId": block_id }),
            )
            .await;
            let text = full["block"]["output"][0]["text"]
                .as_str()
                .unwrap_or_default();
            if let Ok(value) = serde_json::from_str::<Value>(text) {
                if value[key] == true {
                    return value;
                }
            }
        }
        // timing-guard: poll interval while the tool result block persists.
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("no {key} tool result");
}

async fn approve(ws: &mut Ws, id: i64, agent_id: &str, proposal_id: &str) -> Value {
    for attempt in 0..80 {
        let response = wss_rpc_envelope(
            ws,
            id + attempt,
            "agent.resolveRpcProposal",
            json!({
                "workspaceId": CHIEF_WORKSPACE_ID, "agentId": agent_id,
                "proposalId": proposal_id, "decision": "approve",
            }),
        )
        .await;
        if response["error"]["code"] == -32005 {
            // timing-guard: poll interval until the turn records the card as pending.
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }
        assert!(response.get("error").is_none(), "{response}");
        return response["result"].clone();
    }
    panic!("proposal never became pending");
}

#[tokio::test]
async fn assistant_rpc_relays_remote_backends_over_wss() {
    if gate("Assistant remote ws.rpc").is_none() {
        return;
    }
    let data_dir = temp_data_dir();
    let (_daemon, port, cfg) = start(data_dir.path()).await;
    let mut ws = connect_ws(port, cfg.clone()).await;
    let assistant = rpc(
        &mut ws,
        1,
        "agent.create",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID, "name": "Remote Assistant", "model": "default", "provider": "mock" }),
    )
    .await["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let local_ws = rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({ "title": "Remote batch local", "branch": "main", "skipWorktree": true }),
    )
    .await["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // A token-bearing report is refused and stores nothing.
    let mut leaky = connect_ws(port, cfg.clone()).await;
    rpc(
        &mut leaky,
        1,
        "client.hello",
        json!({ "clientId": "leaky", "capabilities": { "backendRelay": 1 } }),
    )
    .await;
    let refused = wss_rpc_envelope(
        &mut leaky,
        2,
        "backends.report",
        json!({ "backends": [{ "name": "X", "host": "x", "port": 1, "authToken": "secret-token-value" }] }),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    drop(leaky);

    let seen_a: Seen = Arc::default();
    let seen_b: Seen = Arc::default();
    let app_a = fake_app(
        port,
        cfg.clone(),
        "mac-a",
        "Clement's MacBook Pro",
        json!([
            { "name": "Studio", "host": "studio.local", "port": 8843, "fingerprint": "aa:bb", "connected": true },
            { "name": "Home server", "host": "Home.Example", "port": 8843 },
        ]),
        seen_a.clone(),
    )
    .await;
    let app_b = fake_app(
        port,
        cfg.clone(),
        "mac-b",
        "Mac Studio",
        json!([{ "name": "Studio", "host": "10.0.0.2", "port": 8843, "fingerprint": " AA:BB " }]),
        seen_b.clone(),
    )
    .await;

    // Discovery, remote info/methods, relayed read, D6, refusals.
    let remote = run_tool(&mut ws, 100, &assistant, "RPC_REMOTE", "rpcRemote").await;
    let backends = remote["b"]["backends"].as_array().expect("backends");
    let ids: Vec<&str> = backends.iter().map(|b| b["id"].as_str().unwrap()).collect();
    assert_eq!(
        ids,
        ["local", "addr:home.example:8843", "fp:AA:BB"],
        "{remote}"
    );
    let studio = &backends[2];
    assert_eq!(
        studio["reporters"].as_array().map(Vec::len),
        Some(2),
        "deduped: {studio}"
    );
    assert_eq!(studio["reachable"], true);
    assert_eq!(remote["info"]["daemonVersion"], "9.9.9", "{remote}");
    assert_eq!(
        remote["info"]["source"],
        "https://github.com/intent-hq/intentd/tree/v9.9.9"
    );
    assert_eq!(remote["methods"]["methods"][0]["method"], "remote.only");
    assert_eq!(
        remote["list"]["result"]["workspaces"][0]["title"], "Remote WS",
        "{remote}"
    );
    assert_eq!(
        remote["lie"]["status"], "pending-approval",
        "local class wins: {remote}"
    );
    assert_eq!(
        remote["remoteOnly"]["result"]["echo"],
        json!({ "x": 1 }),
        "{remote}"
    );
    assert_eq!(
        remote["forbidden"]["error"]["data"]["code"],
        "forbidden-to-agent"
    );
    assert_eq!(
        remote["unknown"]["error"]["data"]["code"],
        "unknown-backend"
    );
    let relayed: Vec<Value> = seen_a
        .lock()
        .unwrap()
        .iter()
        .chain(seen_b.lock().unwrap().iter())
        .cloned()
        .collect();
    assert!(!relayed.is_empty());
    assert!(
        relayed.iter().all(|p| p["backendId"] == "fp:AA:BB"),
        "{relayed:?}"
    );
    assert!(
        !seen_b.lock().unwrap().is_empty(),
        "newest reporter relays first"
    );

    // The only reporter of the home server goes away: listed, named, not lost.
    app_a.abort();
    let _ = app_a.await;
    let offline = run_tool(&mut ws, 2000, &assistant, "RPC_OFFLINE", "rpcOffline").await;
    let home = offline["b"]["backends"]
        .as_array()
        .and_then(|b| {
            b.iter()
                .find(|x| x["id"] == "addr:home.example:8843")
                .cloned()
        })
        .expect("home still listed");
    assert_eq!(home["reachable"], false, "{home}");
    assert_eq!(home["reporters"][0]["online"], false);
    assert_eq!(
        offline["home"]["error"]["data"]["code"], "no-relay-online",
        "{offline}"
    );
    assert!(offline["home"]["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("Clement's MacBook Pro"));
    assert_eq!(
        offline["studio"]["ok"], true,
        "relays through the online reporter: {offline}"
    );
    assert_eq!(offline["local"], true, "local calls always work");
    assert_eq!(
        offline["refused"]["error"]["data"]["code"], "no-relay-online",
        "no card: {offline}"
    );

    // Mixed batch: local, remote (the relay vanishes mid-call), local.
    let mixed = run_tool(&mut ws, 4000, &assistant, "RPC_MIXED", "rpcBatchRemote").await;
    let proposal = mixed["pending"]["proposalId"]
        .as_str()
        .expect("proposal")
        .to_string();
    let approved = approve(&mut ws, 5000, &assistant, &proposal).await;
    let statuses: Vec<&str> = approved["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|i| i["status"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(statuses, ["done", "unknown", "skipped"], "{approved}");
    assert_eq!(
        approved["items"][1]["error"]["data"]["code"],
        "relay-disconnected"
    );
    let _ = local_ws;
    let _ = app_b.await;

    // The home server's app comes back with the same clientId and re-reports.
    let app_a = fake_app(
        port,
        cfg.clone(),
        "mac-a",
        "Clement's MacBook Pro",
        json!([{ "name": "Home server", "host": "home.example", "port": 8843 }]),
        seen_a.clone(),
    )
    .await;
    let back = run_tool(&mut ws, 6000, &assistant, "RPC_RECONNECT", "rpcReconnect").await;
    assert_eq!(
        back["home"]["ok"], true,
        "stable id relays after reconnect: {back}"
    );
    app_a.abort();

    // No token reaches the daemon log.
    let log = std::fs::read_to_string(data_dir.path().join("daemon.log")).unwrap_or_default();
    assert!(!log.contains("secret-token-value"));
}
