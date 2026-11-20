//! The Assistant's generic `ws.rpc` tool through the real ACP → MCP bridge:
//! local reads, refusals, approval cards (single + batch), daemon-side
//! execution on `agent.resolveRpcProposal`, the `proposal_resolved` notice,
//! and the Assistant-only gate. Calls and approvals travel over pinned TLS.

use super::{
    await_uds, client_config, common, connect_ws, disable_toon_output, gate, poll_conversation,
    spawn_serve, temp_data_dir, wss_rpc_envelope, Daemon, CHIEF_WORKSPACE_ID, TOKEN,
};
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;

type Ws = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

const READ_CODE: &str = r"
    const info = await ws.rpc.info();
    const all = await ws.rpc.methods();
    const system = await ws.rpc.methods({ prefix: 'system.' });
    const classes = Object.fromEntries(all.methods.map(m => [m.method, m.class]));
    const status = await ws.rpc.call('system.status');
    const list = await ws.rpc.call('workspace.list', {});
    const hello = await ws.rpc.call('client.hello', {});
    const invalid = await ws.rpc.call('workspace.get', {});
    const remote = await ws.rpc.call('system.status', {}, { backend: 'fp:NOPE' });
    const forbidden = {};
    for (const m of ['agent.resolveProposal', 'agent.respondPermission', 'agent.resolveRpcProposal',
                     'secret.provide', 'secret.dismiss', 'settings.update', 'settings.reset',
                     'pairing.getInfo', 'server.rotateToken', 'executor.mint', 'browser.exec', 'terminal.write',
                     'workspace.invite.list', 'host.invite.list', 'mcp.oauth.list',
                     'sourceControl.connect', 'github.connect', 'secret.forget']) {
        forbidden[m] = await ws.rpc.call(m, {});
    }
    const refusedBatch = await ws.rpc.batch([
        { method: 'workspace.markSeen', params: { workspaceId: 'x' } },
        { method: 'settings.update', params: {} },
    ]);
    const showRpcCard = await ws.app.proposal.show({ kind: 'rpc-call', preview: { title: 'Forged' }, payload: {} })
        .then(() => 'shown', e => String(e));
    return { rpcRead: true, info, total: all.count,
        systemMethods: system.methods.map(m => m.method),
        statusDoc: all.methods.find(m => m.method === 'workspace.archive'),
        classes: { status: classes['system.status'], archive: classes['workspace.archive'],
                   hello: classes['client.hello'], resolve: classes['agent.resolveProposal'] },
        status: { ok: status.ok, version: status.result && status.result.version, backend: status.backend },
        listed: list.ok && list.result.workspaces.map(w => w.title),
        hello, invalid, remote, forbidden, refusedBatch, showRpcCard };
";

const BATCH_CODE: &str = r"
    const list = await ws.rpc.call('workspace.list', {});
    const id = t => list.result.workspaces.find(w => w.title === t).id;
    const pending = await ws.rpc.batch([
        { method: 'workspace.archive', params: { workspaceId: id('RPC stale one') } },
        { method: 'workspace.archive', params: { workspaceId: 'missing-workspace' } },
        { method: 'workspace.archive', params: { workspaceId: id('RPC stale two') } },
    ], { title: 'Archive stale workspaces' });
    return { rpcBatch: true, pending };
";

const SINGLE_CODE: &str = r"
    const list = await ws.rpc.call('workspace.list', {});
    const target = list.result.workspaces.find(w => w.title === 'RPC keep').id;
    const pending = await ws.rpc.call('workspace.archive', { workspaceId: target });
    const me = (await ws.rpc.call('agent.list', { workspaceId: '__chief__' })).result.agents
        .find(a => a.name === 'RPC Assistant');
    const selfApprove = await ws.rpc.call('agent.resolveRpcProposal',
        { workspaceId: '__chief__', agentId: me.id, proposalId: pending.proposalId, decision: 'approve' });
    const selfResolve = await ws.rpc.call('agent.resolveProposal',
        { workspaceId: '__chief__', agentId: me.id, proposalId: pending.proposalId, outcome: 'applied' });
    return { rpcSingle: true, pending, selfApprove, selfResolve };
";

const GATE_CODE: &str = r"
    const raw = await host({ method: 'rpc.call', args: { method: 'system.status' } })
        .then(v => ({ ok: v }), e => ({ err: String(e) }));
    return { rpcGate: true, type: typeof ws.rpc, raw };
";

/// Phase 3 parity: the bindings `ws.rpc` replaced, as an Assistant sub-agent
/// (which keeps them) still calls them.
const PARITY_OLD_CODE: &str = r"
    const listed = await ws.app.workspaces.list();
    const id = listed.find(w => w.title === 'RPC parity').id;
    const got = await ws.app.workspaces.get(id);
    const settings = await ws.app.settings.list({ includeValues: false });
    const specialists = await ws.app.specialists.list();
    const one = await ws.app.specialists.get(specialists[0].id);
    await ws.app.mcp.createServer({ id: 'parity-mcp', name: 'Parity', transport: 'http',
        url: 'http://127.0.0.1:9/mcp', enabled: false });
    const mcp = await ws.app.mcp.listServers();
    return { parityOld: true, id, title: got.title, settings: settings.length,
        specialists: specialists.map(s => s.id).sort(), one: one.id,
        mcp: (mcp.servers || []).map(m => m.id).sort(), crossWorkspace: typeof ws.crossWorkspace.listSiblings };
";

/// The same reads through `ws.rpc` as the top-level Assistant, plus the
/// stubs that now stand in for the replaced bindings.
const PARITY_NEW_CODE: &str = r"
    const listed = await ws.rpc.call('workspace.list', {});
    const id = listed.result.workspaces.find(w => w.title === 'RPC parity').id;
    const got = await ws.rpc.call('workspace.get', { workspaceId: id });
    const settings = await ws.rpc.call('settings.list');
    const specialists = await ws.rpc.call('specialist.list');
    const one = await ws.rpc.call('specialist.get', { id: specialists.result.specialists[0].id });
    const mcp = await ws.rpc.call('mcp.servers.list');
    const stubs = {};
    for (const path of ['app.workspaces.get', 'crossWorkspace.readNote', 'app.settings.list',
                        'app.specialists.get', 'app.mcp.listServers', 'crossWorkspace.search']) {
        const parts = path.split('.');
        const fn = parts.slice(0, -1).reduce((o, k) => o[k], ws)[parts[parts.length - 1]];
        stubs[path] = await Promise.resolve().then(() => fn('x')).then(() => 'ran', e => String(e));
    }
    const workspace = got.result.workspace || got.result;
    const specialist = one.result.specialist || one.result;
    return { parityNew: true, id, title: workspace.title, settings: settings.result.settings.length,
        specialists: specialists.result.specialists.map(s => s.id).sort(), one: specialist.id,
        mcp: mcp.result.servers.map(m => m.id).sort(), stubs, kept: typeof ws.app.workspaces.list };
";

async fn start(data_dir: &Path) -> (Daemon, Ws) {
    let script = gate("Assistant ws.rpc").expect("mock ACP agent");
    let rule = |keyword: &str, code: &str| {
        json!({
            "ifPromptContains": keyword,
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "Exercise ws.rpc" },
            },
            "response": "ws.rpc checked",
            "emitToolBlocks": true,
        })
    };
    let behavior = json!({
        "response": "ok",
        "rules": [
            rule("RPC_READ", READ_CODE),
            rule("RPC_BATCH", BATCH_CODE),
            rule("RPC_SINGLE", SINGLE_CODE),
            rule("RPC_GATE", GATE_CODE),
            rule("RPC_PARITY_OLD", PARITY_OLD_CODE),
            rule("RPC_PARITY_NEW", PARITY_NEW_CODE),
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
    (daemon, connect_ws(port, cfg).await)
}

async fn rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    let response = wss_rpc_envelope(ws, id, method, params).await;
    assert_eq!(response["jsonrpc"], "2.0", "{response}");
    assert_eq!(response["id"], id, "{response}");
    assert!(response.get("error").is_none(), "{method}: {response}");
    response["result"].clone()
}

async fn create_agent(
    ws: &mut Ws,
    id: i64,
    workspace_id: &str,
    name: &str,
    background: bool,
) -> String {
    let created = rpc(
        ws,
        id,
        "agent.create",
        json!({
            "workspaceId": workspace_id, "name": name,
            "model": "default", "provider": "mock", "isBackground": background,
        }),
    )
    .await;
    created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string()
}

async fn create_workspace(ws: &mut Ws, id: i64, title: &str) -> String {
    let created = rpc(
        ws,
        id,
        "workspace.create",
        json!({ "title": title, "branch": "main", "skipWorktree": true }),
    )
    .await;
    created["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string()
}

async fn archived(ws: &mut Ws, id: i64, workspace_id: &str) -> bool {
    let got = rpc(
        ws,
        id,
        "workspace.get",
        json!({ "workspaceId": workspace_id }),
    )
    .await;
    got["workspace"]["archived"] == true || got["workspace"]["status"] == "archived"
}

/// Send `prompt`, then return the full JSON tool result whose `key` is true.
async fn run_tool(
    ws: &mut Ws,
    id: i64,
    workspace_id: &str,
    agent_id: &str,
    prompt: &str,
    key: &str,
) -> Value {
    let sent = rpc(
        ws,
        id,
        "agent.sendMessage",
        json!({ "workspaceId": workspace_id, "agentId": agent_id, "content": prompt }),
    )
    .await;
    assert_eq!(sent["success"], true);
    let mut checked = std::collections::HashSet::new();
    let mut next_id = id + 1;
    for _ in 0..120 {
        next_id += 1;
        let conversation = rpc(
            ws,
            next_id,
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
            next_id += 1;
            let full = rpc(
                ws,
                next_id,
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

/// Approve or reject, retrying while the turn that held the card is still
/// recording it as pending (`-32005`, `turn-in-progress`).
async fn resolve(ws: &mut Ws, id: i64, agent_id: &str, proposal_id: &str, decision: &str) -> Value {
    for attempt in 0..80 {
        let response = wss_rpc_envelope(
            ws,
            id + attempt,
            "agent.resolveRpcProposal",
            json!({
                "workspaceId": CHIEF_WORKSPACE_ID, "agentId": agent_id,
                "proposalId": proposal_id, "decision": decision,
            }),
        )
        .await;
        if response["error"]["code"] == -32005 {
            // timing-guard: poll interval until the turn records the card as pending.
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }
        assert!(
            response.get("error").is_none(),
            "resolveRpcProposal: {response}"
        );
        return response["result"].clone();
    }
    panic!("proposal {proposal_id} never became pending");
}

async fn notice(ws: &mut Ws, id: i64, agent_id: &str, proposal_id: &str) -> Value {
    poll_conversation(ws, id, agent_id, "proposal_resolved notice", |messages| {
        messages
            .as_array()?
            .iter()
            .find(|m| {
                m["metadata"]["type"] == "proposal_resolved"
                    && m["metadata"]["proposalId"] == proposal_id
            })
            .cloned()
    })
    .await
}

#[tokio::test]
async fn assistant_rpc_reads_refusals_and_approvals_over_wss() {
    if gate("Assistant ws.rpc").is_none() {
        return;
    }
    let data_dir = temp_data_dir();
    let (_daemon, mut ws) = start(data_dir.path()).await;
    let stale_one = create_workspace(&mut ws, 1, "RPC stale one").await;
    let stale_two = create_workspace(&mut ws, 2, "RPC stale two").await;
    let keep = create_workspace(&mut ws, 3, "RPC keep").await;
    let assistant = create_agent(&mut ws, 4, CHIEF_WORKSPACE_ID, "RPC Assistant", false).await;

    // Reads, discovery and refusals.
    let read = run_tool(
        &mut ws,
        100,
        CHIEF_WORKSPACE_ID,
        &assistant,
        "RPC_READ",
        "rpcRead",
    )
    .await;
    let version = env!("CARGO_PKG_VERSION");
    assert_eq!(read["info"]["daemonVersion"], version, "{read}");
    assert_eq!(read["info"]["backend"]["id"], "local");
    assert_eq!(
        read["info"]["source"],
        format!("https://github.com/intent-hq/intentd/tree/v{version}")
    );
    assert_eq!(
        read["info"]["protocolDocs"],
        "https://github.com/intent-hq/intent/tree/main/docs/protocol"
    );
    assert!(read["total"].as_u64().unwrap_or(0) > 300, "{read}");
    assert!(read["systemMethods"]
        .as_array()
        .is_some_and(|m| m.iter().any(|v| v == "system.status")));
    assert_eq!(read["statusDoc"]["class"], "mutating");
    assert!(read["statusDoc"]["doc"]
        .as_str()
        .is_some_and(|d| d.ends_with("docs/protocol/methods/workspace.md")));
    assert_eq!(
        read["classes"],
        json!({ "status": "read", "archive": "mutating", "hello": "unsupported-on-agent-path", "resolve": "forbidden-to-agent" })
    );
    // `system.status` has no ws.* binding; it is a fast path served over the loopback.
    assert_eq!(
        read["status"],
        json!({ "ok": true, "version": version, "backend": "local" })
    );
    let listed = read["listed"].as_array().expect("workspace titles");
    for title in ["RPC stale one", "RPC stale two", "RPC keep"] {
        assert!(listed.iter().any(|t| t == title), "{listed:?}");
    }
    assert_eq!(read["hello"]["ok"], false);
    assert_eq!(
        read["hello"]["error"]["data"]["code"],
        "unsupported-on-agent-path"
    );
    assert_eq!(read["invalid"]["ok"], false);
    assert_eq!(
        read["invalid"]["error"]["code"], -32602,
        "{}",
        read["invalid"]
    );
    assert_eq!(read["remote"]["error"]["data"]["code"], "unknown-backend");
    for (method, refused) in read["forbidden"].as_object().expect("forbidden map") {
        assert_eq!(refused["ok"], false, "{method}: {refused}");
        assert_eq!(refused["error"]["code"], -32003, "{method}: {refused}");
        assert_eq!(
            refused["error"]["data"]["code"], "forbidden-to-agent",
            "{method}"
        );
    }
    assert_eq!(read["refusedBatch"]["ok"], false);
    assert_eq!(read["refusedBatch"]["index"], 1);
    assert!(read["refusedBatch"].get("proposalId").is_none());
    assert!(read["showRpcCard"]
        .as_str()
        .is_some_and(|e| e.contains("rpc-call")));

    // Batch: one card, nothing runs until approved, stop at first failure.
    let batch = run_tool(
        &mut ws,
        1000,
        CHIEF_WORKSPACE_ID,
        &assistant,
        "RPC_BATCH",
        "rpcBatch",
    )
    .await;
    let pending = &batch["pending"];
    assert_eq!(pending["status"], "pending-approval", "{batch}");
    assert_eq!(pending["calls"], 3);
    let proposal_id = pending["proposalId"]
        .as_str()
        .expect("proposal id")
        .to_string();
    assert!(
        !archived(&mut ws, 2000, &stale_one).await,
        "nothing runs before approval"
    );
    let approved = resolve(&mut ws, 2100, &assistant, &proposal_id, "approve").await;
    assert_eq!(approved["outcome"], "applied", "{approved}");
    let statuses: Vec<&str> = approved["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|i| i["status"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(statuses, ["done", "failed", "skipped"], "{approved}");
    assert!(archived(&mut ws, 2300, &stale_one).await);
    assert!(
        !archived(&mut ws, 2301, &stale_two).await,
        "skipped after the failure"
    );
    let applied = notice(&mut ws, 2400, &assistant, &proposal_id).await;
    assert_eq!(applied["metadata"]["outcome"], "applied");
    let text = applied["contentBlocks"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(
        text.contains("Archive stale workspaces") && text.contains("\"skipped\""),
        "{text}"
    );
    let again = resolve(&mut ws, 2600, &assistant, &proposal_id, "approve").await;
    assert_eq!(again["outcome"], "applied", "idempotent: {again}");

    // Single mutating call: self-approval is refused; Reject runs nothing.
    let single = run_tool(
        &mut ws,
        3000,
        CHIEF_WORKSPACE_ID,
        &assistant,
        "RPC_SINGLE",
        "rpcSingle",
    )
    .await;
    assert_eq!(single["pending"]["status"], "pending-approval", "{single}");
    for key in ["selfApprove", "selfResolve"] {
        assert_eq!(
            single[key]["error"]["data"]["code"], "forbidden-to-agent",
            "{single}"
        );
    }
    let single_id = single["pending"]["proposalId"]
        .as_str()
        .expect("id")
        .to_string();
    // Plain agent.resolveProposal must never mark held calls applied.
    let plain = wss_rpc_envelope(
        &mut ws,
        3900,
        "agent.resolveProposal",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID, "agentId": assistant,
            "proposalId": single_id, "outcome": "applied",
        }),
    )
    .await;
    assert_eq!(plain["error"]["code"], -32602, "{plain}");
    let rejected = resolve(&mut ws, 4000, &assistant, &single_id, "reject").await;
    assert_eq!(rejected["outcome"], "dismissed", "{rejected}");
    assert!(!archived(&mut ws, 4200, &keep).await, "reject runs nothing");
    let dismissed = notice(&mut ws, 4300, &assistant, &single_id).await;
    assert_eq!(dismissed["metadata"]["outcome"], "dismissed");
}

#[tokio::test]
async fn assistant_rpc_is_top_level_assistant_only_over_wss() {
    if gate("Assistant ws.rpc gate").is_none() {
        return;
    }
    let data_dir = temp_data_dir();
    let (_daemon, mut ws) = start(data_dir.path()).await;
    let ordinary_ws = create_workspace(&mut ws, 1, "RPC ordinary").await;
    let ordinary = create_agent(&mut ws, 2, &ordinary_ws, "Ordinary", false).await;
    let sub = create_agent(&mut ws, 3, CHIEF_WORKSPACE_ID, "Assistant helper", true).await;
    let top = create_agent(&mut ws, 4, CHIEF_WORKSPACE_ID, "Assistant", false).await;

    for (base, workspace_id, agent_id) in [
        (100, ordinary_ws.as_str(), &ordinary),
        (1000, CHIEF_WORKSPACE_ID, &sub),
    ] {
        let probe = run_tool(&mut ws, base, workspace_id, agent_id, "RPC_GATE", "rpcGate").await;
        assert_eq!(probe["type"], "undefined", "{probe}");
        let err = probe["raw"]["err"].as_str().unwrap_or_default();
        assert!(
            err.contains("only available to the top-level Assistant"),
            "{probe}"
        );
    }
    let probe = run_tool(
        &mut ws,
        2000,
        CHIEF_WORKSPACE_ID,
        &top,
        "RPC_GATE",
        "rpcGate",
    )
    .await;
    assert_eq!(probe["type"], "object", "{probe}");
    assert_eq!(probe["raw"]["ok"]["ok"], true, "{probe}");
}

#[tokio::test]
async fn assistant_rpc_replaces_the_older_bindings_over_wss() {
    if gate("Assistant ws.rpc parity").is_none() {
        return;
    }
    let data_dir = temp_data_dir();
    let (_daemon, mut ws) = start(data_dir.path()).await;
    create_workspace(&mut ws, 1, "RPC parity").await;
    let helper = create_agent(&mut ws, 2, CHIEF_WORKSPACE_ID, "Assistant helper", true).await;
    let top = create_agent(&mut ws, 3, CHIEF_WORKSPACE_ID, "Assistant", false).await;

    let old = run_tool(
        &mut ws,
        100,
        CHIEF_WORKSPACE_ID,
        &helper,
        "RPC_PARITY_OLD",
        "parityOld",
    )
    .await;
    let new = run_tool(
        &mut ws,
        1000,
        CHIEF_WORKSPACE_ID,
        &top,
        "RPC_PARITY_NEW",
        "parityNew",
    )
    .await;
    for field in ["id", "title", "settings", "specialists", "one", "mcp"] {
        assert_eq!(old[field], new[field], "{field}: {old} vs {new}");
    }
    assert_eq!(old["title"], "RPC parity", "{old}");
    assert_eq!(old["mcp"], json!(["parity-mcp"]), "{old}");
    assert_eq!(
        old["crossWorkspace"], "function",
        "sub-agents keep ws.crossWorkspace: {old}"
    );
    assert_eq!(
        new["kept"], "function",
        "ws.app.workspaces.list stays: {new}"
    );
    for (path, stub) in new["stubs"].as_object().expect("stubs") {
        let stub = stub.as_str().unwrap_or_default();
        assert!(
            stub.contains(&format!("ws.{path} is retired for the Assistant: use ")),
            "{path}: {stub}"
        );
    }
}
