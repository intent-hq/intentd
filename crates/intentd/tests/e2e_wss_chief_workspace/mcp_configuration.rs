//! Assistant MCP configuration through the real ACP → MCP bridge, observed
//! through authenticated, pinned-TLS daemon RPCs. All servers are local fixtures.

use super::{
    await_uds, client_config, common, connect_ws, disable_toon_output, gate, poll_conversation,
    seed_target, spawn_serve, temp_data_dir, wss_event, wss_rpc_envelope, Daemon,
    CHIEF_WORKSPACE_ID, TOKEN,
};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;

type Ws = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

const AUTH_SECRET: &str = "Bearer assistant-mcp-fixture-secret";
const ENV_SECRET: &str = "assistant-mcp-fixture-env-secret";
const REPLACEMENT: &str = "assistant-mcp-replacement-secret";
const PLACEHOLDER: &str = "********";

async fn start_bridge(data_dir: &Path, script: &str, code: &str) -> (Daemon, Ws) {
    let behavior = json!({
        "response": "ok",
        "rules": [{
            "ifPromptContains": "MCP_CONFIGURATION",
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "Exercise MCP configuration" },
            },
            "response": "MCP configuration checked",
            "emitToolBlocks": true,
        }],
    })
    .to_string();
    let daemon = Daemon {
        child: spawn_serve(
            data_dir,
            "both",
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("MOCK_AGENT_SCRIPT_PATH", script),
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

async fn run_bridge(ws: &mut Ws, workspace_id: &str, agent_id: &str) -> Value {
    let sent = rpc(
        ws,
        20,
        "agent.sendMessage",
        json!({
            "workspaceId": workspace_id,
            "agentId": agent_id,
            "content": "MCP_CONFIGURATION",
        }),
    )
    .await;
    assert_eq!(sent["success"], true);
    let (message_id, block_id) = poll_conversation(
        ws,
        100,
        agent_id,
        "MCP configuration tool result",
        |messages| {
            messages.as_array()?.iter().find_map(|message| {
                let block = message["contentBlocks"]
                    .as_array()?
                    .iter()
                    .find(|block| block["type"] == "tool_result")?;
                Some((message["id"].clone(), block["id"].clone()))
            })
        },
    )
    .await;
    let result = rpc(
        ws,
        190,
        "agent.getMessageBlock",
        json!({
            "workspaceId": workspace_id, "agentId": agent_id,
            "messageId": message_id, "blockId": block_id,
        }),
    )
    .await;
    let text = result["block"]["output"][0]["text"]
        .as_str()
        .expect("tool result text");
    let result: Value = serde_json::from_str(text).expect("MCP configuration JSON result");
    assert_eq!(result["mcpConfiguration"], true, "{result}");
    result
}

fn assert_redacted(value: &Value) {
    let serialized = value.to_string();
    for secret in [AUTH_SECRET, ENV_SECRET, REPLACEMENT] {
        assert!(!serialized.contains(secret), "credential leaked: {value}");
    }
}

/// Requiring the real header makes a successful post-update handshake proof
/// that an echoed redaction placeholder was replaced with the stored credential.
async fn authenticated_fixture() -> (tokio::process::Child, String) {
    let mut child = tokio::process::Command::new("node")
        .arg(
            intentd_test_support::runtime_paths::manifest_dir(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/mock-mcp-server.mjs")
                .display()
                .to_string(),
        )
        .arg("--http")
        .env("MOCK_MCP_REQUIRED_AUTH", AUTH_SECRET)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("start authenticated MCP fixture");
    let mut lines = BufReader::new(child.stdout.take().expect("fixture stdout")).lines();
    let line = timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("fixture startup timed out")
        .expect("read fixture port")
        .expect("fixture exited before startup");
    let port = line.strip_prefix("PORT=").expect("fixture PORT line");
    (child, format!("http://127.0.0.1:{port}"))
}

#[tokio::test]
async fn assistant_mcp_configuration_roundtrip_over_wss() {
    let Some(script) = gate("WSS Assistant MCP configuration") else {
        return;
    };
    let data_dir = temp_data_dir();
    let (_fixture, url) = authenticated_fixture().await;
    let config = json!({
        "id": "assistant-mcp",
        "name": "Assistant fixture",
        "transport": "http",
        "url": url,
        "headers": { "Authorization": AUTH_SECRET, "X-Replace": "original" },
        "env": { "API_KEY": ENV_SECRET, "DROP_ME": "remove-on-update" },
    });
    let stdio_config = json!({
        "id": "assistant-mcp-stdio", "name": "Assistant stdio fixture",
        "transport": "stdio", "command": "node",
        "args": [intentd_test_support::runtime_paths::manifest_dir(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock-mcp-server.mjs").display().to_string()],
        "env": { "API_KEY": ENV_SECRET },
    });
    let missing_command = data_dir.path().join("missing-mcp-executable");
    let rejecting_args = json!([
        "-e",
        r"require('node:readline').createInterface({input: process.stdin}).on('line', line => {
            const request = JSON.parse(line);
            if (request.id !== undefined) process.stdout.write(JSON.stringify({
                jsonrpc: '2.0', id: request.id,
                error: {code: -32603, message: 'fixture handshake rejected'}
            }) + '\n');
        });"
    ]);
    let code = format!(
        r"
        const before = await ws.app.mcp.listServers();
        const created = await ws.app.mcp.createServer({config});
        const listed = await ws.app.mcp.listServers();
        const config = JSON.parse(JSON.stringify(listed.servers.find(s => s.id === created.server.id)));
        config.name = 'Assistant fixture updated';
        config.headers['X-Replace'] = {replacement};
        delete config.env.DROP_ME;
        const updated = await ws.app.mcp.updateServer(config.id, config);
        const running = await ws.app.mcp.getServerStatus(config.id);
        const disabled = await ws.app.mcp.setServerEnabled(config.id, false);
        const disabledList = await ws.app.mcp.listServers();
        const enabled = await ws.app.mcp.setServerEnabled(config.id, true);
        const stopped = await ws.app.mcp.setServerEnabled(config.id, false);
        const savedDisabled = await ws.app.mcp.createServer({{
            id: 'assistant-mcp-disabled', name: 'Saved disabled',
            transport: 'http', url: {url}, enabled: false,
        }});
        const stdioCreated = await ws.app.mcp.createServer({stdio_config});
        const workingStdio = (await ws.app.mcp.listServers()).servers.find(s => s.id === stdioCreated.server.id);
        const spawnFailed = await ws.app.mcp.updateServer(workingStdio.id, {{
            ...workingStdio, command: {missing_command},
        }});
        const spawnStatus = await ws.app.mcp.getServerStatus(workingStdio.id);
        const handshakeFailed = await ws.app.mcp.updateServer(workingStdio.id, {{
            ...workingStdio, args: {rejecting_args},
        }});
        const handshakeStatus = await ws.app.mcp.getServerStatus(workingStdio.id);
        const stdioRecovered = await ws.app.mcp.updateServer(workingStdio.id, workingStdio);
        const recoveredStatus = await ws.app.mcp.getServerStatus(workingStdio.id);
        const stdioStopped = await ws.app.mcp.setServerEnabled(stdioCreated.server.id, false);
        const stdioConfig = (await ws.app.mcp.listServers()).servers.find(s => s.id === stdioCreated.server.id);
        stdioConfig.name = 'Updated while disabled';
        const stdioUpdated = await ws.app.mcp.updateServer(stdioConfig.id, stdioConfig);
        const finalList = await ws.app.mcp.listServers();
        return {{ mcpConfiguration: true, before, created, listed, updated,
            running, disabled, disabledList, enabled, stopped, savedDisabled,
            stdioCreated, spawnFailed, spawnStatus, handshakeFailed, handshakeStatus,
            stdioRecovered, recoveredStatus, stdioStopped, stdioUpdated, finalList }};
        ",
        replacement = json!(REPLACEMENT),
        url = json!(url),
        missing_command = json!(missing_command),
    );
    let (_daemon, mut ws) = start_bridge(data_dir.path(), &script, &code).await;
    let socket = data_dir.path().join("intentd.sock");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut events = connect_ws(port, cfg).await;
    let subscription = rpc(
        &mut events,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["mcp.servers:status-changed"] }),
    )
    .await;
    assert!(subscription["subscriptionId"].is_string());
    let created_agent = rpc(
        &mut ws,
        2,
        "agent.create",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID, "name": "MCP assistant",
            "model": "default", "provider": "mock",
        }),
    )
    .await;
    let agent_id = created_agent["agent"]["id"].as_str().expect("agent id");
    let result = run_bridge(&mut ws, CHIEF_WORKSPACE_ID, agent_id).await;
    assert_redacted(&result);
    assert_eq!(result["before"], json!({ "servers": [] }));
    assert_eq!(result["created"]["server"]["enabled"], true);
    assert_eq!(result["created"]["status"]["state"], "running");
    assert_eq!(result["created"]["status"]["toolCount"], 2);
    assert_eq!(result["listed"]["servers"].as_array().unwrap().len(), 1);
    for server in [
        &result["created"]["server"],
        &result["listed"]["servers"][0],
        &result["updated"]["server"],
    ] {
        assert_eq!(server["id"], "assistant-mcp");
        assert_eq!(server["headers"]["Authorization"], PLACEHOLDER);
        assert_eq!(server["headers"]["X-Replace"], PLACEHOLDER);
        assert_eq!(server["env"]["API_KEY"], PLACEHOLDER);
    }
    assert_eq!(
        result["updated"]["server"]["name"],
        "Assistant fixture updated"
    );
    assert!(result["updated"]["server"]["env"].get("DROP_ME").is_none());
    for key in [
        "updated",
        "running",
        "enabled",
        "stdioCreated",
        "stdioRecovered",
        "recoveredStatus",
    ] {
        assert_eq!(result[key]["status"]["state"], "running", "{result}");
        assert_eq!(result[key]["status"]["toolCount"], 2, "{result}");
        assert!(result[key]["status"].get("lastError").is_none());
    }
    for (failure, observed, expected_error) in [
        ("spawnFailed", "spawnStatus", "missing-mcp-executable"),
        (
            "handshakeFailed",
            "handshakeStatus",
            "fixture handshake rejected",
        ),
    ] {
        let status = &result[failure]["status"];
        assert_eq!(status["state"], "error", "{result}");
        assert!(
            status["lastError"]
                .as_str()
                .unwrap()
                .contains(expected_error),
            "{status}"
        );
        assert!(status.get("pid").is_none());
        assert!(status.get("toolCount").is_none());
        assert_eq!(result[observed]["status"], *status);
        assert_eq!(result[failure]["server"]["enabled"], true);
        assert_eq!(result[failure]["server"]["env"]["API_KEY"], PLACEHOLDER);
    }
    for key in [
        "disabled",
        "stopped",
        "savedDisabled",
        "stdioStopped",
        "stdioUpdated",
    ] {
        assert_eq!(result[key]["status"]["state"], "stopped", "{result}");
    }
    assert_eq!(result["disabledList"]["servers"][0]["enabled"], false);
    assert_eq!(result["savedDisabled"]["server"]["enabled"], false);
    assert_eq!(result["stdioCreated"]["server"]["enabled"], true);
    assert!(result["stdioCreated"]["status"]["pid"].is_number());
    assert_eq!(result["stdioUpdated"]["server"]["enabled"], false);
    assert_eq!(
        result["stdioUpdated"]["server"]["name"],
        "Updated while disabled"
    );
    assert_eq!(
        result["stdioUpdated"]["server"]["env"]["API_KEY"],
        PLACEHOLDER
    );

    // Independently observe the saved definitions through daemon RPC rather
    // than relying only on the values returned inside the Assistant's script.
    let saved = rpc(&mut ws, 200, "mcp.servers.list", json!({})).await;
    assert_eq!(saved, result["finalList"]);
    assert_eq!(saved["servers"].as_array().unwrap().len(), 3);
    assert_redacted(&saved);
    for id in [
        "assistant-mcp",
        "assistant-mcp-disabled",
        "assistant-mcp-stdio",
    ] {
        let status = rpc(
            &mut ws,
            201,
            "mcp.servers.getStatus",
            json!({ "serverId": id }),
        )
        .await;
        assert_eq!(status["status"]["state"], "stopped");
        assert!(status["status"].get("lastError").is_none());
        assert_redacted(&status);
    }
    let store = intent_core::FileSecretStore::with_path(data_dir.path().join("secrets.json"));
    let raw = store
        .load("mcp.servers")
        .unwrap()
        .expect("saved MCP servers");
    let stored: Value = serde_json::from_str(&raw).unwrap();
    let server = &stored["assistant-mcp"];
    assert_eq!(server["name"], "Assistant fixture updated");
    assert_eq!(server["enabled"], false);
    assert_eq!(server["headers"]["Authorization"], AUTH_SECRET);
    assert_eq!(server["headers"]["X-Replace"], REPLACEMENT);
    assert_eq!(server["env"]["API_KEY"], ENV_SECRET);
    assert!(server["env"].get("DROP_ME").is_none());
    assert_eq!(
        stored["assistant-mcp-stdio"]["name"],
        "Updated while disabled"
    );
    assert_eq!(stored["assistant-mcp-stdio"]["enabled"], false);
    assert_eq!(stored["assistant-mcp-stdio"]["env"]["API_KEY"], ENV_SECRET);
    assert!(
        !raw.contains(PLACEHOLDER),
        "placeholder must not replace a credential"
    );

    let mut saw_running = false;
    loop {
        let frame = wss_event(&mut events, 20).await;
        assert_eq!(frame["jsonrpc"], "2.0");
        let event = &frame["params"]["event"];
        assert_eq!(event["type"], "mcp.servers:status-changed");
        assert_redacted(event);
        if event["data"]["serverId"] != "assistant-mcp" {
            continue;
        }
        match event["data"]["status"]["state"].as_str() {
            Some("running") => saw_running = true,
            Some("stopped") if saw_running => break,
            _ => {}
        }
    }
    let mut failures = Vec::new();
    let mut recovered = false;
    loop {
        let frame = wss_event(&mut events, 20).await;
        assert_eq!(frame["jsonrpc"], "2.0");
        let event = &frame["params"]["event"];
        assert_eq!(event["type"], "mcp.servers:status-changed");
        assert_redacted(event);
        if event["data"]["serverId"] != "assistant-mcp-stdio" {
            continue;
        }
        let status = &event["data"]["status"];
        match status["state"].as_str() {
            Some("error") => failures.push(status.clone()),
            Some("running") if failures.len() == 2 => recovered = true,
            Some("stopped") if recovered => break,
            _ => {}
        }
    }
    assert_eq!(
        failures,
        vec![
            result["spawnFailed"]["status"].clone(),
            result["handshakeFailed"]["status"].clone()
        ]
    );
    rpc(&mut ws, 202, "agent.stop", json!({ "agentId": agent_id })).await;
}

#[tokio::test]
async fn ordinary_workspace_mcp_configuration_raw_host_denied_over_wss() {
    let Some(script) = gate("WSS ordinary workspace MCP configuration denial") else {
        return;
    };
    let data_dir = temp_data_dir();
    let config = json!({
        "id": "protected-server", "name": "Protected configuration",
        "transport": "stdio", "command": "node", "enabled": false,
        "env": { "API_KEY": ENV_SECRET },
    });
    let code = format!(
        r"
        const namespaceMissing = typeof ws.app.mcp === 'undefined';
        const config = {config};
        const calls = [
            ['listServers', {{}}],
            ['createServer', {{ config: {{ ...config, id: 'forbidden-new-server' }} }}],
            ['updateServer', {{ serverId: config.id, config: {{ ...config, name: 'Forbidden edit' }} }}],
            ['setServerEnabled', {{ serverId: config.id, enabled: true }}],
            ['getServerStatus', {{ serverId: config.id }}],
        ];
        const denied = [];
        for (const [method, args] of calls) {{
            try {{
                const result = await host({{ method: 'app.mcp.' + method, args }});
                denied.push({{ method, allowed: true, result }});
            }} catch (error) {{
                denied.push({{ method, allowed: false, error: error.message }});
            }}
        }}
        return {{ mcpConfiguration: true, namespaceMissing, denied }};
        "
    );
    let (_daemon, mut ws) = start_bridge(data_dir.path(), &script, &code).await;
    let seeded = rpc(
        &mut ws,
        1,
        "mcp.servers.create",
        json!({ "config": config }),
    )
    .await;
    assert_redacted(&seeded);
    let before = rpc(&mut ws, 2, "mcp.servers.list", json!({})).await;
    let (workspace_id, agent_id) =
        seed_target(&mut ws, 3, "Ordinary workspace", "MCP caller").await;
    let result = run_bridge(&mut ws, &workspace_id, &agent_id).await;
    assert_redacted(&result);
    assert_eq!(result["namespaceMissing"], true, "{result}");
    let denied = result["denied"].as_array().expect("raw host results");
    assert_eq!(denied.len(), 5);
    for call in denied {
        assert_eq!(call["allowed"], false, "raw host bypass: {call}");
        assert!(
            call["error"]
                .as_str()
                .unwrap()
                .contains("only available in the Assistant workspace"),
            "explicit Assistant access denial: {call}"
        );
    }
    let after = rpc(&mut ws, 200, "mcp.servers.list", json!({})).await;
    assert_eq!(after, before, "denied calls must not change configuration");
    let status = rpc(
        &mut ws,
        201,
        "mcp.servers.getStatus",
        json!({ "serverId": "protected-server" }),
    )
    .await;
    assert_eq!(status["status"]["state"], "stopped");
    rpc(&mut ws, 202, "agent.stop", json!({ "agentId": agent_id })).await;
}
