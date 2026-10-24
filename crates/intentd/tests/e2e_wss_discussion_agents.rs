//! Real WSS + deterministic mock ACP coverage for explicit discussion addressing.
//! Ordinary discussion traffic never wakes agents. An addressed agent explicitly
//! reads bounded history and posts its reply through the caller-bound MCP tools.
//! Private targeting and unaddressed reads fail closed; a delivered retry never
//! starts a second turn. No live model or external identity service is used.

#![cfg(unix)]

use crate::common;

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intentd_test_support::GuardedChild;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

const ADDRESSED: &str = "DISCUSSION_ADDRESSED_E2E";
const ORDINARY: &str = "DISCUSSION_ORDINARY_E2E";
const PROBE: &str = "DISCUSSION_UNADDRESSED_PROBE";
const REPLY: &str = "DISCUSSION_AGENT_RELAY_E2E";
const RELAY_REPLY: &str = "Plain reply to the first agent without addressing it";

/// Live `intentd serve` process; killed (whole process group) on drop, with
/// the daemon log echoed for post-mortems.
struct Daemon {
    child: GuardedChild,
    data_dir: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.path().join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
    }
}

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-discussion-agents-")
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> GuardedChild {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = common::hermetic_serve_command(data_dir);
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    common::hermetic_fixture_identity(&mut cmd, data_dir);
    GuardedChild::spawn(&mut cmd).expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if common::daemon_is_ready(socket).await {
                return;
            }
            // timing-guard: poll interval
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let mut line = serde_json::to_string(
        &json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }),
    )
    .unwrap();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();
    timeout(common::rpc_read_timeout(), reader.read_line(&mut buf))
        .await
        .expect("uds rpc timed out")
        .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
}

/// Pin `workspaceApi.toonOutput` off over UDS so `workspace_api` tool result
/// bodies stay plain JSON for the `serde_json::from_str` assertions below
/// (TOON encoding is on by default).
async fn disable_toon_output(socket: &Path) {
    let resp = uds_rpc(
        socket,
        900,
        "settings.update",
        json!({ "changes": [ { "path": "workspaceApi.toonOutput", "value": false } ] }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "disable toonOutput failed: {resp}"
    );
}

/// Pin the server's SHA-256 fingerprint (colon-UPPER hex over the DER cert).
#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fp = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if fp == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("fingerprint mismatch".into()))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config(fingerprint: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

/// Open an authenticated WSS connection (token in the query string).
async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> common::TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the result whose id matches; any
/// out-of-band notifications (`events.event`) are ignored. Preserve errors
/// so negative cases can assert the actual wire envelope.
async fn wss_response<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(common::rpc_read_timeout(), ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert_eq!(v["jsonrpc"], "2.0", "RPC envelope: {v}");
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Read one `events.event` notification from a subscriber connection, or
/// `None` once `secs` elapse without one.
async fn wss_event_opt<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(secs));
    loop {
        let next = tokio::time::timeout_at(deadline, ws.next()).await.ok()?;
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    return Some(v);
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Extract every JSON-parsable `tool_result` text payload from an
/// `agent.getConversation` result's `messages` array. The mock provider's
/// `emitToolBlocks` persists each MCP tool call as a `tool_use` +
/// `tool_result` block pair whose `output[0].text` carries the JSON the
/// agent-side JS returned.
fn tool_result_jsons(messages: &Value) -> Vec<Value> {
    messages
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .filter(|b| b["type"] == json!("tool_result"))
        .filter_map(|b| b["output"].as_array().and_then(|arr| arr.first()))
        .filter_map(|item| item["text"].as_str())
        .filter_map(|text| serde_json::from_str(text).ok())
        .collect()
}

/// Mock-agent gate (parity with the other WSS E2E suites).
fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping {test}: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping {test}: mock script missing at {script}");
        return None;
    }
    Some(script)
}

async fn seed_workspace(data_dir: &Path) -> (String, String) {
    use intent_core::{
        now_iso, Principal, PrincipalId, Workspace, WorkspaceActivity, WorkspaceAttention,
        WorkspaceId, WorkspaceRole, WorkspaceStatus,
    };
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    let ts = now_iso();
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "WSS-DISCUSSION-AGENTS-E2E".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            attention_reminder: None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            last_content_activity: None,
            tags: vec![],
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
            browser_client_id: None,
            pull_requests_total: None,
            display_status: None,
            waiting: false,
            scripts_running: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
            membership: None,
        })
        .await
        .expect("insert ws");
    let member = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: Some("discussion-member".into()),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    store
        .upsert_principal(&member)
        .await
        .expect("seed private-chat participant");
    store
        .add_workspace_member(&ws, &member.id, WorkspaceRole::Collaborator)
        .await
        .expect("seed membership");
    store.close().await;
    (ws.0, member.id.0)
}

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let response = wss_response(ws, id, method, params).await;
    assert!(response.get("error").is_none(), "{method}: {response}");
    response["result"].clone()
}

async fn completed_turns(sub: &mut common::TlsWs, agents: &[&str]) -> Vec<Value> {
    timeout(common::test_timeout(Duration::from_secs(60)), async {
        let mut events = Vec::new();
        let mut remaining = agents.to_vec();
        loop {
            let frame = wss_event_opt(sub, 30).await.expect("agent turn event");
            let event = frame["params"]["event"].clone();
            if event["type"] == "agent:stream:end" {
                remaining.retain(|agent| event["data"]["agentId"] != *agent);
            }
            events.push(event);
            if remaining.is_empty() {
                return events;
            }
        }
    })
    .await
    .expect("addressed agent must complete its real ACP turn")
}

// Normal queued sends announce their persisted user row, not the stream-start
// event used by harness-initiated wakes. Provider logs independently count turns.
fn is_delivered_user_message(event: &Value) -> bool {
    event["type"] == "agent:message" && event["data"]["role"] == "user"
}

async fn hydrate_tool_results(rpc: &mut common::TlsWs, agent: &str, conversation: &mut Value) {
    for message in conversation["messages"].as_array_mut().unwrap() {
        let message_id = message["id"].clone();
        for block in message["contentBlocks"].as_array_mut().unwrap() {
            if block["type"] == "tool_result" && block["outputTruncated"] == true {
                let full = wss_rpc(
                    rpc,
                    90,
                    "agent.getMessageBlock",
                    json!({"agentId":agent,"messageId":message_id,"blockId":block["id"]}),
                )
                .await;
                *block = full["block"].clone();
                assert_ne!(block["outputTruncated"], true, "full tool result: {block}");
            }
        }
    }
}

/// Bound the negative observation window; do not use a blind sleep as evidence.
async fn assert_no_turn(sub: &mut common::TlsWs) -> Vec<Value> {
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(1));
    let mut seen = Vec::new();
    while let Ok(Some(frame)) = tokio::time::timeout_at(deadline, wss_event_opt(sub, 1)).await {
        let event = frame["params"]["event"].clone();
        assert!(
            !is_delivered_user_message(&event)
                && event["type"] != "agent:queue:processing"
                && event["type"] != "agent:stream:start",
            "unexpected wake: {event}"
        );
        seen.push(event);
    }
    seen
}

fn prompt_log(path: &Path) -> Vec<Value> {
    match std::fs::read_to_string(path) {
        Ok(text) => text
            .lines()
            .map(|line| serde_json::from_str(line).expect("prompt JSON"))
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("read mock prompt log: {error}"),
    }
}

fn assert_discussion_event(events: &[Value], message: &Value) {
    let event = events
        .iter()
        .find(|event| {
            event["type"] == "discussion:message" && event["data"]["message"]["id"] == message["id"]
        })
        .unwrap_or_else(|| panic!("live discussion event for {message}: {events:?}"));
    assert_eq!(event["data"]["conversationId"], "default");
    assert_eq!(event["data"]["message"]["body"], message["body"]);
    assert_eq!(event["data"]["message"]["author"], message["author"]);
}

#[intent_test_macros::daemon_test]
async fn explicit_discussion_address_drives_history_and_reply_without_automatic_subscription() {
    let Some(script) = gate("explicit discussion agents WSS") else {
        return;
    };
    let data_dir = temp_data_dir();
    let (workspace, member) = seed_workspace(data_dir.path()).await;
    let log = data_dir.path().join("mock-prompts.jsonl");
    let history_and_reply = format!(
        r"
        const page = await ws.discussion.list('default', {{ limit: 1 }});
        const addressed = page.messages[0];
        const older = await ws.discussion.list('default', {{ beforeSequence: addressed.sequence, limit: 1 }});
        const reaction = await ws.discussion.react('default', addressed.id, '👀', true);
        const reactionRetry = await ws.discussion.react('default', addressed.id, '👀', true);
        const reactedPage = await ws.discussion.list('default', {{limit:1}});
        const relay = (await ws.agent.list(true)).find(agent => agent.name === 'Relay agent');
        if (!relay) throw new Error('Relay agent missing');
        const reply = await ws.discussion.send('default', {reply}, {{
            messageId: 'agent-reply', replyToMessageId: addressed.id, recipientAgentIds: [relay.id],
            author: {{ principalId: 'forged-human', agentId: 'forged-agent', displayName: 'Forged' }}
        }});
        return {{ page, older, reply, reaction, reactionRetry, reactedPage }};
    ",
        reply = json!(REPLY)
    );
    let relay_history_and_reply = format!(
        r"
        const page = await ws.discussion.list('default', {{ limit: 1 }});
        const addressed = page.messages[0];
        const reply = await ws.discussion.send('default', {reply}, {{
            messageId: 'relay-reply', replyToMessageId: addressed.id
        }});
        return {{ page, reply }};
    ",
        reply = json!(RELAY_REPLY)
    );
    let denied_probe = r"
        const out = {};
        try { out.history = await ws.discussion.list('default', {limit: 1}); }
        catch (e) { out.historyError = e.message; }
        try { out.sent = await ws.discussion.send('default', 'unauthorized reply', {messageId: 'unaddressed-send'}); }
        catch (e) { out.sendError = e.message; }
        return out;
    ";
    let behavior = json!({
        "response": "Unexpected automatic discussion wake",
        "rules": [
            {"ifPromptContains": PROBE, "toolCall": {"name":"workspace_api",
                "arguments":{"code":denied_probe,"summary":"Check unaddressed discussion access"}},
                "response":"Unaddressed access checked", "emitToolBlocks":true},
            {"ifPromptContains": REPLY, "toolCall":{"name":"workspace_api",
                "arguments":{"code":relay_history_and_reply,"summary":"Reply to an agent without addressing it"}},
                "response":"Relay final response stays outside discussion", "emitToolBlocks":true},
            {"ifPromptContains": ADDRESSED, "toolCall":{"name":"workspace_api",
                "arguments":{"code":history_and_reply,"summary":"Read addressed chat and explicitly reply"}},
                "response":"This final response must not be posted to the discussion", "emitToolBlocks":true}
        ]
    }).to_string();
    let log_string = log.to_str().expect("UTF-8 fixture path");
    let child = spawn_serve(
        data_dir.path(),
        &[
            ("INTENTD_AUTH_TOKEN", TOKEN),
            ("MOCK_AGENT_SCRIPT_PATH", &script),
            ("MOCK_AGENT_BEHAVIOR", &behavior),
            ("MOCK_AGENT_PROMPT_LOG", log_string),
        ],
    );
    let daemon = Daemon { child, data_dir };
    let socket = daemon.data_dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await, "fixture daemon must start");
    disable_toon_output(&socket).await;
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().expect("port")).unwrap();
    let cfg = client_config(
        status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint"),
    );
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut sub = connect_ws(port, cfg).await;
    let capabilities = wss_rpc(&mut rpc, 1, "system.capabilities", json!({})).await;
    assert_eq!(capabilities["discussionAgentMessaging"], 1);
    assert_eq!(capabilities["discussionReactions"], 1);
    let subscription = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "workspaceId":workspace, "eventTypes":["discussion:message", "discussion:reaction", "agent:*"]
        }),
    )
    .await;
    assert!(subscription["subscriptionId"].is_string());
    let target = wss_rpc(
        &mut rpc,
        2,
        "agent.create",
        json!({
            "workspaceId":workspace,"name":"Addressed agent","provider":"mock","model":"default"
        }),
    )
    .await["agent"]["id"]
        .as_str()
        .expect("target agent id")
        .to_owned();
    let observer = wss_rpc(
        &mut rpc,
        3,
        "agent.create",
        json!({
            "workspaceId":workspace,"name":"Unaddressed agent","provider":"mock","model":"default"
        }),
    )
    .await["agent"]["id"]
        .as_str()
        .expect("observer agent id")
        .to_owned();

    let relay = wss_rpc(
        &mut rpc,
        30,
        "agent.create",
        json!({
            "workspaceId":workspace,"name":"Relay agent","provider":"mock","model":"default"
        }),
    )
    .await["agent"]["id"]
        .as_str()
        .expect("relay agent id")
        .to_owned();

    // None of the agents has been addressed: human traffic alone does not run ACP.
    let ordinary = wss_rpc(
        &mut rpc,
        4,
        "discussion.send",
        json!({
            "workspaceId":workspace,"messageId":"ordinary","body":ORDINARY
        }),
    )
    .await;
    let quiet = assert_no_turn(&mut sub).await;
    assert_discussion_event(&quiet, &ordinary["message"]);
    assert!(
        prompt_log(&log).is_empty(),
        "ordinary traffic reached the provider"
    );

    // A real agent tool call cannot read or reply merely by knowing the chat ID.
    wss_rpc(
        &mut rpc,
        5,
        "agent.sendMessage",
        json!({
            "workspaceId":workspace,"agentId":observer,"content":PROBE
        }),
    )
    .await;
    completed_turns(&mut sub, &[&observer]).await;
    let observer_history = wss_rpc(
        &mut rpc,
        6,
        "agent.getConversation",
        json!({
            "workspaceId":workspace,"agentId":observer
        }),
    )
    .await;
    let probe = tool_result_jsons(&observer_history["messages"])
        .into_iter()
        .find(|value| value.get("historyError").is_some())
        .unwrap_or_else(|| panic!("unaddressed tool denial missing: {observer_history}"));
    assert!(
        probe["historyError"]
            .as_str()
            .is_some_and(|s| s.contains("not found")),
        "{probe}"
    );
    assert!(
        probe["sendError"]
            .as_str()
            .is_some_and(|s| s.contains("not found")),
        "{probe}"
    );
    assert!(
        probe.get("history").is_none() && probe.get("sent").is_none(),
        "{probe}"
    );
    assert_eq!(prompt_log(&log).len(), 1);

    // Private targeting must fail even when the human is a participant.
    let private = wss_rpc(
        &mut rpc,
        7,
        "discussion.send",
        json!({
            "workspaceId":workspace,"messageId":"private-create","body":"Private seed",
            "recipientPrincipalIds":[member]
        }),
    )
    .await;
    let private_id = private["conversation"]["id"]
        .as_str()
        .expect("private conversation id");
    let refused = wss_response(
        &mut rpc,
        8,
        "discussion.send",
        json!({
            "workspaceId":workspace,"conversationId":private_id,"messageId":"private-address",
            "body":"Private address must not wake","recipientAgentIds":[target]
        }),
    )
    .await;
    assert!(refused.get("result").is_none(), "{refused}");
    assert_eq!(refused["error"]["code"], -32003, "{refused}");
    assert_eq!(refused["error"]["data"]["code"], "forbidden", "{refused}");
    assert_no_turn(&mut sub).await;
    assert_eq!(
        prompt_log(&log).len(),
        1,
        "private rejection started a turn"
    );
    let private_history = wss_rpc(
        &mut rpc,
        9,
        "discussion.list",
        json!({
            "workspaceId":workspace,"conversationId":private_id
        }),
    )
    .await;
    assert_eq!(private_history["messages"].as_array().unwrap().len(), 1);

    // The first explicit address grants context and drives the real mock turn.
    let request = json!({"workspaceId":workspace,"conversationId":"default",
        "messageId":"addressed","body":ADDRESSED,"recipientAgentIds":[target]});
    let sent = wss_rpc(&mut rpc, 10, "discussion.send", request.clone()).await;
    assert_eq!(sent["message"]["recipientAgentIds"], json!([target]));
    assert!(sent["message"]["author"]["principalId"].is_string());
    assert!(sent["message"]["author"].get("agentId").is_none());
    let events = completed_turns(&mut sub, &[&target, &relay]).await;
    assert_discussion_event(&events, &sent["message"]);
    assert!(
        !events
            .iter()
            .any(|event| is_delivered_user_message(event) && event["data"]["agentId"] == observer),
        "unaddressed observer woke: {events:?}"
    );
    let mut target_history = wss_rpc(
        &mut rpc,
        11,
        "agent.getConversation",
        json!({
            "workspaceId":workspace,"agentId":target
        }),
    )
    .await;
    hydrate_tool_results(&mut rpc, &target, &mut target_history).await;
    let output = tool_result_jsons(&target_history["messages"])
        .into_iter()
        .find(|value| value.get("reply").is_some())
        .unwrap_or_else(|| panic!("addressed history/reply tool result missing: {target_history}"));
    assert_eq!(output["page"]["messages"].as_array().unwrap().len(), 1);
    assert_eq!(output["page"]["hasMore"], true);
    assert_eq!(output["page"]["messages"][0]["id"], sent["message"]["id"]);
    assert_eq!(output["older"]["messages"].as_array().unwrap().len(), 1);
    assert_eq!(output["older"]["messages"][0]["body"], ORDINARY);
    assert_eq!(output["older"]["hasMore"], false);
    assert_eq!(output["reaction"], output["reactionRetry"]);
    assert_eq!(output["reaction"]["reactionVersion"], 1);
    assert_eq!(
        output["reaction"]["reactions"],
        json!([{"reaction":"👀","count":1,"reacted":true,"reactors":[{"principalId":null,"agentId":target,"login":null,"displayName":"Addressed agent","avatarUrl":null}]}])
    );
    assert_eq!(
        output["reactedPage"]["messages"][0]["reactions"],
        output["reaction"]["reactions"]
    );
    let reaction_events: Vec<_> = events
        .iter()
        .filter(|e| e["type"] == "discussion:reaction" || e["eventType"] == "discussion:reaction")
        .collect();
    assert_eq!(reaction_events.len(), 1, "no-op must not emit: {events:?}");
    assert_eq!(reaction_events[0]["data"]["reactions"][0]["reacted"], false);
    assert_eq!(
        reaction_events[0]["data"]["reactions"][0]["reactors"][0]["agentId"],
        target
    );
    let reply = &output["reply"]["message"];
    assert_eq!(reply["body"], REPLY);
    assert_eq!(reply["recipientAgentIds"], json!([relay]));
    assert_eq!(reply["author"]["agentId"], target);
    assert!(reply["author"]["principalId"].is_null());
    assert_eq!(reply["author"]["displayName"], "Addressed agent");
    assert_eq!(reply["replyToMessageId"], sent["message"]["id"]);
    assert_discussion_event(&events, reply);
    let rows = target_history["messages"].as_array().unwrap();
    let user_rows: Vec<_> = rows.iter().filter(|row| row["role"] == "user").collect();
    assert_eq!(
        user_rows.len(),
        1,
        "one addressed delivery: {target_history}"
    );
    assert_eq!(user_rows[0]["metadata"]["type"], "discussion_message");
    assert_eq!(
        user_rows[0]["metadata"]["discussionMessageId"],
        sent["message"]["id"]
    );
    let mut relay_history = wss_rpc(
        &mut rpc,
        31,
        "agent.getConversation",
        json!({
            "workspaceId":workspace,"agentId":relay
        }),
    )
    .await;
    hydrate_tool_results(&mut rpc, &relay, &mut relay_history).await;
    let relay_output = tool_result_jsons(&relay_history["messages"])
        .into_iter()
        .find(|value| value.get("reply").is_some())
        .unwrap_or_else(|| panic!("agent-addressed relay did not read and reply: {relay_history}"));
    assert_eq!(
        relay_output["page"]["messages"].as_array().unwrap().len(),
        1
    );
    assert_eq!(relay_output["page"]["messages"][0]["id"], reply["id"]);
    let plain_reply = &relay_output["reply"]["message"];
    assert_eq!(plain_reply["body"], RELAY_REPLY);
    assert_eq!(plain_reply["author"]["agentId"], relay);
    assert!(plain_reply["author"]["principalId"].is_null());
    assert_eq!(plain_reply["replyToMessageId"], reply["id"]);
    assert_eq!(plain_reply["recipientAgentIds"], json!([]));
    assert_discussion_event(&events, plain_reply);
    let relay_rows: Vec<_> = relay_history["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["role"] == "user")
        .collect();
    assert_eq!(
        relay_rows.len(),
        1,
        "one agent-to-agent delivery: {relay_history}"
    );
    assert_eq!(
        relay_rows[0]["metadata"]["discussionMessageId"],
        reply["id"]
    );
    assert_eq!(relay_rows[0]["metadata"]["fromAgentId"], target);
    for agent in [&target, &relay] {
        assert_eq!(events.iter().filter(|event| is_delivered_user_message(event)
            && event["data"]["agentId"] == *agent).count(), 1,
            "each explicitly addressed agent runs once; plain replyTo never wakes its author: {events:?}");
    }
    let prompts = prompt_log(&log);
    assert_eq!(
        prompts.len(),
        3,
        "one probe and two explicitly addressed turns: {prompts:?}"
    );
    let addressed_prompt = prompts
        .iter()
        .find(|p| p["text"].as_str().is_some_and(|s| s.contains(ADDRESSED)))
        .unwrap();
    assert!(
        !addressed_prompt["text"]
            .as_str()
            .unwrap()
            .contains(ORDINARY),
        "ordinary history was injected before the explicit history tool read"
    );

    // Retry only after completion; pending-queue dedup alone cannot satisfy this.
    let retry = wss_rpc(&mut rpc, 12, "discussion.send", request).await;
    let mut original = sent["message"].clone();
    original["agentActivity"] = retry["message"]["agentActivity"].clone();
    original["reactions"] = json!([{"reaction":"👀","count":1,"reacted":false,"reactors":[{"principalId":null,"agentId":target,"login":null,"displayName":"Addressed agent","avatarUrl":null}]}]);
    original["reactionVersion"] = json!(1);
    assert_eq!(
        retry["message"], original,
        "retry preserves every immutable message field"
    );
    assert_eq!(retry["message"]["agentActivity"][0]["attempt"], 1);
    assert_eq!(retry["message"]["agentActivity"][0]["state"], "completed");
    assert!(
        retry["message"]["agentActivity"][0]["version"]
            .as_i64()
            .unwrap()
            > sent["message"]["agentActivity"][0]["version"]
                .as_i64()
                .unwrap()
    );
    let late = assert_no_turn(&mut sub).await;
    assert!(
        !late
            .iter()
            .any(|event| event["type"] == "discussion:message"
                && event["data"]["message"]["id"] == sent["message"]["id"]),
        "retry rebroadcast: {late:?}"
    );
    assert_eq!(
        prompt_log(&log).len(),
        3,
        "delivered retry or plain replyTo woke an agent"
    );
    for agent in [&target, &relay, &observer] {
        let queue = wss_rpc(
            &mut rpc,
            13,
            "agent.getQueue",
            json!({
                "workspaceId":workspace,"agentId":agent
            }),
        )
        .await;
        assert!(
            queue["queue"].as_array().is_some_and(Vec::is_empty),
            "{queue}"
        );
    }
    let history = wss_rpc(
        &mut rpc,
        14,
        "discussion.list",
        json!({"workspaceId":workspace}),
    )
    .await;
    assert_eq!(
        history["messages"].as_array().unwrap().len(),
        4,
        "only two human messages and two explicit agent posts belong in discussion: {history}"
    );
    assert_eq!(history["messages"][2]["id"], reply["id"]);
    assert_eq!(history["messages"][3]["id"], plain_reply["id"]);
    rpc.close(None).await.expect("close RPC socket");
    sub.close(None).await.expect("close event socket");
}

/// Persist the exact two crash boundaries while the fixture daemon is stopped.
/// The outbox case uses the production store transaction; the admitted case
/// uses the real service handoff without a runtime manager, so no provider runs.
async fn seed_recovery_delivery(
    data_dir: &Path,
    workspace: &str,
    agent: &str,
    admitted: bool,
) -> String {
    use intent_core::discussion::{DiscussionAuthor, DiscussionTarget};
    use intent_core::{with_caller, AgentId, Caller, HostRole, WorkspaceApi, WorkspaceId};
    use intent_services::Services;
    use intent_store::Store;

    let store = Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open stopped fixture database");
    let principal = store.get_primary_principal().await.expect("fixture owner");
    let workspace = WorkspaceId(workspace.into());
    let target = DiscussionTarget {
        conversation_id: Some("default".into()),
        recipient_agent_ids: vec![AgentId(agent.into())],
        ..Default::default()
    };
    let sent = if admitted {
        let services =
            Services::new(store.clone()).with_workspaces_root(data_dir.join("workspaces"));
        with_caller(
            Caller::Wire {
                principal_id: principal.id.clone(),
                host_role: HostRole::Owner,
            },
            services.discussion_send(
                workspace.clone(),
                "restart-address".into(),
                "Recover this addressed message".into(),
                target,
            ),
        )
        .await
        .expect("seed already-admitted durable queue")
    } else {
        let author = DiscussionAuthor {
            principal_id: Some(principal.id.clone()),
            agent_id: None,
            login: principal.login.clone(),
            display_name: principal.display_name.clone(),
            avatar_url: principal.avatar_url.clone(),
        };
        let (sent, inserted) = store
            .send_discussion_message(
                &workspace,
                "restart-address",
                "Recover this addressed message",
                &author,
                &target,
            )
            .await
            .expect("seed committed message and pending outbox");
        assert!(inserted);
        sent
    };
    let pending = store
        .discussion_pending_deliveries(Some(&sent.message.id))
        .await
        .unwrap();
    assert_eq!(pending.len(), usize::from(!admitted));
    let queue = store
        .load_all_agent_queues()
        .await
        .expect("read durable fixture queue");
    assert_eq!(queue.len(), usize::from(admitted));
    if admitted {
        assert_eq!(
            queue[0].payload["messageMetadata"]["discussionMessageId"],
            sent.message.id
        );
        assert_eq!(
            queue[0].payload["messageMetadata"]["fromPrincipalId"],
            principal.id.0
        );
        assert_eq!(queue[0].payload["userOrigin"], true);
    }
    store.close().await;
    sent.message.id
}

async fn restart_and_retry_discussion_delivery(admitted: bool) {
    let Some(script) = gate("discussion agent restart retry WSS") else {
        return;
    };
    let data_dir = temp_data_dir();
    common::disable_resume_on_start(data_dir.path());
    let (workspace, _) = seed_workspace(data_dir.path()).await;
    let log = data_dir.path().join("restart-prompts.jsonl");
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        (
            "MOCK_AGENT_PROMPT_LOG",
            log.to_str().expect("fixture log path"),
        ),
        (
            "MOCK_AGENT_BEHAVIOR",
            r#"{"response":"Recovered addressed delivery"}"#,
        ),
    ];
    let child = spawn_serve(data_dir.path(), &env);
    let mut daemon = Daemon { child, data_dir };
    let socket = daemon.data_dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let mut rpc = connect_ws(
        port,
        client_config(status["result"]["fingerprint"].as_str().unwrap()),
    )
    .await;
    let agent = wss_rpc(
        &mut rpc,
        1,
        "agent.create",
        json!({
            "workspaceId":workspace,"name":"Restart recipient","provider":"mock","model":"default"
        }),
    )
    .await["agent"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        prompt_log(&log).is_empty(),
        "fixture agent must be idle before crash boundary"
    );
    rpc.close(None).await.expect("close pre-restart socket");
    daemon
        .child
        .kill()
        .expect("stop fixture before seeding crash boundary");
    daemon.child.wait().expect("wait for stopped fixture");

    let message =
        seed_recovery_delivery(daemon.data_dir.path(), &workspace, &agent, admitted).await;
    daemon.child = spawn_serve(daemon.data_dir.path(), &env);
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut sub = connect_ws(port, cfg).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "workspaceId":workspace,"eventTypes":["agent:*", "discussion:message"]
        }),
    )
    .await;
    // Startup is restore-only. The exact original human retry must kick this
    // existing queue entry, without relying on interrupted-agent auto-resume.
    let queue = wss_rpc(
        &mut rpc,
        2,
        "agent.getQueue",
        json!({
            "workspaceId":workspace,"agentId":agent
        }),
    )
    .await;
    assert_eq!(
        queue["queue"].as_array().unwrap().len(),
        1,
        "restored queue: {queue}"
    );
    assert_eq!(
        queue["queue"][0]["messageMetadata"]["discussionMessageId"],
        message
    );
    assert_no_turn(&mut sub).await;
    assert!(
        prompt_log(&log).is_empty(),
        "no startup turn before explicit retry"
    );
    let request = json!({"workspaceId":workspace,"conversationId":"default",
        "messageId":"restart-address","body":"Recover this addressed message","recipientAgentIds":[agent]});
    let retry = wss_rpc(&mut rpc, 3, "discussion.send", request.clone()).await;
    assert_eq!(retry["message"]["id"], message);
    let events = completed_turns(&mut sub, &[&agent]).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| is_delivered_user_message(event) && event["data"]["agentId"] == agent)
            .count(),
        1,
        "one recovered turn: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event["type"] == "discussion:message"),
        "retry must not rebroadcast the old discussion message: {events:?}"
    );
    assert_eq!(prompt_log(&log).len(), 1, "one provider turn after retry");
    let transcript = wss_rpc(
        &mut rpc,
        4,
        "agent.getConversation",
        json!({
            "workspaceId":workspace,"agentId":agent
        }),
    )
    .await;
    let rows: Vec<_> = transcript["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["role"] == "user")
        .collect();
    assert_eq!(
        rows.len(),
        1,
        "one durable delivery after restart: {transcript}"
    );
    assert_eq!(rows[0]["metadata"]["discussionMessageId"], message);
    wss_rpc(&mut rpc, 5, "discussion.send", request).await;
    assert_no_turn(&mut sub).await;
    assert_eq!(
        prompt_log(&log).len(),
        1,
        "completed recovery retry duplicated provider work"
    );
    let queue = wss_rpc(
        &mut rpc,
        6,
        "agent.getQueue",
        json!({
            "workspaceId":workspace,"agentId":agent
        }),
    )
    .await;
    assert!(
        queue["queue"].as_array().is_some_and(Vec::is_empty),
        "{queue}"
    );
    let history = wss_rpc(
        &mut rpc,
        7,
        "discussion.list",
        json!({"workspaceId":workspace}),
    )
    .await;
    assert_eq!(history["messages"].as_array().unwrap().len(), 1);
    assert_eq!(history["messages"][0]["id"], message);
    rpc.close(None).await.expect("close restarted RPC socket");
    sub.close(None).await.expect("close restarted subscriber");
}

#[intent_test_macros::daemon_test]
async fn pending_discussion_outbox_restarts_and_sender_retry_runs_once() {
    restart_and_retry_discussion_delivery(false).await;
}

#[intent_test_macros::daemon_test]
async fn admitted_discussion_queue_restarts_and_sender_retry_runs_once() {
    restart_and_retry_discussion_delivery(true).await;
}

async fn wait_activity(
    rpc: &mut common::TlsWs,
    workspace: &str,
    message: &str,
    state: &str,
) -> Value {
    timeout(common::test_timeout(Duration::from_secs(40)), async {
        loop {
            let page = wss_rpc(
                rpc,
                900,
                "discussion.list",
                json!({"workspaceId":workspace}),
            )
            .await;
            let row = page["messages"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| m["id"] == message)
                .unwrap();
            if row["agentActivity"][0]["state"] == state {
                return row["agentActivity"][0].clone();
            }
            // timing-guard: poll durable activity until the mock reaches the requested state.
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("discussion activity transition")
}

#[tokio::test]
async fn discussion_stop_retains_partial_output_and_never_stops_another_invocation() {
    let Some(script) = gate("discussion exact Stop WSS") else {
        return;
    };
    let data_dir = temp_data_dir();
    let (workspace, _) = seed_workspace(data_dir.path()).await;
    let log = data_dir.path().join("control-prompts.jsonl");
    let behavior=json!({"blockUntilCancel":true,"parkIfPromptEndsWith":"UNRELATED_PARK","response":"retry completed"}).to_string();
    let child = spawn_serve(
        data_dir.path(),
        &[
            ("INTENTD_AUTH_TOKEN", TOKEN),
            ("MOCK_AGENT_SCRIPT_PATH", &script),
            ("MOCK_AGENT_BEHAVIOR", &behavior),
            ("MOCK_AGENT_PROMPT_LOG", log.to_str().unwrap()),
        ],
    );
    let daemon = Daemon { child, data_dir };
    let socket = daemon.data_dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut sub = connect_ws(port, cfg.clone()).await;
    assert_eq!(
        wss_rpc(&mut rpc, 1, "system.capabilities", json!({})).await["discussionAgentActivity"],
        1
    );
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({"workspaceId":workspace,"eventTypes":["discussion:activity","agent:*"]}),
    )
    .await;
    let agent=wss_rpc(&mut rpc,2,"agent.create",json!({"workspaceId":workspace,"name":"Controlled recipient","provider":"mock","model":"default"})).await["agent"]["id"].as_str().unwrap().to_owned();
    let sent=wss_rpc(&mut rpc,3,"discussion.send",json!({"workspaceId":workspace,"messageId":"control-original","body":"Please do this work","recipientAgentIds":[agent]})).await;
    let message = sent["message"]["id"].as_str().unwrap().to_owned();
    wait_activity(&mut rpc, &workspace, &message, "working").await;
    timeout(common::test_timeout(Duration::from_secs(30)), async {
        loop {
            let frame = wss_event_opt(&mut sub, 10)
                .await
                .expect("streamed mock output");
            if frame.to_string().contains("streaming-before-cancel") {
                break;
            }
        }
    })
    .await
    .unwrap();
    wss_rpc(&mut rpc,4,"agent.sendMessage",json!({"workspaceId":workspace,"agentId":agent,"content":"UNRELATED_PARK","priority":"queue"})).await;
    let control = json!({"workspaceId":workspace,"messageId":message,"agentId":agent,"attempt":1});
    assert_eq!(
        wss_rpc(&mut rpc, 5, "discussion.stopAgent", control.clone()).await["activity"]["state"],
        "stopped"
    );
    timeout(common::test_timeout(Duration::from_secs(30)), async {
        loop {
            if prompt_log(&log).len() >= 2 {
                break;
            }
            // timing-guard: observe the independent queued prompt entering the mock.
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let transcript = wss_rpc(
        &mut rpc,
        6,
        "agent.getConversation",
        json!({"agentId":agent}),
    )
    .await;
    assert!(
        transcript.to_string().contains("streaming-before-cancel"),
        "Stop retains partial text: {transcript}"
    );
    assert_eq!(
        wss_rpc(&mut rpc, 7, "discussion.retryAgent", control.clone()).await["activity"]["attempt"],
        2
    );
    assert_eq!(
        wss_rpc(&mut rpc, 8, "discussion.retryAgent", control.clone()).await["activity"]["attempt"],
        2
    );
    assert_eq!(
        wss_rpc(&mut rpc, 9, "discussion.stopAgent", control.clone()).await["activity"]["state"],
        "queued",
        "stale Stop must not affect attempt2"
    );
    let mut next = control.clone();
    next["attempt"] = json!(2);
    assert_eq!(
        wss_rpc(&mut rpc, 10, "discussion.stopAgent", next.clone()).await["activity"]["state"],
        "stopped"
    );
    assert_eq!(
        prompt_log(&log).len(),
        2,
        "queued Retry was never delivered into unrelated work"
    );
    let session = wss_rpc(&mut rpc, 11, "agent.get", json!({"agentId":agent})).await;
    assert_eq!(
        session["agent"]["status"], "active",
        "stopping a queued discussion must retain unrelated active turn: {session}"
    );
    rpc.close(None).await.unwrap();
    rpc = connect_ws(port, cfg).await;
    let restored = wait_activity(&mut rpc, &workspace, &message, "stopped").await;
    assert_eq!(restored["attempt"], 2);
    wss_rpc(&mut rpc, 12, "agent.stop", json!({"agentId":agent})).await;
    assert_eq!(
        wss_rpc(&mut rpc, 13, "discussion.retryAgent", next).await["activity"]["attempt"],
        3
    );
    wait_activity(&mut rpc, &workspace, &message, "completed").await;
    let page = wss_rpc(
        &mut rpc,
        14,
        "discussion.list",
        json!({"workspaceId":workspace}),
    )
    .await;
    assert_eq!(page["messages"].as_array().unwrap().len(), 1);
    assert_eq!(
        prompt_log(&log).len(),
        3,
        "exactly one provider delivery per admitted attempt"
    );
}

#[tokio::test]
async fn discussion_failed_retry_redrives_only_the_observed_attempt() {
    let Some(script) = gate("discussion failed Retry WSS") else {
        return;
    };
    let data_dir = temp_data_dir();
    let (workspace, _) = seed_workspace(data_dir.path()).await;
    let log = data_dir.path().join("failed-prompts.jsonl");
    let behavior =
        json!({"promptRpcError":{"code":-32603,"message":"Discussion fixture provider failure"}})
            .to_string();
    let child = spawn_serve(
        data_dir.path(),
        &[
            ("INTENTD_AUTH_TOKEN", TOKEN),
            ("MOCK_AGENT_SCRIPT_PATH", &script),
            ("MOCK_AGENT_BEHAVIOR", &behavior),
            ("MOCK_AGENT_PROMPT_LOG", log.to_str().unwrap()),
        ],
    );
    let daemon = Daemon { child, data_dir };
    let socket = daemon.data_dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut rpc = connect_ws(port, cfg).await;
    let agent=wss_rpc(&mut rpc,1,"agent.create",json!({"workspaceId":workspace,"name":"Failing recipient","provider":"mock","model":"default"})).await["agent"]["id"].as_str().unwrap().to_owned();
    let sent=wss_rpc(&mut rpc,2,"discussion.send",json!({"workspaceId":workspace,"messageId":"failed-original","body":"Do the work","recipientAgentIds":[agent]})).await;
    let message = sent["message"]["id"].as_str().unwrap().to_owned();
    let failed = wait_activity(&mut rpc, &workspace, &message, "failed").await;
    assert!(failed["error"]
        .as_str()
        .unwrap()
        .contains("Discussion fixture provider failure"));
    let request = json!({"workspaceId":workspace,"messageId":message,"agentId":agent,"attempt":1});
    assert_eq!(
        wss_rpc(&mut rpc, 3, "discussion.retryAgent", request.clone()).await["activity"]["attempt"],
        2
    );
    let failed_again = wait_activity(&mut rpc, &workspace, &message, "failed").await;
    assert_eq!(failed_again["attempt"], 2);
    assert_eq!(
        wss_rpc(&mut rpc, 4, "discussion.retryAgent", request).await["activity"],
        failed_again
    );
    let page = wss_rpc(
        &mut rpc,
        5,
        "discussion.list",
        json!({"workspaceId":workspace}),
    )
    .await;
    assert_eq!(page["messages"].as_array().unwrap().len(), 1);
    assert_eq!(
        prompt_log(&log).len(),
        2,
        "stale Retry does not create a third provider invocation"
    );
    let queue = wss_rpc(&mut rpc, 6, "agent.getQueue", json!({"agentId":agent})).await;
    assert!(
        queue["queue"].as_array().unwrap().is_empty(),
        "terminal discussion failures require explicit correlated Retry"
    );
}
