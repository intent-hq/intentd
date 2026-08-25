//! WSS end-to-end: a BACKGROUND HOOK calling an external MCP tool via
//! `ws.mcp.*` (§18.3 hub forwarding).
//!
//! Drives the full chain over the production transport: `mcp.servers.create`
//! and toggle spawn the stdio mock fixture → an agent turn schedules a hook
//! whose (validation) run calls `ws.mcp.listServers` / `listTools` /
//! `callTool` and dispatches with the tool result → the owner's wake carries
//! `tools/call`'s content back, with only the non-sensitive server projection
//! (never the fixture command path). Then flips `agentFeatures.mcpTools` off
//! via `settings.update` and asserts the gated hook environment: `ws.mcp` is
//! pruned from the prelude (`TypeError` on use) and a raw `host({...})` MCP
//! frame is denied at dispatch with the settings-gate error.
//!
//! Gated on `node` + the mock scripts; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::UnixStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use common::TlsWs;

const TOKEN: &str = "4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d";

/// Kickoff markers for the two hook-scheduling agent turns.
const SCHEDULE_OK_MARKER: &str = "SCHEDULE_MCP_HOOK_E2E";
const SCHEDULE_GATED_MARKER: &str = "SCHEDULE_MCP_GATED_E2E";

static NEXT_ID: AtomicI64 = AtomicI64::new(100);

fn next_id() -> i64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// Live `intentd serve` process; killed (whole process group) and its data
/// dir removed on drop, with the daemon log echoed on failure.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(self.child.id().cast_signed());
            let _ = signal::killpg(pid, Signal::SIGKILL);
        }
        let _ = self.child.wait();
        if std::thread::panicking() {
            let log_path = self.data_dir.join("daemon.log");
            if let Ok(log) = std::fs::read_to_string(&log_path) {
                eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-hookmcp-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    common::enable_ws_api(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    // Group leader so Daemon::drop can killpg the daemon + ACP children.
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
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
async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the matching result; out-of-band
/// notifications are ignored.
async fn wss_rpc(ws: &mut TlsWs, method: &str, params: Value) -> Value {
    let id = next_id();
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
                    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                    return v["result"].clone();
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

/// Poll the agent's conversation until `needle` appears (or panic at the
/// deadline). Returns the serialized conversation containing the needle.
async fn await_conversation_contains(
    rpc: &mut TlsWs,
    ws_id: &str,
    agent_id: &str,
    needle: &str,
    deadline: tokio::time::Instant,
) -> String {
    loop {
        let convo = wss_rpc(
            rpc,
            "agent.getConversation",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        let text = convo.to_string();
        if text.contains(needle) {
            return text;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "conversation for {agent_id} never contained {needle:?}: {text}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Mock-agent gate (parity with the other WSS E2E suites): requires `node`,
/// the mock ACP agent, and the mock MCP server fixture.
fn gate(test: &str) -> Option<(String, String)> {
    let agent_script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    let mcp_script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/mock-mcp-server.mjs"
    )
    .to_string();
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping {test}: node not on PATH");
        return None;
    }
    for script in [&agent_script, &mcp_script] {
        if !Path::new(script).exists() {
            eprintln!("skipping {test}: fixture missing at {script}");
            return None;
        }
    }
    Some((agent_script, mcp_script))
}

async fn seed_workspace_only(data_dir: &Path) -> String {
    use intent_core::{
        now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    let ts = now_iso();
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "WSS-HOOK-MCP-E2E".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
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
        })
        .await
        .expect("insert ws");
    ws.0
}

/// End-to-end §18.3 hub forwarding from a background hook: the hook's
/// schedule-time validation run calls `ws.mcp.listServers` → `listTools` →
/// `callTool` against the live stdio mock fixture and dispatches with the
/// results; the owner's wake carries the `tools/call` content back. Then the
/// `agentFeatures.mcpTools` toggle is flipped off and a second hook proves
/// the gated environment: `ws.mcp` is pruned (`TypeError`) and a raw
/// `host({...})` MCP frame is denied at dispatch with the settings-gate
/// error.
#[tokio::test]
async fn hook_calls_mcp_tool_end_to_end_and_gated_toggle_rejects() {
    let Some((agent_script, mcp_script)) = gate("WSS hook-MCP E2E") else {
        return;
    };

    // OK path: discover the one configured server, list its tools, call
    // `echo`, and ride everything back on the dispatch wake.
    let ok_code = "const ls = await ws.mcp.listServers(); \
                   const s = ls.servers[0]; \
                   const tools = await ws.mcp.listTools(s.id); \
                   const names = tools.tools.map((t) => t.name).sort().join(','); \
                   const r = await ws.mcp.callTool(s.id, 'echo', { input: 'hook-ping' }); \
                   return { dispatch: true, message: 'MCP_HOOK_OK server=' + JSON.stringify(s) + ' tools=' + names + ' result=' + JSON.stringify(r) };";
    // Gated path: `ws.mcp` is pruned from the prelude (property access
    // throws) and the raw dispatch frame is denied with the settings error.
    let gated_code = "let threw = ''; \
                      try { await ws.mcp.callTool('x', 'echo', {}); } \
                      catch (e) { threw = String(e); } \
                      let denied = ''; \
                      try { await host({ method: 'mcp.callTool', args: { serverId: 'x', toolName: 'echo' } }); } \
                      catch (e) { denied = String(e); } \
                      return { dispatch: true, message: 'MCP_HOOK_GATED typeof=' + typeof ws.mcp + ' threw=' + threw + ' denied=' + denied };";
    let ok_schedule = format!(
        "return await ws.hook.schedule({{ name: 'mcp-caller', code: {}, delayMs: 600000 }});",
        json!(ok_code)
    );
    let gated_schedule = format!(
        "return await ws.hook.schedule({{ name: 'mcp-gated', code: {}, delayMs: 600000 }});",
        json!(gated_code)
    );
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": SCHEDULE_OK_MARKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": ok_schedule, "summary": "schedule mcp-calling hook" }
                },
                "response": "scheduled the mcp hook",
            },
            {
                "ifPromptContains": SCHEDULE_GATED_MARKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": gated_schedule, "summary": "schedule gated mcp hook" }
                },
                "response": "scheduled the gated hook",
            },
        ],
        "response": "acknowledged",
    })
    .to_string();

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &agent_script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Configure + start the stdio mock MCP server on the daemon.
    let created = wss_rpc(
        &mut rpc,
        "mcp.servers.create",
        json!({ "config": {
            "name": "Mock Stdio",
            "transport": "stdio",
            "command": "node",
            "args": [mcp_script.clone()],
            "enabled": false,
        } }),
    )
    .await;
    let server_id = created["server"]["id"].as_str().expect("id").to_string();
    let toggled = wss_rpc(
        &mut rpc,
        "mcp.servers.toggle",
        json!({ "serverId": server_id, "enabled": true }),
    )
    .await;
    assert_eq!(toggled["status"]["state"], "running", "{toggled}");
    assert_eq!(toggled["status"]["toolCount"], json!(2), "{toggled}");

    let created = wss_rpc(
        &mut rpc,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-HOOK-MCP", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // ===== OK path: hook → ws.mcp.* → mock fixture → dispatch wake =====
    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("{SCHEDULE_OK_MARKER} call the mcp tool"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "kickoff sendMessage ok: {sent}");

    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(90));
    let text =
        await_conversation_contains(&mut rpc, &ws_id, &agent_id, "MCP_HOOK_OK", deadline).await;
    // The dispatch wake carries the `tools/call` result (the fixture answers
    // `<tool>:<args.input>`), the tool catalog, and the non-sensitive server
    // projection.
    assert!(
        text.contains("echo:hook-ping"),
        "tool result in wake: {text}"
    );
    assert!(text.contains("tools=echo,reverse"), "tool names: {text}");
    assert!(
        text.contains(r#"\"transport\":\"stdio\""#) || text.contains(r#""transport":"stdio""#),
        "server projection in wake: {text}"
    );
    // The projection is allowlisted: the fixture command path (config
    // `command`/`args`) must never ride back to the agent.
    assert!(
        !text.contains("mock-mcp-server.mjs"),
        "server command leaked into the wake: {text}"
    );

    // ===== Gated path: toggle off → ws.mcp pruned + dispatch denied =====
    let updated = wss_rpc(
        &mut rpc,
        "settings.update",
        json!({ "changes": [{ "path": "agentFeatures.mcpTools", "value": false }] }),
    )
    .await;
    assert_eq!(
        updated["applied"][0]["value"],
        json!(false),
        "mcpTools off: {updated}"
    );

    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("{SCHEDULE_GATED_MARKER} try the mcp tool anyway"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "gated sendMessage ok: {sent}");

    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(90));
    let text =
        await_conversation_contains(&mut rpc, &ws_id, &agent_id, "MCP_HOOK_GATED", deadline).await;
    // Prelude pruning: `ws.mcp` is gone, so using it throws a TypeError.
    assert!(text.contains("typeof=undefined"), "ws.mcp pruned: {text}");
    assert!(text.contains("TypeError"), "clear TypeError on use: {text}");
    // Defense in depth: the raw dispatch frame is denied with the settings
    // gate naming the toggle.
    assert!(
        text.contains("disabled in settings (agentFeatures.mcpTools = false)"),
        "dispatch denied with the settings gate: {text}"
    );
}
