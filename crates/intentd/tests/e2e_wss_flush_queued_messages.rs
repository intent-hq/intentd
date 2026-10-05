//! WSS end-to-end coverage for queued-message batching.
//! Messages queued while an agent is busy are delivered as ONE combined turn when the busy turn ends.
//!
//! Case 1 (default): start a slow turn, queue 2 messages behind it,
//! let the turn end. The provider-received prompt (via the mock fixture's
//! `MOCK_AGENT_PROMPT_LOG` seam) is a single message starting with
//! `2 queued messages while you were working` carrying `Message #1:` /
//! `Message #2:` plus each entry's dequeue-wait `[SYSTEM NOTE]`; the
//! transcript keeps two separate user rows; `agent:queue:updated` empties in
//! one snapshot (2 → 0, never through 1) and exactly ONE
//! `agent:queue:processing` fires.
//!
//! Legacy off, systemOnly, and boolean preferences are retired: all ready
//! entries still batch, the catalog omits the setting, and old writes are ignored.
//!
//! Case 4: participants share the queue, consecutive submissions by the same
//! author merge, and direct RPC mutations remain author/owner restricted.
//! Different authors still drain as distinct rows in a combined turn.
//!
//! Case 5 (`agent.diagnostics`, two members + one agent-sent entry): the
//! `queues[]` view is projected per caller exactly like `agent.getQueue`.
//! Over the wire only the administrator reaches it — the guest's call is
//! refused by the collaborator allowlist (`-32003`) — and the owner's
//! diagnostics list all three entries with `queueLength` and
//! `summary.queuedAgents` following the projected entries.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use intentd_test_support::GuardedChild;
use std::path::{Path, PathBuf};
use std::process::Stdio;
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
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";
const GUEST_TOKEN: &str = "beefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef";

const KICKOFF_MSG: &str = "kick-off slow turn";
const QUEUED_ONE: &str = "queued flush one";
const QUEUED_TWO_INPUT: &str = "queued flush two";
const QUEUED_TWO: &str = "Message from @guest (Guest User), a collaborator (guest) of this workspace — not the workspace owner.\n\nqueued flush two";
const OWNER_QUEUED: &str = "queued by owner";
const GUEST_QUEUED: &str = "queued by guest";
const GUEST_PREAMBLE: &str = "Message from @guest";
const AGENT_QUEUED: &str = "queued by a sibling agent";
const RELAY_MARKER: &str = "relay to the busy target";
const FLUSH_HEADER: &str = "2 queued messages while you were working";
const WAIT_NOTE_PREFIX: &str = "[SYSTEM NOTE] This message was queued at";

struct Daemon {
    child: GuardedChild,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Providers own separate process groups. Let normal daemon shutdown
        // stop them before reaping the daemon; the guard remains the fallback.
        let stopped = self.child.try_wait().and_then(|status| {
            if let Some(status) = status {
                Ok(Some(status))
            } else {
                self.child
                    .signal(nix::sys::signal::Signal::SIGTERM)
                    .map_err(std::io::Error::other)?;
                self.child
                    .wait_with_timeout(common::test_timeout(Duration::from_secs(5)))
            }
        });
        eprintln!("queue fixture daemon {} wait: {stopped:?}", self.child.id());
        let normal = matches!(stopped, Ok(Some(status)) if status.success());
        if !normal {
            if std::thread::panicking() {
                eprintln!("queue fixture shutdown failed while preserving the original panic");
            } else {
                panic!("queue fixture daemon did not stop normally: {stopped:?}");
            }
        }
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
    }
}

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-flush-")
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> GuardedChild {
    GuardedChild::spawn(&mut fixture_command(data_dir, env)).expect("spawn intentd serve")
}

fn fixture_command(data_dir: &Path, env: &[(&str, &str)]) -> std::process::Command {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    common::hermetic_github_identity(&mut cmd, data_dir);
    cmd.env("INTENTD_SECRETS_FILE", data_dir.join("secrets.json"));
    cmd
}

mod fixture_command_tests {
    use super::*;
    use std::ffi::OsStr;

    #[derive(Debug, PartialEq, Eq)]
    enum EnvSetting<'a> {
        Inherited,
        Removed,
        Set(&'a OsStr),
    }

    fn explicit_env<'a>(cmd: &'a std::process::Command, name: &str) -> EnvSetting<'a> {
        match cmd.get_envs().find(|(key, _)| *key == OsStr::new(name)) {
            None => EnvSetting::Inherited,
            Some((_, None)) => EnvSetting::Removed,
            Some((_, Some(value))) => EnvSetting::Set(value),
        }
    }

    #[test]
    fn removes_synthetic_host_tokens() {
        let dir = common::test_tempdir("queue-fixture-env-");
        let cmd = fixture_command(
            dir.path(),
            &[
                ("GH_TOKEN", "synthetic-gh-token"),
                ("GITHUB_TOKEN", "synthetic-github-token"),
            ],
        );
        assert_eq!(explicit_env(&cmd, "GH_TOKEN"), EnvSetting::Removed);
        assert_eq!(explicit_env(&cmd, "GITHUB_TOKEN"), EnvSetting::Removed);
    }

    #[test]
    fn replaces_synthetic_host_gh_config() {
        let dir = common::test_tempdir("queue-fixture-env-");
        let host = common::test_tempdir("queue-synthetic-host-");
        let hosts_file = host.path().join("hosts.yml");
        std::fs::write(&hosts_file, "synthetic host configuration").unwrap();
        let cmd = fixture_command(
            dir.path(),
            &[("GH_CONFIG_DIR", host.path().to_str().unwrap())],
        );
        let private = dir.path().join("gh-config");
        assert_eq!(
            explicit_env(&cmd, "GH_CONFIG_DIR"),
            EnvSetting::Set(private.as_os_str())
        );
        assert_eq!(std::fs::read_dir(&private).unwrap().count(), 0);
        assert_eq!(
            std::fs::read_to_string(hosts_file).unwrap(),
            "synthetic host configuration"
        );
    }

    #[test]
    fn replaces_synthetic_host_secrets_file() {
        let dir = common::test_tempdir("queue-fixture-env-");
        let host = common::test_tempdir("queue-synthetic-host-");
        let secrets_file = host.path().join("secrets.json");
        let synthetic = r#"{"github.token":"synthetic-token"}"#;
        std::fs::write(&secrets_file, synthetic).unwrap();
        let cmd = fixture_command(
            dir.path(),
            &[("INTENTD_SECRETS_FILE", secrets_file.to_str().unwrap())],
        );
        let private = dir.path().join("secrets.json");
        assert_eq!(
            explicit_env(&cmd, "INTENTD_SECRETS_FILE"),
            EnvSetting::Set(private.as_os_str())
        );
        assert!(
            !private.exists(),
            "fixture starts with an empty secret store"
        );
        assert_eq!(std::fs::read_to_string(secrets_file).unwrap(), synthetic);
    }

    #[test]
    fn preserves_daemon_and_mock_configuration() {
        let dir = common::test_tempdir("queue-fixture-env-");
        let cmd = fixture_command(
            dir.path(),
            &[
                ("MOCK_AGENT_BEHAVIOR", "synthetic behavior"),
                ("INTENTD_AUTH_TOKEN", TOKEN),
            ],
        );
        assert_eq!(cmd.get_program(), OsStr::new(env!("CARGO_BIN_EXE_intentd")));
        assert_eq!(
            cmd.get_args().collect::<Vec<_>>(),
            vec![OsStr::new("serve")]
        );
        assert_eq!(
            explicit_env(&cmd, "INTENTD_DATA_DIR"),
            EnvSetting::Set(dir.path().as_os_str())
        );
        let workspaces = dir.path().join("workspaces");
        assert_eq!(
            explicit_env(&cmd, "INTENTD_WORKSPACES_DIR"),
            EnvSetting::Set(workspaces.as_os_str())
        );
        assert_eq!(
            explicit_env(&cmd, "INTENTD_TCP_PORT"),
            EnvSetting::Set(OsStr::new("0"))
        );
        assert_eq!(
            explicit_env(&cmd, "MOCK_AGENT_BEHAVIOR"),
            EnvSetting::Set(OsStr::new("synthetic behavior"))
        );
        assert_eq!(
            explicit_env(&cmd, "INTENTD_AUTH_TOKEN"),
            EnvSetting::Set(OsStr::new(TOKEN))
        );
    }
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

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> common::TlsWs {
    connect_ws_as(port, cfg, TOKEN).await
}

async fn connect_ws_as(port: u16, cfg: Arc<ClientConfig>, token: &str) -> common::TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={token}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// One JSON-RPC round-trip returning the full response envelope (pushes and
/// pings interleaved on the same socket are skipped) — for refusal
/// assertions on `error`.
async fn wss_rpc_envelope<S>(
    ws: &mut WebSocketStream<S>,
    id: i64,
    method: &str,
    params: Value,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(15), ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
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

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let v = wss_rpc_envelope(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

async fn wss_event<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(secs), ws.next())
            .await
            .expect("wss event timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
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

/// Seed a workspace plus a non-primary `guest` principal — credential bound
/// to `GUEST_TOKEN`, collaborator member of the workspace — BEFORE boot.
/// Returns `(workspace id, guest principal)`.
async fn seed_workspace_with_guest(data_dir: &Path) -> (String, intent_core::Principal) {
    use intent_core::{now_iso, Principal, PrincipalId, WorkspaceId, WorkspaceRole};
    use intent_store::Store;
    let store = Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&ws))
        .await
        .expect("insert ws");
    let guest = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: Some("guest".to_string()),
        display_name: Some("Guest User".to_string()),
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    store
        .upsert_principal(&guest)
        .await
        .expect("guest principal");
    let token_hash =
        Sha256::digest(GUEST_TOKEN.as_bytes())
            .iter()
            .fold(String::new(), |mut s, b| {
                use std::fmt::Write as _;
                let _ = write!(s, "{b:02x}");
                s
            });
    store
        .insert_principal_credential(&guest.id, &token_hash)
        .await
        .expect("guest credential");
    store
        .add_workspace_member(&ws, &guest.id, WorkspaceRole::Collaborator)
        .await
        .expect("guest membership");
    (ws.0, guest)
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WSS-FLUSH-E2E".to_string(),
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
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        membership: None,
    }
}

/// Seed `agents.flushQueuedMessages = <mode>` into the data dir's
/// `config.toml` BEFORE boot (must run before `enable_ws_api` appends the
/// `[server.wsApi]` table, and the two tables must not collide).
fn seed_flush_mode(data_dir: &Path, mode: &str) {
    std::fs::create_dir_all(data_dir).expect("mkdir data dir");
    let path = data_dir.join("config.toml");
    assert!(
        !path.exists(),
        "seed_flush_mode must run before other config seeding"
    );
    // stateSnapshot off: drain turns are built while messages are still
    // queued, so the per-turn snapshot line would otherwise lead the prompt
    // and break this suite's byte-precise prompt-prefix assertions (the
    // injection has its own e2e in e2e_wss_agent_state_snapshot.rs).
    std::fs::write(
        &path,
        format!(
            "[agents]\nflushQueuedMessages = {mode}\n\n[agentFeatures]\nstateSnapshot = false\n"
        ),
    )
    .expect("seed config.toml with flushQueuedMessages mode");
}

/// Boot a daemon with the slow-first-turn mock, create an agent, start the
/// kick-off turn, and queue TWO messages behind it (both `queued: true`).
/// Returns everything the per-case assertions need. The `sub` connection is
/// already subscribed to `agent:*` for the workspace — subscription happens
/// BEFORE the kick-off send, so no drain event can be missed.
struct FlushSetup {
    _daemon: Daemon,
    sub: common::TlsWs,
    rpc: common::TlsWs,
    agent_id: String,
    prompt_log: PathBuf,
    /// Queue entry ids of `QUEUED_ONE` / `QUEUED_TWO`, in queue order.
    queued_ids: [String; 2],
}

/// A booted daemon with the slow-first-turn mock: the first turn parks
/// `first_turn_delay_ms` (a deterministic window to queue messages while the
/// worker is busy); queue-drained turns run at full mock speed.
struct Booted {
    daemon: Daemon,
    port: u16,
    cfg: Arc<ClientConfig>,
    prompt_log: PathBuf,
}

/// `kickoff_release`: when set, the mock ALSO holds the kick-off turn
/// (`KICKOFF_MSG`) open until this file exists — a barrier the test releases
/// once its busy-window work is provably done, instead of a timer that host
/// scheduling can outrun. `extra_rules` are appended to the mock's
/// prompt-matched `rules` after the kick-off barrier.
async fn boot_daemon(
    data_dir: &Path,
    script: &str,
    first_turn_delay_ms: u64,
    kickoff_release: Option<&Path>,
    extra_rules: &[Value],
) -> Booted {
    let prompt_log = data_dir.join("prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let mut behavior =
        json!({ "response": "flush reply", "firstTurnDelayMs": first_turn_delay_ms });
    let mut rules: Vec<Value> = Vec::new();
    if let Some(release) = kickoff_release {
        rules.push(json!({ "ifPromptContains": KICKOFF_MSG, "releaseFile": release }));
    }
    rules.extend(extra_rules.iter().cloned());
    if !rules.is_empty() {
        behavior["rules"] = Value::Array(rules);
    }
    let behavior = behavior.to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        // The busy window sits below the 5s dequeue-wait annotation
        // threshold (monorepo#2353); drop it so the wait-note assertions
        // exercise the annotation without slowing the suite.
        ("INTENTD_DEQUEUE_WAIT_MIN_MS", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
    ];
    let child = spawn_serve(data_dir, &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.to_path_buf(),
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
    Booted {
        daemon,
        port,
        cfg: client_config(&fingerprint),
        prompt_log,
    }
}

async fn setup_busy_agent_with_two_queued(data_dir: &Path, script: &str) -> FlushSetup {
    // Different human authors remain separate entries for batch-flush coverage.
    let (ws_id, _guest) = seed_workspace_with_guest(data_dir).await;
    let Booted {
        daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(data_dir, script, 2000, None, &[]).await;

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-FLUSH", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Kick off the slow turn (idle agent → streams immediately, not queued).
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": KICKOFF_MSG }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    assert_eq!(
        sent["queued"], false,
        "kick-off streams, not queued: {sent}"
    );

    // Queue two messages behind the parked turn. `queued: true` on both
    // proves they landed on the queue (no self-drain race).
    let q1 = wss_rpc(
        &mut rpc,
        12,
        "agent.queueMessage",
        json!({ "agentId": agent_id, "content": QUEUED_ONE }),
    )
    .await;
    assert_eq!(q1["success"], true, "queue one: {q1}");
    let mut guest_rpc = connect_ws_as(port, cfg.clone(), GUEST_TOKEN).await;
    let q2 = wss_rpc(
        &mut guest_rpc,
        13,
        "agent.queueMessage",
        json!({ "agentId": agent_id, "content": QUEUED_TWO_INPUT }),
    )
    .await;
    assert_eq!(q2["success"], true, "queue two: {q2}");
    let entry_id = |resp: &Value| {
        resp["queuedMessage"]["id"]
            .as_str()
            .expect("queueMessage returns the entry id")
            .to_string()
    };
    let queued_ids = [entry_id(&q1), entry_id(&q2)];
    assert_ne!(queued_ids[0], queued_ids[1], "distinct entry ids");

    let queue = wss_rpc(
        &mut rpc,
        14,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    let entries = queue["queue"].as_array().expect("queue array");
    assert_eq!(entries.len(), 2, "both messages queued mid-turn: {queue}");
    assert_eq!(entries[0]["content"], json!(QUEUED_ONE));
    assert_eq!(entries[1]["content"], json!(QUEUED_TWO));
    assert_eq!(entries[0]["id"], json!(queued_ids[0]));
    assert_eq!(entries[1]["id"], json!(queued_ids[1]));

    FlushSetup {
        _daemon: daemon,
        sub,
        rpc,
        agent_id,
        prompt_log,
        queued_ids,
    }
}

/// Poll the mock fixture's prompt log until `min_lines` prompts have been
/// recorded, returning the prompt texts in turn order.
async fn await_prompts(prompt_log: &Path, min_lines: usize) -> Vec<String> {
    for _ in 0..150 {
        if let Ok(log) = std::fs::read_to_string(prompt_log) {
            let texts: Vec<String> = log
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .filter_map(|p| p["text"].as_str().map(str::to_string))
                .collect();
            if texts.len() >= min_lines {
                return texts;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("prompt log never reached {min_lines} entries");
}

/// The `agent.getConversation` user row whose first text block starts with
/// `needle` (metadata assertions need the full row, not just its text).
fn user_row<'a>(conv: &'a Value, needle: &str) -> &'a Value {
    conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .find(|m| {
            m["contentBlocks"][0]["text"]
                .as_str()
                .is_some_and(|t| t.starts_with(needle))
        })
        .unwrap_or_else(|| panic!("missing user row {needle:?}: {conv}"))
}

/// User-row texts from `agent.getConversation`, in transcript order.
fn user_row_texts(conv: &Value) -> Vec<String> {
    conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .filter_map(|m| {
            m["contentBlocks"]
                .as_array()
                .and_then(|blocks| blocks.first())
                .and_then(|b| b["text"].as_str())
                .map(String::from)
        })
        .collect()
}

/// Drain-phase observation shared by both cases: consume subscription events
/// until `want_stream_ends` terminal `agent:stream:end`s for the agent have
/// been seen (kick-off turn + drained turn(s)), recording every non-empty
/// `agent:queue:updated` queue length, every `agent:queue:processing`
/// `turnId`, and the `turnId` of every user-row `agent:message` echo (the
/// kick-off send's echo carries its own turn's id, so it appears first),
/// plus each user-row echo's drain identity link `queuedMessageId`
/// (intentd#1783; `None` for the direct-send kick-off echo).
/// `processing_frames` keeps each `agent:queue:processing` `data` payload
/// whole, for the per-subscriber `content` projection.
struct DrainObservation {
    queue_lengths: Vec<usize>,
    processing_turn_ids: Vec<String>,
    processing_frames: Vec<Value>,
    user_row_turn_ids: Vec<String>,
    user_row_queued_message_ids: Vec<Option<String>>,
    user_frames: Vec<Value>,
}

async fn observe_drain(
    sub: &mut common::TlsWs,
    agent_id: &str,
    want_stream_ends: usize,
) -> DrainObservation {
    let mut queue_lengths = Vec::new();
    let mut processing_turn_ids = Vec::new();
    let mut processing_frames = Vec::new();
    let mut user_row_turn_ids = Vec::new();
    let mut user_row_queued_message_ids = Vec::new();
    let mut user_frames = Vec::new();
    let mut stream_ends = 0usize;
    for _ in 0..400 {
        let frame = wss_event(sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:queue:updated") => {
                let len = event["data"]["queue"].as_array().map_or(0, Vec::len);
                queue_lengths.push(len);
            }
            Some("agent:queue:processing") => {
                processing_turn_ids.push(
                    event["data"]["turnId"]
                        .as_str()
                        .expect("queue:processing carries a turnId")
                        .to_string(),
                );
                processing_frames.push(event["data"].clone());
            }
            Some("agent:message") => {
                if event["data"]["role"] == "user" {
                    user_frames.push(event["data"].clone());
                    if let Some(tid) = event["data"]["turnId"].as_str() {
                        user_row_turn_ids.push(tid.to_string());
                    }
                    user_row_queued_message_ids.push(
                        event["data"]["queuedMessageId"]
                            .as_str()
                            .map(str::to_string),
                    );
                }
            }
            Some("agent:stream:end") => {
                stream_ends += 1;
                if stream_ends >= want_stream_ends {
                    break;
                }
            }
            _ => {}
        }
    }
    assert_eq!(
        stream_ends, want_stream_ends,
        "expected {want_stream_ends} terminal stream:ends (saw {stream_ends})"
    );
    DrainObservation {
        queue_lengths,
        processing_turn_ids,
        processing_frames,
        user_row_turn_ids,
        user_row_queued_message_ids,
        user_frames,
    }
}

/// Queue snapshot lengths from the DRAIN phase: everything after the last
/// length-2 snapshot (the enqueue phase publishes 1 → 2 before the busy turn
/// ends; the subscription sees those buffered events too).
fn shrink_lengths(queue_lengths: &[usize]) -> &[usize] {
    let last_two = queue_lengths
        .iter()
        .rposition(|&l| l == 2)
        .unwrap_or_else(|| panic!("never observed the 2-entry queue snapshot: {queue_lengths:?}"));
    &queue_lengths[last_two + 1..]
}

/// FLUSH-1 (default batching): two messages
/// queued behind a busy turn are delivered as ONE combined turn.
///
/// Contract locked down:
/// 1. The provider-received prompt (mock fixture's `MOCK_AGENT_PROMPT_LOG`)
///    is a single message starting with `2 queued messages while you were
///    working`, carrying `Message #1:` / `Message #2:` in queue order, each
///    followed by its dequeue-wait `[SYSTEM NOTE] This message was queued
///    at … and waited …` annotation.
/// 2. The transcript (`agent.getConversation`) keeps TWO separate user rows
///    for the queued messages — the combined prompt is wire-only.
/// 3. `agent:queue:updated` empties in ONE snapshot (2 → 0, never through
///    1) and exactly ONE `agent:queue:processing` fires for the batch.
/// 4. Turn correlation (monorepo#1022): BOTH user-row `agent:message`
///    echoes carry the combined turn's `turnId` — the one named by the
///    single `agent:queue:processing` — never a per-entry id that matches
///    no processing/stream lifecycle.
/// 5. Batch grouping: both flushed rows carry the SAME
///    `metadata.queueInfo.batchId` on `agent.getConversation`; the direct
///    kick-off row carries none.
/// 6. Drain identity link (intentd#1783): the two flushed rows carry
///    DISTINCT `metadata.queueInfo.queuedMessageId`s — each its own queue
///    entry's id, in queue order — and each row's `agent:message` echo
///    lifts the same id as `queuedMessageId`; the kick-off row/echo carry
///    none.
#[tokio::test]
async fn flush_combines_queued_messages_into_one_turn_over_wss() {
    let Some(script) = gate("WSS queued-message flush E2E") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let mut setup = setup_busy_agent_with_two_queued(&data_dir, &script).await;

    // Two terminal stream:ends: the kick-off turn, then the ONE combined
    // flush turn (a third would mean the drain split the batch).
    let obs = observe_drain(&mut setup.sub, &setup.agent_id, 2).await;

    // (3) One-shot queue shrink: after the last 2-entry snapshot (the
    // enqueue phase publishes 1 → 2), the batch dequeue publishes the fully
    // drained queue in a single snapshot — an intermediate length-1 snapshot
    // means one-at-a-time drain leaked through the flush arm.
    let shrink = shrink_lengths(&obs.queue_lengths);
    assert!(
        !shrink.contains(&1),
        "queue must empty in one snapshot (2 → 0), never through 1: {:?}",
        obs.queue_lengths
    );
    assert!(
        shrink.ends_with(&[0]),
        "final queue snapshot is empty: {:?}",
        obs.queue_lengths
    );
    assert_eq!(
        obs.processing_turn_ids.len(),
        1,
        "exactly ONE agent:queue:processing for the combined turn: {:?}",
        obs.processing_turn_ids
    );
    let processing_rows = obs.processing_frames[0]["queuedMessages"]
        .as_array()
        .unwrap();
    assert_eq!(processing_rows.len(), 2);
    for (index, row) in processing_rows.iter().enumerate() {
        assert_eq!(row["id"], setup.queued_ids[index]);
        assert_eq!(
            row["messageMetadata"]["queueInfo"]["queuedMessageId"],
            setup.queued_ids[index]
        );
        assert!(row["author"]["principalId"].is_string());
    }
    assert_ne!(processing_rows[0]["author"], processing_rows[1]["author"]);
    assert_eq!(
        processing_rows[0]["turnId"],
        obs.processing_frames[0]["turnId"]
    );
    assert_ne!(
        processing_rows[1]["turnId"],
        obs.processing_frames[0]["turnId"]
    );
    // (4) Every flushed row's echo correlates with the combined turn. The
    // first user-row echo is the kick-off's (its own direct turn's id).
    let combined_turn_id = &obs.processing_turn_ids[0];
    assert_eq!(
        obs.user_row_turn_ids.len(),
        3,
        "kick-off + two flushed user-row echoes: {:?}",
        obs.user_row_turn_ids
    );
    assert_eq!(
        &obs.user_row_turn_ids[1..],
        &[combined_turn_id.clone(), combined_turn_id.clone()],
        "both flushed user-row echoes carry the combined turn's turnId"
    );

    // (1) Outbound-prompt contract: prompt #2 is the combined flush prompt.
    let prompts = await_prompts(&setup.prompt_log, 2).await;
    assert_eq!(prompts.len(), 2, "kick-off + ONE flush turn: {prompts:?}");
    assert!(
        prompts[0].contains(KICKOFF_MSG),
        "first prompt is the kick-off: {}",
        prompts[0]
    );
    let flush = &prompts[1];
    assert!(
        flush.starts_with(FLUSH_HEADER),
        "flush prompt starts with the batch header: {flush}"
    );
    let m1 = flush
        .find("Message #1:")
        .unwrap_or_else(|| panic!("flush prompt carries Message #1: {flush}"));
    let m2 = flush
        .find("Message #2:")
        .unwrap_or_else(|| panic!("flush prompt carries Message #2: {flush}"));
    assert!(m1 < m2, "messages appear in queue order: {flush}");
    let i_one = flush
        .find(QUEUED_ONE)
        .unwrap_or_else(|| panic!("flush prompt carries {QUEUED_ONE:?}: {flush}"));
    let i_two = flush
        .find(QUEUED_TWO)
        .unwrap_or_else(|| panic!("flush prompt carries {QUEUED_TWO:?}: {flush}"));
    assert!(
        m1 < i_one && i_one < m2 && m2 < i_two,
        "each label precedes its content: {flush}"
    );
    // Each entry carries its own dequeue-wait note (queuedAt + wait info).
    assert_eq!(
        flush.matches(WAIT_NOTE_PREFIX).count(),
        2,
        "one dequeue-wait [SYSTEM NOTE] per batched entry: {flush}"
    );
    assert!(
        flush.contains("before delivery."),
        "wait note carries the waited duration: {flush}"
    );

    // (2) Transcript contract: two SEPARATE user rows for the queued
    // messages (prefix match — drained rows carry the appended wait note),
    // and no row carries the wire-only combined header.
    let conv = wss_rpc(
        &mut setup.rpc,
        20,
        "agent.getConversation",
        json!({ "agentId": setup.agent_id }),
    )
    .await;
    let users = user_row_texts(&conv);
    let idx = |needle: &str| {
        users
            .iter()
            .position(|t| t.starts_with(needle))
            .unwrap_or_else(|| panic!("missing user row {needle:?}: {users:?}"))
    };
    assert!(
        idx(KICKOFF_MSG) < idx(QUEUED_ONE) && idx(QUEUED_ONE) < idx(QUEUED_TWO),
        "three user rows in delivery order: {users:?}"
    );
    for needle in [QUEUED_ONE, QUEUED_TWO] {
        assert_eq!(
            users.iter().filter(|t| t.starts_with(needle)).count(),
            1,
            "queued message {needle:?} persists as exactly one row: {users:?}"
        );
    }
    assert!(
        users.iter().all(|t| !t.contains(FLUSH_HEADER)),
        "combined header is wire-only, never a transcript row: {users:?}"
    );

    // (5) Batch grouping stamp: both flushed rows share ONE
    // metadata.queueInfo.batchId; the kick-off row (direct send) has none.
    let row = |needle: &str| user_row(&conv, needle);
    let batch_id = row(QUEUED_ONE)["metadata"]["queueInfo"]["batchId"]
        .as_str()
        .expect("flushed row carries queueInfo.batchId")
        .to_string();
    assert!(!batch_id.is_empty(), "batchId is a non-empty string");
    assert_eq!(
        row(QUEUED_TWO)["metadata"]["queueInfo"]["batchId"].as_str(),
        Some(batch_id.as_str()),
        "both flushed user rows share the batch's id"
    );
    assert!(
        row(KICKOFF_MSG)["metadata"]["queueInfo"]["batchId"].is_null(),
        "the direct-send kick-off row carries no batchId"
    );

    // (6) Drain identity link: each flushed row names ITS OWN queue entry
    // (distinct ids, queue order), next to the shared batchId; each row's
    // echo lifted the same id; the kick-off row/echo carry none.
    let [one_id, two_id] = &setup.queued_ids;
    assert_eq!(
        row(QUEUED_ONE)["metadata"]["queueInfo"]["queuedMessageId"],
        json!(one_id),
        "first flushed row links its own entry"
    );
    assert_eq!(
        row(QUEUED_TWO)["metadata"]["queueInfo"]["queuedMessageId"],
        json!(two_id),
        "second flushed row links its own entry"
    );
    assert!(
        row(KICKOFF_MSG)["metadata"]["queueInfo"]["queuedMessageId"].is_null(),
        "the direct-send kick-off row carries no queuedMessageId"
    );
    assert_eq!(
        obs.user_row_queued_message_ids,
        vec![None, Some(one_id.clone()), Some(two_id.clone())],
        "kick-off echo unlinked; each flushed echo lifts its own entry id"
    );

    // Queue is empty after the flush.
    let queue = wss_rpc(
        &mut setup.rpc,
        21,
        "agent.getQueue",
        json!({ "agentId": setup.agent_id }),
    )
    .await;
    assert!(
        queue["queue"].as_array().expect("queue array").is_empty(),
        "queue empty after flush: {queue}"
    );
}

/// Legacy preferences load safely and cannot disable batching.
#[tokio::test]
async fn legacy_flush_preferences_always_batch_over_wss() {
    let Some(script) = gate("WSS legacy queued-message preferences E2E") else {
        return;
    };
    for raw in [r#""off""#, r#""systemOnly""#, "false", "true", r#""all""#] {
        let data_dir_guard = temp_data_dir();
        let data_dir = data_dir_guard.path();
        seed_flush_mode(data_dir, raw);
        let mut setup = setup_busy_agent_with_two_queued(data_dir, &script).await;
        let obs = observe_drain(&mut setup.sub, &setup.agent_id, 2).await;
        let shrink = shrink_lengths(&obs.queue_lengths);
        assert!(!shrink.contains(&1), "{raw}: queue must batch: {shrink:?}");
        assert!(
            shrink.ends_with(&[0]),
            "{raw}: queue must empty: {shrink:?}"
        );
        assert_eq!(obs.processing_turn_ids.len(), 1, "{raw}: one batch turn");
        let prompts = await_prompts(&setup.prompt_log, 2).await;
        assert_eq!(prompts.len(), 2, "{raw}: kickoff plus batch");
        assert!(
            prompts[1].starts_with(FLUSH_HEADER),
            "{raw}: {}",
            prompts[1]
        );
        assert!(prompts[1].find(QUEUED_ONE).unwrap() < prompts[1].find(QUEUED_TWO).unwrap());
        assert_eq!(prompts[1].matches(WAIT_NOTE_PREFIX).count(), 2);
        let conv = wss_rpc(
            &mut setup.rpc,
            20,
            "agent.getConversation",
            json!({"agentId": setup.agent_id}),
        )
        .await;
        let first = user_row(&conv, QUEUED_ONE);
        let second = user_row(&conv, QUEUED_TWO);
        assert!(first["metadata"]["queueInfo"]["batchId"].is_string());
        assert_eq!(
            first["metadata"]["queueInfo"]["batchId"],
            second["metadata"]["queueInfo"]["batchId"]
        );
        let settings = wss_rpc(&mut setup.rpc, 21, "settings.list", json!({})).await;
        assert!(settings["settings"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["path"] != "agents.flushQueuedMessages"));
        let updated = wss_rpc(
            &mut setup.rpc,
            22,
            "settings.update",
            json!({"changes":[{"path":"agents.flushQueuedMessages","value":"off"}]}),
        )
        .await;
        assert_eq!(updated["applied"], json!([]));
        let got = wss_rpc_envelope(
            &mut setup.rpc,
            23,
            "settings.get",
            json!({"path":"agents.flushQueuedMessages"}),
        )
        .await;
        assert_eq!(got["jsonrpc"], "2.0");
        assert_eq!(got["id"], 23);
        assert_eq!(got["error"]["code"], -32602);
        let config = std::fs::read_to_string(data_dir.join("config.toml")).unwrap();
        assert!(
            !config.contains("flushQueuedMessages"),
            "{raw}: legacy key stripped"
        );
    }
}

/// Queue entry ids of an `agent:queue:updated` payload, in drain order.
fn queue_ids(queue: &Value) -> Vec<String> {
    queue
        .as_array()
        .expect("queue array")
        .iter()
        .map(|e| e["id"].as_str().expect("entry id").to_string())
        .collect()
}

/// Enqueue-phase observation: consume subscription events until an
/// `agent:queue:updated` snapshot for the agent satisfies `done`, returning
/// EVERY `agent:queue:updated` payload seen for the agent (the matching one
/// last). A terminal `agent:stream:end` for the agent before then means the
/// busy window closed early — fail loudly rather than mis-attribute the
/// drain-phase snapshots.
async fn await_queue_snapshots(
    sub: &mut common::TlsWs,
    agent_id: &str,
    done: impl Fn(&Value) -> bool,
) -> Vec<Value> {
    let mut seen = Vec::new();
    for _ in 0..200 {
        let frame = wss_event(sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:queue:updated") => {
                let queue = event["data"]["queue"].clone();
                let finished = done(&queue);
                seen.push(queue);
                if finished {
                    return seen;
                }
            }
            Some("agent:stream:end") => {
                panic!("busy turn ended before the enqueue phase completed: {seen:?}")
            }
            _ => {}
        }
    }
    panic!("never observed the awaited agent:queue:updated snapshot: {seen:?}")
}

/// Two workspace participants share queue snapshots and processing events.
/// Consecutive owner submissions merge across both enqueue entry points;
/// a held edit preserves a concurrent append. Different authors stay separate
/// and direct RPC mutations remain restricted before the batch drain.
#[tokio::test]
async fn two_members_see_shared_queue_and_flush_combines_both_over_wss() {
    let Some(script) = gate("WSS two-member queue visibility + flush E2E") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let (ws_id, guest) = seed_workspace_with_guest(&data_dir).await;
    // Two connections' worth of enqueue reads, pushes and refusals must all
    // land before the kick-off turn ends (an early end panics in
    // `await_queue_snapshots`), so the busy window is a barrier the test
    // releases before (4) rather than a timer host scheduling could outrun.
    let kickoff_release = data_dir.join("release-kickoff");
    let Booted {
        daemon: _daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(&data_dir, &script, 0, Some(&kickoff_release), &[]).await;

    // Both members subscribe to `agent:*` BEFORE the kick-off send.
    let mut owner_sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut owner_sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "owner subscribed: {sub_resp}"
    );
    let mut guest_sub = connect_ws_as(port, cfg.clone(), GUEST_TOKEN).await;
    let sub_resp = wss_rpc(
        &mut guest_sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "guest subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut guest_rpc = connect_ws_as(port, cfg.clone(), GUEST_TOKEN).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-FLUSH-2M", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": KICKOFF_MSG }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    assert_eq!(
        sent["queued"], false,
        "kick-off streams, not queued: {sent}"
    );

    // Owner queues first, guest second.
    let owner_q = wss_rpc(
        &mut rpc,
        12,
        "agent.queueMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": OWNER_QUEUED, "messageId":"owner-original" }),
    )
    .await;
    assert_eq!(owner_q["success"], true, "owner queue: {owner_q}");
    let owner_id = owner_q["queuedMessage"]["id"]
        .as_str()
        .expect("owner entry id")
        .to_string();
    // Both queueMessage and busy sendMessage append to the same durable row.
    let appended = wss_rpc(
        &mut rpc,
        120,
        "agent.queueMessage",
        json!({"agentId":agent_id,"content":"second owner submission","messageId":"owner-second","messageMetadata":{"submissionIds":["forged"],"recoverySources":[{}]}}),
    )
    .await;
    assert_eq!(appended["queuedMessage"]["id"], owner_id);
    assert_eq!(appended["turnId"], owner_q["turnId"]);
    assert_eq!(
        appended["queuedMessage"]["submissionIds"],
        json!(["owner-second", "owner-original"])
    );
    assert!(appended["queuedMessage"]["messageMetadata"]
        .get("recoverySources")
        .is_none());
    assert!(appended["queuedMessage"]["messageMetadata"]
        .get("submissionIds")
        .is_none());
    let hello = wss_rpc(
        &mut rpc,
        900,
        "client.hello",
        json!({"clientId":"correlation-client"}),
    )
    .await;
    assert_eq!(hello["server"]["capabilities"]["submissionCorrelation"], 1);
    assert_eq!(hello["protocolVersion"], intent_transport::PROTOCOL_VERSION);
    assert_eq!(
        hello["server"]["protocolVersion"],
        intent_transport::PROTOCOL_VERSION
    );
    for (index, invalid) in [json!(null), json!(""), json!(123), json!([])]
        .into_iter()
        .enumerate()
    {
        let result = wss_rpc_envelope(
            &mut rpc,
            901 + i64::try_from(index).unwrap(),
            "agent.queueMessage",
            json!({"agentId":agent_id,"content":"invalid","messageId":invalid}),
        )
        .await;
        assert_eq!(result["jsonrpc"], "2.0");
        assert_eq!(result["error"]["code"], -32602);
    }
    let foreign = wss_rpc_envelope(
        &mut guest_rpc,
        906,
        "agent.queueMessage",
        json!({"agentId":agent_id,"content":"forged replay","messageId":"owner-second"}),
    )
    .await;
    assert_eq!(foreign["error"]["code"], -32602);
    let replay = wss_rpc(
        &mut rpc,
        907,
        "agent.queueMessage",
        json!({"agentId":agent_id,"content":"must not append","messageId":"owner-second"}),
    )
    .await;
    assert_eq!(replay["queuedMessage"], appended["queuedMessage"]);
    assert_eq!(appended["queuedMessage"]["position"], 0);
    assert_eq!(
        appended["queuedMessage"]["content"],
        format!("{OWNER_QUEUED}\n\nsecond owner submission")
    );
    let busy = wss_rpc(&mut rpc, 121, "agent.sendMessage",
        json!({"workspaceId":ws_id,"agentId":agent_id,"content":"busy owner submission","priority":"queue","messageId":"stable-busy-submission","messageMetadata":{"mergedMessageMetadata":[{"type":"question_answers","answeredQuestionsMessageId":"q1","fromPrincipalId":"spoofed"},{"type":"question_answers","answeredQuestionsMessageId":"q2","fromAgentId":"spoofed"}]}})).await;
    assert_eq!(busy["queued"], true);
    assert_eq!(busy["queuedMessage"]["id"], owner_id);
    assert_eq!(busy["turnId"], owner_q["turnId"]);
    assert_eq!(
        busy["submissionIds"],
        busy["queuedMessage"]["submissionIds"]
    );
    let contributions = busy["queuedMessage"]["messageMetadata"]["mergedMessageMetadata"]
        .as_array()
        .unwrap();
    for question in ["q1", "q2"] {
        let contribution = contributions
            .iter()
            .find(|entry| entry["answeredQuestionsMessageId"] == question)
            .unwrap();
        assert_eq!(
            contribution["fromPrincipalId"],
            owner_q["queuedMessage"]["messageMetadata"]["fromPrincipalId"]
        );
        assert!(contribution.get("fromAgentId").is_none());
    }
    assert_eq!(
        busy["queuedMessage"]["content"],
        format!("{OWNER_QUEUED}\n\nsecond owner submission\n\nbusy owner submission")
    );
    let retry = wss_rpc(&mut rpc, 122, "agent.sendMessage",
        json!({"workspaceId":ws_id,"agentId":agent_id,"content":"busy owner submission","priority":"queue","messageId":"stable-busy-submission","messageMetadata":{"mergedMessageMetadata":[{"type":"question_answers","answeredQuestionsMessageId":"q1","fromPrincipalId":"spoofed"},{"type":"question_answers","answeredQuestionsMessageId":"q2","fromAgentId":"spoofed"}]}})).await;
    assert_eq!(
        retry["queuedMessage"], busy["queuedMessage"],
        "retry must not append twice"
    );
    wss_rpc(
        &mut rpc,
        123,
        "agent.editQueuedMessage",
        json!({"agentId":agent_id,"messageId":owner_id,"content":OWNER_QUEUED,"editing":true}),
    )
    .await;
    let held = wss_rpc(
        &mut rpc,
        124,
        "agent.queueMessage",
        json!({"agentId":agent_id,"content":"held append"}),
    )
    .await;
    assert_eq!(held["queuedMessage"]["id"], owner_id);
    assert_eq!(held["queuedMessage"]["editing"], true);
    let saved = wss_rpc(
        &mut rpc,
        125,
        "agent.editQueuedMessage",
        json!({"agentId":agent_id,"messageId":owner_id,"content":OWNER_QUEUED,"editing":false}),
    )
    .await;
    assert_eq!(
        saved["queuedMessage"]["content"],
        format!(
            "{OWNER_QUEUED}\n\nsecond owner submission\n\nbusy owner submission\n\nheld append"
        )
    );
    // Restore the text so the existing full drain assertions stay precise.
    wss_rpc(
        &mut rpc,
        126,
        "agent.editQueuedMessage",
        json!({"agentId":agent_id,"messageId":owner_id,"content":OWNER_QUEUED}),
    )
    .await;

    let guest_q = wss_rpc(
        &mut guest_rpc,
        100,
        "agent.queueMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": GUEST_QUEUED }),
    )
    .await;
    assert_eq!(guest_q["success"], true, "guest queue: {guest_q}");
    let guest_id = guest_q["queuedMessage"]["id"]
        .as_str()
        .expect("guest entry id")
        .to_string();
    assert_ne!(owner_id, guest_id, "distinct entry ids");

    let after_guest = wss_rpc(
        &mut rpc,
        908,
        "agent.queueMessage",
        json!({"agentId":agent_id,"content":OWNER_QUEUED,"messageId":"owner-after-guest"}),
    )
    .await;
    assert_ne!(
        after_guest["queuedMessage"]["id"], owner_id,
        "foreign arrival splits even identical text"
    );
    let split = wss_rpc(&mut rpc, 909, "agent.getQueue", json!({"agentId":agent_id})).await;
    assert_eq!(
        queue_ids(&split["queue"]),
        vec![
            owner_id.clone(),
            guest_id.clone(),
            "owner-after-guest".into()
        ]
    );
    assert_eq!(split["queue"][0]["mergeEligible"], false);
    assert_eq!(split["queue"][1]["mergeEligible"], false);
    assert_eq!(split["queue"][2]["mergeEligible"], true);
    wss_rpc(
        &mut rpc,
        910,
        "agent.removeQueuedMessage",
        json!({"agentId":agent_id,"messageId":"owner-after-guest"}),
    )
    .await;

    // (1) agent.getQueue: owner sees both; guest sees only its own.
    let owner_view = wss_rpc(
        &mut rpc,
        13,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        queue_ids(&owner_view["queue"]),
        vec![owner_id.clone(), guest_id.clone()],
        "owner reads the full queue in order: {owner_view}"
    );
    let entries = owner_view["queue"].as_array().expect("queue array");
    assert_eq!(entries[0]["mergeEligible"], false);
    assert_eq!(entries[1]["mergeEligible"], true);
    assert_eq!(entries[0]["position"], json!(0), "{owner_view}");
    assert_eq!(entries[1]["position"], json!(1), "{owner_view}");
    assert_eq!(entries[0]["content"], json!(OWNER_QUEUED), "{owner_view}");
    let owner_principal_id = entries[0]["author"]["principalId"]
        .as_str()
        .unwrap_or_else(|| panic!("owner entry resolves its author: {owner_view}"))
        .to_string();
    assert_ne!(
        owner_principal_id, guest.id.0,
        "owner entry is authored by the administrator: {owner_view}"
    );
    assert_eq!(
        entries[1]["author"]["principalId"],
        json!(guest.id.0),
        "guest entry is authored by the guest: {owner_view}"
    );
    assert_eq!(
        entries[1]["author"]["login"],
        json!("guest"),
        "{owner_view}"
    );
    let guest_content = entries[1]["content"].as_str().expect("guest content");
    assert!(
        guest_content.starts_with(GUEST_PREAMBLE) && guest_content.ends_with(GUEST_QUEUED),
        "guest entry carries the sender preamble above its body: {guest_content}"
    );

    let guest_view = wss_rpc(
        &mut guest_rpc,
        101,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        queue_ids(&guest_view["queue"]),
        vec![owner_id.clone(), guest_id.clone()],
        "guest reads the shared queue: {guest_view}"
    );
    let guest_entry = &guest_view["queue"][1];
    assert_eq!(
        guest_entry["position"],
        json!(1),
        "position is not renumbered for the projected view: {guest_view}"
    );
    assert_eq!(guest_entry["author"]["principalId"], json!(guest.id.0));

    // (2) agent:queue:updated pushes: full snapshots to the owner, projected
    // ones to the guest (the owner's entry never appears).
    let owner_pushes = await_queue_snapshots(&mut owner_sub, &agent_id, |q| {
        queue_ids(q) == [owner_id.clone(), guest_id.clone()]
    })
    .await;
    assert!(
        owner_pushes
            .iter()
            .any(|q| queue_ids(q) == [owner_id.clone()]),
        "owner sees the 1-entry snapshot before the 2-entry one: {owner_pushes:?}"
    );
    assert!(
        owner_pushes.iter().any(|queue| {
            queue.as_array().is_some_and(|entries| entries.len() == 1)
                && queue[0]["id"] == owner_id
                && queue[0]["content"]
                    == format!("{OWNER_QUEUED}\n\nsecond owner submission\n\nbusy owner submission")
        }),
        "queue:updated publishes the full merged survivor: {owner_pushes:?}"
    );
    let guest_pushes = await_queue_snapshots(&mut guest_sub, &agent_id, |q| {
        queue_ids(q).contains(&guest_id)
    })
    .await;
    assert!(
        guest_pushes
            .iter()
            .all(|q| queue_ids(q).contains(&owner_id)),
        "the owner's entry is shared with the guest: {guest_pushes:?}"
    );
    let guest_last = guest_pushes.last().expect("guest saw a snapshot");
    assert_eq!(
        queue_ids(guest_last),
        vec![owner_id.clone(), guest_id.clone()],
        "guest's snapshot includes both entries: {guest_last}"
    );
    assert_eq!(
        guest_last[1]["position"],
        json!(1),
        "projected push keeps position 1: {guest_last}"
    );

    // Consume the repeated-text barrier and its removal before observing drain.
    for sub in [&mut owner_sub, &mut guest_sub] {
        let split_pushes = await_queue_snapshots(sub, &agent_id, |q| {
            queue_ids(q).contains(&"owner-after-guest".to_string())
        })
        .await;
        assert_eq!(split_pushes.last().unwrap()[2]["mergeEligible"], true);
        let restored = await_queue_snapshots(sub, &agent_id, |q| {
            queue_ids(q) == [owner_id.clone(), guest_id.clone()]
        })
        .await;
        assert_eq!(restored.last().unwrap()[1]["mergeEligible"], true);
    }

    // (3) Ownership refusals — none publishes a snapshot.
    let owner_edit = wss_rpc_envelope(
        &mut rpc,
        14,
        "agent.editQueuedMessage",
        json!({ "agentId": agent_id, "messageId": guest_id, "content": "owner rewrite" }),
    )
    .await;
    assert_eq!(owner_edit["jsonrpc"], "2.0", "{owner_edit}");
    assert!(owner_edit.get("result").is_none(), "{owner_edit}");
    assert_eq!(owner_edit["error"]["code"], -32602, "{owner_edit}");
    assert!(
        owner_edit["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("can only be edited by its author")),
        "owner cannot edit the guest's entry: {owner_edit}"
    );
    let guest_edit = wss_rpc_envelope(
        &mut guest_rpc,
        102,
        "agent.editQueuedMessage",
        json!({ "agentId": agent_id, "messageId": owner_id, "content": "guest rewrite" }),
    )
    .await;
    assert_eq!(guest_edit["error"]["code"], -32602, "{guest_edit}");
    assert!(
        guest_edit["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("queued message not found")),
        "the owner's entry is invisible to the guest's edit: {guest_edit}"
    );
    let guest_remove = wss_rpc_envelope(
        &mut guest_rpc,
        103,
        "agent.removeQueuedMessage",
        json!({ "agentId": agent_id, "messageId": owner_id }),
    )
    .await;
    assert!(guest_remove.get("result").is_none(), "{guest_remove}");
    assert_eq!(guest_remove["error"]["code"], -32602, "{guest_remove}");
    assert!(
        guest_remove["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("queued message not found")),
        "the owner's entry is invisible to the guest's remove: {guest_remove}"
    );
    let owner_after = wss_rpc(
        &mut rpc,
        15,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        owner_after["queue"], owner_view["queue"],
        "refused edits/removes leave the queue untouched"
    );
    let guest_after = wss_rpc(
        &mut guest_rpc,
        104,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        guest_after["queue"], guest_view["queue"],
        "refused edits/removes leave the guest's view untouched"
    );

    // (4) Flush intact — observed on both subscriptions: kick-off
    // stream:end, then the ONE combined flush turn. Everything the busy
    // window had to cover is done; let the kick-off turn end.
    std::fs::write(&kickoff_release, b"go").expect("write kick-off release file");
    let (owner_obs, guest_obs) = tokio::join!(
        observe_drain(&mut owner_sub, &agent_id, 2),
        observe_drain(&mut guest_sub, &agent_id, 2),
    );
    // Each persisted row publishes the full draining overlay before the
    // guard retires the batch in one step. No partial queue is published.
    assert_eq!(
        owner_obs.queue_lengths,
        vec![2, 2, 0],
        "owner: both draining entries remain visible until retirement"
    );
    assert_eq!(
        guest_obs.queue_lengths,
        vec![2, 2, 0],
        "guest: both draining entries remain visible until retirement"
    );
    assert_eq!(
        owner_obs.processing_turn_ids.len(),
        1,
        "exactly ONE agent:queue:processing for the combined turn: {:?}",
        owner_obs.processing_turn_ids
    );
    assert_eq!(
        guest_obs.processing_turn_ids, owner_obs.processing_turn_ids,
        "the guest observes the same single combined turn"
    );
    let combined_turn_id = &owner_obs.processing_turn_ids[0];
    // The drain-start frame is keyed on the batch head — the owner's entry.
    // All participants receive the same content and surviving identity.
    let owner_processing = &owner_obs.processing_frames[0];
    for (row, original) in owner_processing["queuedMessages"]
        .as_array()
        .unwrap()
        .iter()
        .zip(entries)
    {
        assert_eq!(row["submissionIds"], original["submissionIds"]);
        assert_eq!(row["mergeEligible"], false);
    }
    for original in entries {
        let delivered = owner_obs
            .user_frames
            .iter()
            .find(|frame| frame["queuedMessageId"] == original["id"])
            .unwrap();
        assert_eq!(delivered["submissionIds"], original["submissionIds"]);
    }
    assert_eq!(
        owner_processing["messageId"],
        json!(owner_id),
        "processing keys on the head entry: {owner_processing}"
    );
    assert!(
        owner_processing["content"]
            .as_str()
            .is_some_and(|c| c.starts_with(OWNER_QUEUED)),
        "the owner sees its own entry's content (plus the dequeue-wait note): {owner_processing}"
    );
    assert_eq!(
        guest_obs.processing_frames[0], *owner_processing,
        "all participants receive the same processing content"
    );
    for obs in [&owner_obs, &guest_obs] {
        let linked: Vec<&String> = obs.user_row_queued_message_ids.iter().flatten().collect();
        assert_eq!(
            linked,
            vec![&owner_id, &guest_id],
            "flushed echoes link both entries in queue order: {:?}",
            obs.user_row_queued_message_ids
        );
        assert!(
            obs.user_row_turn_ids
                .ends_with(&[combined_turn_id.clone(), combined_turn_id.clone()]),
            "both flushed echoes carry the combined turn's turnId: {:?}",
            obs.user_row_turn_ids
        );
    }

    let prompts = await_prompts(&prompt_log, 2).await;
    assert_eq!(prompts.len(), 2, "kick-off + ONE flush turn: {prompts:?}");
    let flush = &prompts[1];
    assert!(
        flush.starts_with(FLUSH_HEADER),
        "flush prompt starts with the batch header: {flush}"
    );
    let i_owner = flush
        .find(OWNER_QUEUED)
        .unwrap_or_else(|| panic!("flush prompt carries {OWNER_QUEUED:?}: {flush}"));
    let i_guest = flush
        .find(GUEST_QUEUED)
        .unwrap_or_else(|| panic!("flush prompt carries {GUEST_QUEUED:?}: {flush}"));
    assert!(i_owner < i_guest, "entries appear in queue order: {flush}");
    assert!(
        flush.contains(GUEST_PREAMBLE),
        "the guest's sender preamble survives into the combined prompt: {flush}"
    );

    let conv = wss_rpc(
        &mut rpc,
        20,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let owner_row = user_row(&conv, OWNER_QUEUED);
    let guest_row = conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .find(|m| {
            m["contentBlocks"][0]["text"]
                .as_str()
                .is_some_and(|t| t.starts_with(GUEST_PREAMBLE) && t.contains(GUEST_QUEUED))
        })
        .unwrap_or_else(|| panic!("missing guest user row: {conv}"));
    assert_eq!(
        owner_row["metadata"]["submissionIds"],
        entries[0]["submissionIds"]
    );
    assert_eq!(
        guest_row["metadata"]["submissionIds"],
        entries[1]["submissionIds"]
    );
    let batch_id = owner_row["metadata"]["queueInfo"]["batchId"]
        .as_str()
        .expect("flushed row carries queueInfo.batchId");
    assert_eq!(
        guest_row["metadata"]["queueInfo"]["batchId"].as_str(),
        Some(batch_id),
        "both members' rows share the batch's id"
    );
    assert_eq!(
        owner_row["metadata"]["queueInfo"]["queuedMessageId"],
        json!(owner_id)
    );
    assert_eq!(
        guest_row["metadata"]["queueInfo"]["queuedMessageId"],
        json!(guest_id)
    );
    assert_eq!(
        guest_row["metadata"]["fromPrincipalId"],
        json!(guest.id.0),
        "guest row keeps its principal stamp: {guest_row}"
    );
    assert_eq!(
        owner_row["metadata"]["fromPrincipalId"],
        json!(owner_principal_id),
        "owner row keeps its principal stamp: {owner_row}"
    );

    for (ws, id) in [(&mut rpc, 21), (&mut guest_rpc, 105)] {
        let queue = wss_rpc(ws, id, "agent.getQueue", json!({ "agentId": agent_id })).await;
        assert!(
            queue["queue"].as_array().expect("queue array").is_empty(),
            "queue empty after flush: {queue}"
        );
    }
}

/// Entry ids of one `agent.diagnostics` `queues[]` row, in drain order.
fn diagnostics_entry_ids(queue_row: &Value) -> Vec<String> {
    queue_ids(&queue_row["entries"])
}

/// FLUSH-5 (`agent.diagnostics`, two members): the owner (administrator)
/// and a collaborator (`guest`) each queue ONE entry behind the target's
/// busy turn, and a sibling agent queues a third via `ws.agent.send(…,
/// 'queue')` — the agent-sent tier no wire caller can forge. `queues[]` is
/// projected to the bound caller exactly like `agent.getQueue`
/// (`intent_core::project_queue_for_caller`), and the wire reaches it only
/// for the administrator:
///
/// 1. As the guest: `agent.diagnostics` is outside the collaborator
///    allowlist (`COLLABORATOR_METHODS`), so the envelope is
///    `{ jsonrpc: "2.0", id, error: { code: -32003, message: "Forbidden" } }`
///    with no `result` — the transport refuses before the router runs. The
///    guest-side projection (own + agent-sent entries, owner's hidden) is
///    the services harness's cell, not a WSS-observable one.
/// 2. As the owner: `{ jsonrpc: "2.0", id, result }` (no `error`),
///    `result.ok` is true, `queues` holds ONE row for the target whose
///    `entries` are all three in drain order (`[owner, guest, agent-sent]`)
///    with each entry's attribution intact, `queueLength` 3 and
///    `summary.queuedAgents` 1.
#[tokio::test]
async fn diagnostics_projects_queues_per_caller_over_wss() {
    diagnostics_queue_projection(drop, true).await;
}

#[tokio::test]
async fn diagnostics_fixture_preserves_error_after_teardown() {
    diagnostics_queue_projection(
        |daemon| {
            fn fail(daemon: Daemon) -> Result<(), &'static str> {
                let _daemon = daemon;
                Err("deliberate diagnostic fixture error")
            }
            assert_eq!(fail(daemon), Err("deliberate diagnostic fixture error"));
        },
        false,
    )
    .await;
}

#[tokio::test]
async fn diagnostics_fixture_preserves_panic_after_teardown() {
    diagnostics_queue_projection(
        |daemon| {
            let outcome = std::panic::catch_unwind(|| {
                let _daemon = daemon;
                panic!("deliberate diagnostic fixture panic");
            });
            assert_eq!(
                outcome.unwrap_err().downcast_ref::<&str>(),
                Some(&"deliberate diagnostic fixture panic")
            );
        },
        false,
    )
    .await;
}

async fn diagnostics_queue_projection(teardown: impl FnOnce(Daemon), release_kickoff: bool) {
    let Some(script) = gate("WSS agent.diagnostics queue projection E2E") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let _retention = common::suppress_failure_retention();
    let (ws_id, guest) = seed_workspace_with_guest(&data_dir).await;
    // The target's kick-off turn is a barrier (released at the end) so all
    // three enqueues and both diagnostics reads land inside the busy window.
    let kickoff_release = data_dir.join("release-kickoff");
    let relay_code = format!(
        "const agents = await ws.agent.list(true); \
         const target = agents.find(a => a.name === 'WSS-DIAG-TARGET'); \
         return await ws.agent.send(target.id, '{AGENT_QUEUED}', 'queue');"
    );
    let relay_rule = json!({
        "ifPromptContains": RELAY_MARKER,
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": relay_code, "summary": "sibling queues on the busy target" }
        },
        "response": "relay dispatched"
    });
    let Booted {
        daemon,
        port,
        cfg,
        prompt_log: _,
    } = boot_daemon(&data_dir, &script, 0, Some(&kickoff_release), &[relay_rule]).await;

    // The owner's subscription observes the target's queue growing so the
    // diagnostics reads run only once all three entries are parked.
    let mut owner_sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut owner_sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "owner subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut guest_rpc = connect_ws_as(port, cfg.clone(), GUEST_TOKEN).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-DIAG-TARGET", "model": "default", "provider": "mock" }),
    )
    .await;
    let target_id = created["agent"]["id"]
        .as_str()
        .expect("target agent id")
        .to_string();
    let created = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-DIAG-SENDER", "model": "default", "provider": "mock" }),
    )
    .await;
    let sender_id = created["agent"]["id"]
        .as_str()
        .expect("sender agent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": target_id, "content": KICKOFF_MSG }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    assert_eq!(
        sent["queued"], false,
        "kick-off streams, not queued: {sent}"
    );

    // (a) owner-stamped, (b) guest-stamped, (c) agent-sent — in that order.
    let owner_q = wss_rpc(
        &mut rpc,
        13,
        "agent.queueMessage",
        json!({ "workspaceId": ws_id, "agentId": target_id, "content": OWNER_QUEUED }),
    )
    .await;
    assert_eq!(owner_q["success"], true, "owner queue: {owner_q}");
    let owner_id = owner_q["queuedMessage"]["id"]
        .as_str()
        .expect("owner entry id")
        .to_string();
    let guest_q = wss_rpc(
        &mut guest_rpc,
        100,
        "agent.queueMessage",
        json!({ "workspaceId": ws_id, "agentId": target_id, "content": GUEST_QUEUED }),
    )
    .await;
    assert_eq!(guest_q["success"], true, "guest queue: {guest_q}");
    let guest_id = guest_q["queuedMessage"]["id"]
        .as_str()
        .expect("guest entry id")
        .to_string();
    let relayed = wss_rpc(
        &mut rpc,
        14,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": sender_id, "content": RELAY_MARKER }),
    )
    .await;
    assert_eq!(relayed["success"], true, "sender kick-off ok: {relayed}");
    let grown =
        await_queue_snapshots(&mut owner_sub, &target_id, |q| queue_ids(q).len() == 3).await;
    let full = grown.last().expect("three-entry snapshot");
    let full_ids = queue_ids(full);
    assert_eq!(
        &full_ids[..2],
        [owner_id.clone(), guest_id.clone()],
        "{full}"
    );
    let agent_entry_id = full_ids[2].clone();
    assert_eq!(
        full[2]["messageMetadata"]["fromAgentId"],
        json!(sender_id),
        "third entry is the sibling's agent-sent one: {full}"
    );
    assert!(
        full[2]["messageMetadata"].get("fromPrincipalId").is_none(),
        "agent-sent entry carries no principal stamp: {full}"
    );

    let diagnostics_params = json!({ "workspaceId": ws_id, "agentId": target_id });

    // (1) Guest: refused at the transport — `agent.diagnostics` is not a
    // collaborator method, so no projected view ever reaches the wire.
    let guest_env = wss_rpc_envelope(
        &mut guest_rpc,
        101,
        "agent.diagnostics",
        diagnostics_params.clone(),
    )
    .await;
    assert_eq!(guest_env["jsonrpc"], "2.0", "{guest_env}");
    assert_eq!(guest_env["id"], json!(101), "{guest_env}");
    assert!(guest_env.get("result").is_none(), "{guest_env}");
    assert_eq!(
        guest_env["error"],
        json!({ "code": -32003, "message": "Forbidden" }),
        "collaborator allowlist refuses agent.diagnostics: {guest_env}"
    );

    // (2) Owner: all three, in drain order, attribution intact.
    let owner_env = wss_rpc_envelope(&mut rpc, 15, "agent.diagnostics", diagnostics_params).await;
    assert_eq!(owner_env["jsonrpc"], "2.0", "{owner_env}");
    assert_eq!(owner_env["id"], json!(15), "{owner_env}");
    assert!(owner_env.get("error").is_none(), "{owner_env}");
    let result = &owner_env["result"];
    assert_eq!(result["ok"], true, "{result}");
    let queues = result["diagnostics"]["queues"]
        .as_array()
        .unwrap_or_else(|| panic!("queues array: {result}"));
    assert_eq!(queues.len(), 1, "one queued agent in scope: {result}");
    let owner_row = &queues[0];
    assert_eq!(owner_row["agentId"], json!(target_id), "{result}");
    assert_eq!(owner_row["agentName"], json!("WSS-DIAG-TARGET"), "{result}");
    assert_eq!(
        result["diagnostics"]["summary"]["queuedAgents"],
        json!(1),
        "{result}"
    );
    assert_eq!(
        diagnostics_entry_ids(owner_row),
        vec![owner_id, guest_id, agent_entry_id],
        "owner sees the full queue: {owner_row}"
    );
    assert_eq!(owner_row["queueLength"], json!(3), "{owner_row}");
    let entries = owner_row["entries"].as_array().expect("entries array");
    assert!(
        entries[0]["messageMetadata"]["fromPrincipalId"]
            .as_str()
            .is_some_and(|p| p != guest.id.0),
        "owner entry is stamped with the administrator's principal: {owner_row}"
    );
    assert_eq!(
        entries[1]["messageMetadata"]["fromPrincipalId"],
        json!(guest.id.0),
        "guest entry keeps its principal stamp: {owner_row}"
    );
    assert_eq!(
        entries[2]["messageMetadata"]["fromAgentId"],
        json!(sender_id),
        "agent-sent entry keeps its agent stamp: {owner_row}"
    );

    // Error and panic controls unwind while the provider is still parked.
    if release_kickoff {
        std::fs::write(&kickoff_release, b"go").expect("write kick-off release file");
    }
    teardown(daemon);
}

#[tokio::test]
async fn explicit_batch_validates_snapshot_and_sends_once_over_wss() {
    explicit_batch_over_wss(false).await;
}

#[tokio::test]
async fn explicit_batch_restores_partial_persistence_without_duplicate_rows_over_wss() {
    explicit_batch_over_wss(true).await;
}

async fn explicit_batch_over_wss(fail_second_append: bool) {
    let Some(script) = gate("WSS explicit queue batch") else {
        return;
    };
    let scratch = temp_data_dir();
    let data_dir = scratch.path();
    let (workspace_id, guest) = seed_workspace_with_guest(data_dir).await;
    let release = data_dir.join("release-kickoff");
    let Booted {
        daemon: _daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(data_dir, &script, 0, Some(&release), &[]).await;
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut guest_rpc = connect_ws_as(port, cfg.clone(), GUEST_TOKEN).await;
    let mut sub = connect_ws(port, cfg).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({"workspaceId":workspace_id,"eventTypes":["agent:*"]}),
    )
    .await;
    let created = wss_rpc(&mut rpc, 2, "agent.create", json!({"workspaceId":workspace_id,"name":"explicit-batch","model":"default","provider":"mock"})).await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();
    wss_rpc(
        &mut rpc,
        3,
        "agent.sendMessage",
        json!({"workspaceId":workspace_id,"agentId":agent_id,"content":KICKOFF_MSG}),
    )
    .await;
    await_prompts(&prompt_log, 1).await;
    let first = wss_rpc(
        &mut rpc,
        4,
        "agent.queueMessage",
        json!({"workspaceId":workspace_id,"agentId":agent_id,"content":QUEUED_ONE}),
    )
    .await;
    let second = wss_rpc(
        &mut guest_rpc,
        5,
        "agent.queueMessage",
        json!({"workspaceId":workspace_id,"agentId":agent_id,"content":QUEUED_TWO}),
    )
    .await;
    let first_id = first["queuedMessage"]["id"].as_str().unwrap().to_string();
    let second_id = second["queuedMessage"]["id"].as_str().unwrap().to_string();
    let selected = vec![first_id.clone(), second_id.clone()];
    for ids in [
        json!([]),
        json!([first_id, first_id]),
        json!([first_id, "missing"]),
        json!([first_id, 3]),
    ] {
        let response = wss_rpc_envelope(
            &mut rpc,
            6,
            "agent.sendQueuedMessagesNow",
            json!({"workspaceId":workspace_id,"agentId":agent_id,"messageIds":ids}),
        )
        .await;
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 6);
        assert_eq!(response["error"]["code"], -32602, "{response}");
    }
    let forbidden = wss_rpc_envelope(
        &mut guest_rpc,
        7,
        "agent.sendQueuedMessagesNow",
        json!({"workspaceId":workspace_id,"agentId":agent_id,"messageIds":selected}),
    )
    .await;
    assert_eq!(
        forbidden["error"]["code"], -32602,
        "foreign entry rejects the whole batch: {forbidden}"
    );
    wss_rpc(
        &mut rpc,
        8,
        "agent.editQueuedMessage",
        json!({"agentId":agent_id,"messageId":first_id,"content":QUEUED_ONE,"editing":true}),
    )
    .await;
    let held = wss_rpc_envelope(
        &mut rpc,
        9,
        "agent.sendQueuedMessagesNow",
        json!({"workspaceId":workspace_id,"agentId":agent_id,"messageIds":selected}),
    )
    .await;
    assert_eq!(held["error"]["code"], -32602, "{held}");
    wss_rpc(
        &mut rpc,
        10,
        "agent.editQueuedMessage",
        json!({"agentId":agent_id,"messageId":first_id,"content":QUEUED_ONE,"editing":false}),
    )
    .await;
    let later = wss_rpc(
        &mut rpc,
        11,
        "agent.queueMessage",
        json!({"agentId":agent_id,"content":"later held entry"}),
    )
    .await;
    let later_id = later["queuedMessage"]["id"].as_str().unwrap().to_string();
    wss_rpc(&mut rpc, 12, "agent.editQueuedMessage", json!({"agentId":agent_id,"messageId":later_id,"content":"later held entry","editing":true})).await;
    // Monitor wakes retain individual lifecycle admission even for explicit sends.
    let protected = wss_rpc(&mut guest_rpc, 120, "agent.queueMessage", json!({
        "agentId":agent_id, "content":"protected monitor wake",
        "messageMetadata":{"type":"script_monitor_wake","monitorId":"batch-protected-monitor","workspaceId":workspace_id}
    })).await;
    let protected_id = protected["queuedMessage"]["id"].as_str().unwrap();
    let refused = wss_rpc_envelope(
        &mut rpc,
        121,
        "agent.sendQueuedMessagesNow",
        json!({
            "workspaceId":workspace_id,"agentId":agent_id,"messageIds":[first_id,protected_id]
        }),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    wss_rpc(
        &mut rpc,
        122,
        "agent.removeQueuedMessage",
        json!({"agentId":agent_id,"messageId":protected_id}),
    )
    .await;
    let queue = wss_rpc(&mut rpc, 13, "agent.getQueue", json!({"agentId":agent_id})).await;
    assert_eq!(
        queue_ids(&queue["queue"]),
        vec![first_id.clone(), second_id.clone(), later_id.clone()]
    );
    assert_eq!(
        std::fs::read_to_string(&prompt_log)
            .unwrap()
            .lines()
            .count(),
        1,
        "rejections never preempt"
    );
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .unwrap();
    if !fail_second_append {
        sqlx::query("UPDATE agent_session SET status='error', stop_reason='The model provider blocked this response for safety reasons' WHERE id=?")
            .bind(&agent_id).execute(store.write_pool()).await.unwrap();
        let quarantined = wss_rpc(
            &mut rpc,
            131,
            "agent.sendQueuedMessagesNow",
            json!({"workspaceId":workspace_id,"agentId":agent_id,"messageIds":selected}),
        )
        .await;
        assert_eq!(
            quarantined,
            json!({"success":true,"queued":true,"quarantined":true,"messageIds":selected})
        );
        let preserved = wss_rpc(&mut rpc, 132, "agent.getQueue", json!({"agentId":agent_id})).await;
        assert_eq!(preserved, queue, "quarantine leaves every entry untouched");
        sqlx::query("UPDATE agent_session SET status='active', stop_reason=NULL WHERE id=?")
            .bind(&agent_id)
            .execute(store.write_pool())
            .await
            .unwrap();
    }
    if fail_second_append {
        sqlx::query("CREATE TRIGGER fail_explicit_batch BEFORE INSERT ON agent_message WHEN NEW.role='user' AND (SELECT COUNT(*) FROM agent_message WHERE role='user') >= 2 BEGIN SELECT RAISE(ABORT, 'injected second batch append failure'); END")
            .execute(store.write_pool()).await.unwrap();
    }
    let response = wss_rpc_envelope(
        &mut rpc,
        14,
        "agent.sendQueuedMessagesNow",
        json!({"workspaceId":workspace_id,"agentId":agent_id,"messageIds":[second_id,first_id]}),
    )
    .await;
    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 14);
    assert_eq!(response["result"]["success"], true, "{response}");
    assert_eq!(
        response["result"]["messageIds"],
        json!(selected),
        "queue order wins over request order"
    );
    assert_eq!(
        response["result"]["queued"], fail_second_append,
        "{response}"
    );
    if fail_second_append {
        let restored = wss_rpc(&mut rpc, 15, "agent.getQueue", json!({"agentId":agent_id})).await;
        assert_eq!(
            queue_ids(&restored["queue"]),
            vec![first_id.clone(), second_id.clone(), later_id.clone()],
            "{restored}"
        );
        sqlx::query("DROP TRIGGER fail_explicit_batch")
            .execute(store.write_pool())
            .await
            .unwrap();
        let retry = wss_rpc(
            &mut rpc,
            16,
            "agent.sendQueuedMessagesNow",
            json!({"workspaceId":workspace_id,"agentId":agent_id,"messageIds":selected}),
        )
        .await;
        assert_eq!(retry["queued"], false, "{retry}");
    } else {
        assert!(response["result"]["turnId"].is_string(), "{response}");
        let mut echoes = Vec::new();
        let mut processing = Vec::new();
        loop {
            let frame = wss_event(&mut sub, 30).await;
            let event = &frame["params"]["event"];
            if event["data"]["agentId"] != agent_id {
                continue;
            }
            match event["type"].as_str() {
                Some("agent:queue:processing") => processing.push(event["data"]["turnId"].clone()),
                Some("agent:message") if event["data"]["role"] == "user" => {
                    if selected
                        .iter()
                        .any(|id| event["data"]["queuedMessageId"] == *id)
                    {
                        echoes.push(event["data"]["turnId"].clone());
                    }
                }
                Some("agent:queue:updated")
                    if queue_ids(&event["data"]["queue"]) == vec![later_id.clone()] =>
                {
                    break
                }
                _ => {}
            }
        }
        assert_eq!(processing, vec![response["result"]["turnId"].clone()]);
        assert_eq!(
            echoes,
            vec![response["result"]["turnId"].clone(); 2],
            "row echoes precede the single shrink"
        );
    }
    let prompts = await_prompts(&prompt_log, 2).await;
    assert_eq!(prompts.len(), 2, "one explicit batch prompt: {prompts:?}");
    assert!(prompts[1].find(QUEUED_ONE).unwrap() < prompts[1].find(QUEUED_TWO).unwrap());
    assert!(prompts[1].contains(FLUSH_HEADER));
    let remaining = wss_rpc(&mut rpc, 17, "agent.getQueue", json!({"agentId":agent_id})).await;
    assert_eq!(queue_ids(&remaining["queue"]), vec![later_id]);
    let conv = wss_rpc(
        &mut rpc,
        18,
        "agent.getConversation",
        json!({"agentId":agent_id}),
    )
    .await;
    let texts = user_row_texts(&conv);
    assert_eq!(texts.iter().filter(|s| s.contains(QUEUED_ONE)).count(), 1);
    assert_eq!(texts.iter().filter(|s| s.contains(QUEUED_TWO)).count(), 1);
    assert_eq!(
        user_row(&conv, GUEST_PREAMBLE)["author"]["principalId"],
        guest.id.0
    );
    let stale = wss_rpc_envelope(
        &mut rpc,
        19,
        "agent.sendQueuedMessagesNow",
        json!({"workspaceId":workspace_id,"agentId":agent_id,"messageIds":selected}),
    )
    .await;
    assert_eq!(stale["error"]["code"], -32602);
    std::fs::write(&release, "release").unwrap();
    eprintln!(
        "EXPLICIT_BATCH_EVIDENCE {}",
        json!({"partialPersistence":fail_second_append,"response":response,"prompts":prompts,"remainingQueue":remaining,"conversation":conv})
    );
}

/// An agent reads its own queue through real MCP while humans read it over
/// WSS, then ends its turn so the original entries drain normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_queue_reads_do_not_reveal_or_consume_pending_messages_over_wss() {
    let Some(script) = gate("WSS self queue visibility E2E") else {
        return;
    };
    let tmp = temp_data_dir();
    let data_dir = tmp.path();
    let (ws_id, _) = seed_workspace_with_guest(data_dir).await;
    let release = data_dir.join("release-self-read");
    let code = r"
        const secret = ['PENDING', 'WSS', 'SECRET'].join('-');
        const target = (await ws.agent.list(true)).find(a => a.name === 'SELF-QUEUE-READER');
        const status = await ws.agent.status(target.id);
        const queue = await ws.agent.getQueue(target.id);
        const diagnostics = await ws.agent.diagnostics();
        const filtered = await ws.agent.diagnostics({agentId: target.id});
        const events = await ws.event.query({eventType: 'agent:queue:*', paginate: true});
        const activity = await ws.event.agentActivity(target.id);
        const output = JSON.stringify({status, queue, diagnostics, filtered, events, activity});
        if (output.includes(secret)) throw new Error('self queue leaked: ' + output);
        if (status.queueLength !== 2 || status.queue.length !== 0 || queue.queueLength !== 2 || !queue.refused)
            throw new Error('self counts/refusal wrong: ' + output);
        const row = diagnostics.diagnostics.queues.find(q => q.agentId === target.id);
        if (row.queueLength !== 2 || row.entries.length !== 0) throw new Error('diagnostics wrong');
        if (!status.queueNotice.includes('after the current turn') || !diagnostics.text.includes('contents hidden'))
            throw new Error('missing visibility explanation');
        const hook = await ws.hook.schedule({name:'Self queue read probe', delayMs:10000, ttlMs:10000,
            code: `const q = await ws.agent.getQueue('${target.id}');
                const d = await ws.agent.diagnostics();
                const row = d.diagnostics.queues.find(q => q.agentId === '${target.id}');
                if (row.queueLength !== 2 || row.entries.length !== 0 || !d.text.includes('contents hidden'))
                    throw new Error('hook diagnostics lost count or visibility notice');
                if (d.diagnostics.stuckRisks.some(r => r.type === 'stale-queue-entry' && r.agentId === '${target.id}'))
                    throw new Error('fresh active owner queue incorrectly flagged stale');
                const e = await ws.event.query({eventType:'agent:queue:*'});
                if (!q.refused || q.queueLength !== 2 || JSON.stringify({q,d,e}).includes(['PENDING','WSS','SECRET'].join('-')))
                    throw new Error('hook self queue leaked');
                return {dispatch:false};`});
        await ws.hook.cancel(hook.hook.hookId);
        return {__mcpContentItems:[{type:'text',text:JSON.stringify({proof:'SELF-QUEUE-READS-PASSED'})}]};
    ";
    let rule = json!({"ifPromptContains":KICKOFF_MSG,"releaseFile":release,
        "toolCall":{"name":"workspace_api","arguments":{"code":code,"summary":"Check self queue visibility"}},
        "responseFromToolResultField":"proof"});
    let delivered_rule = json!({
        "ifPromptContains":"PENDING-WSS-SECRET-owner",
        "toolCall":{"name":"workspace_api","arguments":{
            "code":"const a=(await ws.agent.list(true)).find(a=>a.name==='SELF-QUEUE-READER'); const c=await ws.agent.readConversation(a.id); if(!JSON.stringify(c).includes('PENDING-WSS-SECRET-owner')) throw new Error('delivered transcript hidden'); return {__mcpContentItems:[{type:'text',text:JSON.stringify({proof:'DELIVERED-TRANSCRIPT-READ-PASSED'})}]};",
            "summary":"Read the normally delivered message"
        }},
        "responseFromToolResultField":"proof"
    });
    let Booted {
        daemon: _daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(data_dir, &script, 0, None, &[rule, delivered_rule]).await;
    let mut sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({"workspaceId":ws_id,"eventTypes":["agent:*"]}),
    )
    .await;
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut guest = connect_ws_as(port, cfg, GUEST_TOKEN).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "agent.create",
        json!({"workspaceId":ws_id,"name":"SELF-QUEUE-READER","provider":"mock","model":"default"}),
    )
    .await;
    let agent = created["agent"]["id"].as_str().unwrap();
    let started = wss_rpc(
        &mut rpc,
        3,
        "agent.sendMessage",
        json!({"workspaceId":ws_id,"agentId":agent,"content":KICKOFF_MSG}),
    )
    .await;
    assert_eq!(started["queued"], false, "{started}");
    await_prompts(&prompt_log, 1).await;
    for (socket, content) in [
        (&mut rpc, "PENDING-WSS-SECRET-owner"),
        (&mut guest, "PENDING-WSS-SECRET-guest"),
    ] {
        let q = wss_rpc(
            socket,
            4,
            "agent.queueMessage",
            json!({"workspaceId":ws_id,"agentId":agent,"content":content}),
        )
        .await;
        assert_eq!(q["success"], true, "{q}");
    }
    for socket in [&mut rpc, &mut guest] {
        let envelope = wss_rpc_envelope(
            socket,
            5,
            "agent.getQueue",
            json!({"workspaceId":ws_id,"agentId":agent}),
        )
        .await;
        assert_eq!(envelope["jsonrpc"], "2.0");
        assert_eq!(envelope["id"], 5);
        let entries = envelope["result"]["queue"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["content"], "PENDING-WSS-SECRET-owner");
        let guest_content = entries[1]["content"].as_str().unwrap();
        assert!(
            guest_content.starts_with(GUEST_PREAMBLE)
                && guest_content.ends_with("PENDING-WSS-SECRET-guest"),
            "guest attribution remains intact: {guest_content}"
        );
    }
    std::fs::write(&release, "go").unwrap();
    let observed = observe_drain(&mut sub, agent, 2).await;
    assert_eq!(observed.processing_frames.len(), 1, "one batch delivered");
    assert_eq!(
        observed.processing_frames[0]["queuedMessages"]
            .as_array()
            .unwrap()
            .len(),
        2,
        "both original entries delivered"
    );
    let prompts = await_prompts(&prompt_log, 2).await;
    assert_eq!(prompts.len(), 2, "one initial turn and one queued batch");
    let batch = &prompts[1];
    assert!(
        batch.find("PENDING-WSS-SECRET-owner").unwrap()
            < batch.find("PENDING-WSS-SECRET-guest").unwrap()
    );
    for content in ["PENDING-WSS-SECRET-owner", "PENDING-WSS-SECRET-guest"] {
        assert_eq!(
            batch.matches(content).count(),
            1,
            "each entry delivered once"
        );
    }
    let conv = wss_rpc(
        &mut rpc,
        6,
        "agent.getConversation",
        json!({"agentId":agent}),
    )
    .await;
    assert!(
        conv["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["role"] == "assistant" && m.to_string().contains("SELF-QUEUE-READS-PASSED")),
        "MCP assertions completed: {conv}"
    );
    assert!(
        conv["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["role"] == "assistant"
                && m.to_string().contains("DELIVERED-TRANSCRIPT-READ-PASSED")),
        "agent can read its delivered transcript: {conv}"
    );
    assert_eq!(user_row(&conv, "PENDING-WSS-SECRET-owner")["role"], "user");
    assert!(
        conv["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["role"] == "user" && m.to_string().contains("PENDING-WSS-SECRET-guest")),
        "delivered guest transcript remains readable: {conv}"
    );
    let queue = wss_rpc(&mut rpc, 7, "agent.getQueue", json!({"agentId":agent})).await;
    assert_eq!(queue["queue"], json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attachment_groups_survive_natural_flush_over_wss() {
    attachment_groups_over_wss(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attachment_groups_survive_explicit_flush_over_wss() {
    attachment_groups_over_wss(true).await;
}

/// Failure cases: either side of a merge has attachments, image-only input,
/// multiple images, file references, mixed authors, empty attachment arrays,
/// editing/removing a sibling, and a flush pooling attachments after all text.
/// The provider's complete blocks plus queue/transcript are the replay artifact.
async fn attachment_groups_over_wss(explicit: bool) {
    let Some(script) = gate("WSS queued attachment groups") else {
        return;
    };
    let tmp = temp_data_dir();
    let data_dir = tmp.path();
    let (workspace_id, guest) = seed_workspace_with_guest(data_dir).await;
    let release = data_dir.join("release-group-kickoff");
    let Booted {
        daemon: _daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(data_dir, &script, 0, Some(&release), &[]).await;
    let mut sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({"workspaceId":workspace_id,"eventTypes":["agent:*"]}),
    )
    .await;
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut guest_rpc = connect_ws_as(port, cfg, GUEST_TOKEN).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "agent.create",
        json!({"workspaceId":workspace_id,"name":"Attachment groups","provider":"mock","model":"default"}),
    )
    .await;
    let agent = created["agent"]["id"].as_str().unwrap();
    let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
    let placed = wss_rpc(
        &mut rpc,
        3,
        "file.placeAttachment",
        json!({"workspaceId":workspace_id,"fileName":"group.png","data":png,"mimeType":"image/png"}),
    )
    .await;
    let image_id = placed["attachmentId"].as_str().unwrap();
    wss_rpc(
        &mut rpc,
        4,
        "agent.sendMessage",
        json!({"workspaceId":workspace_id,"agentId":agent,"content":KICKOFF_MSG}),
    )
    .await;
    await_prompts(&prompt_log, 1).await;
    let submissions = [
        json!({"content":"group plain before","imageBlocks":[],"fileBlocks":[]}),
        json!({"content":"group plain append"}),
        json!({"content":"group two images","imageBlocks":[
            {"type":"image","data":png,"mimeType":"image/png"},
            {"type":"image","attachmentId":image_id}
        ]}),
        json!({"content":"","imageBlocks":[{"type":"image","data":png,"mimeType":"image/png"}]}),
        json!({"content":"group file","fileBlocks":[{"type":"file","attachmentId":"att-group-file","fileName":"group.txt","mimeType":"text/plain"}]}),
        json!({"content":"group plain after"}),
        json!({"content":"group plain tail"}),
    ];
    let mut acknowledgements = Vec::new();
    for (i, mut params) in submissions.into_iter().enumerate() {
        params["workspaceId"] = json!(workspace_id);
        params["agentId"] = json!(agent);
        let envelope =
            wss_rpc_envelope(&mut rpc, 10 + i as i64, "agent.queueMessage", params).await;
        assert_eq!(envelope["jsonrpc"], "2.0");
        assert_eq!(envelope["id"], 10 + i as i64);
        assert!(envelope.get("error").is_none(), "{envelope}");
        acknowledgements.push(envelope["result"]["queuedMessage"].clone());
    }
    assert_eq!(acknowledgements[0]["id"], acknowledgements[1]["id"]);
    assert_eq!(acknowledgements[5]["id"], acknowledgements[6]["id"]);
    for i in 2..6 {
        assert_ne!(acknowledgements[i - 1]["id"], acknowledgements[i]["id"]);
    }
    let guest_ack = wss_rpc(
        &mut guest_rpc,
        20,
        "agent.queueMessage",
        json!({"workspaceId":workspace_id,"agentId":agent,"content":"group guest image",
            "imageBlocks":[{"type":"image","data":png,"mimeType":"image/png"}]}),
    )
    .await;
    let removed = wss_rpc(
        &mut rpc,
        21,
        "agent.queueMessage",
        json!({"workspaceId":workspace_id,"agentId":agent,"content":"group removed",
            "imageBlocks":[{"type":"image","data":png,"mimeType":"image/png"}]}),
    )
    .await;
    wss_rpc(
        &mut rpc,
        22,
        "agent.removeQueuedMessage",
        json!({"agentId":agent,"messageId":removed["queuedMessage"]["id"]}),
    )
    .await;
    wss_rpc(
        &mut rpc,
        23,
        "agent.editQueuedMessage",
        json!({"agentId":agent,"messageId":acknowledgements[2]["id"],"content":"group edited two images","editing":false}),
    )
    .await;
    let queue = wss_rpc(&mut rpc, 24, "agent.getQueue", json!({"agentId":agent})).await;
    let rows = queue["queue"].as_array().unwrap();
    assert_eq!(rows.len(), 6, "{queue}");
    for row in rows {
        assert_eq!(
            row["mergeEligible"], false,
            "last author has attachments: {row}"
        );
    }
    assert_eq!(rows[1]["imageBlocks"].as_array().unwrap().len(), 2);
    assert_eq!(rows[2]["imageBlocks"].as_array().unwrap().len(), 1);
    assert_eq!(rows[3]["fileBlocks"].as_array().unwrap().len(), 1);
    assert_eq!(rows[5]["id"], guest_ack["queuedMessage"]["id"]);
    assert_eq!(rows[5]["author"]["principalId"], guest.id.0);
    if explicit {
        let ids: Vec<_> = rows.iter().map(|row| row["id"].clone()).collect();
        wss_rpc(
            &mut rpc,
            25,
            "agent.sendQueuedMessagesNow",
            json!({"workspaceId":workspace_id,"agentId":agent,"messageIds":ids}),
        )
        .await;
    } else {
        std::fs::write(&release, "go").unwrap();
    }
    let observed = observe_drain(&mut sub, agent, 2).await;
    assert_eq!(observed.processing_frames.len(), 1, "one ACP turn");
    assert_eq!(
        observed.processing_frames[0]["queuedMessages"]
            .as_array()
            .unwrap()
            .len(),
        6
    );
    let prompts = await_prompts(&prompt_log, 2).await;
    assert_eq!(prompts.len(), 2, "initial turn plus a single batch");
    let log = std::fs::read_to_string(&prompt_log).unwrap();
    let records: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let blocks = records[1]["blocks"].as_array().unwrap();
    let position = |needle: &str| {
        blocks
            .iter()
            .position(|b| b["text"].as_str().is_some_and(|t| t.contains(needle)))
            .unwrap()
    };
    if explicit {
        assert!(
            position(KICKOFF_MSG) < position("Message #1:"),
            "preempted turn stays ahead of the batch"
        );
    }
    let first = position("Message #1:");
    let second = position("Message #2:");
    let third = position("Message #3:");
    let fourth = position("Message #4:");
    let fifth = position("Message #5:");
    let sixth = position("Message #6:");
    assert!(first < second && second < third && third < fourth && fourth < fifth && fifth < sixth);
    assert!(blocks[second]["text"]
        .as_str()
        .unwrap()
        .contains("group edited two images"));
    assert_eq!(blocks[second + 1]["type"], "image");
    assert_eq!(blocks[second + 2]["type"], "image");
    assert_eq!(
        blocks[second + 2]["data"],
        png,
        "reference resolved within its group"
    );
    assert_eq!(third, second + 3);
    assert_eq!(blocks[third + 1]["type"], "image");
    assert_eq!(fourth, third + 2);
    assert!(blocks[fourth + 1]["text"]
        .as_str()
        .unwrap()
        .contains("group.txt"));
    assert_eq!(fifth, fourth + 2);
    assert_eq!(blocks[sixth + 1]["type"], "image");
    assert_eq!(blocks.iter().filter(|b| b["type"] == "image").count(), 4);
    for needle in [
        "group plain before",
        "group plain append",
        "group edited two images",
        "group file",
        "group plain after",
        "group plain tail",
        "group guest image",
    ] {
        assert_eq!(
            prompts[1].matches(needle).count(),
            1,
            "no duplicated text: {needle}"
        );
    }
    assert!(!prompts[1].contains("group removed"));
    let conversation = wss_rpc(
        &mut rpc,
        26,
        "agent.getConversation",
        json!({"agentId":agent}),
    )
    .await;
    assert_eq!(
        user_row(&conversation, "group edited two images")["contentBlocks"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        user_row(&conversation, "group file")["contentBlocks"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let evidence = json!({"explicit":explicit,"queue":queue,"providerPrompts":records,"conversation":conversation});
    let artifact = save_group_artifact(
        &data_dir,
        if explicit {
            "explicit.json"
        } else {
            "natural.json"
        },
        &evidence,
    );
    eprintln!(
        "ATTACHMENT_GROUPS_ARTIFACT {}\n{}",
        artifact.display(),
        evidence
    );
    std::fs::write(&release, "go").unwrap();
}

/// Failure cases: flattened failed flush, lost durable groups after restart,
/// retry nesting wrapper groups, changed combined-row permissions, and stale
/// grouped text after a real edit. This drives failure/restart/retry over WSS.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_attachment_batch_keeps_groups_through_restart_and_retry_over_wss() {
    let Some(script) = gate("WSS attachment group restart") else {
        return;
    };
    let tmp = temp_data_dir();
    let data_dir = tmp.path();
    let (workspace_id, _) = seed_workspace_with_guest(data_dir).await;
    let release = data_dir.join("release-group-failure");
    let failure = json!({"ifPromptContains":"durable first group","promptRpcError":{"code":-32603,"message":"group fixture failure"}});
    let Booted {
        daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(data_dir, &script, 0, Some(&release), &[failure]).await;
    let mut sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({"workspaceId":workspace_id,"eventTypes":["agent:*"]}),
    )
    .await;
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut guest_rpc = connect_ws_as(port, cfg, GUEST_TOKEN).await;
    let created = wss_rpc(&mut rpc, 2, "agent.create", json!({"workspaceId":workspace_id,"name":"Durable groups","provider":"mock","model":"default"})).await;
    let agent = created["agent"]["id"].as_str().unwrap();
    wss_rpc(
        &mut rpc,
        3,
        "agent.sendMessage",
        json!({"workspaceId":workspace_id,"agentId":agent,"content":KICKOFF_MSG}),
    )
    .await;
    await_prompts(&prompt_log, 1).await;
    let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
    wss_rpc(&mut rpc, 4, "agent.queueMessage", json!({"workspaceId":workspace_id,"agentId":agent,"content":"durable first group","imageBlocks":[{"type":"image","data":png,"mimeType":"image/png"}]})).await;
    wss_rpc(&mut guest_rpc, 5, "agent.queueMessage", json!({"workspaceId":workspace_id,"agentId":agent,"content":"durable second group","fileBlocks":[{"type":"file","attachmentId":"durable-file","fileName":"durable.txt"}]})).await;
    std::fs::write(&release, "go").unwrap();
    observe_drain(&mut sub, agent, 2).await;
    // Queue publication follows stream:end, so wait for its observable state.
    let before = timeout(common::test_timeout(Duration::from_secs(30)), async {
        loop {
            let queue = wss_rpc(&mut rpc, 6, "agent.getQueue", json!({"agentId":agent})).await;
            if queue["queue"][0]["requeuedAfterFailure"] == true {
                break queue;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        before["queue"].as_array().unwrap().len(),
        1,
        "one combined retry"
    );
    let groups = before["queue"][0]["deliveryGroups"].as_array().unwrap();
    assert_eq!(groups.len(), 2);
    assert!(groups[0]["content"]
        .as_str()
        .unwrap()
        .contains("durable first group"));
    assert!(groups[1]["content"]
        .as_str()
        .unwrap()
        .contains("durable second group"));
    assert_eq!(groups[0]["imageBlocks"].as_array().unwrap().len(), 1);
    assert_eq!(groups[1]["fileBlocks"].as_array().unwrap().len(), 1);
    let retry_id = before["queue"][0]["id"].clone();
    let refused = wss_rpc_envelope(
        &mut guest_rpc,
        7,
        "agent.editQueuedMessage",
        json!({"agentId":agent,"messageId":retry_id,"content":"foreign edit"}),
    )
    .await;
    assert!(
        refused.get("error").is_some(),
        "combined retry keeps head ACL: {refused}"
    );
    drop(sub);
    drop(rpc);
    drop(guest_rpc);
    drop(daemon);
    let Booted {
        daemon: _daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(data_dir, &script, 0, None, &[]).await;
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let after = wss_rpc(&mut rpc, 10, "agent.getQueue", json!({"agentId":agent})).await;
    assert_eq!(
        after["queue"][0]["deliveryGroups"],
        before["queue"][0]["deliveryGroups"]
    );
    assert_eq!(after["queue"][0]["turnId"], before["queue"][0]["turnId"]);
    let mut sub = connect_ws(port, cfg).await;
    wss_rpc(
        &mut sub,
        11,
        "events.subscribe",
        json!({"workspaceId":workspace_id,"eventTypes":["agent:*"]}),
    )
    .await;
    wss_rpc(
        &mut rpc,
        12,
        "agent.retry",
        json!({"workspaceId":workspace_id,"agentId":agent}),
    )
    .await;
    observe_drain(&mut sub, agent, 1).await;
    let prompts = await_prompts(&prompt_log, 3).await;
    assert_eq!(prompts[2].matches("durable first group").count(), 1);
    assert_eq!(prompts[2].matches("durable second group").count(), 1);
    let log = std::fs::read_to_string(&prompt_log).unwrap();
    let records: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let blocks = records[2]["blocks"].as_array().unwrap();
    let first = blocks
        .iter()
        .position(|b| {
            b["text"]
                .as_str()
                .is_some_and(|t| t.contains("durable first group"))
        })
        .unwrap();
    let second = blocks
        .iter()
        .position(|b| {
            b["text"]
                .as_str()
                .is_some_and(|t| t.contains("durable second group"))
        })
        .unwrap();
    assert_eq!(blocks[first + 1]["type"], "image");
    assert_eq!(second, first + 2);
    assert!(blocks[second + 1]["text"]
        .as_str()
        .unwrap()
        .contains("durable.txt"));
    let conversation = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({"agentId":agent}),
    )
    .await;
    for needle in ["durable first group", GUEST_PREAMBLE] {
        assert_eq!(
            user_row_texts(&conversation)
                .iter()
                .filter(|t| t.starts_with(needle))
                .count(),
            1,
            "retry does not append duplicate rows"
        );
    }
    let evidence = json!({"beforeRestart":before,"afterRestart":after,"prompts":records,"conversation":conversation});
    let artifact = save_group_artifact(&data_dir, "restart.json", &evidence);
    eprintln!(
        "ATTACHMENT_GROUPS_RESTART_ARTIFACT {}\n{}",
        artifact.display(),
        evidence
    );
}

/// A zero-output interrupt must carry the whole flushed batch, not only the
/// transcript's last user row, ahead of the interrupt's own attachment group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupt_keeps_all_flushed_attachment_groups_over_wss() {
    let Some(script) = gate("WSS grouped batch interrupt") else {
        return;
    };
    let tmp = temp_data_dir();
    let data_dir = tmp.path();
    let (workspace_id, _) = seed_workspace_with_guest(data_dir).await;
    let release = data_dir.join("release-group-start");
    let batch_release = data_dir.join("release-group-batch");
    let rule = json!({"ifPromptContains":"interrupt group first","releaseFile":batch_release});
    let Booted {
        daemon: _daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(data_dir, &script, 0, Some(&release), &[rule]).await;
    let mut rpc = connect_ws(port, cfg).await;
    let created = wss_rpc(&mut rpc, 1, "agent.create", json!({"workspaceId":workspace_id,"name":"Interrupted groups","provider":"mock","model":"default"})).await;
    let agent = created["agent"]["id"].as_str().unwrap();
    wss_rpc(
        &mut rpc,
        2,
        "agent.sendMessage",
        json!({"workspaceId":workspace_id,"agentId":agent,"content":KICKOFF_MSG}),
    )
    .await;
    await_prompts(&prompt_log, 1).await;
    let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
    for (i, text) in ["interrupt group first", "interrupt group second"]
        .into_iter()
        .enumerate()
    {
        wss_rpc(&mut rpc, 3 + i as i64, "agent.queueMessage", json!({"workspaceId":workspace_id,"agentId":agent,"content":text,"imageBlocks":[{"type":"image","data":png,"mimeType":"image/png"}]})).await;
    }
    std::fs::write(&release, "go").unwrap();
    await_prompts(&prompt_log, 2).await;
    wss_rpc(&mut rpc, 5, "agent.sendMessage", json!({"workspaceId":workspace_id,"agentId":agent,"content":"interrupt group urgent","priority":"interrupt","imageBlocks":[{"type":"image","data":png,"mimeType":"image/png"}]})).await;
    await_prompts(&prompt_log, 3).await;
    let log = std::fs::read_to_string(&prompt_log).unwrap();
    let records: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let blocks = records[2]["blocks"].as_array().unwrap();
    let mut previous = None;
    for text in [
        "interrupt group first",
        "interrupt group second",
        "interrupt group urgent",
    ] {
        let positions: Vec<_> = blocks
            .iter()
            .enumerate()
            .filter(|(_, b)| b["text"].as_str().is_some_and(|t| t.contains(text)))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(positions.len(), 1, "each message exactly once: {records:?}");
        let index = positions[0];
        assert_eq!(blocks[index + 1]["type"], "image");
        if let Some(prev) = previous {
            assert!(prev < index);
        }
        previous = Some(index);
    }
    assert_eq!(blocks.iter().filter(|b| b["type"] == "image").count(), 3);
    let artifact = save_group_artifact(&data_dir, "interrupt.json", &json!({"prompts":records}));
    eprintln!(
        "ATTACHMENT_GROUPS_INTERRUPT_ARTIFACT {}\n{}",
        artifact.display(),
        log
    );
    std::fs::write(&batch_release, "go").unwrap();
}

/// Failure cases: a matching direct turn hides a retry source, identical queue
/// submissions collapse, stop carry-over repeats captured sources, or legacy
/// unknown sources are falsely joined. All delivery goes through real WSS.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_carry_over_preserves_source_identity_over_wss() {
    identity_carry_over_over_wss(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_carry_over_keeps_unknown_legacy_sources_over_wss() {
    identity_carry_over_over_wss(true).await;
}

async fn identity_carry_over_over_wss(legacy: bool) {
    let Some(script) = gate("WSS carry-over identity") else {
        return;
    };
    let tmp = temp_data_dir();
    let data_dir = tmp.path();
    let (workspace_id, _) = seed_workspace_with_guest(data_dir).await;
    let release = data_dir.join("identity-kickoff");
    let held = data_dir.join("identity-held");
    let text = "identical accepted image submission";
    let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
    let images = json!([{"type":"image","data":png,"mimeType":"image/png"}]);
    let failure = json!({"ifPromptContains":text,"promptRpcError":{"code":-32603,"message":"identity fixture failure"}});
    let Booted {
        daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(data_dir, &script, 0, Some(&release), &[failure]).await;
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut sub = connect_ws(port, cfg).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({"workspaceId":workspace_id,"eventTypes":["agent:*"]}),
    )
    .await;
    let created = wss_rpc(&mut rpc, 2, "agent.create", json!({"workspaceId":workspace_id,"name":"Identity proof","provider":"mock","model":"default"})).await;
    let agent = created["agent"]["id"].as_str().unwrap();
    wss_rpc(
        &mut rpc,
        3,
        "agent.sendMessage",
        json!({"workspaceId":workspace_id,"agentId":agent,"content":KICKOFF_MSG}),
    )
    .await;
    await_prompts(&prompt_log, 1).await;
    for id in [4, 5] {
        wss_rpc(
            &mut rpc,
            id,
            "agent.queueMessage",
            json!({"workspaceId":workspace_id,"agentId":agent,"content":text,"imageBlocks":images}),
        )
        .await;
    }
    std::fs::write(&release, "go").unwrap();
    observe_drain(&mut sub, agent, 2).await;
    let queue = timeout(common::test_timeout(Duration::from_secs(30)), async {
        loop {
            let queue = wss_rpc(&mut rpc, 6, "agent.getQueue", json!({"agentId":agent})).await;
            if queue["queue"][0]["requeuedAfterFailure"] == true {
                break queue;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        queue["queue"][0]["deliveryGroups"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    for group in queue["queue"][0]["deliveryGroups"].as_array().unwrap() {
        assert!(
            group
                .as_object()
                .unwrap()
                .keys()
                .all(|k| matches!(k.as_str(), "content" | "imageBlocks" | "fileBlocks")),
            "internal identity must stay off the wire: {group}"
        );
    }
    let retry_id = queue["queue"][0]["id"].clone();
    drop(rpc);
    drop(sub);
    drop(daemon);
    // Reproduce persisted captured-source overlap. This fixture mutation is
    // internal-only: clients still consume and deliver the row through WSS.
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .unwrap();
    let mut rows = store.load_all_agent_queues().await.unwrap();
    let row = rows
        .iter_mut()
        .find(|row| row.id == retry_id.as_str().unwrap())
        .unwrap();
    if legacy {
        for group in row.payload["deliveryGroups"].as_array_mut().unwrap() {
            group.as_object_mut().unwrap().remove("sourceId");
        }
    }
    let mut captured = row.payload["deliveryGroups"].clone();
    for group in captured.as_array_mut().unwrap() {
        group["isPrepend"] = json!(true);
    }
    row.payload["prependDeliveryGroups"] = captured;
    store
        .replace_agent_queue(&row.agent_id, std::slice::from_ref(row))
        .await
        .unwrap();
    drop(store);
    let rules = [json!({"ifPromptContains":text,"releaseFile":held})];
    let Booted {
        daemon: _daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(data_dir, &script, 0, None, &rules).await;
    let mut rpc = connect_ws(port, cfg).await;
    // First retry proves true shared captured sources join once, whereas
    // equal legacy payloads with no identity remain independently readable.
    wss_rpc(
        &mut rpc,
        7,
        "agent.sendQueuedMessageNow",
        json!({"workspaceId":workspace_id,"agentId":agent,"messageId":retry_id}),
    )
    .await;
    await_prompts(&prompt_log, 3).await;
    let records = || -> Vec<Value> {
        std::fs::read_to_string(&prompt_log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    };
    let assert_groups = |record: &Value, count: usize| {
        let blocks = record["blocks"].as_array().unwrap();
        assert_eq!(
            blocks
                .iter()
                .filter(|b| b["type"] == "image" && b["data"] == png)
                .count(),
            count,
            "all accepted images: {record}"
        );
        // Recreated-session recap may contain text too; attachment-adjacent
        // blocks identify the delivered groups rather than the history XML.
        let adjacent = blocks
            .windows(2)
            .filter(|pair| {
                pair[0]["text"].as_str().is_some_and(|t| t.contains(text))
                    && pair[1]["type"] == "image"
            })
            .count();
        if legacy {
            // Legacy recap text can be supplied by recreated-session history;
            // its unknown source identity must still retain every image.
            assert!(adjacent >= 2, "original groups remain readable: {record}");
        } else {
            assert_eq!(adjacent, count, "each text beside its own image: {record}");
        }
    };
    let base_count = if legacy { 4 } else { 2 };
    assert_groups(&records()[2], base_count);
    // Repeated zero-output stops must not multiply the captured source set.
    wss_rpc(&mut rpc, 8, "agent.stop", json!({"agentId":agent})).await;
    wss_rpc(&mut rpc, 9, "agent.stop", json!({"agentId":agent})).await;
    wss_rpc(
        &mut rpc,
        10,
        "agent.sendMessage",
        json!({"workspaceId":workspace_id,"agentId":agent,"content":text,"imageBlocks":images}),
    )
    .await;
    await_prompts(&prompt_log, 4).await;
    assert_groups(&records()[3], base_count + 1);
    // Interrupt a distinct matching live source with a queued durable retry.
    // Retain its original source set to expose content-based false overlap.
    wss_rpc(&mut rpc, 11, "agent.stop", json!({"agentId":agent})).await;
    drop(rpc);
    drop(_daemon);
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .unwrap();
    // Restore the original failed row as a durable retry, not a fresh source.
    let row = rows
        .iter_mut()
        .find(|row| row.id == retry_id.as_str().unwrap())
        .unwrap();
    row.payload["prependDeliveryGroups"] = json!([]);
    // Isolate the direct-source interruption from the stop proof above.
    // That arm was already consumed and asserted; do not replay it here.
    store.clear_stop_redelivery(&row.agent_id).await.unwrap();
    store
        .replace_agent_queue(&row.agent_id, std::slice::from_ref(row))
        .await
        .unwrap();
    drop(store);
    let Booted {
        daemon: _daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(data_dir, &script, 0, None, &rules).await;
    let mut rpc = connect_ws(port, cfg).await;
    wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({"workspaceId":workspace_id,"agentId":agent,"content":text,"imageBlocks":images}),
    )
    .await;
    await_prompts(&prompt_log, 5).await;
    wss_rpc(
        &mut rpc,
        13,
        "agent.sendQueuedMessageNow",
        json!({"workspaceId":workspace_id,"agentId":agent,"messageId":retry_id}),
    )
    .await;
    await_prompts(&prompt_log, 6).await;
    let evidence = records();
    assert_groups(&evidence[5], 3);
    let name = if legacy {
        "identity-legacy.json"
    } else {
        "identity.json"
    };
    let artifact = save_group_artifact(
        data_dir,
        name,
        &json!({"legacy":legacy,"failedQueue":queue,"prompts":evidence}),
    );
    eprintln!("ATTACHMENT_GROUPS_IDENTITY_ARTIFACT {}", artifact.display());
    std::fs::write(&held, "go").unwrap();
}

/// Keep repeatable evidence outside temporary daemon data when requested.
fn save_group_artifact(data_dir: &Path, name: &str, evidence: &serde_json::Value) -> PathBuf {
    let directory = std::env::var_os("INTENT_QUEUED_GROUP_ARTIFACT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| data_dir.to_path_buf());
    std::fs::create_dir_all(&directory).unwrap();
    let artifact = directory.join(name);
    std::fs::write(&artifact, serde_json::to_vec_pretty(evidence).unwrap()).unwrap();
    artifact
}
