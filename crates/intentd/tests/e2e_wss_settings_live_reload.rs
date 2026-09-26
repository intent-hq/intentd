//! WSS e2e — the new TOML-backed settings lifecycle (§5.12, §9.8):
//!
//! 1. `settings.update` over a real WSS connection atomically rewrites
//!    `config.toml` on disk (user comments preserved) and emits
//!    `settings:changed` to WSS subscribers;
//! 2. an external hand-edit of config.toml (atomic tmp+rename, editor-style)
//!    live-reloads: `settings:changed` arrives over WSS naming the changed
//!    key and `settings.get` reflects the file value;
//! 3. an invalid external edit (TOML syntax error or unknown key) keeps
//!    last-good values without crashing the daemon, and a subsequent valid
//!    edit recovers;
//! 4. the one-time boot migration of the deprecated `providers.active`
//!    rewrites config.toml (key removed, value carried into
//!    `model.defaultProvider`, comments preserved) and a restart from the
//!    migrated file never rewrites it again;
//! 5. unsupported newer-client batches and failed file writes preserve
//!    values, revision, bytes and events; subsequent update/reset writes
//!    survive a real daemon restart without leaking rejected changes.
//!
//! Adjacent coverage lives elsewhere and is intentionally not duplicated:
//! startup refusal on malformed config + flag-pin precedence in
//! `e2e_config_precedence.rs`, mixed-batch rollback in
//! `e2e_wss_settings_atomic_rollback.rs`, pinned-port rejection in
//! `e2e_wss_runtime_control.rs`.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// Pure-liveness deadline for positive event-driven waits (monorepo#1849,
/// mirroring the `LIVENESS` pattern from intent-hq/intentd#1030/#1043): the
/// waits below return as soon as the awaited event arrives, so this bound
/// only has to outlast a genuine wedge (fs-event registration/delivery
/// stalls under full-suite parallel load), never a passing run. Negative
/// assertions keep their short bounds.
const LIVENESS: Duration = Duration::from_secs(300);

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
    }
}

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-livereload-")
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = common::serve_command();
    // Guarantee the config-watcher readiness marker (INFO, target `intentd`)
    // reaches daemon.log even when the caller's RUST_LOG is stricter (e.g.
    // `warn`): append a crate-scoped directive, which EnvFilter resolves in
    // favor of the more specific target. `await_config_watcher_ready` gates
    // on that marker.
    let rust_log = match std::env::var("RUST_LOG") {
        Ok(v) if !v.is_empty() => format!("{v},intentd=info"),
        _ => "info".to_string(),
    };
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("RUST_LOG", rust_log)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
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

type Wss = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> Wss {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc(ws: &mut Wss, id: i64, method: &str, params: Value) -> Value {
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

/// Pump the dedicated subscriber connection until a `settings:changed`
/// `events.event` frame arrives (or [`LIVENESS`] elapses). Returns the full
/// frame. Pure-liveness positive wait: returns as soon as the event lands.
async fn next_settings_event(ws: &mut Wss) -> Value {
    let deadline = tokio::time::Instant::now() + LIVENESS;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let next = timeout(remaining, ws.next())
            .await
            .expect("timed out waiting for settings:changed");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == json!("events.event")
                    && v["params"]["event"]["type"] == json!("settings:changed")
                {
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

/// Assert NO `settings:changed` frame arrives on the subscriber connection
/// within `secs` (bounded negative wait; covers the 300ms watcher debounce
/// with a wide margin).
async fn assert_no_settings_event(ws: &mut Wss, secs: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match timeout(remaining, ws.next()).await {
            Err(_) => return, // window elapsed with no settings event — pass
            Ok(Some(Ok(Message::Text(text)))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                assert_ne!(
                    v["params"]["event"]["type"],
                    json!("settings:changed"),
                    "unexpected settings:changed for an invalid edit: {v}"
                );
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
            Ok(other) => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Editor-style atomic save: write a temp file in the same directory, then
/// rename it over config.toml (the watcher handles rename-style saves).
fn atomic_write(path: &Path, content: &str) {
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, content).expect("write tmp config");
    std::fs::rename(&tmp, path).expect("rename tmp over config.toml");
}

/// Wait until the daemon's config.toml live-reload watcher is registered,
/// by polling daemon.log for the readiness line the composition root emits
/// (`spawn_config_watcher_init` in `crates/intentd/src/main.rs`). The
/// watcher registers in a background task (monorepo#1581), so a fast test
/// can otherwise hand-edit config.toml before the `FSEvents` watch exists and
/// the edit is missed entirely — no wait on `settings:changed`, however
/// long, can recover it (monorepo#1849). Bounded by [`LIVENESS`]; fails
/// fast if the daemon reports the watcher failed to start.
async fn await_config_watcher_ready(data_dir: &Path) {
    let log_path = data_dir.join("daemon.log");
    let deadline = tokio::time::Instant::now() + LIVENESS;
    loop {
        let log = std::fs::read_to_string(&log_path).unwrap_or_default();
        if log.contains("config.toml live-reload watcher ready") {
            return;
        }
        assert!(
            !log.contains("config.toml live-reload watcher failed to start"),
            "config watcher failed to start\n--- daemon log ---\n{log}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "config.toml live-reload watcher never became ready within {LIVENESS:?}\n\
             --- daemon log ---\n{log}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The readiness marker `await_config_watcher_ready` gates on must mean the
/// directory watch is actually live, not merely requested: the registration
/// runs on the hub's registrar thread after `ConfigWatcher::start` returns
/// (intent-hq/intent#4953), so a marker logged straight after `start` would
/// let a test hand-edit config.toml before the watch exists. Under the
/// watcher-creation-failure seam every registration settles as failed, so
/// the daemon must report `failed to start` and never `ready`.
#[tokio::test]
async fn config_watcher_readiness_marker_waits_for_a_live_watch() {
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            "uds",
            &[("INTENTD_TEST_FAIL_WATCHER_CREATION", "1")],
        ),
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let log_path = data_dir.join("daemon.log");
    let deadline = tokio::time::Instant::now() + LIVENESS;
    loop {
        let log = std::fs::read_to_string(&log_path).unwrap_or_default();
        assert!(
            !log.contains("config.toml live-reload watcher ready"),
            "readiness must not be reported while the config directory watch is not live\n\
             --- daemon log ---\n{log}"
        );
        if log.contains("config.toml live-reload watcher failed to start") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the daemon never reported the config watcher failing to start within {LIVENESS:?}\n\
             --- daemon log ---\n{log}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    drop(daemon);
}

/// Boot with the WSS listener enabled, discover the WSS port + fingerprint via
/// `system.status` over UDS, and return (daemon, rpc conn, subscriber conn)
/// with the subscriber already subscribed to `settings:changed`.
async fn boot_with_wss(data_dir: &Path) -> (Daemon, Wss, Wss) {
    let env: [(&str, &str); 1] = [("INTENTD_AUTH_TOKEN", TOKEN)];
    let child = spawn_serve(data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.to_path_buf(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(
        status["result"]["port"]
            .as_u64()
            .expect("port should be set at boot"),
    )
    .expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint should be set")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut sub = connect_ws(port, cfg).await;
    let ack = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["settings:changed"] }),
    )
    .await;
    assert!(ack.get("error").is_none(), "subscribe failed: {ack}");
    // Sanity: the rpc connection answers before we start mutating.
    let ping = wss_rpc(
        &mut rpc,
        2,
        "settings.get",
        json!({ "path": "rtk.enabled" }),
    )
    .await;
    assert!(ping.get("error").is_none(), "settings.get failed: {ping}");
    (daemon, rpc, sub)
}

/// Capture complete read results, including origins and revisions, so a
/// rejected request cannot silently change anything clients observe.
async fn settings_snapshot(rpc: &mut Wss, paths: &[&str]) -> Vec<Value> {
    let mut settings = Vec::new();
    for path in paths {
        let get = wss_rpc(rpc, 100, "settings.get", json!({ "path": path })).await;
        assert_eq!(get["jsonrpc"], json!("2.0"), "{get}");
        assert_eq!(get["id"], json!(100), "{get}");
        assert!(get.get("error").is_none(), "{get}");
        assert_eq!(get["result"]["path"], json!(path), "{get}");
        settings.push(get["result"].clone());
    }
    settings
}

async fn assert_settings_change(sub: &mut Wss, changes: &Value, revision: u64) {
    let event = next_settings_event(sub).await;
    assert_eq!(event["jsonrpc"], json!("2.0"), "{event}");
    assert_eq!(event["params"]["event"]["data"]["changes"], *changes);
    assert_eq!(
        event["params"]["event"]["data"]["revision"],
        json!(revision)
    );
}

/// A newer client may send a path or enum this daemon does not know. The
/// whole batch must reject before writes/events, then supported update/reset
/// requests must still persist a startup-compatible file (intent#5909).
#[tokio::test]
async fn newer_client_settings_batches_reject_atomically_and_restart() {
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path();
    let config_path = data_dir.join("config.toml");
    let comment = "# Operator comment survives validation and reset.";
    std::fs::write(
        &config_path,
        format!("{comment}\n[git]\nautoCommit = true\n[workspace]\nbranchPrefix = \"seed/\"\n"),
    )
    .expect("seed config.toml");

    let (daemon, mut rpc, mut sub) = boot_with_wss(data_dir).await;
    await_config_watcher_ready(data_dir).await;
    let paths = [
        "git.autoCommit",
        "workspace.branchPrefix",
        "agents.flushQueuedMessages",
        "quickActions.providerSettings",
    ];
    let before = settings_snapshot(&mut rpc, &paths).await;
    let revision = before[0]["revision"].as_u64().expect("initial revision");
    let original = std::fs::read(&config_path).expect("read seeded config");

    for (path, value) in [
        ("future.setting", json!(true)),
        ("agents.flushQueuedMessages", json!("future-policy")),
        // Object-shaped at the wire catalog, but invalid for the daemon's
        // typed config: provider option values must be strings.
        (
            "quickActions.providerSettings",
            json!({ "future-provider": { "option": null } }),
        ),
    ] {
        let rejected = wss_rpc(
            &mut rpc,
            10,
            "settings.update",
            json!({ "changes": [
                { "path": "git.autoCommit", "value": false },
                { "path": "workspace.branchPrefix", "value": "rejected/" },
                { "path": path, "value": value }
            ] }),
        )
        .await;
        assert_eq!(rejected["jsonrpc"], json!("2.0"), "{rejected}");
        assert_eq!(rejected["id"], json!(10), "{rejected}");
        assert!(rejected.get("result").is_none(), "{rejected}");
        assert_eq!(rejected["error"]["code"], json!(-32602), "{rejected}");
        assert!(
            rejected["error"]["message"]
                .as_str()
                .unwrap()
                .contains(path),
            "error must identify {path}: {rejected}"
        );
        assert_eq!(std::fs::read(&config_path).unwrap(), original);
        assert_eq!(settings_snapshot(&mut rpc, &paths).await, before);
        assert_no_settings_event(&mut sub, 3).await;
    }

    let update = wss_rpc(
        &mut rpc,
        11,
        "settings.update",
        json!({ "changes": [
            { "path": "git.autoCommit", "value": false },
            { "path": "workspace.branchPrefix", "value": "accepted/" }
        ] }),
    )
    .await;
    let applied = json!([
        { "path": "git.autoCommit", "value": false, "origin": "file" },
        { "path": "workspace.branchPrefix", "value": "accepted/", "origin": "file" }
    ]);
    assert_eq!(
        update,
        json!({ "jsonrpc": "2.0", "id": 11, "result": {
            "applied": applied, "revision": revision + 1
        }})
    );
    assert_settings_change(&mut sub, &applied, revision + 1).await;
    let accepted = settings_snapshot(&mut rpc, &paths).await;
    assert_eq!(accepted[0]["value"], json!(false));
    assert_eq!(accepted[1]["value"], json!("accepted/"));

    let reset = wss_rpc(
        &mut rpc,
        12,
        "settings.reset",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(
        reset,
        json!({ "jsonrpc": "2.0", "id": 12, "result": {
            "path": "git.autoCommit", "value": true, "origin": "default", "revision": revision + 2
        }})
    );
    assert_settings_change(
        &mut sub,
        &json!([{ "path": "git.autoCommit", "value": true, "origin": "default" }]),
        revision + 2,
    )
    .await;
    let after_reset = settings_snapshot(&mut rpc, &paths).await;
    assert_eq!(after_reset[0]["value"], json!(true));
    assert_eq!(after_reset[0]["origin"], json!("default"));
    assert_eq!(after_reset[1]["value"], json!("accepted/"));
    assert_eq!(after_reset[1]["origin"], json!("file"));
    for (after, prior) in after_reset.iter().zip(&before).skip(2) {
        assert_eq!(after["value"], prior["value"]);
        assert_eq!(after["origin"], prior["origin"]);
    }
    assert!(after_reset
        .iter()
        .all(|s| s["revision"] == json!(revision + 2)));
    let committed = std::fs::read_to_string(&config_path).unwrap();
    intent_core::settings_file::SettingsFile::parse_str(&committed)
        .expect("successful writes must pass the startup parser");
    assert!(committed.contains(comment), "{committed}");
    assert!(
        !committed.contains("autoCommit"),
        "reset must remove the key"
    );
    assert!(!committed.contains("rejected/"), "{committed}");
    assert_no_settings_event(&mut sub, 3).await;

    drop(sub);
    drop(rpc);
    drop(daemon);
    // Keep the exact file and data directory: only the daemon is replaced.
    let (_restarted, mut rpc, mut sub) = boot_with_wss(data_dir).await;
    await_config_watcher_ready(data_dir).await;
    let restarted = settings_snapshot(&mut rpc, &paths).await;
    for (after, prior) in restarted.iter().zip(&after_reset) {
        assert_eq!(after["value"], prior["value"]);
        assert_eq!(after["origin"], prior["origin"]);
        assert_eq!(after["revision"], json!(0), "revision is process-local");
    }
    assert_eq!(std::fs::read_to_string(&config_path).unwrap(), committed);
    assert_no_settings_event(&mut sub, 3).await;
}

/// A failed atomic replacement must not leave a candidate document behind
/// for a later unrelated write to persist. Exercise both update and reset
/// failures through the real transport, then restart on the recovered file.
#[tokio::test]
async fn failed_settings_writes_do_not_leak_into_later_wss_writes_or_restart() {
    for reset in [false, true] {
        let data_dir_guard = temp_data_dir();
        let data_dir = data_dir_guard.path();
        let config_path = data_dir.join("config.toml");
        let saved_path = data_dir.join("saved-config.toml");
        let comment = "# Preserve the last committed settings after an I/O failure.";
        std::fs::write(
            &config_path,
            format!(
                "{comment}\n[git]\nautoCommit = false\n[workspace]\nbranchPrefix = \"seed/\"\n"
            ),
        )
        .unwrap();
        let (daemon, mut rpc, mut sub) = boot_with_wss(data_dir).await;
        await_config_watcher_ready(data_dir).await;
        let paths = ["git.autoCommit", "model.default", "workspace.branchPrefix"];
        let before = settings_snapshot(&mut rpc, &paths).await;
        let revision = before[0]["revision"].as_u64().unwrap();
        let original = std::fs::read(&config_path).unwrap();

        // A directory at the target makes rename fail even as root. Keep
        // the original bytes aside until the successful write: restoring
        // them earlier could let the watcher repair leaked candidate state
        // and mask this regression. Missing/unreadable files keep last-good.
        std::fs::rename(&config_path, &saved_path).unwrap();
        std::fs::create_dir(&config_path).unwrap();
        let (method, params) = if reset {
            ("settings.reset", json!({ "path": "git.autoCommit" }))
        } else {
            (
                "settings.update",
                json!({ "changes": [
                { "path": "git.autoCommit", "value": true },
                { "path": "model.default", "value": "rejected-model" }
            ] }),
            )
        };
        let failed = wss_rpc(&mut rpc, 10, method, params).await;
        assert_eq!(failed["jsonrpc"], json!("2.0"), "{failed}");
        assert_eq!(failed["id"], json!(10), "{failed}");
        assert!(failed.get("result").is_none(), "{failed}");
        assert_eq!(failed["error"]["code"], json!(-32603), "{failed}");
        assert_eq!(failed["error"]["message"], json!("Internal error"));
        assert!(
            failed["error"]["data"]
                .as_str()
                .unwrap()
                .contains("could not write config"),
            "must reach persistence, not fail input validation: {failed}"
        );
        assert!(
            config_path.is_dir(),
            "failed write must not replace the target"
        );
        assert_eq!(std::fs::read_dir(&config_path).unwrap().count(), 0);
        assert_eq!(std::fs::read(&saved_path).unwrap(), original);
        assert_eq!(settings_snapshot(&mut rpc, &paths).await, before);
        assert_no_settings_event(&mut sub, 3).await;

        std::fs::remove_dir(&config_path).unwrap();
        let recovered = wss_rpc(
            &mut rpc,
            11,
            "settings.update",
            json!({ "changes": [{ "path": "workspace.branchPrefix", "value": "recovered/" }] }),
        )
        .await;
        let applied =
            json!([{ "path": "workspace.branchPrefix", "value": "recovered/", "origin": "file" }]);
        assert_eq!(
            recovered,
            json!({ "jsonrpc": "2.0", "id": 11, "result": {
                "applied": applied, "revision": revision + 1
            }})
        );
        // This positive event is also a barrier for the earlier no-event assertion.
        assert_settings_change(&mut sub, &applied, revision + 1).await;
        let after = settings_snapshot(&mut rpc, &paths).await;
        for (value, prior) in after.iter().zip(&before).take(2) {
            assert_eq!(value["value"], prior["value"], "failed {method} leaked");
            assert_eq!(value["origin"], prior["origin"], "failed {method} leaked");
        }
        assert_eq!(after[2]["value"], json!("recovered/"));
        assert!(after.iter().all(|s| s["revision"] == json!(revision + 1)));
        let committed = std::fs::read_to_string(&config_path).unwrap();
        intent_core::settings_file::SettingsFile::parse_str(&committed)
            .expect("recovered file must pass the startup parser");
        assert!(committed.contains(comment), "{committed}");
        assert!(committed.contains("autoCommit = false"), "{committed}");
        assert!(!committed.contains("rejected-model"), "{committed}");
        assert_eq!(std::fs::read(&saved_path).unwrap(), original);
        assert_no_settings_event(&mut sub, 3).await;

        drop(sub);
        drop(rpc);
        drop(daemon);
        let (_restarted, mut rpc, _sub) = boot_with_wss(data_dir).await;
        let restarted = settings_snapshot(&mut rpc, &paths).await;
        for (value, prior) in restarted.iter().zip(&after) {
            assert_eq!(value["value"], prior["value"]);
            assert_eq!(value["origin"], prior["origin"]);
        }
        assert_eq!(std::fs::read_to_string(&config_path).unwrap(), committed);
    }
}

/// §5.12 scenario 1: `settings.update` over WSS rewrites config.toml on disk
/// (atomic, comment-preserving) and emits `settings:changed` to WSS
/// subscribers; envelope shapes match PROTOCOL §5.12 (plus the additive
/// `origin` field on reads).
#[tokio::test]
async fn settings_update_over_wss_rewrites_config_toml_and_emits_event() {
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let config_path = data_dir.join("config.toml");
    std::fs::write(
        &config_path,
        "# Custom operator comment — must survive daemon rewrites.\n\
         [git]\n\
         autoCommit = true\n\
         \n\
         [workspace]\n\
         branchPrefix = \"seed/\"\n",
    )
    .expect("seed config.toml");

    let (_daemon, mut rpc, mut sub) = boot_with_wss(&data_dir).await;

    // Baseline: the seeded file value is effective with origin=file.
    let get = wss_rpc(
        &mut rpc,
        10,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(get["jsonrpc"], json!("2.0"));
    assert_eq!(get["result"]["path"], json!("git.autoCommit"));
    assert_eq!(get["result"]["value"], json!(true), "{get}");
    assert_eq!(get["result"]["origin"], json!("file"), "{get}");
    // §5.12 settings.get result carries the full definition.
    assert_eq!(get["result"]["definition"]["path"], json!("git.autoCommit"));
    assert_eq!(get["result"]["definition"]["type"], json!("boolean"));
    assert_eq!(get["result"]["revision"], json!(0));

    // settings.update over WSS → §5.12 result with post-commit origin.
    let update = wss_rpc(
        &mut rpc,
        11,
        "settings.update",
        json!({ "changes": [{ "path": "git.autoCommit", "value": false }] }),
    )
    .await;
    assert_eq!(update["jsonrpc"], json!("2.0"));
    assert_eq!(update["id"], json!(11));
    assert_eq!(
        update["result"]["applied"],
        json!([{ "path": "git.autoCommit", "value": false, "origin": "file" }]),
        "settings.update result shape per §5.12: {update}"
    );
    let update_revision = update["result"]["revision"]
        .as_u64()
        .expect("settings.update revision");

    // §6.5: settings:changed with data.changes = applied pairs.
    let ev = next_settings_event(&mut sub).await;
    assert_eq!(ev["method"], json!("events.event"));
    assert_eq!(
        ev["params"]["event"]["data"]["changes"],
        json!([{ "path": "git.autoCommit", "value": false, "origin": "file" }]),
        "{ev}"
    );
    assert_eq!(
        ev["params"]["event"]["data"]["revision"],
        json!(update_revision)
    );

    // The daemon rewrote config.toml on disk: new value present, user comment
    // and untouched keys preserved (toml_edit comment-preserving write-back).
    let text = std::fs::read_to_string(&config_path).expect("read config.toml");
    assert!(
        text.contains("autoCommit = false"),
        "file must carry the new value: {text}"
    );
    assert!(
        text.contains("# Custom operator comment — must survive daemon rewrites."),
        "user comment must survive the rewrite: {text}"
    );
    assert!(
        text.contains("branchPrefix = \"seed/\""),
        "untouched keys must survive the rewrite: {text}"
    );

    // Wire read-back agrees with the file.
    let get = wss_rpc(
        &mut rpc,
        12,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(get["result"]["value"], json!(false), "{get}");
    assert_eq!(get["result"]["origin"], json!("file"), "{get}");
}

/// §9.8 scenarios 2 + 3: an external editor-style edit of config.toml
/// live-reloads (settings:changed over WSS + settings.get reflects it), an
/// invalid edit (syntax error, then unknown key) keeps last-good values with
/// the daemon up, and a subsequent valid edit recovers.
#[tokio::test]
async fn external_edit_live_reloads_and_invalid_edit_keeps_last_good() {
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let config_path = data_dir.join("config.toml");
    std::fs::write(&config_path, "[workspace]\nbranchPrefix = \"before/\"\n")
        .expect("seed config.toml");

    let (mut daemon, mut rpc, mut sub) = boot_with_wss(&data_dir).await;

    // Capture the harness-seeded [server.wsApi] table (enabled + ephemeral
    // port): every valid rewrite below must carry it unchanged so the WSS
    // listener (and these connections) stays up across reloads.
    let ws_api_block = {
        let text = std::fs::read_to_string(&config_path).expect("read config.toml");
        let idx = text.find("[server.wsApi]").expect("wsApi table seeded");
        text[idx..].to_string()
    };

    let get = wss_rpc(
        &mut rpc,
        10,
        "settings.get",
        json!({ "path": "workspace.branchPrefix" }),
    )
    .await;
    assert_eq!(get["result"]["value"], json!("before/"), "{get}");
    assert_eq!(get["result"]["origin"], json!("file"), "{get}");

    // The watcher registers in a background task off the boot path
    // (monorepo#1581): gate the first external edit on its readiness so the
    // edit cannot land before the FSEvents watch exists (monorepo#1849).
    await_config_watcher_ready(&data_dir).await;

    // Valid external edit (atomic tmp+rename) → live-reload: the watcher
    // emits settings:changed naming the changed key with the new value.
    atomic_write(
        &config_path,
        &format!("[workspace]\nbranchPrefix = \"after/\"\n\n{ws_api_block}"),
    );
    let ev = next_settings_event(&mut sub).await;
    assert!(
        ev["params"]["event"]["data"]["revision"].as_u64().unwrap() > 0,
        "live reload must carry a daemon revision: {ev}"
    );
    let changes = ev["params"]["event"]["data"]["changes"]
        .as_array()
        .expect("changes array");
    assert!(
        changes.iter().any(|c| c
            == &json!({ "path": "workspace.branchPrefix", "value": "after/", "origin": "file" })),
        "live-reload event must carry the edited key: {ev}"
    );
    let get = wss_rpc(
        &mut rpc,
        11,
        "settings.get",
        json!({ "path": "workspace.branchPrefix" }),
    )
    .await;
    assert_eq!(get["result"]["value"], json!("after/"), "{get}");
    assert_eq!(get["result"]["origin"], json!("file"), "{get}");

    // Invalid edit #1 — TOML syntax error: no event, last-good kept, daemon up.
    atomic_write(&config_path, "[workspace\nbranchPrefix = ???\n");
    assert_no_settings_event(&mut sub, 3).await;

    // Invalid edit #2 — valid TOML, unknown key (strict schema): same outcome.
    atomic_write(&config_path, "[workspace]\nbogusKey = 1\n");
    assert_no_settings_event(&mut sub, 3).await;

    assert!(
        daemon.child.try_wait().expect("try_wait").is_none(),
        "daemon must survive invalid config.toml edits"
    );
    let get = wss_rpc(
        &mut rpc,
        12,
        "settings.get",
        json!({ "path": "workspace.branchPrefix" }),
    )
    .await;
    assert_eq!(
        get["result"]["value"],
        json!("after/"),
        "last-good value must survive invalid edits: {get}"
    );

    // Recovery: a subsequent valid edit applies and emits settings:changed.
    atomic_write(
        &config_path,
        &format!("[workspace]\nbranchPrefix = \"recovered/\"\n\n{ws_api_block}"),
    );
    let ev = next_settings_event(&mut sub).await;
    let changes = ev["params"]["event"]["data"]["changes"]
        .as_array()
        .expect("changes array");
    assert!(
        changes
            .iter()
            .any(|c| c == &json!({ "path": "workspace.branchPrefix", "value": "recovered/", "origin": "file" })),
        "recovery event must carry the edited key: {ev}"
    );
    let get = wss_rpc(
        &mut rpc,
        13,
        "settings.get",
        json!({ "path": "workspace.branchPrefix" }),
    )
    .await;
    assert_eq!(get["result"]["value"], json!("recovered/"), "{get}");
    assert_eq!(get["result"]["origin"], json!("file"), "{get}");
}

/// monorepo#1729 over the wire: a `config.toml` still carrying the renamed
/// `[backgroundAgents]` table is migrated into `quickActions.*` at boot and
/// the legacy table is stripped; the retired paths are gone from
/// `settings.list` and rejected by `settings.get`, while `settings.update`
/// still tolerates-and-ignores them for pre-rename clients.
#[tokio::test]
async fn background_agents_table_migrates_to_quick_actions_over_wss() {
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let config_path = data_dir.join("config.toml");
    std::fs::write(
        &config_path,
        "[backgroundAgents]\ndefaultModel = \"auggie:haiku\"\ntypeOverrides = { commit = \"auggie:fast\" }\n",
    )
    .expect("seed legacy config.toml");

    let (_daemon, mut rpc, _sub) = boot_with_wss(&data_dir).await;

    // The legacy values landed on the renamed keys, read back over the wire.
    let get = wss_rpc(
        &mut rpc,
        10,
        "settings.get",
        json!({ "path": "quickActions.defaultModel" }),
    )
    .await;
    assert_eq!(get["result"]["value"], json!("auggie:haiku"), "{get}");
    let get = wss_rpc(
        &mut rpc,
        11,
        "settings.get",
        json!({ "path": "quickActions.typeOverrides" }),
    )
    .await;
    assert_eq!(
        get["result"]["value"],
        json!({ "commit": "auggie:fast" }),
        "{get}"
    );

    // The legacy table is stripped from disk.
    let text = std::fs::read_to_string(&config_path).expect("read config.toml");
    assert!(!text.contains("backgroundAgents"), "{text}");

    // The retired path is gone from the catalog and rejected by settings.get.
    let list = wss_rpc(&mut rpc, 12, "settings.list", json!({})).await;
    let paths: Vec<&str> = list["result"]["settings"]
        .as_array()
        .expect("settings array")
        .iter()
        .filter_map(|d| d["path"].as_str())
        .collect();
    assert!(
        !paths.iter().any(|p| p.starts_with("backgroundAgents.")),
        "retired paths must not be advertised: {paths:?}"
    );
    assert!(paths.contains(&"quickActions.defaultModel"), "{paths:?}");
    let get = wss_rpc(
        &mut rpc,
        13,
        "settings.get",
        json!({ "path": "backgroundAgents.defaultModel" }),
    )
    .await;
    assert_eq!(get["error"]["code"], json!(-32602), "{get}");

    // settings.update on the retired path is tolerated and ignored.
    let update = wss_rpc(
        &mut rpc,
        14,
        "settings.update",
        json!({ "changes": [{ "path": "backgroundAgents.defaultModel", "value": "auggie:opus" }] }),
    )
    .await;
    assert_eq!(update["result"]["applied"], json!([]), "{update}");
    assert_eq!(
        update["result"]["revision"], list["result"]["revision"],
        "retired-only updates must not advance the revision: {update}"
    );
    let get = wss_rpc(
        &mut rpc,
        15,
        "settings.get",
        json!({ "path": "quickActions.defaultModel" }),
    )
    .await;
    assert_eq!(
        get["result"]["value"],
        json!("auggie:haiku"),
        "an ignored retired write must not change the renamed key: {get}"
    );
}

/// The settings model triple over the wire: a user-authored config carrying
/// a legacy compound `model.default` (and an own-prefixed
/// `model.providerDefaults` entry) reads back over WSS as the split triple —
/// bare `model.default`, split-off `model.defaultProvider`, both with
/// `origin: file` — while the on-disk file stays untouched at load. The wire
/// keeps rejecting compound writes (`settings.update` is bare-id only), so
/// normalization is strictly read-side.
#[tokio::test]
async fn legacy_compound_model_default_reads_back_as_the_split_triple_over_wss() {
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let config_path = data_dir.join("config.toml");
    let seed =
        "[model]\ndefault = \"codex:gpt-5\"\nproviderDefaults = { codex = \"codex:gpt-5-mini\" }\n";
    std::fs::write(&config_path, seed).expect("seed legacy config.toml");

    let (_daemon, mut rpc, _sub) = boot_with_wss(&data_dir).await;

    // The compound reads back split: bare model + split-off provider, both
    // reporting file origin (the value came from the user's file, not a
    // schema default — origin badges must not mislabel it).
    let get = wss_rpc(
        &mut rpc,
        10,
        "settings.get",
        json!({ "path": "model.default" }),
    )
    .await;
    assert_eq!(get["result"]["value"], json!("gpt-5"), "{get}");
    assert_eq!(get["result"]["origin"], json!("file"), "{get}");
    let get = wss_rpc(
        &mut rpc,
        11,
        "settings.get",
        json!({ "path": "model.defaultProvider" }),
    )
    .await;
    assert_eq!(get["result"]["value"], json!("codex"), "{get}");
    assert_eq!(get["result"]["origin"], json!("file"), "{get}");

    // The own-prefixed providerDefaults entry reads back bare.
    let get = wss_rpc(
        &mut rpc,
        12,
        "settings.get",
        json!({ "path": "model.providerDefaults" }),
    )
    .await;
    assert_eq!(
        get["result"]["value"],
        json!({ "codex": "gpt-5-mini" }),
        "{get}"
    );

    // Normalization is read-side only: the user's model section is untouched
    // at load (the harness boot appends `[server.wsApi]`, so compare the
    // seeded lines, not the whole file).
    let text = std::fs::read_to_string(&config_path).expect("read config.toml");
    assert!(
        text.starts_with(seed),
        "normalization must not rewrite the user's model section: {text}"
    );

    // …and the wire still hard-rejects compound writes.
    let update = wss_rpc(
        &mut rpc,
        13,
        "settings.update",
        json!({ "changes": [{ "path": "model.default", "value": "codex:gpt-5" }] }),
    )
    .await;
    assert_eq!(update["error"]["code"], json!(-32602), "{update}");
}

/// One-time boot migration of the deprecated `providers.active`: a real
/// daemon boot carries the legacy value into `model.defaultProvider` and
/// removes the key from config.toml with a comment-preserving rewrite, all
/// observable over WSS (`settings.get` reports the carried value with
/// `origin: file` and the legacy key back at its schema default). A restart
/// from the migrated file leaves it byte-identical — the migration rewrite is
/// genuinely one-time.
#[tokio::test]
async fn active_provider_boot_migration_rewrites_config_once_over_wss() {
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let config_path = data_dir.join("config.toml");
    std::fs::write(
        &config_path,
        "# Operator comment — must survive the migration rewrite.\n\
         [providers]\n\
         active = \"codex\"\n\
         \n\
         [git]\n\
         autoCommit = false\n",
    )
    .expect("seed legacy config.toml");

    let migrated = {
        let (_daemon, mut rpc, _sub) = boot_with_wss(&data_dir).await;

        // The legacy value carried over, reading back over the wire with
        // file origin (it came from the user's config, not a schema default).
        let get = wss_rpc(
            &mut rpc,
            10,
            "settings.get",
            json!({ "path": "model.defaultProvider" }),
        )
        .await;
        assert_eq!(get["result"]["value"], json!("codex"), "{get}");
        assert_eq!(get["result"]["origin"], json!("file"), "{get}");

        // The legacy key is back at its schema default — no file layer left.
        let get = wss_rpc(
            &mut rpc,
            11,
            "settings.get",
            json!({ "path": "providers.active" }),
        )
        .await;
        assert_eq!(get["result"]["origin"], json!("default"), "{get}");

        // On disk: key removed, carried value written, comment and untouched
        // keys preserved (toml_edit comment-preserving rewrite).
        let text = std::fs::read_to_string(&config_path).expect("read config.toml");
        let has_active_key = text.lines().any(|l| {
            l.trim_start()
                .strip_prefix("active")
                .is_some_and(|rest| rest.trim_start().starts_with('='))
        });
        assert!(
            !has_active_key,
            "providers.active must be removed from the file: {text}"
        );
        assert!(
            text.contains("defaultProvider = \"codex\""),
            "the carried-over value must be persisted: {text}"
        );
        assert!(
            text.contains("# Operator comment — must survive the migration rewrite."),
            "user comment must survive the migration rewrite: {text}"
        );
        assert!(
            text.contains("autoCommit = false"),
            "untouched keys must survive the migration rewrite: {text}"
        );
        text
    }; // first daemon killed + data dir removed (Drop)

    // Restart on the migrated file: the migration finds no legacy key and
    // never rewrites — the file stays byte-identical across the boot. Drop
    // removed the data dir, so reseed a fresh one with the migrated bytes.
    std::fs::create_dir_all(&data_dir).expect("recreate data dir for restart");
    std::fs::write(&config_path, &migrated).expect("reseed migrated config.toml");
    let (_daemon, mut rpc, _sub) = boot_with_wss(&data_dir).await;
    let get = wss_rpc(
        &mut rpc,
        12,
        "settings.get",
        json!({ "path": "model.defaultProvider" }),
    )
    .await;
    assert_eq!(get["result"]["value"], json!("codex"), "{get}");
    assert_eq!(get["result"]["origin"], json!("file"), "{get}");
    let after = std::fs::read_to_string(&config_path).expect("re-read config.toml");
    assert_eq!(
        after, migrated,
        "a file without the legacy key is never rewritten at boot"
    );
}
