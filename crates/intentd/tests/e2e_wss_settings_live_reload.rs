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
//!    edit recovers.
//!
//! Adjacent coverage lives elsewhere and is intentionally not duplicated:
//! startup refusal on malformed config + flag-pin precedence in
//! `e2e_config_precedence.rs`, mixed-batch rollback in
//! `e2e_wss_settings_atomic_rollback.rs`, pinned-port rejection in
//! `e2e_wss_runtime_control.rs`.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
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
use uuid::Uuid;

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
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-livereload-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    // Guarantee the config-watcher readiness marker (INFO, target `intentd`)
    // reaches daemon.log even when the caller's RUST_LOG is stricter (e.g.
    // `warn`): append a crate-scoped directive, which EnvFilter resolves in
    // favor of the more specific target. `await_config_watcher_ready` gates
    // on that marker.
    let rust_log = match std::env::var("RUST_LOG") {
        Ok(v) if !v.is_empty() => format!("{v},intentd=info"),
        _ => "info".to_string(),
    };
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
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

/// Boot with the WSS listener enabled, discover the WSS port + fingerprint via
/// `system.status` over UDS, and return (daemon, rpc conn, subscriber conn)
/// with the subscriber already subscribed to `settings:changed`.
async fn boot_with_wss(data_dir: &Path) -> (Daemon, Wss, Wss) {
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
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

/// §5.12 scenario 1: `settings.update` over WSS rewrites config.toml on disk
/// (atomic, comment-preserving) and emits `settings:changed` to WSS
/// subscribers; envelope shapes match PROTOCOL §5.12 (plus the additive
/// `origin` field on reads).
#[tokio::test]
async fn settings_update_over_wss_rewrites_config_toml_and_emits_event() {
    let data_dir = temp_data_dir();
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

/// Client-local notification sound paths use the existing TOML lifecycle:
/// absent defaults, string-only updates, persistence, clearing and reset.
#[tokio::test]
async fn notification_sound_path_round_trips_and_resets_over_wss() {
    let data_dir = temp_data_dir();
    let config_path = data_dir.join("config.toml");
    std::fs::write(&config_path, "[notifications]\nsoundEnabled = false\n")
        .expect("seed legacy notification config");
    let (_daemon, mut rpc, mut sub) = boot_with_wss(&data_dir).await;
    let path = "notifications.soundPath";
    let custom = "/desktop-only/My sounds/通知.mp3";

    let get = wss_rpc(&mut rpc, 10, "settings.get", json!({ "path": path })).await;
    assert_eq!(get["jsonrpc"], "2.0");
    assert_eq!(get["id"], 10);
    assert_eq!(get["result"]["path"], path);
    assert_eq!(get["result"]["value"], "");
    assert_eq!(get["result"]["origin"], "default");
    assert_eq!(get["result"]["definition"]["type"], "string");
    assert_eq!(get["result"]["definition"]["defaultValue"], "");
    let initial_revision = get["result"]["revision"].clone();
    let original = std::fs::read_to_string(&config_path).unwrap();

    for invalid in [json!(true), json!(42), json!([]), json!({}), Value::Null] {
        let update = wss_rpc(
            &mut rpc,
            11,
            "settings.update",
            json!({ "changes": [
                { "path": "notifications.soundEnabled", "value": true },
                { "path": path, "value": invalid }
            ] }),
        )
        .await;
        assert_eq!(update["jsonrpc"], "2.0");
        assert_eq!(update["id"], 11);
        assert_eq!(update["error"]["code"], -32602, "{update}");
    }
    let get = wss_rpc(&mut rpc, 12, "settings.get", json!({ "path": path })).await;
    assert_eq!(get["result"]["value"], "");
    assert_eq!(get["result"]["revision"], initial_revision);
    assert_eq!(std::fs::read_to_string(&config_path).unwrap(), original);

    for value in [custom, "", custom] {
        let update = wss_rpc(
            &mut rpc,
            13,
            "settings.update",
            json!({ "changes": [{ "path": path, "value": value }] }),
        )
        .await;
        assert_eq!(update["jsonrpc"], "2.0");
        assert_eq!(update["id"], 13);
        let applied = json!([{ "path": path, "value": value, "origin": "file" }]);
        assert_eq!(update["result"]["applied"], applied);
        let event = next_settings_event(&mut sub).await;
        assert_eq!(event["params"]["event"]["data"]["changes"], applied);
        assert_eq!(
            event["params"]["event"]["data"]["revision"],
            update["result"]["revision"]
        );
        let get = wss_rpc(&mut rpc, 14, "settings.get", json!({ "path": path })).await;
        assert_eq!(get["result"]["value"], value);
        assert_eq!(get["result"]["origin"], "file");
        let saved = intent_core::settings_file::SettingsFile::load_or_init(&config_path)
            .expect("load persisted config");
        assert_eq!(saved.notifications.sound_path, value);
        assert!(!saved.notifications.sound_enabled);
    }

    let reset = wss_rpc(&mut rpc, 15, "settings.reset", json!({ "path": path })).await;
    assert_eq!(reset["jsonrpc"], "2.0");
    assert_eq!(reset["id"], 15);
    assert_eq!(reset["result"]["path"], path);
    assert_eq!(reset["result"]["value"], "");
    assert_eq!(reset["result"]["origin"], "default");
    let event = next_settings_event(&mut sub).await;
    assert_eq!(
        event["params"]["event"]["data"]["changes"],
        json!([{ "path": path, "value": "", "origin": "default" }])
    );
    assert_eq!(
        event["params"]["event"]["data"]["revision"],
        reset["result"]["revision"]
    );
    assert!(!std::fs::read_to_string(&config_path)
        .unwrap()
        .contains("soundPath"));
    let saved = intent_core::settings_file::SettingsFile::load_or_init(&config_path)
        .expect("load reset config");
    assert!(saved.notifications.sound_path.is_empty());
    assert!(!saved.notifications.sound_enabled);
}

/// §9.8 scenarios 2 + 3: an external editor-style edit of config.toml
/// live-reloads (settings:changed over WSS + settings.get reflects it), an
/// invalid edit (syntax error, then unknown key) keeps last-good values with
/// the daemon up, and a subsequent valid edit recovers.
#[tokio::test]
async fn external_edit_live_reloads_and_invalid_edit_keeps_last_good() {
    let data_dir = temp_data_dir();
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
    let data_dir = temp_data_dir();
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
