//! WSS end-to-end coverage for named provider profiles (PROTOCOL §5.54):
//! `providers.profiles.list | create | update | delete` request/response
//! envelopes, `invalid-params` field errors, `not-found`, delete blocking
//! (`provider-profile-in-use`) and `model.providerDefaults` pruning,
//! `settings.update` reference checks (`provider-profile-not-found`), the
//! `providers:profiles-changed` event, persistence across a daemon restart,
//! and that environment values never leave the daemon (wire, events,
//! metadata file).
//!
//! Drives a real TLS WebSocket connection against `intentd serve` (WSS listener enabled via config).

#![cfg(unix)]

use crate::common;

use std::path::{Path, PathBuf};
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
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

struct Daemon {
    child: GuardedChild,
    data_dir: PathBuf,
    scratch: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
        let _ = std::fs::remove_dir_all(&self.scratch);
    }
}

/// Short base under /tmp (UDS `SUN_LEN` cap); the returned guard removes the
/// root on drop — hold it for the full test (`INTENTD_TEST_KEEP_TMP` keeps
/// it). The `Daemon` drop still removes `data`/`scratch` first so the daemon
/// is dead before its tree goes away.
fn scratch_dir(prefix: &str) -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", &format!("itd-wss-prof-{prefix}-"))
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
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            // timing-guard: poll for the daemon socket bind under the startup deadline
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

async fn boot(root: &Path) -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = root.join("data");
    std::fs::create_dir_all(&data_dir).expect("mkdir data");
    let env: [(&str, &str); 1] = [("INTENTD_AUTH_TOKEN", TOKEN)];
    let child = spawn_serve(&data_dir, &env);
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let fp_hex = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint");
    let port = u16::try_from(status["result"]["port"].as_u64().expect("bound port"))
        .expect("value fits in u16");
    let cfg = client_config(fp_hex);
    let scratch = root.join("scratch");
    std::fs::create_dir_all(&scratch).expect("mkdir scratch");
    let daemon = Daemon {
        child,
        data_dir,
        scratch,
    };
    (daemon, port, cfg)
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

fn client_config(fp_hex: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("default protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fp_hex.to_string(),
            provider: provider.clone(),
        }))
        .with_no_client_auth();
    Arc::new(config)
}

async fn connect_ws(
    port: u16,
    tls_cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, tls_cfg, &url).await
}

type Ws = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

const SECRET: &str = "sk-e2e-profile-secret-value";

/// Send one request and return the full response envelope, skipping any
/// interleaved notifications.
async fn rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    let msg = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(msg.to_string().into()))
        .await
        .expect("send rpc");
    let deadline =
        tokio::time::Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(15));
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match timeout(remaining, ws.next()).await.expect("rpc timeout") {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert_eq!(v["jsonrpc"], "2.0");
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

async fn ok(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    let v = rpc(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "{method} failed: {v}");
    v["result"].clone()
}

async fn next_event(ws: &mut Ws, event_type: &str) -> Value {
    let deadline =
        tokio::time::Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(15));
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {event_type}"))
        {
            Some(Ok(Message::Text(text))) => {
                assert!(
                    !text.contains(SECRET),
                    "a profile value reached the wire: {text}"
                );
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" && v["params"]["event"]["type"] == event_type {
                    return v["params"]["event"].clone();
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

async fn connect(data_dir: &Path) -> (Ws, Ws) {
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().expect("bound port")).unwrap();
    let cfg = client_config(
        status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint"),
    );
    (
        connect_ws(port, cfg.clone()).await,
        connect_ws(port, cfg).await,
    )
}

#[tokio::test]
async fn provider_profiles_crud_contract_over_wss() {
    let root = scratch_dir("crud");
    let (mut daemon, _port, _cfg) = boot(root.path()).await;
    let data_dir = daemon.data_dir.clone();
    let (mut ws, mut sub) = connect(&data_dir).await;
    let ack = rpc(
        &mut sub,
        100,
        "events.subscribe",
        json!({ "eventTypes": ["providers:profiles-changed", "settings:changed"] }),
    )
    .await;
    assert!(ack.get("error").is_none(), "{ack}");

    // Empty list.
    let list = rpc(&mut ws, 1, "providers.profiles.list", json!({})).await;
    assert_eq!(
        list,
        json!({ "jsonrpc": "2.0", "id": 1, "result": { "profiles": [], "revision": 0 } })
    );

    // Validation: every failure listed, nothing written.
    let bad = rpc(
        &mut ws,
        2,
        "providers.profiles.create",
        json!({ "baseProviderId": "auggie", "name": " ", "env": [
            { "key": "HOME", "kind": "set", "value": "/x" },
            { "key": "X", "kind": "set", "value": "********" },
        ] }),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");
    assert_eq!(bad["error"]["data"]["code"], "invalid-params");
    let codes: Vec<(&str, &str)> = bad["error"]["data"]["fieldErrors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| (e["field"].as_str().unwrap(), e["code"].as_str().unwrap()))
        .collect();
    assert_eq!(
        codes,
        vec![
            ("baseProviderId", "base-provider-unsupported"),
            ("name", "name-blank"),
            ("env[0].key", "env-key-reserved"),
            ("env[1].value", "env-value-placeholder"),
        ]
    );
    assert!(bad["error"]["data"]["fieldErrors"][2]["message"]
        .as_str()
        .unwrap()
        .contains("CODEX_HOME"));
    assert!(!data_dir.join("provider-profiles.json").exists());

    // Create.
    let created = ok(
        &mut ws,
        3,
        "providers.profiles.create",
        json!({ "baseProviderId": "codex", "name": " Work ", "env": [
            { "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET },
            { "key": "EMPTY", "kind": "set", "value": "" },
            { "key": "OPENAI_BASE_URL", "kind": "unset" },
        ] }),
    )
    .await;
    assert!(!created.to_string().contains(SECRET));
    let profile = created["profile"].clone();
    let id = profile["id"].as_str().unwrap().to_string();
    let reference = format!("codex@{id}");
    assert_eq!(created["revision"], 1);
    assert_eq!(profile["providerRef"], reference.as_str());
    assert_eq!(profile["baseProviderId"], "codex");
    assert_eq!(profile["name"], "Work");
    assert_eq!(profile["backup"], false);
    assert_eq!(
        profile["env"],
        json!([
            { "key": "OPENAI_API_KEY", "kind": "set", "value": "********" },
            { "key": "EMPTY", "kind": "set", "value": "" },
            { "key": "OPENAI_BASE_URL", "kind": "unset" },
        ])
    );
    let mut keys: Vec<&String> = profile.as_object().unwrap().keys().collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "activation",
            "backup",
            "baseProviderId",
            "createdAt",
            "env",
            "id",
            "name",
            "providerRef",
            "settingsFingerprint",
            "updatedAt"
        ]
    );
    // No provider process runs this profile yet.
    assert_eq!(profile["activation"], json!({ "runningProcesses": 0 }));
    let ev = next_event(&mut sub, "providers:profiles-changed").await;
    assert_eq!(ev["workspaceId"], "");
    assert_eq!(
        ev["data"],
        json!({ "revision": 1, "change": "created", "profileId": id,
                "profile": { "id": id, "providerRef": reference, "baseProviderId": "codex", "name": "Work", "backup": false } })
    );

    // Values stay out of the metadata file and settings.list.
    let file = std::fs::read_to_string(data_dir.join("provider-profiles.json")).unwrap();
    assert!(
        !file.contains(SECRET) && !file.contains("OPENAI_API_KEY"),
        "{file}"
    );
    let settings = ok(&mut ws, 4, "settings.list", json!({})).await;
    assert!(!settings.to_string().contains(SECRET));
    let get = rpc(
        &mut ws,
        5,
        "settings.get",
        json!({ "path": "providerProfiles" }),
    )
    .await;
    assert_eq!(get["error"]["code"], -32602);

    // Update: keep the masked secret, rename.
    let updated = ok(
        &mut ws,
        6,
        "providers.profiles.update",
        json!({ "profileId": id, "name": "Work 2", "env": [
            { "key": "OPENAI_API_KEY", "kind": "keep" },
        ] }),
    )
    .await;
    assert_eq!(updated["revision"], 2);
    assert_eq!(updated["profile"]["name"], "Work 2");
    assert_eq!(updated["profile"]["providerRef"], reference.as_str());
    assert_eq!(
        updated["profile"]["env"],
        json!([{ "key": "OPENAI_API_KEY", "kind": "set", "value": "********" }])
    );
    assert_ne!(
        updated["profile"]["settingsFingerprint"],
        profile["settingsFingerprint"]
    );
    assert_eq!(
        updated["activation"],
        json!({ "envChanged": true, "runningProcesses": 0 })
    );
    let ev = next_event(&mut sub, "providers:profiles-changed").await;
    assert_eq!(ev["data"]["change"], "updated");
    assert_eq!(ev["data"]["revision"], 2);
    assert_eq!(ev["data"]["envChanged"], true);
    let secrets = std::fs::read_to_string(data_dir.join("secrets.json")).unwrap();
    assert!(secrets.contains(&format!("providerProfiles.{id}.env")));
    assert!(
        secrets.contains(SECRET),
        "the kept value is preserved in the secret store"
    );

    let missing = rpc(
        &mut ws,
        7,
        "providers.profiles.update",
        json!({ "profileId": "pnope", "name": "x" }),
    )
    .await;
    assert_eq!(missing["error"]["code"], -32602);
    assert_eq!(missing["error"]["data"]["code"], "not-found");
    let no_id = rpc(&mut ws, 8, "providers.profiles.delete", json!({})).await;
    assert_eq!(no_id["error"]["code"], -32602);

    // settings.update reference checks.
    let unknown = rpc(
        &mut ws,
        9,
        "settings.update",
        json!({ "changes": [{ "path": "model.defaultProvider", "value": "codex@pmissing" }] }),
    )
    .await;
    assert_eq!(unknown["error"]["code"], -32602);
    assert_eq!(
        unknown["error"]["data"]["code"],
        "provider-profile-not-found"
    );
    ok(
        &mut ws,
        10,
        "settings.update",
        json!({ "changes": [
            { "path": "model.defaultProvider", "value": reference },
            { "path": "model.providerDefaults", "value": { reference.clone(): "gpt-5" } },
        ] }),
    )
    .await;
    let _ = next_event(&mut sub, "settings:changed").await;

    // Delete is blocked while the default provider selects the profile.
    let blocked = rpc(
        &mut ws,
        11,
        "providers.profiles.delete",
        json!({ "profileId": id }),
    )
    .await;
    assert_eq!(blocked["error"]["code"], -32602, "{blocked}");
    assert_eq!(
        blocked["error"]["data"],
        json!({ "code": "provider-profile-in-use", "inUse": [{ "kind": "defaultProvider" }] })
    );
    ok(
        &mut ws,
        12,
        "settings.update",
        json!({ "changes": [{ "path": "model.defaultProvider", "value": "codex" }] }),
    )
    .await;
    let _ = next_event(&mut sub, "settings:changed").await;

    // Persistence across a real restart.
    daemon.child.kill().unwrap();
    daemon.child.wait().unwrap();
    let env: [(&str, &str); 1] = [("INTENTD_AUTH_TOKEN", TOKEN)];
    daemon.child = spawn_serve(&data_dir, &env);
    let (mut ws, mut sub) = connect(&data_dir).await;
    let ack = rpc(
        &mut sub,
        100,
        "events.subscribe",
        json!({ "eventTypes": ["providers:profiles-changed", "settings:changed"] }),
    )
    .await;
    assert!(ack.get("error").is_none(), "{ack}");
    let list = ok(
        &mut ws,
        20,
        "providers.profiles.list",
        json!({ "baseProviderId": "codex" }),
    )
    .await;
    assert_eq!(list["revision"], 0, "revision is process-local");
    assert_eq!(list["profiles"], json!([updated["profile"]]));

    // Delete removes both records, prunes providerDefaults, emits both events.
    let deleted = rpc(
        &mut ws,
        21,
        "providers.profiles.delete",
        json!({ "profileId": id }),
    )
    .await;
    assert_eq!(
        deleted,
        json!({ "jsonrpc": "2.0", "id": 21, "result": { "profileId": id, "revision": 1 } })
    );
    let ev = next_event(&mut sub, "providers:profiles-changed").await;
    assert_eq!(
        ev["data"],
        json!({ "revision": 1, "change": "deleted", "profileId": id })
    );
    let changed = next_event(&mut sub, "settings:changed").await;
    assert_eq!(
        changed["data"]["changes"][0]["path"],
        "model.providerDefaults"
    );
    assert_eq!(changed["data"]["changes"][0]["value"], json!({}));
    let secrets = std::fs::read_to_string(data_dir.join("secrets.json")).unwrap();
    assert!(!secrets.contains(&id), "the env secret is removed");
    let list = ok(&mut ws, 22, "providers.profiles.list", json!({})).await;
    assert_eq!(list["profiles"], json!([]));
    // Values never reach the settings file or the daemon log.
    let config = std::fs::read_to_string(data_dir.join("config.toml")).unwrap();
    assert!(!config.contains(SECRET), "{config}");
    let log = std::fs::read_to_string(data_dir.join("daemon.log")).unwrap();
    assert!(!log.is_empty(), "the daemon log is captured");
    assert!(
        !log.contains(SECRET),
        "a profile value reached the daemon log"
    );
    drop(daemon);
}

/// Backup flag and saved order over the wire (PROTOCOL §5.54): `backup`
/// round-trips through create/update/list/catalog, `providers.profiles.reorder`
/// validates the id set, returns the base's rows in the new order, emits one
/// `reordered` event, and the order and flags survive a daemon restart.
#[tokio::test]
async fn provider_profiles_backup_and_reorder_contract_over_wss() {
    let root = scratch_dir("reorder");
    let (mut daemon, _port, _cfg) = boot(root.path()).await;
    let data_dir = daemon.data_dir.clone();
    let (mut ws, mut sub) = connect(&data_dir).await;
    let ack = rpc(
        &mut sub,
        100,
        "events.subscribe",
        json!({ "eventTypes": ["providers:profiles-changed"] }),
    )
    .await;
    assert!(ack.get("error").is_none(), "{ack}");

    let mut ids = Vec::new();
    for (id, (base, name, backup)) in (1_i64..).zip([
        ("claude-code", "Main", None),
        ("claude-code", "Spare", Some(true)),
        ("codex", "Other", None),
        ("claude-code", "Spare 2", Some(false)),
    ]) {
        let mut params = json!({ "baseProviderId": base, "name": name });
        if let Some(backup) = backup {
            params["backup"] = json!(backup);
        }
        let created = ok(&mut ws, id, "providers.profiles.create", params).await;
        assert_eq!(created["profile"]["backup"], backup.unwrap_or(false));
        let ev = next_event(&mut sub, "providers:profiles-changed").await;
        assert_eq!(ev["data"]["profile"]["backup"], backup.unwrap_or(false));
        ids.push(created["profile"]["id"].as_str().unwrap().to_string());
    }
    let (main, spare, other, spare2) = (&ids[0], &ids[1], &ids[2], &ids[3]);

    // A backup-only update.
    let updated = ok(
        &mut ws,
        10,
        "providers.profiles.update",
        json!({ "profileId": spare2, "backup": true }),
    )
    .await;
    assert_eq!(updated["profile"]["backup"], true);
    assert_eq!(
        updated["activation"],
        json!({ "envChanged": false, "runningProcesses": 0 })
    );
    let ev = next_event(&mut sub, "providers:profiles-changed").await;
    assert_eq!(ev["data"]["change"], "updated");
    assert_eq!(ev["data"]["envChanged"], false);
    assert_eq!(ev["data"]["profile"]["backup"], true);
    for (id, value) in (40_i64..).zip([json!("yes"), Value::Null]) {
        let bad = rpc(
            &mut ws,
            id,
            "providers.profiles.update",
            json!({ "profileId": spare2, "backup": value }),
        )
        .await;
        assert_eq!(bad["error"]["code"], -32602, "{bad}");
        assert!(bad["error"]["data"].get("fieldErrors").is_none(), "{bad}");
    }
    let bad = rpc(
        &mut ws,
        30,
        "providers.profiles.create",
        json!({ "baseProviderId": "codex", "name": "Null", "backup": null }),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");

    // Invalid id sets are field errors.
    let bad = rpc(
        &mut ws,
        12,
        "providers.profiles.reorder",
        json!({ "baseProviderId": "claude-code", "profileIds": [spare, other] }),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");
    assert_eq!(bad["error"]["data"]["code"], "invalid-params");
    let codes: Vec<(&str, &str)> = bad["error"]["data"]["fieldErrors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| (e["field"].as_str().unwrap(), e["code"].as_str().unwrap()))
        .collect();
    assert_eq!(
        codes,
        vec![
            ("profileIds[1]", "profile-order-unknown"),
            ("profileIds", "profile-order-incomplete"),
        ]
    );
    let malformed = rpc(
        &mut ws,
        13,
        "providers.profiles.reorder",
        json!({ "baseProviderId": "claude-code" }),
    )
    .await;
    assert_eq!(malformed["error"]["code"], -32602, "{malformed}");

    // Reorder.
    let reordered = rpc(
        &mut ws,
        14,
        "providers.profiles.reorder",
        json!({ "baseProviderId": "claude-code", "profileIds": [spare2, main, spare] }),
    )
    .await;
    assert_eq!(reordered["jsonrpc"], "2.0");
    assert_eq!(reordered["id"], 14);
    let result = &reordered["result"];
    assert_eq!(result["revision"], 6);
    let order: Vec<&str> = result["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap())
        .collect();
    assert_eq!(order, [spare2.as_str(), main.as_str(), spare.as_str()]);
    let mut keys: Vec<&String> = result["profiles"][0].as_object().unwrap().keys().collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "activation",
            "backup",
            "baseProviderId",
            "createdAt",
            "env",
            "id",
            "name",
            "providerRef",
            "settingsFingerprint",
            "updatedAt"
        ]
    );
    let ev = next_event(&mut sub, "providers:profiles-changed").await;
    assert_eq!(ev["workspaceId"], "");
    assert_eq!(
        ev["data"],
        json!({ "revision": 6, "change": "reordered", "baseProviderId": "claude-code" })
    );

    // Claude Code's slots (0, 1, 3) take the new order; Codex keeps slot 2.
    let catalog = ok(&mut ws, 15, "providers.catalog", json!({})).await;
    let rows: Vec<(String, bool)> = catalog["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            (
                p["id"].as_str().unwrap().to_string(),
                p["backup"].as_bool().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            (spare2.clone(), true),
            (main.clone(), false),
            (other.clone(), false),
            (spare.clone(), true),
        ]
    );

    // Persistence across a real restart.
    daemon.child.kill().unwrap();
    daemon.child.wait().unwrap();
    let env: [(&str, &str); 1] = [("INTENTD_AUTH_TOKEN", TOKEN)];
    daemon.child = spawn_serve(&data_dir, &env);
    let (mut ws, _sub) = connect(&data_dir).await;
    let list = ok(
        &mut ws,
        20,
        "providers.profiles.list",
        json!({ "baseProviderId": "claude-code" }),
    )
    .await;
    let rows: Vec<(&str, bool)> = list["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| (p["id"].as_str().unwrap(), p["backup"].as_bool().unwrap()))
        .collect();
    assert_eq!(
        rows,
        vec![
            (spare2.as_str(), true),
            (main.as_str(), false),
            (spare.as_str(), true),
        ]
    );
    drop(daemon);
}

/// Selection surfaces over the wire (PROTOCOL §5.54): the catalog offers
/// profiles beside one row per provider type, a profile's auth status is
/// keyed by its reference, and a missing reference needs a replacement on
/// every account-check surface instead of reporting the Default account.
#[tokio::test]
async fn provider_profile_selection_surfaces_over_wss() {
    let root = scratch_dir("select");
    let (daemon, _port, _cfg) = boot(root.path()).await;
    let data_dir = daemon.data_dir.clone();
    let (mut ws, _sub) = connect(&data_dir).await;

    let created = ok(
        &mut ws,
        1,
        "providers.profiles.create",
        json!({ "baseProviderId": "codex", "name": "Work", "env": [
            { "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET },
        ] }),
    )
    .await;
    let profile = &created["profile"];
    let reference = profile["providerRef"].as_str().unwrap().to_string();

    let catalog = rpc(&mut ws, 2, "providers.catalog", json!({})).await;
    assert!(!catalog.to_string().contains(SECRET), "{catalog}");
    let catalog = &catalog["result"];
    let rows = catalog["providers"].as_array().unwrap();
    let supports =
        |id: &str| rows.iter().find(|r| r["id"] == id).unwrap()["supportsProfiles"].clone();
    assert_eq!(supports("codex"), json!(true));
    assert_eq!(supports("claude-code"), json!(true));
    assert_eq!(supports("auggie"), json!(false));
    assert_eq!(
        rows.iter().filter(|r| r["id"] == "codex").count(),
        1,
        "providers stays one row per type"
    );
    assert_eq!(
        catalog["profiles"],
        json!([{
            "id": profile["id"],
            "providerRef": reference,
            "baseProviderId": "codex",
            "name": "Work",
            "backup": false,
        }])
    );

    let status = rpc(
        &mut ws,
        3,
        "host.providerAuthStatus",
        json!({ "providerId": reference }),
    )
    .await;
    assert!(!status.to_string().contains(SECRET), "{status}");
    let entries = status["result"]["providers"].as_array().expect("{status}");
    assert_eq!(entries.len(), 1, "{status}");
    assert_eq!(entries[0]["id"], reference.as_str());

    for (id, method) in [
        (4, "host.providerAuthStatus"),
        (5, "host.providerTestPrompt"),
    ] {
        let missing = rpc(&mut ws, id, method, json!({ "providerId": "codex@pgone" })).await;
        assert_eq!(missing["error"]["code"], -32602, "{method}: {missing}");
        assert_eq!(
            missing["error"]["data"]["code"], "provider-profile-replacement-required",
            "{method}: {missing}"
        );
        assert_eq!(missing["error"]["data"]["providerRef"], "codex@pgone");
        assert_eq!(missing["error"]["data"]["reason"], "profile-missing");
    }

    let models = rpc(
        &mut ws,
        6,
        "models.list",
        json!({ "providerId": "codex@pgone" }),
    )
    .await;
    assert_eq!(
        models["error"]["data"]["code"], "provider-profile-replacement-required",
        "{models}"
    );

    // An agent on the profile: no activation state while nothing runs, and
    // after removal its next turn needs an explicit replacement.
    let workspace = ok(
        &mut ws,
        8,
        "workspace.create",
        json!({ "title": "Profiles WSS E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = workspace["workspace"]["id"].as_str().unwrap().to_string();
    let agent = ok(
        &mut ws,
        9,
        "agent.create",
        json!({ "workspaceId": ws_id, "provider": reference }),
    )
    .await;
    let agent_id = agent["agent"]["id"].as_str().unwrap().to_string();
    assert_eq!(agent["agent"]["provider"], reference.as_str());
    let get = ok(&mut ws, 10, "agent.get", json!({ "agentId": agent_id })).await;
    assert!(
        get["agent"].get("usingEarlierProfileSettings").is_none(),
        "{get}"
    );
    assert!(
        get["agent"].get("providerReplacementRequired").is_none(),
        "{get}"
    );
    ok(
        &mut ws,
        11,
        "providers.profiles.delete",
        json!({ "profileId": profile["id"] }),
    )
    .await;
    let get = ok(&mut ws, 12, "agent.get", json!({ "agentId": agent_id })).await;
    assert_eq!(
        get["agent"]["providerReplacementRequired"],
        json!({ "providerRef": reference, "reason": "profile-missing" }),
        "{get}"
    );
    // Sending is refused at admission with the structured error.
    let sent = rpc(
        &mut ws,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "next" }),
    )
    .await;
    assert_eq!(sent["error"]["code"], -32602, "{sent}");
    assert_eq!(
        sent["error"]["data"],
        json!({ "code": "provider-profile-replacement-required",
                "providerRef": reference, "reason": "profile-missing" }),
        "{sent}"
    );
    assert!(
        !sent["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&reference),
        "{sent}"
    );
    drop(daemon);
}
