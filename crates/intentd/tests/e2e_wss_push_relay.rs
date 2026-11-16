//! WSS end-to-end for the push relay methods (monorepo
//! `docs/protocol/methods/push.md`): `push.status`, `push.createLink` and
//! `push.revokeLink` against a real `intentd serve` whose relay origin
//! (`INTENTD_PUSH_RELAY_URL`) is a local fake. Linking needs
//! `push.relayEnabled`, is for host members (guests are refused), and a
//! revoke reaches the relay.

#![cfg(unix)]

use crate::common;

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{now_iso, Principal, PrincipalId, WorkspaceId};
use intent_store::Store;
use intentd_test_support::GuardedChild;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";
const MEMBER_TOKEN: &str = "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1";
const GUEST_TOKEN: &str = "adadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadad";

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
    common::test_tempdir_in("/tmp", &format!("itd-wss-push-{prefix}-"))
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

async fn boot(root: &Path, relay_url: &str) -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = root.join("data");
    std::fs::create_dir_all(&data_dir).expect("mkdir data");
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_PUSH_RELAY_URL", relay_url),
    ];
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

fn hash(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

/// A host member and a workspace guest, each with a per-principal token.
async fn seed_member_and_guest(data_dir: &Path) {
    let store = Store::open(&data_dir.join("intentd.db")).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&intent_core::Workspace {
            id: ws.clone(),
            title: "Push-E2E".into(),
            branch: "main".into(),
            created_at: now_iso(),
            updated_at: now_iso(),
            ..Default::default()
        })
        .await
        .unwrap();
    for (login, token) in [("member", MEMBER_TOKEN), ("guest", GUEST_TOKEN)] {
        let principal = Principal {
            id: PrincipalId::new(),
            identity: None,
            github_user_id: None,
            login: Some(login.into()),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.upsert_principal(&principal).await.unwrap();
        store
            .insert_principal_credential(&principal.id, &hash(token))
            .await
            .unwrap();
        if login == "member" {
            sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
                .bind(principal.id.as_str())
                .bind(now_iso())
                .execute(store.write_pool())
                .await
                .unwrap();
        } else {
            store
                .add_workspace_member(&ws, &principal.id, intent_core::WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
    }
}

async fn connect_as(port: u16, cfg: Arc<ClientConfig>, token: &str) -> Ws {
    common::wss_connect_with_retry(
        port,
        cfg,
        &format!("wss://localhost:{port}/ws?token={token}"),
    )
    .await
}

/// Answers like the relay: registration, link tokens, revocations and
/// publishes, recording each `METHOD path`.
async fn fake_relay() -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let log = log.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let head_end = loop {
                    let n = stream.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                };
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let length: usize = head
                    .lines()
                    .find_map(|l| {
                        l.split_once(':')
                            .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                    })
                    .and_then(|(_, v)| v.trim().parse().ok())
                    .unwrap_or(0);
                while buf.len() < head_end + length {
                    let n = stream.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let mut line = head.lines().next().unwrap_or("").split(' ');
                let method = line.next().unwrap_or("").to_string();
                let path = line.next().unwrap_or("").to_string();
                log.lock().unwrap().push(format!("{method} {path}"));
                let (status, reply) = match method.as_str() {
                    "POST" if path == "/v1/daemons" => (200, json!({ "daemonId": "relay-daemon" })),
                    "POST" => (
                        200,
                        json!({ "linkToken": "relay-daemon.secret", "expiresAt": 1_800_000_600 }),
                    ),
                    "PUT" => (200, json!({ "linkedDevices": 0, "pendingLinks": 1 })),
                    _ => (204, Value::Null),
                };
                let body = if reply.is_null() {
                    String::new()
                } else {
                    reply.to_string()
                };
                let response = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    (url, seen)
}

#[tokio::test]
async fn members_link_and_revoke_through_the_relay_over_wss() {
    let (relay_url, seen) = fake_relay().await;
    let root = scratch_dir("link");
    let data_dir = root.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    seed_member_and_guest(&data_dir).await;
    let (_daemon, port, cfg) = boot(root.path(), &relay_url).await;
    let mut owner = connect_ws(port, cfg.clone()).await;
    let mut member = connect_as(port, cfg.clone(), MEMBER_TOKEN).await;
    let mut guest = connect_as(port, cfg, GUEST_TOKEN).await;

    let off = rpc(&mut member, 1, "push.status", json!({})).await;
    assert_eq!(
        off["result"],
        json!({
            "enabled": false,
            "relayUrl": relay_url,
            "daemonId": null,
            "linked": false,
            "linkedDevices": null,
            "publishing": false,
        })
    );
    let refused = rpc(&mut member, 2, "push.createLink", json!({})).await;
    assert_eq!(refused["error"]["code"], json!(-32602), "{refused}");

    let enabled = rpc(
        &mut owner,
        3,
        "settings.update",
        json!({ "changes": [{ "path": "push.relayEnabled", "value": true }] }),
    )
    .await;
    assert!(enabled.get("error").is_none(), "{enabled}");

    let link = rpc(&mut member, 4, "push.createLink", json!({})).await;
    let link = &link["result"];
    assert_eq!(link["linkToken"], json!("relay-daemon.secret"));
    assert_eq!(link["expiresAt"], json!("2027-01-15T08:10:00Z"));
    assert_eq!(link["relayUrl"], json!(relay_url));
    let daemon_id = link["daemonId"].as_str().expect("daemonId").to_string();
    assert!(!daemon_id.is_empty());

    let on = rpc(&mut member, 5, "push.status", json!({})).await;
    assert_eq!(on["result"]["enabled"], json!(true));
    assert_eq!(on["result"]["linked"], json!(true));
    assert_eq!(on["result"]["daemonId"], json!(daemon_id));

    let guest_link = rpc(&mut guest, 6, "push.createLink", json!({})).await;
    assert!(guest_link.get("error").is_some(), "{guest_link}");

    let revoked = rpc(&mut member, 7, "push.revokeLink", json!({})).await;
    assert_eq!(revoked["result"], json!({ "revoked": true }));
    let after = rpc(&mut member, 8, "push.status", json!({})).await;
    assert_eq!(after["result"]["linked"], json!(false));

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.first().map(String::as_str), Some("POST /v1/daemons"));
    assert!(seen
        .iter()
        .any(|r| r.starts_with("POST /v1/daemons/") && r.ends_with("/links")));
    assert!(
        seen.iter().any(|r| r.starts_with("DELETE /v1/daemons/")),
        "{seen:?}"
    );
}
