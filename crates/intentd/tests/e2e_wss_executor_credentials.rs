//! WSS end-to-end coverage for daemon-to-daemon executor credentials
//! (monorepo `docs/protocol/methods/nodes.md`, "Machine methods: executor
//! credentials"): owner-only `executor.mint` / `list` / `revoke` against a
//! real `intentd serve`, members and guests refused with `-32003`, no
//! setting needed to mint (the retired `remoteAgents.enabled` is an unknown
//! setting), the minted routes matching the listener, and the token never
//! reaching the log.

#![cfg(unix)]

use crate::common;

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
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
    common::test_tempdir_in("/tmp", &format!("itd-wss-exec-{prefix}-"))
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

const MEMBER_TOKEN: &str = "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1";
const GUEST_TOKEN: &str = "adadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadad";

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
            title: "Executor-E2E".into(),
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

fn error_code(response: &Value) -> (i64, Value) {
    (
        response["error"]["code"].as_i64().unwrap_or_default(),
        response["error"]["data"]["code"].clone(),
    )
}

#[tokio::test]
async fn executor_credentials_are_owner_only_over_wss() {
    let root = scratch_dir("owner");
    let data_dir = root.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    seed_member_and_guest(&data_dir).await;
    let (daemon, port, cfg) = boot(root.path()).await;
    let socket = daemon.data_dir.join("intentd.sock");
    let status = common::await_wss_status(&socket).await;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .unwrap()
        .to_string();
    let mut owner = connect_ws(port, cfg.clone()).await;
    let mint_params = json!({ "backendId": "backend-1", "backendName": "Linux backend" });

    let retired = rpc(
        &mut owner,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "remoteAgents.enabled", "value": true }] }),
    )
    .await;
    assert!(retired.get("error").is_some(), "{retired}");

    let minted = rpc(&mut owner, 3, "executor.mint", mint_params.clone()).await;
    let result = minted["result"].clone();
    let token = result["token"].as_str().expect("token").to_string();
    assert_eq!(token.len(), 64);
    let credential_id = result["credentialId"].as_str().unwrap().to_string();
    let machine = &result["machine"];
    assert_eq!(machine["port"], json!(port));
    assert_eq!(machine["fingerprint"], json!(fingerprint));
    assert!(machine["daemonId"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));
    assert!(machine["name"]
        .as_str()
        .is_some_and(|name| !name.is_empty()));
    assert!(machine["hosts"].is_array());
    assert!(machine["platform"]["os"].is_string() && machine["platform"]["arch"].is_string());

    for token in [MEMBER_TOKEN, GUEST_TOKEN] {
        let mut other = connect_as(port, cfg.clone(), token).await;
        for (id, method, params) in [
            (10, "executor.mint", mint_params.clone()),
            (11, "executor.list", json!({})),
            (
                12,
                "executor.revoke",
                json!({ "credentialId": credential_id }),
            ),
        ] {
            let refused = rpc(&mut other, id, method, params).await;
            assert_eq!(refused["error"]["code"], -32003, "{method}: {refused}");
        }
    }

    let listed = rpc(&mut owner, 4, "executor.list", json!({})).await;
    let backends = listed["result"]["backends"]
        .as_array()
        .expect("backends")
        .clone();
    assert_eq!(backends.len(), 1, "{listed}");
    assert_eq!(backends[0]["credentialId"], json!(credential_id));
    assert_eq!(backends[0]["backendId"], "backend-1");
    assert_eq!(backends[0]["connected"], false);
    assert_eq!(backends[0]["runningAgents"], 0);
    assert!(!listed.to_string().contains(&token));

    let revoked = rpc(
        &mut owner,
        5,
        "executor.revoke",
        json!({ "credentialId": credential_id }),
    )
    .await;
    assert_eq!(revoked["result"], json!({ "ok": true }), "{revoked}");
    let again = rpc(
        &mut owner,
        6,
        "executor.revoke",
        json!({ "credentialId": credential_id }),
    )
    .await;
    assert_eq!(again["result"], json!({ "ok": true }), "{again}");
    let unknown = rpc(
        &mut owner,
        7,
        "executor.revoke",
        json!({ "credentialId": "nope" }),
    )
    .await;
    assert_eq!(
        error_code(&unknown),
        (-32602, json!("not-found")),
        "{unknown}"
    );
    let listed = rpc(&mut owner, 8, "executor.list", json!({})).await;
    assert_eq!(listed["result"], json!({ "backends": [] }));

    let log = std::fs::read_to_string(daemon.data_dir.join("daemon.log")).unwrap_or_default();
    assert!(
        !log.contains(&token),
        "the executor token reached the daemon log"
    );
}
