//! WSS e2e: captured login-shell credential env merges into `host.exec`
//! spawns with the documented precedence — caller-supplied `env` wins, then
//! the daemon's own process env (never overridden), and captured allow-listed
//! login-shell vars fill gaps only (intent-hq/monorepo#1671).
//!
//! The login-shell capture is cached in a process-global `OnceLock` inside
//! `intent-core`, so an in-process harness cannot vary the captured map per
//! test. This suite instead drives a real `intentd serve` subprocess — a
//! fresh process gets a fresh capture — with `SHELL` pointed at a fake shell
//! script that exports sentinel allow-listed vars (`CODEX_` prefix) before
//! running the capture command. Assertions run over a real pinned-TLS WSS
//! connection, and the daemon log is checked to prove no captured value ever
//! leaks anywhere other than the child's own stdout.

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

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// Allow-listed (`CODEX_` prefix) var exported ONLY by the fake login shell —
/// absent from the daemon's start env, so the capture must fill the gap.
const GAP_VAR: &str = "CODEX_E2E_SENTINEL";
const GAP_VALUE: &str = "shell-sentinel-value";

/// Allow-listed var exported by the fake shell AND set in the daemon's own
/// start env with a different value — the daemon env must win.
const DAEMON_VAR: &str = "CODEX_E2E_DAEMON_SET";
const DAEMON_VALUE: &str = "daemon-env-value";
const SHELL_SHADOW_VALUE: &str = "shell-shadow-value";

/// Live `intentd serve` process; killed and its data dir removed on drop.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-credenv-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

/// Write the fake login shell: exports the sentinel vars, then execs the
/// capture command (`$2` after the `-ilc` / `-lc` flag) through /bin/sh so
/// the daemon's sentinel-wrapped PATH + `env -0` capture succeeds.
fn write_fake_shell(data_dir: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = data_dir.join("fake-login-shell.sh");
    let script = format!(
        "#!/bin/sh\nexport {GAP_VAR}='{GAP_VALUE}'\nexport {DAEMON_VAR}='{SHELL_SHADOW_VALUE}'\nshift\nexec /bin/sh -c \"$1\"\n"
    );
    std::fs::write(&path, script).expect("write fake shell");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("chmod fake shell");
    path
}

/// Spawn `intentd serve` with `SHELL` pointing at the fake login shell, the
/// daemon-env sentinel set, and the gap sentinel scrubbed from the inherited
/// env so only the login-shell capture can supply it.
fn spawn_serve(data_dir: &Path, fake_shell: &Path) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", "0")
        .env("SHELL", fake_shell)
        .env(DAEMON_VAR, DAEMON_VALUE)
        .env_remove(GAP_VAR)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
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
async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the result whose id matches.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let next = tokio::time::timeout_at(deadline, ws.next())
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

/// Boot the daemon with the fake login shell and return the live handle plus
/// a pinned WSS client config and the bound TCP port.
async fn boot() -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = temp_data_dir();
    let fake_shell = write_fake_shell(&data_dir);
    let child = spawn_serve(&data_dir, &fake_shell);
    let daemon = Daemon {
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
    (daemon, port, client_config(&fingerprint))
}

/// Run `sh -c <script>` via `host.exec` (argv only — the daemon never shells
/// out itself) and return the child's trimmed stdout.
async fn exec_stdout<S>(ws: &mut WebSocketStream<S>, id: i64, script: &str, env: Value) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let out = wss_rpc(
        ws,
        id,
        "host.exec",
        json!({
            "command": "sh",
            "args": ["-c", script],
            "env": env,
            "timeoutMs": 15000,
        }),
    )
    .await;
    assert_eq!(out["exitCode"], 0, "exec succeeded: {out}");
    out["stdout"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// Precedence contract over the real wire: captured login-shell var fills the
/// gap in the child env; caller-supplied env overrides it; a var already in
/// the daemon's own process env is never overridden by the capture. Finally,
/// no captured value leaks into the daemon log.
#[tokio::test]
async fn host_exec_merges_captured_credential_env_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    // 1) Gap fill: the sentinel is NOT in the daemon's start env, so only the
    // login-shell capture can have delivered it to the child.
    let gap = exec_stdout(
        &mut ws,
        300,
        &format!("printf %s \"${GAP_VAR}\""),
        json!({}),
    )
    .await;
    assert_eq!(gap, GAP_VALUE, "captured login-shell var fills the gap");

    // 2) Caller env wins over the captured var.
    let overridden = exec_stdout(
        &mut ws,
        301,
        &format!("printf %s \"${GAP_VAR}\""),
        json!({ GAP_VAR: "caller-wins" }),
    )
    .await;
    assert_eq!(overridden, "caller-wins", "caller-supplied env wins");

    // 3) Daemon process env wins over the shell's shadow value.
    let daemon_owned = exec_stdout(
        &mut ws,
        302,
        &format!("printf %s \"${DAEMON_VAR}\""),
        json!({}),
    )
    .await;
    assert_eq!(
        daemon_owned, DAEMON_VALUE,
        "daemon's own env is never overridden by the capture"
    );

    // 4) Secret safety: captured values never appear anywhere but the child's
    // own stdout — in particular, not in the daemon log.
    let log = std::fs::read_to_string(daemon.data_dir.join("daemon.log")).unwrap_or_default();
    assert!(
        !log.contains(GAP_VALUE) && !log.contains(SHELL_SHADOW_VALUE),
        "captured credential values must never be logged"
    );
}
