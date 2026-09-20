//! WSS end-to-end for the on-device `fm` route of `agent.completeOnce`
//! (§5.32): with `quickActions.localModel = "auto"` and a usable `fm`
//! (`INTENTD_FM_BIN` → fake script, which also lifts the macOS-only gate), the
//! first eligible call on a fresh daemon is served by auggie while the `fm`
//! probe warms off-path, then an eligible call returns `{ text }` produced by
//! `fm respond` over the real pinned-TLS WebSocket transport, and a
//! provider-bound `type` still routes to auggie. A second daemon proves the
//! `fm` failure path: a fake whose `respond` exits non-zero degrades to the
//! auggie reply with the §5.32 result shape unchanged.

#![cfg(unix)]

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use intentd_test_support::GuardedChild;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "fafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafa";

/// Short base under /tmp (UDS `SUN_LEN` cap); hold the guard for the whole
/// test — it sweeps on drop (`INTENTD_TEST_KEEP_TMP` keeps it).
fn scratch_dir(tag: &str) -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", &format!("itd-wss-fm-{tag}-"))
}

fn write_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let bin = dir.join(name);
    std::fs::write(&bin, format!("#!/bin/sh\n{body}\n")).expect("write fake script");
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    bin
}

/// Fake auggie: swallows stdin, replies with a fixed cleaned message.
fn fake_auggie(dir: &Path) -> PathBuf {
    write_script(
        dir,
        "auggie",
        "cat > /dev/null\nprintf '🤖\\nfrom-auggie\\n'",
    )
}

/// Fake `fm`: `available` / `license --status` exit 0; `respond` runs
/// `respond_body` with the prompt on stdin. Every invocation appends its argv
/// to `<dir>/fm-calls.log`.
fn fake_fm(dir: &Path, respond_body: &str) -> (PathBuf, PathBuf) {
    let log = dir.join("fm-calls.log");
    let body = format!(
        "echo \"$*\" >> '{}'\n[ \"$1\" = respond ] || exit 0\n{respond_body}",
        log.display()
    );
    (write_script(dir, "fm", &body), log)
}

fn fm_calls(log: &Path) -> Vec<String> {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

/// Boot `intentd serve` with the WSS listener, auggie pinned to `auggie`,
/// `model.defaultProvider = auggie`, `quickActions.localModel = auto`, and
/// `INTENTD_FM_BIN = fm`. Returns the guard, the bound port, and a pinned
/// client config.
async fn boot(root: &Path, auggie: &Path, fm: &Path) -> (GuardedChild, u16, Arc<ClientConfig>) {
    let data_dir = root.join("data");
    std::fs::create_dir_all(&data_dir).expect("mkdir data");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir workspaces");
    std::fs::write(
        data_dir.join("config.toml"),
        format!(
            "[context]\nauggiePath = {:?}\n\n[model]\ndefaultProvider = \"auggie\"\n\n\
             [quickActions]\nlocalModel = \"auto\"\n",
            auggie.to_string_lossy()
        ),
    )
    .expect("seed config.toml");
    common::enable_ws_api(&data_dir);
    let log_path = data_dir.join("daemon.log");
    let log = std::fs::File::create(&log_path).expect("create daemon log");
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_FM_BIN", fm)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    let mut child = GuardedChild::spawn(&mut cmd).expect("spawn intentd serve");
    let socket = data_dir.join("intentd.sock");
    common::await_daemon_listening(&mut child, &socket, &log_path).await;
    let status = common::await_wss_status_logged(&socket, &log_path).await;
    let fp_hex = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint");
    let port = u16::try_from(status["result"]["port"].as_u64().expect("bound port"))
        .expect("value fits in u16");
    (child, port, client_config(fp_hex))
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
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

type Ws = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> Ws {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC request and return the full response envelope for `id`.
async fn wss_call(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
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

/// The first eligible call on a fresh daemon: the fm probe cache is cold, so
/// the reply comes from auggie at once while the probe warms off-path. Then
/// repeat the call until the warm cache routes it to `fm respond` (`done`
/// inspects the fm call log) and return that response. Each iteration is a
/// full round trip through the fake auggie, so the loop is self-paced.
async fn first_call_cold_then_warm(
    ws: &mut Ws,
    log: &Path,
    params: Value,
    done: impl Fn(&[String]) -> bool,
) -> Value {
    let resp = wss_call(ws, 40, "agent.completeOnce", params.clone()).await;
    assert_eq!(resp["id"], 40);
    assert_eq!(
        resp["result"],
        json!({ "text": "from-auggie" }),
        "cold probe cache ⇒ provider route without waiting, got {resp}"
    );
    assert!(
        !fm_calls(log).iter().any(|c| c.starts_with("respond")),
        "the cold call never ran fm respond: {:?}",
        fm_calls(log)
    );
    let mut id = 41;
    timeout(std::time::Duration::from_secs(30), async {
        loop {
            let resp = wss_call(ws, id, "agent.completeOnce", params.clone()).await;
            assert_eq!(resp["id"], id);
            assert_eq!(resp["jsonrpc"], "2.0");
            if done(&fm_calls(log)) {
                return resp;
            }
            id += 1;
        }
    })
    .await
    .expect("the off-path probe warmed the cache and fm was attempted")
}

#[tokio::test]
async fn wss_complete_once_eligible_call_returns_fm_reply() {
    let root = scratch_dir("ok");
    let auggie = fake_auggie(root.path());
    let (fm, log) = fake_fm(root.path(), "printf 'fm says: '\ncat");
    let (_daemon, port, cfg) = boot(root.path(), &auggie, &fm).await;
    let mut ws = connect_ws(port, cfg).await;

    // Eligible: no `type`, small prompt, system prompt rides `-i`.
    let resp = first_call_cold_then_warm(
        &mut ws,
        &log,
        json!({ "prompt": "slug for login fix", "systemPrompt": "be terse" }),
        |calls| calls.iter().any(|c| c.starts_with("respond")),
    )
    .await;
    assert_eq!(
        resp["result"],
        json!({ "text": "fm says: slug for login fix" }),
        "the §5.32 result shape carries the fm reply, got {resp}"
    );
    assert_eq!(
        fm_calls(&log),
        vec![
            "available",
            "license --status",
            "respond --no-stream --greedy -i be terse",
        ],
        "probe once (off-path, cached), then respond with the prompt on stdin"
    );

    // A provider-bound type never touches fm: auggie answers.
    let resp = wss_call(
        &mut ws,
        60,
        "agent.completeOnce",
        json!({ "prompt": "msg", "type": "commit" }),
    )
    .await;
    assert_eq!(resp["id"], 60);
    assert_eq!(resp["result"], json!({ "text": "from-auggie" }), "{resp}");
    assert_eq!(fm_calls(&log).len(), 3, "type: commit spawned no fm");
}

#[tokio::test]
async fn wss_complete_once_fm_failure_falls_back_to_provider() {
    let root = scratch_dir("fallback");
    let auggie = fake_auggie(root.path());
    let (fm, log) = fake_fm(
        root.path(),
        "cat > /dev/null\necho 'transcript exceeded the model context size' >&2\nexit 1",
    );
    let (_daemon, port, cfg) = boot(root.path(), &auggie, &fm).await;
    let mut ws = connect_ws(port, cfg).await;

    let resp = first_call_cold_then_warm(
        &mut ws,
        &log,
        json!({ "prompt": "slug for login fix" }),
        |calls| calls.iter().any(|c| c.starts_with("respond")),
    )
    .await;
    assert_eq!(
        resp["result"],
        json!({ "text": "from-auggie" }),
        "an fm failure degrades to the provider reply with no error, got {resp}"
    );
    assert_eq!(
        fm_calls(&log).last().map(String::as_str),
        Some("respond --no-stream --greedy"),
        "fm was attempted before the provider route"
    );
}
