//! WSS end-to-end lifecycle-driven watcher registration (#611): a workspace
//! created AFTER the daemon starts serving is picked up by the watcher
//! registry at runtime — no restart — so mutating its project-tier
//! specialists directory emits `specialists:changed` over a live WSS
//! subscription; deleting the workspace deregisters the watch and further
//! mutations stay silent. Mirrors the harness of
//! `e2e_wss_specialists_changed.rs` (which needs a restart because it
//! predates runtime registration) but boots exactly once.

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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-lifecycle-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
    dir
}

/// Spawn `intentd serve` with a hermetic HOME so the user-tier specialists
/// directory (`~/.intent/specialists`) never touches the real home.
fn spawn_serve(data_dir: &Path, home_dir: &Path) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", "0")
        .env("HOME", home_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn intentd serve")
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

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
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

/// Wait up to `window` for the next `events.event` notification whose payload
/// `type` matches one of `types`; ignore other frames. Returns the event
/// object (the `params.event` sub-object), or `None` if the window elapses
/// without a match.
async fn try_next_event<S>(
    ws: &mut WebSocketStream<S>,
    types: &[&str],
    window: Duration,
) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + window;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let Ok(next) = timeout(remaining, ws.next()).await else {
            return None;
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    let evt = &v["params"]["event"];
                    let ty = evt["type"].as_str().unwrap_or("");
                    if types.contains(&ty) {
                        return Some(evt.clone());
                    }
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

/// Drain any additional `events.event` frames matching `event_type` in
/// `window_ms`; return the first extra observed, or `None` if the socket
/// stayed quiet.
async fn drain_extra<S>(
    ws: &mut WebSocketStream<S>,
    event_type: &str,
    window_ms: u64,
) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    timeout(Duration::from_millis(window_ms), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = match serde_json::from_str(&text) {
                        Ok(x) => x,
                        Err(_) => continue,
                    };
                    if v["method"] == json!("events.event")
                        && v["params"]["event"]["type"] == json!(event_type)
                    {
                        return v;
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => panic!("subscription socket errored during drain: {e:?}"),
                None => panic!("subscription socket closed during drain"),
            }
        }
    })
    .await
    .ok()
}

fn specialist_md(name: &str, body: &str) -> String {
    format!("---\nname: \"{name}\"\ndescription: \"d\"\n---\n\n{body}")
}

/// End-to-end #611 lifecycle behavior over a single daemon boot: a workspace
/// created after `intentd serve` is already up gains watching at runtime (a
/// project-tier specialist write emits `specialists:changed` to a subscribed
/// WSS client, no restart needed), and `workspace.delete` deregisters the
/// watch (a subsequent write stays silent).
#[tokio::test]
async fn workspace_created_after_serve_gains_watching_and_deletion_stops_it() {
    let data_dir = scratch_dir("data");
    let home_dir = data_dir.join("home");
    std::fs::create_dir_all(&home_dir).expect("mkdir hermetic home");
    // On-disk checkout whose project tier already exists at create time, so
    // the runtime registration places a recursive watch on it directly.
    let checkout = data_dir.join("checkout");
    let specialists_dir = checkout.join(".intent").join("specialists");
    std::fs::create_dir_all(&specialists_dir).expect("mkdir project specialists tier");

    // Single boot: the watcher registry starts with no workspaces.
    let child = spawn_serve(&data_dir, &home_dir);
    let _guard = common::DaemonGuard::new(child, data_dir.clone(), true);
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

    // Create the workspace AFTER the daemon is serving: `workspace:created`
    // drives the runtime watcher registration (#611).
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({
            "title": "Lifecycle",
            "branch": "main",
            "skipWorktree": true,
            "path": checkout.to_string_lossy(),
        }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["specialists:changed"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Mutate: create a project-tier specialist file. The watch was placed by
    // runtime registration, so this must emit without any daemon restart.
    //
    // Retry-until-observed (intent-hq/monorepo#1622): registration is async
    // (#611) and the OS watch (FSEvents/inotify) can take arbitrarily long to
    // establish under load, so a fixed warm-up sleep races it — a write that
    // lands before the watch is live never emits. Instead, write the file and
    // wait a short window for `specialists:changed`; on a miss, rewrite with
    // changed content (once the watch is live the next mutation emits) and
    // retry within one scaled overall budget.
    let overall = common::test_timeout(Duration::from_secs(30));
    let deadline = tokio::time::Instant::now() + overall;
    let attempt_window = common::test_timeout(Duration::from_secs(2));
    let mut attempt = 0u32;
    let evt = loop {
        attempt += 1;
        std::fs::write(
            specialists_dir.join("custom.md"),
            specialist_md("Custom", &format!("project-tier body rev {attempt}")),
        )
        .expect("write specialist");
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "no specialists:changed after {attempt} write attempts within {overall:?}"
        );
        if let Some(evt) = try_next_event(
            &mut sub,
            &["specialists:changed"],
            attempt_window.min(remaining),
        )
        .await
        {
            break evt;
        }
    };
    assert_eq!(evt["type"], json!("specialists:changed"));
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(evt["actor"], json!({ "type": "system" }));
    assert_eq!(evt["data"], json!({ "workspaceId": ws_id }));

    // Retry writes may have queued further debounced emissions; drain until
    // the socket stays quiet so leftovers cannot pollute the post-delete
    // silence assertion (window comfortably beyond the 500ms debounce and
    // scaled like the other waits, so a load-delayed debounce cannot slip
    // past the drain).
    let drain_window_ms =
        u64::try_from(common::test_timeout(Duration::from_secs(1)).as_millis()).unwrap_or(u64::MAX);
    while drain_extra(&mut sub, "specialists:changed", drain_window_ms)
        .await
        .is_some()
    {}

    // Delete the workspace: `workspace:deleted` deregisters the watch. The
    // caller-supplied checkout survives (skipWorktree), so writes into the
    // same directory still happen on disk — they just must no longer emit.
    let delete = uds_rpc(
        &socket,
        3,
        "workspace.delete",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(delete["result"]["success"], json!(true), "delete: {delete}");
    // Settle window for watch deregistration, scaled so heavy load cannot
    // race the deregistration itself (intent-hq/monorepo#1622).
    tokio::time::sleep(common::test_timeout(Duration::from_millis(750))).await;

    std::fs::write(
        specialists_dir.join("after-delete.md"),
        specialist_md("AfterDelete", "must not emit"),
    )
    .expect("write specialist after delete");

    // Silence window comfortably beyond the 500ms watcher debounce.
    let extra = drain_extra(&mut sub, "specialists:changed", 2000).await;
    assert!(
        extra.is_none(),
        "deleted workspace must stop emitting specialists:changed, got: {extra:?}"
    );
}
