//! WSS e2e for the batch `agent.delegate` form (PROTOCOL §5.5): drives the
//! real pinned-TLS WebSocket against a live `intentd serve` and asserts the
//! `tasks: [entry]` request/response contract:
//!
//! - ready tasks start (per-task agent creation + assignment), dep-blocked
//!   tasks are `held:blocked-on-deps` naming the unmet ids, conflicting tasks
//!   are `held:conflict` naming the pair, and the `unlockPlan` projects what
//!   settlement unlocks;
//! - re-calling with the same list is idempotent (`skipped` with the running
//!   agent id, nothing new starts);
//! - object entries (`{ taskNoteId, specialist?, model?, reasoningEffort? }`)
//!   deserialize alongside bare-string ids;
//! - the removed `greedy` param is rejected with `-32602`;
//! - mixing `tasks` with `taskNoteId` is rejected with `-32602`.
//!
//! Gated on `node` + the mock ACP agent fixture (same gate as the other
//! delegate e2e suites).

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

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-delegbatch-{}", &id[..8]));
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

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// One WSS JSON-RPC round-trip, returning the raw envelope (caller checks
/// `error`/`result` itself). Asserts the JSON-RPC 2.0 response envelope
/// shape per PROTOCOL.md §1: `jsonrpc: "2.0"`, the echoed `id`, and exactly
/// one of `result` / `error`.
async fn wss_rpc_raw<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
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
                    assert_eq!(v["jsonrpc"], json!("2.0"), "rpc {method} envelope: {v}");
                    let obj = v.as_object().expect("response object");
                    assert!(
                        obj.contains_key("result") ^ obj.contains_key("error"),
                        "rpc {method} must carry exactly one of result/error: {v}"
                    );
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// [`wss_rpc_raw`], asserting the call succeeded and returning `result`.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let v = wss_rpc_raw(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

fn gate() -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping WSS delegate-batch E2E: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping WSS delegate-batch E2E: mock script missing at {script}");
        return None;
    }
    Some(script)
}

/// Create a note and mark it as a task; returns the note id. `depends_on` /
/// `conflicts_with` seed the relation lists at markAsTask time.
async fn seed_task<S>(
    ws: &mut WebSocketStream<S>,
    id_base: i64,
    ws_id: &str,
    title: &str,
    depends_on: &[&str],
    conflicts_with: &[&str],
    effort: Option<&str>,
) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let created = wss_rpc(
        ws,
        id_base,
        "note.create",
        json!({ "workspaceId": ws_id, "title": title, "content": format!("{title} body") }),
    )
    .await;
    let note_id = created["note"]["id"].as_str().expect("note id").to_string();
    let mut params = json!({
        "workspaceId": ws_id,
        "noteId": note_id,
        "status": "not_started",
    });
    if !depends_on.is_empty() {
        params["dependsOn"] = json!(depends_on);
    }
    if !conflicts_with.is_empty() {
        params["conflictsWith"] = json!(conflicts_with);
    }
    if let Some(effort) = effort {
        params["effort"] = json!(effort);
    }
    let marked = wss_rpc(ws, id_base + 1, "task.markAsTask", params).await;
    assert_eq!(marked["ok"], json!(true), "markAsTask ok: {marked}");
    note_id
}

fn row_for<'a>(resp: &'a Value, id: &str) -> &'a Value {
    resp["tasks"]
        .as_array()
        .expect("tasks array")
        .iter()
        .find(|r| r["taskNoteId"] == json!(id))
        .unwrap_or_else(|| panic!("row for {id} in {resp}"))
}

/// The full batch request/response shape over the wire: start + hold + skip
/// dispositions, unlock plan (including the mixed-estimate shadowing shape
/// for `criticalPathMinutes`, monorepo#2128), idempotent re-call, object
/// entries, the greedy-removal rejection, and the mixed-addressing rejection.
#[tokio::test]
async fn batch_delegate_request_response_shape_over_wss() {
    let Some(script) = gate() else {
        return;
    };

    let data_dir = temp_data_dir();
    let behavior = json!({ "response": "mock response" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
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
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut ws = connect_ws(port, cfg).await;

    let ws_result = wss_rpc(
        &mut ws,
        10,
        "workspace.create",
        json!({ "title": "Batch delegate E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // t1 ready (~1h); t2 dependsOn t1 (~30 min); t3 conflictsWith t1 (no
    // estimate → 30-min default in the critical-path math).
    let t1 = seed_task(&mut ws, 20, &ws_id, "T1", &[], &[], Some("~1h")).await;
    let t2 = seed_task(&mut ws, 22, &ws_id, "T2", &[&t1], &[], Some("30 min")).await;
    let t3 = seed_task(&mut ws, 24, &ws_id, "T3", &[], &[&t1], None).await;

    let resp = wss_rpc(
        &mut ws,
        30,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "tasks": [t1, t2, t3],
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(resp["ok"], json!(true), "batch ok: {resp}");
    assert!(
        resp.as_object().unwrap().get("greedy").is_none(),
        "greedy echo removed from the result: {resp}"
    );
    assert_eq!(resp["tasks"].as_array().unwrap().len(), 3);

    let r1 = row_for(&resp, &t1);
    assert_eq!(r1["disposition"], json!("started"), "{resp}");
    let agent_id = r1["agentId"].as_str().expect("agentId").to_string();
    assert_eq!(r1["title"], json!("T1"));
    assert_eq!(resp["startedTaskIds"], json!([t1]));

    let r2 = row_for(&resp, &t2);
    assert_eq!(r2["disposition"], json!("held:blocked-on-deps"));
    assert_eq!(r2["unmetDependsOn"], json!([t1]));
    assert!(r2["reason"].as_str().unwrap().contains(&t1), "{r2}");

    let r3 = row_for(&resp, &t3);
    assert_eq!(r3["disposition"], json!("held:conflict"));
    assert_eq!(r3["conflictsWith"], json!([t1]));
    assert!(
        r3["reason"]
            .as_str()
            .unwrap()
            .contains("delegate it individually to force it"),
        "{r3}"
    );

    // Unlock plan: t2 and t3 both become startable when t1 settles.
    let unlocked = resp["unlockPlan"]["unlockedBySettlement"]
        .as_array()
        .expect("unlockedBySettlement");
    assert!(
        unlocked.contains(&json!(t2)) && unlocked.contains(&json!(t3)),
        "unlock plan projects both held tasks: {resp}"
    );
    assert!(
        resp["unlockPlan"]["message"]
            .as_str()
            .unwrap()
            .contains("re-call agent.delegate"),
        "{resp}"
    );
    // Estimates exist (t1 ~1h, t2 30 min), so the plan surfaces the
    // effort-weighted critical path: t1 → t2 = 60 + 30 = 90 min (t3's
    // 30-min-default chain is shorter).
    assert_eq!(
        resp["unlockPlan"]["criticalPathMinutes"],
        json!(90),
        "{resp}"
    );
    assert!(
        resp["unlockPlan"]["message"]
            .as_str()
            .unwrap()
            .contains("~90 min of serial work remains on the critical path"),
        "{resp}"
    );

    // Idempotent re-call: t1 skips naming its running agent; nothing starts.
    let again = wss_rpc(
        &mut ws,
        40,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "tasks": [t1, t2, t3],
            "model": "mock:default",
        }),
    )
    .await;
    let r1 = row_for(&again, &t1);
    assert_eq!(r1["disposition"], json!("skipped"), "{again}");
    assert_eq!(r1["agentId"], json!(agent_id));
    assert!(
        r1["reason"]
            .as_str()
            .unwrap()
            .contains("already being worked"),
        "{r1}"
    );
    assert_eq!(
        row_for(&again, &t2)["disposition"],
        json!("held:blocked-on-deps")
    );
    assert_eq!(row_for(&again, &t3)["disposition"], json!("held:conflict"));
    assert_eq!(again["startedTaskIds"], json!([] as [String; 0]));

    // The removed greedy param is rejected with -32602 pointing at
    // individual delegation.
    let err = wss_rpc_raw(
        &mut ws,
        50,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "tasks": [t3],
            "greedy": true,
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(err["error"]["code"], json!(-32602), "{err}");
    assert!(
        err["error"]["message"].as_str().unwrap().contains(
            "greedy was removed; delegate a held task individually to force it past the conflict hold"
        ),
        "{err}"
    );

    // Object entries deserialize alongside bare strings: t3 is still held
    // (conflict with running t1), and the row shape is unchanged.
    let mixed = wss_rpc(
        &mut ws,
        55,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "tasks": [{ "taskNoteId": t3, "model": "mock:default", "reasoningEffort": "low" }],
        }),
    )
    .await;
    let r3 = row_for(&mixed, &t3);
    assert_eq!(r3["disposition"], json!("held:conflict"), "{mixed}");
    assert_eq!(r3["conflictsWith"], json!([t1]));

    // Mixed addressing is rejected with -32602.
    let err = wss_rpc_raw(
        &mut ws,
        60,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "tasks": [t2],
            "taskNoteId": t2,
        }),
    )
    .await;
    assert_eq!(err["error"]["code"], json!(-32602), "{err}");
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("mutually exclusive"),
        "{err}"
    );

    // Mixed-estimate shadowing (monorepo#2128): an estimated chain
    // s1(10) → s2(15) → s3(5) = 30 min alongside a longer pure-defaults
    // chain u1 → u2 = 60 min. The unestimated chain must not suppress the
    // number — the plan reports the estimated chain's 30.
    let s1 = seed_task(&mut ws, 70, &ws_id, "S1", &[], &[], Some("10 min")).await;
    let s2 = seed_task(&mut ws, 72, &ws_id, "S2", &[&s1], &[], Some("15 min")).await;
    let _s3 = seed_task(&mut ws, 74, &ws_id, "S3", &[&s2], &[], Some("5 min")).await;
    let u1 = seed_task(&mut ws, 76, &ws_id, "U1", &[], &[], None).await;
    let _u2 = seed_task(&mut ws, 78, &ws_id, "U2", &[&u1], &[], None).await;
    let shadowed = wss_rpc(
        &mut ws,
        90,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "tasks": [s1, u1],
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(shadowed["ok"], json!(true), "{shadowed}");
    assert_eq!(
        shadowed["unlockPlan"]["criticalPathMinutes"],
        json!(30),
        "estimated chain reported despite longer defaults chain: {shadowed}"
    );
    assert!(
        shadowed["unlockPlan"]["message"]
            .as_str()
            .unwrap()
            .contains("~30 min of serial work remains on the critical path"),
        "{shadowed}"
    );
}
