//! Real-WSS e2e for the background first-boot legacy import: the WSS
//! listener answers RPCs while the import is in flight (held by the
//! `INTENTD_TEST_LEGACY_IMPORT_HOLD_FILE` seam), and each imported workspace
//! publishes a `workspace:created` event that a live `events.subscribe`
//! subscriber observes as `events.event` notifications (PROTOCOL §6.3).

#![cfg(unix)]

mod common;

use std::collections::HashSet;
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
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

struct Daemon {
    child: Child,
    data_dir: PathBuf,
    legacy_root: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
        let _ = std::fs::remove_dir_all(&self.legacy_root);
    }
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
        let actual = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if actual == self.fingerprint {
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

/// One workspace of the synthetic legacy tree: a manifest plus one note.
fn write_synthetic_workspace(root: &Path, id: &str) {
    let metadata = root.join(id).join(".workspace");
    std::fs::create_dir_all(metadata.join("notes")).unwrap();
    std::fs::write(
        metadata.join("workspace.json"),
        json!({
            "id": id,
            "title": format!("Synthetic {id}"),
            "status": "Active",
            "createdAt": "2025-05-01T00:00:00Z",
            "updatedAt": "2025-05-02T00:00:00Z"
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        metadata.join("notes").join("note-0.md"),
        "---\nid: note-0\ntitle: Note 0\n---\n\nBody\n",
    )
    .unwrap();
}

fn spawn_daemon(data_dir: &Path, legacy_root: &Path, hold_file: &Path) -> Child {
    common::enable_ws_api(data_dir);
    let log = std::fs::File::create(data_dir.join("daemon.log")).unwrap();
    let workspaces = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", workspaces)
        .env("INTENTD_LEGACY_IMPORT_ROOTS", legacy_root)
        .env("INTENTD_LEGACY_APP_DIR", "")
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", "0")
        .env("INTENTD_TEST_LEGACY_IMPORT_HOLD_FILE", hold_file)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    cmd.spawn().unwrap()
}

/// One authenticated WSS JSON-RPC round-trip, asserting the response
/// envelope (`jsonrpc: "2.0"`, echoed `id`, no `error`); out-of-band
/// `events.event` notifications are skipped, pings answered.
async fn wss_rpc(ws: &mut common::TlsWs, id: i64, method: &str, params: Value) -> Value {
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
                    assert_eq!(v["jsonrpc"], "2.0", "response envelope: {v}");
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

/// Read the next `events.event` notification's `params` before the deadline,
/// asserting the notification envelope; `None` on deadline.
async fn wss_event_until(ws: &mut common::TlsWs, deadline: tokio::time::Instant) -> Option<Value> {
    loop {
        let next = match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(next) => next,
            Err(_) => return None,
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    assert_eq!(v["jsonrpc"], "2.0", "notification envelope: {v}");
                    return Some(v["params"].clone());
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

/// `workspace.list` over WSS, returning how many `ws-wss-inflight-*` legacy
/// workspaces have landed — the responsiveness probe.
async fn imported_count(ws: &mut common::TlsWs, id: i64) -> usize {
    let result = wss_rpc(ws, id, "workspace.list", json!({})).await;
    result["workspaces"]
        .as_array()
        .unwrap_or_else(|| panic!("workspaces array missing: {result}"))
        .iter()
        .filter(|w| {
            w["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("ws-wss-inflight-"))
        })
        .count()
}

/// The daemon serves RPCs over the REAL WSS transport while the first-boot
/// legacy import is in flight, and every imported workspace reaches a live
/// subscriber as a `workspace:created` `events.event` notification.
#[tokio::test]
async fn wss_serves_rpcs_and_streams_workspace_created_during_first_boot_import() {
    const WORKSPACES: usize = 5;
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itd-wli-{}", &id[..8]));
    let legacy_root = PathBuf::from("/tmp").join(format!("itd-wlr-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&legacy_root).unwrap();
    for i in 0..WORKSPACES {
        write_synthetic_workspace(&legacy_root, &format!("ws-wss-inflight-{i}"));
    }
    // The hold file pre-exists the daemon, so the background import pauses
    // before its first workspace lands — "in flight" is deterministic.
    let hold = data_dir.join("import.hold");
    std::fs::write(&hold, []).unwrap();
    // No pre-created DB file: a truly fresh boot fires the first-boot hook.
    let socket = data_dir.join("intentd.sock");
    let _daemon = Daemon {
        child: spawn_daemon(&data_dir, &legacy_root, &hold),
        data_dir: data_dir.clone(),
        legacy_root,
    };

    // Resolve the live WSS port + fingerprint over UDS, then connect a real
    // pinned TLS WebSocket (bearer token in the query string).
    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
    let port = u16::try_from(status["result"]["port"].as_u64().expect("bound port"))
        .expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let mut rpc = common::wss_connect_with_retry(port, cfg.clone(), &url).await;

    // SUBSCRIBER conn — registered while the import is held, so it observes
    // every workspace:created the import publishes after release.
    let mut sub = common::wss_connect_with_retry(port, cfg.clone(), &url).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:created"] }),
    )
    .await;
    let subscription_id = sub_resp["subscriptionId"]
        .as_str()
        .expect("subscriptionId")
        .to_string();

    // Import in flight: WSS RPCs must answer, with nothing landed yet.
    for i in 0..3 {
        assert_eq!(
            imported_count(&mut rpc, 10 + i).await,
            0,
            "no legacy workspace may land while the import is held"
        );
    }

    // Release the import; collect one workspace:created per workspace.
    std::fs::remove_file(&hold).unwrap();
    let mut created: HashSet<String> = HashSet::new();
    let deadline = tokio::time::Instant::now() + common::rpc_read_timeout();
    while created.len() < WORKSPACES {
        let params = wss_event_until(&mut sub, deadline)
            .await
            .unwrap_or_else(|| panic!("only {}/{WORKSPACES} events arrived", created.len()));
        assert_eq!(params["subscriptionId"], json!(subscription_id), "{params}");
        let event = &params["event"];
        assert_eq!(event["type"], "workspace:created", "{event}");
        let ws_id = event["workspaceId"]
            .as_str()
            .expect("workspaceId")
            .to_string();
        assert_eq!(event["data"]["workspaceId"], json!(ws_id), "{event}");
        assert_eq!(
            event["data"]["workspace"]["id"],
            json!(ws_id),
            "self-sufficient payload carries the row: {event}"
        );
        assert!(event["id"].is_string(), "{event}");
        assert!(event["timestamp"].is_string(), "{event}");
        assert_eq!(event["actor"]["type"], "system", "{event}");
        assert!(
            ws_id.starts_with("ws-wss-inflight-"),
            "unexpected workspace: {ws_id}"
        );
        assert!(created.insert(ws_id), "duplicate workspace:created event");
    }

    // The imported rows are readable over the same WSS transport.
    timeout(common::rpc_read_timeout(), async {
        let mut id = 100;
        loop {
            if imported_count(&mut rpc, id).await == WORKSPACES {
                return;
            }
            id += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("imported workspaces not visible over WSS");
}
