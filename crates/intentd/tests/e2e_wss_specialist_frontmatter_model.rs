//! WSS e2e for specialist frontmatter model resolution (fix for review thread
//! `PRRT_kwDOS9Wxuc6SIhDY`): validates that agent.create with a specialistId but
//! no explicit model parameter resolves the specialist's frontmatter `model`
//! field through the 3-tier precedence (project > user > bundled).

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
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

/// Send one JSON-RPC frame and return the result whose id matches; any
/// out-of-band notifications (`events.event`) are ignored.
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
                    // Validate JSON-RPC envelope (review thread PRRT_kwDOS9Wxuc6SIhDr)
                    assert_eq!(v["jsonrpc"], "2.0", "invalid jsonrpc field");
                    assert_eq!(v["id"], json!(id), "response id mismatch");
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

#[tokio::test]
async fn specialist_frontmatter_model_resolved_over_wss() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    // Pre-seed the database with a workspace that has a repository_path pointing
    // to the data_dir (so specialist resolution works)
    let ws_id = {
        use intent_core::WorkspaceId;
        use intent_store::Store;
        let db_path = data_dir.join("intentd.db");
        let store = Store::open(&db_path).await.expect("open store");
        let ws = WorkspaceId::new();
        let mut workspace = workspace_seed(&ws);
        // Set repository_path so specialist resolution can find the specialist file
        workspace.repository_path = Some(data_dir.to_string_lossy().to_string());
        store.insert_workspace(&workspace).await.expect("insert ws");
        ws.0
    };

    // Create a user-tier specialist with a model frontmatter field.
    // (Hermetic: set HOME=data_dir below so the daemon reads $HOME/.intent/specialists/.)
    let specialists_dir = data_dir.join(".intent").join("specialists");
    std::fs::create_dir_all(&specialists_dir).expect("mkdir specialists dir");
    let specialist_content =
        "---\nmodel: auggie:opus\n---\n# Test Specialist\nTest behavior prompt.";
    std::fs::write(
        specialists_dir.join("test-specialist.md"),
        specialist_content,
    )
    .expect("write specialist file");

    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("HOME", data_dir.to_str().expect("data_dir to str")),
    ];
    let daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not boot");

    // Discover bound port + fingerprint via UDS
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    // Connect over WSS
    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    // Create an agent with specialistId but no explicit model (review thread PRRT_kwDOS9Wxuc6SIhDg)
    let agent_res = wss_rpc(
        &mut ws,
        2,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "specialistId": "test-specialist",  // Correct param name
            "name": "TestAgent"
        }),
    )
    .await;

    let agent_id = agent_res["agent"]["id"].as_str().expect("agent id");

    // Get agent and verify model was pinned to specialist frontmatter value
    let get_res = wss_rpc(&mut ws, 3, "agent.get", json!({ "agentId": agent_id })).await;

    // Assert the session's model IS the frontmatter model (make the test fail if resolution is skipped)
    assert_eq!(
        get_res["agent"]["model"], "auggie:opus",
        "specialist frontmatter model not resolved"
    );

    drop(daemon);
}

/// WSS e2e for specialist aliases (PROTOCOL §5.11): the bundled v1.1
/// `spec-writer` carries `aliases: ["coordinator"]`, so `agent.create` with
/// `specialistId: "coordinator"` persists the CANONICAL id (`spec-writer`)
/// on the session — surfaced as `metadata.specialist` — and resolves the
/// specialist's display name; `specialist.get` on the alias serves the
/// canonical resolved view.
#[tokio::test]
async fn specialist_alias_resolves_and_persists_canonical_id_over_wss() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    // Pre-seed a workspace (repository_path so project-tier resolution has a
    // root; the alias itself resolves from the embedded bundled floor).
    let ws_id = {
        use intent_core::WorkspaceId;
        use intent_store::Store;
        let db_path = data_dir.join("intentd.db");
        let store = Store::open(&db_path).await.expect("open store");
        let ws = WorkspaceId::new();
        let mut workspace = workspace_seed(&ws);
        workspace.repository_path = Some(data_dir.to_string_lossy().to_string());
        store.insert_workspace(&workspace).await.expect("insert ws");
        ws.0
    };

    // Hermetic empty user tier: HOME=data_dir with no specialists written.
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("HOME", data_dir.to_str().expect("data_dir to str")),
    ];
    let daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not boot");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    // specialist.get on the alias serves the canonical resolved view.
    let got = wss_rpc(&mut ws, 2, "specialist.get", json!({ "id": "coordinator" })).await;
    assert_eq!(
        got["specialist"]["id"], "spec-writer",
        "alias resolves to the canonical def over WSS"
    );
    assert_eq!(got["specialist"]["aliases"], json!(["coordinator"]));

    // agent.create with the alias persists the canonical specialist id and
    // derives the display name from the canonical specialist. The explicit
    // compound model satisfies provider resolution (the bundled spec-writer
    // pins no frontmatter model and this hermetic env configures no default
    // provider) without touching the alias seam under test.
    let created = wss_rpc(
        &mut ws,
        3,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "specialistId": "coordinator",
            "model": "auggie:opus"
        }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().expect("agent id");
    assert_eq!(
        created["agent"]["metadata"]["specialist"], "spec-writer",
        "alias create persists the canonical id, not the alias"
    );
    assert_eq!(created["agent"]["name"], "Coordinator");

    // agent.get round-trips the canonical id from the persisted row.
    let get_res = wss_rpc(&mut ws, 4, "agent.get", json!({ "agentId": agent_id })).await;
    assert_eq!(
        get_res["agent"]["metadata"]["specialist"], "spec-writer",
        "persisted session carries the canonical id"
    );

    // An UNKNOWN specialist id is rejected with `-32602` naming the id and
    // the known catalog ids (monorepo#3497) — no session is created.
    let rejected = wss_rpc_raw(
        &mut ws,
        5,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "specialistId": "no-such-specialist",
            "model": "auggie:opus"
        }),
    )
    .await;
    assert_eq!(
        rejected["error"]["code"], -32602,
        "unknown specialist rejects with invalid-params: {rejected}"
    );
    let msg = rejected["error"]["message"]
        .as_str()
        .expect("error message");
    assert!(
        msg.contains("unknown specialist: no-such-specialist"),
        "error names the id: {msg}"
    );
    assert!(
        msg.contains("known specialists:") && msg.contains("spec-writer"),
        "error lists the known ids: {msg}"
    );

    drop(daemon);
}

/// WSS e2e for the optional `hidden` flag (PROTOCOL §5.11): a user-tier
/// specialist whose frontmatter sets `hidden: true` surfaces the boolean on
/// `specialist.get` and `specialist.list` over the real WSS transport, while a
/// non-hidden specialist omits the field entirely.
#[tokio::test]
async fn specialist_hidden_round_trips_over_wss() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    // Hermetic user tier: HOME=data_dir so the daemon reads
    // $HOME/.intent/specialists/.
    let specialists_dir = data_dir.join(".intent").join("specialists");
    std::fs::create_dir_all(&specialists_dir).expect("mkdir specialists dir");
    std::fs::write(
        specialists_dir.join("ghost.md"),
        "---\nname: \"Ghost\"\ndescription: \"Hidden helper\"\nhidden: true\n---\n\nGhost body.",
    )
    .expect("write hidden specialist");
    std::fs::write(
        specialists_dir.join("visible.md"),
        "---\nname: \"Visible\"\ndescription: \"Shown helper\"\n---\n\nVisible body.",
    )
    .expect("write visible specialist");

    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("HOME", data_dir.to_str().expect("data_dir to str")),
    ];
    let daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not boot");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    // get — the hidden boolean surfaces on the resolved view.
    let got = wss_rpc(&mut ws, 2, "specialist.get", json!({ "id": "ghost" })).await;
    assert_eq!(
        got["specialist"]["hidden"], true,
        "hidden frontmatter surfaces on specialist.get over WSS"
    );

    // list — hidden surfaces in the list projection; non-hidden omits it.
    let list = wss_rpc(&mut ws, 3, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().expect("specialists array");
    let ghost = specs
        .iter()
        .find(|s| s["id"] == "ghost")
        .expect("ghost listed");
    assert_eq!(ghost["hidden"], true, "hidden surfaces in specialist.list");
    let visible = specs
        .iter()
        .find(|s| s["id"] == "visible")
        .expect("visible listed");
    assert!(
        visible.get("hidden").is_none(),
        "non-hidden specialists omit the field over WSS"
    );

    drop(daemon);
}

/// WSS e2e for the embedded bundled catalog: with an empty user tier and no
/// bundled-dir override, `specialist.list` over WSS returns exactly the eight
/// catalog-visible embedded reference specialists. Retired Ralph stays directly
/// resolvable for pinned v1 sessions, while `pr-shepherd` remains gone.
#[tokio::test]
async fn embedded_bundled_catalog_over_wss() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    // Hermetic empty user tier: HOME=data_dir with no specialists written.
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("HOME", data_dir.to_str().expect("data_dir to str")),
    ];
    let daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not boot");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    let list = wss_rpc(&mut ws, 2, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().expect("specialists array");
    let mut ids: Vec<&str> = specs
        .iter()
        .map(|s| s["id"].as_str().expect("specialist id"))
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        [
            "chief-of-staff",
            "developer",
            "implementor",
            "pr-reviewer",
            "spec-writer",
            "ui-designer",
            "verifier",
            "vulnerability-scanner",
        ],
        "bundled catalog over WSS is exactly the eight catalog-visible embedded ids"
    );
    assert!(!ids.contains(&"ralph"), "retired Ralph is not cataloged");
    for spec in specs {
        assert_eq!(spec["source"], "bundled", "{}: embedded tier", spec["id"]);
    }

    let scanner = wss_rpc(
        &mut ws,
        3,
        "specialist.get",
        json!({ "id": "vulnerability-scanner" }),
    )
    .await;
    let scanner = &scanner["specialist"];
    assert_eq!(scanner["name"], "Vulnerability Scanner");
    assert_eq!(
        scanner["description"],
        "Finds real, exploitable security vulnerabilities in code"
    );
    assert_eq!(scanner["codingAgent"], "auggie");
    assert_eq!(scanner["model"], "opus4.7");
    assert_eq!(scanner["icon"], "pr-reviewer");
    assert!(scanner["prompt"]
        .as_str()
        .is_some_and(|body| body.starts_with("## Vulnerability Scanner\n")));

    let ralph = wss_rpc(&mut ws, 4, "specialist.get", json!({ "id": "ralph" })).await;
    assert_eq!(ralph["specialist"]["source"], "bundled");
    assert_eq!(ralph["specialist"]["hidden"], true);
    assert_eq!(ralph["specialist"]["agentType"], "ralph-loop");
    assert!(ralph["specialist"]["roleReminder"]
        .as_str()
        .unwrap()
        .starts_with("You are Ralph."));

    drop(daemon);
}

/// WSS e2e for the base-tier replacement (`INTENTD_SPECIALISTS_DIR`): with the
/// startup pin set, `specialist.list` over the real WSS transport returns only
/// the replacement directory's specialists (as `bundled`) plus user-tier
/// folds — none of the nine embedded ids survive — and the pin surfaces as
/// the read-only `specialists.dir` setting.
#[tokio::test]
async fn specialists_replacement_dir_over_wss() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    // The replacement base tier: a single specialist, nothing else survives.
    let replacement_dir = data_dir.join("replacement-specialists");
    std::fs::create_dir_all(&replacement_dir).expect("mkdir replacement dir");
    std::fs::write(
        replacement_dir.join("solo.md"),
        "---\nname: \"Solo\"\ndescription: \"Replacement base\"\n---\n\nSolo body.",
    )
    .expect("write replacement solo");

    // A user-tier specialist still folds on top of the replacement.
    let specialists_dir = data_dir.join(".intent").join("specialists");
    std::fs::create_dir_all(&specialists_dir).expect("mkdir specialists dir");
    std::fs::write(
        specialists_dir.join("extra.md"),
        "---\nname: \"Extra\"\ndescription: \"User tier\"\n---\n\nExtra body.",
    )
    .expect("write user extra");

    let replacement_dir_str = replacement_dir
        .to_str()
        .expect("replacement dir to str")
        .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("HOME", data_dir.to_str().expect("data_dir to str")),
        ("INTENTD_SPECIALISTS_DIR", &replacement_dir_str),
    ];
    let daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not boot");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    // list — exactly the replacement base + the user fold; no embedded ids.
    let list = wss_rpc(&mut ws, 2, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().expect("specialists array");
    let mut ids: Vec<&str> = specs
        .iter()
        .map(|s| s["id"].as_str().expect("specialist id"))
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        ["extra", "solo"],
        "replacement dir excludes every embedded specialist over WSS"
    );
    let solo = specs.iter().find(|s| s["id"] == "solo").expect("solo");
    assert_eq!(solo["source"], "bundled", "replacement is the base tier");
    let extra = specs.iter().find(|s| s["id"] == "extra").expect("extra");
    assert_eq!(extra["source"], "user", "user tier folds on top");

    // get — a shipped id not restated in the replacement does not resolve.
    let missing = wss_rpc_raw(&mut ws, 3, "specialist.get", json!({ "id": "developer" })).await;
    assert!(
        missing.get("error").is_some(),
        "shipped id is gone under replacement: {missing}"
    );

    // The pin surfaces as the read-only specialists.dir setting.
    let got = wss_rpc(
        &mut ws,
        4,
        "settings.get",
        json!({ "path": "specialists.dir" }),
    )
    .await;
    assert_eq!(
        got["value"],
        json!(replacement_dir_str),
        "specialists.dir reports the startup pin over WSS"
    );

    drop(daemon);
}

/// WSS e2e for config-scalar inheritance across tiers (PROTOCOL §5.11,
/// monorepo#718): a user-tier override that omits `model`/`agentType`
/// inherits the bundled tier's values on `specialist.get` and
/// `specialist.list`, an explicit empty value (`model: ""`) clears the
/// inherited one, and `roleReminder` stays winner-takes-all (not inherited).
#[tokio::test]
async fn specialist_config_scalars_inherit_over_wss() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    // Bundled tier via the INTENTD_BUNDLED_SPECIALISTS_DIR seam.
    let bundled_dir = data_dir.join("bundled-specialists");
    std::fs::create_dir_all(&bundled_dir).expect("mkdir bundled dir");
    std::fs::write(
        bundled_dir.join("zeta.md"),
        "---\nname: \"Zeta\"\ndescription: \"Bundled\"\nmodel: \"auggie:opus\"\nagentType: \"zeta-type\"\nroleReminder: \"Bundled reminder.\"\n---\n\nBundled body.",
    )
    .expect("write bundled zeta");
    std::fs::write(
        bundled_dir.join("omega.md"),
        "---\nname: \"Omega\"\ndescription: \"Bundled\"\nmodel: \"auggie:opus\"\n---\n\nBundled body.",
    )
    .expect("write bundled omega");

    // Hermetic user tier: HOME=data_dir so the daemon reads
    // $HOME/.intent/specialists/.
    let specialists_dir = data_dir.join(".intent").join("specialists");
    std::fs::create_dir_all(&specialists_dir).expect("mkdir specialists dir");
    // Omits model/agentType/roleReminder: the config scalars inherit from the
    // bundled tier; the reminder does not.
    std::fs::write(
        specialists_dir.join("zeta.md"),
        "---\nname: \"Zeta\"\ndescription: \"User override\"\n---\n\nUser body.",
    )
    .expect("write user zeta");
    // Explicit empty model: the explicit-clear that stops inheritance.
    std::fs::write(
        specialists_dir.join("omega.md"),
        "---\nname: \"Omega\"\ndescription: \"User override\"\nmodel: \"\"\n---\n\nUser body.",
    )
    .expect("write user omega");

    let bundled_dir_str = bundled_dir
        .to_str()
        .expect("bundled dir to str")
        .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("HOME", data_dir.to_str().expect("data_dir to str")),
        ("INTENTD_BUNDLED_SPECIALISTS_DIR", &bundled_dir_str),
    ];
    let daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not boot");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    // get — omitted scalars inherit the bundled values; roleReminder does not.
    let got = wss_rpc(&mut ws, 2, "specialist.get", json!({ "id": "zeta" })).await;
    let def = &got["specialist"];
    assert_eq!(def["source"], "user", "user tier wins the merge");
    assert_eq!(
        def["model"], "auggie:opus",
        "omitted model inherits the bundled value on specialist.get over WSS"
    );
    assert_eq!(
        def["agentType"], "zeta-type",
        "omitted agentType inherits the bundled value on specialist.get over WSS"
    );
    assert!(
        def.get("roleReminder").is_none(),
        "roleReminder is not inherited over WSS"
    );

    // get — an explicit empty value clears the inherited model.
    let got = wss_rpc(&mut ws, 3, "specialist.get", json!({ "id": "omega" })).await;
    assert!(
        got["specialist"].get("model").is_none(),
        "explicit empty model clears the inherited value on specialist.get over WSS"
    );

    // list — the same fold applies to the list projection.
    let list = wss_rpc(&mut ws, 4, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().expect("specialists array");
    let zeta = specs
        .iter()
        .find(|s| s["id"] == "zeta")
        .expect("zeta listed");
    assert_eq!(
        zeta["model"], "auggie:opus",
        "omitted model inherits in specialist.list over WSS"
    );
    assert_eq!(
        zeta["agentType"], "zeta-type",
        "omitted agentType inherits in specialist.list over WSS"
    );
    let omega = specs
        .iter()
        .find(|s| s["id"] == "omega")
        .expect("omega listed");
    assert!(
        omega.get("model").is_none(),
        "explicit empty model clears in specialist.list over WSS"
    );

    drop(daemon);
}

/// Like [`wss_rpc`] but returns the full response envelope so callers can
/// assert JSON-RPC error codes (PROTOCOL §9).
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
                    assert_eq!(v["jsonrpc"], "2.0", "invalid jsonrpc field");
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

/// WSS e2e for the `modelOptions` list (PROTOCOL §5.11): a user-tier override
/// that omits the key inherits the bundled tier's list on `specialist.get`
/// and `specialist.list`, an explicit `[]` clears it, `create` round-trips a
/// supplied list, and an invalid `modelOptions` shape on `create` is rejected
/// with `-32602`.
#[tokio::test]
async fn specialist_model_options_round_trip_over_wss() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    // Bundled tier via the INTENTD_BUNDLED_SPECIALISTS_DIR seam.
    let bundled_dir = data_dir.join("bundled-specialists");
    std::fs::create_dir_all(&bundled_dir).expect("mkdir bundled dir");
    std::fs::write(
        bundled_dir.join("zeta.md"),
        "---\nname: \"Zeta\"\ndescription: \"Bundled\"\nmodelOptions: [{\"model\":\"auggie:opus\",\"hint\":\"smart\"}]\n---\n\nBundled body.",
    )
    .expect("write bundled zeta");

    // Hermetic user tier: HOME=data_dir so the daemon reads
    // $HOME/.intent/specialists/.
    let specialists_dir = data_dir.join(".intent").join("specialists");
    std::fs::create_dir_all(&specialists_dir).expect("mkdir specialists dir");
    // Omits modelOptions: inherits the bundled tier's list.
    std::fs::write(
        specialists_dir.join("zeta.md"),
        "---\nname: \"Zeta\"\ndescription: \"User override\"\n---\n\nUser body.",
    )
    .expect("write user zeta");
    // Explicit []: the explicit clear that stops inheritance.
    std::fs::write(
        specialists_dir.join("omega.md"),
        "---\nname: \"Omega\"\ndescription: \"Cleared\"\nmodelOptions: []\n---\n\nUser body.",
    )
    .expect("write user omega");

    let bundled_dir_str = bundled_dir
        .to_str()
        .expect("bundled dir to str")
        .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("HOME", data_dir.to_str().expect("data_dir to str")),
        ("INTENTD_BUNDLED_SPECIALISTS_DIR", &bundled_dir_str),
    ];
    let daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not boot");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    let expected = json!([{ "model": "auggie:opus", "hint": "smart" }]);

    // get — an omitted key inherits the bundled tier's list.
    let got = wss_rpc(&mut ws, 2, "specialist.get", json!({ "id": "zeta" })).await;
    assert_eq!(got["specialist"]["source"], "user", "user tier wins");
    assert_eq!(
        got["specialist"]["modelOptions"], expected,
        "omitted modelOptions inherits the bundled list on specialist.get over WSS"
    );

    // list — the same fold applies to the list projection; the explicit []
    // clear omits the field.
    let list = wss_rpc(&mut ws, 3, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().expect("specialists array");
    let zeta = specs
        .iter()
        .find(|s| s["id"] == "zeta")
        .expect("zeta listed");
    assert_eq!(
        zeta["modelOptions"], expected,
        "omitted modelOptions inherits in specialist.list over WSS"
    );
    let omega = specs
        .iter()
        .find(|s| s["id"] == "omega")
        .expect("omega listed");
    assert!(
        omega.get("modelOptions").is_none(),
        "explicit [] clears modelOptions in specialist.list over WSS"
    );

    // create — a supplied list round-trips through the write path.
    let created = wss_rpc(
        &mut ws,
        4,
        "specialist.create",
        json!({
            "id": "sigma",
            "scope": "user",
            "spec": {
                "id": "sigma", "name": "Sigma", "description": "Authored",
                "modelOptions": [{ "model": "opencode:kimi-k3", "hint": "cheap" }],
                "prompt": "Sigma body."
            }
        }),
    )
    .await;
    assert_eq!(
        created["specialist"]["modelOptions"],
        json!([{ "model": "opencode:kimi-k3", "hint": "cheap" }]),
        "create round-trips modelOptions over WSS"
    );
    let got = wss_rpc(&mut ws, 5, "specialist.get", json!({ "id": "sigma" })).await;
    assert_eq!(
        got["specialist"]["modelOptions"], created["specialist"]["modelOptions"],
        "create response agrees with the following get over WSS"
    );

    // create — an invalid modelOptions shape is rejected with -32602.
    let rejected = wss_rpc_raw(
        &mut ws,
        6,
        "specialist.create",
        json!({
            "id": "tau",
            "scope": "user",
            "spec": {
                "id": "tau", "name": "Tau", "description": "Bad",
                "modelOptions": [{ "hint": "no model" }],
                "prompt": "Tau body."
            }
        }),
    )
    .await;
    assert_eq!(
        rejected["error"]["code"], -32602,
        "invalid modelOptions → -32602 over WSS: {rejected}"
    );

    drop(daemon);
}

/// WSS e2e for the picker-metadata fields (`role`/`teamAgents`/`icon`,
/// PROTOCOL §5.11): a user-tier override that omits the keys inherits the
/// bundled tier's values on `specialist.get` and `specialist.list`, `create`
/// round-trips supplied values and agrees with the following `get`, `edit`
/// clears them, and invalid wire shapes (out-of-enum `role`, non-string
/// `icon`, non-array `teamAgents`) are rejected with `-32602`.
#[tokio::test]
async fn specialist_picker_metadata_round_trips_over_wss() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    // Bundled tier via the INTENTD_BUNDLED_SPECIALISTS_DIR seam.
    let bundled_dir = data_dir.join("bundled-specialists");
    std::fs::create_dir_all(&bundled_dir).expect("mkdir bundled dir");
    std::fs::write(
        bundled_dir.join("zeta.md"),
        "---\nname: \"Zeta\"\ndescription: \"Bundled\"\nrole: \"orchestrator\"\nicon: \"coordinator\"\nteamAgents: [\"implementor\",\"verifier\"]\n---\n\nBundled body.",
    )
    .expect("write bundled zeta");

    // Hermetic user tier: HOME=data_dir so the daemon reads
    // $HOME/.intent/specialists/. Omits the metadata keys: inherits.
    let specialists_dir = data_dir.join(".intent").join("specialists");
    std::fs::create_dir_all(&specialists_dir).expect("mkdir specialists dir");
    std::fs::write(
        specialists_dir.join("zeta.md"),
        "---\nname: \"Zeta\"\ndescription: \"User override\"\n---\n\nUser body.",
    )
    .expect("write user zeta");

    let bundled_dir_str = bundled_dir
        .to_str()
        .expect("bundled dir to str")
        .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("HOME", data_dir.to_str().expect("data_dir to str")),
        ("INTENTD_BUNDLED_SPECIALISTS_DIR", &bundled_dir_str),
    ];
    let daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not boot");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    // get — omitted keys inherit the bundled tier's metadata.
    let got = wss_rpc(&mut ws, 2, "specialist.get", json!({ "id": "zeta" })).await;
    let def = &got["specialist"];
    assert_eq!(def["source"], "user", "user tier wins the merge");
    assert_eq!(def["role"], "orchestrator", "role inherits over WSS");
    assert_eq!(def["icon"], "coordinator", "icon inherits over WSS");
    assert_eq!(
        def["teamAgents"],
        json!(["implementor", "verifier"]),
        "teamAgents inherits over WSS"
    );

    // list — the same fold applies to the list projection.
    let list = wss_rpc(&mut ws, 3, "specialist.list", json!({})).await;
    let specs = list["specialists"].as_array().expect("specialists array");
    let zeta = specs
        .iter()
        .find(|s| s["id"] == "zeta")
        .expect("zeta listed");
    assert_eq!(zeta["role"], "orchestrator", "role in specialist.list");
    assert_eq!(zeta["icon"], "coordinator", "icon in specialist.list");
    assert_eq!(
        zeta["teamAgents"],
        json!(["implementor", "verifier"]),
        "teamAgents in specialist.list"
    );

    // create — supplied metadata round-trips through the write path and
    // agrees with the following get.
    let created = wss_rpc(
        &mut ws,
        4,
        "specialist.create",
        json!({
            "id": "sigma",
            "scope": "user",
            "spec": {
                "id": "sigma", "name": "Sigma", "description": "Authored",
                "role": "internal", "icon": "verifier",
                "teamAgents": ["implementor"],
                "prompt": "Sigma body."
            }
        }),
    )
    .await;
    assert_eq!(created["specialist"]["role"], "internal");
    assert_eq!(created["specialist"]["icon"], "verifier");
    assert_eq!(created["specialist"]["teamAgents"], json!(["implementor"]));
    let got = wss_rpc(&mut ws, 5, "specialist.get", json!({ "id": "sigma" })).await;
    assert_eq!(
        got["specialist"]["role"], created["specialist"]["role"],
        "create response agrees with the following get over WSS"
    );

    // edit — the explicit clears drop the fields.
    let edited = wss_rpc(
        &mut ws,
        6,
        "specialist.edit",
        json!({
            "id": "sigma",
            "scope": "user",
            "spec": {
                "id": "sigma", "name": "Sigma", "description": "Authored",
                "role": "", "icon": "", "teamAgents": [],
                "prompt": "Sigma body."
            }
        }),
    )
    .await;
    assert!(edited["specialist"].get("role").is_none(), "role cleared");
    assert!(edited["specialist"].get("icon").is_none(), "icon cleared");
    assert!(
        edited["specialist"].get("teamAgents").is_none(),
        "teamAgents cleared"
    );

    // create — invalid wire shapes are rejected with -32602 (PROTOCOL §9).
    let invalid_specs = [
        json!({ "id": "tau", "name": "Tau", "description": "Bad", "role": "manager", "prompt": "b" }),
        json!({ "id": "tau", "name": "Tau", "description": "Bad", "icon": 42, "prompt": "b" }),
        json!({ "id": "tau", "name": "Tau", "description": "Bad", "teamAgents": "not-an-array", "prompt": "b" }),
    ];
    for (i, spec) in invalid_specs.iter().enumerate() {
        let rejected = wss_rpc_raw(
            &mut ws,
            7 + i64::try_from(i).expect("index fits in i64"),
            "specialist.create",
            json!({ "id": "tau", "scope": "user", "spec": spec }),
        )
        .await;
        assert_eq!(
            rejected["error"]["code"], -32602,
            "invalid picker metadata → -32602 over WSS: {rejected}"
        );
    }

    drop(daemon);
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WSS-E2E".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}
