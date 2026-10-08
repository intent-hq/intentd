//! Setup must finish without a terminal renderer answering `ConPTY`'s startup
//! cursor query. Run on Windows for the native regression; Unix is a control.
//! Export `setup-headless-evidence.json` with `INTENTD_SETUP_EVIDENCE_DIR`.

mod common;

use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

type Socket = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;
const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

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

async fn next_json(socket: &mut Socket) -> Value {
    loop {
        match timeout(common::rpc_read_timeout(), socket.next())
            .await
            .expect("WSS frame deadline")
        {
            Some(Ok(Message::Text(text))) => {
                return serde_json::from_str(&text).expect("JSON frame")
            }
            Some(Ok(Message::Ping(data))) => socket.send(Message::Pong(data)).await.unwrap(),
            Some(Ok(_)) => {}
            other => panic!("WSS ended: {other:?}"),
        }
    }
}

async fn rpc_response(socket: &mut Socket, id: i64, method: &str, params: Value) -> Value {
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    loop {
        let frame = next_json(socket).await;
        if frame["id"] == id {
            assert_eq!(frame["jsonrpc"], "2.0");
            return frame;
        }
    }
}

async fn rpc(socket: &mut Socket, id: i64, method: &str, params: Value) -> Value {
    let frame = rpc_response(socket, id, method, params).await;
    assert!(frame.get("error").is_none(), "{method}: {frame}");
    frame["result"].clone()
}

async fn wait_for_output(socket: &mut Socket, terminal_id: &str, markers: &[&str]) -> String {
    timeout(common::test_timeout(Duration::from_secs(30)), async {
        loop {
            let result = rpc(
                socket,
                50,
                "terminal.getBuffer",
                json!({"terminalId":terminal_id}),
            )
            .await;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(result["data"].as_str().unwrap())
                .unwrap();
            let output = String::from_utf8_lossy(&bytes).into_owned();
            if markers.iter().all(|marker| output.contains(marker)) {
                break output;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal output did not reach its completion markers")
}

fn seed_repo(path: &Path) {
    for args in [
        vec!["init", "--initial-branch=main"],
        vec!["config", "user.name", "Test"],
        vec!["config", "user.email", "test@example.com"],
    ] {
        assert!(Command::new("git")
            .args(args)
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }
    std::fs::write(path.join("README.md"), "setup fixture\n").unwrap();
    for args in [vec!["add", "."], vec!["commit", "-m", "fixture"]] {
        assert!(Command::new("git")
            .args(args)
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }
}

#[tokio::test]
async fn setup_finishes_without_terminal_client_input() {
    let data = common::test_tempdir("setup headless data ");
    let repo = common::test_tempdir("setup headless repo ");
    seed_repo(repo.path());
    common::enable_ws_api(data.path());
    let config = std::fs::read_to_string(data.path().join("config.toml")).unwrap();
    let port: u16 = config
        .lines()
        .find_map(|line| line.strip_prefix("port = "))
        .unwrap()
        .parse()
        .unwrap();
    let cert = intent_transport::ensure_tls_certificate(data.path()).unwrap();
    let workspaces = data.path().join("workspaces with spaces");
    std::fs::create_dir_all(&workspaces).unwrap();
    let log = std::fs::File::create(data.path().join("daemon.log")).unwrap();
    let mut command = common::hermetic_serve_command_fixed_port(data.path());
    command
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_WORKSPACES_DIR", &workspaces)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    let child = command.spawn().expect("spawn daemon");
    let _daemon = common::DaemonGuard::process_only(child);
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                break;
            }
            // timing-guard: poll the observable WSS listener bind during daemon startup.
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "daemon listener did not start: {}",
            std::fs::read_to_string(data.path().join("daemon.log")).unwrap_or_default()
        )
    });
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let cfg = client_config(&cert.fingerprint256);
    let mut client = common::wss_connect_with_retry(port, cfg.clone(), &url).await;
    let mut sub = common::wss_connect_with_retry(port, cfg, &url).await;
    rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({"eventTypes":["workspace:setup:*", "terminal:*"]}),
    )
    .await;
    let mut evidence = Vec::new();
    for (case, code) in [("success", 0), ("failure", 7)] {
        let script =
            format!("echo SETUP_{case} > setup-marker.txt\necho SETUP_{case}\nexit {code}\n");
        let created = rpc(&mut client, 10, "workspace.create", json!({"title":format!("headless-{case}"), "repositoryPath":repo.path().to_string_lossy(), "setupScript":script})).await;
        let workspace = &created["workspace"];
        let id = workspace["id"].as_str().unwrap();
        let worktree = Path::new(workspace["worktreePath"].as_str().unwrap());
        let (completed, terminal_id) =
            timeout(common::test_timeout(Duration::from_secs(30)), async {
                let mut completed = None;
                let mut terminal = None;
                let mut started = false;
                loop {
                    let frame = next_json(&mut sub).await;
                    if frame["method"] != "events.event" {
                        continue;
                    }
                    let event = &frame["params"]["event"];
                    if event["workspaceId"] != id {
                        continue;
                    }
                    match event["type"].as_str() {
                        Some("workspace:setup:started") => {
                            assert!(!started);
                            started = true;
                        }
                        Some("workspace:setup:completed") => {
                            assert!(started);
                            assert!(completed.is_none(), "duplicate completion");
                            assert_eq!(
                                event["data"],
                                json!({"workspaceId":id,"ranScript":true,"exitCode":code})
                            );
                            completed = Some(event.clone());
                        }
                        Some("terminal:data" | "terminal:exit") => {
                            terminal = event["data"]["terminalId"].as_str().map(str::to_owned);
                        }
                        _ => {}
                    }
                    if let (Some(done), Some(terminal)) = (&completed, &terminal) {
                        break (done.clone(), terminal.clone());
                    }
                }
            })
            .await
            .expect("setup stayed running without terminal client input");
        let marker = std::fs::read_to_string(worktree.join("setup-marker.txt")).unwrap();
        assert!(marker.contains(&format!("SETUP_{case}")));
        let buffer = timeout(common::test_timeout(Duration::from_secs(10)), async {
            loop {
                let result = rpc(
                    &mut client,
                    20,
                    "terminal.getBuffer",
                    json!({"terminalId":terminal_id}),
                )
                .await;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(result["data"].as_str().unwrap())
                    .unwrap();
                let output = String::from_utf8_lossy(&bytes).into_owned();
                if output.contains(&format!("SETUP_{case}")) {
                    break output;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("setup output missing");
        let terminals = rpc(&mut client, 30, "terminal.list", json!({"workspaceId":id})).await;
        assert!(terminals["terminals"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["id"] != terminal_id));
        evidence.push(json!({"case":case,"completion":completed,"terminalId":terminal_id,"marker":marker,"buffer":buffer}));
        if case == "success" {
            let missing = data.path().join("missing terminal command.exe");
            let failure = timeout(
                common::test_timeout(Duration::from_secs(10)),
                rpc_response(
                    &mut client,
                    40,
                    "terminal.create",
                    json!({"workspaceId":id,"cols":80,"rows":24,"command":missing.to_string_lossy()}),
                ),
            )
            .await
            .expect("failed terminal launch did not finish its reader cleanup");
            assert_eq!(failure["error"]["code"], -32603, "{failure}");
            let terminals = rpc(&mut client, 41, "terminal.list", json!({"workspaceId":id})).await;
            assert!(terminals["terminals"].as_array().unwrap().is_empty());
            evidence.push(
                json!({"case":"failed_terminal_launch","response":failure,"terminals":terminals}),
            );

            let preload = data.path().join("terminal io fixture.cjs");
            std::fs::write(&preload, include_str!("fixtures/pty-io-backpressure.cjs")).unwrap();
            let node_options = format!(
                "--require {}",
                serde_json::to_string(&preload.to_string_lossy()).unwrap()
            );
            let terminal = rpc(
                &mut client,
                42,
                "terminal.create",
                json!({"workspaceId":id,"cols":80,"rows":24,"command":"node","env":{"NODE_OPTIONS":node_options}}),
            )
            .await;
            let terminal_id = terminal["terminalId"].as_str().unwrap();
            wait_for_output(&mut client, terminal_id, &["IO_READY"]).await;
            let line_end = if cfg!(windows) { "\r" } else { "\n" };
            let input = format!("{}{line_end}", "x".repeat(1024)).repeat(128);
            let write = timeout(
                common::test_timeout(Duration::from_secs(30)),
                rpc(
                    &mut client,
                    43,
                    "terminal.write",
                    json!({"terminalId":terminal_id,"data":base64::engine::general_purpose::STANDARD.encode(input.as_bytes())}),
                ),
            )
            .await
            .expect("simultaneous terminal input and output blocked");
            assert_eq!(write["ok"], true);
            let output = wait_for_output(
                &mut client,
                terminal_id,
                &["IO_INPUT_DONE", "IO_OUTPUT_DONE"],
            )
            .await;
            rpc(
                &mut client,
                44,
                "terminal.kill",
                json!({"terminalId":terminal_id}),
            )
            .await;
            let terminals = rpc(&mut client, 45, "terminal.list", json!({"workspaceId":id})).await;
            assert!(terminals["terminals"].as_array().unwrap().is_empty());
            let tail = output.chars().rev().take(512).collect::<String>();
            let tail = tail.chars().rev().collect::<String>();
            evidence.push(json!({"case":"simultaneous_terminal_io","terminalId":terminal_id,"inputBytes":input.len(),"inputLines":128,"bufferTail":tail,"terminals":terminals}));
        }
    }
    let evidence_dir = std::env::var_os("INTENTD_SETUP_EVIDENCE_DIR")
        .map_or_else(|| data.path().to_path_buf(), std::path::PathBuf::from);
    std::fs::create_dir_all(&evidence_dir).unwrap();
    let artifact = evidence_dir.join("setup-headless-evidence.json");
    let record = json!({"platform":std::env::consts::OS,"cases":evidence});
    std::fs::write(&artifact, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    eprintln!("Setup evidence: {}", artifact.display());
}
