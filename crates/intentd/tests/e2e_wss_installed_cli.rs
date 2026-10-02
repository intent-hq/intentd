//! Actual pinned npm adapters + controlled installed CLIs, through production WSS.
//! Opt in with `INTENTD_TEST_CODEX_ADAPTER` / `INTENTD_TEST_CLAUDE_ADAPTER` pointing
//! at each package directory (containing package.json), then run this ignored test.
//! No npm downloads, account credentials or paid prompts occur in this test.
//! The only package-dispatch shim selects those unchanged adapter entrypoints.
#![cfg(unix)]
mod common;
use futures_util::{SinkExt, StreamExt};
use intentd_test_support::GuardedChild;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";
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

fn executable(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}
fn install_cli(home: &Path, provider: &str, generation: u32, log: &Path) {
    let python = intent_providers::resolve_on_path("python3").expect("Python fixture prerequisite");
    let name = if provider == "codex" {
        "codex"
    } else {
        "claude"
    };
    let body = format!(
        "#!{} -S\nPROVIDER = {:?}\nGENERATION = {}\nLOG = {:?}\n{}",
        python.display(),
        provider,
        generation,
        log.to_str().unwrap(),
        include_str!("fixtures/installed-provider-cli.py")
    );
    executable(&home.join(".local/bin").join(name), &body);
}
fn package(key: &str, spec: &str, dependency: &str) -> (PathBuf, Vec<u8>) {
    let dir = PathBuf::from(
        std::env::var_os(key)
            .unwrap_or_else(|| panic!("set {key} to the actual pinned npm package directory")),
    );
    let metadata: Value =
        serde_json::from_slice(&std::fs::read(dir.join("package.json")).unwrap()).unwrap();
    assert_eq!(
        format!(
            "{}@{}",
            metadata["name"].as_str().unwrap(),
            metadata["version"].as_str().unwrap()
        ),
        spec
    );
    assert!(
        dir.parent()
            .unwrap()
            .parent()
            .unwrap()
            .join(dependency)
            .join("package.json")
            .is_file(),
        "bundled runtime dependency must be installed"
    );
    let entry = dir.join("dist/index.js");
    let bytes = std::fs::read(&entry).unwrap();
    (entry, bytes)
}
async fn rpc(ws: &mut common::TlsWs, method: &str, params: Value) -> Value {
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let frame: Value = serde_json::from_str(&text).unwrap();
                    if frame["id"] == 1 {
                        assert_eq!(frame["jsonrpc"], "2.0");
                        assert!(frame.get("error").is_none(), "{method}: {frame}");
                        return frame["result"].clone();
                    }
                }
                Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                _ => {}
            }
        }
    })
    .await
    .expect("bounded WSS response")
}
fn records(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}
async fn start(root: &Path, home: &Path, bin: &Path) -> (GuardedChild, common::TlsWs) {
    common::enable_ws_api(root);
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("daemon.log"))
        .unwrap();
    let mut cmd = common::serve_command();
    cmd.env_clear()
        .env("INTENTD_TCP_PORT", "0")
        .env("INTENTD_DATA_DIR", root)
        .env("INTENTD_WORKSPACES_DIR", root.join("workspaces"))
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_ASSERT_BOUND_CALLER", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("HOME", home)
        .env("SHELL", "/bin/sh")
        .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
        .env("CODEX_FIXTURE_MARKER", "codex-environment")
        .env("CLAUDE_FIXTURE_MARKER", "claude-environment")
        .env("CODEX_PATH", "/must-not-run/inherited-codex")
        .env("CLAUDE_CODE_EXECUTABLE", "/must-not-run/inherited-claude")
        .env("CODEX_CONFIG", r#"{"agents":{"enabled":true}}"#)
        .env("HTTPS_PROXY", "http://127.0.0.1:9")
        .env("DD_INSTRUMENT_SERVICE_WITH_APM", "false")
        .env("NODE_DISABLE_COMPILE_CACHE", "1")
        .stdout(Stdio::null())
        .stderr(log);
    let child = GuardedChild::spawn(&mut cmd).unwrap();
    let status = common::await_wss_status(&root.join("intentd.sock")).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let ws = common::wss_connect_with_retry(
        port,
        cfg,
        &format!("wss://localhost:{port}/ws?token={TOKEN}"),
    )
    .await;
    (child, ws)
}
async fn catalog(ws: &mut common::TlsWs, provider: &str) -> Value {
    let result = rpc(
        ws,
        "models.list",
        json!({"providerId":provider,"forceRefresh":true}),
    )
    .await;
    assert_eq!(result["providerId"], provider);
    result
}
async fn prompt(ws: &mut common::TlsWs, workspace: &str, agent: &str, log: &Path, expected: usize) {
    rpc(
        ws,
        "agent.sendMessage",
        json!({"workspaceId":workspace,"agentId":agent,"content":"controlled fixture turn"}),
    )
    .await;
    timeout(common::rpc_read_timeout(), async {
        loop {
            let state = rpc(
                ws,
                "agent.get",
                json!({"workspaceId":workspace,"agentId":agent}),
            )
            .await;
            if records(log)
                .iter()
                .filter(|r| r["kind"] == "prompt")
                .count()
                >= expected
                && state["agent"]["status"] == "idle"
            {
                break;
            }
            assert_ne!(state["agent"]["status"], "error", "{state}");
            tokio::time::sleep(Duration::from_millis(50)).await; // timing-guard: bounded poll for completed fixture turn
        }
    })
    .await
    .expect("fixture prompt completes");
}
#[tokio::test]
#[ignore = "requires actual pinned npm packages; see module prerequisites; no model access"]
async fn installed_cli_upgrade_through_pinned_adapters_over_wss() {
    let (codex, codex_bytes) = package(
        "INTENTD_TEST_CODEX_ADAPTER",
        intent_providers::CODEX_ACP_NPX_PACKAGE,
        "@openai/codex",
    );
    let (claude, claude_bytes) = package(
        "INTENTD_TEST_CLAUDE_ADAPTER",
        intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE,
        "@anthropic-ai/claude-agent-sdk",
    );
    let dir = common::test_tempdir("itd-installed-adapters-");
    let root = dir.path();
    eprintln!("controlled adapter artifacts: {}", root.display());
    let home = root.join("home");
    let bin = root.join("toolchain");
    std::fs::create_dir_all(home.join(".local/bin")).unwrap();
    std::fs::create_dir_all(&bin).unwrap();
    std::fs::create_dir_all(root.join("workspaces")).unwrap();
    let node = intent_providers::resolve_on_path("node").expect("node prerequisite");
    executable(
        &bin.join("node"),
        &format!(
            "#!/bin/sh\nexport DD_INSTRUMENT_SERVICE_WITH_APM=false\nexec '{}' \"$@\"\n",
            node.display()
        ),
    );
    executable(
        &bin.join("npx"),
        &format!(
            r#"#!/bin/sh
export DD_INSTRUMENT_SERVICE_WITH_APM=false
if [ "$1" = --version ]; then echo 10.9.2; exit 0; fi
for arg in "$@"; do
case "$arg" in
'{}') exec '{}' '{}' ;;
'{}') exec '{}' '{}' ;;
esac
done
exit 97
"#,
            intent_providers::CODEX_ACP_NPX_PACKAGE,
            node.display(),
            codex.display(),
            intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE,
            node.display(),
            claude.display()
        ),
    );
    let log = root.join("cli.jsonl");
    for provider in ["codex", "claude-code"] {
        install_cli(&home, provider, 1, &log);
    }
    let (mut daemon, mut ws) = start(root, &home, &bin).await;
    for provider in ["codex", "claude-code"] {
        let first = catalog(&mut ws, provider).await;
        assert!(
            first["models"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["id"] == "fixture-base"),
            "{first}"
        );
        assert!(!first["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["id"] == "fixture-added"));
        install_cli(&home, provider, 2, &log);
        let upgraded = catalog(&mut ws, provider).await;
        assert!(
            upgraded["models"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["id"] == "fixture-added"),
            "{upgraded}"
        );
    }
    assert!(
        !records(&log).iter().any(|r| r["kind"] == "prompt"),
        "automatic catalogs must never prompt"
    );
    let workspace = rpc(
        &mut ws,
        "workspace.create",
        json!({"title":"Installed CLI adapters","noPrompt":true}),
    )
    .await;
    let workspace = workspace["workspace"]["id"].as_str().unwrap();
    for (index, provider) in ["codex", "claude-code"].iter().enumerate() {
        let created = rpc(&mut ws, "agent.create", json!({"workspaceId":workspace,"name":provider,"model":"fixture-added","provider":provider,"reasoningEffort":"high"})).await;
        let agent = created["agent"]["id"].as_str().unwrap();
        prompt(&mut ws, workspace, agent, &log, index * 2 + 1).await;
        install_cli(&home, provider, 3, &log);
        let refreshed = catalog(&mut ws, provider).await;
        assert!(
            refreshed["models"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["id"] == "fixture-added"),
            "{refreshed}"
        );
        prompt(&mut ws, workspace, agent, &log, index * 2 + 2).await;
    }
    let rows = records(&log);
    for provider in ["codex", "claude-code"] {
        let prompts: Vec<_> = rows
            .iter()
            .filter(|r| r["provider"] == provider && r["kind"] == "prompt")
            .collect();
        assert_eq!(prompts.len(), 2);
        assert!(
            prompts.iter().all(|r| r["generation"] == 2),
            "an installed upgrade does not replace the running session"
        );
        if provider == "codex" {
            assert!(prompts.iter().all(|r| r["effort"] == "high"));
        }
        let session_launch = rows
            .iter()
            .find(|r| r["kind"] == "launch" && r["pid"] == prompts[0]["pid"])
            .unwrap();
        if provider == "claude-code" {
            let args = session_launch["args"].as_array().unwrap();
            let index = args
                .iter()
                .position(|a| a == "--disallowedTools")
                .expect("Claude tool restrictions reach installed CLI");
            assert!(args[index + 1]
                .as_str()
                .unwrap()
                .split(',')
                .any(|tool| tool == "Task"));
            assert!(
                args.iter()
                    .any(|a| a.as_str().is_some_and(|s| s.contains("workspace-mcp"))),
                "Intent MCP reaches actual SDK CLI spawn"
            );
        } else {
            let thread = rows
                .iter()
                .find(|r| r["pid"] == prompts[0]["pid"] && r["method"] == "thread/start")
                .unwrap();
            assert_eq!(thread["params"]["config"]["agents"]["enabled"], false);
            assert_eq!(
                thread["params"]["config"]["features"]["multi_agent_v2"],
                false
            );
            assert!(thread["params"]["config"]["mcp_servers"]["workspace-mcp"].is_object());
        }
        assert!(prompts.iter().all(|r| r["model"] == "fixture-added"));
        assert_eq!(
            prompts[0]["pid"], prompts[1]["pid"],
            "persistent session retained"
        );
        for launch in rows
            .iter()
            .filter(|r| r["provider"] == provider && r["kind"] == "launch")
        {
            assert_eq!(launch["proxy"], "http://127.0.0.1:9");
            let name = if provider == "codex" {
                "codex"
            } else {
                "claude"
            };
            assert_eq!(
                launch["selected"],
                home.join(".local/bin").join(name).to_str().unwrap()
            );
            assert_eq!(launch["marker"], format!("{name}-environment"));
            if provider == "codex" {
                let policy: Value =
                    serde_json::from_str(launch["policy"].as_str().unwrap()).unwrap();
                assert_eq!(policy["agents"]["enabled"], false);
                assert_eq!(policy["features"]["multi_agent_v2"], false);
            }
        }
    }
    drop(ws);
    daemon.signal(nix::sys::signal::Signal::SIGTERM).unwrap();
    assert!(daemon
        .wait_with_timeout(Duration::from_secs(30))
        .unwrap()
        .expect("graceful shutdown")
        .success());
    for name in ["codex", "claude"] {
        std::fs::remove_file(home.join(".local/bin").join(name)).unwrap();
    }
    let before = records(&log).len();
    let (_restarted, mut ws) = start(root, &home, &bin).await;
    for provider in ["codex", "claude-code"] {
        let absent = catalog(&mut ws, provider).await;
        assert_eq!(
            absent["models"],
            json!([]),
            "no persisted previous-runtime rows: {absent}"
        );
        assert!(
            absent["warning"].as_str().unwrap().contains("Install"),
            "{absent}"
        );
    }
    assert_eq!(
        records(&log).len(),
        before,
        "no CLI or bundled fallback launched"
    );
    assert_eq!(std::fs::read(codex).unwrap(), codex_bytes);
    assert_eq!(std::fs::read(claude).unwrap(), claude_bytes);
}
