//! WSS end-to-end coverage for workload memory containment
//! (intent-hq/intent#9809), over the real pinned-TLS WebSocket against a live
//! `intentd serve`:
//!
//! - Fallback: with `agents.workloadMemoryMaxMb = 0` (cgroup containment
//!   off, standing in for a host without a usable cgroup) the daemon starts,
//!   reports `workloadContainment.mode == "oomScoreAdj"`, and script and
//!   `host.exec` children run normally at `oom_score_adj` 800.
//! - Containment: the daemon runs as a delegated systemd service (as the
//!   `.deb` unit does) with a 512 MB workloads ceiling. A runaway script is
//!   OOM-killed inside its leaf and reports `exitReason: "memoryLimit"`, the
//!   daemon pid is unchanged and `system.status` answers throughout; a
//!   service whose shell forks before `exec` has both processes in its leaf;
//!   a sitter-style respawn re-adopts the hierarchy and sweeps empty leaves.
//!   Needs passwordless `sudo` and `systemd-run`; skips with the reason
//!   otherwise.
//!
//! Artifact: the containment test writes `containment-report.json` (status
//! readings, leaf paths, the killed run's `lastRun`) next to the daemon log in
//! its scratch dir; `INTENTD_TEST_KEEP_TMP=1` keeps it.

#![cfg(target_os = "linux")]

use crate::common;

use intentd_test_support::GuardedChild;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
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

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";
const MIB: u64 = 1024 * 1024;

type TlsWs = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

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
    Arc::new(
        ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
                fingerprint: fingerprint.to_string(),
                provider,
            }))
            .with_no_client_auth(),
    )
}

/// One RPC that must succeed; returns `result`.
async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(60)),
            ws.next(),
        )
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

/// A booted daemon; a delegated one is stopped through systemd on drop.
struct Daemon {
    child: GuardedChild,
    unit: Option<String>,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(unit) = &self.unit {
            let _ = Command::new("sudo")
                .args(["-n", "systemctl", "stop", unit])
                .status();
        }
        let _ = self.child.kill_tree();
    }
}

fn seed(data_dir: &Path, agents: &str) {
    std::fs::create_dir_all(data_dir.join("workspaces")).expect("mkdir workspaces");
    std::fs::write(
        data_dir.join("config.toml"),
        format!("[agents]\n{agents}\n"),
    )
    .expect("seed config");
    common::enable_ws_api(data_dir);
}

fn serve(data_dir: &Path) -> Command {
    let mut cmd = common::hermetic_serve_command(data_dir);
    cmd.env("INTENTD_WORKSPACES_DIR", data_dir.join("workspaces"))
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN);
    cmd
}

fn log_file(data_dir: &Path) -> std::fs::File {
    std::fs::File::options()
        .create(true)
        .append(true)
        .open(data_dir.join("daemon.log"))
        .expect("open daemon log")
}

/// Why a delegated systemd service cannot be started here, if it cannot.
fn delegation_unavailable() -> Option<String> {
    if !Path::new("/sys/fs/cgroup/cgroup.controllers").is_file() {
        return Some("cgroup v2 is not mounted".into());
    }
    let sudo = Command::new("sudo")
        .args(["-n", "systemd-run", "--version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match sudo {
        Ok(status) if status.success() => None,
        _ => Some("needs passwordless sudo and systemd-run".into()),
    }
}

/// `serve` as a transient delegated systemd service running as this user,
/// like the `.deb` unit with `Delegate=memory pids`. A shell runs the daemon
/// twice in a row, standing in for the sitter respawning it in place.
fn delegated(data_dir: &Path, unit: &str) -> Command {
    let serve = serve(data_dir);
    let user = String::from_utf8(Command::new("id").arg("-un").output().unwrap().stdout).unwrap();
    let group = String::from_utf8(Command::new("id").arg("-gn").output().unwrap().stdout).unwrap();
    // serve-spawn: allow — wraps the complete hermetic fixture's env and argv in a delegated systemd service
    let mut cmd = Command::new("sudo");
    cmd.args([
        "-n",
        "systemd-run",
        "--pipe",
        "--wait",
        "--quiet",
        "--collect",
    ])
    .arg(format!("--unit={unit}"))
    .args(["-p", "Delegate=memory pids"])
    .arg("-p")
    .arg(format!("User={}", user.trim()))
    .arg("-p")
    .arg(format!("Group={}", group.trim()));
    for (key, value) in serve.get_envs() {
        if let Some(value) = value {
            let mut arg = std::ffi::OsString::from("--setenv=");
            arg.push(key);
            arg.push("=");
            arg.push(value);
            cmd.arg(arg);
        }
    }
    for key in ["PATH", "HOME"] {
        if let Some(value) = std::env::var_os(key) {
            let mut arg = std::ffi::OsString::from(format!("--setenv={key}="));
            arg.push(value);
            cmd.arg(arg);
        }
    }
    cmd.arg("--")
        .args(["sh", "-c", "\"$0\" serve; exec \"$0\" serve"])
        .arg(serve.get_program());
    cmd
}

async fn boot(
    data_dir: &Path,
    mut cmd: Command,
    unit: Option<String>,
) -> (Daemon, Arc<ClientConfig>, u16) {
    let child = GuardedChild::spawn(
        cmd.stdout(Stdio::null())
            .stderr(Stdio::from(log_file(data_dir))),
    )
    .expect("spawn intentd serve");
    let daemon = Daemon { child, unit };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let (cfg, port) = wss_endpoint(&socket).await;
    (daemon, cfg, port)
}

async fn wss_endpoint(socket: &Path) -> (Arc<ClientConfig>, u16) {
    let status = common::await_wss_status(socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().expect("port")).unwrap();
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint");
    (client_config(fingerprint), port)
}

async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

fn create_repo() -> tempfile::TempDir {
    let repo = common::test_tempdir("wlc-repo-");
    let git = |args: &[&str]| {
        let status = Command::new("git")
            .args(args)
            .current_dir(repo.path())
            .stdout(Stdio::null())
            .status()
            .expect("git spawn");
        assert!(status.success(), "git {args:?} failed");
    };
    git(&["init", "--initial-branch=main"]);
    git(&["config", "user.email", "test@example.com"]);
    git(&["config", "user.name", "Test"]);
    std::fs::write(repo.path().join("README.md"), "# Test\n").expect("write readme");
    git(&["add", "."]);
    git(&["commit", "-m", "initial commit"]);
    repo
}

async fn workspace(ws: &mut TlsWs, repo: &Path) -> String {
    let created = wss_rpc(
        ws,
        1,
        "workspace.create",
        json!({ "title": "containment", "repositoryPath": repo.to_string_lossy(), "skipWorktree": true }),
    )
    .await;
    created["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_owned()
}

async fn script(
    ws: &mut TlsWs,
    id: i64,
    ws_id: &str,
    name: &str,
    command: &str,
    mode: &str,
) -> String {
    let created = wss_rpc(
        ws,
        id,
        "script.create",
        json!({ "workspaceId": ws_id, "name": name, "command": command, "mode": mode }),
    )
    .await;
    created["id"].as_str().expect("script id").to_owned()
}

/// Poll `path` until it holds a line (written by a script).
async fn read_when_written(path: &Path) -> String {
    let deadline =
        tokio::time::Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(30));
    loop {
        if let Ok(text) = std::fs::read_to_string(path) {
            if text.ends_with('\n') {
                return text.trim().to_owned();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} was never written",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn cgroup_of(pid: &str) -> String {
    std::fs::read_to_string(format!("/proc/{pid}/cgroup"))
        .unwrap_or_default()
        .trim()
        .trim_start_matches("0::")
        .to_owned()
}

fn daemon_pid(data_dir: &Path) -> String {
    std::fs::read_to_string(data_dir.join("intentd.pid"))
        .expect("pidfile")
        .trim()
        .to_owned()
}

#[tokio::test]
async fn fallback_reports_oom_score_adj_and_scores_workloads() {
    let data_guard = common::test_tempdir_in("/tmp", "itd-wlc-fallback-");
    let data_dir = data_guard.path().to_path_buf();
    let repo = create_repo();
    seed(&data_dir, "workloadMemoryMaxMb = 0");
    let (daemon, cfg, port) = boot(&data_dir, serve(&data_dir), None).await;
    let mut ws = connect(port, cfg).await;

    let status = wss_rpc(&mut ws, 2, "system.status", json!({})).await;
    let containment = &status["workloadContainment"];
    assert_eq!(containment["mode"], "oomScoreAdj", "{status}");
    assert!(
        containment["reason"]
            .as_str()
            .is_some_and(|r| r.contains("workloadMemoryMaxMb")),
        "{containment}"
    );
    assert!(containment.get("memoryMaxBytes").is_none(), "{containment}");

    let ws_id = workspace(&mut ws, repo.path()).await;
    let out = data_dir.join("score.txt");
    let id = script(
        &mut ws,
        3,
        &ws_id,
        "score",
        &format!("cat /proc/self/oom_score_adj > {}", out.display()),
        "command",
    )
    .await;
    let run = wss_rpc(
        &mut ws,
        4,
        "script.run",
        json!({ "workspaceId": ws_id, "scriptId": id }),
    )
    .await;
    assert_eq!(run["exitCode"], 0, "{run}");
    assert_eq!(read_when_written(&out).await, "800");

    let exec = wss_rpc(
        &mut ws,
        5,
        "host.exec",
        json!({ "command": "cat", "args": ["/proc/self/oom_score_adj"], "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        exec["stdout"].as_str().map(str::trim),
        Some("800"),
        "{exec}"
    );
    assert!(exec.get("exitReason").is_none(), "{exec}");
    drop(daemon);
}

#[tokio::test]
async fn cgroup_box_kills_a_runaway_script_and_the_daemon_survives() {
    if let Some(reason) = delegation_unavailable() {
        eprintln!("skipping cgroup_box_kills_a_runaway_script_and_the_daemon_survives: {reason}");
        return;
    }
    let data_guard = common::test_tempdir_in("/tmp", "itd-wlc-cgroup-");
    let data_dir = data_guard.path().to_path_buf();
    let repo = create_repo();
    seed(&data_dir, "workloadMemoryMaxMb = 512");
    let unit = format!("intentd-wlc-test-{}", std::process::id());
    let (daemon, cfg, port) =
        boot(&data_dir, delegated(&data_dir, &unit), Some(unit.clone())).await;
    let mut ws = connect(port, cfg).await;
    let mut report = json!({ "unit": unit });

    let status = wss_rpc(&mut ws, 2, "system.status", json!({})).await;
    let containment = status["workloadContainment"].clone();
    report["statusBefore"] = containment.clone();
    assert_eq!(containment["mode"], "cgroup", "{containment}");
    assert_eq!(containment["memoryMaxBytes"], 512 * MIB, "{containment}");
    let pid = daemon_pid(&data_dir);
    let daemon_cgroup = cgroup_of(&pid);
    assert!(
        daemon_cgroup.ends_with(&format!("{unit}.service/daemon")),
        "{daemon_cgroup}"
    );
    let unit_dir = PathBuf::from(format!(
        "/sys/fs/cgroup{}",
        daemon_cgroup.trim_end_matches("/daemon")
    ));

    // PTY path: the shell forks a child before it execs; both are in the
    // script's leaf from their first instruction.
    let ws_id = workspace(&mut ws, repo.path()).await;
    let child_pid = data_dir.join("child.pid");
    let shell_pid = data_dir.join("shell.pid");
    let service = script(
        &mut ws,
        3,
        &ws_id,
        "forker",
        &format!(
            "sleep 300 & echo $! > {}; echo $$ > {}; exec sleep 300",
            child_pid.display(),
            shell_pid.display()
        ),
        "service",
    )
    .await;
    wss_rpc(
        &mut ws,
        4,
        "script.start",
        json!({ "workspaceId": ws_id, "scriptId": service }),
    )
    .await;
    let forked = cgroup_of(&read_when_written(&child_pid).await);
    let shell = cgroup_of(&read_when_written(&shell_pid).await);
    report["serviceLeaf"] = json!(shell);
    assert!(shell.contains("/workloads/script-"), "{shell}");
    assert_eq!(forked, shell, "the forked child shares the shell's leaf");

    // A runaway allocation (1 GiB with no newline, buffered by `tail`) is
    // OOM-killed inside the 512 MB box.
    let hog = script(
        &mut ws,
        5,
        &ws_id,
        "hog",
        "head -c 1073741824 /dev/zero | tail -c 1",
        "command",
    )
    .await;
    let run = wss_rpc(
        &mut ws,
        6,
        "script.run",
        json!({ "workspaceId": ws_id, "scriptId": hog }),
    )
    .await;
    assert_ne!(run["exitCode"], 0, "{run}");
    let got = wss_rpc(
        &mut ws,
        7,
        "script.get",
        json!({ "workspaceId": ws_id, "scriptId": hog }),
    )
    .await;
    report["killedRun"] = got.clone();
    assert_eq!(got["runtime"]["exitReason"], "memoryLimit", "{got}");
    assert_eq!(got["lastRun"]["exitReason"], "memoryLimit", "{got}");
    assert_eq!(got["lastRun"]["outcome"], "failed", "{got}");

    let after = wss_rpc(&mut ws, 8, "system.status", json!({})).await;
    report["statusAfter"] = after["workloadContainment"].clone();
    assert!(
        after["workloadContainment"]["oomKills"].as_u64() >= Some(1),
        "{after}"
    );
    assert_eq!(daemon_pid(&data_dir), pid, "the daemon survived");
    assert_eq!(cgroup_of(&pid), daemon_cgroup);

    // Respawn in place, as the sitter does: the new daemon starts in
    // `supervisor`, re-adopts the hierarchy, and empty leaves are swept.
    wss_rpc(
        &mut ws,
        9,
        "script.stop",
        json!({ "workspaceId": ws_id, "scriptId": service }),
    )
    .await;
    drop(ws);
    let killed = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("kill");
    assert!(killed.success());
    let socket = data_dir.join("intentd.sock");
    let deadline =
        tokio::time::Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(60));
    let respawned = loop {
        if let Ok(new_pid) = std::fs::read_to_string(data_dir.join("intentd.pid")) {
            let new_pid = new_pid.trim().to_owned();
            if !new_pid.is_empty() && new_pid != pid && await_uds(&socket).await {
                break new_pid;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the daemon was not respawned"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let (cfg, port) = wss_endpoint(&socket).await;
    let mut ws = connect(port, cfg).await;
    let status = wss_rpc(&mut ws, 10, "system.status", json!({})).await;
    report["statusRespawned"] = status["workloadContainment"].clone();
    assert_eq!(status["workloadContainment"]["mode"], "cgroup", "{status}");
    assert_eq!(
        cgroup_of(&respawned),
        daemon_cgroup,
        "re-adopted the daemon leaf"
    );
    let leaves: Vec<String> = std::fs::read_dir(unit_dir.join("workloads"))
        .expect("workloads dir")
        .flatten()
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    report["leavesAfterRespawn"] = json!(leaves);
    for leaf in &leaves {
        let procs =
            std::fs::read_to_string(unit_dir.join("workloads").join(leaf).join("cgroup.procs"))
                .unwrap_or_default();
        assert!(
            !procs.trim().is_empty(),
            "empty leaf {leaf} leaked across the respawn"
        );
    }
    std::fs::write(
        data_dir.join("containment-report.json"),
        serde_json::to_string_pretty(&report).unwrap(),
    )
    .expect("write report");
    drop(daemon);
}
