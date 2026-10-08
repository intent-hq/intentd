//! Real detached launcher/daemon lifecycle, including authenticated WSS.
mod common;
use futures_util::{SinkExt, StreamExt};
#[cfg(unix)]
use intentd_test_support::GuardedChild;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
#[cfg(windows)]
use windows_child::GuardedChild;
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";
/// This cross-package test needs a freshly built sitter artifact, which Cargo
/// does not expose through `CARGO_BIN_EXE` for a sibling package. Run explicitly
/// after building intentd-sitter, setting `INTENTD_TEST_SITTER_BIN` to its binary.
#[tokio::test]
#[ignore = "requires freshly built INTENTD_TEST_SITTER_BIN; run with --run-ignored ignored-only"]
async fn detached_sitter_lifecycle_over_wss() {
    use intentd_sitter::{paths::SitterPaths, state};
    #[cfg(unix)]
    use nix::sys::signal::{killpg, Signal};
    #[cfg(unix)]
    use nix::unistd::Pid;

    struct SessionGuard(std::path::PathBuf);
    impl Drop for SessionGuard {
        fn drop(&mut self) {
            if let Some(pid) = intentd_sitter::supervisor::read_live_pid(&self.0) {
                #[cfg(unix)]
                let _ = killpg(pid, Signal::SIGKILL);
                #[cfg(windows)]
                if let Ok(process) =
                    intentd_sitter::windows::Process::open(pid.as_raw().cast_unsigned(), true)
                {
                    let _ = process.terminate();
                }
            }
        }
    }

    struct HttpGuard(
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        Option<std::thread::JoinHandle<()>>,
    );
    impl Drop for HttpGuard {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Relaxed);
            self.1.take().unwrap().join().unwrap();
        }
    }

    let sitter = std::env::var_os("INTENTD_TEST_SITTER_BIN")
        .expect("build intentd-sitter and set INTENTD_TEST_SITTER_BIN");
    let dir = common::test_tempdir("intentd-start-wss-");
    let data = dir.path();
    let paths = SitterPaths::from_data_dir(data);
    let _session = SessionGuard(paths.pid_path.clone());
    let binary = paths.daemon_binary(env!("CARGO_PKG_VERSION"));
    std::fs::create_dir_all(binary.parent().unwrap()).unwrap();
    std::fs::copy(env!("CARGO_BIN_EXE_intentd"), &binary).unwrap();
    state::save(
        &paths.state_path,
        &state::SitterState {
            current_version: Some(env!("CARGO_PKG_VERSION").into()),
            ..Default::default()
        },
    )
    .unwrap();
    common::enable_ws_api(data);
    let config_path = data.join("custom-config.toml");
    std::fs::rename(data.join("config.toml"), &config_path).unwrap();
    let workspaces = data.join("workspaces");
    std::fs::create_dir(&workspaces).unwrap();
    // A held local listener cannot ever serve a real release manifest. It also
    // proves a startup timeout can cancel the supervisor's initial update check.
    // For this successful lifecycle use HTTP 404 rather than the public network.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopped = stop.clone();
    let _http = HttpGuard(
        stop,
        Some(std::thread::spawn(move || {
            use std::io::Write;
            while !stopped.load(std::sync::atomic::Ordering::Relaxed) {
                if let Ok((mut stream, _)) = listener.accept() {
                    let _ = stream.write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                }
                // timing-guard: poll the private fixture's shutdown flag
                std::thread::sleep(Duration::from_millis(10));
            }
        })),
    );
    let make_command = |verb: &str| {
        let mut command = Command::new(&sitter);
        // serve-spawn: allow — the installed sitter owns serve; copy the complete hermetic constructor environment
        let isolated = common::hermetic_serve_command(data);
        for (key, value) in isolated.get_envs() {
            match value {
                Some(value) => {
                    command.env(key, value);
                }
                None => {
                    command.env_remove(key);
                }
            }
        }
        command
            .arg(verb)
            .env("INTENTD_AUTH_TOKEN", TOKEN)
            .env("INTENTD_CONFIG", &config_path)
            .env("INTENTD_WORKSPACES_DIR", &workspaces)
            .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
            .env(
                "INTENTD_SITTER_MANIFEST_BASE_URL",
                format!("http://{address}"),
            )
            .env("INTENTD_SITTER_READINESS_TIMEOUT_MS", "30000")
            .env_remove("INTENTD_CHANNEL")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        common::hermetic_fixture_identity(&mut command, data);
        command
    };
    let invoke = |verb: &str| {
        let mut command = make_command(verb);
        let mut child = GuardedChild::spawn(&mut command).unwrap();
        assert!(
            child
                .wait_with_timeout(Duration::from_secs(70))
                .unwrap()
                .is_some(),
            "{verb} timed out"
        );
        let output = child.disarm().wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{verb}: {output:?}; log: {}",
            std::fs::read_to_string(paths.sitter_dir.join("start.log")).unwrap_or_default()
        );
    };
    // Start from a stale supervisor record; restart shares detached startup.
    std::fs::write(&paths.pid_path, "4294967294\n").unwrap();
    invoke("restart");
    #[cfg(unix)]
    let supervisor = intentd_sitter::supervisor::read_live_pid(&paths.pid_path).unwrap();
    #[cfg(unix)]
    assert_eq!(nix::unistd::getsid(Some(supervisor)).unwrap(), supervisor);
    let socket = data.join("intentd.sock");
    let log = paths.sitter_dir.join("start.log");
    let first_pid = std::fs::read_to_string(data.join("intentd.pid")).unwrap();
    invoke("start");
    assert_eq!(
        std::fs::read_to_string(data.join("intentd.pid")).unwrap(),
        first_pid
    );
    invoke("status");
    #[cfg(windows)]
    let first_pid = {
        // Leave a legitimate prior restart request in B's control directory:
        // signaling its event from A must not replay that previous request.
        invoke("restart");
        let replacement = std::fs::read_to_string(data.join("intentd.pid")).unwrap();
        assert_ne!(replacement, first_pid);
        replacement
    };
    #[cfg(windows)]
    {
        // Instance A's stale numeric PID must never control live instance B.
        let foreign = common::test_tempdir("intentd-stale-supervisor-");
        let foreign_paths = SitterPaths::from_data_dir(foreign.path());
        std::fs::create_dir_all(&foreign_paths.sitter_dir).unwrap();
        let supervisor_pid = std::fs::read_to_string(&paths.pid_path).unwrap();
        let supervisor =
            intentd_sitter::windows::Process::open(supervisor_pid.trim().parse().unwrap(), false)
                .unwrap();
        let daemon =
            intentd_sitter::windows::Process::open(first_pid.trim().parse().unwrap(), false)
                .unwrap();
        for option in ["--help", "-h", "--invalid-lifecycle-option"] {
            let mut command = make_command("stop");
            command.arg(option);
            let mut child = GuardedChild::spawn(&mut command).unwrap();
            assert!(child
                .wait_with_timeout(Duration::from_secs(5))
                .unwrap()
                .is_some());
            let output = child.disarm().wait_with_output().unwrap();
            let help = option != "--invalid-lifecycle-option";
            assert_eq!(output.status.success(), help, "{option}: {output:?}");
            if help {
                assert!(String::from_utf8_lossy(&output.stdout).contains("Usage:"));
            }
            assert!(
                !supervisor.exited().unwrap(),
                "stop option killed supervisor"
            );
            assert!(!daemon.exited().unwrap(), "stop option killed daemon");
            assert_eq!(
                std::fs::read_to_string(&paths.pid_path).unwrap(),
                supervisor_pid
            );
            assert_eq!(
                std::fs::read_to_string(data.join("intentd.pid")).unwrap(),
                first_pid
            );
        }
        std::fs::write(&foreign_paths.pid_path, &supervisor_pid).unwrap();
        for missing_identity in [true, false] {
            if !missing_identity {
                std::fs::write(
                    foreign_paths.pid_path.with_extension("identity"),
                    format!("{}:0", supervisor_pid.trim()),
                )
                .unwrap();
            }
            for verb in ["restart", "stop"] {
                let mut command = make_command(verb);
                command
                    .env("INTENTD_DATA_DIR", foreign.path())
                    .env("INTENTD_CONFIG", foreign.path().join("config.toml"))
                    .env("INTENTD_SITTER_READINESS_TIMEOUT_MS", "1000");
                common::hermetic_fixture_identity(&mut command, foreign.path());
                let mut child = GuardedChild::spawn(&mut command).unwrap();
                assert!(!child
                    .wait_with_timeout(Duration::from_secs(5))
                    .unwrap()
                    .expect("foreign control must fail promptly")
                    .success());
                assert!(
                    !supervisor.exited().unwrap(),
                    "foreign stop killed supervisor"
                );
                assert!(!daemon.exited().unwrap(), "foreign control replaced daemon");
                assert_eq!(
                    std::fs::read_to_string(&paths.pid_path).unwrap(),
                    supervisor_pid
                );
                assert_eq!(
                    std::fs::read_to_string(data.join("intentd.pid")).unwrap(),
                    first_pid
                );
                invoke("status");
            }
        }
    }
    for restarted in [false, true] {
        if restarted {
            invoke("restart");
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            while !std::fs::read_to_string(data.join("intentd.pid"))
                .is_ok_and(|pid| pid != first_pid)
            {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "replacement pid missing"
                );
                // timing-guard: poll replacement daemon ownership
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        let status = local_status(data, &socket, &log).await;
        let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
        let mut ws = connect_ws(
            port,
            client_config(status["result"]["fingerprint"].as_str().unwrap()),
        )
        .await;
        let response = wss_rpc(&mut ws, 701, "system.status", json!({})).await;
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 701);
        assert_eq!(response["result"]["version"], env!("CARGO_PKG_VERSION"));
        ws.close(None).await.unwrap();
    }
    invoke("stop");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while paths.pid_path.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "sitter did not stop"
        );
        // timing-guard: poll supervisor cleanup after graceful daemon shutdown
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    #[cfg(unix)]
    assert_eq!(
        nix::sys::signal::kill(Pid::from_raw(first_pid.trim().parse().unwrap()), None),
        Err(nix::errno::Errno::ESRCH)
    );
    invoke("stop");
    std::thread::scope(|scope| {
        let start = scope.spawn(|| invoke("start"));
        let restart = scope.spawn(|| invoke("restart"));
        start.join().unwrap();
        restart.join().unwrap();
    });
    invoke("status");
    invoke("stop");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while paths.pid_path.exists() {
        assert!(tokio::time::Instant::now() < deadline);
        // timing-guard: wait for the previous supervisor to release ownership
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // Invalid forwarded options must fail and clean only the owned launch.
    let mut invalid = make_command("start");
    invalid.arg("--invalid-lifecycle-option");
    let mut failed = GuardedChild::spawn(&mut invalid).unwrap();
    assert!(!failed
        .wait_with_timeout(Duration::from_secs(40))
        .unwrap()
        .unwrap()
        .success());
    assert!(
        !paths.pid_path.exists(),
        "failed launch leaked supervisor ownership"
    );
    #[cfg(windows)]
    // A failed spawned child must not shut down a different, unsupervised
    // daemon whose endpoint became available during initial readiness.
    {
        let template = make_command("serve");
        let mut direct = Command::new(&binary);
        direct.arg("serve");
        for (key, value) in template.get_envs() {
            match value {
                Some(value) => {
                    direct.env(key, value);
                }
                None => {
                    direct.env_remove(key);
                }
            }
        }
        common::hermetic_fixture_identity(&mut direct, data);
        direct.stdout(Stdio::null()).stderr(Stdio::null());
        let mut owner = GuardedChild::spawn(&mut direct).unwrap();
        local_status(data, &socket, &log).await;
        let owner_pid = std::fs::read(data.join("intentd.pid")).unwrap();
        let mut duplicate = make_command("serve");
        duplicate.env("INTENTD_SITTER_STARTING", "1");
        // The real data-directory lock makes the spawned child exit. The
        // surviving daemon is ready by the time failed-start cleanup runs.
        let mut failed = GuardedChild::spawn(&mut duplicate).unwrap();
        assert!(
            failed
                .wait_with_timeout(Duration::from_secs(10))
                .unwrap()
                .is_some(),
            "duplicate launch must exit after failing the data-directory lock"
        );
        let output = failed.disarm().wait_with_output().unwrap();
        assert!(!output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("daemon exited before startup readiness"),
            "regression must exercise failed-child cleanup: {output:?}"
        );
        assert!(
            owner.wait_with_timeout(Duration::ZERO).unwrap().is_none(),
            "failed launch stopped the other daemon: {output:?}"
        );
        assert_eq!(std::fs::read(data.join("intentd.pid")).unwrap(), owner_pid);
        local_status(data, &socket, &log).await;
        // A different retained process, or malformed guard, cannot authorize
        // shutdown of this endpoint. No guarded request may reach the daemon.
        for expected in [std::process::id().to_string(), "invalid".into(), "0".into()] {
            let rejected = tokio::process::Command::new(&binary)
                .args(["call", "system.shutdown"])
                .env("INTENTD_DATA_DIR", data)
                .env("INTENTD_SHUTDOWN_EXPECTED_PID", expected)
                .kill_on_drop(true)
                .output()
                .await
                .unwrap();
            assert!(
                !rejected.status.success(),
                "foreign endpoint accepted shutdown: {rejected:?}"
            );
            assert!(owner.wait_with_timeout(Duration::ZERO).unwrap().is_none());
        }
        let accepted = tokio::process::Command::new(&binary)
            .args(["call", "system.shutdown"])
            .env("INTENTD_DATA_DIR", data)
            .env("INTENTD_SHUTDOWN_EXPECTED_PID", owner.id().to_string())
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(
            accepted.status.success(),
            "owned endpoint rejected shutdown: {accepted:?}"
        );
        assert!(owner
            .wait_with_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .success());
    }
    {
        // Stop must reach the sitter before there is any daemon pidfile.
        // A held HTTP endpoint keeps its initial update check pending.
        let stalled = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut command = make_command("serve");
        command.env(
            "INTENTD_SITTER_MANIFEST_BASE_URL",
            format!("http://{}", stalled.local_addr().unwrap()),
        );
        let mut booting = GuardedChild::spawn(&mut command).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !paths.pid_path.exists() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "booting supervisor did not publish ownership"
            );
            // timing-guard: wait for control publication before stopping during startup
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        #[cfg(windows)]
        {
            let mut restart = make_command("restart");
            restart.env("INTENTD_SITTER_READINESS_TIMEOUT_MS", "300");
            let mut waiting = GuardedChild::spawn(&mut restart).unwrap();
            assert!(
                !waiting
                    .wait_with_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap()
                    .success(),
                "restart must not report ready while startup is stalled"
            );
        }
        invoke("stop");
        let stopped = booting
            .wait_with_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        #[cfg(unix)]
        assert!(
            stopped.success(),
            "private stop must not trigger service restart: {stopped}"
        );
        #[cfg(windows)]
        let _ = stopped;
        assert!(!paths.pid_path.exists());
        drop(stalled);

        // A crashing child with a long backoff must not respawn after stop.
        let log_path = data.join("backoff.log");
        let log_file = std::fs::File::create(&log_path).unwrap();
        let mut command = make_command("serve");
        command
            .arg("--invalid-lifecycle-option")
            .env("INTENTD_SITTER_BACKOFF_INITIAL_MS", "30000")
            .stderr(log_file);
        let mut recovering = GuardedChild::spawn(&mut command).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !std::fs::read_to_string(&log_path)
            .is_ok_and(|log| log.contains("respawning intentd in"))
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "supervisor did not reach crash backoff"
            );
            // timing-guard: observe the supervisor's backoff state before requesting stop
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        invoke("stop");
        let stopped = recovering
            .wait_with_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        #[cfg(unix)]
        assert!(
            stopped.success(),
            "private stop must not trigger service restart: {stopped}"
        );
        #[cfg(windows)]
        let _ = stopped;
        assert!(!paths.pid_path.exists());
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // The installer starts `serve` directly from Task Scheduler, with no
        // STARTING marker and no inherited console. Exercise that same path.
        let mut command = make_command("serve");
        command
            .creation_flags(0x0800_0000)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut foreground = GuardedChild::spawn(&mut command).unwrap();
        local_status(data, &socket, &log).await;
        let old_pid = std::fs::read_to_string(data.join("intentd.pid")).unwrap();
        invoke("restart");
        assert_ne!(
            std::fs::read_to_string(data.join("intentd.pid")).unwrap(),
            old_pid
        );
        invoke("stop");
        assert!(foreground
            .wait_with_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .success());
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
        let next = timeout(common::test_timeout(Duration::from_secs(30)), ws.next())
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
async fn local_status(
    data: &std::path::Path,
    _socket: &std::path::Path,
    log: &std::path::Path,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_intentd"));
        command
            .args(["call", "system.status"])
            .env("INTENTD_DATA_DIR", data)
            .kill_on_drop(true);
        let custom_config = data.join("custom-config.toml");
        if custom_config.exists() {
            command.env("INTENTD_CONFIG", custom_config);
        }
        if let Ok(Ok(output)) = timeout(Duration::from_secs(5), command.output()).await {
            if let Ok(value) = serde_json::from_slice::<Value>(&output.stdout) {
                if value["port"].as_u64().is_some() {
                    return json!({"result":value});
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "WSS not ready: {}",
            std::fs::read_to_string(log).unwrap_or_default()
        );
        // timing-guard: poll actual WSS listener readiness with a deadline
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Regression: Windows used to always return failure even after graceful exit.
#[cfg(windows)]
#[tokio::test]
async fn windows_stop_confirms_graceful_exit_without_supervisor() {
    let dir = common::test_tempdir("windows-stop-regression-");
    common::enable_ws_api(dir.path());
    let mut command = common::hermetic_serve_command(dir.path());
    let mut daemon = GuardedChild::spawn(&mut command).unwrap();
    local_status(
        dir.path(),
        &dir.path().join("intentd.sock"),
        &dir.path().join("daemon.log"),
    )
    .await;
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("stop")
        .env("INTENTD_DATA_DIR", dir.path())
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "stop must confirm graceful shutdown: {output:?}"
    );
    assert!(daemon
        .wait_with_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap()
        .success());
}

#[cfg(windows)]
#[test]
fn windows_hung_fixture() {
    if std::env::var_os("INTENTD_LIFECYCLE_HUNG_FIXTURE").is_some() {
        std::thread::park_timeout(Duration::from_secs(60));
    }
}

#[cfg(windows)]
#[tokio::test]
async fn windows_stop_bounds_hung_rpc_and_only_terminates_owned_process() {
    use intentd_sitter::windows::Process;
    use tokio::net::windows::named_pipe::ServerOptions;
    for owned in [false, true] {
        let dir = common::test_tempdir("windows-stop-owned-");
        let pid_path = dir.path().join("intentd.pid");
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "windows_hung_fixture"])
            .env("INTENTD_LIFECYCLE_HUNG_FIXTURE", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = GuardedChild::spawn(&mut command).unwrap();
        let process = Process::open(child.id(), false).unwrap();
        std::fs::write(&pid_path, child.id().to_string()).unwrap();
        if owned {
            std::fs::write(
                pid_path.with_extension("identity"),
                format!("{}:{}", child.id(), process.creation_time().unwrap()),
            )
            .unwrap();
        } else {
            // A reused PID has a different creation timestamp.
            std::fs::write(
                pid_path.with_extension("identity"),
                format!("{}:0", child.id()),
            )
            .unwrap();
        }
        let name =
            intent_transport::pipe_name_for_socket_path(&dir.path().join("intentd.sock")).unwrap();
        let server = ServerOptions::new()
            .first_pipe_instance(true)
            .create(name)
            .unwrap();
        let peer = tokio::spawn(async move {
            server.connect().await.unwrap();
            std::future::pending::<()>().await;
            drop(server);
        });
        let output = timeout(
            Duration::from_secs(20),
            tokio::process::Command::new(env!("CARGO_BIN_EXE_intentd"))
                .arg("stop")
                .env("INTENTD_DATA_DIR", dir.path())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        peer.abort();
        assert_eq!(output.status.success(), owned, "{output:?}");
        assert_eq!(process.exited().unwrap(), owned);
        if owned {
            assert!(child
                .wait_with_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
                .success());
        }
    }
}

#[cfg(windows)]
mod windows_child {
    use std::io;
    use std::process::{Child, Command, ExitStatus};
    use std::time::{Duration, Instant};
    // The shared GuardedChild is Unix-only. Windows Child retains a kernel
    // handle across PID reuse; the sitter itself contains descendants in a job.
    // raw-child: allow — Windows has no shared GuardedChild; this guard retains the kernel handle
    pub struct GuardedChild(Option<Child>);
    impl GuardedChild {
        pub fn spawn(command: &mut Command) -> io::Result<Self> {
            command.spawn().map(|child| Self(Some(child)))
        }
        pub fn id(&self) -> u32 {
            self.0.as_ref().unwrap().id()
        }
        pub fn wait_with_timeout(&mut self, budget: Duration) -> io::Result<Option<ExitStatus>> {
            let deadline = Instant::now() + budget;
            loop {
                if let Some(status) = self.0.as_mut().unwrap().try_wait()? {
                    return Ok(Some(status));
                }
                if Instant::now() >= deadline {
                    return Ok(None);
                }
                // timing-guard: bounded child exit polling against a retained handle
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        // raw-child: allow — transfer the retained Windows child to wait_with_output
        pub fn disarm(mut self) -> Child {
            self.0.take().unwrap()
        }
    }
    impl Drop for GuardedChild {
        fn drop(&mut self) {
            if let Some(child) = &mut self.0 {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}
