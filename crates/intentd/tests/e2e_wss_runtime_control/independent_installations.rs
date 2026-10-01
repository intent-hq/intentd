//! Real independent processes using production port selection. Isolated data
//! directories simulate installations; they do not prove macOS `LaunchAgent` or
//! two-OS-user behavior. Exact numeric scan/exhaustion has transport unit coverage.

use super::*;
use std::net::TcpListener;

const OTHER_TOKEN: &str = "abababababababababababababababababababababababababababababababab";

// This sidecar fixture records the production target argument before reporting
// readiness. It proves forwarding configuration, not real relay connectivity.
fn write_recording_tailcat(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let script = r#"#!/bin/sh
for arg in "$@"; do
  case "$arg" in --key=*) key="${arg#--key=}" ;; esac
  port="$arg"
done
case "$1" in
  genkey) printf 'test-key' > "$key" ;;
  serve)
    fifo="$(dirname "$0")/sidecar-stop"
    [ -p "$fifo" ] || mkfifo "$fifo"
    printf '%s\n' "$port" > "$(dirname "$0")/tunnel-target-port"
    printf '{"listenAddr":"tc-test-installation"}\n'
    exec cat "$fifo"
    ;;
esac
"#;
    let path = dir.join("fake-tailcat.sh");
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn spawn_installation(dir: &Path, token: &str) -> GuardedChild {
    let mut cmd = common::serve_command_fixed_port();
    // Do not call enable_ws_api: it seeds an explicit numeric port. Do not use
    // serve_command: its env-zero seam bypasses production first selection.
    configure_serve(&mut cmd, dir, "uds", &[("INTENTD_AUTH_TOKEN", token)]);
    cmd.env_remove("INTENTD_TCP_PORT")
        .env_remove("INTENTD_INSECURE")
        .env("INTENTD_CONFIG", dir.join("config.toml"))
        .env("INTENTD_SECRETS_FILE", dir.join("secrets.json"));
    if dir.join("fake-tailcat.sh").exists() {
        cmd.env("INTENTD_TAILCAT_BIN", dir.join("fake-tailcat.sh"));
    }
    common::hermetic_github_identity(&mut cmd, dir);
    GuardedChild::spawn(&mut cmd).expect("spawn isolated installation")
}

fn config(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("config.toml")).unwrap()
}

async fn discover(dir: &Path, token: &str) -> (u16, String, Value) {
    let socket = dir.join("intentd.sock");
    assert!(
        await_uds(&socket).await,
        "{}",
        std::fs::read_to_string(dir.join("daemon.log")).unwrap()
    );
    let status = common::await_wss_status_logged(&socket, &dir.join("daemon.log")).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let fp = status["result"]["fingerprint"].as_str().unwrap().to_owned();
    let saved = intent_core::settings_file::SettingsFile::parse_str(&config(dir)).unwrap();
    assert_eq!(saved.server.ws_api.port, port, "persisted before discovery");

    let pairing = uds_rpc(&socket, 301, "server.pairingInfo", json!({})).await;
    assert_eq!(pairing["jsonrpc"], "2.0");
    assert_eq!(pairing["id"], 301);
    assert_eq!(pairing["result"]["port"], port);
    assert_eq!(pairing["result"]["certFingerprint"], fp);
    assert_eq!(pairing["result"]["token"], token);
    assert_eq!(pairing["result"]["path"], "/ws");

    // Discover via UDS, pin that installation's certificate, then authenticate
    // against the published port exactly as a local client does.
    let mut ws = common::wss_connect_with_retry(
        port,
        client_config(&fp),
        &format!("wss://localhost:{port}/ws?token={token}"),
    )
    .await;
    let remote_status = wss_rpc(&mut ws, 302, "system.status", json!({})).await;
    assert_eq!(remote_status["jsonrpc"], "2.0");
    assert_eq!(remote_status["id"], 302);
    assert_eq!(remote_status["result"]["port"], port);
    assert_eq!(remote_status["result"]["fingerprint"], fp);
    let setting = wss_rpc(
        &mut ws,
        307,
        "settings.get",
        json!({"path":"server.wsApi.port"}),
    )
    .await;
    assert_eq!(setting["result"]["origin"], "file");
    assert_eq!(setting["result"]["value"].as_f64(), Some(f64::from(port)));
    assert_eq!(
        std::fs::read_to_string(dir.join("tunnel-target-port"))
            .unwrap()
            .trim(),
        port.to_string()
    );
    let pairing_uri = uds_rpc(&socket, 308, "pairing.getInfo", json!({})).await;
    assert_eq!(pairing_uri["result"]["port"], port);
    assert_eq!(pairing_uri["result"]["fingerprint"], fp);
    assert!(pairing_uri["result"]["uri"]
        .as_str()
        .unwrap()
        .contains(&format!("&port={port}&")));
    let principal = wss_rpc(&mut ws, 303, "principal.me", json!({})).await;
    assert_eq!(principal["jsonrpc"], "2.0");
    assert_eq!(principal["id"], 303);
    assert!(principal["result"]["id"].is_string(), "{principal}");
    (port, fp, principal["result"]["id"].clone())
}

async fn reject_other_token(port: u16, fp: &str, token: &str) {
    let tls = common::tls_connect_with_retry(port, client_config(fp)).await;
    let result = timeout(
        common::daemon_startup_timeout(),
        tokio_tungstenite::client_async(format!("wss://localhost:{port}/ws?token={token}"), tls),
    )
    .await
    .unwrap();
    match result {
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            assert_eq!(response.status(), 401);
        }
        other => panic!("another installation's token must fail authentication: {other:?}"),
    }
}

fn stop(child: &mut GuardedChild) {
    child.signal(nix::sys::signal::Signal::SIGTERM).unwrap();
    assert!(child
        .wait_with_timeout(common::daemon_startup_timeout())
        .unwrap()
        .expect("daemon exits after SIGTERM")
        .success());
}

#[tokio::test]
async fn simultaneous_installations_keep_identity_and_saved_ports_across_restart_and_conflict() {
    let a_dir = temp_data_dir();
    let b_dir = temp_data_dir();
    for dir in [a_dir.path(), b_dir.path()] {
        std::fs::write(
            dir.join("config.toml"),
            "[server.wsApi]\nenabled = true\n[server.tunnel]\nenabled = true\n",
        )
        .unwrap();
        write_recording_tailcat(dir);
    }
    // Hold only a socket we own. A host service may already own 5181: never
    // kill it or assume a particular first port on a shared developer host.
    let preferred = match TcpListener::bind(("127.0.0.1", 5181)) {
        Ok(listener) => Some(listener),
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => None,
        Err(error) => panic!("reserve preferred port: {error}"),
    };
    // Launch both before awaiting either readiness signal; selection must
    // arbitrate via retained kernel sockets, not a process-global test mutex.
    let mut a = spawn_installation(a_dir.path(), TOKEN);
    let mut b = spawn_installation(b_dir.path(), OTHER_TOKEN);
    assert_ne!(a.id(), b.id());
    let (a_identity, b_identity) = tokio::join!(
        discover(a_dir.path(), TOKEN),
        discover(b_dir.path(), OTHER_TOKEN)
    );
    assert!(a_identity.0 >= 5181 && b_identity.0 >= 5181);
    assert_ne!(a_identity.0, b_identity.0);
    assert_ne!(a_identity.1, b_identity.1, "independent TLS certificates");
    assert_ne!(a_identity.2, b_identity.2, "independent durable principals");
    eprintln!(
        "independent installations selected ports {} and {}",
        a_identity.0, b_identity.0
    );
    reject_other_token(a_identity.0, &a_identity.1, OTHER_TOKEN).await;
    reject_other_token(b_identity.0, &b_identity.1, TOKEN).await;
    let a_saved = config(a_dir.path());
    let b_saved = config(b_dir.path());
    stop(&mut a);
    stop(&mut b);
    drop(preferred);
    match TcpListener::bind(("127.0.0.1", 5181)) {
        Ok(probe) => {
            drop(probe);
            eprintln!("preferred port 5181 is free before reversed restart");
        }
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => eprintln!(
            "host owns 5181; freed-preferred-port branch is covered by controlled transport tests"
        ),
        Err(error) => panic!("probe preferred port: {error}"),
    }
    b = spawn_installation(b_dir.path(), OTHER_TOKEN);
    assert_eq!(discover(b_dir.path(), OTHER_TOKEN).await, b_identity);
    a = spawn_installation(a_dir.path(), TOKEN);
    assert_eq!(discover(a_dir.path(), TOKEN).await, a_identity);
    assert_eq!(config(a_dir.path()), a_saved);
    assert_eq!(config(b_dir.path()), b_saved);

    stop(&mut a);
    let occupied = TcpListener::bind(("127.0.0.1", a_identity.0)).unwrap();
    for _ in 0..2 {
        // Repeated supervisor-like starts must fail at the saved port; they
        // must not assign a new number or degrade to a UDS-only service.
        let log_path = a_dir.path().join("daemon.log");
        let previous_log_len =
            usize::try_from(std::fs::metadata(&log_path).unwrap().len()).unwrap();
        let mut failed = spawn_installation(a_dir.path(), TOKEN);
        let exit = failed
            .wait_with_timeout(common::daemon_startup_timeout())
            .unwrap()
            .expect("occupied saved port must exit");
        assert!(!exit.success());
        assert_eq!(config(a_dir.path()), a_saved);
        assert!(!a_dir.path().join("intentd.pid").exists());
        assert!(UnixStream::connect(a_dir.path().join("intentd.sock"))
            .await
            .is_err());
        let whole_log = std::fs::read_to_string(&log_path).unwrap();
        let log = &whole_log[previous_log_len..];
        assert!(log.contains("Address already in use"), "{log}");
        assert!(
            log.contains(&a_identity.0.to_string()),
            "error identifies saved port: {log}"
        );
    }
    // The other installation remains authenticated and unchanged throughout.
    assert_eq!(discover(b_dir.path(), OTHER_TOKEN).await, b_identity);
    drop(occupied);
    a = spawn_installation(a_dir.path(), TOKEN);
    assert_eq!(discover(a_dir.path(), TOKEN).await, a_identity);
    assert_eq!(config(a_dir.path()), a_saved);
    stop(&mut a);
    stop(&mut b);
}

#[tokio::test]
async fn new_installation_defaults_to_local_only_without_allocating_a_port() {
    let dir = temp_data_dir();
    let mut daemon = spawn_installation(dir.path(), TOKEN);
    let socket = dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = uds_rpc(&socket, 304, "system.status", json!({})).await;
    assert!(status["result"].is_object(), "{status}");
    assert!(status["result"]["port"].is_null(), "{status}");
    let port = uds_rpc(
        &socket,
        306,
        "settings.get",
        json!({"path":"server.wsApi.port"}),
    )
    .await;
    assert_eq!(
        port["result"]["origin"], "default",
        "disabled startup must leave the port unassigned"
    );
    let enabled = uds_rpc(
        &socket,
        305,
        "settings.get",
        json!({"path":"server.wsApi.enabled"}),
    )
    .await;
    assert_eq!(enabled["result"]["value"], false);
    stop(&mut daemon);
}
