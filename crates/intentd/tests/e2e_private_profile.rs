//! Process-start containment through the real daemon and UDS settings surface.
//! WSS is intentionally unavailable in this profile; the gh-only policy has
//! separate real TLS/WSS coverage in `e2e_wss_github_device_flow`.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use intentd_test_support::GuardedChild;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::{interval, timeout};

const PROFILE: &str = "INTENTD_PRIVATE_TEST_PROFILE";

fn command(data: &Path) -> Command {
    std::fs::create_dir_all(data.join("workspaces")).unwrap();
    let log = std::fs::File::create(data.join("daemon.log")).unwrap();
    let mut command = common::hermetic_serve_command(data);
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap())
        .env("HOME", data)
        .env(PROFILE, "1")
        .env("INTENTD_DATA_DIR", data)
        .env("INTENTD_CONFIG", data.join("config.toml"))
        .env("INTENTD_WORKSPACES_DIR", data.join("workspaces"))
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_LEGACY_APP_DIR", "")
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_ASSERT_BOUND_CALLER", "1")
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    common::hermetic_fixture_identity(&mut command, data);
    command
}

async fn ready(data: &Path, daemon: &mut GuardedChild) {
    timeout(common::daemon_startup_timeout(), async {
        let mut tick = interval(Duration::from_millis(20));
        loop {
            assert!(
                daemon.try_wait().unwrap().is_none(),
                "daemon exited before ready"
            );
            let log = std::fs::read_to_string(data.join("daemon.log")).unwrap();
            if log.contains("config.toml live-reload watcher ready")
                && UnixStream::connect(data.join("intentd.sock")).await.is_ok()
            {
                break;
            }
            tick.tick().await;
        }
    })
    .await
    .expect("original daemon/socket/config-watcher readiness");
}

async fn rpc(data: &Path, method: &str, params: Value) -> Value {
    let socket = UnixStream::connect(data.join("intentd.sock"))
        .await
        .unwrap();
    let (read, mut write) = socket.into_split();
    write
        .write_all(
            format!(
                "{}\n",
                json!({"jsonrpc":"2.0","id":1,"method":method,"params":params})
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut line = String::new();
    timeout(
        common::rpc_read_timeout(),
        BufReader::new(read).read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    let response: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(response["id"], 1, "{response}");
    response
}

fn pins() -> [(&'static str, Value); 7] {
    [
        ("server.bindAddress", json!("127.0.0.1")),
        ("server.wsApi.enabled", json!(false)),
        ("server.tunnel.enabled", json!(false)),
        ("server.tls.enabled", json!(true)),
        ("server.auth.enabled", json!(true)),
        ("updates.checkOnIdle", json!(false)),
        (
            "sourceControl.github.exposeGitCredentialToChildren",
            json!(false),
        ),
    ]
}

async fn assert_private(data: &Path) {
    for (path, expected) in pins() {
        let value = rpc(data, "settings.get", json!({"path":path})).await;
        eprintln!("private pin {path}: {value}");
        assert_eq!(value["result"]["value"], expected, "{value}");
        assert_eq!(value["result"]["origin"], "flag", "{value}");
    }
    let status = rpc(data, "system.status", json!({})).await;
    eprintln!("private runtime status: {status}");
    assert_eq!(status["result"]["listenMode"], "uds", "{status}");
    assert_eq!(status["result"]["transports"], json!(["uds"]), "{status}");
    assert!(status["result"]["port"].is_null(), "{status}");
    assert_eq!(status["result"]["localIps"], json!([]), "{status}");
    assert_eq!(
        status["result"]["idleUpdateCheck"]["enabled"], false,
        "{status}"
    );
    let pairing = rpc(data, "server.pairingInfo", json!({})).await;
    assert!(pairing.get("error").is_none(), "{pairing}");
    assert!(pairing["result"]["port"].is_null(), "{pairing}");
    assert!(
        data.join("ws-cert.pem").is_file(),
        "original TLS provisioning"
    );
    assert!(
        data.join("ws-key.pem").is_file(),
        "original TLS key provisioning"
    );
    let credential = rpc(
        data,
        "system.gitCredential",
        json!({"protocol":"https","host":"github.com"}),
    )
    .await;
    assert!(credential.get("error").is_none(), "{credential}");
    assert!(credential["result"]["credential"].is_null(), "{credential}");
}

fn stop(daemon: &mut GuardedChild) {
    daemon.signal(nix::sys::signal::Signal::SIGTERM).unwrap();
    let exit = daemon
        .wait_with_timeout(Duration::from_secs(10))
        .unwrap()
        .expect("owned graceful exit");
    eprintln!("private daemon pid={} original exit={exit}", daemon.id());
    assert!(exit.success(), "{exit}");
}

const CONFLICTING: &str = r#"
[server]
bindAddress = "0.0.0.0"
[server.wsApi]
enabled = true
[server.tunnel]
enabled = true
[server.tls]
enabled = false
[server.auth]
enabled = false
[updates]
checkOnIdle = true
[sourceControl.github]
exposeGitCredentialToChildren = true
"#;

#[tokio::test]
async fn private_profile_pins_real_startup_settings_reload_and_recovery() {
    let dir = common::test_tempdir_in("/tmp", "itd-private-");
    let data = dir.path();
    std::fs::write(data.join("config.toml"), CONFLICTING).unwrap();
    let mut daemon = GuardedChild::spawn(&mut command(data)).unwrap();
    ready(data, &mut daemon).await;
    assert_private(data).await;
    for (path, value) in pins() {
        let replacement = value
            .as_bool()
            .map_or_else(|| json!("0.0.0.0"), |v| json!(!v));
        for (method, params) in [
            (
                "settings.update",
                json!({"changes":[{"path":path,"value":replacement}]}),
            ),
            ("settings.reset", json!({"path":path})),
        ] {
            let response = rpc(data, method, params).await;
            eprintln!("private refused {method} {path}: {response}");
            assert_eq!(response["error"]["code"], -32602, "{response}");
            assert!(response.to_string().contains(PROFILE), "{response}");
        }
    }
    // A private, explicit token stays useful for direct GitHub APIs, but
    // cannot be handed to a child credential helper.
    let changed = rpc(
        data,
        "settings.update",
        json!({"changes":[
            {"path":"sourceControl.github.token","value":"fixture-private-token"},
            {"path":"git.autoCommit","value":false}
        ]}),
    )
    .await;
    assert!(changed.get("error").is_none(), "{changed}");
    let current = rpc(data, "settings.get", json!({"path":"git.autoCommit"})).await;
    eprintln!(
        "private preference immediate readback: updateRevision={} get={current}",
        changed["result"]["revision"]
    );
    assert_eq!(current["result"]["value"], false);
    assert_private(data).await;
    // A real external file replacement must apply the unrelated preference,
    // proving the watcher completed; conflicting safety values stay pinned.
    std::fs::write(
        data.join("external.toml"),
        format!("{CONFLICTING}\n[git]\nautoCommit = true\n"),
    )
    .unwrap();
    std::fs::rename(data.join("external.toml"), data.join("config.toml")).unwrap();
    timeout(common::rpc_read_timeout(), async {
        let mut tick = interval(Duration::from_millis(20));
        loop {
            if rpc(data, "settings.get", json!({"path":"git.autoCommit"})).await["result"]["value"]
                == true
            {
                break;
            }
            tick.tick().await;
        }
    })
    .await
    .expect("actual config live reload");
    assert_private(data).await;
    stop(&mut daemon);
    // Recovery is a new genuine process with the same immutable launch policy.
    let mut recovered = GuardedChild::spawn(&mut command(data)).unwrap();
    ready(data, &mut recovered).await;
    assert_private(data).await;
    assert_eq!(
        rpc(data, "settings.get", json!({"path":"git.autoCommit"})).await["result"]["value"],
        true
    );
    stop(&mut recovered);
}

#[test]
fn private_profile_refuses_insecure_before_store_or_socket_creation() {
    for flag in [true, false] {
        let dir = common::test_tempdir_in("/tmp", "itd-private-refuse-");
        let data = dir.path();
        let mut launch = command(data);
        if flag {
            launch.arg("--insecure");
        } else {
            launch.env("INTENTD_INSECURE", "1");
        }
        let mut daemon = GuardedChild::spawn(&mut launch).unwrap();
        let exit = daemon
            .wait_with_timeout(Duration::from_secs(10))
            .unwrap()
            .expect("startup refusal");
        eprintln!("private insecure flag={flag} exit={exit}");
        assert!(!exit.success());
        assert!(!data.join("intentd.db").exists());
        assert!(!data.join("intentd.sock").exists());
        assert!(!data.join("ws-cert.pem").exists());
        let log = std::fs::read_to_string(data.join("daemon.log")).unwrap();
        assert!(
            log.contains("INTENTD_PRIVATE_TEST_PROFILE forbids"),
            "{log}"
        );
    }
}

// The distributable daemon must not interpret test transport declarations as
// authority, whether the private process policy is present or absent.
#[cfg(not(feature = "repository-test-fixtures"))]
#[tokio::test]
async fn ordinary_and_private_profiles_cannot_enable_fixture_transport_authority() {
    for private in [false, true] {
        let dir = common::test_tempdir_in("/tmp", "itd-no-fixture-authority-");
        let data = dir.path();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        std::fs::write(
            data.join("config.toml"),
            format!(
                "[server.wsApi]\nenabled = false\n[updates]\ncheckOnIdle = false\n\
                 [sourceControl.github]\ntokenSource = \"explicit\"\n\
                 [sourceControl.gitlab]\nhost = \"gitlab.fixture.test\"\napiBaseUrl = {endpoint:?}\n"
            ),
        )
        .unwrap();
        let mut launch = command(data);
        if !private {
            launch.env_remove(PROFILE);
        }
        launch.env("INTENTD_DISABLE_GH_CREDENTIALS", "1").env(
            "INTENTD_REPOSITORY_TEST_TRANSPORTS",
            json!([["https://gitlab.fixture.test", endpoint]]).to_string(),
        );
        let mut daemon = GuardedChild::spawn(&mut launch).unwrap();
        ready(data, &mut daemon).await;
        let result = rpc(
            data,
            "sourceControl.connect",
            json!({"provider":"gitlab","host":"gitlab.fixture.test",
                "method":"pat","token":"fixture-only-not-sent"}),
        )
        .await;
        eprintln!("normal artifact private={private}, unapproved transport: {result}");
        assert_eq!(result["error"]["code"], -32603, "{result}");
        let status = rpc(
            data,
            "sourceControl.authStatus",
            json!({"provider":"gitlab"}),
        )
        .await;
        assert_eq!(status["result"]["isConfigured"], false, "{status}");
        assert!(
            intent_core::FileSecretStore::with_path(data.join("secrets.json"))
                .load("sourceControl.gitlab.token")
                .unwrap()
                .is_none()
        );
        stop(&mut daemon);
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "no provider connection may be admitted by the declaration"
        );
    }
}

#[test]
fn private_profile_command_reapplies_paths_after_clearing_environment() {
    use std::ffi::OsStr;
    let dir = common::test_tempdir("private-profile-contract-");
    let cmd = command(dir.path());
    let environment: std::collections::HashMap<_, _> = cmd.get_envs().collect();
    // env_clear means these absent entries are removed, not inherited.
    for key in ["GITHUB_TOKEN", "GH_TOKEN"] {
        assert!(environment.get(OsStr::new(key)).is_none_or(Option::is_none));
    }
    for (key, path) in [
        ("GH_CONFIG_DIR", dir.path().join("gh-config")),
        ("INTENTD_SECRETS_FILE", dir.path().join("secrets.json")),
    ] {
        assert_eq!(
            environment.get(OsStr::new(key)),
            Some(&Some(path.as_os_str())),
            "{key}"
        );
    }
}
