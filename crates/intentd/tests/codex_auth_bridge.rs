//! Exercise the shipped helper executable, owned launcher, and private stdio.
#![cfg(unix)]
mod common;
use base64::Engine;
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

struct Client {
    _owner: intent_services::codex_auth::OwnerLease,
    // raw-child: allow — Tokio kill_on_drop child, with process-group cleanup in Drop.
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}
impl Drop for Client {
    fn drop(&mut self) {
        if let Some(id) = self.child.id() {
            if let Ok(pid) = i32::try_from(id) {
                // The child leads its own group; tear down its helpers on panic.
                unsafe {
                    libc::kill(-pid, libc::SIGKILL);
                }
            }
        }
    }
}
impl Client {
    async fn start(profile: &Path, native: &Path, runtime: &Path) -> Self {
        let wrapper = intent_services::codex_auth::install_wrapper(
            profile,
            runtime,
            native,
            native,
            Path::new(env!("CARGO_BIN_EXE_intentd")),
        )
        .unwrap();
        let mut context = Command::new(runtime);
        context
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("INTENT_CODEX_NATIVE_XDG_CONFIG_HOME", native.join("config"))
            .env(
                "INTENT_CODEX_NATIVE_DBUS_SESSION_BUS_ADDRESS",
                "synthetic-native-bus",
            );
        let owner = intent_services::codex_auth::start_owner(
            runtime,
            native,
            native,
            Path::new(env!("CARGO_BIN_EXE_intentd")),
            &context,
        )
        .unwrap();
        let mut child = Command::new(wrapper)
            .arg("app-server")
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("INTENT_CODEX_AUTH_SOCKET", owner.socket())
            .env("HOME", native)
            .env("XDG_CONFIG_HOME", profile)
            .env("INTENT_CODEX_NATIVE_XDG_CONFIG_HOME", native.join("config"))
            .env(
                "INTENT_CODEX_NATIVE_DBUS_SESSION_BUS_ADDRESS",
                "synthetic-native-bus",
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .process_group(0)
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut client = Self {
            _owner: owner,
            child,
            input,
            output,
        };
        assert!(client
            .call(1, "initialize", json!({"capabilities":{}}))
            .await
            .get("result")
            .is_some());
        client
    }
    async fn call(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.input
            .write_all(format!("{}\n", json!({"id":id,"method":method,"params":params})).as_bytes())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                let mut line = String::new();
                assert_ne!(
                    self.output.read_line(&mut line).await.unwrap(),
                    0,
                    "bridge ended before response"
                );
                let value: Value = serde_json::from_str(&line).unwrap();
                if value["id"] == id {
                    return value;
                }
            }
        })
        .await
        .expect("bridge response deadline")
    }
}
fn token(generation: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = json!({"exp":now+3600,"generation":generation,"https://api.openai.com/auth":{"chatgpt_account_id":"account","chatgpt_user_id":"user"}});
    format!(
        "header.{}.signature",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string())
    )
}
#[tokio::test]
async fn refresh_callback_deadline_abandons_reply_but_preserves_native_rotation() {
    let root = common::test_tempdir("codex-auth-callback-lifetime");
    let native = root.path().join("native");
    let profile = root.path().join("worker");
    std::fs::create_dir(&native).unwrap();
    std::fs::create_dir(&profile).unwrap();
    std::fs::write(profile.join("worker"), "").unwrap();
    let runtime = root.path().join("codex");
    std::fs::write(
        &runtime,
        include_str!("../../intent-services/src/codex_auth/fixture.py"),
    )
    .unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    let a = token(1);
    let b = token(2);
    std::fs::write(
        native.join("auth.json"),
        json!({
            "authMethod":"chatgpt", "authToken":a, "refresh_token":"single-use", "next":b
        })
        .to_string(),
    )
    .unwrap();
    let mut client = Client::start(&profile, &native, &runtime).await;
    std::fs::write(native.join("delay-refresh"), "8").unwrap();
    std::fs::write(profile.join("request-refresh"), "").unwrap();
    let started = std::time::Instant::now();
    let response = client.call(2, "turn/start", json!({})).await;
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("busy"));
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(native.join("refresh-consumed").exists());
    assert_eq!(
        std::fs::read_to_string(profile.join("refresh-outcome")).unwrap(),
        "error"
    );
    // Both the bridge process and daemon lease disappear while native I/O is
    // still outstanding. Only the independent owner may complete persistence.
    drop(client);
    // timing-guard: observe the native write, never infer success from a sleep.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state: Value =
                serde_json::from_slice(&std::fs::read(native.join("auth.json")).unwrap()).unwrap();
            if state["refresh_token"] == "single-use-next" {
                break;
            }
            // timing-guard: poll the persisted replacement after issuer consumption.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    std::fs::remove_file(profile.join("request-refresh")).unwrap();
    let mut retry = Client::start(&profile, &native, &runtime).await;
    assert!(retry
        .call(3, "thread/resume", json!({"threadId":"existing"}))
        .await
        .get("result")
        .is_some());
    assert_eq!(
        std::fs::read_to_string(profile.join("injected")).unwrap(),
        b
    );
    assert_eq!(
        std::fs::read_to_string(native.join("consumed"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert!(!native.join("revoked").exists());
    assert!(!profile.join("revoked").exists());
    assert!(!profile.join("auth.json").exists());
}

#[tokio::test]
async fn startup_error_cleanup_preserves_native_login_and_retry_uses_relogin() {
    let root = common::test_tempdir("codex-auth-binary");
    let native = root.path().join("native");
    std::fs::create_dir(&native).unwrap();
    let runtime = root.path().join("codex");
    std::fs::write(
        &runtime,
        include_str!("../../intent-services/src/codex_auth/fixture.py"),
    )
    .unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(native.join("expected-env"), json!({"XDG_CONFIG_HOME":native.join("config"),"DBUS_SESSION_BUS_ADDRESS":"synthetic-native-bus","HOME":native}).to_string()).unwrap();
    let a = token(1);
    let b = token(2);
    for (i, error) in [
        "failed to load workspace requirements",
        "cloud requirements failed",
        "please log out and try again",
    ]
    .iter()
    .enumerate()
    {
        let profile = root.path().join(format!("worker-{i}"));
        std::fs::create_dir(&profile).unwrap();
        std::fs::write(profile.join("worker"), "").unwrap();
        std::fs::write(profile.join("session"), "existing-conversation").unwrap();
        std::fs::write(
            profile.join("auth.json"),
            json!({"tokens":{"access_token":a,"refresh_token":"obsolete-profile-secret"}})
                .to_string(),
        )
        .unwrap();
        let login = |access: &str| {
            std::fs::write(
                native.join("auth.json"),
                json!({"authMethod":"chatgpt","authToken":access,"refresh_token":"native-only"})
                    .to_string(),
            )
            .unwrap();
        };
        login(&a);
        std::fs::write(profile.join("setup-error"), error).unwrap();
        let mut client = Client::start(&profile, &native, &runtime).await;
        assert!(!profile.join("auth.json").exists());
        assert!(!profile.join("auth.json.intent-legacy").exists());
        assert!(profile.join(".intent-native-account").exists());
        let failed = client
            .call(
                2,
                "thread/resume",
                json!({"threadId":"existing-conversation"}),
            )
            .await;
        assert_eq!(failed["error"]["message"], *error);
        assert_eq!(
            client.call(3, "account/logout", json!({})).await["result"],
            json!({})
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(3), client.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        assert!(!native.join("revoked").exists());
        assert!(!profile.join("revoked").exists());
        assert!(!profile.join("auth.json").exists());
        login(&b);
        std::fs::remove_file(profile.join("setup-error")).unwrap();
        let mut retry = Client::start(&profile, &native, &runtime).await;
        assert!(retry
            .call(
                2,
                "thread/resume",
                json!({"threadId":"existing-conversation"})
            )
            .await
            .get("result")
            .is_some());
        assert_eq!(
            std::fs::read_to_string(profile.join("injected")).unwrap(),
            b
        );
        assert_eq!(
            std::fs::read_to_string(profile.join("session")).unwrap(),
            "existing-conversation"
        );
        retry.call(3, "account/logout", json!({})).await;
        retry.child.wait().await.unwrap();
    }
    let methods = std::fs::read_to_string(native.join("requests")).unwrap();
    assert!(!methods.contains("logout"));
    assert!(!methods.contains("thread/"));
}

#[tokio::test]
async fn managed_storage_rejection_happens_before_worker_bootstrap() {
    let root = common::test_tempdir("codex-auth-policy");
    let native = root.path().join("native");
    let profile = root.path().join("worker");
    std::fs::create_dir(&native).unwrap();
    std::fs::create_dir(&profile).unwrap();
    std::fs::write(profile.join("worker"), "").unwrap();
    let runtime = root.path().join("codex");
    std::fs::write(
        &runtime,
        include_str!("../../intent-services/src/codex_auth/fixture.py"),
    )
    .unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        native.join("auth.json"),
        json!({"authMethod":"chatgpt","authToken":token(1)}).to_string(),
    )
    .unwrap();
    for mode in ["file", "keyring", "auto"] {
        std::fs::write(native.join("managed-store"), mode).unwrap();
        let wrapper = intent_services::codex_auth::install_wrapper(
            &profile,
            &runtime,
            &native,
            &native,
            Path::new(env!("CARGO_BIN_EXE_intentd")),
        )
        .unwrap();
        let owner = intent_services::codex_auth::start_owner(
            &runtime,
            &native,
            &native,
            Path::new(env!("CARGO_BIN_EXE_intentd")),
            Command::new(&runtime)
                .env_clear()
                .env("PATH", "/usr/bin:/bin"),
        )
        .unwrap();
        let output = tokio::time::timeout(
            Duration::from_secs(10),
            Command::new(wrapper)
                .arg("app-server")
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env("INTENT_CODEX_AUTH_SOCKET", owner.socket())
                .stdin(Stdio::null())
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("Managed Codex credential storage")
        );
        assert!(
            !profile.join("requests").exists(),
            "worker must not start under an overriding store policy"
        );
        assert!(!profile.join("injected").exists());
    }
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires INTENTD_PROFILE_CODEX_BIN; isolated installed-runtime and loopback issuer contract"]
fn installed_codex_native_auth_contract_and_bridge() {
    let runtime =
        std::env::var_os("INTENTD_PROFILE_CODEX_BIN").expect("set installed Codex executable");
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/codex-auth-contract.py"
        ))
        .arg(runtime)
        .arg(env!("CARGO_BIN_EXE_intentd"))
        .env_remove("NODE_OPTIONS")
        .output()
        .unwrap();
    // Harness outputs only named assertions; subprocess credential frames are private.
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires installed Codex and ACP paths; synthetic busy-session recovery contract"]
fn installed_codex_acp_busy_recovery() {
    let runtime = std::env::var_os("INTENTD_PROFILE_CODEX_BIN").expect("set Codex path");
    let adapter = std::env::var_os("INTENTD_PROFILE_CODEX_ADAPTER_JS").expect("set ACP path");
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/codex-auth-contract.py"
        ))
        .arg(runtime)
        .arg(env!("CARGO_BIN_EXE_intentd"))
        .arg(adapter)
        .env_remove("NODE_OPTIONS")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
