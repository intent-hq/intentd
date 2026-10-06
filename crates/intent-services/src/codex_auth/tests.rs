use super::*;
use std::os::unix::fs::PermissionsExt;

struct Fixture {
    _root: tempfile::TempDir,
    native: PathBuf,
    profile: PathBuf,
    runtime: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let root = crate::test_support::test_tempdir("codex-auth-bridge");
        let native = root.path().join("native");
        let profile = root.path().join("worker");
        std::fs::create_dir_all(&native).unwrap();
        std::fs::create_dir_all(&profile).unwrap();
        std::fs::write(profile.join("worker"), "").unwrap();
        let runtime = root.path().join("codex");
        std::fs::write(&runtime, include_str!("fixture.py")).unwrap();
        std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            _root: root,
            native,
            profile,
            runtime,
        }
    }
    fn authority(&self) -> Authority {
        Authority {
            runtime: self.runtime.clone(),
            home: self.native.clone(),
            user_home: self.native.clone(),
        }
    }
    fn bridge(&self) -> Bridge {
        Bridge {
            authority: self.authority(),
            profile: self.profile.clone(),
            credentials: None,
        }
    }
    fn login(&self, token: &str, next: Option<&str>) {
        let mut state =
            json!({"authMethod":"chatgpt","authToken":token,"refresh_token":"synthetic-R0"});
        if let Some(next) = next {
            state["next"] = json!(next);
        }
        std::fs::write(self.native.join("auth.json"), state.to_string()).unwrap();
    }
    fn server(&self) -> Server {
        Server::spawn(
            Command::new(&self.runtime)
                .args([
                    "app-server",
                    "--strict-config",
                    "-c",
                    "cli_auth_credentials_store=\"ephemeral\"",
                ])
                .env("CODEX_HOME", &self.profile)
                .stderr(Stdio::null()),
        )
        .unwrap()
    }
}
fn token(account: &str, user: &str, generation: u64, expired: bool) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = json!({"exp":if expired {1} else {now+3600},"generation":generation,"https://api.openai.com/auth":{"chatgpt_account_id":account,"chatgpt_user_id":user}});
    format!(
        "header.{}.signature",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string())
    )
}
fn credentials(token: &str) -> Credentials {
    Credentials::from_status(
        &json!({"requiresOpenaiAuth":true,"authMethod":"chatgpt","authToken":token}),
    )
    .unwrap()
    .unwrap()
}
#[tokio::test]
async fn native_relogin_and_logout_are_observed_by_fresh_helpers() {
    let f = Fixture::new();
    let a = token("account", "user", 1, false);
    let b = token("account", "user", 2, false);
    f.login(&a, None);
    assert_eq!(f.authority().read(None).await.unwrap().unwrap().token, a);
    f.login(&b, None);
    assert_eq!(f.authority().read(None).await.unwrap().unwrap().token, b);
    std::fs::remove_file(f.native.join("auth.json")).unwrap();
    assert!(matches!(f.authority().read(None).await, Err(AUTH_ERROR)));
}
#[tokio::test]
async fn refresh_owner_serializes_rotation_and_persists_it_past_probe_cleanup() {
    let f = Fixture::new();
    let a = token("account", "user", 1, false);
    let b = token("account", "user", 2, false);
    f.login(&a, Some(&b));
    let first = f.authority();
    let second = f.authority();
    let (one, two) = tokio::join!(first.read(Some(&a)), second.read(Some(&a)));
    assert_eq!(one.unwrap().unwrap().token, b);
    assert_eq!(two.unwrap().unwrap().token, b);
    assert_eq!(
        std::fs::read_to_string(f.native.join("consumed"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    std::fs::remove_dir_all(&f.profile).unwrap();
    assert_eq!(f.authority().read(None).await.unwrap().unwrap().token, b);
    let persisted: Value =
        serde_json::from_slice(&std::fs::read(f.native.join("auth.json")).unwrap()).unwrap();
    assert_eq!(persisted["refresh_token"], "synthetic-R0-next");
}
#[tokio::test]
async fn unchanged_rejected_and_expired_tokens_never_become_refresh_success() {
    let f = Fixture::new();
    let a = token("account", "user", 1, false);
    f.login(&a, None); // models upstream swallowing a transient or permanent refresh error
    assert!(matches!(
        f.authority().read(Some(&a)).await,
        Err(AUTH_ERROR)
    ));
    f.login(&token("account", "user", 2, true), None);
    assert!(matches!(f.authority().read(None).await, Err(AUTH_ERROR)));
}
#[tokio::test]
async fn expired_access_token_refreshes_through_native_authority_before_worker_login() {
    let f = Fixture::new();
    let expired = token("account", "user", 1, true);
    let fresh = token("account", "user", 2, false);
    f.login(&expired, Some(&fresh));
    assert_eq!(
        f.authority().read(None).await.unwrap().unwrap().token,
        fresh
    );
    let persisted: Value =
        serde_json::from_slice(&std::fs::read(f.native.join("auth.json")).unwrap()).unwrap();
    assert_eq!(persisted["refresh_token"], "synthetic-R0-next");
    assert!(!f.profile.join("auth.json").exists());
}
#[tokio::test]
async fn invalid_native_config_does_not_export_default_file_credentials() {
    let f = Fixture::new();
    f.login(&token("account", "user", 1, false), None);
    std::fs::write(f.native.join("invalid-config"), "keyring config invalid").unwrap();
    assert!(f.authority().read(None).await.is_err());
    assert!(!f.native.join("requests").exists());
    assert!(!f.profile.join("injected").exists());
}
#[tokio::test]
async fn local_logout_terminates_worker_without_native_or_worker_logout() {
    let f = Fixture::new();
    f.login(&token("account", "user", 1, false), None);
    let mut server = f.server();
    let input =
        b"{\"id\":1,\"method\":\"account/read\"}\n{\"id\":2,\"method\":\"account/logout\"}\n";
    let mut output = Vec::new();
    f.bridge()
        .proxy(&mut server, &mut Frames::new(&input[..]), &mut output)
        .await
        .unwrap();
    assert!(server.child.try_wait().unwrap().is_some());
    assert!(!f.native.join("revoked").exists());
    assert!(!f.profile.join("revoked").exists());
    assert!(!f.profile.join("auth.json").exists());
    assert!(String::from_utf8(output)
        .unwrap()
        .contains("\"id\":2,\"result\":{}"));
}
#[test]
fn expired_legacy_identity_migrates_but_account_or_user_changes_do_not() {
    for (account, user, allowed) in [
        ("account", "user", true),
        ("other", "user", false),
        ("account", "other", false),
    ] {
        let f = Fixture::new();
        std::fs::write(f.profile.join("auth.json"), json!({"tokens":{"access_token":token("account", "user", 0, true),"refresh_token":"stale-secret"}}).to_string()).unwrap();
        std::fs::write(f.profile.join("session"), "preserved").unwrap();
        install_wrapper(
            &f.profile,
            &f.runtime,
            &f.native,
            &f.native,
            Path::new("/intentd"),
        )
        .unwrap();
        assert!(!f.profile.join("auth.json").exists());
        assert!(!f.profile.join("auth.json.intent-legacy").exists());
        assert!(f.profile.join(".intent-native-account").exists());
        assert_eq!(
            f.bridge()
                .accept_identity(&credentials(&token(account, user, 1, false)))
                .is_ok(),
            allowed
        );
        assert_eq!(
            std::fs::read_to_string(f.profile.join("session")).unwrap(),
            "preserved"
        );
        if allowed {
            assert_eq!(
                f.bridge()
                    .accept_identity(&credentials(&token("other", "user", 2, false))),
                Err(ACCOUNT_ERROR)
            );
        }
    }
}
#[test]
fn migration_failure_preserves_credentials_and_retry_removes_only_owned_legacy_files() {
    let f = Fixture::new();
    let legacy = json!({"tokens":{"access_token":token("account", "user", 0, true),"refresh_token":"old-secret"}}).to_string();
    let marker = f.profile.join(".intent-native-account");
    let install = || {
        install_wrapper(
            &f.profile,
            &f.runtime,
            &f.native,
            &f.native,
            Path::new("/intentd"),
        )
    };
    std::fs::write(f.native.join("auth.json"), "native-untouched").unwrap();
    std::fs::write(f.profile.join("auth.json"), &legacy).unwrap();
    std::fs::write(f.profile.join("auth.json.intent-legacy"), &legacy).unwrap();
    std::fs::write(f.profile.join("session"), "keep-history").unwrap();
    // A non-file marker deterministically prevents durable binding, even as root.
    std::fs::create_dir(&marker).unwrap();
    assert!(install().is_err());
    for name in ["auth.json", "auth.json.intent-legacy"] {
        assert_eq!(
            std::fs::read_to_string(f.profile.join(name)).unwrap(),
            legacy
        );
    }
    std::fs::remove_dir(&marker).unwrap();
    install().unwrap();
    let expected = credentials(&token("account", "user", 1, false)).identity;
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), expected);
    for name in ["auth.json", "auth.json.intent-legacy"] {
        assert!(!f.profile.join(name).exists());
    }
    // Simulate interruption after binding but before removing the old source.
    std::fs::write(f.profile.join("auth.json.intent-legacy"), &legacy).unwrap();
    install().unwrap();
    install().unwrap();
    assert!(!f.profile.join("auth.json.intent-legacy").exists());
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), expected);
    assert_eq!(
        std::fs::read_to_string(f.profile.join("session")).unwrap(),
        "keep-history"
    );
    assert_eq!(
        std::fs::read_to_string(f.native.join("auth.json")).unwrap(),
        "native-untouched"
    );
}

#[test]
fn migration_rejects_conflicting_bindings_and_never_migrates_the_native_home() {
    let f = Fixture::new();
    for (name, account) in [("auth.json", "one"), ("auth.json.intent-legacy", "two")] {
        std::fs::write(f.profile.join(name), json!({"tokens":{"access_token":token(account, "user", 0, true),"refresh_token":"secret"}}).to_string()).unwrap();
    }
    assert!(install_wrapper(
        &f.profile,
        &f.runtime,
        &f.native,
        &f.native,
        Path::new("/intentd")
    )
    .is_err());
    assert!(!f.profile.join(".intent-native-account").exists());
    assert!(f.profile.join("auth.json").exists());
    assert!(f.profile.join("auth.json.intent-legacy").exists());
    std::fs::write(f.native.join("auth.json"), "native-only").unwrap();
    assert!(install_wrapper(
        &f.native,
        &f.runtime,
        &f.native,
        &f.native,
        Path::new("/intentd")
    )
    .is_err());
    assert_eq!(
        std::fs::read_to_string(f.native.join("auth.json")).unwrap(),
        "native-only"
    );
}

#[test]
fn migration_does_not_follow_legacy_credential_symlinks() {
    let f = Fixture::new();
    let unrelated = f.native.join("unrelated.json");
    let bytes = json!({"tokens":{"access_token":token("account", "user", 0, true),"refresh_token":"unrelated-secret"}}).to_string();
    std::fs::write(&unrelated, &bytes).unwrap();
    std::os::unix::fs::symlink(&unrelated, f.profile.join("auth.json.intent-legacy")).unwrap();
    assert!(install_wrapper(
        &f.profile,
        &f.runtime,
        &f.native,
        &f.native,
        Path::new("/intentd")
    )
    .is_err());
    assert!(!f.profile.join(".intent-native-account").exists());
    assert!(
        std::fs::symlink_metadata(f.profile.join("auth.json.intent-legacy"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read_to_string(unrelated).unwrap(), bytes);
}

#[tokio::test]
async fn worker_refresh_receives_only_access_token_and_persists_native_rotation() {
    let f = Fixture::new();
    let a = token("account", "user", 1, false);
    let b = token("account", "user", 2, false);
    f.login(&a, Some(&b));
    let mut server = f.server();
    let mut bridge = f.bridge();
    bridge
        .synchronize(&mut server, &mut Vec::new())
        .await
        .unwrap();
    bridge
        .refresh(
            &json!({"id":9,"params":{"previousAccountId":"account"}}),
            &mut server,
        )
        .await
        .unwrap();
    assert_eq!(bridge.credentials.unwrap().token, b);
    assert!(!f.profile.join("auth.json").exists());
    server.stop().await;
    assert_eq!(f.authority().read(None).await.unwrap().unwrap().token, b);
}
#[tokio::test]
async fn account_switch_blocks_all_adapter_requests_and_unknown_future_methods() {
    // Complete ACP2.1.1 request inventory except explicit startup/auth/cleanup paths.
    for method in [
        "account/rateLimits/read",
        "account/read",
        "config/read",
        "mcpServer/oauth/login",
        "mcpServerStatus/list",
        "model/list",
        "review/start",
        "skills/extraRoots/set",
        "skills/list",
        "thread/archive",
        "thread/backgroundTerminals/list",
        "thread/compact/start",
        "thread/fork",
        "thread/goal/get",
        "thread/goal/set",
        "thread/items/list",
        "thread/list",
        "thread/loaded/list",
        "thread/name/set",
        "thread/read",
        "thread/resume",
        "thread/settings/update",
        "thread/start",
        "thread/turns/list",
        "turn/start",
        "turn/steer",
        "future/authenticated/request",
    ] {
        let f = Fixture::new();
        f.login(&token("account", "user", 1, false), None);
        let mut server = f.server();
        let mut bridge = f.bridge();
        bridge
            .synchronize(&mut server, &mut Vec::new())
            .await
            .unwrap();
        f.login(&token("other", "user", 2, false), None);
        let input = format!("{}\n", json!({"id":2,"method":method}));
        let mut output = Vec::new();
        bridge
            .proxy(&mut server, &mut Frames::new(input.as_bytes()), &mut output)
            .await
            .unwrap();
        server.stop().await;
        assert!(
            String::from_utf8(output).unwrap().contains(ACCOUNT_ERROR),
            "{method}"
        );
        assert!(!std::fs::read_to_string(f.profile.join("requests"))
            .unwrap()
            .contains(method));
    }
}
#[tokio::test]
async fn startup_cleanup_responses_and_notifications_remain_usable_after_logout() {
    let f = Fixture::new();
    f.login(&token("account", "user", 1, false), None);
    let mut server = f.server();
    let mut bridge = f.bridge();
    bridge
        .synchronize(&mut server, &mut Vec::new())
        .await
        .unwrap();
    std::fs::remove_file(f.native.join("auth.json")).unwrap();
    let native_requests = std::fs::read(f.native.join("requests")).unwrap();
    let (mut client, input) = tokio::io::duplex(4096);
    let (mut output, client_output) = tokio::io::duplex(4096);
    let mut input = Frames::new(BufReader::new(input));
    let exchange = async {
        let mut responses = Frames::new(BufReader::new(client_output));
        // Notifications and responses finish protocol work without starting a
        // new authenticated operation. A subsequent reply is a delivery barrier.
        write_frame(&mut client, &json!({"method":"initialized"}))
            .await
            .unwrap();
        write_frame(
            &mut client,
            &json!({"method":"$/cancelRequest","params":{"id":"pending"}}),
        )
        .await
        .unwrap();
        for (id, method) in [
            "initialize",
            "turn/interrupt",
            "thread/unsubscribe",
            "thread/backgroundTerminals/terminate",
            "thread/goal/clear",
        ]
        .into_iter()
        .enumerate()
        {
            write_frame(
                &mut client,
                &json!({"id":id,"method":method,"params":{"capabilities":{}}}),
            )
            .await
            .unwrap();
            let response = responses.next().await.unwrap().unwrap();
            assert_eq!(response, json!({"id":id,"result":{}}));
        }
        // The fixture acknowledges incoming approval/elicitation responses so
        // the test can prove they reached the child without an auth lookup.
        for frame in [
            json!({"id":"approval","result":{"decision":"decline"}}),
            json!({"id":"elicitation","error":{"code":-1,"message":"cancelled"}}),
        ] {
            write_frame(&mut client, &frame).await.unwrap();
            assert_eq!(responses.next().await.unwrap().unwrap()["id"], frame["id"]);
        }
        drop(client);
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        let (proxy, ()) =
            tokio::join!(bridge.proxy(&mut server, &mut input, &mut output), exchange);
        proxy.unwrap();
    })
    .await
    .unwrap();
    server.stop().await;
    assert_eq!(
        std::fs::read(f.native.join("requests")).unwrap(),
        native_requests
    );
    let worker_requests = std::fs::read_to_string(f.profile.join("requests")).unwrap();
    assert!(worker_requests.contains("initialized"));
    assert!(worker_requests.contains("$/cancelRequest"));
}

#[tokio::test]
async fn lock_contention_is_bounded_and_cancellation_releases_authority() {
    let f = Fixture::new();
    f.login(&token("account", "user", 1, false), None);
    let lock = auth_lock(&f.native).await.unwrap();
    let start = tokio::time::Instant::now();
    assert!(f.authority().read(None).await.is_err());
    assert!(start.elapsed() < Duration::from_secs(4));
    drop(lock);
    std::fs::write(f.native.join("delay"), "30").unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), f.authority().read(None))
            .await
            .is_err()
    );
    std::fs::remove_file(f.native.join("delay")).unwrap();
    assert!(f.authority().read(None).await.is_ok());
}

#[test]
fn managed_storage_never_silently_overrides_worker_ephemeral_mode() {
    for mode in ["file", "keyring", "auto"] {
        let requirements = json!({"requirements":{"cliAuthCredentialsStore":mode}});
        assert_eq!(
            worker_policy(&requirements, &json!({"layers":[]})),
            Err(POLICY_ERROR)
        );
        for kind in [
            "mdm",
            "enterpriseManaged",
            "legacyManagedConfigTomlFromFile",
            "legacyManagedConfigTomlFromMdm",
        ] {
            assert_eq!(
                worker_policy(
                    &json!({"requirements":null}),
                    &json!({"layers":[{"name":{"type":kind},"config":{"cli_auth_credentials_store":mode}}]})
                ),
                Err(POLICY_ERROR)
            );
        }
    }
    assert!(worker_policy(&json!({"requirements":null}), &json!({"layers":[{"name":{"type":"user"},"config":{"cli_auth_credentials_store":"keyring"}}]})).is_ok());
    assert_eq!(
        worker_policy(&json!({"requirements":{}}), &json!({"layers":[]})),
        Err(CONTRACT_ERROR)
    );
}

#[tokio::test]
async fn delayed_refresh_callback_fails_within_upstream_deadline() {
    let f = Fixture::new();
    let a = token("account", "user", 1, false);
    f.login(&a, None);
    let mut bridge = f.bridge();
    bridge.credentials = Some(credentials(&a));
    let mut server = f.server();
    std::fs::write(f.native.join("delay"), "30").unwrap();
    let lock = auth_lock(&f.native).await.unwrap();
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(800)).await;
        drop(lock);
    });
    let start = tokio::time::Instant::now();
    bridge
        .refresh(
            &json!({"id":"refresh","params":{"previousAccountId":"account"}}),
            &mut server,
        )
        .await
        .unwrap();
    assert!(start.elapsed() < Duration::from_secs(9));
    tokio::time::timeout(Duration::from_secs(1), server.output.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(f.profile.join("refresh-outcome")).unwrap(),
        "error"
    );
    assert_eq!(bridge.credentials.unwrap().token, a);
    server.stop().await;
    release.await.unwrap();
}

#[test]
fn custom_provider_api_key_and_unsupported_modes_have_explicit_boundaries() {
    assert!(
        Credentials::from_status(&json!({"requiresOpenaiAuth":false}))
            .unwrap()
            .is_none()
    );
    let key = Credentials::from_status(
        &json!({"requiresOpenaiAuth":true,"authMethod":"apikey","authToken":"synthetic-key"}),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        key.login(),
        json!({"type":"apiKey","apiKey":"synthetic-key"})
    );
    assert!(key.refresh().is_err());
    let f = Fixture::new();
    f.bridge().accept_identity(&key).unwrap();
    let rotated = Credentials::from_status(
        &json!({"requiresOpenaiAuth":true,"authMethod":"apikey","authToken":"rotated-key"}),
    )
    .unwrap()
    .unwrap();
    assert_eq!(f.bridge().accept_identity(&rotated), Err(ACCOUNT_ERROR));
    assert!(matches!(
        Credentials::from_status(
            &json!({"requiresOpenaiAuth":true,"authMethod":"agentIdentity","authToken":"never-echo-this"})
        ),
        Err(CONTRACT_ERROR)
    ));
}

#[tokio::test]
async fn cancelled_partial_frame_is_retained_and_malformed_payload_is_redacted() {
    let (mut write, read) = tokio::io::duplex(128);
    let mut frames = Frames::new(BufReader::new(read));
    write.write_all(b"{\"id\":7,").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), frames.next())
            .await
            .is_err()
    );
    write
        .write_all(b"\"result\":{}}\n{never-echo-this}\n")
        .await
        .unwrap();
    assert_eq!(
        frames.next().await.unwrap(),
        Some(json!({"id":7,"result":{}}))
    );
    assert_eq!(frames.next().await, Err(CONTRACT_ERROR));
}
