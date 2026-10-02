use super::*;
use intent_core::{Caller, HostRole, PrincipalId, ServerControl, WorkspaceApi};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

tokio::task_local! {
    static TEST_TCP: bool;
}

#[intent_test_macros::daemon_test]
async fn gitlab_committed_settings_identity_tail_survives_early_close() {
    assert_committed_identity_tail_survives_early_close(false).await;
}

#[intent_test_macros::daemon_test]
async fn gitlab_config_watcher_identity_tail_survives_early_close() {
    assert_committed_identity_tail_survives_early_close(true).await;
}

async fn assert_committed_identity_tail_survives_early_close(external: bool) {
    use std::future::Future;
    {
        let (dir, services, bus) = harness().await;
        let raw = intent_core::FileSecretStore::with_path(dir.path().join("derived-identity.json"));
        raw.store(
            intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
            "saved-pat",
        )
        .unwrap();
        let services = services
            .with_gitlab_secret_store(raw.clone())
            .with_secret_store(Arc::new(raw));
        let (host, server) = crate::source_control_auth_ops::startup_tests::pat_host().await;
        let registry = services.settings_registry.as_ref().unwrap();
        registry
            .apply(&[
                ("sourceControl.gitlab.host".into(), json!(host.host())),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(host.base_url()),
                ),
            ])
            .unwrap();
        assert!(services
            .store
            .get_primary_principal()
            .await
            .unwrap()
            .login
            .is_none());
        if external {
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let mut watcher = crate::ConfigWatcher::start(
                &crate::SharedWatchHub::new(),
                registry.clone(),
                services.settings_revision_gate(),
                {
                    let callback_services = services.clone();
                    let entered = entered.clone();
                    let release = release.clone();
                    move |notice| {
                        let callback_services = callback_services.clone();
                        let entered = entered.clone();
                        let release = release.clone();
                        async move {
                            entered.notify_one();
                            release.notified().await;
                            callback_services
                                .apply_external_settings_change(&notice)
                                .await;
                        }
                    }
                },
            )
            .unwrap();
            assert!(watcher.ready().await);
            let mut config: toml_edit::DocumentMut =
                std::fs::read_to_string(registry.config_path())
                    .unwrap()
                    .parse()
                    .unwrap();
            config["identity"]["provider"] = toml_edit::value("gitlab");
            std::fs::write(registry.config_path(), config.to_string()).unwrap();
            timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            // The actual watcher has committed the registry and entered its
            // callback under the revision gate before root admission closes.
            services.begin_settings_shutdown();
            assert!(
                matches!(services.source_control_get_user("gitlab".into(),None).await,
                Err(Error::Internal(message)) if message.contains("shutting down"))
            );
            let stopped = watcher.shutdown();
            tokio::pin!(stopped);
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    stopped.as_mut().poll(cx).is_pending()
                ))
                .await
            );
            release.notify_one();
            timeout(Duration::from_secs(5), stopped).await.unwrap();
        } else {
            let guard = services.gitlab_credential_gate.clone().lock_owned().await;
            let mut request = Box::pin(services.settings_update(json!([
                {"path":"sourceControl.gitlab.token","value":"new-pat"},
                {"path":"identity.provider","value":"gitlab"}
            ])));
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    request.as_mut().poll(cx).is_pending()
                ))
                .await
            );
            services.begin_settings_shutdown();
            drop(request);
            drop(guard);
        }
        timeout(Duration::from_secs(5), services.shutdown_store_writers())
            .await
            .unwrap();
        server.abort();
        let _ = server.await;
        bus.shutdown().await.unwrap();
        services.store.close().await;
        let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
        let primary = reopened.get_primary_principal().await.unwrap();
        let events = reopened
            .query_events(&intent_store::EventQuery::default())
            .await
            .unwrap();
        reopened.close().await;
        assert_eq!(
            primary.login.as_deref(),
            Some("test-user"),
            "committed identity tail lost after early closure (external={external})"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "principal:identity-changed")
                .count(),
            1
        );
    }
}

#[intent_test_macros::daemon_test]
async fn gitlab_identity_connected_tail_cannot_overtake_newer_setting() {
    assert_identity_tail_preserves_newer_setting(false).await;
}

#[intent_test_macros::daemon_test]
async fn gitlab_identity_rejected_tail_cannot_unlink_newer_setting() {
    assert_identity_tail_preserves_newer_setting(true).await;
}

async fn assert_identity_tail_preserves_newer_setting(rejected: bool) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let (dir, services, bus) = harness().await;
    let raw = intent_core::FileSecretStore::with_path(dir.path().join("identity-generation.json"));
    raw.store(
        intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
        "old-pat",
    )
    .unwrap();
    let services = services
        .with_gitlab_secret_store(raw.clone())
        .with_secret_store(Arc::new(raw))
        .with_source_control(Arc::new(crate::tests::pr::StubForge::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    services
        .settings_registry
        .as_ref()
        .unwrap()
        .apply(&[
            (
                "sourceControl.gitlab.host".into(),
                json!("gitlab.identity.test"),
            ),
            ("sourceControl.gitlab.apiBaseUrl".into(), json!(origin)),
        ])
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let server = tokio::spawn({
        let entered = entered.clone();
        let release = release.clone();
        async move {
            for index in 0..if rejected { 1 } else { 2 } {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut request = String::new();
                reader.read_line(&mut request).await.unwrap();
                assert!(request.starts_with(if index == 0 {
                    "GET /api/v4/user "
                } else {
                    "GET /api/v4/personal_access_tokens/self "
                }));
                loop {
                    let mut line = String::new();
                    assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                }
                if index == 0 {
                    entered.notify_one();
                    release.notified().await;
                }
                let status = if rejected {
                    "401 Unauthorized"
                } else {
                    "200 OK"
                };
                let body = if rejected {
                    json!({"message":"old identity rejected"})
                } else if index == 0 {
                    json!({"id":42,"username":"old-gitlab-user"})
                } else {
                    json!({"scopes":["api"]})
                };
                let body = body.to_string();
                reader.get_mut().write_all(format!("HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        }
    });
    services
        .settings_update(json!([{"path":"identity.provider","value":"gitlab"}]))
        .await
        .unwrap();
    timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    services
        .settings_update(json!([{"path":"identity.provider","value":"github"}]))
        .await
        .unwrap();
    let newer = timeout(Duration::from_secs(5), async {
        loop {
            let primary = services.store.get_primary_principal().await.unwrap();
            if primary
                .identity_key()
                .is_some_and(|identity| identity.provider == "github")
            {
                break primary;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The newer identity has actually persisted while the older real network
    // response is still held. Drain must retain that old derived owner too.
    let (pending, pending_rx) = tokio::sync::oneshot::channel();
    *services.secrets.writer_drain_pending.lock().unwrap() = Some(pending);
    let worker = services.clone();
    let drain = intent_core::spawn_daemon(async move { worker.shutdown_store_writers().await });
    assert_eq!(
        timeout(Duration::from_secs(5), pending_rx)
            .await
            .unwrap()
            .unwrap(),
        "store-tasks"
    );
    assert!(!drain.is_finished());
    release.notify_one();
    timeout(Duration::from_secs(5), drain)
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    bus.shutdown().await.unwrap();
    services.store.close().await;
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    let final_primary = reopened.get_primary_principal().await.unwrap();
    let events = reopened
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap();
    reopened.close().await;
    assert_eq!(final_primary.identity_key(), newer.identity_key());
    assert_eq!(final_primary.login, newer.login);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "principal:identity-changed")
            .count(),
        1
    );
}

#[intent_test_macros::daemon_test]
async fn gitlab_probe_rechecks_newer_token_after_held_initial_get() {
    assert_probe_preserves_newer_token_during_get(false).await;
}

#[intent_test_macros::daemon_test]
async fn gitlab_probe_cannot_disconnect_newer_token_after_held_retry_get() {
    assert_probe_preserves_newer_token_during_get(true).await;
}

async fn assert_probe_preserves_newer_token_during_get(retry: bool) {
    use intent_sourcecontrol::gitlab_token::{
        EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT,
    };
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let (dir, services, bus) = harness().await;
    let raw = intent_core::FileSecretStore::with_path(dir.path().join("probe-newer-token.json"));
    for (key, value) in [
        (SECRET_ACCOUNT, "old-device"),
        (REFRESH_SECRET_ACCOUNT, "old-refresh"),
        (EXPIRES_AT_SECRET_ACCOUNT, "9999999999"),
    ] {
        raw.store(key, value).unwrap();
    }
    let services = services
        .with_gitlab_secret_store(raw.clone())
        .with_secret_store(Arc::new(raw.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    services
        .settings_registry
        .as_ref()
        .unwrap()
        .apply(&[
            (
                "sourceControl.gitlab.host".into(),
                json!("gitlab.race.test"),
            ),
            ("sourceControl.gitlab.apiBaseUrl".into(), json!(origin)),
            (
                "sourceControl.gitlab.oauthClientId".into(),
                json!("test-client"),
            ),
        ])
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let refreshes = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn({
        let entered = entered.clone();
        let release = release.clone();
        let refreshes = refreshes.clone();
        async move {
            let mut clients = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream,_) = accepted.unwrap();
                        let entered = entered.clone();
                        let release = release.clone();
                        let refreshes = refreshes.clone();
                        clients.spawn(async move {
                            let mut reader = BufReader::new(stream);
                            let mut request = String::new();
                            reader.read_line(&mut request).await.unwrap();
                            let mut token = String::new();
                            let mut length = 0;
                            loop {
                                let mut line=String::new();
                                reader.read_line(&mut line).await.unwrap();
                                if line=="\r\n" {break;}
                                if let Some((key,value))=line.split_once(':') {
                                    if key.eq_ignore_ascii_case("authorization") {token=value.trim().strip_prefix("Bearer ").unwrap().to_owned();}
                                    if key.eq_ignore_ascii_case("content-length") {length=value.trim().parse().unwrap();}
                                }
                            }
                            reader.read_exact(&mut vec![0;length]).await.unwrap();
                            let (status,body)=if request.starts_with("POST /oauth/token ") {
                                refreshes.fetch_add(1,Ordering::SeqCst);
                                ("200 OK",json!({"access_token":"rotated-old","refresh_token":"rotated-refresh","expires_in":7200}))
                            } else if request.starts_with("GET /api/v4/personal_access_tokens/self ") {
                                assert_eq!(token,"newer-pat");
                                ("200 OK",json!({"scopes":["api"]}))
                            } else {
                                assert!(request.starts_with("GET /api/v4/user "));
                                if token=="newer-pat" {
                                    ("200 OK",json!({"id":99,"username":"newer-user","name":"Newer User"}))
                                } else {
                                    assert!(matches!(token.as_str(),"old-device"|"rotated-old"));
                                    if token==if retry {"rotated-old"} else {"old-device"} {
                                        entered.notify_one();
                                        release.notified().await;
                                    }
                                    ("401 Unauthorized",json!({"message":"old token rejected"}))
                                }
                            };
                            let body=body.to_string();
                            reader.get_mut().write_all(format!("HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                        });
                    }
                    result = clients.join_next(), if !clients.is_empty() => {result.unwrap().unwrap();}
                }
            }
        }
    });
    let worker = services.clone();
    let probe = intent_core::spawn_daemon(async move {
        worker.source_control_get_user("gitlab".into(), None).await
    });
    timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    // This full public mutation must acquire the credential gate and finish its
    // actual persistence while the older real HTTP response remains held.
    let newer = timeout(
        Duration::from_secs(5),
        services.source_control_connect(
            "gitlab".into(),
            None,
            Some("pat".into()),
            Some("newer-pat".into()),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(newer["method"], "pat");
    assert_eq!(
        raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
        Some("newer-pat")
    );
    assert_eq!(
        services
            .secrets
            .load(SECRET_ACCOUNT)
            .await
            .unwrap()
            .as_deref(),
        Some("newer-pat")
    );
    release.notify_one();
    let result = timeout(Duration::from_secs(5), probe)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    if !retry {
        assert_eq!(result["user"]["login"], "newer-user");
    }
    services.shutdown_store_writers().await;
    server.abort();
    let _ = server.await;
    assert_eq!(refreshes.load(Ordering::SeqCst), usize::from(retry));
    assert_eq!(
        raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
        Some("newer-pat")
    );
    assert_eq!(
        services
            .secrets
            .load(SECRET_ACCOUNT)
            .await
            .unwrap()
            .as_deref(),
        Some("newer-pat")
    );
    assert!(raw.load(REFRESH_SECRET_ACCOUNT).unwrap().is_none());
    assert!(raw.load(EXPIRES_AT_SECRET_ACCOUNT).unwrap().is_none());
    bus.shutdown().await.unwrap();
    services.store.close().await;
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    let events = reopened
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap();
    reopened.close().await;
    let phases: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "sourceControl:auth-changed")
        .map(|e| e.data["status"].as_str().unwrap())
        .collect();
    assert_eq!(phases, vec!["authorized"]);
}

#[intent_test_macros::daemon_test]
async fn gitlab_probe_retains_refresh_and_disconnect_results_through_shutdown() {
    use intent_sourcecontrol::gitlab_token::{
        EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT,
    };
    for disconnect in [false, true] {
        for outcome in 0..5 {
            let (dir, services, bus) = harness().await;
            let raw = intent_core::FileSecretStore::with_path(dir.path().join("probe-held.json"));
            for (account, value) in [
                (SECRET_ACCOUNT, "old"),
                (REFRESH_SECRET_ACCOUNT, "old-refresh"),
                (EXPIRES_AT_SECRET_ACCOUNT, "1"),
            ] {
                raw.store(account, value).unwrap();
            }
            let mut services = services
                .with_gitlab_secret_store(raw.clone())
                .with_secret_store(Arc::new(raw.clone()));
            Arc::get_mut(&mut services.secrets).unwrap().write_timeout = Duration::from_millis(10);
            for account in [
                SECRET_ACCOUNT,
                REFRESH_SECRET_ACCOUNT,
                EXPIRES_AT_SECRET_ACCOUNT,
            ] {
                services.secrets.load(account).await.unwrap();
            }
            let (host, server) =
                crate::source_control_auth_ops::startup_tests::refresh_host(disconnect).await;
            services
                .settings_registry
                .as_ref()
                .unwrap()
                .apply(&[
                    ("sourceControl.gitlab.host".into(), json!(host.host())),
                    (
                        "sourceControl.gitlab.apiBaseUrl".into(),
                        json!(host.base_url()),
                    ),
                    (
                        "sourceControl.gitlab.oauthClientId".into(),
                        json!("test-client"),
                    ),
                ])
                .unwrap();
            let entered = Arc::new(tokio::sync::Notify::new());
            let signal = entered.clone();
            let (release, held) = std::sync::mpsc::channel();
            *services.secrets.before_gitlab_persistence.lock().unwrap() =
                Some(Box::new(move || {
                    signal.notify_one();
                    let _ = held.recv();
                    match outcome {
                        1 => Err(Error::InvalidParams(
                            "controlled probe persistence failure".into(),
                        )),
                        2 => panic!("controlled probe backend panic"),
                        _ => Ok(()),
                    }
                }));
            let (fail, failing) = tokio::sync::oneshot::channel();
            if outcome == 3 {
                *services.secrets.panic_mutation_caller.lock().unwrap() = Some(failing);
            }
            let worker = services.clone();
            let caller = intent_core::spawn_daemon(async move {
                worker
                    .source_control_auth_status("gitlab".into(), None)
                    .await
            });
            timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            match outcome {
                0 => {
                    caller.abort();
                    let _ = caller.await;
                }
                3 => {
                    fail.send(()).unwrap();
                    let error = timeout(Duration::from_secs(5), caller)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap_err();
                    assert!(error.to_string().contains("probe failed"));
                }
                _ => {
                    let error = timeout(Duration::from_secs(5), caller)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap_err();
                    assert!(error.to_string().contains("timed out"));
                }
            }
            assert!(services.gitlab_credential_gate.try_lock().is_err());
            let (pending, at_drain) = tokio::sync::oneshot::channel();
            *services.secrets.writer_drain_pending.lock().unwrap() = Some(pending);
            let closer = services.clone();
            let drain = intent_core::spawn_daemon(async move {
                closer.shutdown_store_writers().await;
            });
            assert_eq!(
                timeout(Duration::from_secs(5), at_drain)
                    .await
                    .unwrap()
                    .unwrap(),
                "settings-tasks"
            );
            assert!(services.settings_tasks.is_closed());
            assert!(!drain.is_finished());
            assert_eq!(raw.load(SECRET_ACCOUNT).unwrap().as_deref(), Some("old"));
            release.send(()).unwrap();
            timeout(Duration::from_secs(5), drain)
                .await
                .unwrap()
                .unwrap();
            server.abort();
            let _ = server.await;
            assert!(services.gitlab_credential_gate.try_lock().is_ok());
            assert!(services.secrets.state.lock().unwrap().mutations.is_empty());
            assert_eq!(
                raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
                if matches!(outcome, 0 | 3 | 4) {
                    if disconnect {
                        None
                    } else {
                        Some("refreshed-token")
                    }
                } else {
                    Some("old")
                }
            );
            for account in [
                SECRET_ACCOUNT,
                REFRESH_SECRET_ACCOUNT,
                EXPIRES_AT_SECRET_ACCOUNT,
            ] {
                assert_eq!(
                    services.secrets.load(account).await.unwrap(),
                    raw.load(account).unwrap()
                );
            }
            bus.shutdown().await.unwrap();
            services.store.close().await;
            let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
            let events = reopened
                .query_events(&intent_store::EventQuery::default())
                .await
                .unwrap();
            let statuses: Vec<_> = events
                .iter()
                .filter(|e| e.event_type == "sourceControl:auth-changed")
                .map(|e| e.data["status"].as_str().unwrap())
                .collect();
            assert_eq!(
                statuses,
                if disconnect && matches!(outcome, 0 | 4) {
                    vec!["expired"]
                } else {
                    vec![]
                }
            );
            reopened.close().await;
        }
    }
}

#[intent_test_macros::daemon_test]
async fn gitlab_probe_roots_refuse_after_early_close() {
    for auth_status in [false, true] {
        let (dir, services, bus) = harness().await;
        let raw = intent_core::FileSecretStore::with_path(dir.path().join("probe-root.json"));
        raw.store(
            intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
            "saved-pat",
        )
        .unwrap();
        let services = services
            .with_gitlab_secret_store(raw.clone())
            .with_secret_store(Arc::new(raw.clone()));
        let (host, server) = crate::source_control_auth_ops::startup_tests::pat_host().await;
        services
            .settings_registry
            .as_ref()
            .unwrap()
            .apply(&[
                ("sourceControl.gitlab.host".into(), json!(host.host())),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(host.base_url()),
                ),
            ])
            .unwrap();
        services.begin_settings_shutdown();
        let result = if auth_status {
            services
                .source_control_auth_status("gitlab".into(), None)
                .await
        } else {
            services
                .source_control_get_user("gitlab".into(), None)
                .await
        };
        server.abort();
        let _ = server.await;
        services.shutdown_store_writers().await;
        bus.shutdown().await.unwrap();
        services.store.close().await;
        assert!(
            matches!(&result, Err(Error::Internal(message)) if message.contains("shutting down")),
            "new probe root escaped closed admission: {result:?}"
        );
        assert_eq!(
            raw.load(intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT)
                .unwrap()
                .as_deref(),
            Some("saved-pat")
        );
    }
}

#[intent_test_macros::daemon_test]
async fn gitlab_direct_endpoints_require_administrator_before_admission() {
    use intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT;
    let (dir, services, bus) = harness().await;
    let raw = intent_core::FileSecretStore::with_path(dir.path().join("direct-auth.json"));
    raw.store(SECRET_ACCOUNT, "saved-token").unwrap();
    let services = services
        .with_gitlab_secret_store(raw.clone())
        .with_secret_store(Arc::new(raw.clone()));
    services
        .settings_registry
        .as_ref()
        .unwrap()
        .apply(&[
            (
                "sourceControl.gitlab.host".into(),
                json!("gitlab.result.test"),
            ),
            (
                "sourceControl.gitlab.apiBaseUrl".into(),
                json!("http://127.0.0.1:1"),
            ),
        ])
        .unwrap();
    let member = Caller::Wire {
        principal_id: PrincipalId::new(),
        host_role: HostRole::Member,
    };
    intent_core::with_caller(member, async {
        for result in [
            services
                .source_control_connect(
                    "gitlab".into(),
                    Some("gitlab.result.test".into()),
                    Some("pat".into()),
                    Some("valid-test-token".into()),
                )
                .await,
            services
                .source_control_revoke("gitlab".into(), Some("gitlab.result.test".into()))
                .await,
            services
                .source_control_get_user("gitlab".into(), Some("gitlab.result.test".into()))
                .await,
            services
                .source_control_auth_status("gitlab".into(), Some("gitlab.result.test".into()))
                .await,
        ] {
            assert!(matches!(result, Err(Error::Forbidden(_))), "{result:?}");
        }
    })
    .await;
    assert_eq!(services.secrets.state.lock().unwrap().next_owner, 0);
    assert_eq!(
        raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
        Some("saved-token")
    );
    services.shutdown_store_writers().await;
    bus.shutdown().await.unwrap();
    services.store.close().await;
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    assert!(!reopened
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap()
        .iter()
        .any(|event| event.event_type == "sourceControl:auth-changed"));
    reopened.close().await;
}

#[intent_test_macros::daemon_test]
async fn gitlab_direct_mutations_retain_actual_results_and_guards() {
    use intent_sourcecontrol::gitlab_token::{
        EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT,
    };
    for pat in [false, true] {
        for outcome in 0..5 {
            let (dir, services, bus) = harness().await;
            let raw = intent_core::FileSecretStore::with_path(dir.path().join("direct-held.json"));
            for (account, value) in [
                (SECRET_ACCOUNT, "old"),
                (REFRESH_SECRET_ACCOUNT, "old-refresh"),
                (EXPIRES_AT_SECRET_ACCOUNT, "1"),
            ] {
                raw.store(account, value).unwrap();
            }
            let mut services = services
                .with_gitlab_secret_store(raw.clone())
                .with_secret_store(Arc::new(raw.clone()));
            Arc::get_mut(&mut services.secrets).unwrap().write_timeout = Duration::from_millis(10);
            for account in [
                SECRET_ACCOUNT,
                REFRESH_SECRET_ACCOUNT,
                EXPIRES_AT_SECRET_ACCOUNT,
            ] {
                services.secrets.load(account).await.unwrap();
            }
            let entered = Arc::new(tokio::sync::Notify::new());
            let signal = entered.clone();
            let (release, held) = std::sync::mpsc::channel();
            *services.secrets.before_gitlab_persistence.lock().unwrap() =
                Some(Box::new(move || {
                    signal.notify_one();
                    let _ = held.recv();
                    match outcome {
                        1 => Err(Error::InvalidParams(
                            "controlled direct write failure".into(),
                        )),
                        2 => panic!("controlled direct backend panic"),
                        _ => Ok(()),
                    }
                }));
            let (fail, failing) = tokio::sync::oneshot::channel();
            if outcome == 3 {
                *services.secrets.panic_mutation_caller.lock().unwrap() = Some(failing);
            }
            let owner = services.clone();
            let caller = if pat {
                let (host, mock) = crate::source_control_auth_ops::startup_tests::pat_host().await;
                intent_core::spawn_daemon(async move {
                    let result = owner.gitlab_connect_pat(host, "new-pat".into()).await;
                    mock.await.unwrap();
                    result
                })
            } else {
                intent_core::spawn_daemon(async move {
                    owner
                        .source_control_revoke("gitlab".into(), Some("gitlab.com".into()))
                        .await
                })
            };
            timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            match outcome {
                0 => {
                    caller.abort();
                    let _ = caller.await;
                }
                3 => {
                    fail.send(()).unwrap();
                    let error = timeout(Duration::from_secs(5), caller)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap_err();
                    assert!(error.to_string().contains("operation failed"));
                }
                _ => {
                    let error = timeout(Duration::from_secs(5), caller)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap_err();
                    assert!(error.to_string().contains("timed out"));
                }
            }
            assert!(services.gitlab_credential_gate.try_lock().is_err());
            let (pending, at_drain) = tokio::sync::oneshot::channel();
            *services.secrets.writer_drain_pending.lock().unwrap() = Some(pending);
            let closer = services.clone();
            let drain = intent_core::spawn_daemon(async move {
                closer.shutdown_store_writers().await;
            });
            assert_eq!(
                timeout(Duration::from_secs(5), at_drain)
                    .await
                    .unwrap()
                    .unwrap(),
                "settings-tasks"
            );
            assert!(services.settings_tasks.is_closed());
            assert!(!drain.is_finished());
            assert_eq!(raw.load(SECRET_ACCOUNT).unwrap().as_deref(), Some("old"));
            release.send(()).unwrap();
            timeout(Duration::from_secs(5), drain)
                .await
                .unwrap()
                .unwrap();
            assert!(services.gitlab_credential_gate.try_lock().is_ok());
            assert!(services.secrets.state.lock().unwrap().mutations.is_empty());
            assert_eq!(
                raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
                if outcome == 0 || outcome == 3 || outcome == 4 {
                    if pat {
                        Some("new-pat")
                    } else {
                        None
                    }
                } else {
                    Some("old")
                }
            );
            for account in [
                SECRET_ACCOUNT,
                REFRESH_SECRET_ACCOUNT,
                EXPIRES_AT_SECRET_ACCOUNT,
            ] {
                assert_eq!(
                    services.secrets.load(account).await.unwrap(),
                    raw.load(account).unwrap()
                );
            }
            assert_eq!(
                services.effective_settings().source_control.gitlab.host,
                if pat && (outcome == 0 || outcome == 4) {
                    "gitlab.result.test"
                } else {
                    "gitlab.com"
                }
            );
            bus.shutdown().await.unwrap();
            services.store.close().await;
            let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
            let events = reopened
                .query_events(&intent_store::EventQuery::default())
                .await
                .unwrap();
            let statuses: Vec<_> = events
                .iter()
                .filter(|event| {
                    event.event_type == "sourceControl:auth-changed"
                        && event.data["provider"] == "gitlab"
                })
                .map(|event| event.data["status"].as_str().unwrap())
                .collect();
            assert_eq!(
                statuses,
                if outcome == 0 || outcome == 4 {
                    vec![if pat { "authorized" } else { "revoked" }]
                } else {
                    vec![]
                }
            );
            reopened.close().await;
        }
    }
}

#[intent_test_macros::daemon_test]
async fn gitlab_direct_revoke_refuses_after_early_close() {
    assert_gitlab_direct_refused(false).await;
}

#[intent_test_macros::daemon_test]
async fn gitlab_direct_pat_refuses_after_early_close() {
    assert_gitlab_direct_refused(true).await;
}

async fn assert_gitlab_direct_refused(pat: bool) {
    use intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT;
    let (dir, services, bus) = harness().await;
    let raw = intent_core::FileSecretStore::with_path(dir.path().join("direct-gitlab.json"));
    raw.store(SECRET_ACCOUNT, "old-token").unwrap();
    let services = services
        .with_gitlab_secret_store(raw.clone())
        .with_secret_store(Arc::new(raw.clone()));
    services.begin_settings_shutdown();
    let result = if pat {
        let (host, server) = crate::source_control_auth_ops::startup_tests::pat_host().await;
        let result = services
            .gitlab_connect_pat(host, "new-test-pat".into())
            .await;
        server.await.unwrap();
        result
    } else {
        services
            .source_control_revoke("gitlab".into(), Some("gitlab.com".into()))
            .await
    };
    services.shutdown_store_writers().await;
    bus.shutdown().await.unwrap();
    services.store.close().await;
    assert!(
        matches!(&result, Err(Error::Internal(message)) if message.contains("shutting down")),
        "direct GitLab mutation escaped closed admission: {result:?}"
    );
    assert_eq!(
        raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
        Some("old-token")
    );
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    assert!(!reopened
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap()
        .iter()
        .any(|event| event.event_type == "sourceControl:auth-changed"));
    reopened.close().await;
}

struct HeldSiblingReads {
    inner: InMemorySecretStore,
    held: Mutex<HashMap<String, std::sync::mpsc::Receiver<()>>>,
    entered: tokio::sync::mpsc::UnboundedSender<String>,
}

impl SecretStore for HeldSiblingReads {
    fn load(&self, account: &str) -> Result<Option<String>> {
        let value = self.inner.load(account)?;
        let held = self.held.lock().unwrap().remove(account);
        if let Some(held) = held {
            self.entered.send(account.to_owned()).unwrap();
            let _ = held.recv();
        }
        Ok(value)
    }

    fn store(&self, account: &str, value: &str) -> Result<()> {
        self.inner.store(account, value)
    }

    fn delete(&self, account: &str) -> Result<()> {
        self.inner.delete(account)
    }
}

#[intent_test_macros::daemon_test]
async fn gitlab_settlement_prevents_stale_sibling_reads_refilling_cache() {
    use intent_sourcecontrol::gitlab_token::{
        EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT,
    };
    let accounts = [
        SECRET_ACCOUNT,
        REFRESH_SECRET_ACCOUNT,
        EXPIRES_AT_SECRET_ACCOUNT,
    ];
    for outcome in 0..3 {
        let (entered, mut ready) = tokio::sync::mpsc::unbounded_channel();
        let backend = Arc::new(HeldSiblingReads {
            inner: InMemorySecretStore::default(),
            held: Mutex::new(HashMap::new()),
            entered,
        });
        let mut releases = Vec::new();
        for account in accounts {
            backend.inner.store(account, "old").unwrap();
            let (release, held) = std::sync::mpsc::channel();
            releases.push(release);
            backend
                .held
                .lock()
                .unwrap()
                .insert(account.to_owned(), held);
        }
        let mut secrets = AsyncSecretStore::new(backend.clone());
        secrets.load_timeout = Duration::from_secs(5);
        let mut readers = Vec::new();
        for account in accounts {
            let reader = secrets.clone();
            readers.push(intent_core::spawn_daemon(async move {
                reader.load(account).await
            }));
        }
        for _ in accounts {
            timeout(Duration::from_secs(5), ready.recv())
                .await
                .unwrap()
                .unwrap();
        }
        let writer = backend.clone();
        let operation = secrets.operation();
        // The same retained record finalizes full and partial tuple outcomes.
        let result = operation
            .mutate(SECRET_ACCOUNT, move || {
                writer.store(SECRET_ACCOUNT, "new")?;
                match outcome {
                    1 => return Err(Error::InvalidParams("partial tuple error".into())),
                    2 => panic!("partial tuple panic"),
                    _ => {}
                }
                writer.store(REFRESH_SECRET_ACCOUNT, "new")?;
                writer.store(EXPIRES_AT_SECRET_ACCOUNT, "new")
            })
            .await;
        assert_eq!(result.is_ok(), outcome == 0);
        for account in accounts {
            assert!(!secrets.state.lock().unwrap().entries.contains_key(account));
            assert_eq!(
                secrets.load(account).await.unwrap(),
                backend.inner.load(account).unwrap()
            );
        }
        for release in releases {
            release.send(()).unwrap();
        }
        for reader in readers {
            // These callers began before the mutation; their stale completion
            // must not replace the newer cache entry after invalidation.
            assert_eq!(reader.await.unwrap().unwrap().as_deref(), Some("old"));
        }
        for account in accounts {
            assert_eq!(
                secrets.load(account).await.unwrap(),
                backend.inner.load(account).unwrap()
            );
        }
        operation.finish_operation().await;
        secrets.shutdown_mutations().await;
    }
}

#[intent_test_macros::daemon_test]
async fn gitlab_partial_error_cannot_publish_after_newer_revoke() {
    use intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT;
    let (dir, services, bus) = harness().await;
    let raw = intent_core::FileSecretStore::with_path(dir.path().join("gitlab-order.json"));
    raw.store(SECRET_ACCOUNT, "old-token").unwrap();
    let services = services
        .with_gitlab_secret_store(raw.clone())
        .with_secret_store(Arc::new(raw.clone()));
    services
        .settings_registry
        .as_ref()
        .unwrap()
        .apply(&[
            (
                "sourceControl.gitlab.oauthClientId".into(),
                json!("private-client"),
            ),
            (
                "sourceControl.gitlab.host".into(),
                json!("gitlab.result.test"),
            ),
        ])
        .unwrap();
    let partial = raw.clone();
    *services.secrets.before_gitlab_persistence.lock().unwrap() = Some(Box::new(move || {
        partial.store(SECRET_ACCOUNT, "partial-token")?;
        Err(Error::InvalidParams(
            "controlled error after token write".into(),
        ))
    }));
    let (entered, at_publication) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    let (gate_polled, gate_result) = tokio::sync::oneshot::channel();
    {
        let mut state = services.gitlab_auth.lock().await;
        state.before_terminal_publish = Some((entered, held));
        state.revoke_gate_polled = Some(gate_polled);
    }
    let (host, server) = crate::source_control_auth_ops::startup_tests::authorized_host().await;
    services.gitlab_connect_device(host).await.unwrap();
    timeout(Duration::from_secs(5), at_publication)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
        Some("partial-token")
    );
    let newer = services.clone();
    let revoked = intent_core::spawn_daemon(async move {
        newer
            .source_control_revoke("gitlab".into(), Some("gitlab.result.test".into()))
            .await
    });
    let blocked = timeout(Duration::from_secs(5), gate_result)
        .await
        .unwrap()
        .unwrap();
    // Synchronize at the actual credential gate, not the outer request poll.
    // In the faulty ordering revoke can finish before the old event is released.
    if blocked {
        assert_eq!(
            raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
            Some("partial-token")
        );
        release.send(()).unwrap();
        timeout(Duration::from_secs(5), revoked)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    } else {
        timeout(Duration::from_secs(5), revoked)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        release.send(()).unwrap();
    }
    services.shutdown_store_writers().await;
    server.await.unwrap();
    assert_eq!(raw.load(SECRET_ACCOUNT).unwrap(), None);
    bus.shutdown().await.unwrap();
    services.store.close().await;
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    let mut events = reopened
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap();
    // Store queries are newest-first; UUIDv7 event IDs preserve creation order.
    events.sort_by(|a, b| a.id.cmp(&b.id));
    let statuses: Vec<_> = events
        .iter()
        .filter(|event| {
            event.event_type == "sourceControl:auth-changed" && event.data["provider"] == "gitlab"
        })
        .map(|event| event.data["status"].as_str().unwrap())
        .collect();
    assert_eq!(
        statuses,
        vec!["error", "revoked"],
        "old failure overtook a newer credential result"
    );
    reopened.close().await;
}

#[intent_test_macros::daemon_test]
async fn gitlab_poll_retains_prewrite_and_worker_failure_through_real_drain() {
    use intent_sourcecontrol::gitlab_token::{
        EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT,
    };
    for outcome in 0..4 {
        let (dir, services, bus) = harness().await;
        let raw = intent_core::FileSecretStore::with_path(dir.path().join("gitlab-secrets.json"));
        for (account, value) in [
            (SECRET_ACCOUNT, "old"),
            (REFRESH_SECRET_ACCOUNT, "old-refresh"),
            (EXPIRES_AT_SECRET_ACCOUNT, "1"),
        ] {
            raw.store(account, value).unwrap();
        }
        let mut services = services
            .with_gitlab_secret_store(raw.clone())
            .with_secret_store(Arc::new(raw.clone()));
        assert_eq!(services.gitlab_secret_store.path(), raw.path());
        services
            .settings_registry
            .as_ref()
            .unwrap()
            .apply(&[(
                "sourceControl.gitlab.oauthClientId".into(),
                json!("private-client"),
            )])
            .unwrap();
        Arc::get_mut(&mut services.secrets).unwrap().write_timeout = Duration::from_millis(10);
        for account in [
            SECRET_ACCOUNT,
            REFRESH_SECRET_ACCOUNT,
            EXPIRES_AT_SECRET_ACCOUNT,
        ] {
            services.secrets.load(account).await.unwrap();
        }
        let entered = Arc::new(tokio::sync::Notify::new());
        let signal = entered.clone();
        let (release, held) = std::sync::mpsc::channel();
        *services.secrets.before_gitlab_persistence.lock().unwrap() = Some(Box::new(move || {
            signal.notify_one();
            let _ = held.recv();
            match outcome {
                1 => Err(Error::InvalidParams("controlled prewrite error".into())),
                2 => panic!("controlled GitLab blocking panic"),
                _ => Ok(()),
            }
        }));
        let (panic_worker, worker_failed) = tokio::sync::oneshot::channel();
        if outcome == 3 {
            *services.secrets.panic_mutation_caller.lock().unwrap() = Some(worker_failed);
        }
        let (host, server) = crate::source_control_auth_ops::startup_tests::authorized_host().await;
        services.gitlab_connect_device(host).await.unwrap();
        timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        if outcome == 3 {
            panic_worker.send(()).unwrap();
            timeout(
                Duration::from_secs(5),
                services.secrets.gitlab_poll_worker_failed.notified(),
            )
            .await
            .unwrap();
        } else {
            timeout(Duration::from_secs(5), async {
                loop {
                    if services
                        .secrets
                        .state
                        .lock()
                        .unwrap()
                        .mutations
                        .iter()
                        .any(|r| r.account == SECRET_ACCOUNT && r.timed_out.load(Ordering::SeqCst))
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        assert!(services.gitlab_credential_gate.try_lock().is_err());
        assert_eq!(raw.load(SECRET_ACCOUNT).unwrap().as_deref(), Some("old"));
        let (pending, at_fence) = tokio::sync::oneshot::channel();
        *services.secrets.writer_drain_pending.lock().unwrap() = Some(pending);
        let owner = services.clone();
        let draining = intent_core::spawn_daemon(async move {
            owner.shutdown_store_writers().await;
        });
        let stage = timeout(Duration::from_secs(5), at_fence)
            .await
            .unwrap()
            .unwrap();
        assert!(services.settings_tasks.is_closed());
        assert_eq!(
            stage,
            if outcome == 3 {
                "store-tasks"
            } else {
                "gitlab-state"
            }
        );
        assert!(
            !draining.is_finished(),
            "real shutdown passed the held write"
        );
        release.send(()).unwrap();
        timeout(Duration::from_secs(5), draining)
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
        assert!(services.gitlab_credential_gate.try_lock().is_ok());
        assert!(services.secrets.state.lock().unwrap().mutations.is_empty());
        assert_eq!(
            raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
            Some(if outcome == 0 || outcome == 3 {
                "controlled-gitlab-token"
            } else {
                "old"
            })
        );
        for account in [
            SECRET_ACCOUNT,
            REFRESH_SECRET_ACCOUNT,
            EXPIRES_AT_SECRET_ACCOUNT,
        ] {
            assert_eq!(
                services.secrets.load(account).await.unwrap(),
                raw.load(account).unwrap()
            );
        }
        assert_eq!(
            services.effective_settings().source_control.gitlab.host,
            if outcome == 0 {
                "gitlab.result.test"
            } else {
                "gitlab.com"
            }
        );
        bus.shutdown().await.unwrap();
        services.store.close().await;
        let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
        let authorized = reopened
            .query_events(&intent_store::EventQuery::default())
            .await
            .unwrap()
            .iter()
            .any(|e| {
                e.event_type == "sourceControl:auth-changed"
                    && e.data["provider"] == "gitlab"
                    && e.data["status"] == "authorized"
            });
        assert_eq!(authorized, outcome == 0);
        reopened.close().await;
    }
}

#[intent_test_macros::daemon_test]
async fn gitlab_partial_result_invalidates_siblings_without_consuming_other_receipts() {
    use intent_sourcecontrol::gitlab_token::{
        EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT,
    };
    let dir = crate::test_support::test_tempdir("gitlab-partial-receipt");
    let raw = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
    for account in [
        SECRET_ACCOUNT,
        REFRESH_SECRET_ACCOUNT,
        EXPIRES_AT_SECRET_ACCOUNT,
    ] {
        raw.store(account, "old").unwrap();
    }
    let mut secrets = AsyncSecretStore::new(Arc::new(raw.clone()));
    secrets.write_timeout = Duration::from_millis(10);
    let first = secrets.operation();
    let unrelated = secrets.operation();
    for account in [
        SECRET_ACCOUNT,
        REFRESH_SECRET_ACCOUNT,
        EXPIRES_AT_SECRET_ACCOUNT,
    ] {
        secrets.load(account).await.unwrap();
    }
    // Model the engine's sequential-write error boundary using real file IO:
    // token changed, then error before refresh/expiry; no atomic tuple claim.
    let partial = raw.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let (release, held) = std::sync::mpsc::channel();
    let writer = first.clone();
    let response = intent_core::spawn_daemon(async move {
        writer
            .mutate(SECRET_ACCOUNT, move || {
                partial.store(SECRET_ACCOUNT, "partial-token")?;
                signal.notify_one();
                let _ = held.recv();
                Err(Error::InvalidParams("failure before refresh/expiry".into()))
            })
            .await
    });
    timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert!(response
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("timed out"));
    let record = secrets.state.lock().unwrap().mutations[0].clone();
    release.send(()).unwrap();
    assert!(secrets.settle_detached(SECRET_ACCOUNT).await.unwrap());
    assert!(matches!(
        first.mutation_result(&record).await,
        Err(Error::InvalidParams(_))
    ));
    {
        let state = secrets.state.lock().unwrap();
        for account in [
            SECRET_ACCOUNT,
            REFRESH_SECRET_ACCOUNT,
            EXPIRES_AT_SECRET_ACCOUNT,
        ] {
            assert!(!state.entries.contains_key(account));
        }
    }
    // A different operation cannot acknowledge the first operation's result.
    unrelated.finish_operation().await;
    assert!(secrets
        .state
        .lock()
        .unwrap()
        .mutations
        .iter()
        .any(|r| r.owner == first.mutation_owner));
    first.finish_operation().await;
    assert!(secrets.state.lock().unwrap().mutations.is_empty());
    for account in [
        SECRET_ACCOUNT,
        REFRESH_SECRET_ACCOUNT,
        EXPIRES_AT_SECRET_ACCOUNT,
    ] {
        assert_eq!(
            secrets.load(account).await.unwrap(),
            raw.load(account).unwrap()
        );
    }
    secrets.shutdown_mutations().await;
    let invoked = Arc::new(AtomicBool::new(false));
    let effect = invoked.clone();
    assert!(secrets
        .operation()
        .mutate(SECRET_ACCOUNT, move || {
            effect.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await
        .is_err());
    assert!(!invoked.load(Ordering::SeqCst));
    assert!(secrets.state.lock().unwrap().mutations.is_empty());
}

fn hold_github_write(
    secrets: &AsyncSecretStore,
    outcome: u8,
) -> (Arc<tokio::sync::Notify>, std::sync::mpsc::Sender<()>) {
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let (release, held) = std::sync::mpsc::channel();
    *secrets.before_github_persistence.lock().unwrap() = Some(Box::new(move || {
        signal.notify_one();
        let _ = held.recv();
        match outcome {
            1 => Err(Error::InvalidParams(
                "controlled engine write failure".into(),
            )),
            2 => panic!("controlled engine blocking panic"),
            _ => Ok(()),
        }
    }));
    (entered, release)
}

async fn wait_github_write_timeout(secrets: &AsyncSecretStore) {
    timeout(Duration::from_secs(5), async {
        loop {
            if secrets
                .state
                .lock()
                .unwrap()
                .mutations
                .iter()
                .any(|record| {
                    record.account == crate::github_auth_ops::SECRET_ACCOUNT
                        && record.timed_out.load(Ordering::SeqCst)
                })
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

struct HeldGithubProvenance {
    raw: intent_core::FileSecretStore,
    entered: tokio::sync::Notify,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl SecretStore for HeldGithubProvenance {
    fn load(&self, account: &str) -> Result<Option<String>> {
        self.raw.load(account)
    }
    fn store(&self, account: &str, value: &str) -> Result<()> {
        if account == crate::source_control_auth_ops::GITHUB_TOKEN_METHOD_ACCOUNT {
            self.entered.notify_one();
            let _ = self.release.lock().unwrap().recv();
        }
        self.raw.store(account, value)
    }
    fn delete(&self, account: &str) -> Result<()> {
        self.raw.delete(account)
    }
}

#[intent_test_macros::daemon_test]
async fn github_poll_provenance_panic_keeps_finalization_guard() {
    use std::future::{poll_fn, Future};
    use std::task::Poll;
    let (dir, services, bus) = harness().await;
    let raw = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
    let (release_marker, marker_held) = std::sync::mpsc::channel();
    let backend = Arc::new(HeldGithubProvenance {
        raw: raw.clone(),
        entered: tokio::sync::Notify::new(),
        release: Mutex::new(marker_held),
    });
    let services = services.with_secret_store(backend.clone());
    let (engine_entered, release_engine) = hold_github_write(&services.secrets, 0);
    let flow = crate::github_auth_ops::credential_tests::authorize(
        &services,
        raw.clone(),
        crate::github_auth_ops::credential_tests::accept_identity(),
    )
    .await;
    timeout(Duration::from_secs(5), engine_entered.notified())
        .await
        .unwrap();
    let record = services
        .secrets
        .state
        .lock()
        .unwrap()
        .mutations
        .iter()
        .find(|record| record.account == crate::github_auth_ops::SECRET_ACCOUNT)
        .unwrap()
        .clone();
    let (panic_worker, worker_failure) = tokio::sync::oneshot::channel();
    let secrets = services.secrets.clone();
    // Arm the worker failure only after the engine Result has been captured.
    // Its mutation already passed the caller-panic seam; the next mutation is
    // the provenance write under the separately acquired finalization guard.
    *record.after_capture.lock().unwrap() = Some(Box::new(move || {
        *secrets.panic_mutation_caller.lock().unwrap() = Some(worker_failure);
    }));
    release_engine.send(()).unwrap();
    timeout(Duration::from_secs(5), backend.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        raw.load(crate::github_auth_ops::SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("new-device-token")
    );
    panic_worker.send(()).unwrap();
    timeout(
        Duration::from_secs(5),
        services.secrets.github_poll_worker_failed.notified(),
    )
    .await
    .unwrap();
    assert!(
        services.secrets.github_mutation.try_lock().is_err(),
        "provenance worker panic released the supervisor's finalization guard"
    );
    let drain = services.shutdown_store_writers();
    tokio::pin!(drain);
    assert!(poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx).is_pending())).await);
    assert_eq!(
        raw.load(crate::source_control_auth_ops::GITHUB_TOKEN_METHOD_ACCOUNT)
            .unwrap(),
        None
    );
    release_marker.send(()).unwrap();
    timeout(Duration::from_secs(5), drain).await.unwrap();
    flow.await.unwrap();
    assert!(services.secrets.github_mutation.try_lock().is_ok());
    assert!(services.secrets.state.lock().unwrap().mutations.is_empty());
    assert_eq!(
        services
            .secrets
            .load(crate::source_control_auth_ops::GITHUB_TOKEN_METHOD_ACCOUNT)
            .await
            .unwrap()
            .as_deref(),
        Some("device")
    );
    bus.shutdown().await.unwrap();
    services.store.close().await;
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    assert!(!reopened
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap()
        .iter()
        .any(|event| {
            event.event_type == "github:auth-changed" && event.data["status"] == "authorized"
        }));
    reopened.close().await;
}

#[intent_test_macros::daemon_test]
async fn github_poll_shutdown_retains_real_write_and_panic_settlement() {
    use std::future::{poll_fn, Future};
    use std::task::Poll;
    for outcome in 0..4 {
        let (dir, services, bus) = harness().await;
        let raw = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
        let mut services = services.with_secret_store(Arc::new(raw.clone()));
        Arc::get_mut(&mut services.secrets).unwrap().write_timeout = Duration::from_millis(10);
        let (entered, release) = hold_github_write(&services.secrets, outcome);
        let (panic_worker, worker_failure) = tokio::sync::oneshot::channel();
        if outcome == 3 {
            *services.secrets.panic_mutation_caller.lock().unwrap() = Some(worker_failure);
        }
        let flow = crate::github_auth_ops::credential_tests::authorize(
            &services,
            raw.clone(),
            crate::github_auth_ops::credential_tests::accept_identity(),
        )
        .await;
        timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        if outcome == 3 {
            panic_worker.send(()).unwrap();
            timeout(
                Duration::from_secs(5),
                services.secrets.github_poll_worker_failed.notified(),
            )
            .await
            .unwrap();
        } else {
            wait_github_write_timeout(&services.secrets).await;
        }
        assert!(services.secrets.github_mutation.try_lock().is_err());
        assert_eq!(
            raw.load(crate::github_auth_ops::SECRET_ACCOUNT).unwrap(),
            None
        );
        let drain = services.shutdown_store_writers();
        tokio::pin!(drain);
        assert!(
            poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx).is_pending())).await,
            "shutdown abandoned the actual engine blocking closure"
        );
        release.send(()).unwrap();
        timeout(Duration::from_secs(5), drain).await.unwrap();
        flow.await.unwrap();
        assert!(services.secrets.github_mutation.try_lock().is_ok());
        assert!(services.secrets.state.lock().unwrap().mutations.is_empty());
        let expected = (outcome == 3).then_some("new-device-token");
        assert_eq!(raw.load(crate::github_auth_ops::SECRET_ACCOUNT).unwrap().as_deref(), expected,
            "normal shutdown must reconcile orphan success; worker panic remains explicitly unknown");
        assert_eq!(
            services
                .secrets
                .load(crate::github_auth_ops::SECRET_ACCOUNT)
                .await
                .unwrap()
                .as_deref(),
            expected
        );
        assert_eq!(
            raw.load(crate::source_control_auth_ops::GITHUB_TOKEN_METHOD_ACCOUNT)
                .unwrap(),
            None
        );
        bus.shutdown().await.unwrap();
        services.store.close().await;
        let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
        assert!(!reopened
            .query_events(&intent_store::EventQuery::default())
            .await
            .unwrap()
            .iter()
            .any(|event| {
                event.event_type == "github:auth-changed" && event.data["status"] == "authorized"
            }));
        reopened.close().await;
    }
}

#[intent_test_macros::daemon_test]
async fn github_poll_late_result_cannot_overwrite_newer_generation() {
    use std::future::{poll_fn, Future};
    use std::task::Poll;
    let (dir, services, bus) = harness().await;
    let raw = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
    let mut services = services.with_secret_store(Arc::new(raw.clone()));
    Arc::get_mut(&mut services.secrets).unwrap().write_timeout = Duration::from_millis(10);
    let (entered, release) = hold_github_write(&services.secrets, 0);
    let flow = crate::github_auth_ops::credential_tests::authorize(
        &services,
        raw.clone(),
        crate::github_auth_ops::credential_tests::accept_identity(),
    )
    .await;
    timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    wait_github_write_timeout(&services.secrets).await;
    let (blocked, observed_blocked) = tokio::sync::oneshot::channel();
    *services.secrets.github_lock_pending.lock().unwrap() = Some(blocked);
    // Only the old held operation needs an artificially short wait budget.
    // Share its actual state/generation, while allowing the new real filesystem
    // operation its normal wait budget after it acquires that same lock.
    let mut newer_services = services.clone();
    let mut newer_secrets = (*services.secrets).clone();
    newer_secrets.write_timeout = DEFAULT_WRITE_TIMEOUT;
    newer_services.secrets = Arc::new(newer_secrets);
    let newer = newer_services
        .settings_update(json!([{"path":"sourceControl.github.token","value":"newer-token"}]));
    tokio::pin!(newer);
    assert!(poll_fn(|cx| Poll::Ready(newer.as_mut().poll(cx).is_pending())).await);
    timeout(Duration::from_secs(5), observed_blocked)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        raw.load(crate::github_auth_ops::SECRET_ACCOUNT).unwrap(),
        None
    );
    release.send(()).unwrap();
    timeout(Duration::from_secs(5), newer)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(5), flow)
        .await
        .unwrap()
        .unwrap();
    services.shutdown_store_writers().await;
    assert_eq!(
        raw.load(crate::github_auth_ops::SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("newer-token")
    );
    assert!(services.secrets.state.lock().unwrap().mutations.is_empty());
    bus.shutdown().await.unwrap();
    services.store.close().await;
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    assert!(!reopened
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap()
        .iter()
        .any(|event| {
            event.event_type == "github:auth-changed" && event.data["status"] == "authorized"
        }));
    reopened.close().await;
}

#[intent_test_macros::daemon_test]
async fn admitted_settings_owner_survives_cancellation_before_first_poll() {
    let (_dir, services, bus) = harness().await;
    {
        let mut request = services.settings_update(json!([
            {"path":"notifications.volume","value":0.75}
        ]));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                request.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        // The single-thread runtime has not polled the registered supervisor;
        // dropping only this response future must not abort its ownership.
    }
    services.shutdown_store_writers().await;
    assert_eq!(
        services
            .settings_get("notifications.volume".into())
            .await
            .unwrap()["value"],
        json!(0.75)
    );
    bus.shutdown().await.unwrap();
    services.store.close().await;
}

#[tokio::test]
async fn cancelling_observer_after_pending_poll_preserves_actual_handle() {
    let secrets = AsyncSecretStore::new(Arc::new(InMemorySecretStore::default())).operation();
    let (admit_completion, held_completion) = tokio::sync::oneshot::channel();
    *secrets.completion_start.lock().unwrap() = Some(held_completion);
    let (first_polled, first_seen) = tokio::sync::oneshot::channel();
    *secrets.pending_polled.lock().unwrap() = Some(first_polled);
    let (release, held) = std::sync::mpsc::channel();
    let writer = secrets.clone();
    let request = tokio::spawn(async move {
        writer
            .mutate("linear.token", move || {
                let _ = held.recv();
                Err(Error::InvalidParams("same actual result".into()))
            })
            .await
    });
    timeout(Duration::from_secs(5), first_seen)
        .await
        .unwrap()
        .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let record = secrets.state.lock().unwrap().mutations[0].clone();
    let (observer_polled, observer_seen) = tokio::sync::oneshot::channel();
    *secrets.pending_polled.lock().unwrap() = Some(observer_polled);
    let observer = secrets.clone();
    let observer = tokio::spawn(async move { observer.settle_detached("linear.token").await });
    // The signal comes AFTER JoinHandle::poll returned Pending, with the
    // record lock held. This is not cancellation of a record-lock waiter.
    timeout(Duration::from_secs(5), observer_seen)
        .await
        .unwrap()
        .unwrap();
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    assert!(matches!(
        *record.state.lock().await,
        MutationState::Pending(_)
    ));
    admit_completion.send(()).unwrap();
    let drain = secrets.shutdown_mutations();
    tokio::pin!(drain);
    tokio::select! {
        biased;
        () = &mut drain => panic!("cancelled polling observer lost actual handle"),
        () = std::future::ready(()) => {}
    }
    release.send(()).unwrap();
    timeout(Duration::from_secs(5), drain).await.unwrap();
    assert!(
        matches!(secrets.mutation_result(&record).await, Err(Error::InvalidParams(message)) if message == "same actual result")
    );
    secrets.finish_operation().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provisional_secret_is_not_executable_before_admission() {
    let secrets = AsyncSecretStore::new(Arc::new(InMemorySecretStore::default()));
    let (constructed, reached) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    *secrets.before_mutation_admission.lock().unwrap() = Some(Box::new(move || {
        let _ = constructed.send(());
        let _ = held.recv();
    }));
    let effects = Arc::new(AtomicUsize::new(0));
    let invoked = effects.clone();
    let writer = secrets.clone();
    let write = tokio::spawn(async move {
        writer
            .mutate("linear.token", move || {
                invoked.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
    });
    reached.await.unwrap();
    let observer_can_snapshot = secrets.state.try_lock().is_ok();
    let observer = secrets.clone();
    let mut observer = tokio::spawn(async move { observer.settle_detached("linear.token").await });
    // If publication is not serialized with admission, force the competing
    // observer all the way through before allowing registration to proceed.
    // Otherwise it cannot snapshot the private record until admission ends.
    if observer_can_snapshot {
        timeout(Duration::from_secs(5), &mut observer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    secrets.mutation_tasks.close();
    release.send(()).unwrap();
    assert!(write.await.unwrap().is_err());
    if !observer_can_snapshot {
        timeout(Duration::from_secs(5), observer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    secrets.shutdown_mutations().await;
    assert_eq!(
        effects.load(Ordering::SeqCst),
        0,
        "a provisional observer started a refused backend write"
    );
    assert!(
        secrets.state.lock().unwrap().mutations.is_empty(),
        "refused registration leaked a receipt"
    );
}

struct HeldBackend {
    inner: InMemorySecretStore,
    entered: tokio::sync::Notify,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

struct HeldDirectBackend {
    inner: InMemorySecretStore,
    account: &'static str,
    delete: bool,
    outcome: usize,
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl HeldDirectBackend {
    fn wait_first(&self, account: &str, delete: bool) -> Result<()> {
        if account == self.account
            && delete == self.delete
            && self.calls.fetch_add(1, Ordering::SeqCst) == 0
        {
            self.entered.notify_one();
            let _ = self.release.lock().unwrap().recv();
            match self.outcome {
                1 => {
                    return Err(Error::InvalidParams(
                        "controlled late backend failure".into(),
                    ))
                }
                2 => panic!("controlled late backend panic"),
                _ => {}
            }
        }
        Ok(())
    }
}

impl SecretStore for HeldDirectBackend {
    fn load(&self, account: &str) -> Result<Option<String>> {
        self.inner.load(account)
    }
    fn store(&self, account: &str, value: &str) -> Result<()> {
        self.wait_first(account, false)?;
        self.inner.store(account, value)
    }
    fn delete(&self, account: &str) -> Result<()> {
        self.wait_first(account, true)?;
        self.inner.delete(account)
    }
}

#[intent_test_macros::daemon_test]
async fn direct_worker_panic_retains_live_mutation_and_guard() {
    use std::future::{poll_fn, Future};
    use std::task::Poll;
    for github in [true, false] {
        let (dir, services, bus) = harness().await;
        let (release, held) = std::sync::mpsc::channel();
        let backend = Arc::new(HeldDirectBackend {
            inner: InMemorySecretStore::default(),
            account: if github {
                crate::github_auth_ops::SECRET_ACCOUNT
            } else {
                "mcp.servers"
            },
            delete: github,
            outcome: 0,
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: Mutex::new(held),
        });
        let marker = crate::source_control_auth_ops::GITHUB_TOKEN_METHOD_ACCOUNT;
        if github {
            backend.inner.store(backend.account, "prior-token").unwrap();
            backend.inner.store(marker, "device").unwrap();
        } else {
            backend
                .inner
                .store(
                    backend.account,
                    &json!({"held":{"id":"held","command":"unused-test-command","enabled":true}})
                        .to_string(),
                )
                .unwrap();
        }
        let mut services = services.with_secret_store(backend.clone());
        services.github_login_base_uri = Some("http://127.0.0.1:1".into());
        let (fail, failure) = tokio::sync::oneshot::channel();
        *services.secrets.panic_mutation_caller.lock().unwrap() = Some(failure);
        let worker = services.clone();
        let request = intent_core::spawn_daemon(async move {
            if github {
                worker.github_revoke().await
            } else {
                worker.mcp_servers_toggle("held".into(), false, None).await
            }
        });
        timeout(Duration::from_secs(5), backend.entered.notified())
            .await
            .unwrap();
        fail.send(()).unwrap();
        let error = timeout(Duration::from_secs(5), request)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("state may be unknown"));
        if github {
            assert!(services.secrets.github_mutation.try_lock().is_err());
        }
        let account = services.settings_secret_gates.write(&["mcp.servers"]);
        tokio::pin!(account);
        if !github {
            assert!(
                poll_fn(|cx| Poll::Ready(account.as_mut().poll(cx).is_pending())).await,
                "worker panic released MCP ownership before physical completion"
            );
        }
        let drain = services.shutdown_store_writers();
        tokio::pin!(drain);
        assert!(poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx).is_pending())).await);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        release.send(()).unwrap();
        timeout(Duration::from_secs(5), drain).await.unwrap();
        if github {
            assert!(services.secrets.github_mutation.try_lock().is_ok());
            assert_eq!(services.secrets.load(backend.account).await.unwrap(), None);
            assert_eq!(
                backend.inner.load(marker).unwrap().as_deref(),
                Some("device"),
                "unknown operation must not claim completed reconciliation"
            );
        } else {
            drop(timeout(Duration::from_secs(5), account).await.unwrap());
            let actual: Value = serde_json::from_str(
                &services
                    .secrets
                    .load(backend.account)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(actual["held"]["enabled"], false);
            assert_eq!(
                services
                    .settings_get("mcp.disabledServers".into())
                    .await
                    .unwrap()["value"],
                json!([])
            );
        }
        assert!(services.secrets.state.lock().unwrap().mutations.is_empty());
        bus.shutdown().await.unwrap();
        services.store.close().await;
        let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
        assert!(!reopened
            .query_events(&intent_store::EventQuery::default())
            .await
            .unwrap()
            .iter()
            .any(|event| event.event_type == "settings:changed"
                || (event.event_type == "github:auth-changed"
                    && event.data["status"] == "revoked")));
        reopened.close().await;
    }
}

#[intent_test_macros::daemon_test]
async fn direct_endpoints_reject_unprivileged_callers_before_admission() {
    let (_dir, services, bus) = harness().await;
    let secrets = Arc::new(InMemorySecretStore::default());
    let config = json!({"id":"existing","transport":"stdio","command":"unused-test-command","enabled":false});
    let catalog = json!({"existing":config}).to_string();
    secrets.store("mcp.servers", &catalog).unwrap();
    secrets
        .store(crate::github_auth_ops::SECRET_ACCOUNT, "prior-token")
        .unwrap();
    let services = services.with_secret_store(secrets.clone());
    let member = Caller::Wire {
        principal_id: PrincipalId::new(),
        host_role: HostRole::Member,
    };
    intent_core::with_caller(member, async {
        let results = [
            services.github_revoke().await,
            services.mcp_servers_create(json!({"id":"new","transport":"stdio","command":"unused-test-command","enabled":false})).await,
            services.mcp_servers_update("existing".into(), config.clone()).await,
            services.mcp_servers_delete("existing".into()).await,
            services.mcp_servers_toggle("existing".into(), false, None).await,
            services.mcp_servers_restart("existing".into()).await,
        ];
        for result in results { assert!(matches!(result, Err(Error::Forbidden(_))), "{result:?}"); }
    }).await;
    assert_eq!(secrets.load("mcp.servers").unwrap(), Some(catalog));
    assert_eq!(
        secrets
            .load(crate::github_auth_ops::SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("prior-token")
    );
    assert_eq!(
        services.secrets.state.lock().unwrap().next_owner,
        0,
        "authorization must precede operation registration"
    );
    services.shutdown_store_writers().await;
    bus.shutdown().await.unwrap();
    services.store.close().await;
}

#[intent_test_macros::daemon_test]
async fn direct_workspace_toggle_preserves_manager_scope() {
    use intent_core::{now_iso, Principal, WorkspaceId, WorkspaceRole};
    let (dir, services, bus) = harness().await;
    let secrets = Arc::new(InMemorySecretStore::default());
    let catalog = json!({"existing":{"id":"existing","transport":"stdio","command":"unused-test-command","enabled":true}}).to_string();
    secrets.store("mcp.servers", &catalog).unwrap();
    let services = services.with_secret_store(secrets.clone());
    let target = WorkspaceId::new();
    let other = WorkspaceId::new();
    for id in [&target, &other] {
        services
            .store
            .insert_workspace(&crate::tests::workspace(id))
            .await
            .unwrap();
    }
    let primary = services.store.get_primary_principal().await.unwrap().id;
    let owner = PrincipalId::new();
    let collaborator = PrincipalId::new();
    let outsider = PrincipalId::new();
    for id in [&owner, &collaborator, &outsider] {
        services
            .store
            .upsert_principal(&Principal {
                id: id.clone(),
                identity: None,
                github_user_id: None,
                login: None,
                display_name: None,
                avatar_url: None,
                is_primary: false,
                created_at: now_iso(),
                updated_at: now_iso(),
            })
            .await
            .unwrap();
    }
    services
        .store
        .set_workspace_member_role(&target, &primary, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    services
        .store
        .add_workspace_member(&target, &owner, WorkspaceRole::Owner)
        .await
        .unwrap();
    services
        .store
        .add_workspace_member(&target, &collaborator, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let caller = |id: &PrincipalId| Caller::Wire {
        principal_id: id.clone(),
        host_role: HostRole::Guest,
    };
    let denied = intent_core::with_caller(
        caller(&collaborator),
        services.mcp_servers_toggle("existing".into(), false, Some(target.clone())),
    )
    .await;
    assert!(matches!(denied, Err(Error::Forbidden(_))), "{denied:?}");
    let hidden = intent_core::with_caller(
        caller(&outsider),
        services.mcp_servers_toggle("existing".into(), false, Some(target.clone())),
    )
    .await;
    assert!(matches!(hidden, Err(Error::NotFound(_))), "{hidden:?}");
    assert_eq!(services.secrets.state.lock().unwrap().next_owner, 0);
    let allowed = intent_core::with_caller(
        caller(&owner),
        services.mcp_servers_toggle("existing".into(), false, Some(target.clone())),
    )
    .await
    .unwrap();
    assert_eq!(allowed["workspaceDisabled"], true);
    let cross = intent_core::with_caller(
        caller(&owner),
        services.mcp_servers_toggle("existing".into(), false, Some(other.clone())),
    )
    .await;
    assert!(matches!(cross, Err(Error::NotFound(_))), "{cross:?}");
    assert_eq!(
        services
            .store
            .workspace_mcp_disabled_servers(&target)
            .await
            .unwrap(),
        vec!["existing".to_string()]
    );
    assert!(services
        .store
        .workspace_mcp_disabled_servers(&other)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(secrets.load("mcp.servers").unwrap(), Some(catalog));
    assert_eq!(
        services
            .settings_get("mcp.disabledServers".into())
            .await
            .unwrap()["value"],
        json!([])
    );
    services.shutdown_store_writers().await;
    bus.shutdown().await.unwrap();
    services.store.close().await;
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    let events = reopened
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap();
    let toggled: Vec<_> = events
        .iter()
        .filter(|e| {
            e.event_type == "workspace:updated"
                && e.data["changes"].get("mcpServerToggled").is_some()
        })
        .collect();
    assert_eq!(toggled.len(), 1, "{events:?}");
    assert_eq!(toggled[0].workspace_id, target);
    reopened.close().await;
}

#[intent_test_macros::daemon_test]
async fn direct_secret_timeout_retains_actual_outcome_and_refuses_after_close() {
    for github in [true, false] {
        for outcome in 0..3 {
            let (dir, services, bus) = harness().await;
            let (release, held) = std::sync::mpsc::channel();
            let backend = Arc::new(HeldDirectBackend {
                inner: InMemorySecretStore::default(),
                account: if github {
                    crate::github_auth_ops::SECRET_ACCOUNT
                } else {
                    "mcp.servers"
                },
                delete: github,
                outcome,
                calls: AtomicUsize::new(0),
                entered: tokio::sync::Notify::new(),
                release: Mutex::new(held),
            });
            let marker = crate::source_control_auth_ops::GITHUB_TOKEN_METHOD_ACCOUNT;
            if github {
                backend.inner.store(backend.account, "prior-token").unwrap();
                backend.inner.store(marker, "device").unwrap();
            } else {
                backend.inner.store(backend.account, &json!({"held":{"id":"held","command":"unused-test-command","enabled":true}}).to_string()).unwrap();
            }
            let mut services = services.with_secret_store(backend.clone());
            services.github_login_base_uri = Some("http://127.0.0.1:1".into());
            Arc::get_mut(&mut services.secrets).unwrap().write_timeout = Duration::from_millis(10);
            let writer = services.clone();
            let response = intent_core::spawn_daemon(async move {
                if github {
                    writer.github_revoke().await
                } else {
                    writer.mcp_servers_toggle("held".into(), false, None).await
                }
            });
            timeout(Duration::from_secs(5), backend.entered.notified())
                .await
                .unwrap();
            let error = timeout(Duration::from_secs(5), response)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert!(error.to_string().contains("state may be unknown"));
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            assert!(services
                .secrets
                .state
                .lock()
                .unwrap()
                .mutations
                .iter()
                .all(|record| record.owner.is_some()));
            if github {
                assert!(
                    services.secrets.github_mutation.try_lock().is_err(),
                    "timeout released credential ownership"
                );
            }
            let drain = services.shutdown_store_writers();
            tokio::pin!(drain);
            tokio::select! {
                biased;
                () = &mut drain => panic!("timeout let shutdown pass a live mutation"),
                () = std::future::ready(()) => {}
            }
            release.send(()).unwrap();
            timeout(Duration::from_secs(5), drain).await.unwrap();
            assert!(services.secrets.state.lock().unwrap().mutations.is_empty());
            if github {
                let expected = if outcome == 0 {
                    None
                } else {
                    Some("device".into())
                };
                assert_eq!(backend.inner.load(marker).unwrap(), expected);
                assert_eq!(
                    services.secrets.load(backend.account).await.unwrap(),
                    if outcome == 0 {
                        None
                    } else {
                        Some("prior-token".into())
                    }
                );
                assert!(services
                    .github_revoke()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("shutting down"));
            } else {
                assert_eq!(
                    services
                        .settings_get("mcp.disabledServers".into())
                        .await
                        .unwrap()["value"],
                    if outcome == 0 {
                        json!(["held"])
                    } else {
                        json!([])
                    }
                );
                let config: Value = serde_json::from_str(
                    &services
                        .secrets
                        .load(backend.account)
                        .await
                        .unwrap()
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(config["held"]["enabled"], outcome != 0);
                assert!(services
                    .mcp_servers_toggle("held".into(), true, None)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("shutting down"));
            }
            assert_eq!(
                backend.calls.load(Ordering::SeqCst),
                1,
                "refused root invoked secret backend"
            );
            bus.shutdown().await.unwrap();
            services.store.close().await;
            let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
            let revoked = reopened
                .query_events(&intent_store::EventQuery::default())
                .await
                .unwrap()
                .iter()
                .any(|event| {
                    event.event_type == "github:auth-changed" && event.data["status"] == "revoked"
                });
            assert_eq!(
                revoked,
                github && outcome == 0,
                "late failure/panic must not publish revoke success"
            );
            reopened.close().await;
        }
    }
}

#[intent_test_macros::daemon_test]
async fn direct_revoke_timeout_serializes_newer_settings_owner() {
    use std::future::{poll_fn, Future};
    use std::task::Poll;
    let (_dir, services, bus) = harness().await;
    let (release, held) = std::sync::mpsc::channel();
    let backend = Arc::new(HeldDirectBackend {
        inner: InMemorySecretStore::default(),
        account: crate::github_auth_ops::SECRET_ACCOUNT,
        delete: true,
        outcome: 0,
        calls: AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
        release: Mutex::new(held),
    });
    backend.inner.store(backend.account, "old").unwrap();
    let mut services = services.with_secret_store(backend.clone());
    services.github_login_base_uri = Some("http://127.0.0.1:1".into());
    Arc::get_mut(&mut services.secrets).unwrap().write_timeout = Duration::from_millis(10);
    let writer = services.clone();
    let response = intent_core::spawn_daemon(async move { writer.github_revoke().await });
    timeout(Duration::from_secs(5), backend.entered.notified())
        .await
        .unwrap();
    assert!(response
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("unknown"));
    let (blocked, observed_blocked) = tokio::sync::oneshot::channel();
    *services.secrets.github_lock_pending.lock().unwrap() = Some(blocked);
    let newer =
        services.settings_update(json!([{"path":"sourceControl.github.token","value":"newer"}]));
    tokio::pin!(newer);
    assert!(poll_fn(|cx| Poll::Ready(newer.as_mut().poll(cx).is_pending())).await);
    timeout(Duration::from_secs(5), observed_blocked)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        backend.calls.load(Ordering::SeqCst),
        1,
        "newer owner must not touch backend while old revoke holds credential lock"
    );
    assert_eq!(
        backend.inner.load(backend.account).unwrap().as_deref(),
        Some("old")
    );
    let drain = services.shutdown_store_writers();
    tokio::pin!(drain);
    tokio::select! {
        biased;
        () = &mut drain => panic!("shutdown passed retained revoke"),
        () = std::future::ready(()) => {}
    }
    release.send(()).unwrap();
    timeout(Duration::from_secs(5), &mut newer)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(5), drain).await.unwrap();
    assert_eq!(
        services
            .secrets
            .load(backend.account)
            .await
            .unwrap()
            .as_deref(),
        Some("newer")
    );
    bus.shutdown().await.unwrap();
    services.store.close().await;
}

#[intent_test_macros::daemon_test]
async fn direct_github_revoke_finishes_after_caller_cancel() {
    let (dir, services, bus) = harness().await;
    let (release, held) = std::sync::mpsc::channel();
    let backend = Arc::new(HeldDirectBackend {
        inner: InMemorySecretStore::default(),
        account: crate::github_auth_ops::SECRET_ACCOUNT,
        delete: true,
        outcome: 0,
        calls: AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
        release: Mutex::new(held),
    });
    backend.inner.store(backend.account, "test-token").unwrap();
    let marker = crate::source_control_auth_ops::GITHUB_TOKEN_METHOD_ACCOUNT;
    backend.inner.store(marker, "device").unwrap();
    let mut services = services.with_secret_store(backend.clone());
    services.github_login_base_uri = Some("http://127.0.0.1:1".into());
    let writer = services.clone();
    let response = intent_core::spawn_daemon(async move { writer.github_revoke().await });
    timeout(Duration::from_secs(5), backend.entered.notified())
        .await
        .unwrap();
    response.abort();
    assert!(response.await.unwrap_err().is_cancelled());
    let drain = services.shutdown_store_writers();
    tokio::pin!(drain);
    tokio::select! {
        biased;
        () = &mut drain => panic!("shutdown lost direct revoke's physical write"),
        () = std::future::ready(()) => {}
    }
    release.send(()).unwrap();
    timeout(Duration::from_secs(5), drain).await.unwrap();
    bus.shutdown().await.unwrap();
    services.store.close().await;
    assert_eq!(backend.inner.load(backend.account).unwrap(), None);
    assert_eq!(
        backend.inner.load(marker).unwrap(),
        None,
        "cancelled revoke abandoned provenance deletion"
    );
    assert!(
        services.secrets.state.lock().unwrap().mutations.is_empty(),
        "direct revoke left an unacknowledged shared-owner receipt"
    );
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    assert!(reopened
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap()
        .iter()
        .any(
            |event| event.event_type == "github:auth-changed" && event.data["status"] == "revoked"
        ));
    reopened.close().await;
}

#[intent_test_macros::daemon_test]
async fn direct_mcp_toggle_finishes_after_caller_cancel() {
    let (_dir, services, bus) = harness().await;
    let (release, held) = std::sync::mpsc::channel();
    let backend = Arc::new(HeldDirectBackend {
        inner: InMemorySecretStore::default(),
        account: "mcp.servers",
        delete: false,
        outcome: 0,
        calls: AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
        release: Mutex::new(held),
    });
    backend
        .inner
        .store(
            "mcp.servers",
            &json!({"held":{"id":"held","command":"unused-test-command","enabled":true}})
                .to_string(),
        )
        .unwrap();
    let services = services.with_secret_store(backend.clone());
    let writer = services.clone();
    let response = intent_core::spawn_daemon(async move {
        writer.mcp_servers_toggle("held".into(), false, None).await
    });
    timeout(Duration::from_secs(5), backend.entered.notified())
        .await
        .unwrap();
    response.abort();
    assert!(response.await.unwrap_err().is_cancelled());
    let drain = services.shutdown_store_writers();
    tokio::pin!(drain);
    tokio::select! {
        biased;
        () = &mut drain => panic!("shutdown lost direct MCP physical write"),
        () = std::future::ready(()) => {}
    }
    release.send(()).unwrap();
    timeout(Duration::from_secs(5), drain).await.unwrap();
    assert_eq!(
        services
            .settings_get("mcp.disabledServers".into())
            .await
            .unwrap()["value"],
        json!(["held"]),
        "cancelled MCP toggle abandoned settings continuation"
    );
    assert!(
        services.secrets.state.lock().unwrap().mutations.is_empty(),
        "direct MCP left an unacknowledged shared-owner receipt"
    );
    bus.shutdown().await.unwrap();
    services.store.close().await;
}

impl SecretStore for HeldBackend {
    fn load(&self, account: &str) -> Result<Option<String>> {
        self.inner.load(account)
    }
    fn store(&self, account: &str, value: &str) -> Result<()> {
        self.entered.notify_one();
        let _ = self.release.lock().unwrap().recv();
        self.inner.store(account, value)
    }
    fn delete(&self, account: &str) -> Result<()> {
        self.inner.delete(account)
    }
}

#[intent_test_macros::daemon_test]
async fn settings_worker_panic_retains_live_write_and_account_guards() {
    let (dir, services, bus) = harness().await;
    let (release, held) = std::sync::mpsc::channel();
    let backend = Arc::new(HeldBackend {
        inner: InMemorySecretStore::default(),
        entered: tokio::sync::Notify::new(),
        release: Mutex::new(held),
    });
    let services = services.with_secret_store(backend.clone());
    let (fail, failure) = tokio::sync::oneshot::channel();
    *services.secrets.panic_mutation_caller.lock().unwrap() = Some(failure);
    let prior = services
        .settings_get("notifications.volume".into())
        .await
        .unwrap()["value"]
        .clone();
    let writer = services.clone();
    let request = intent_core::spawn_daemon(async move {
        writer
            .settings_update(json!([
                {"path":"sourceControl.gitlab.token","value":"surviving-write"},
                {"path":"notifications.volume","value":0.75}
            ]))
            .await
    });
    timeout(Duration::from_secs(5), backend.entered.notified())
        .await
        .unwrap();
    fail.send(()).unwrap();
    let error = timeout(Duration::from_secs(5), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("state may be unknown"));
    assert!(
        services.gitlab_credential_gate.try_lock().is_err(),
        "panic released primary ownership before physical completion"
    );
    let accounts = services
        .settings_secret_gates
        .write(&["sourceControl.gitlab.token"]);
    let drain = services.shutdown_store_writers();
    tokio::pin!(accounts, drain);
    tokio::select! {
        biased;
        _ = &mut accounts => panic!("panic released account ownership before physical completion"),
        () = &mut drain => panic!("shutdown passed the panicked worker's live mutation"),
        () = std::future::ready(()) => {}
    }
    assert_eq!(
        backend.inner.load("sourceControl.gitlab.token").unwrap(),
        None
    );
    release.send(()).unwrap();
    timeout(Duration::from_secs(5), &mut drain).await.unwrap();
    drop(timeout(Duration::from_secs(5), accounts).await.unwrap());
    assert!(services.gitlab_credential_gate.try_lock().is_ok());
    assert!(services.secrets.state.lock().unwrap().mutations.is_empty());
    assert_eq!(
        services
            .secrets
            .load("sourceControl.gitlab.token")
            .await
            .unwrap()
            .as_deref(),
        Some("surviving-write")
    );
    assert_eq!(
        services
            .settings_get("notifications.volume".into())
            .await
            .unwrap()["value"],
        prior
    );
    bus.shutdown().await.unwrap();
    services.store.close().await;
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    assert!(
        !reopened
            .query_events(&intent_store::EventQuery::default())
            .await
            .unwrap()
            .iter()
            .any(|event| event.event_type == "settings:changed"),
        "worker panic must not publish batch success"
    );
    reopened.close().await;
}

#[intent_test_macros::daemon_test]
async fn settings_owner_recovers_captured_result_after_completion_observer_panics() {
    let (dir, services, bus) = harness().await;
    let (release, held) = std::sync::mpsc::channel();
    let backend = Arc::new(HeldBackend {
        inner: InMemorySecretStore::default(),
        entered: tokio::sync::Notify::new(),
        release: Mutex::new(held),
    });
    let mut services = services.with_secret_store(backend.clone());
    let secrets = Arc::get_mut(&mut services.secrets).unwrap();
    secrets.write_timeout = Duration::from_millis(10);
    secrets.settle_timeout = Duration::from_millis(10);
    let prior = services
        .settings_get("notifications.volume".into())
        .await
        .unwrap()["value"]
        .clone();
    let writer = services.clone();
    let request = intent_core::spawn_daemon(async move {
        writer
            .settings_update(json!([
                {"path":"sourceControl.gitlab.token","value":"late-success"},
                {"path":"notifications.volume","value":0.75}
            ]))
            .await
    });
    timeout(Duration::from_secs(5), backend.entered.notified())
        .await
        .unwrap();
    let error = timeout(Duration::from_secs(5), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("unknown"));
    let record = services.secrets.state.lock().unwrap().mutations[0].clone();
    let (captured, observed) = tokio::sync::oneshot::channel();
    *record.after_capture.lock().unwrap() = Some(Box::new(move || {
        let _ = captured.send(());
        panic!("injected observer failure after capturing actual result");
    }));
    let drain = services.shutdown_store_writers();
    tokio::pin!(drain);
    tokio::select! {
        biased;
        () = &mut drain => panic!("unknown response lost actual write ownership"),
        () = std::future::ready(()) => {}
    }
    release.send(()).unwrap();
    timeout(Duration::from_secs(5), drain).await.unwrap();
    observed.await.unwrap();
    assert!(matches!(
        *record.state.lock().await,
        MutationState::Finalized { .. }
    ));
    assert_eq!(
        backend
            .inner
            .load("sourceControl.gitlab.token")
            .unwrap()
            .as_deref(),
        Some("late-success")
    );
    assert_eq!(
        services
            .secrets
            .load("sourceControl.gitlab.token")
            .await
            .unwrap()
            .as_deref(),
        Some("late-success")
    );
    assert_eq!(
        services
            .settings_get("notifications.volume".into())
            .await
            .unwrap()["value"],
        prior,
        "unknown outcome must not resume the skipped ordinary batch"
    );
    bus.shutdown().await.unwrap();
    services.store.close().await;
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    assert!(
        !reopened
            .query_events(&intent_store::EventQuery::default())
            .await
            .unwrap()
            .iter()
            .any(|event| event.event_type == "settings:changed"),
        "no obsolete success publication"
    );
    reopened.close().await;
}

struct HeldHook {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    hold: AtomicBool,
    starts: AtomicUsize,
    running: AtomicBool,
    seen: Mutex<Vec<(Option<Caller>, Option<PrincipalId>)>>,
}

impl HeldHook {
    fn new(hold: bool) -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            hold: AtomicBool::new(hold),
            starts: AtomicUsize::new(0),
            running: AtomicBool::new(true),
            seen: Mutex::new(Vec::new()),
        }
    }
}

impl ServerControl for HeldHook {
    fn start_ws_listener(&self) -> intent_core::BoxFuture<'_, Result<u16>> {
        Box::pin(async move {
            self.seen.lock().unwrap().push((
                intent_core::current_caller(),
                intent_core::caller::current_wire_credential()
                    .map(|credential| credential.principal_id().clone()),
            ));
            if self.hold.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.starts.fetch_add(1, Ordering::SeqCst);
            self.running.store(true, Ordering::SeqCst);
            Ok(5181)
        })
    }

    fn stop_ws_listener(&self) -> intent_core::BoxFuture<'_, ()> {
        Box::pin(async move { self.running.store(false, Ordering::SeqCst) })
    }

    fn ws_listener_port(&self) -> intent_core::BoxFuture<'_, Option<u16>> {
        Box::pin(async move { self.running.load(Ordering::SeqCst).then_some(5181) })
    }

    fn is_tcp_connection(&self) -> bool {
        TEST_TCP.try_with(|tcp| *tcp).unwrap_or(true)
    }
}

async fn harness() -> (tempfile::TempDir, crate::Services, crate::EventBus) {
    let dir = crate::test_support::test_tempdir("owned-settings-shutdown");
    let store = Store::open(&dir.path().join("state.db")).await.unwrap();
    let bus = crate::EventBus::new(store.clone());
    let registry = Arc::new(SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    let services = crate::Services::new_with_file_secrets(
        store,
        intent_core::FileSecretStore::with_path(dir.path().join("constructor-secrets.json")),
    )
    .with_settings_registry(registry)
    .with_secret_store(Arc::new(InMemorySecretStore::default()))
    .with_event_bus(bus.clone());
    (dir, services, bus)
}

#[intent_test_macros::daemon_test]
async fn cancelled_hook_owner_finishes_before_final_listener_stop() {
    let (dir, services, bus) = harness().await;
    let control = Arc::new(HeldHook::new(true));
    services.attach_server_control(control.clone());
    let writer = services.clone();
    let caller = intent_core::spawn_daemon(TEST_TCP.scope(false, async move {
        writer
            .settings_update(json!([{"path":"server.wsApi.enabled","value":true}]))
            .await
    }));
    timeout(Duration::from_secs(5), control.entered.notified())
        .await
        .unwrap();
    caller.abort();
    let _ = caller.await;
    services.begin_settings_shutdown();
    let first = services.shutdown_settings();
    let second = services.shutdown_settings();
    tokio::pin!(first, second);
    tokio::select! {
        biased;
        () = &mut first => panic!("first drain passed the held runtime hook"),
        () = &mut second => panic!("concurrent drain passed the held runtime hook"),
        () = std::future::ready(()) => {}
    }
    control.release.notify_one();
    timeout(Duration::from_secs(5), async {
        tokio::join!(first, second);
    })
    .await
    .unwrap();
    // The same composition dependency as main: early settings join, then the
    // final listener stop, then durable tails/event bus/store closure.
    control.stop_ws_listener().await;
    assert!(services
        .settings_update(json!([{"path":"server.wsApi.enabled","value":true}]))
        .await
        .is_err());
    services.shutdown_store_writers().await;
    bus.shutdown().await.unwrap();
    services.store.close().await;
    assert_eq!(control.starts.load(Ordering::SeqCst), 1);
    assert!(!control.running.load(Ordering::SeqCst));
    let reopened = Store::open(&dir.path().join("state.db")).await.unwrap();
    let events = reopened
        .query_events(&intent_store::EventQuery::default())
        .await
        .unwrap();
    assert!(events
        .iter()
        .any(|event| event.event_type == "settings:changed"
            && event.data["changes"]
                .as_array()
                .is_some_and(|changes| changes
                    .iter()
                    .any(|change| change["path"] == "server.wsApi.enabled"
                        && change["value"] == true))));
    reopened.close().await;
}

#[intent_test_macros::daemon_test]
async fn settings_owner_preserves_caller_credential_and_local_origin() {
    let (_dir, services, bus) = harness().await;
    let control = Arc::new(HeldHook::new(false));
    services.attach_server_control(control.clone());
    let principal = PrincipalId::new();
    let caller = Caller::Wire {
        principal_id: principal.clone(),
        host_role: HostRole::Owner,
    };
    let credential = intent_core::caller::WireCredential::Principal {
        principal_id: principal.clone(),
        token_hash: "test-only-provenance".into(),
    };
    let result = intent_core::with_caller(
        caller.clone(),
        intent_core::caller::with_wire_credential(
            Some(credential),
            TEST_TCP.scope(
                false,
                services.settings_update(json!([{"path":"server.tunnel.only","value":true}])),
            ),
        ),
    )
    .await;
    result.unwrap();
    assert_eq!(
        *control.seen.lock().unwrap(),
        vec![(Some(caller), Some(principal))]
    );
    services
        .settings_update(json!([{"path":"server.wsApi.enabled","value":true}]))
        .await
        .unwrap();
    // Missing transport context must retain the existing remote default.
    let error = services
        .settings_update(json!([{"path":"server.wsApi.enabled","value":false}]))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidParams(_)));
    let rejected = intent_core::with_caller(
        Caller::Wire {
            principal_id: PrincipalId::new(),
            host_role: HostRole::Member,
        },
        TEST_TCP.scope(
            false,
            services.settings_update(json!([{"path":"git.autoCommit","value":true}])),
        ),
    )
    .await;
    assert!(rejected.is_err());
    services.shutdown_store_writers().await;
    bus.shutdown().await.unwrap();
    services.store.close().await;
}

#[tokio::test]
async fn cancelled_settlement_observer_leaves_original_receipt_owned() {
    let secrets = AsyncSecretStore::with_timings(
        Arc::new(InMemorySecretStore::default()),
        Duration::from_secs(1),
        Duration::from_millis(10),
        Duration::from_secs(60),
        Duration::from_secs(60),
    )
    .operation();
    let (entered, began) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    let writer = secrets.clone();
    let request = tokio::spawn(async move {
        writer
            .mutate("linear.token", move || {
                let _ = entered.send(());
                let _ = held.recv();
                Err(Error::InvalidParams("late receipt result".into()))
            })
            .await
    });
    began.await.unwrap();
    assert!(request.await.unwrap().is_err());
    let record = secrets.state.lock().unwrap().mutations[0].clone();
    let observer = secrets.clone();
    let observer = tokio::spawn(async move { observer.settle_detached("linear.token").await });
    timeout(Duration::from_secs(5), async {
        while record.settling.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    assert_eq!(record.settling.load(Ordering::SeqCst), 0);
    let drain = secrets.shutdown_mutations();
    tokio::pin!(drain);
    tokio::select! {
        biased;
        () = &mut drain => panic!("observer cancellation lost the held write"),
        () = std::future::ready(()) => {}
    }
    release.send(()).unwrap();
    timeout(Duration::from_secs(5), drain).await.unwrap();
    assert!(secrets.settle_detached("linear.token").await.unwrap());
    assert!(matches!(
        secrets.mutation_result(&record).await,
        Err(Error::InvalidParams(_))
    ));
    secrets.finish_operation().await;
    assert!(secrets.state.lock().unwrap().mutations.is_empty());
}

#[tokio::test]
async fn completed_secret_receipt_survives_other_observers_and_preserves_error() {
    for outcome in 0..3 {
        let secrets = AsyncSecretStore::with_timings(
            Arc::new(InMemorySecretStore::default()),
            Duration::from_secs(1),
            Duration::from_millis(10),
            Duration::from_secs(60),
            Duration::from_secs(60),
        )
        .operation();
        let (entered, began) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        let writer = secrets.clone();
        let request = tokio::spawn(async move {
            writer
                .mutate("linear.token", move || {
                    let _ = entered.send(());
                    let _ = held.recv();
                    match outcome {
                        0 => Ok(()),
                        1 => Err(Error::InvalidParams("late typed failure".into())),
                        _ => panic!("late backend panic"),
                    }
                })
                .await
        });
        began.await.unwrap();
        assert!(request.await.unwrap().is_err());
        let record = secrets.state.lock().unwrap().mutations[0].clone();
        let shutdown = secrets.shutdown_mutations();
        tokio::pin!(shutdown);
        tokio::select! {
            biased;
            () = &mut shutdown => panic!("write fence passed a held backend"),
            () = std::future::ready(()) => {}
        }
        release.send(()).unwrap();
        timeout(Duration::from_secs(5), &mut shutdown)
            .await
            .unwrap();
        assert!(secrets.settle_detached("linear.token").await.unwrap());
        assert!(secrets
            .clone()
            .settle_detached("linear.token")
            .await
            .unwrap());
        assert_eq!(
            secrets.state.lock().unwrap().mutations.len(),
            1,
            "observers must not retire the originating receipt"
        );
        let actual = secrets.mutation_result(&record).await;
        match outcome {
            0 => actual.unwrap(),
            1 => assert!(matches!(actual, Err(Error::InvalidParams(_)))),
            _ => assert!(actual.unwrap_err().to_string().contains("panicked")),
        }
        secrets.finish_operation().await;
        assert!(secrets.state.lock().unwrap().mutations.is_empty());
        let effects = Arc::new(AtomicUsize::new(0));
        let attempted = effects.clone();
        assert!(secrets
            .mutate("linear.token", move || {
                attempted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
            .is_err());
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert!(
            secrets.state.lock().unwrap().mutations.is_empty(),
            "refusal must clean its provisional receipt"
        );
    }
}
