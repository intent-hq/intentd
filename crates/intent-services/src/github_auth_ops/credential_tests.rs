use super::*;
use intent_core::WorkspaceApi;
use intent_sourcecontrol::device_flow::{IdentityGuard, IdentityLease};
use std::future::Future;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

pub(crate) async fn authorize(
    services: &crate::Services,
    raw: intent_core::FileSecretStore,
    identity: IdentityGuard,
) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for body in [
            json!({"device_code":"code", "user_code":"CODE", "verification_uri":"https://github.com/login/device", "expires_in":60, "interval":1}),
            json!({"access_token":"new-device-token", "token_type":"bearer", "scope":"repo"}),
        ] {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some((key, value)) = line.split_once(':') {
                    if key.eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse().unwrap();
                    }
                }
            }
            reader.read_exact(&mut vec![0; length]).await.unwrap();
            let body = body.to_string();
            reader.get_mut().write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
        }
    });
    let (_, flow) = intent_sourcecontrol::device_flow::start_at(&base, "client", &["repo"])
        .await
        .unwrap();
    let flow_id = next_flow_id();
    let deadline = Instant::now() + Duration::from_secs(30);
    let state = services.github_auth_flow.clone();
    *state.lock().await = Some(FlowSlot {
        flow_id,
        user_code: "CODE".into(),
        verification_uri: "https://github.com/login/device".into(),
        interval: 1,
        deadline,
        phase: FlowPhase::Pending,
    });
    let owner = CredentialOwner::new(services.secrets.clone());
    let flow = flow
        .with_store(raw)
        .with_identity_guard(None, owner.identity_guard(identity, state.clone(), flow_id));
    let bus = services.event_bus.clone();
    intent_core::spawn_daemon(async move {
        run_poll_loop(state, bus, owner, flow_id, flow, deadline, false).await;
        server.await.unwrap();
    })
}

pub(crate) fn accept_identity() -> IdentityGuard {
    Arc::new(|_| Box::pin(async { Ok(Box::new(()) as IdentityLease) }))
}

struct ParkLease {
    entered: Arc<tokio::sync::Notify>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Drop for ParkLease {
    fn drop(&mut self) {
        self.entered.notify_one();
        let _ = self.release.lock().unwrap().recv();
    }
}

async fn revoke_or_cancel_before_publication(cancel: bool) {
    let dir = crate::test_support::test_tempdir("github-credential-completion-owner");
    let store = intent_store::Store::open(&dir.path().join("state.db"))
        .await
        .unwrap();
    let raw = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
    let bus = EventBus::new(store.clone());
    let mut events = bus.subscribe(crate::events::SubscriptionFilter {
        event_types: vec![
            GITHUB_AUTH_CHANGED.into(),
            intent_core::events::SOURCE_CONTROL_AUTH_CHANGED.into(),
        ],
        ..Default::default()
    });
    let services = crate::Services::new(store)
        .with_secret_store(Arc::new(raw.clone()))
        .with_event_bus(bus)
        .with_github_login_base_uri("http://127.0.0.1:1");
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release = Arc::new(std::sync::Mutex::new(Some(release_rx)));
    let identity: IdentityGuard = Arc::new({
        let entered = entered.clone();
        move |_| {
            let lease = ParkLease {
                entered: entered.clone(),
                release: std::sync::Mutex::new(release.lock().unwrap().take().unwrap()),
            };
            Box::pin(async move { Ok(Box::new(lease) as IdentityLease) })
        }
    });
    let flow = authorize(&services, raw.clone(), identity).await;
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert_eq!(
        raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
        Some("new-device-token")
    );
    let mut revoke = services.github_revoke();
    if cancel {
        assert_eq!(
            services.github_cancel_auth(None).await.unwrap()["cancelled"],
            true
        );
    } else {
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                revoke.as_mut().poll(cx).is_pending()
            ))
            .await,
            "revocation must wait for the actual persistence lease"
        );
    }
    release_tx.send(()).unwrap();
    if !cancel {
        revoke.await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), flow)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw.load(SECRET_ACCOUNT).unwrap(), None);
    assert_eq!(
        raw.load(crate::source_control_auth_ops::GITHUB_TOKEN_METHOD_ACCOUNT)
            .unwrap(),
        None
    );
    let mut seen = Vec::new();
    loop {
        match std::future::poll_fn(|cx| {
            std::task::Poll::Ready(Box::pin(events.recv()).as_mut().poll(cx))
        })
        .await
        {
            std::task::Poll::Ready(Some(events)) => seen.extend(events),
            std::task::Poll::Pending => break,
            std::task::Poll::Ready(None) => panic!("event subscription closed unexpectedly"),
        }
    }
    assert_eq!(seen.len(), if cancel { 0 } else { 2 });
    assert!(
        seen.iter().all(|event| event.data["status"] == "revoked"),
        "no obsolete authorized notification"
    );
}

#[intent_test_macros::daemon_test]
async fn github_revoke_wins_before_authorization_publication() {
    revoke_or_cancel_before_publication(false).await;
}

#[intent_test_macros::daemon_test]
async fn github_cancel_cleans_its_persisted_grant_without_authorized_notification() {
    revoke_or_cancel_before_publication(true).await;
}

async fn finalizer_ownership_timeout(replace_flow: bool) {
    let dir = crate::test_support::test_tempdir("github-finalizer-timeout");
    let store = intent_store::Store::open(&dir.path().join("state.db"))
        .await
        .unwrap();
    let raw = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
    let bus = EventBus::new(store.clone());
    let mut events = bus.subscribe(crate::events::SubscriptionFilter {
        event_types: vec![
            GITHUB_AUTH_CHANGED.into(),
            intent_core::events::SOURCE_CONTROL_AUTH_CHANGED.into(),
        ],
        ..Default::default()
    });
    let mut services = crate::Services::new(store).with_event_bus(bus);
    services.secrets = Arc::new(
        crate::settings::AsyncSecretStore::new(Arc::new(raw.clone()))
            .with_settle_timeout(Duration::from_millis(100)),
    );
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release = Arc::new(std::sync::Mutex::new(Some(release_rx)));
    let identity: IdentityGuard = Arc::new({
        let entered = entered.clone();
        move |_| {
            let lease = ParkLease {
                entered: entered.clone(),
                release: std::sync::Mutex::new(release.lock().unwrap().take().unwrap()),
            };
            Box::pin(async move { Ok(Box::new(lease) as IdentityLease) })
        }
    });
    let flow = authorize(&services, raw.clone(), identity).await;
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    // Queue ahead of the finalizer while the persistence lease is still held.
    let mut mutation = Box::pin(services.secrets.github_mutation());
    assert!(
        std::future::poll_fn(|cx| {
            std::task::Poll::Ready(mutation.as_mut().poll(cx).is_pending())
        })
        .await
    );
    release_tx.send(()).unwrap();
    let mut owner = mutation.await.unwrap();
    *owner += 1;
    raw.store(SECRET_ACCOUNT, "newer-token").unwrap();
    raw.store(
        crate::source_control_auth_ops::GITHUB_TOKEN_METHOD_ACCOUNT,
        "pat",
    )
    .unwrap();
    let resident_id = {
        let mut slot = services.github_auth_flow.lock().await;
        let slot = slot.as_mut().unwrap();
        if replace_flow {
            slot.flow_id = next_flow_id();
        }
        slot.flow_id
    };
    // Keep the newer mutation owned until the finalizer's bounded wait expires.
    tokio::time::timeout(Duration::from_secs(5), flow)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
        Some("newer-token")
    );
    assert_eq!(
        raw.load(crate::source_control_auth_ops::GITHUB_TOKEN_METHOD_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("pat")
    );
    assert!(
        std::future::poll_fn(|cx| {
            std::task::Poll::Ready(Box::pin(events.recv()).as_mut().poll(cx).is_pending())
        })
        .await,
        "a timed-out finalizer must not announce authorization"
    );
    let slot = services.github_auth_flow.lock().await;
    let slot = slot.as_ref().unwrap();
    assert_eq!(slot.flow_id, resident_id);
    assert_eq!(
        slot.phase,
        if replace_flow {
            FlowPhase::Pending
        } else {
            FlowPhase::Error
        }
    );
    drop(owner);
}

#[intent_test_macros::daemon_test]
async fn github_finalizer_ownership_timeout_terminates_resident_flow() {
    finalizer_ownership_timeout(false).await;
}

#[intent_test_macros::daemon_test]
async fn github_finalizer_ownership_timeout_preserves_replacement_flow() {
    finalizer_ownership_timeout(true).await;
}
