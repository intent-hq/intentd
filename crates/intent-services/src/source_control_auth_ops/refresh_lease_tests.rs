//! The GitLab credential refresh across processes (`try_refresh`): two
//! `Services` on one store and one secrets file stand for two processes of
//! a tenant (each with its own credential gate; the store's leases and the
//! stored pair are shared).

use std::sync::{Arc, Mutex};

use intent_core::FileSecretStore;
use intent_sourcecontrol::gitlab_token::{REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT};
use intent_sourcecontrol::GitlabHost;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use super::{try_refresh, RefreshFailure};
use crate::Services;

/// A GitLab token endpoint that records each refresh token it is shown,
/// answers the scripted exchanges in order, and parks the first exchange
/// until `release` (after notifying `entered`).
struct TokenServer {
    host: GitlabHost,
    exchanged: Arc<Mutex<Vec<String>>>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

async fn token_server(exchanges: Vec<(&'static str, Value)>) -> TokenServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let host = GitlabHost::parse("gitlab.lease.test")
        .unwrap()
        .with_api_origin(&origin)
        .unwrap();
    let exchanged = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let server = TokenServer {
        host,
        exchanged: Arc::clone(&exchanged),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    tokio::spawn(async move {
        let mut exchanges = exchanges.into_iter();
        let mut first = true;
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
            let mut length = 0;
            let mut line = String::new();
            loop {
                line.clear();
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
            let mut body = vec![0; length];
            reader.read_exact(&mut body).await.unwrap();
            let (status, reply) = if request.starts_with("POST /oauth/token ") {
                let form = String::from_utf8(body).unwrap();
                let presented = form
                    .split('&')
                    .find_map(|pair| pair.strip_prefix("refresh_token="))
                    .unwrap_or_default()
                    .to_owned();
                exchanged.lock().unwrap().push(presented);
                if std::mem::take(&mut first) {
                    entered.notify_one();
                    release.notified().await;
                }
                exchanges.next().expect("an unscripted token exchange")
            } else if request.starts_with("GET /api/v4/user ") {
                ("200 OK", json!({"id": 42, "username": "lease-user"}))
            } else {
                ("200 OK", json!({"scopes": ["api"]}))
            };
            let reply = reply.to_string();
            reader
                .get_mut()
                .write_all(
                    format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                        reply.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    server
}

fn rotated(access: &str, refresh: &str) -> (&'static str, Value) {
    (
        "200 OK",
        json!({"access_token": access, "refresh_token": refresh, "expires_in": 7200}),
    )
}

/// One tenant's store and secrets file, opened by `processes` services.
async fn processes(count: usize) -> (tempfile::TempDir, Vec<Services>) {
    let dir = tempfile::tempdir().unwrap();
    let store = intent_store::Store::open(&dir.path().join("store.db"))
        .await
        .unwrap();
    let secrets = dir.path().join("secrets.json");
    let file = FileSecretStore::with_path(&secrets);
    file.store(SECRET_ACCOUNT, "access-0").unwrap();
    file.store(REFRESH_SECRET_ACCOUNT, "refresh-0").unwrap();
    let services = (0..count)
        .map(|_| {
            Services::new_with_file_secrets(store.clone(), FileSecretStore::with_path(&secrets))
        })
        .collect();
    (dir, services)
}

async fn refresh(svc: &Services, host: &GitlabHost) -> Result<(), RefreshFailure> {
    let guard = svc.gitlab_credential_gate.lock().await;
    try_refresh(
        host,
        Some("lease-client"),
        svc.gitlab_secret_store.clone(),
        &svc.secrets,
        &guard,
        None,
    )
    .await
}

fn stored(svc: &Services, account: &str) -> Option<String> {
    svc.gitlab_secret_store.load(account).unwrap()
}

/// GitLab rotates the refresh token on every exchange. Two processes that
/// refresh together exchange one after the other under the host's lease:
/// the second waits for the first, then exchanges the token the first
/// stored, so no refresh token is ever presented twice.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_never_exchange_one_refresh_token_twice() {
    let server = token_server(vec![
        rotated("access-1", "refresh-1"),
        rotated("access-2", "refresh-2"),
    ])
    .await;
    let (_dir, services) = processes(2).await;
    let start = Arc::new(tokio::sync::Barrier::new(2));
    let refreshes: Vec<_> = services
        .iter()
        .map(|svc| {
            let svc = svc.clone();
            let host = server.host.clone();
            let start = Arc::clone(&start);
            tokio::spawn(async move {
                start.wait().await;
                refresh(&svc, &host).await.is_ok()
            })
        })
        .collect();
    server.entered.notified().await;
    assert_eq!(
        *server.exchanged.lock().unwrap(),
        ["refresh-0"],
        "while one exchange is in flight the other process waits for the lease"
    );
    server.release.notify_one();
    for refreshed in refreshes {
        assert!(refreshed.await.unwrap(), "both refreshes succeed");
    }
    assert_eq!(
        *server.exchanged.lock().unwrap(),
        ["refresh-0", "refresh-1"],
        "the second exchange used the token the first stored"
    );
    assert_eq!(
        stored(&services[0], REFRESH_SECRET_ACCOUNT).as_deref(),
        Some("refresh-2")
    );
    assert_eq!(
        stored(&services[1], SECRET_ACCOUNT).as_deref(),
        Some("access-2")
    );
}

/// The persist is a compare-and-set on the stored refresh token: a pair
/// written elsewhere during the exchange (a reconnect in another process)
/// stands, and the exchanged grant is dropped unwritten.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refresh_keeps_a_pair_that_changed_during_its_exchange() {
    let server = token_server(vec![rotated("access-1", "refresh-1")]).await;
    let (_dir, services) = processes(1).await;
    let svc = services[0].clone();
    let host = server.host.clone();
    let refreshing = tokio::spawn(async move { refresh(&svc, &host).await });
    server.entered.notified().await;
    let file = &services[0].gitlab_secret_store;
    file.store(SECRET_ACCOUNT, "access-external").unwrap();
    file.store(REFRESH_SECRET_ACCOUNT, "refresh-external")
        .unwrap();
    server.release.notify_one();
    assert!(matches!(
        refreshing.await.unwrap(),
        Err(RefreshFailure::Transient)
    ));
    assert_eq!(
        stored(&services[0], SECRET_ACCOUNT).as_deref(),
        Some("access-external")
    );
    assert_eq!(
        stored(&services[0], REFRESH_SECRET_ACCOUNT).as_deref(),
        Some("refresh-external")
    );
}

/// A refused refresh token (`invalid_grant`) that is no longer the stored
/// one was rotated by another refresh: the connection is kept (transient),
/// not cleared as unrecoverable.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_token_that_another_refresh_rotated_is_not_a_disconnect() {
    let server = token_server(vec![("400 Bad Request", json!({"error": "invalid_grant"}))]).await;
    let (_dir, services) = processes(1).await;
    let svc = services[0].clone();
    let host = server.host.clone();
    let refreshing = tokio::spawn(async move { refresh(&svc, &host).await });
    server.entered.notified().await;
    services[0]
        .gitlab_secret_store
        .store(REFRESH_SECRET_ACCOUNT, "refresh-rotated")
        .unwrap();
    server.release.notify_one();
    assert!(matches!(
        refreshing.await.unwrap(),
        Err(RefreshFailure::Transient)
    ));
    assert_eq!(
        stored(&services[0], REFRESH_SECRET_ACCOUNT).as_deref(),
        Some("refresh-rotated")
    );
}

/// The same refusal of the token still stored is unrecoverable, as before.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_stored_token_is_still_unrecoverable() {
    let server = token_server(vec![("400 Bad Request", json!({"error": "invalid_grant"}))]).await;
    let (_dir, services) = processes(1).await;
    let svc = services[0].clone();
    let host = server.host.clone();
    let refreshing = tokio::spawn(async move { refresh(&svc, &host).await });
    server.entered.notified().await;
    server.release.notify_one();
    assert!(matches!(
        refreshing.await.unwrap(),
        Err(RefreshFailure::Unrecoverable)
    ));
}
