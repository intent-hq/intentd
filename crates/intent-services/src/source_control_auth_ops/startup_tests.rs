use super::*;
use intent_core::WorkspaceApi;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const SAVED_PAT: &str = "glpat-startup-saved-canary";

struct DeferredStart {
    host: GitlabHost,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    task: JoinHandle<()>,
}

impl Drop for DeferredStart {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl DeferredStart {
    async fn new(host: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let hit = entered.clone();
        let resume = release.clone();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                assert_ne!(n, 0);
                request.extend_from_slice(&buf[..n]);
                if let Some(end) = request.windows(4).position(|v| v == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&request[..end]);
                    let length: usize = head
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("POST /oauth/authorize_device "));
            assert!(!request.contains(SAVED_PAT));
            hit.notify_one();
            resume.notified().await;
            let body = json!({
                "device_code": "private-startup-device-code",
                "user_code": "TEST-CODE",
                "verification_uri": "https://gitlab.test/oauth/device",
                "expires_in": 900,
                "interval": 3600
            })
            .to_string();
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        Self {
            host: GitlabHost::parse(host)
                .unwrap()
                .with_api_origin(&origin)
                .unwrap(),
            entered,
            release,
            task,
        }
    }

    async fn start(&self, svc: &Arc<crate::Services>) -> JoinHandle<Result<Value>> {
        let svc = svc.clone();
        let host = self.host.clone();
        let task = tokio::spawn(async move { svc.gitlab_connect_device(host).await });
        timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .expect("startup reached private upstream");
        task
    }

    async fn finish(&self, task: JoinHandle<Result<Value>>) -> Result<Value> {
        self.release.notify_one();
        timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
    }
}

async fn services() -> (tempfile::TempDir, Arc<crate::Services>) {
    let dir = crate::test_support::test_tempdir("gitlab-startup-");
    let store = intent_store::Store::open(&dir.path().join("store.db"))
        .await
        .unwrap();
    let registry = Arc::new(crate::SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    registry
        .apply(&[(
            "sourceControl.gitlab.oauthClientId".into(),
            json!("private-client"),
        )])
        .unwrap();
    let secrets = FileSecretStore::with_path(dir.path().join("secrets.json"));
    secrets.store(GITLAB_SECRET_ACCOUNT, SAVED_PAT).unwrap();
    let svc = crate::Services::new(store)
        .with_settings_registry(registry)
        .with_gitlab_secret_store(secrets)
        .with_workspaces_root(dir.path().join("workspaces"));
    (dir, Arc::new(svc))
}

#[intent_test_macros::daemon_test]
async fn newer_startup_owns_both_response_orders() {
    for newer_finishes_first in [true, false] {
        let (_dir, svc) = services().await;
        let a = DeferredStart::new("gitlab.old.test").await;
        let b = DeferredStart::new("gitlab.new.test").await;
        let old = a.start(&svc).await;
        let new = b.start(&svc).await;
        let (old_result, new_result) = if newer_finishes_first {
            let new_result = b.finish(new).await;
            (a.finish(old).await, new_result)
        } else {
            let old_result = a.finish(old).await;
            assert!(
                svc.gitlab_auth.lock().await.flow.is_none(),
                "an older response cannot become resident while newer startup is pending"
            );
            (old_result, b.finish(new).await)
        };
        assert!(
            old_result.is_err(),
            "superseded startup must not hand out abandoned codes: {old_result:?}"
        );
        let new_result = new_result.unwrap();
        let reused = svc.gitlab_connect_device(b.host.clone()).await.unwrap();
        assert_eq!(reused["flowId"], new_result["flowId"]);
        let guard = svc.gitlab_auth.lock().await;
        assert_eq!(guard.flow.as_ref().unwrap().host, b.host.host());
        assert_eq!(
            svc.gitlab_secret_store
                .load(GITLAB_SECRET_ACCOUNT)
                .unwrap()
                .as_deref(),
            Some(SAVED_PAT)
        );
    }
}

#[intent_test_macros::daemon_test]
async fn cancel_startup_is_host_scoped_and_preserves_saved_credentials() {
    let (_dir, svc) = services().await;
    let a = DeferredStart::new("gitlab.old.test").await;
    let old = a.start(&svc).await;
    let unmatched = svc
        .source_control_cancel_auth("gitlab".into(), Some("gitlab.other.test".into()))
        .await
        .unwrap();
    assert_eq!(unmatched, json!({"ok": true, "cancelled": false}));
    let matched = svc
        .source_control_cancel_auth("gitlab".into(), Some(a.host.host().into()))
        .await
        .unwrap();
    // Release even on the RED path, so the fixture always has a terminal response.
    let result = a.finish(old).await;
    assert_eq!(matched, json!({"ok": true, "cancelled": true}));
    assert!(
        result.is_err(),
        "cancelled startup must not install a flow: {result:?}"
    );
    assert!(svc.gitlab_auth.lock().await.flow.is_none());
    assert_eq!(
        svc.gitlab_secret_store
            .load(GITLAB_SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some(SAVED_PAT)
    );
}

#[intent_test_macros::daemon_test]
async fn startup_reuse_and_pending_only_cancellation() {
    let (_dir, svc) = services().await;
    let a = DeferredStart::new("gitlab.old.test").await;
    let started = a.start(&svc).await;
    let first = a.finish(started).await.unwrap();
    let reused = svc.gitlab_connect_device(a.host.clone()).await.unwrap();
    assert_eq!(first["flowId"], reused["flowId"]);
    for phase in [FlowPhase::Denied, FlowPhase::Expired, FlowPhase::Error] {
        svc.gitlab_auth
            .lock()
            .await
            .flow
            .as_mut()
            .unwrap()
            .slot
            .phase = phase;
        let result = svc
            .source_control_cancel_auth("gitlab".into(), Some(a.host.host().into()))
            .await
            .unwrap();
        assert_eq!(result, json!({"ok": true, "cancelled": false}));
        assert_eq!(
            svc.gitlab_auth
                .lock()
                .await
                .flow
                .as_ref()
                .unwrap()
                .slot
                .phase,
            phase
        );
    }
    svc.gitlab_auth
        .lock()
        .await
        .flow
        .as_mut()
        .unwrap()
        .slot
        .phase = FlowPhase::Pending;
    let result = svc
        .source_control_cancel_auth("gitlab".into(), Some(a.host.host().into()))
        .await
        .unwrap();
    assert_eq!(result, json!({"ok": true, "cancelled": true}));
    assert_eq!(
        svc.gitlab_secret_store
            .load(GITLAB_SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some(SAVED_PAT)
    );
}
