use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::{boot_with_command, close, common, connect, hello, scratch_dir, wss_rpc};

const SYNTHETIC_GITLAB_TOKEN: &str = "glpat-device-fixture-only";

struct LocalForge {
    origin: String,
    reads: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Drop for LocalForge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl LocalForge {
    async fn start() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let reads = Arc::new(AtomicUsize::new(0));
        let recorded = reads.clone();
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = Vec::new();
                let mut buf = [0; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let count = stream.read(&mut buf).await.unwrap();
                    assert_ne!(count, 0);
                    request.extend_from_slice(&buf[..count]);
                    assert!(request.len() < 8192);
                }
                let request = String::from_utf8(request).unwrap();
                assert!(request.lines().any(|line| line
                    .split_once(':')
                    .is_some_and(|(key, value)| key.eq_ignore_ascii_case("authorization")
                        && value.trim() == format!("Bearer {SYNTHETIC_GITLAB_TOKEN}"))));
                recorded.fetch_add(1, Ordering::SeqCst);
                let body = match request.lines().next().unwrap() {
                    "GET /api/v4/user HTTP/1.1" => json!({
                        "id": 4242, "username": "fixture-gitlab-person",
                        "name": "Fixture Person", "avatar_url": null
                    }),
                    "GET /api/v4/personal_access_tokens/self HTTP/1.1" => {
                        json!({"scopes": ["api"]})
                    }
                    route => panic!("unexpected local provider route: {route}"),
                }
                .to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        Self {
            origin,
            reads,
            task,
        }
    }
}

/// Exercise the fixture's real child boundary with a synthetic ambient token.
/// Both provider endpoints are loopback-only; no inherited token is inspected.
#[tokio::test]
async fn browser_fixture_ignores_ambient_gitlab_during_both_forge_refresh() {
    let root = scratch_dir();
    let forge = LocalForge::start().await;
    let mut command = common::serve_command();
    // Supply the token BEFORE the fixture applies its isolation, as if it were
    // inherited. Setting it afterwards would intentionally bypass the boundary.
    command
        .env("GITLAB_TOKEN", SYNTHETIC_GITLAB_TOKEN)
        .env("INTENTD_GITHUB_API_BASE_URI", &forge.origin)
        .env("INTENTD_GITLAB_API_BASE_URI", &forge.origin);
    let (_daemon, port, cfg) = boot_with_command(root.path(), command).await;
    let mut rpc = connect(port, cfg).await;
    let provider = wss_rpc(
        &mut rpc,
        1,
        "settings.get",
        json!({"path": "identity.provider"}),
    )
    .await;
    assert_eq!(provider["result"].get("value"), Some(&Value::Null));
    // Unset provider means this primary read refreshes BOTH forges off-path.
    let _ = wss_rpc(&mut rpc, 2, "principal.me", json!({})).await;
    // The synchronous probe exercises the same credential resolution, making a
    // leaked token observable without a scheduling-dependent negative sleep.
    let status = wss_rpc(
        &mut rpc,
        3,
        "sourceControl.authStatus",
        json!({"provider": "gitlab"}),
    )
    .await;
    assert_eq!(
        forge.reads.load(Ordering::SeqCst),
        0,
        "the fixture borrowed its synthetic ambient GitLab token"
    );
    assert_eq!(status["result"]["isConfigured"], false);
    assert!(!forge.task.is_finished(), "local provider must remain live");
    let me = wss_rpc(&mut rpc, 4, "principal.me", json!({})).await;
    assert_eq!(me["result"]["hostRole"], "owner");
    assert!(me["result"].get("identity").is_none());
    let _ = wss_rpc(&mut rpc, 5, "client.hello", hello("isolated-browser", true)).await;
    let listed = wss_rpc(&mut rpc, 6, "client.list", json!({})).await;
    let rows = listed["result"]["clients"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["principalId"], me["result"]["id"]);
    assert_eq!(rows[0]["hostRole"], "owner");
    assert_eq!(rows[0]["capabilities"]["browserExec"], true);
    for key in ["login", "displayName", "avatarUrl"] {
        assert_eq!(rows[0].get(key), Some(&Value::Null));
    }
    assert!(rows[0].get("identity").is_none());
    close(rpc).await;
    assert_eq!(forge.reads.load(Ordering::SeqCst), 0);
}
