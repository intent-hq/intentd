//! Regression guard for the test egress block (`super::block_test_egress`):
//! a forge request that forgets its fake server must fail on the dead local
//! proxy, whatever host the code picked, while loopback fixtures still answer.
//! Per-address fixes (intentd#3743, intentd#3758) kept finding new leaks.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn budget() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(10))
}

fn chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(next) = source {
        text.push_str(": ");
        text.push_str(&next.to_string());
        source = next.source();
    }
    text
}

async fn loopback_fixture() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0_u8; 4096];
                let _ = socket.read(&mut buf).await;
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-length: 7\r\nconnection: close\r\n\r\nfixture",
                    )
                    .await;
            });
        }
    });
    port
}

#[tokio::test]
async fn public_host_fails_on_the_dead_proxy_while_loopback_fixtures_answer() {
    let client = reqwest::Client::new();
    let port = loopback_fixture().await;
    for host in ["127.0.0.1", "localhost"] {
        let response = tokio::time::timeout(
            budget(),
            client
                .get(format!("http://{host}:{port}/api/v3/user"))
                .send(),
        )
        .await
        .expect("loopback fixture answers in time")
        .unwrap_or_else(|e| panic!("{host} fixture bypasses the proxy: {}", chain(&e)));
        assert_eq!(response.text().await.unwrap(), "fixture", "{host}");
    }

    for url in [
        "https://api.github.com/user",
        "https://gitlab.com/api/v4/user",
    ] {
        let error = tokio::time::timeout(budget(), client.get(url).send())
            .await
            .expect("blocked request fails fast")
            .expect_err("a real forge request must not leave the test process");
        let text = chain(&error);
        assert!(error.is_connect(), "{url}: {text}");
        // Through the proxy the client never resolves the forge host itself.
        assert!(!text.to_ascii_lowercase().contains("dns"), "{url}: {text}");
    }
}

#[tokio::test]
async fn git_subprocess_is_sent_to_the_dead_proxy() {
    let output = tokio::time::timeout(
        budget(),
        tokio::process::Command::new("git")
            .args(["ls-remote", "https://github.com/git/git.git", "HEAD"])
            .env("GIT_TERMINAL_PROMPT", "0")
            // A developer's `http.proxy` setting would win over the proxy vars.
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("blocked git fails fast")
    .expect("run git");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "git reached the network: {stderr}"
    );
    assert!(stderr.contains("127.0.0.1"), "{stderr}");
}
