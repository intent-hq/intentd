//! A spawned daemon carries the dead test proxy
//! (`intentd_test_support::block_test_egress` in the suite ctor and
//! `apply_test_egress` in `common::hermetic_fixture_identity`): its forge
//! request to a real public host fails on the proxy, while the same request
//! to a loopback fixture answers. Before this guard a daemon fixture that
//! forgot its mock sent the fake token to api.github.com (intentd#3743,
//! intentd#3758). Both legs boot with `env_clear()`, so the command, not the
//! inherited test env, has to carry the guard.

#![cfg(unix)]

use crate::common;
use crate::common::uds_exchange::uds_rpc;

use std::path::Path;
use std::process::Stdio;

use intentd_test_support::GuardedChild;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const FIXTURE_TOKEN: &str = "gho_egress_guard_fixture_token";
const FIXTURE_LOGIN: &str = "egress-fixture";

/// Answers every request with one GitHub user.
async fn github_user_fixture() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0_u8; 4096];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let body = json!({
                    "login": FIXTURE_LOGIN,
                    "id": 7,
                    "avatar_url": "https://avatars.githubusercontent.com/u/7",
                    "html_url": format!("https://github.com/{FIXTURE_LOGIN}"),
                })
                .to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                     X-OAuth-Scopes: repo, read:org\r\ncontent-length: {}\r\n\
                     connection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    format!("http://127.0.0.1:{port}")
}

fn spawn_daemon(data_dir: &Path, api_base: &str) -> GuardedChild {
    std::fs::write(
        data_dir.join("secrets.json"),
        json!({ "sourceControl.github.token": FIXTURE_TOKEN }).to_string(),
    )
    .unwrap();
    let log = std::fs::File::create(data_dir.join("daemon.log")).unwrap();
    let mut cmd = common::hermetic_serve_command(data_dir);
    cmd.env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|value| ("LLVM_PROFILE_FILE", value)))
        .env("HOME", data_dir)
        .env("PATH", "/usr/bin:/bin")
        .env("INTENTD_TCP_PORT", "0")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", data_dir.join("workspaces"))
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_ASSERT_BOUND_CALLER", "1")
        .env("INTENTD_GITHUB_API_BASE_URI", api_base)
        .env("INTENTD_GITHUB_LOGIN_BASE_URI", "http://127.0.0.1:0")
        .stdout(Stdio::null())
        .stderr(log);
    common::hermetic_fixture_identity(&mut cmd, data_dir);
    GuardedChild::spawn(&mut cmd).expect("spawn intentd serve")
}

async fn get_user(api_base: &str) -> Value {
    let dir = common::test_tempdir_in("/tmp", "itd-egress-");
    let mut daemon = spawn_daemon(dir.path(), api_base);
    let socket = dir.path().join("intentd.sock");
    common::await_daemon_listening(&mut daemon, &socket, &dir.path().join("daemon.log")).await;
    let reply = uds_rpc(&socket, 1, "github.getUser", json!({})).await;
    let _ = daemon.kill_tree();
    reply
}

#[tokio::test]
async fn spawned_daemon_reaches_loopback_fixture_but_not_a_public_forge() {
    if intentd_test_support::test_egress_allowed() {
        eprintln!("skipping: the egress guard is off for a live run");
        return;
    }

    let reply = get_user(&github_user_fixture().await).await;
    assert_eq!(reply["result"]["user"]["login"], FIXTURE_LOGIN, "{reply}");

    let reply = get_user("https://api.github.com").await;
    let error = reply["error"].to_string();
    assert!(reply.get("result").is_none(), "{reply}");
    assert!(
        !error.contains("Bad credentials") && !error.contains("401"),
        "the fake token reached the real GitHub: {reply}"
    );
    // The dead proxy refuses the tunnel before any TLS or HTTP exchange.
    assert!(
        error.contains("(Connect)"),
        "not refused by the dead proxy: {reply}"
    );
}
