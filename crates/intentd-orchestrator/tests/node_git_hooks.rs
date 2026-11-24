//! A machine's push into an orchestrator head: serving a tenant installs
//! head's git receive hooks, so a node's `git receive-pack` session (home
//! backups, agent turn-end pushes) is accepted. Without them every push
//! failed closed with `node-git-failed`, and remote agents never started.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(all(feature = "postgres", unix))]

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use intent_git::node_git::{service_command, AgentRemote, GitService};
use intent_store::postgres::{self, PgPool};
use intent_store::OrchestratorOnly;
use intentd_orchestrator::serve::{serve_tenant, LeaseTiming};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

async fn pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("INTENT_STORE_TEST_PG_URL") else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_PG").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_PG=1 but INTENT_STORE_TEST_PG_URL is unset"
        );
        eprintln!("skipping: INTENT_STORE_TEST_PG_URL is unset");
        return None;
    };
    let pool = postgres::connect(&url, 4).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    Some(pool)
}

fn keyring() -> std::sync::Arc<postgres::SecretKeyring> {
    std::sync::Arc::new(postgres::SecretKeyring::new("test", &[0x5a; 32]).unwrap())
}

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// A repository with one commit; its id.
fn source_commit(dir: &Path) -> String {
    std::fs::create_dir_all(dir).unwrap();
    git(dir, &["init", "-q"]);
    std::fs::write(dir.join("work.txt"), "work\n").unwrap();
    git(dir, &["add", "work.txt"]);
    git(
        dir,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-q",
            "-m",
            "work",
        ],
    );
    git(dir, &["rev-parse", "HEAD"])
}

/// The receive-pack request creating `ref_name` at `commit`: its command,
/// a flush and a pack of `commit` from `source`.
fn push_request(source: &Path, commit: &str, ref_name: &str) -> Vec<u8> {
    let zero = "0".repeat(commit.len());
    let line = format!("{zero} {commit} {ref_name}\0report-status\n");
    let mut request = format!("{:04x}{line}0000", line.len() + 4).into_bytes();
    let mut pack = Command::new("git")
        .arg("-C")
        .arg(source)
        .args(["pack-objects", "--quiet", "--stdout", "--revs"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    {
        use std::io::Write as _;
        let mut stdin = pack.stdin.take().unwrap();
        writeln!(stdin, "{commit}").unwrap();
    }
    let out = pack.wait_with_output().unwrap();
    assert!(out.status.success());
    request.extend(out.stdout);
    request
}

/// After `serve_tenant`, head's receive-pack for a node session runs with
/// head's update hook and lands the pushed agent ref.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_served_tenant_accepts_a_node_push() {
    let Some(pool) = pool().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let run = uuid::Uuid::new_v4().simple().to_string();
    let mut served = serve_tenant(
        pool,
        &format!("node-git-{run}"),
        dir.path(),
        &format!("orchestrator-{run}"),
        LeaseTiming::default(),
        keyring(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("serve the tenant");

    let head = dir.path().join("head-repo");
    std::fs::create_dir_all(&head).unwrap();
    git(&head, &["init", "-q", "--bare"]);
    let commit = source_commit(&dir.path().join("source"));
    let remote = AgentRemote::new("ws-1", "agent-1").unwrap();
    let ref_name = format!("{}head", remote.ref_prefix());

    let mut command = service_command(GitService::ReceivePack, &head, &remote)
        .expect("head's receive hooks are installed once a tenant is served");
    let mut child = command
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let request = push_request(&dir.path().join("source"), &commit, &ref_name);
    let reply = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(30)),
        async move {
            // Receive-pack advertises first; read it all while writing.
            let read = tokio::spawn(async move {
                let mut reply = Vec::new();
                stdout.read_to_end(&mut reply).await.unwrap();
                reply
            });
            stdin.write_all(&request).await.unwrap();
            drop(stdin);
            let reply = read.await.unwrap();
            let status = child.wait().await.unwrap();
            (reply, status)
        },
    )
    .await
    .expect("receive-pack finishes");
    let (reply, status) = reply;
    let reply = String::from_utf8_lossy(&reply);
    assert!(status.success(), "receive-pack: {status:?}: {reply}");
    assert!(reply.contains(&format!("ok {ref_name}")), "{reply}");
    assert_eq!(git(&head, &["rev-parse", &ref_name]), commit);

    served.fence().fire("test done");
    served.stopped().await;
}
