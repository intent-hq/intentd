//! E2E for the daemon-backed git credential helper (monorepo#884 Phase 2):
//! `intentd git-credential` ↔ `system.gitCredential` over UDS.
//!
//! Spawns the REAL `intentd serve` daemon (hermetic data dir, `GITHUB_TOKEN` in
//! the daemon env so the Auto token chain resolves deterministically without
//! touching the developer's secrets/`gh`), then runs the REAL
//! `intentd git-credential` subcommand end-to-end and asserts:
//! - `get` for HTTPS github.com prints `username=`/`password=` and exits 0;
//! - non-github hosts, non-https protocols, and `store`/`erase` stay silent;
//! - the `exposeGitCredentialToChildren = false` gate stays silent;
//! - a stopped daemon stays silent (exit 0) so git falls through;
//! - `github.revoke` applies to the very next `get` (Phase 2.2: the helper
//!   resolves per-invocation, so revocation is immediate — no stale token in
//!   any child environment).

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::net::UnixStream;
use tokio::time::timeout;
use uuid::Uuid;

/// A deterministic fake token; never a real credential.
const TEST_TOKEN: &str = "gho_e2e_test_token_1234567890";

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-gitcred-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", data_dir.join("secrets.json"))
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        // Deterministic token resolution: the Auto chain finds this env token
        // (hermetic secrets file above is empty) without shelling out to gh.
        .env("GITHUB_TOKEN", TEST_TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

/// Run `intentd git-credential <operation>` with `input` on stdin against the
/// daemon in `data_dir`; returns `(exit_ok, stdout)`. `GITHUB_TOKEN` is
/// deliberately NOT set in the helper env — the token must come from the
/// daemon over UDS, never from the helper's own environment.
fn run_helper(data_dir: &Path, operation: &str, input: &str) -> (bool, String) {
    use std::io::Write;
    let mut child = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["git-credential", operation])
        .env("INTENTD_DATA_DIR", data_dir)
        .env_remove("GITHUB_TOKEN")
        .env_remove("GH_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn intentd git-credential");
    child
        .stdin
        .take()
        .expect("helper stdin")
        .write_all(input.as_bytes())
        .expect("write helper stdin");
    let output = child.wait_with_output().expect("wait for helper");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
    )
}

const GITHUB_GET: &str = "protocol=https\nhost=github.com\n\n";

#[tokio::test]
async fn get_emits_credential_and_other_shapes_stay_silent() {
    let data_dir = temp_data_dir();
    let _daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    assert!(
        await_uds(&data_dir.join("intentd.sock")).await,
        "daemon did not start"
    );

    // The happy path: get + https + github.com → username/password lines.
    let (ok, stdout) = run_helper(&data_dir, "get", GITHUB_GET);
    assert!(ok);
    assert_eq!(
        stdout,
        format!("username=x-access-token\npassword={TEST_TOKEN}\n")
    );

    // Non-github hosts, subdomains, and non-https protocols: silent success.
    for input in [
        "protocol=https\nhost=gitlab.com\n\n",
        "protocol=https\nhost=api.github.com\n\n",
        "protocol=http\nhost=github.com\n\n",
        "protocol=ssh\nhost=github.com\n\n",
    ] {
        let (ok, stdout) = run_helper(&data_dir, "get", input);
        assert!(ok, "silent exit 0 for {input:?}");
        assert!(stdout.is_empty(), "no output for {input:?}: {stdout:?}");
    }

    // store/erase are no-ops even for github.com.
    for op in ["store", "erase"] {
        let (ok, stdout) = run_helper(&data_dir, op, GITHUB_GET);
        assert!(ok, "silent exit 0 for {op}");
        assert!(stdout.is_empty(), "no output for {op}: {stdout:?}");
    }

    // The token value must never land in the daemon log (grants are logged
    // by pid only).
    let log = std::fs::read_to_string(data_dir.join("daemon.log")).expect("read daemon log");
    assert!(
        !log.contains(TEST_TOKEN),
        "token bytes leaked into the daemon log"
    );
}

#[tokio::test]
async fn gate_off_stays_silent() {
    let data_dir = temp_data_dir();
    // Persist the opt-out BEFORE the daemon boots so the effective setting is
    // off from the first request.
    std::fs::write(
        data_dir.join("config.toml"),
        "[sourceControl.github]\nexposeGitCredentialToChildren = false\n",
    )
    .expect("write config.toml");
    let _daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    assert!(
        await_uds(&data_dir.join("intentd.sock")).await,
        "daemon did not start"
    );

    let (ok, stdout) = run_helper(&data_dir, "get", GITHUB_GET);
    assert!(ok, "silent exit 0 when the gate is off");
    assert!(
        stdout.is_empty(),
        "no output when the gate is off: {stdout:?}"
    );
}

#[tokio::test]
async fn daemon_down_stays_silent() {
    // No daemon at all: the data dir exists but nothing listens on the socket.
    let data_dir = temp_data_dir();
    let (ok, stdout) = run_helper(&data_dir, "get", GITHUB_GET);
    assert!(ok, "helper must exit 0 when the daemon is unreachable");
    assert!(stdout.is_empty(), "no output without a daemon: {stdout:?}");
    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Spawn `intentd serve` with the token seeded in the hermetic secrets store
/// and `tokenSource = "explicit"` (config.toml written before boot), and
/// **no** `GITHUB_TOKEN/GH_TOKEN` in the daemon env — the stored token is the
/// only possible source, so `github.revoke` deleting it must leave the chain
/// empty.
fn spawn_serve_with_stored_token(data_dir: &Path) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    std::fs::write(
        data_dir.join("secrets.json"),
        format!("{{\"sourceControl.github.token\":\"{TEST_TOKEN}\"}}"),
    )
    .expect("seed secrets file");
    std::fs::write(
        data_dir.join("config.toml"),
        "[sourceControl.github]\ntokenSource = \"explicit\"\n",
    )
    .expect("write config.toml");
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", data_dir.join("secrets.json"))
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        // Mock login host: the production-host gate must skip the gh CLI
        // logout side effect of `github.revoke`, so this e2e never touches
        // a real `gh` on the host.
        .env("INTENTD_GITHUB_LOGIN_BASE_URI", "http://127.0.0.1:0")
        .env_remove("GITHUB_TOKEN")
        .env_remove("GH_TOKEN")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn intentd serve")
}

/// Phase 2.2 revocation e2e: with the token stored in the secrets store
/// (`tokenSource = "explicit"`), the helper answers `get`; after
/// `intentd call github.revoke` deletes the stored token, the very next
/// `get` stays silent — daemon-backed resolution is per-invocation, so a
/// revocation applies immediately to every child.
#[tokio::test]
async fn revoke_applies_to_next_helper_get() {
    let data_dir = temp_data_dir();
    let _daemon = Daemon {
        child: spawn_serve_with_stored_token(&data_dir),
        data_dir: data_dir.clone(),
    };
    assert!(
        await_uds(&data_dir.join("intentd.sock")).await,
        "daemon did not start"
    );

    // Before revocation: the stored token flows through the helper.
    let (ok, stdout) = run_helper(&data_dir, "get", GITHUB_GET);
    assert!(ok);
    assert_eq!(
        stdout,
        format!("username=x-access-token\npassword={TEST_TOKEN}\n")
    );

    // Revoke over the same UDS control plane the helper uses.
    let revoke = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["call", "github.revoke"])
        .env("INTENTD_DATA_DIR", &data_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .expect("run intentd call github.revoke");
    assert!(revoke.status.success(), "github.revoke must succeed");

    // After revocation: no stored token, no env fallback (tokenSource is
    // explicit) — the helper stays silent so git falls through.
    let (ok, stdout) = run_helper(&data_dir, "get", GITHUB_GET);
    assert!(ok, "silent exit 0 after revocation");
    assert!(
        stdout.is_empty(),
        "no credential after revocation: {stdout:?}"
    );
}
