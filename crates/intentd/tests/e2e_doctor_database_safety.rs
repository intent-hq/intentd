//! Diagnostics must not mistake a live turn's staged payloads for dead turns.
//! Uses the locally built CLI, synthetic data, and no running application daemon.

#![cfg(unix)]

mod common;

use intent_core::AgentId;
use intent_store::Store;
use intentd_test_support::GuardedChild;
use nix::fcntl::{Flock, FlockArg};
use serde_json::{json, Value};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

const ISOLATION_PROBE: &str = "INTENTD_DATABASE_SAFETY_FIXTURE";
const STAGED: &str = "synthetic-live-turn";
const FINALIZED: &str = "synthetic-finalized";
const TIMESTAMP: &str = "2026-01-01T00:00:00Z";

struct Fixture {
    root: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let root = common::test_tempdir("doctor-database-safety");
        for dir in [
            "data",
            "bin",
            "home",
            "config",
            "cache",
            "workspaces",
            "tmp",
        ] {
            fs::create_dir(root.path().join(dir)).unwrap();
        }
        fs::write(root.path().join("config.toml"), "").unwrap();
        // Enhanced PATH includes host install directories. Shadow every known
        // provider executable, then verify actual selection in a fresh process.
        let mut commands = vec!["node", "npx", "npm", "pi", "auggie", "git", "gh"];
        for provider in intent_providers::ACP_PROVIDERS {
            commands.push(provider.command);
            commands.extend(provider.requires_secondary_binary);
        }
        for name in commands {
            let path = root.path().join("bin").join(name);
            fs::write(&path, "#!/bin/sh\nif [ \"$1\" = --version ]; then printf '24.0.0\\n'; exit 0; fi\nexit 1\n").unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        Self { root }
    }

    fn data_dir(&self) -> PathBuf {
        self.root.path().join("data")
    }

    fn lock(&self) -> Flock<File> {
        // Same lock path and operation as cmd_serve's startup ownership guard.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.data_dir().join("intentd.lock"))
            .unwrap();
        Flock::lock(file, FlockArg::LockExclusiveNonblock).unwrap()
    }

    fn assert_owned(&self) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.data_dir().join("intentd.lock"))
            .unwrap();
        let (_, error) = Flock::lock(file, FlockArg::LockExclusiveNonblock)
            .expect_err("synthetic writer must retain exclusive startup ownership");
        assert_eq!(error, nix::errno::Errno::EWOULDBLOCK);
    }

    fn command(&self, program: &Path) -> Command {
        let root = self.root.path();
        let mut command = Command::new(program);
        command
            .env_clear()
            .env("PATH", root.join("bin"))
            .env("HOME", root.join("home"))
            .env("SHELL", root.join("no-login-shell"))
            .env("XDG_CONFIG_HOME", root.join("config"))
            .env("XDG_DATA_HOME", root.join("data"))
            .env("XDG_CACHE_HOME", root.join("cache"))
            .env("CODEX_HOME", root.join("home/codex"))
            .env("CLAUDE_CONFIG_DIR", root.join("home/claude"))
            .env("INTENTD_CONFIG", root.join("config.toml"))
            .env("INTENTD_DATA_DIR", self.data_dir())
            .env("INTENTD_WORKSPACES_DIR", root.join("workspaces"))
            .env("INTENTD_SECRETS_FILE", root.join("secrets.json"))
            .env("INTENTD_TCP_PORT", "0")
            .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
            .env("TMPDIR", root.join("tmp"))
            .current_dir(root);
        command
    }

    fn run(&self, mut command: Command, label: &str) -> String {
        let stdout_path = self.root.path().join(format!("{label}.stdout"));
        let stderr_path = self.root.path().join(format!("{label}.stderr"));
        command
            .stdin(Stdio::null())
            .stdout(File::create(&stdout_path).unwrap())
            .stderr(File::create(&stderr_path).unwrap());
        let mut child = GuardedChild::spawn(&mut command).unwrap();
        let status = child
            .wait_with_timeout(common::test_timeout(Duration::from_secs(30)))
            .unwrap()
            .expect("isolated diagnostic exceeded deadline");
        let stdout = fs::read_to_string(stdout_path).unwrap();
        let stderr = fs::read_to_string(stderr_path).unwrap();
        assert!(status.success(), "{label}: {status}\n{stdout}\n{stderr}");
        println!("{label}: {status}\n{stdout}\n{stderr}");
        stdout
    }

    fn doctor(&self) {
        let mut probe = self.command(&std::env::current_exe().unwrap());
        probe
            .args(["--exact", "diagnostic_fixture_is_isolated", "--nocapture"])
            .env(ISOLATION_PROBE, self.root.path());
        self.run(probe, "isolation");
        let mut command = self.command(Path::new(env!("CARGO_BIN_EXE_intentd")));
        command.arg("doctor");
        let stdout = self.run(command, "doctor");
        assert!(stdout.contains("migrations current"), "{stdout}");
        assert!(stdout.contains("integrity_check: ok"), "{stdout}");
    }
}

#[test]
fn diagnostic_fixture_is_isolated() {
    let Some(root) = std::env::var_os(ISOLATION_PROBE) else {
        return; // Invoked with the fixture environment before the CLI may run.
    };
    let root = fs::canonicalize(root).unwrap();
    let config = intent_core::Config::resolve().unwrap();
    assert_eq!(
        fs::canonicalize(&config.data_dir).unwrap(),
        root.join("data")
    );
    assert_eq!(config.db_path, config.data_dir.join("intentd.db"));
    assert_eq!(
        fs::canonicalize(&config.config_path).unwrap(),
        root.join("config.toml")
    );
    let assert_local = |path: PathBuf| {
        assert!(
            fs::canonicalize(&path)
                .unwrap()
                .starts_with(root.join("bin")),
            "provider discovery escaped the synthetic fixture: {}",
            path.display()
        );
    };
    for provider in intent_providers::discover_providers_with_overrides(&|_| None) {
        if let Some(path) = provider.resolved_path {
            assert_local(path);
        }
        if let Some(path) = provider.secondary_binary.and_then(|s| s.resolved_path) {
            assert_local(path);
        }
    }
    for path in [
        intent_providers::find_node(),
        intent_providers::find_npx(),
        intent_providers::find_codex_npx(),
        intent_providers::find_pi_cli("pi"),
    ] {
        assert_local(path.expect("required synthetic executable"));
    }
    for path in intent_providers::find_auggie_candidates(None) {
        assert_local(path);
    }
}

fn agent() -> AgentId {
    AgentId("synthetic-agent".into())
}

fn heavy_block(label: &str) -> Value {
    json!({"type": "tool_result", "toolCallId": label,
        "output": format!("{label}:{}:full-body-tail", "payload-λ\n".repeat(4096))})
}

async fn seed(store: &Store) -> Value {
    sqlx::query("INSERT INTO workspace (id,title,branch,status,created_at,updated_at) VALUES ('synthetic-workspace','Synthetic','main','active',?,?)")
        .bind(TIMESTAMP).bind(TIMESTAMP).execute(store.write_pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_session (id,workspace_id,name,status,is_active,created_at,updated_at) VALUES ('synthetic-agent','synthetic-workspace','Synthetic','active',1,?,?)")
        .bind(TIMESTAMP).bind(TIMESTAMP).execute(store.write_pool()).await.unwrap();
    let finalized = store
        .prestage_agent_message_payload(&agent(), FINALIZED, 0, &heavy_block(FINALIZED))
        .await
        .unwrap()
        .expect("finalized fixture requires a side-table payload");
    finalize(store, FINALIZED, finalized).await;
    let slim = store
        .prestage_agent_message_payload(&agent(), STAGED, 0, &heavy_block(STAGED))
        .await
        .unwrap()
        .expect("live fixture requires a side-table payload");
    assert_ne!(
        slim,
        heavy_block(STAGED),
        "fixture must model the in-memory placeholder"
    );
    let envelopes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_message WHERE id = ?")
        .bind(STAGED)
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(
        envelopes, 0,
        "staged live payload must have no envelope yet"
    );
    assert_eq!(payloads(store, STAGED).await.len(), 1);
    assert!(full_body_matches(store, FINALIZED).await);
    slim
}

async fn finalize(store: &Store, id: &str, slim: Value) {
    store
        .append_agent_message_prestaged(&agent(), id, "assistant", &json!([slim]), None, TIMESTAMP)
        .await
        .unwrap();
}

async fn payloads(store: &Store, id: &str) -> Vec<(i64, String, String, Vec<u8>)> {
    sqlx::query_as("SELECT block_ordinal, kind, encoding, body FROM agent_message_payload WHERE message_id = ? ORDER BY block_ordinal, kind")
        .bind(id).fetch_all(store.read_pool()).await.unwrap()
}

async fn full_body_matches(store: &Store, id: &str) -> bool {
    store
        .get_agent_message_by_id(&agent(), id)
        .await
        .unwrap()
        .unwrap()
        .content
        == json!([heavy_block(id)])
}

#[tokio::test]
async fn doctor_preserves_live_staged_and_finalized_full_bodies() {
    let fixture = Fixture::new();
    let _owner = fixture.lock();
    let writer = Store::open(&fixture.data_dir().join("intentd.db"))
        .await
        .unwrap();
    let slim = seed(&writer).await;
    let staged_before = payloads(&writer, STAGED).await;
    let finalized_before = payloads(&writer, FINALIZED).await;
    fixture.assert_owned();

    // Keep writer AND daemon startup ownership alive throughout diagnostics.
    fixture.doctor();
    fixture.assert_owned();
    let staged_after = payloads(&writer, STAGED).await;
    let finalized_unchanged = payloads(&writer, FINALIZED).await == finalized_before
        && full_body_matches(&writer, FINALIZED).await;
    // Finalize from the actual retained placeholder, never re-stage the body.
    finalize(&writer, STAGED, slim).await;
    let live_full_body = full_body_matches(&writer, STAGED).await;
    println!("staged rows before={}, after={}; finalized unchanged={finalized_unchanged}; live finalized full body={live_full_body}", staged_before.len(), staged_after.len());
    writer.close().await;
    assert_eq!(
        (staged_after == staged_before, finalized_unchanged, live_full_body),
        (true, true, true),
        "doctor must preserve staged bytes, finalized bytes, and subsequent full-fidelity finalization"
    );
}

#[tokio::test]
async fn owned_startup_reaps_dead_turn_and_preserves_finalized_full_body() {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let finalized_before;
    {
        let _owner = fixture.lock();
        let writer = Store::open(&db).await.unwrap();
        seed(&writer).await;
        finalized_before = payloads(&writer, FINALIZED).await;
        // The turn dies: close both pools and relinquish ownership before restart.
        writer.close().await;
    }
    let _new_owner = fixture.lock();
    fixture.assert_owned();
    let restarted = Store::open(&db).await.unwrap();
    assert!(
        payloads(&restarted, STAGED).await.is_empty(),
        "dead-turn payload should be reaped at owned startup"
    );
    assert_eq!(payloads(&restarted, FINALIZED).await, finalized_before);
    assert!(full_body_matches(&restarted, FINALIZED).await);
    let (recorded, actual): (i64, i64) = sqlx::query_as("SELECT conversation_bytes, (SELECT COALESCE(SUM(OCTET_LENGTH(content)),0) FROM agent_message WHERE agent_id = agent_session.id) + (SELECT COALESCE(SUM(OCTET_LENGTH(body)),0) FROM agent_message_payload WHERE agent_id = agent_session.id) FROM agent_session WHERE id = ?")
        .bind(&agent().0).fetch_one(restarted.read_pool()).await.unwrap();
    assert_eq!(
        recorded, actual,
        "dead-turn cleanup must rebalance stored byte accounting"
    );
    restarted.close().await;
}
