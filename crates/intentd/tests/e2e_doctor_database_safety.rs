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

    fn database_files(&self) -> Vec<String> {
        let mut names: Vec<_> = fs::read_dir(self.data_dir())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name.starts_with("intentd.db"))
            .collect();
        names.sort();
        names
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

    fn run(&self, command: Command, label: &str) -> String {
        self.run_expected(command, label, true)
    }

    fn run_expected(&self, mut command: Command, label: &str, success: bool) -> String {
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
        assert_eq!(
            status.success(),
            success,
            "{label}: {status}\n{stdout}\n{stderr}"
        );
        println!("{label}: {status}\n{stdout}\n{stderr}");
        stdout
    }

    fn preflight(&self) {
        let mut probe = self.command(&std::env::current_exe().unwrap());
        probe
            .args(["--exact", "diagnostic_fixture_is_isolated", "--nocapture"])
            .env(ISOLATION_PROBE, self.root.path());
        self.run(probe, "isolation");
    }

    fn doctor_output(&self, success: bool) -> String {
        self.preflight();
        let mut command = self.command(Path::new(env!("CARGO_BIN_EXE_intentd")));
        command.arg("doctor");
        self.run_expected(command, "doctor", success)
    }

    fn doctor(&self) {
        let stdout = self.doctor_output(true);
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
    let writer = Store::open_for_daemon(&fixture.data_dir().join("intentd.db"))
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
async fn doctor_preserves_desktop_journal_and_owned_deletion_cleans_it() {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let writer = Store::open_for_daemon(&db).await.unwrap();
    let _slim = seed(&writer).await;
    let workspace = intent_core::WorkspaceId::from("synthetic-workspace");
    let principal = writer.get_primary_principal().await.unwrap().id;
    let binding = json!({"principalId": principal, "requestId": "desktop-request"});
    writer
        .desktop_set_permission(&principal, &workspace, &agent(), "synthetic-computer", true)
        .await
        .unwrap();
    writer
        .desktop_insert_request("desktop-request", &workspace, &agent(), &binding)
        .await
        .unwrap();
    writer
        .desktop_insert_terminal(
            "desktop-session",
            &workspace,
            &agent(),
            &binding,
            "synthetic-hash",
        )
        .await
        .unwrap();
    assert!(writer
        .desktop_resolve_request(
            "desktop-request",
            &workspace,
            &agent(),
            "granted",
            &json!({"sessionId":"desktop-session"})
        )
        .await
        .unwrap());
    let before: Vec<(String, String)> =
        sqlx::query_as("SELECT key,value FROM settings WHERE key GLOB 'desktop.v1/*' ORDER BY key")
            .fetch_all(writer.read_pool())
            .await
            .unwrap();
    assert_eq!(
        before.len(),
        4,
        "consent, request, terminal and durable wake"
    );
    fixture.doctor();
    let after: Vec<(String, String)> =
        sqlx::query_as("SELECT key,value FROM settings WHERE key GLOB 'desktop.v1/*' ORDER BY key")
            .fetch_all(writer.read_pool())
            .await
            .unwrap();
    assert_eq!(
        after, before,
        "diagnostics cannot terminate or consume desktop state"
    );
    assert!(writer
        .desktop_terminal("desktop-session")
        .await
        .unwrap()
        .unwrap()["reason"]
        .is_null());
    assert!(
        Store::open_for_daemon(&db).await.is_err(),
        "doctor must not release database ownership"
    );
    tokio::time::timeout(
        Duration::from_secs(10),
        writer.clone().delete_workspace(&workspace),
    )
    .await
    .expect("owned desktop deletion must not deadlock")
    .unwrap();
    let remaining: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM settings WHERE key GLOB 'desktop.v1/*'")
            .fetch_one(writer.read_pool())
            .await
            .unwrap();
    assert_eq!(
        remaining, 0,
        "owned workspace deletion must clean private state"
    );
    writer.close().await;
}

#[tokio::test]
async fn owned_startup_reaps_dead_turn_and_preserves_finalized_full_body() {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let finalized_before;
    {
        let _owner = fixture.lock();
        let writer = Store::open_for_daemon(&db).await.unwrap();
        seed(&writer).await;
        finalized_before = payloads(&writer, FINALIZED).await;
        // The turn dies: close both pools and relinquish ownership before restart.
        writer.close().await;
    }
    let _new_owner = fixture.lock();
    fixture.assert_owned();
    let restarted = Store::open_for_daemon(&db).await.unwrap();
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

#[tokio::test]
async fn ordinary_open_and_competing_startup_preserve_live_payloads() {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let owner = Store::open_for_daemon(&db).await.unwrap();
    let slim = seed(&owner).await;
    let before = payloads(&owner, STAGED).await;
    let other = Store::open(&db).await.unwrap();
    assert_eq!(payloads(&other, STAGED).await, before);
    assert!(Store::open_for_daemon(&db).await.is_err());
    // A symlink alias cannot obtain a second startup lease either.
    let alias = fixture.data_dir().join("alias.db");
    std::os::unix::fs::symlink(&db, &alias).unwrap();
    assert!(Store::open_for_daemon(&alias).await.is_err());
    let retained = owner.clone();
    drop(owner);
    assert!(
        Store::open_for_daemon(&db).await.is_err(),
        "clones retain ownership"
    );
    assert_eq!(payloads(&retained, STAGED).await, before);
    finalize(&retained, STAGED, slim).await;
    assert!(full_body_matches(&retained, STAGED).await);
    other.close().await;
    retained.close().await;
    drop(retained);
    let restarted = Store::open_for_daemon(&db).await.unwrap();
    assert!(full_body_matches(&restarted, STAGED).await);
    restarted.close().await;
}

#[tokio::test]
async fn competing_serve_exits_before_touching_live_payloads() {
    let fixture = Fixture::new();
    let _owner = fixture.lock();
    let writer = Store::open_for_daemon(&fixture.data_dir().join("intentd.db"))
        .await
        .unwrap();
    let slim = seed(&writer).await;
    let before = payloads(&writer, STAGED).await;
    fixture.preflight();
    let mut command = common::hermetic_serve_command(&fixture.data_dir());
    // Keep the standard serve builder while replacing its environment with the
    // same fail-closed synthetic routing verified by preflight.
    let isolated = fixture.command(Path::new(env!("CARGO_BIN_EXE_intentd")));
    command
        .env_clear()
        .envs(isolated.get_envs().filter_map(|(k, v)| v.map(|v| (k, v))));
    common::hermetic_fixture_identity(&mut command, &fixture.data_dir());
    command.current_dir(fixture.root.path());
    fixture.run_expected(command, "competing-serve", false);
    fixture.assert_owned();
    assert_eq!(payloads(&writer, STAGED).await, before);
    finalize(&writer, STAGED, slim).await;
    assert!(full_body_matches(&writer, STAGED).await);
    writer.close().await;
}

#[test]
fn doctor_missing_database_does_not_create_database_or_schema() {
    let fixture = Fixture::new();
    let output = fixture.doctor_output(false);
    assert!(
        output.contains("read-only database open failed"),
        "{output}"
    );
    assert!(!fixture.data_dir().join("intentd.db").exists());
    assert!(
        fixture.database_files().is_empty(),
        "no database or SQLite sidecars created"
    );
}

#[tokio::test]
async fn doctor_old_schema_reports_without_migrating() {
    use sqlx::Connection;
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&db)
        .create_if_missing(true);
    let mut connection = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE _sqlx_migrations (version INTEGER PRIMARY KEY, success BOOLEAN NOT NULL, checksum BLOB NOT NULL); CREATE TABLE synthetic_old (value TEXT); INSERT INTO synthetic_old VALUES ('preserve')")
        .execute(&mut connection).await.unwrap();
    connection.close().await.unwrap();
    let before = fs::read(&db).unwrap();
    let output = fixture.doctor_output(false);
    assert!(output.contains("migrations not current"), "{output}");
    assert_eq!(
        fs::read(&db).unwrap(),
        before,
        "old schema and data must be byte-identical"
    );
    assert_eq!(
        fixture.database_files(),
        vec!["intentd.db"],
        "no journal or WAL created"
    );
}

#[tokio::test]
async fn doctor_does_not_checkpoint_pending_wal_frames() {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let writer = Store::open_for_daemon(&db).await.unwrap();
    sqlx::query("PRAGMA wal_autocheckpoint=0")
        .execute(writer.write_pool())
        .await
        .unwrap();
    // Establish a known base, then create committed frames that a PASSIVE
    // checkpoint could copy. No reader holds a snapshot that would block it.
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(writer.write_pool())
        .await
        .unwrap();
    seed(&writer).await;
    let wal = db.with_file_name("intentd.db-wal");
    let shm = db.with_file_name("intentd.db-shm");
    let database_before = fs::read(&db).unwrap();
    let wal_before = fs::read(&wal).unwrap();
    assert!(
        wal_before.len() > 32,
        "fixture must have committed WAL frames"
    );
    // SQLite's native-endian WAL-index nBackfill counter sits at byte 96.
    let backfill_before = fs::read(&shm).unwrap()[96..100].to_vec();
    assert_eq!(backfill_before, 0_u32.to_ne_bytes());
    fixture.doctor();
    assert_eq!(
        fs::read(&db).unwrap(),
        database_before,
        "doctor checkpointed the DB"
    );
    assert_eq!(
        fs::read(&wal).unwrap(),
        wal_before,
        "doctor changed the WAL"
    );
    assert_eq!(
        &fs::read(&shm).unwrap()[96..100],
        backfill_before,
        "doctor advanced checkpoint progress"
    );
    writer.close().await;
}

#[tokio::test]
async fn diagnostic_handle_rejects_writes_and_checkpoint() {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let writer = Store::open(&db).await.unwrap();
    seed(&writer).await;
    let diagnostic = intent_store::DiagnosticStore::open(&db).await.unwrap();
    assert!(sqlx::query("DELETE FROM agent_message_payload")
        .execute(diagnostic.read_pool())
        .await
        .is_err());
    assert!(sqlx::query("CREATE TABLE forbidden (id INTEGER)")
        .execute(diagnostic.read_pool())
        .await
        .is_err());
    assert!(sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(diagnostic.read_pool())
        .await
        .is_err());
    assert!(diagnostic.migration_status().await.unwrap().is_current());
    diagnostic.close().await;
    assert_eq!(payloads(&writer, STAGED).await.len(), 1);
    writer.close().await;
}

#[tokio::test]
async fn startup_lock_precedes_database_creation() {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(fixture.data_dir().join("intentd.db.daemon.lock"))
        .unwrap();
    let lock = Flock::lock(lock, FlockArg::LockExclusiveNonblock).unwrap();
    assert!(Store::open_for_daemon(&db).await.is_err());
    assert!(
        !db.exists(),
        "a rejected startup cannot create or migrate the DB"
    );
    drop(lock);
    let owner = Store::open_for_daemon(&db).await.unwrap();
    assert!(owner.migration_status().await.unwrap().is_current());
    owner.close().await;
}

#[tokio::test]
async fn doctor_newer_schema_reports_without_modifying_database() {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let writer = Store::open(&db).await.unwrap();
    sqlx::query("INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (999999, 'synthetic future', 1, X'00', 0)")
        .execute(writer.write_pool()).await.unwrap();
    writer.close().await;
    let before = fs::read(&db).unwrap();
    let output = fixture.doctor_output(false);
    assert!(output.contains("migrations not current"), "{output}");
    assert_eq!(fs::read(&db).unwrap(), before);
}

#[tokio::test]
async fn dangling_database_alias_is_rejected_before_mutation() {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let alias = fixture.data_dir().join("alias.db");
    std::os::unix::fs::symlink("intentd.db", &alias).unwrap();
    for _ in 0..2 {
        assert!(
            Store::open_for_daemon(&alias).await.is_err(),
            "dangling final symlink must be rejected"
        );
        assert!(!db.exists());
        assert!(!fixture.data_dir().join("alias.db.daemon.lock").exists());
    }
    let parent_alias = fixture.root.path().join("data-alias");
    std::os::unix::fs::symlink(fixture.data_dir(), &parent_alias).unwrap();
    let owner = Store::open_for_daemon(&parent_alias.join("intentd.db"))
        .await
        .unwrap();
    let slim = seed(&owner).await;
    let before = payloads(&owner, STAGED).await;
    for path in [&alias, &alias, &db, &parent_alias.join("intentd.db")] {
        assert!(Store::open_for_daemon(path).await.is_err());
    }
    assert_eq!(payloads(&owner, STAGED).await, before);
    finalize(&owner, STAGED, slim).await;
    assert!(full_body_matches(&owner, STAGED).await);
    owner.close().await;
    drop(owner);
    let reopened = Store::open_for_daemon(&alias).await.unwrap();
    assert!(full_body_matches(&reopened, STAGED).await);
    reopened.close().await;
}

async fn assert_released(db: &Path) {
    // Dropping a SQLx pool signals asynchronous worker shutdown. Observe the
    // actual release, rather than assuming the signal has already closed SQLite.
    let owner = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(owner) = Store::open_for_daemon(db).await {
                break owner;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("last connection/options drop must eventually release ownership");
    assert!(full_body_matches(&owner, STAGED).await);
    owner.close().await;
}

async fn escaped_handle_retains_ownership(read_pool: bool, kind: &str) {
    use sqlx::Connection;
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let owner = Store::open_for_daemon(&db).await.unwrap();
    let slim = seed(&owner).await;
    let before = payloads(&owner, STAGED).await;
    let pool = if read_pool {
        owner.read_pool()
    } else {
        owner.write_pool()
    }
    .clone();
    let mut checked_out = if kind == "checked-out" {
        Some(pool.acquire().await.unwrap())
    } else {
        None
    };
    let mut detached = if kind == "detached" {
        Some(pool.acquire().await.unwrap().detach())
    } else {
        None
    };
    drop(owner);
    // Drop the last pool handle for detached/checked-out cases; retain it only
    // when testing pool clones (including their future connection creation).
    let retained_pool = if kind == "pool" {
        Some(pool)
    } else {
        drop(pool);
        None
    };
    assert!(
        Store::open_for_daemon(&db).await.is_err(),
        "{kind}, read_pool={read_pool} outlived Store but lost ownership"
    );
    let query = "UPDATE agent_session SET name = 'still writable' WHERE id = 'synthetic-agent'";
    if let Some(pool) = &retained_pool {
        // Force a fresh connection after dropping Store: pool options must
        // retain ownership as well as the original SQLite connection.
        pool.acquire()
            .await
            .unwrap()
            .detach()
            .close()
            .await
            .unwrap();
        assert!(Store::open_for_daemon(&db).await.is_err());
        sqlx::query(query).execute(pool).await.unwrap();
    }
    if let Some(conn) = &mut checked_out {
        sqlx::query(query).execute(&mut **conn).await.unwrap();
    }
    if let Some(conn) = &mut detached {
        sqlx::query(query).execute(&mut *conn).await.unwrap();
    }
    let observer = Store::open(&db).await.unwrap();
    assert_eq!(payloads(&observer, STAGED).await, before);
    // Use the original owner's retained placeholder, without re-staging.
    // Release a checked-out writer before finalizing on the separate pool.
    if let Some(conn) = checked_out.take() {
        conn.close().await.unwrap();
    }
    finalize(&observer, STAGED, slim).await;
    assert!(full_body_matches(&observer, STAGED).await);
    observer.close().await;
    drop(observer);
    if let Some(conn) = detached {
        conn.close().await.unwrap();
    }
    if let Some(pool) = retained_pool {
        pool.close().await;
        drop(pool);
    }
    assert_released(&db).await;
}

#[tokio::test]
async fn write_pool_clone_retains_ownership() {
    escaped_handle_retains_ownership(false, "pool").await;
}
#[tokio::test]
async fn read_pool_clone_retains_ownership() {
    escaped_handle_retains_ownership(true, "pool").await;
}
#[tokio::test]
async fn write_checked_out_retains_ownership() {
    escaped_handle_retains_ownership(false, "checked-out").await;
}
#[tokio::test]
async fn read_checked_out_retains_ownership() {
    escaped_handle_retains_ownership(true, "checked-out").await;
}
#[tokio::test]
async fn write_detached_retains_ownership() {
    escaped_handle_retains_ownership(false, "detached").await;
}
#[tokio::test]
async fn read_detached_retains_ownership() {
    escaped_handle_retains_ownership(true, "detached").await;
}

async fn invalid_migration_is_read_only_failure(update: &str) {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let writer = Store::open(&db).await.unwrap();
    sqlx::query(update)
        .execute(writer.write_pool())
        .await
        .unwrap();
    writer.close().await;
    drop(writer);
    let before = fs::read(&db).unwrap();
    let diagnostic = intent_store::DiagnosticStore::open(&db).await.unwrap();
    let status = diagnostic.migration_status().await;
    diagnostic.close().await;
    assert!(
        status.is_err(),
        "invalid migration metadata must not report current"
    );
    fixture.doctor_output(false);
    assert_eq!(fs::read(&db).unwrap(), before);
    // Same rejection as writable startup, checked only after byte preservation.
    assert!(Store::open(&db).await.is_err());
}

#[tokio::test]
async fn failed_migration_is_rejected_without_repair() {
    invalid_migration_is_read_only_failure(
        "UPDATE _sqlx_migrations SET success = 0 WHERE version = 1",
    )
    .await;
}
#[tokio::test]
async fn changed_migration_is_rejected_without_repair() {
    invalid_migration_is_read_only_failure(
        "UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = 1",
    )
    .await;
}

// Releases the blocked SQLite worker even if a contract assertion panics.
struct WorkerRelease(std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
impl Drop for WorkerRelease {
    fn drop(&mut self) {
        *self.0 .0.lock().unwrap() = true;
        self.0 .1.notify_all();
    }
}

async fn cancelled_query_ownership(renewed_read_pool: Option<bool>) {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let owner = Store::open_for_daemon(&db).await.unwrap();
    let slim = seed(&owner).await;
    let before = payloads(&owner, STAGED).await;
    let pool = if renewed_read_pool == Some(true) {
        owner.write_pool().close().await;
        owner.read_pool().clone()
    } else {
        owner.read_pool().close().await;
        owner.write_pool().clone()
    };
    if renewed_read_pool.is_some() {
        retire_pool_connections(&pool).await;
    }
    let mut connection = pool.acquire().await.unwrap().detach();
    pool.close().await;
    drop(pool);
    let release = WorkerRelease(std::sync::Arc::default());
    let worker_release = release.0.clone();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    connection
        .lock_handle()
        .await
        .unwrap()
        .create_collation("synthetic_wait", move |left, right| {
            started_tx.send(()).unwrap();
            let (flag, changed) = &*worker_release;
            let _ = changed
                .wait_timeout_while(flag.lock().unwrap(), Duration::from_secs(20), |released| {
                    !*released
                })
                .unwrap();
            left.cmp(right)
        })
        .unwrap();
    let query = tokio::spawn(async move {
        sqlx::query("SELECT 'a' < 'b' COLLATE synthetic_wait")
            .execute(&mut connection)
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(10), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    drop(owner);
    query.abort();
    assert!(query.await.unwrap_err().is_cancelled());
    assert!(
        Store::open_for_daemon(&db).await.is_err(),
        "cancelled Rust task must not release ownership while SQLite is executing"
    );
    let observer = Store::open(&db).await.unwrap();
    assert_eq!(payloads(&observer, STAGED).await, before);
    finalize(&observer, STAGED, slim).await;
    assert!(full_body_matches(&observer, STAGED).await);
    observer.close().await;
    drop(observer);
    drop(release);
    assert_released(&db).await;
}

#[tokio::test]
async fn pending_pool_close_retains_ownership_until_connections_release() {
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let owner = Store::open_for_daemon(&db).await.unwrap();
    let slim = seed(&owner).await;
    let pool = owner.write_pool().clone();
    let mut connection = pool.acquire().await.unwrap();
    drop(owner);
    let closing_pool = pool.clone();
    let closing = tokio::spawn(async move {
        closing_pool.close().await;
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !pool.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !closing.is_finished(),
        "close must wait for the checked-out connection"
    );
    assert!(Store::open_for_daemon(&db).await.is_err());
    sqlx::query("UPDATE agent_session SET name = 'closing' WHERE id = 'synthetic-agent'")
        .execute(&mut *connection)
        .await
        .unwrap();
    let observer = Store::open(&db).await.unwrap();
    finalize(&observer, STAGED, slim).await;
    assert!(full_body_matches(&observer, STAGED).await);
    observer.close().await;
    connection.close().await.unwrap();
    closing.await.unwrap();
    // A closed pool's clonable connection options conservatively retain the
    // lease until the handles themselves are dropped.
    assert!(Store::open_for_daemon(&db).await.is_err());
    drop(pool);
    assert_released(&db).await;
}

#[tokio::test]
async fn cancelled_startup_keeps_ownership_during_initialization() {
    use sqlx::Connection;
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let initial = Store::open(&db).await.unwrap();
    let slim = seed(&initial).await;
    finalize(&initial, STAGED, slim).await;
    initial.close().await;
    drop(initial);
    let options = sqlx::sqlite::SqliteConnectOptions::new().filename(&db);
    let mut blocker = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::query("PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE")
        .execute(&mut blocker)
        .await
        .unwrap();
    let start_path = db.clone();
    let startup = tokio::spawn(async move { Store::open_for_daemon(&start_path).await });
    let lock_path = fixture.data_dir().join("intentd.db.daemon.lock");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(file) = OpenOptions::new().read(true).write(true).open(&lock_path) {
                if Flock::lock(file, FlockArg::LockExclusiveNonblock).is_err() {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    startup.abort();
    assert!(matches!(startup.await, Err(error) if error.is_cancelled()));
    assert!(
        Store::open_for_daemon(&db).await.is_err(),
        "initialization must retain ownership after caller cancellation"
    );
    sqlx::query("COMMIT").execute(&mut blocker).await.unwrap();
    blocker.close().await.unwrap();
    assert_released(&db).await;
}

async fn retire_pool_connections(pool: &intent_store::StorePool) {
    // Reads can have opened multiple connections; wait for returns and retire
    // every original connection, not just the first available idle one.
    tokio::time::timeout(Duration::from_secs(10), async {
        while pool.num_idle() != pool.size() as usize {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut connections = Vec::new();
    for _ in 0..pool.size() {
        connections.push(pool.acquire().await.unwrap());
    }
    for connection in connections {
        connection.close().await.unwrap();
    }
    assert_eq!(pool.size(), 0);
}

async fn renewed_facade_retains_ownership(read_pool: bool, handle: &str) {
    use sqlx::Connection;
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let owner = Store::open_for_daemon(&db).await.unwrap();
    let slim = seed(&owner).await;
    let before = payloads(&owner, STAGED).await;
    let pool = if read_pool {
        owner.write_pool().close().await;
        owner.read_pool().clone()
    } else {
        owner.read_pool().close().await;
        owner.write_pool().clone()
    };
    // The facade disallows mutable/exported connection options at compile time.
    // Keep runtime renewal/detachment and original-body preservation coverage.
    retire_pool_connections(&pool).await;
    drop(owner);
    let pool = if handle == "facade-clone" {
        let retained = pool.clone();
        let settings = pool.options();
        assert_eq!(
            settings.get_max_connections(),
            if read_pool { 32 } else { 1 }
        );
        drop(pool);
        retained
    } else {
        pool
    };
    let mut connection = pool.acquire().await.unwrap().detach();
    if handle == "pool" {
        // Test the pool's own lifetime independently from a checked-out handle.
        connection.close().await.unwrap();
        assert!(
            Store::open_for_daemon(&db).await.is_err(),
            "renewed pool lost ownership, read_pool={read_pool}"
        );
        connection = pool.acquire().await.unwrap().detach();
    }
    pool.close().await;
    drop(pool);
    sqlx::query("UPDATE agent_session SET name = 'renewed' WHERE id = 'synthetic-agent'")
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(
        Store::open_for_daemon(&db).await.is_err(),
        "renewed {handle} lost ownership, read_pool={read_pool}"
    );
    let observer = Store::open(&db).await.unwrap();
    assert_eq!(payloads(&observer, STAGED).await, before);
    finalize(&observer, STAGED, slim).await;
    assert!(full_body_matches(&observer, STAGED).await);
    observer.close().await;
    connection.close().await.unwrap();
    assert_released(&db).await;
}

#[tokio::test]
async fn renewed_write_facade_retains_ownership() {
    renewed_facade_retains_ownership(false, "pool").await;
}
#[tokio::test]
async fn renewed_read_facade_retains_ownership() {
    renewed_facade_retains_ownership(true, "pool").await;
}
#[tokio::test]
async fn renewed_write_detached_connection_retains_ownership() {
    renewed_facade_retains_ownership(false, "detached").await;
}
#[tokio::test]
async fn renewed_read_detached_connection_retains_ownership() {
    renewed_facade_retains_ownership(true, "detached").await;
}
#[tokio::test]
async fn cloned_write_facade_retains_ownership() {
    renewed_facade_retains_ownership(false, "facade-clone").await;
}
#[tokio::test]
async fn cloned_read_facade_retains_ownership() {
    renewed_facade_retains_ownership(true, "facade-clone").await;
}

#[tokio::test]
async fn cancelled_query_retains_ownership_until_sqlite_worker_exits() {
    cancelled_query_ownership(None).await;
}
#[tokio::test]
async fn cancelled_renewed_write_query_retains_ownership() {
    cancelled_query_ownership(Some(false)).await;
}
#[tokio::test]
async fn cancelled_renewed_read_query_retains_ownership() {
    cancelled_query_ownership(Some(true)).await;
}

#[cfg(target_os = "linux")]
fn sqlite_workers() -> Vec<(PathBuf, String)> {
    fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            let name = fs::read_to_string(path.join("comm")).unwrap_or_default();
            name.starts_with("sqlx-sqlite").then(|| {
                let wait = fs::read_to_string(path.join("wchan")).unwrap_or_default();
                (path, wait)
            })
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn database_lock_available(db: &Path) -> bool {
    let mut lock = db.as_os_str().to_os_string();
    lock.push(".daemon.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(PathBuf::from(lock))
        .unwrap();
    Flock::lock(file, FlockArg::LockExclusiveNonblock).is_ok()
}

#[cfg(target_os = "linux")]
async fn renewal_initialization_ownership(read: bool) {
    use sqlx::Connection;
    let fixture = Fixture::new();
    let db = fixture.data_dir().join("intentd.db");
    let owner = Store::open_for_daemon(&db).await.unwrap();
    let slim = seed(&owner).await;
    let before = payloads(&owner, STAGED).await;
    let pool = if read {
        owner.write_pool().close().await;
        owner.read_pool().clone()
    } else {
        owner.read_pool().close().await;
        owner.write_pool().clone()
    };
    retire_pool_connections(&pool).await;
    drop(owner);
    let mut blocker = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(&db),
    )
    .await
    .unwrap();
    sqlx::query("PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE")
        .execute(&mut blocker)
        .await
        .unwrap();
    let prior: Vec<_> = sqlite_workers().into_iter().map(|(path, _)| path).collect();
    let acquisition = tokio::spawn(async move { pool.acquire().await });
    let worker = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if let Some((path, _)) = sqlite_workers()
                .into_iter()
                .find(|(path, wait)| !prior.contains(path) && wait.contains("hrtimer_nanosleep"))
            {
                break path;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("renewal must reach SQLite initialization busy wait");
    assert!(!database_lock_available(&db));
    acquisition.abort();
    assert!(acquisition.await.unwrap_err().is_cancelled());
    let worker_running = worker.exists();
    let exclusive = !database_lock_available(&db);
    println!(
        "read={read}, initialization worker alive={worker_running}, ownership retained={exclusive}"
    );
    // Always release the synthetic blocker before asserting, including fail-before.
    sqlx::query("COMMIT").execute(&mut blocker).await.unwrap();
    blocker.close().await.unwrap();
    assert!(
        worker_running && exclusive,
        "cancelled initialization must retain exclusive ownership, read={read}"
    );
    let observer = Store::open(&db).await.unwrap();
    assert_eq!(payloads(&observer, STAGED).await, before);
    finalize(&observer, STAGED, slim).await;
    assert!(full_body_matches(&observer, STAGED).await);
    observer.close().await;
    assert_released(&db).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn write_renewal_initialization_retains_ownership_after_cancellation() {
    renewal_initialization_ownership(false).await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn read_renewal_initialization_retains_ownership_after_cancellation() {
    renewal_initialization_ownership(true).await;
}
