//! Regression guard: a representative services test flow can never read,
//! write, create, rename or migrate anything under the developer's real
//! `~/intent` (intentd#2736 wrote test credentials into the real
//! `~/intent/.secrets.json.collaboration`; `Services::new` could rename the
//! real legacy `~/intent/secrets.json`).

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{setup, Services, TempDb};
use intent_store::Store;

const FORBIDDEN_HOME: &str = "INTENTD_TEST_FORBIDDEN_HOME";
const SECRETS_FILE: &str = "INTENTD_SECRETS_FILE";
const WORKSPACES_DIR: &str = "INTENTD_WORKSPACES_DIR";
const ASSERT_HERMETIC_ROOT: &str = "INTENTD_ASSERT_HERMETIC_ROOT";

/// Points `HOME` and the forbidden-home marker at a sentinel directory and
/// restores both on drop. Nextest runs each test in its own process, and the
/// lock serializes these tests under plain `cargo test`.
struct SentinelHome {
    dir: tempfile::TempDir,
    saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

impl SentinelHome {
    fn new() -> Self {
        let lock = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = crate::test_support::test_tempdir("intent-home-isolation-");
        let intent = dir.path().join("intent");
        std::fs::create_dir_all(&intent).expect("sentinel intent dir");
        std::fs::write(intent.join("secrets.json"), r#"{"real":"user-secret"}"#)
            .expect("legacy secrets");
        let saved = [
            "HOME",
            "USERPROFILE",
            SECRETS_FILE,
            FORBIDDEN_HOME,
            WORKSPACES_DIR,
            ASSERT_HERMETIC_ROOT,
        ]
        .into_iter()
        .map(|k| (k, std::env::var_os(k)))
        .collect();
        std::env::set_var("HOME", dir.path());
        std::env::set_var("USERPROFILE", dir.path());
        std::env::remove_var(SECRETS_FILE);
        std::env::remove_var(WORKSPACES_DIR);
        std::env::remove_var(ASSERT_HERMETIC_ROOT);
        std::env::set_var(FORBIDDEN_HOME, dir.path());
        Self {
            dir,
            saved,
            _lock: lock,
        }
    }

    fn snapshot(&self) -> Vec<(PathBuf, Vec<u8>, SystemTime)> {
        fn walk(root: &Path, dir: &Path, out: &mut Vec<(PathBuf, Vec<u8>, SystemTime)>) {
            let mut entries: Vec<_> = std::fs::read_dir(dir)
                .expect("read sentinel")
                .map(|e| e.expect("entry").path())
                .collect();
            entries.sort();
            for path in entries {
                let meta = std::fs::metadata(&path).expect("metadata");
                let rel = path.strip_prefix(root).expect("relative").to_path_buf();
                let modified = meta.modified().expect("mtime");
                if meta.is_dir() {
                    out.push((rel, Vec::new(), modified));
                    walk(root, &path, out);
                } else {
                    out.push((rel, std::fs::read(&path).expect("read"), modified));
                }
            }
        }
        let mut out = Vec::new();
        walk(self.dir.path(), self.dir.path(), &mut out);
        out
    }
}

impl Drop for SentinelHome {
    fn drop(&mut self) {
        for (key, value) in &self.saved {
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }
}

/// The shared `setup()` fixture plus a secret write leaves the home untouched.
#[tokio::test]
async fn home_isolation_setup_fixture_leaves_home_untouched() {
    let home = SentinelHome::new();
    let before = home.snapshot();

    let (_db, services, _ws, _note) = setup("body").await;
    services
        .secrets
        .store("home-isolation.probe", "test-only")
        .await
        .expect("store probe secret");
    assert_eq!(
        services
            .secrets
            .load("home-isolation.probe")
            .await
            .expect("load probe secret"),
        Some("test-only".to_string())
    );
    drop(services);

    assert_eq!(home.snapshot(), before, "sentinel home must stay untouched");
}

/// The production default constructor never resolves the real home's
/// secrets store under the test harness: it is redirected to an empty
/// scratch store, so the real tokens are unreadable and writes land outside
/// the home. The token loaders read the same store.
#[tokio::test]
async fn home_isolation_default_services_redirect_away_from_forbidden_home() {
    let home = SentinelHome::new();
    std::fs::write(
        home.dir.path().join("intent").join(".secrets.json"),
        r#"{"real":"user-secret","sourceControl.github.token":"real-gh-token"}"#,
    )
    .expect("real dotfile secrets");
    let before = home.snapshot();
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");

    let services = Services::new(store);
    assert!(
        !services
            .gitlab_secret_store
            .path()
            .starts_with(home.dir.path()),
        "default Services must not use the forbidden home's store"
    );
    assert_eq!(
        services.secrets.load("real").await.expect("load"),
        None,
        "the real secrets must not be readable"
    );
    services
        .secrets
        .store("home-isolation.probe", "redirected")
        .await
        .expect("store probe secret");
    assert_eq!(
        intent_sourcecontrol::token::resolve(
            &intent_sourcecontrol::TokenSource::Explicit,
            &services.secret_source(),
        )
        .await,
        None,
        "the GitHub token loader must not read the forbidden home"
    );
    let scratch = services
        .gitlab_secret_store
        .path()
        .parent()
        .map(Path::to_path_buf);
    drop(services);
    if let Some(dir) = scratch {
        let _ = std::fs::remove_dir_all(dir);
    }

    assert_eq!(home.snapshot(), before, "sentinel home must stay untouched");
}

/// `Services::new_isolated` with no explicit root owns a scratch workspaces
/// root: it never resolves the default (`$INTENTD_WORKSPACES_DIR`, else the
/// home's `~/intent/workspaces`). Plain `cargo test` has no hermetic guard,
/// so a test that deleted a workspace under the default reached the real
/// workspaces root. The root is removed when the last clone drops.
#[tokio::test]
async fn home_isolation_isolated_services_own_a_scratch_workspaces_root() {
    let home = SentinelHome::new();
    let env_root = home.dir.path().join("env-workspaces");
    std::env::set_var(WORKSPACES_DIR, &env_root);
    let db = TempDb::new();
    let store = Store::open(&db.path).await.expect("open store");

    let services = Services::new_isolated(store);
    let root = services
        .configured_workspaces_root()
        .map(Path::to_path_buf)
        .expect("new_isolated must own a workspaces root");
    assert!(
        !root.starts_with(home.dir.path()),
        "the isolated root {} must not be the home workspaces root or the env override",
        root.display()
    );
    assert_ne!(root, env_root);

    std::fs::create_dir_all(root.join("probe")).expect("create under scratch root");
    let clone = services.clone();
    drop(services);
    assert!(root.exists(), "a live clone keeps the scratch root");
    drop(clone);
    assert!(!root.exists(), "the last clone removes the scratch root");
}
