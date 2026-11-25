//! A store-backed settings registry ([`SettingsRegistry::load_from_store`]):
//! nothing reaches disk, each write is a compare-and-set on the document's
//! revision (of two registries writing from one revision, one wins and the
//! other gets `Error::Conflict`), and another registry's write is picked up
//! by re-reading the revision — in place ([`SettingsRegistry::refresh`]) or
//! through `Services` with its event ([`crate::settings_store_follower`]).
//! Also the GitLab pair's handle over a non-file backend.

use std::sync::{Arc, Mutex};

use intent_core::{Error, Result, SecretStore as _, WorkspaceApi};
use intent_store::Store;
use serde_json::{json, Value};

use crate::settings_registry::{SettingsDocumentStore, SettingsWrite, StoredSettings};
use crate::settings_store_follower::{poll_once, FollowOutcome};
use crate::{InMemorySecretStore, PreparedReloadAdmission, Services, SettingsRegistry};

/// One shared in-memory document, as a tenant's row would be.
#[derive(Default, Clone)]
struct MemoryDocument {
    row: Arc<Mutex<Option<StoredSettings>>>,
    writes: Arc<Mutex<u32>>,
}

impl MemoryDocument {
    fn current(&self) -> Option<StoredSettings> {
        self.row.lock().unwrap().clone()
    }
}

impl SettingsDocumentStore for MemoryDocument {
    fn load(&self) -> Result<Option<StoredSettings>> {
        Ok(self.current())
    }

    fn compare_and_set(&self, expected: u64, text: &str) -> Result<SettingsWrite> {
        let mut row = self.row.lock().unwrap();
        let current = row.as_ref().map_or(0, |r| r.revision);
        if current != expected {
            return Ok(SettingsWrite::Conflict(current));
        }
        *row = Some(StoredSettings {
            text: text.to_owned(),
            revision: current + 1,
        });
        *self.writes.lock().unwrap() += 1;
        Ok(SettingsWrite::Written(current + 1))
    }

    fn label(&self) -> String {
        "memory/tenant".into()
    }
}

fn set(path: &str, value: Value) -> Vec<(String, Value)> {
    vec![(path.to_owned(), value)]
}

fn registry(doc: &MemoryDocument) -> Arc<SettingsRegistry> {
    Arc::new(SettingsRegistry::load_from_store(Arc::new(doc.clone())).unwrap())
}

#[test]
fn a_fresh_document_reads_the_defaults_and_writes_nothing_until_a_change() {
    let doc = MemoryDocument::default();
    let reg = registry(&doc);
    assert!(reg.is_store_backed());
    assert_eq!(reg.file_path(), None);
    assert_eq!(reg.store_revision(), 0);
    assert_eq!(reg.get("git.autoCommit"), Some(json!(true)));
    assert_eq!(*doc.writes.lock().unwrap(), 0, "loading writes nothing");

    reg.apply(&set("git.autoCommit", json!(false))).unwrap();
    assert_eq!(reg.store_revision(), 1);
    let stored = doc.current().unwrap();
    assert_eq!(stored.revision, 1);
    assert!(
        stored.text.contains("autoCommit = false"),
        "{}",
        stored.text
    );
    // The rendered document keeps the template's comments, like the file.
    assert!(
        stored.text.starts_with("# intentd configuration"),
        "{}",
        stored.text
    );

    reg.apply(&set("git.autoCommit", json!(true))).unwrap();
    assert_eq!(reg.store_revision(), 2);
    let reloaded = registry(&doc);
    assert_eq!(reloaded.store_revision(), 2);
    assert_eq!(reloaded.get("git.autoCommit"), Some(json!(true)));
}

#[test]
fn of_two_registries_writing_from_one_revision_one_wins_and_the_other_conflicts() {
    let doc = MemoryDocument::default();
    let a = registry(&doc);
    let b = registry(&doc);
    a.apply(&set("git.autoCommit", json!(false))).unwrap();
    let err = b
        .apply(&set("agents.maxConcurrent", json!(3)))
        .expect_err("b still holds revision 0");
    let Error::Conflict { current } = &err else {
        panic!("expected a revision conflict, got {err:?}");
    };
    assert_eq!(current["settingsRevision"], json!(1));
    assert_eq!(current["heldRevision"], json!(0));
    assert_eq!(err.code(), -32005);
    // The loser kept its state and wrote nothing.
    assert_eq!(b.store_revision(), 0);
    assert_eq!(
        b.get("agents.maxConcurrent"),
        registry(&MemoryDocument::default()).get("agents.maxConcurrent")
    );
    assert_eq!(doc.current().unwrap().revision, 1);
    assert!(!doc.current().unwrap().text.contains("maxConcurrent = 3"));

    // Once it re-reads, its retry wins.
    let mut changes = b.subscribe();
    let notice = b.refresh().unwrap();
    assert!(notice.changed.contains("git.autoCommit"), "{notice:?}");
    assert!(
        changes.has_changed().unwrap(),
        "refresh notifies the watch channel"
    );
    assert_eq!(changes.borrow_and_update().changed, notice.changed);
    assert_eq!(b.get("git.autoCommit"), Some(json!(false)));
    assert_eq!(b.store_revision(), 1);
    b.apply(&set("agents.maxConcurrent", json!(3))).unwrap();
    assert_eq!(doc.current().unwrap().revision, 2);
    a.refresh().unwrap();
    assert_eq!(a.get("agents.maxConcurrent"), Some(json!(3)));
    assert_eq!(a.get("git.autoCommit"), Some(json!(false)));
}

#[test]
fn refresh_of_a_current_registry_changes_nothing_and_an_older_revision_is_refused() {
    let doc = MemoryDocument::default();
    let reg = registry(&doc);
    assert!(reg.fetch_newer().unwrap().is_none(), "nothing written yet");
    reg.apply(&set("git.autoCommit", json!(false))).unwrap();
    // Its own write never looks newer: no self-write guard needed.
    assert!(reg.fetch_newer().unwrap().is_none());
    assert!(reg.refresh().unwrap().changed.is_empty());
    let stale = StoredSettings {
        text: crate::settings_registry::tests_default_template().to_owned(),
        revision: 1,
    };
    let err = reg
        .prepare_stored_reload_at(&stale, reg.snapshot())
        .err()
        .expect("a revision the registry already holds is refused");
    assert!(err.to_string().contains("not newer"), "{err}");
    assert_eq!(reg.get("git.autoCommit"), Some(json!(false)));
}

#[test]
fn a_malformed_stored_document_keeps_the_last_good_state() {
    let doc = MemoryDocument::default();
    let reg = registry(&doc);
    reg.apply(&set("git.autoCommit", json!(false))).unwrap();
    *doc.row.lock().unwrap() = Some(StoredSettings {
        text: "[git]\nautoCommit = 'nope'\n".into(),
        revision: 2,
    });
    assert!(reg.refresh().is_err());
    assert_eq!(reg.get("git.autoCommit"), Some(json!(false)));
    assert_eq!(reg.store_revision(), 1);
}

#[test]
fn a_store_backed_registry_cannot_be_file_watched() {
    let doc = MemoryDocument::default();
    let reg = registry(&doc);
    let hub = Arc::new(crate::SharedWatchHub::new());
    let err = crate::ConfigWatcher::start(&hub, reg, Arc::default(), |_| async {})
        .err()
        .expect("no file to watch");
    assert!(err.to_string().contains("store-backed"), "{err}");
}

#[test]
fn a_file_backed_registry_has_no_store_revision_to_follow() {
    let dir = crate::test_support::test_tempdir("settings-file-backed");
    let reg = SettingsRegistry::load(dir.path().join("config.toml")).unwrap();
    assert!(!reg.is_store_backed());
    assert_eq!(
        reg.file_path(),
        Some(dir.path().join("config.toml").as_path())
    );
    assert!(reg.fetch_newer().unwrap().is_none());
    assert!(reg.refresh().unwrap().changed.is_empty());
}

async fn tenant_services(
    dir: &tempfile::TempDir,
    doc: &MemoryDocument,
) -> (Services, crate::EventBus) {
    let store = Store::open(&dir.path().join("state.db")).await.unwrap();
    let bus = crate::EventBus::new(store.clone());
    let services = Services::new_isolated(store)
        .with_settings_registry(registry(doc))
        .with_secret_store(Arc::new(InMemorySecretStore::default()))
        .with_event_bus(bus.clone());
    (services, bus)
}

/// The follower adopts another process's write through `Services`: the
/// `settings.get` view moves, and `settings:changed` is published.
#[tokio::test]
async fn the_follower_adopts_another_writers_revision_with_its_event() {
    let dir = crate::test_support::test_tempdir("settings-follower");
    let doc = MemoryDocument::default();
    let (svc, bus) = tenant_services(&dir, &doc).await;
    let mut events = bus.subscribe(crate::events::SubscriptionFilter {
        event_types: vec![intent_core::events::SETTINGS_CHANGED.to_string()],
        ..Default::default()
    });
    let (_stop, stopped) = tokio::sync::watch::channel(false);
    assert!(matches!(
        poll_once(&svc, PreparedReloadAdmission::new(stopped.clone()))
            .await
            .unwrap(),
        FollowOutcome::Current
    ));

    // Another process writes the tenant's document.
    let other = registry(&doc);
    other.apply(&set("git.autoCommit", json!(false))).unwrap();

    let outcome = poll_once(&svc, PreparedReloadAdmission::new(stopped.clone()))
        .await
        .unwrap();
    let FollowOutcome::Adopted(notice) = outcome else {
        panic!("expected the newer revision to be adopted");
    };
    assert!(notice.changed.contains("git.autoCommit"));
    let got = intent_core::with_caller(
        intent_core::Caller::Daemon,
        svc.settings_get("git.autoCommit".into()),
    )
    .await
    .unwrap();
    assert_eq!(got["value"], json!(false));
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(10)),
        events.recv(),
    )
    .await
    .expect("settings:changed")
    .unwrap();
    assert!(
        batch[0].data.to_string().contains("git.autoCommit"),
        "{}",
        batch[0].data
    );

    // And this process's own write is not re-adopted.
    intent_core::with_caller(
        intent_core::Caller::Daemon,
        svc.settings_update(json!([{ "path": "agents.maxConcurrent", "value": 4 }])),
    )
    .await
    .unwrap();
    assert!(matches!(
        poll_once(&svc, PreparedReloadAdmission::new(stopped))
            .await
            .unwrap(),
        FollowOutcome::Current
    ));
    assert_eq!(doc.current().unwrap().revision, 2);
}

/// A `settings.update` that loses the race surfaces the conflict to the
/// caller and writes nothing, but adopts the winner's document before it
/// returns, so a retry made right away lands (no wait for the follower).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_settings_update_that_lost_the_race_is_a_conflict_and_a_retry_lands() {
    let dir = crate::test_support::test_tempdir("settings-update-conflict");
    let doc = MemoryDocument::default();
    let (svc, _bus) = tenant_services(&dir, &doc).await;
    registry(&doc)
        .apply(&set("git.autoCommit", json!(false)))
        .unwrap();
    let err = intent_core::with_caller(
        intent_core::Caller::Daemon,
        svc.settings_update(json!([{ "path": "agents.maxConcurrent", "value": 4 }])),
    )
    .await
    .expect_err("the tenant's document moved on");
    assert!(matches!(err, Error::Conflict { .. }), "{err:?}");
    assert_eq!(doc.current().unwrap().revision, 1);
    assert!(!doc.current().unwrap().text.contains("maxConcurrent = 4"));

    // The loser already holds the winner's revision and value.
    let registry = svc.settings_registry().unwrap();
    assert_eq!(registry.store_revision(), 1);
    let got = intent_core::with_caller(
        intent_core::Caller::Daemon,
        svc.settings_get("git.autoCommit".into()),
    )
    .await
    .unwrap();
    assert_eq!(got["value"], json!(false), "the winner's value is served");
    intent_core::with_caller(
        intent_core::Caller::Daemon,
        svc.settings_update(json!([{ "path": "agents.maxConcurrent", "value": 4 }])),
    )
    .await
    .expect("an immediate retry writes from the winner's revision");
    let stored = doc.current().unwrap();
    assert_eq!(stored.revision, 2);
    assert!(stored.text.contains("maxConcurrent = 4"), "{}", stored.text);
    assert!(
        stored.text.contains("autoCommit = false"),
        "the winner's change is kept"
    );
}

/// A document store whose next compare-and-set blocks until released, as a
/// busy tenant's writer lock would.
#[derive(Clone)]
struct StalledDocument {
    doc: MemoryDocument,
    entered: Arc<std::sync::Barrier>,
    release: Arc<std::sync::Barrier>,
}

impl SettingsDocumentStore for StalledDocument {
    fn load(&self) -> Result<Option<StoredSettings>> {
        self.doc.load()
    }

    fn compare_and_set(&self, expected: u64, text: &str) -> Result<SettingsWrite> {
        self.entered.wait();
        self.release.wait();
        self.doc.compare_and_set(expected, text)
    }

    fn label(&self) -> String {
        "memory/stalled".into()
    }
}

/// While a store write waits on the database, readers of the registry —
/// the revision, the generation, validation, previews, snapshots — and the
/// follower answer at once instead of queueing behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readers_and_the_follower_never_wait_on_an_in_flight_store_write() {
    let dir = crate::test_support::test_tempdir("settings-stalled-write");
    let stalled = StalledDocument {
        doc: MemoryDocument::default(),
        entered: Arc::new(std::sync::Barrier::new(2)),
        release: Arc::new(std::sync::Barrier::new(2)),
    };
    let registry = Arc::new(SettingsRegistry::load_from_store(Arc::new(stalled.clone())).unwrap());
    let store = Store::open(&dir.path().join("state.db")).await.unwrap();
    let svc = Services::new_isolated(store)
        .with_settings_registry(registry.clone())
        .with_secret_store(Arc::new(InMemorySecretStore::default()));

    let writer = {
        let registry = registry.clone();
        std::thread::spawn(move || registry.apply(&set("git.autoCommit", json!(false))))
    };
    stalled.entered.wait();
    // The write is inside its compare-and-set now.
    assert!(registry.write_in_flight());
    assert_eq!(registry.store_revision(), 0);
    assert_eq!(registry.generation(), 0);
    registry
        .validate(&set("agents.maxConcurrent", json!(2)))
        .unwrap();
    registry
        .preview(&set("agents.maxConcurrent", json!(2)))
        .unwrap();
    assert_eq!(registry.get("git.autoCommit"), Some(json!(true)));
    assert!(!registry.is_self_write("x"));
    let (_running, stopped) = tokio::sync::watch::channel(false);
    assert!(matches!(
        poll_once(&svc, PreparedReloadAdmission::new(stopped))
            .await
            .unwrap(),
        FollowOutcome::Current
    ));

    stalled.release.wait();
    writer.join().unwrap().unwrap();
    assert!(!registry.write_in_flight());
    assert_eq!(registry.store_revision(), 1);
    assert_eq!(registry.get("git.autoCommit"), Some(json!(false)));
}

/// The GitLab pair's handle over a backend: every read and write goes to
/// the backend (no file is created), and a derived store keeps its
/// accounts apart under its scope.
#[test]
fn a_backend_secret_handle_writes_no_file_and_scopes_derived_stores() {
    let dir = crate::test_support::test_tempdir("secret-backend");
    let backend = InMemorySecretStore::default();
    let identity = dir.path().join("pair");
    let pair = intent_core::FileSecretStore::with_backend(&identity, Arc::new(backend.clone()));
    assert!(!pair.is_file());
    assert_eq!(pair.path(), identity.as_path());
    pair.store("sourceControl.gitlab.token", "glpat-x").unwrap();
    pair.store("sourceControl.gitlab.refreshToken", "glrt-x")
        .unwrap();
    assert_eq!(
        pair.load_many(&[
            "sourceControl.gitlab.token",
            "sourceControl.gitlab.refreshToken",
            "none"
        ])
        .unwrap(),
        [Some("glpat-x".into()), Some("glrt-x".into()), None]
    );
    assert_eq!(
        backend
            .load("sourceControl.gitlab.token")
            .unwrap()
            .as_deref(),
        Some("glpat-x")
    );
    let derived = pair.derived(
        dir.path().join("pair.collaboration/x.json"),
        "collaboration/x",
    );
    derived
        .store("sourceControl.gitlab.token", "collab")
        .unwrap();
    assert_eq!(
        backend
            .load("collaboration/x/sourceControl.gitlab.token")
            .unwrap()
            .as_deref(),
        Some("collab")
    );
    assert_eq!(
        pair.load("sourceControl.gitlab.token").unwrap().as_deref(),
        Some("glpat-x")
    );
    pair.delete("sourceControl.gitlab.token").unwrap();
    assert_eq!(pair.load("sourceControl.gitlab.token").unwrap(), None);
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "no file was written"
    );

    // A file handle's derived store is the file at the given path.
    let file = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
    assert!(file.is_file());
    let sibling = file.derived(dir.path().join("sibling.json"), "ignored");
    sibling.store("a", "b").unwrap();
    assert!(dir.path().join("sibling.json").exists());
}
