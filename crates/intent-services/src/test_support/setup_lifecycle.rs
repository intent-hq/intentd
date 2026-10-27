/// Temp directory tree swept on drop (see [`test_tempdir`]).
pub(crate) struct TempDir(pub(crate) PathBuf, #[expect(dead_code)] tempfile::TempDir);

pub(crate) fn unique_dir(prefix: &str) -> TempDir {
    let guard = test_tempdir(&format!("{prefix}-"));
    TempDir(guard.path().to_path_buf(), guard)
}

/// Init a git repo with one commit; returns (guard, head branch).
pub(crate) fn seed_repo(prefix: &str) -> (TempDir, String) {
    let dir = unique_dir(prefix);
    let repo = git2::Repository::init(&dir.0).unwrap();
    {
        let mut cfg = repo.config().unwrap();
        cfg.set_str("user.name", "Tester").unwrap();
        cfg.set_str("user.email", "t@e.dev").unwrap();
    }
    std::fs::write(dir.0.join("README.md"), "init\n").unwrap();
    let mut index = repo.index().unwrap();
    index
        .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
        .unwrap();
    index.write().unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let sig = git2::Signature::now("Tester", "t@e.dev").unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, "chore: init", &tree, &[])
        .unwrap();
    let branch = repo.head().unwrap().shorthand().unwrap().to_string();
    (dir, branch)
}

pub(crate) async fn services(root: PathBuf) -> (Services, EventBus, TempDb) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let svc = Services::new(store)
        .with_workspaces_root(root)
        .with_event_bus(bus.clone());
    (svc, bus, tmp)
}

/// Subscribe to only the setup lifecycle events (the setup terminal's
/// `terminal:*` stream and the create's other events stay out of scope).
pub(crate) fn subscribe_setup(bus: &EventBus) -> Subscription {
    bus.subscribe(SubscriptionFilter {
        event_types: vec!["workspace:setup:*".to_string()],
        ..Default::default()
    })
}

/// Receive one setup event (generous timeout: script paths cross a PTY
/// spawn + reap poll), serialized to wire JSON.
pub(crate) async fn recv_setup(sub: &mut Subscription) -> Value {
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(20)),
        sub.recv(),
    )
    .await
    .expect("setup event delivered")
    .expect("subscription open");
    assert_eq!(batch.len(), 1, "expected exactly one event per delivery");
    serde_json::to_value(&batch[0]).expect("serialize event")
}

pub(crate) async fn assert_quiet(sub: &mut Subscription) {
    let quiet = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_millis(300)),
        sub.recv(),
    )
    .await;
    assert!(quiet.is_err(), "no further setup events, got: {quiet:?}");
}
