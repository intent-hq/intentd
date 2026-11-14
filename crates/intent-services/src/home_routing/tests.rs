use super::*;

#[test]
fn file_git_and_search_calls_are_routed_but_hub_and_upload_calls_are_not() {
    for method in [
        "file.write",
        "file.read",
        "git.status",
        "git.agentCommit",
        "git.pull",
        "git.fetch",
        "file-tracking.getChanges",
        "search.inFiles",
        "file.getAttachmentInfo",
    ] {
        assert!(routed(method), "{method}");
    }
    for method in [
        "git.push",
        "git.clone",
        "git.getConfig",
        "git.getRemoteUrl",
        "git.removeLockFile",
        "file.placeAttachment",
        "file.attachmentUpload.begin",
        "file.attachmentUpload.chunk",
        "file.attachmentUpload.commit",
        "file.attachmentUpload.abort",
        "host.exec",
        "terminal.create",
    ] {
        assert!(!routed(method), "{method}");
    }
}

#[test]
fn absolute_paths_under_the_worktree_become_checkout_paths() {
    let dir = tempfile::tempdir().unwrap();
    let worktree = dir.path().join("work");
    std::fs::create_dir_all(worktree.join("src")).unwrap();
    let abs = |rel: &str| worktree.join(rel).to_string_lossy().into_owned();
    assert_eq!(
        checkout_relative(&worktree, "src/a.rs").as_deref(),
        Some("src/a.rs")
    );
    assert_eq!(
        checkout_relative(&worktree, &abs("src/a.rs")).as_deref(),
        Some("src/a.rs")
    );
    // A file that does not exist yet (a write makes it).
    assert_eq!(
        checkout_relative(&worktree, &abs("new/deeper/b.txt")).as_deref(),
        Some("new/deeper/b.txt")
    );
    assert_eq!(
        checkout_relative(&worktree, &worktree.to_string_lossy()).as_deref(),
        Some("")
    );
    // Elsewhere, a sibling sharing the prefix, or climbing out.
    assert_eq!(checkout_relative(&worktree, "/etc/passwd"), None);
    let sibling = dir.path().join("work2").join("x");
    assert_eq!(
        checkout_relative(&worktree, &sibling.to_string_lossy()),
        None
    );
    assert_eq!(checkout_relative(&worktree, &abs("src/../../x")), None);
}

#[cfg(unix)]
#[test]
fn a_worktree_reached_through_a_link_maps_too() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    std::fs::create_dir_all(real.join("src")).unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let through_link = link.join("src").join("a.rs");
    assert_eq!(
        checkout_relative(&real, &through_link.to_string_lossy()).as_deref(),
        Some("src/a.rs")
    );
    let real_path = real.join("src").join("a.rs");
    assert_eq!(
        checkout_relative(&link, &real_path.to_string_lossy()).as_deref(),
        Some("src/a.rs")
    );
}

#[test]
fn params_are_mapped_and_outside_paths_refused() {
    let dir = tempfile::tempdir().unwrap();
    let worktree = dir.path().join("work");
    std::fs::create_dir_all(&worktree).unwrap();
    let abs = |rel: &str| worktree.join(rel).to_string_lossy().into_owned();
    let params = json!({
        "workspaceId": "ws",
        "path": abs("a.txt"),
        "oldPath": "b.txt",
        "paths": [abs("c.txt"), "d.txt"],
        "files": format!("{}, e.txt", abs("f.txt")),
        "repoPath": abs(""),
        "content": abs("not/a/path/param"),
    });
    let Value::Object(params) = params else {
        unreachable!()
    };
    let mapped = home_params(&worktree, params).unwrap();
    assert_eq!(mapped["path"], "a.txt");
    assert_eq!(mapped["oldPath"], "b.txt");
    assert_eq!(mapped["paths"], json!(["c.txt", "d.txt"]));
    assert_eq!(mapped["files"], "f.txt,e.txt");
    assert_eq!(mapped["repoPath"], "");
    assert_eq!(
        mapped["content"],
        abs("not/a/path/param"),
        "contents are left alone"
    );
    assert_eq!(mapped["workspaceId"], "ws");

    for (param, value) in [
        ("path", json!("/etc/passwd")),
        ("paths", json!(["ok.txt", "/etc/passwd"])),
        ("files", json!("ok.txt,/etc/passwd")),
        ("filePath", json!("/tmp/elsewhere")),
    ] {
        let mut params = Map::new();
        params.insert(param.to_owned(), value);
        let error = home_params(&worktree, params).unwrap_err();
        assert!(
            matches!(&error, Error::InvalidParams(message) if message.contains("outside the workspace's checkout")),
            "{param}: {error:?}"
        );
    }
}

#[test]
fn a_path_based_call_for_another_repository_runs_here() {
    let dir = tempfile::tempdir().unwrap();
    let worktree = dir.path().join("work");
    std::fs::create_dir_all(&worktree).unwrap();
    let with = |repo: Option<&str>| {
        let mut params = Map::new();
        if let Some(repo) = repo {
            params.insert("repoPath".into(), json!(repo));
        }
        params
    };
    assert!(names_checkout(
        &worktree,
        &with(Some(&worktree.to_string_lossy()))
    ));
    assert!(names_checkout(&worktree, &with(None)));
    assert!(!names_checkout(&worktree, &with(Some("/some/other/repo"))));
    assert!(!names_checkout(&worktree, &with(Some("relative"))));
}

#[tokio::test]
async fn payload_pieces_cover_the_bytes_in_order() {
    let bytes: Vec<u8> = (0..(HOME_PIECE_BYTES * 2 + 17))
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("payload.bin");
    std::fs::write(&file, &bytes).unwrap();
    let expected = sha2::Sha256::digest(&bytes)
        .iter()
        .fold(String::new(), |mut hex, b| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{b:02x}");
            hex
        });
    for source in [
        crate::file_ops::AttachmentSource::Bytes(&bytes),
        crate::file_ops::AttachmentSource::CopyFrom(&file),
    ] {
        let payload = Payload::open(&source).await.unwrap();
        assert_eq!(payload.size, bytes.len() as u64);
        let mut joined = Vec::new();
        for seq in 0..3 {
            let piece = payload.piece(seq).await.unwrap();
            assert!(piece.len() <= HOME_PIECE_BYTES);
            joined.extend(piece);
        }
        assert_eq!(joined, bytes);
        assert_eq!(payload.sha256().await.unwrap(), expected);
    }
    let missing = dir.path().join("missing.bin");
    let source = crate::file_ops::AttachmentSource::CopyFrom(&missing);
    assert!(matches!(
        Payload::open(&source).await,
        Err(Error::InvalidParams(message)) if message.contains("does not exist")
    ));
}

/// A home whose searches run until they are dropped (the connector then
/// sends `homeCallCancel`), or that answers every call with `reply`.
struct SearchingHome {
    reply: Option<Value>,
    sent: std::sync::Mutex<Vec<(String, Map<String, Value>)>>,
    started: tokio::sync::Notify,
    dropped: std::sync::atomic::AtomicUsize,
}

/// Counts a home call dropped before it answered.
struct Dropped<'a>(&'a std::sync::atomic::AtomicUsize);

impl Drop for Dropped<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Lets [`SearchingHome`] be the services' port while the test keeps it.
struct Port(std::sync::Arc<SearchingHome>);

impl crate::feature_ports::RemoteLaunchPort for Port {
    fn placements<'a>(
        &'a self,
        svc: &'a Services,
    ) -> intent_core::BoxFuture<'a, Result<intent_core::placement::PlatformAvailability>> {
        crate::feature_ports::UnavailableRemoteLaunch.placements(svc)
    }
    fn placement_help<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        render: intent_core::machine_selection::PlacementHelpRenderer,
    ) -> intent_core::BoxFuture<'a, Option<String>> {
        crate::feature_ports::UnavailableRemoteLaunch.placement_help(svc, workspace_id, render)
    }
    fn admit<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        placement: &'a intent_core::placement::AgentPlacement,
        needs: &'a intent_core::remote_launch::RemoteLaunchNeeds,
    ) -> intent_core::BoxFuture<'a, Result<intent_core::remote_launch::RemotePlacementView>> {
        crate::feature_ports::UnavailableRemoteLaunch.admit(svc, workspace_id, placement, needs)
    }
    fn launch<'a>(
        &'a self,
        svc: &'a Services,
        request: intent_core::remote_launch::RemoteLaunchRequest,
    ) -> intent_core::BoxFuture<'a, Result<()>> {
        crate::feature_ports::UnavailableRemoteLaunch.launch(svc, request)
    }
    fn runtime<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a intent_core::AgentId,
    ) -> intent_core::BoxFuture<'a, Result<std::sync::Arc<crate::feature_ports::RemoteRuntime>>>
    {
        crate::feature_ports::UnavailableRemoteLaunch.runtime(svc, agent_id)
    }
    fn home_call<'a>(
        &'a self,
        _svc: &'a Services,
        _home: &'a WorkspaceHome,
        _workspace_id: &'a WorkspaceId,
        method: &'a str,
        params: Map<String, Value>,
        _timeout: Duration,
    ) -> intent_core::BoxFuture<
        'a,
        Result<std::result::Result<Value, intent_core::node_link::HomeCallError>>,
    > {
        let home = &self.0;
        lock_sent(home).push((method.to_owned(), params));
        Box::pin(async move {
            if let Some(reply) = home.reply.clone() {
                return Ok(Ok(reply));
            }
            let _dropped = Dropped(&home.dropped);
            home.started.notify_one();
            std::future::pending().await
        })
    }
}

fn lock_sent(home: &SearchingHome) -> std::sync::MutexGuard<'_, Vec<(String, Map<String, Value>)>> {
    home.sent.lock().unwrap()
}

struct HomeHarness {
    _dir: tempfile::TempDir,
    services: Services,
    bus: crate::EventBus,
    home: std::sync::Arc<SearchingHome>,
    ws: WorkspaceId,
}

/// An orchestrator with one workspace whose home is on a machine.
async fn home_harness(reply: Option<Value>) -> HomeHarness {
    // Homes are on for this test process only (nextest runs each test in
    // its own process).
    std::env::set_var("INTENTD_UNSTABLE_HOMES", "1");
    let dir = crate::test_support::test_tempdir("home-search-cancel");
    let worktree = dir.path().join("work");
    std::fs::create_dir_all(&worktree).unwrap();
    let store = intent_store::Store::open(&dir.path().join("state.db"))
        .await
        .unwrap();
    let ws = WorkspaceId::new();
    let now = intent_core::now_iso();
    store
        .insert_workspace(&intent_core::Workspace {
            id: ws.clone(),
            title: "WS".into(),
            branch: "main".into(),
            created_at: now.clone(),
            updated_at: now.clone(),
            worktree_path: Some(worktree.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .await
        .unwrap();
    store
        .set_workspace_home(
            &ws,
            &WorkspaceHome {
                home_id: intent_core::HomeId::new(),
                node_id: Some("node-1".into()),
                cloud_machine_id: None,
                name: "Machine".into(),
                state: intent_core::HomeState::Attached,
                since: now,
                last_backup_at: None,
                backup_warning: false,
            },
        )
        .await
        .unwrap();
    let home = std::sync::Arc::new(SearchingHome {
        reply,
        sent: std::sync::Mutex::default(),
        started: tokio::sync::Notify::new(),
        dropped: std::sync::atomic::AtomicUsize::new(0),
    });
    let bus = crate::EventBus::new(store.clone());
    let services = Services::new_isolated(store)
        .with_event_bus(bus.clone())
        .with_orchestrator_mode(true)
        .with_remote_launch_port(std::sync::Arc::new(Port(home.clone())));
    assert!(services.workspace_homes());
    HomeHarness {
        _dir: dir,
        services,
        bus,
        home,
        ws,
    }
}

/// `search.cancel` ends a search running on the home: the call to the home
/// is dropped (the connector then sends `homeCallCancel`), and the client
/// gets an empty result at once, as a local search cancelled early does.
#[intent_test_macros::daemon_test]
async fn search_cancel_ends_a_search_running_on_the_home() {
    use intent_core::WorkspaceApi as _;
    let h = home_harness(None).await;
    for (n, (method, query, results)) in [
        ("search.inFiles", "query", "matches"),
        ("search.fileNames", "pattern", "files"),
        ("search.codebase", "query", "matches"),
    ]
    .into_iter()
    .enumerate()
    {
        let request_id = format!("srch-home-{n}");
        let params = json!({
            "workspaceId": h.ws.as_str(),
            query: "needle",
            "requestId": request_id,
        });
        let services = h.services.clone();
        let started = h.home.started.notified();
        let search = tokio::spawn(intent_core::with_caller(
            intent_core::Caller::Daemon,
            async move {
                services
                    .home_route(method, params.as_object().unwrap())
                    .await
            },
        ));
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            started,
        )
        .await
        .expect("the search reached the home");
        assert_eq!(
            lock_sent(&h.home).last().unwrap().1["requestId"],
            json!(request_id),
            "{method}: the home searches under the client's id"
        );
        h.services.search_cancel(request_id.clone()).await.unwrap();
        let ended = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            search,
        )
        .await
        .unwrap_or_else(|_| panic!("{method}: search.cancel did not end the home search"))
        .unwrap()
        .expect("routed")
        .expect("an empty result");
        assert_eq!(ended["requestId"], json!(request_id), "{method}: {ended}");
        assert_eq!(ended[results], json!([]), "{method}: {ended}");
        assert_eq!(
            h.home.dropped.load(std::sync::atomic::Ordering::SeqCst),
            n + 1,
            "{method}: the home call was cancelled"
        );
    }
}

/// A search without a `requestId` gets one here, so the home searches
/// under the id the reply names.
#[intent_test_macros::daemon_test]
async fn a_home_search_without_an_id_gets_one() {
    let h = home_harness(Some(json!({ "matches": [], "truncated": false }))).await;
    let params = json!({ "workspaceId": h.ws.as_str(), "query": "needle" });
    h.services
        .home_route("search.inFiles", params.as_object().unwrap())
        .await
        .expect("routed")
        .expect("found");
    let sent = lock_sent(&h.home).last().unwrap().1.clone();
    assert!(
        sent["requestId"]
            .as_str()
            .is_some_and(|id| id.starts_with("srch-")),
        "{sent:?}"
    );
}

/// More matches than a reply carries stream from here; `search.cancel`
/// stops that stream too.
#[intent_test_macros::daemon_test]
async fn search_cancel_stops_a_home_search_streaming_from_here() {
    use intent_core::WorkspaceApi as _;
    // Enough matches that the stream (a batch every 20 ms) outlasts the
    // cancel by seconds, also on a slow runner.
    let matches: Vec<Value> = (0..3000)
        .map(|i| json!({ "file": format!("n{i}.txt"), "line": 1, "preview": "needle" }))
        .collect();
    let h = home_harness(Some(
        json!({ "requestId": "srch-home-stream", "matches": matches }),
    ))
    .await;
    let mut sub = h.bus.subscribe(crate::SubscriptionFilter {
        event_types: vec!["search:*".to_string()],
        workspace_id: Some(h.ws.as_str().to_owned()),
        ..Default::default()
    });
    let params = json!({
        "workspaceId": h.ws.as_str(),
        "query": "needle",
        "requestId": "srch-home-stream",
    });
    let ack = h
        .services
        .home_route("search.codebase", params.as_object().unwrap())
        .await
        .expect("routed")
        .expect("found");
    assert_eq!(ack["matches"], json!([]), "{ack}");
    assert_eq!(next_event(&mut sub).await["type"], "search:result");
    h.services
        .search_cancel("srch-home-stream".into())
        .await
        .unwrap();
    let done = loop {
        let event = next_event(&mut sub).await;
        if event["type"] == "search:done" {
            break event;
        }
    };
    assert_eq!(done["data"]["truncated"], true, "{done}");
    assert!(done["data"]["total"].as_u64().unwrap() < 3000, "{done}");
}

async fn next_event(sub: &mut crate::Subscription) -> Value {
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        sub.recv(),
    )
    .await
    .expect("event delivered")
    .expect("subscription open");
    serde_json::to_value(&batch[0]).unwrap()
}
