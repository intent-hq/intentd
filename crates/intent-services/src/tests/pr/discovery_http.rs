//! Service regressions count real HTTP attempts through the production adapter.
use super::*;
use intent_sourcecontrol::traffic::{with_traffic, Operation, Traffic};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Api {
    base: String,
    requests: Arc<Mutex<Vec<String>>>,
    pulls: Arc<Mutex<Vec<serde_json::Value>>>,
    fail_page: Arc<AtomicUsize>,
    next_link: Arc<Mutex<Option<String>>>,
    gate: Arc<tokio::sync::Semaphore>,
    entered: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Api {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn pull(number: u64, branch: &str) -> serde_json::Value {
    json!({"number":number,"html_url":format!("https://github.com/o/r/pull/{number}"),
        "title":format!("PR {number}"),"state":"open","draft":false,
        "head":{"ref":branch,"sha":"abc"},"base":{"ref":"main"},
        "created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"})
}

impl Api {
    async fn new(pulls: Vec<serde_json::Value>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let data = Arc::new(Mutex::new(pulls));
        let fail_page = Arc::new(AtomicUsize::new(0));
        let next_link = Arc::new(Mutex::new(None::<String>));
        let gate = Arc::new(tokio::sync::Semaphore::new(10000));
        let entered = Arc::new(tokio::sync::Notify::new());
        let (fail, link, admission, notify) = (
            fail_page.clone(),
            next_link.clone(),
            gate.clone(),
            entered.clone(),
        );
        let (seen, records) = (requests.clone(), data.clone());
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = Vec::new();
                loop {
                    let mut chunk = [0; 2048];
                    let n = socket.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8_lossy(&buf);
                let path = request.split_whitespace().nth(1).unwrap_or("/");
                seen.lock().unwrap().push(path.to_string());
                notify.notify_one();
                admission.acquire().await.unwrap().forget();
                let url = reqwest::Url::parse(&format!("http://fixture{path}")).unwrap();
                let query: std::collections::HashMap<_, _> =
                    url.query_pairs().into_owned().collect();
                let page = query
                    .get("page")
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(1);
                let size = query
                    .get("per_page")
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(30);
                let body = {
                    let items = records.lock().unwrap();
                    if request.starts_with("POST ") {
                        items.last().cloned().unwrap()
                    } else if url.path().ends_with("/pulls") {
                        let filtered: Vec<_> = items
                            .iter()
                            .filter(|p| p["state"] == "open")
                            .filter(|p| {
                                query
                                    .get("head")
                                    .is_none_or(|head| p["head"]["ref"] == *head)
                            })
                            .cloned()
                            .collect();
                        json!(filtered
                            .into_iter()
                            .skip((page - 1) * size)
                            .take(size)
                            .collect::<Vec<_>>())
                    } else {
                        let number = url
                            .path()
                            .rsplit('/')
                            .next()
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(0);
                        items
                            .iter()
                            .find(|p| p["number"] == number)
                            .cloned()
                            .unwrap_or_else(|| pull(number, "feature"))
                    }
                    .to_string()
                };
                let failed = fail.load(Ordering::SeqCst) == page && url.path().ends_with("/pulls");
                let (status, body) = if failed {
                    (
                        "422 Unprocessable Entity",
                        json!({"message":"fixture later page failed"}).to_string(),
                    )
                } else {
                    ("200 OK", body)
                };
                let header = link.lock().unwrap().as_ref().map_or(String::new(), |link| {
                    format!("link: <http://fixture/repos/o/r/pulls?page={link}>; rel=\"next\"\r\n")
                });
                let response = format!("HTTP/1.1 {status}\r\n{header}content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        Self {
            base,
            requests,
            pulls: data,
            fail_page,
            next_link,
            gate,
            entered,
            task,
        }
    }

    fn sc(&self) -> Arc<dyn SourceControl> {
        Arc::new(
            intent_sourcecontrol::GitHubSourceControl::new("fixture-token", Some(&self.base))
                .unwrap(),
        )
    }
}

fn counts(traffic: &Traffic) -> (u64, u64, u64) {
    let snapshot = traffic.snapshot();
    let sum = |op| {
        snapshot
            .counts
            .iter()
            .filter(|((_, o), _)| *o == op)
            .map(|(_, v)| v.rest_requests)
            .sum()
    };
    (
        sum(Operation::Discovery),
        sum(Operation::PrDetail),
        sum(Operation::Rules),
    )
}

async fn consumer(svc: &Services, branch: &str, repo: &str) -> intent_core::Workspace {
    let mut ws = workspace(&WorkspaceId::new());
    ws.branch = branch.into();
    ws.base_ref = None;
    ws.repository_owner = Some("o".into());
    ws.repository_name = Some(repo.into());
    svc.store().insert_workspace(&ws).await.unwrap();
    ws
}

#[tokio::test]
async fn shared_discovery_one_vs_many_workspaces_and_roots_http_counts() {
    for consumers in [1, 20] {
        let api = Api::new(vec![]).await;
        let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
        let sc = api.sc();
        let traffic = Traffic::default();
        with_traffic(traffic.clone(), async {
            for i in 0..consumers {
                let ws = consumer(&svc, &format!("branch-{i}"), "r").await;
                svc.refresh_workspace_pr_with_sc(ws.clone(), &sc)
                    .await
                    .unwrap();
                let checkout =
                    SweepRepo::init(&format!("root-{i}"), Some("https://github.com/o/r.git"));
                let root = sweep_root(&ws.id, &checkout.dir, Some(("o", "r")));
                svc.store().upsert_workspace_git_root(&root).await.unwrap();
                svc.refresh_git_root_pr(root, &sc).await.unwrap();
            }
        })
        .await;
        assert_eq!(
            counts(&traffic),
            (1, 0, 0),
            "consumers={consumers}; HTTP={:?}",
            api.requests.lock().unwrap()
        );
    }
}

#[tokio::test]
async fn shared_discovery_two_repositories_http_counts() {
    let api = Api::new(vec![]).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        for i in 0..12 {
            let ws = consumer(
                &svc,
                &format!("branch-{i}"),
                if i % 2 == 0 { "r" } else { "other" },
            )
            .await;
            svc.refresh_workspace_pr_with_sc(ws, &api.sc())
                .await
                .unwrap();
        }
    })
    .await;
    assert_eq!(counts(&traffic), (2, 0, 0));
}

#[tokio::test]
async fn shared_discovery_concurrent_http_counts() {
    let api = Api::new(vec![]).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let (a, b) = (
        consumer(&svc, "a", "r").await,
        consumer(&svc, "b", "r").await,
    );
    let sc = api.sc();
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        let (a, b) = tokio::join!(
            svc.refresh_workspace_pr_with_sc(a, &sc),
            svc.refresh_workspace_pr_with_sc(b, &sc)
        );
        a.unwrap();
        b.unwrap();
    })
    .await;
    assert_eq!(counts(&traffic), (1, 0, 0));
}

#[tokio::test]
async fn shared_discovery_complete_pages_match_final_page() {
    let mut data: Vec<_> = (1..=200).map(|n| pull(n, "other")).collect();
    data.push(pull(201, "last"));
    let api = Api::new(data).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let ws = consumer(&svc, "last", "r").await;
    let traffic = Traffic::default();
    with_traffic(
        traffic.clone(),
        svc.refresh_workspace_pr_with_sc(ws.clone(), &api.sc()),
    )
    .await
    .unwrap();
    assert_eq!(
        svc.store().get_workspace(&ws.id).await.unwrap().pr_number,
        Some(201)
    );
    assert_eq!(counts(&traffic).0, 3, "one complete 100-item page chain");
    assert!(api
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|p| !p.contains("head=")));
}

#[tokio::test]
async fn shared_discovery_expired_concurrent_reads_and_local_rematch() {
    let api = Api::new(vec![pull(42, "feature")]).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let sc = api.sc();
    let repo = RepoRef::new("o", "r");
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        assert!(svc
            .discover_shared_pr(sc.as_ref(), &repo, "absent", None, None)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            svc.discover_shared_pr(sc.as_ref(), &repo, "feature", None, None)
                .await
                .unwrap()
                .unwrap()
                .number,
            42
        );
        assert_eq!(counts(&traffic), (1, 1, 0));
        svc.pr_discovery.expire();
        let (a, b) = tokio::join!(
            svc.discover_shared_pr(sc.as_ref(), &repo, "absent", None, None),
            svc.discover_shared_pr(sc.as_ref(), &repo, "feature", None, None)
        );
        assert!(a.unwrap().is_none());
        assert_eq!(b.unwrap().unwrap().number, 42);
        assert_eq!(counts(&traffic), (2, 2, 0));
    })
    .await;
}

#[tokio::test]
async fn shared_discovery_exact_boundary_error_cap_and_corrupt_cursor_are_unknown() {
    for (size, failed, link, expected_pages, complete) in [
        (0, 0, None, 1, true),
        (100, 0, None, 2, true),
        (101, 2, None, 2, false),
        (1001, 0, None, 10, false),
        (3, 0, Some("1"), 1, false),
        (3, 0, Some("bad"), 1, false),
        (100, 0, Some("2"), 2, false),
    ] {
        let api = Api::new((1..=size).map(|n| pull(n, "feature")).collect()).await;
        api.fail_page.store(failed, Ordering::SeqCst);
        *api.next_link.lock().unwrap() = link.map(String::from);
        let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
        let sc = api.sc();
        let repo = RepoRef::new("o", "r");
        let traffic = Traffic::default();
        with_traffic(traffic.clone(), async {
            for _ in 0..20 {
                let result = svc
                    .discover_shared_pr(sc.as_ref(), &repo, "missing", None, None)
                    .await;
                assert_eq!(
                    result.is_ok(),
                    complete,
                    "size={size},failed={failed},link={link:?}: {result:?}"
                );
                if complete {
                    assert!(result.unwrap().is_none());
                }
            }
        })
        .await;
        assert_eq!(counts(&traffic), (expected_pages, 0, 0));
        assert!(api
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|p| !p.contains("head=")));
    }
}

#[tokio::test]
async fn shared_discovery_context_isolation_and_changed_authorization_during_fetch() {
    let api = Api::new(vec![]).await;
    let other = Api::new(vec![]).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let sc = api.sc();
    let repo = RepoRef::new("o", "r");
    let token =
        intent_sourcecontrol::GitHubSourceControl::new("different-account", Some(&api.base))
            .unwrap();
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        for (provider, repo) in [
            (sc.clone(), RepoRef::new("O", "R")),
            (api.sc(), repo.clone()),
            (Arc::new(token) as Arc<dyn SourceControl>, repo.clone()),
            (other.sc(), repo.clone()),
            (sc.clone(), RepoRef::new("o", "other")),
        ] {
            svc.discover_shared_pr(provider.as_ref(), &repo, "a", None, None)
                .await
                .unwrap();
        }
        assert_eq!(counts(&traffic), (4, 0, 0));
        svc.pr_discovery.expire();
        api.gate.forget_permits(api.gate.available_permits());
        // Drain the notification from earlier requests before parking the fill.
        api.entered.notified().await;
        let fetch = svc.discover_shared_pr(sc.as_ref(), &repo, "a", None, None);
        let change = async {
            api.entered.notified().await;
            svc.on_settings_applied(&[json!({"path":"sourceControl.github.token"})]);
            api.gate.add_permits(10000);
        };
        let (result, ()) = tokio::join!(fetch, change);
        assert!(
            result.is_err(),
            "an obsolete authorization must not publish a fill"
        );
        assert!(svc
            .discover_shared_pr(sc.as_ref(), &repo, "a", None, None)
            .await
            .is_err());
        svc.discover_shared_pr(api.sc().as_ref(), &repo, "a", None, None)
            .await
            .unwrap();
        assert_eq!(counts(&traffic), (6, 0, 0));
    })
    .await;
}

#[tokio::test]
async fn shared_discovery_matching_precedence_slashes_highest_and_exclusions() {
    let api = Api::new(vec![
        pull(1, "feature/x"),
        pull(8, "feature/x"),
        pull(20, "base/y"),
        pull(30, "origin/base/y"),
        pull(50, "y"),
    ])
    .await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let sc = api.sc();
    let repo = RepoRef::new("o", "r");
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        for (branch, base, exclude, expected) in [
            ("feature/x", Some("origin/base/y"), None, Some(8)),
            ("feature/x", Some("origin/base/y"), Some(8), Some(1)),
            ("absent", Some("origin/base/y"), None, Some(30)),
            ("absent", Some("upstream/base/y"), None, Some(20)),
            ("absent", Some("feature/y"), None, None),
            ("", Some("fork/base/y"), None, Some(20)),
        ] {
            assert_eq!(
                svc.discover_shared_pr(sc.as_ref(), &repo, branch, base, exclude)
                    .await
                    .unwrap()
                    .map(|p| p.number),
                expected
            );
        }
    })
    .await;
    assert_eq!(counts(&traffic), (1, 4, 0));
}

#[tokio::test]
async fn shared_discovery_explicit_refresh_and_pr_creation_invalidate_only_repository() {
    let api = Api::new(vec![]).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let sc = api.sc();
    let svc = svc.with_source_control(sc.clone());
    let a = consumer(&svc, "feature", "r").await;
    let b = consumer(&svc, "absent", "other").await;
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        svc.refresh_workspace_pr_with_sc(a.clone(), &sc)
            .await
            .unwrap();
        svc.refresh_workspace_pr_with_sc(b.clone(), &sc)
            .await
            .unwrap();
        api.pulls.lock().unwrap().push(pull(42, "feature"));
        let (one, two) = tokio::join!(
            svc.refresh_workspace_pr(&a.id),
            svc.refresh_workspace_pr(&a.id)
        );
        one.unwrap();
        two.unwrap();
        assert_eq!(
            svc.store().get_workspace(&a.id).await.unwrap().pr_number,
            Some(42)
        );
        assert_eq!(counts(&traffic), (3, 1, 0));
        api.pulls.lock().unwrap().push(pull(43, "created"));
        svc.github_pulls_create(
            "o".into(),
            "r".into(),
            "Created".into(),
            String::new(),
            "created".into(),
            "main".into(),
            false,
        )
        .await
        .unwrap();
        svc.refresh_workspace_pr_with_sc(b, &sc).await.unwrap();
        let created = svc
            .discover_shared_pr(sc.as_ref(), &RepoRef::new("o", "r"), "created", None, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(created.number, 43);
        assert_eq!(counts(&traffic), (4, 2, 0));
    })
    .await;
}

#[tokio::test]
async fn shared_discovery_merged_reuse_reopened_closed_and_missing_open_confirmation() {
    for terminal in ["merged", "closed"] {
        let mut record = pull(42, "feature");
        record["state"] = json!("closed");
        record["merged"] = json!(terminal == "merged");
        let api = Api::new(vec![record]).await;
        let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
        let sc = api.sc();
        let mut ws = consumer(&svc, "feature", "r").await;
        ws.pr_number = Some(42);
        svc.store().update_workspace(&ws).await.unwrap();
        let traffic = Traffic::default();
        with_traffic(traffic.clone(), async {
            // Listing absence alone cannot mark the known-open link terminal.
            svc.refresh_workspace_pr_with_sc(ws.clone(), &sc)
                .await
                .unwrap();
            let persisted = svc.store().get_workspace(&ws.id).await.unwrap();
            assert_eq!(
                persisted.pr_status,
                Some(if terminal == "merged" {
                    intent_core::PullRequestStatus::Merged
                } else {
                    intent_core::PullRequestStatus::Closed
                })
            );
            svc.refresh_workspace_pr_with_sc(persisted, &sc)
                .await
                .unwrap();
            assert_eq!(
                counts(&traffic),
                (1, 1, 0),
                "terminal confirmation is shared"
            );
            if terminal == "merged" {
                api.pulls.lock().unwrap().push(pull(43, "feature"));
            } else {
                *api.pulls.lock().unwrap() = vec![pull(42, "feature")];
            }
            svc.pr_discovery.expire();
            svc.refresh_workspace_pr_with_sc(svc.store().get_workspace(&ws.id).await.unwrap(), &sc)
                .await
                .unwrap();
            let after = svc.store().get_workspace(&ws.id).await.unwrap();
            assert_eq!(after.pr_status, Some(intent_core::PullRequestStatus::Open));
            assert_eq!(
                after.pr_number,
                Some(if terminal == "merged" { 43 } else { 42 })
            );
            assert_eq!(counts(&traffic), (2, 2, 0));
        })
        .await;
    }
}

#[tokio::test]
async fn shared_discovery_failed_listing_preserves_positive_link_and_rich_details() {
    let mut record = pull(42, "feature");
    record["mergeable"] = json!(true);
    record["mergeable_state"] = json!("clean");
    let api = Api::new(vec![record]).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let sc = api.sc();
    let ws = consumer(&svc, "feature", "r").await;
    svc.refresh_workspace_pr_with_sc(ws.clone(), &sc)
        .await
        .unwrap();
    let mut linked = svc.store().get_workspace(&ws.id).await.unwrap();
    linked.pull_requests.as_mut().unwrap()[0].is_in_merge_queue = Some(true);
    svc.store().update_workspace(&linked).await.unwrap();
    svc.pr_discovery.expire();
    api.fail_page.store(1, Ordering::SeqCst);
    svc.refresh_workspace_pr_with_sc(linked, &sc).await.unwrap();
    let after = svc.store().get_workspace(&ws.id).await.unwrap();
    assert_eq!(after.pr_number, Some(42));
    let info = &after.pull_requests.unwrap()[0];
    assert_eq!(info.mergeable, Some(true));
    assert_eq!(info.mergeable_state.as_deref(), Some("clean"));
    assert_eq!(info.is_in_merge_queue, Some(true));
}

#[tokio::test]
async fn shared_discovery_audited_distribution_http_before_after_counts() {
    // A controlled one-repository fixture with the audited distribution:
    // 224 roots, 85 linked (75 merged), 120 eligible unlinked, 19 unknown HEADs.
    // These are measured fixture counts, not an account-wide saving estimate.
    let records = (0..85)
        .map(|i| {
            let mut p = pull(i + 1, &format!("b-{i}"));
            if i < 75 {
                p["state"] = json!("closed");
                p["merged"] = json!(true);
            }
            p
        })
        .collect();
    let api = Api::new(records).await;
    let sc = api.sc();
    let repo = RepoRef::new("o", "r");
    let legacy = Traffic::default();
    let mut known = Vec::new();
    with_traffic(legacy.clone(), async {
        // Execute the old GET + per-branch query distribution through the
        // same HTTP adapter, independently of the optimized service cache.
        for i in 0..224 {
            if i < 85 {
                known.push(sc.get_pr(&repo, i + 1).await.unwrap());
            }
            if i < 75 || (85..205).contains(&i) {
                crate::pr_ops::discover_matching_open_pr(
                    sc.as_ref(),
                    &repo,
                    &format!("b-{i}"),
                    None,
                    None,
                )
                .await
                .unwrap();
            }
        }
    })
    .await;
    assert_eq!(counts(&legacy), (195, 85, 0));
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let a = consumer(&svc, "primary-a", "r").await;
    let b = consumer(&svc, "primary-b", "r").await;
    let mut checkouts = Vec::new();
    let mut roots = Vec::new();
    for i in 0..224 {
        let checkout = SweepRepo::init(&format!("b-{i}"), Some("https://github.com/o/r.git"));
        if i >= 205 {
            std::fs::remove_file(checkout.dir.join(".git/HEAD")).unwrap();
        }
        let mut root = sweep_root(
            if i % 2 == 0 { &a.id } else { &b.id },
            &checkout.dir,
            Some(("o", "r")),
        );
        if let Some(pr) = known.get(i) {
            root.pr_number = Some(pr.number);
            root.pr_url = Some(pr.url.clone());
            root.pr_status = Some(crate::pr_ops::derive_pr_status(pr));
            root.pull_requests = Some(vec![crate::pr_ops::build_pr_info(pr)]);
        }
        svc.store().upsert_workspace_git_root(&root).await.unwrap();
        roots.push(root);
        checkouts.push(checkout);
    }
    for expected in [(1, 85, 0), (1, 10, 0), (1, 10, 0)] {
        let measured = Traffic::default();
        with_traffic(measured.clone(), async {
            for root in &roots {
                svc.refresh_git_root_pr(root.clone(), &sc).await.unwrap();
            }
        })
        .await;
        assert_eq!(counts(&measured), expected);
        let snapshot = measured.snapshot();
        assert!(snapshot
            .counts
            .values()
            .all(|c| c.graphql_requests == 0 && c.graphql_points == 0));
        svc.pr_discovery.expire();
    }
    assert_eq!(checkouts.len(), 224);
}

#[tokio::test]
async fn shared_discovery_old_open_healing_and_missing_link_reads_are_shared() {
    let mut ended = pull(9, "old");
    ended["state"] = json!("closed");
    ended["merged"] = json!(true);
    let api = Api::new(vec![ended, pull(42, "feature")]).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let sc = api.sc();
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        for _ in 0..2 {
            let ws = consumer(&svc, "primary", "r").await;
            let checkout = SweepRepo::init("feature", Some("https://github.com/o/r.git"));
            let mut root = sweep_root(&ws.id, &checkout.dir, Some(("o", "r")));
            root.pr_number = Some(42);
            root.pull_requests = Some(vec![pool_entry(
                9,
                intent_core::PullRequestStatus::Open,
                "2020-01-01T00:00:00Z",
            )]);
            svc.store().upsert_workspace_git_root(&root).await.unwrap();
            svc.refresh_git_root_pr(root, &sc).await.unwrap();
            let roots = svc.store().list_workspace_git_roots(&ws.id).await.unwrap();
            assert_eq!(
                roots[0]
                    .pull_requests
                    .as_ref()
                    .unwrap()
                    .iter()
                    .find(|p| p.number == 9)
                    .unwrap()
                    .status,
                intent_core::PullRequestStatus::Merged
            );
        }
    })
    .await;
    assert_eq!(counts(&traffic), (1, 2, 0));
}

#[tokio::test]
async fn shared_discovery_restart_backlog_is_bounded_and_eventually_fair() {
    let api = Api::new(vec![]).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let sc = api.sc();
    let mut admitted = std::collections::HashSet::new();
    for _ in 0..6 {
        let traffic = Traffic::default();
        with_traffic(traffic.clone(), async {
            for i in 0..170 {
                let repo = RepoRef::new("o", format!("repository-{i}"));
                if svc
                    .discover_shared_pr(sc.as_ref(), &repo, "branch", None, None)
                    .await
                    .is_ok()
                {
                    admitted.insert(i);
                }
            }
            let before = counts(&traffic);
            for _ in 0..20 {
                let _ = svc
                    .discover_shared_pr(
                        sc.as_ref(),
                        &RepoRef::new("o", "repository-169"),
                        "another-branch",
                        None,
                        None,
                    )
                    .await;
            }
            assert_eq!(
                counts(&traffic),
                before,
                "a deferred repository cannot fan out by branch"
            );
        })
        .await;
        assert!(
            counts(&traffic).0 <= 128,
            "HTTP attempts stay inside the shared window budget"
        );
        assert_eq!((counts(&traffic).1, counts(&traffic).2), (0, 0));
        svc.pr_discovery.expire();
    }
    assert_eq!(
        admitted.len(),
        170,
        "later repositories cannot starve behind the first workspace"
    );
}

#[tokio::test]
async fn shared_discovery_quota_pause_blocks_list_and_record_attempts() {
    let api = Api::new(vec![pull(42, "feature")]).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let sc = api.sc();
    let repo = RepoRef::new("o", "r");
    svc.sweep_rate_limit
        .pause_for(std::time::Duration::from_secs(60), true);
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        for _ in 0..20 {
            assert!(svc
                .discover_shared_pr(sc.as_ref(), &repo, "feature", None, None)
                .await
                .is_err());
            assert!(svc.shared_pr_record(sc.as_ref(), &repo, 42).await.is_err());
        }
        assert_eq!(counts(&traffic), (0, 0, 0));
        svc.sweep_rate_limit.lift();
        crate::pr_discovery::explicitly_refresh(async {
            assert_eq!(
                svc.discover_shared_pr(sc.as_ref(), &repo, "feature", None, None)
                    .await
                    .unwrap()
                    .unwrap()
                    .number,
                42
            );
        })
        .await;
        assert_eq!(counts(&traffic), (1, 1, 0));
    })
    .await;
}

#[tokio::test]
async fn shared_discovery_invalidated_in_flight_result_cannot_publish_absence() {
    let api = Api::new(vec![]).await;
    api.gate.forget_permits(10000);
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let sc = api.sc();
    let repo = RepoRef::new("o", "r");
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        let read = svc.discover_shared_pr(sc.as_ref(), &repo, "feature", None, None);
        let invalidate = async {
            api.entered.notified().await;
            svc.pr_discovery.invalidate(sc.as_ref(), &repo);
            api.gate.add_permits(10000);
        };
        let (result, ()) = tokio::join!(read, invalidate);
        assert!(result.is_err());
        api.pulls.lock().unwrap().push(pull(42, "feature"));
        assert_eq!(
            svc.discover_shared_pr(sc.as_ref(), &repo, "feature", None, None)
                .await
                .unwrap()
                .unwrap()
                .number,
            42
        );
        assert_eq!(counts(&traffic), (2, 1, 0));
    })
    .await;
}

#[tokio::test]
async fn shared_discovery_git_head_change_rematches_shared_listing() {
    let api = Api::new(vec![pull(42, "next")]).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let sc = api.sc();
    let ws = consumer(&svc, "unused", "r").await;
    let checkout = SweepRepo::init("first", Some("https://github.com/o/r.git"));
    let root = sweep_root(&ws.id, &checkout.dir, Some(("o", "r")));
    svc.store().upsert_workspace_git_root(&root).await.unwrap();
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        svc.refresh_git_root_pr(root.clone(), &sc).await.unwrap();
        std::fs::write(checkout.dir.join(".git/HEAD"), "ref: refs/heads/next\n").unwrap();
        svc.refresh_git_root_pr(root, &sc).await.unwrap();
        let roots = svc.store().list_workspace_git_roots(&ws.id).await.unwrap();
        assert_eq!(roots[0].pr_number, Some(42));
        assert_eq!(counts(&traffic), (1, 1, 0));
    })
    .await;
}

#[tokio::test]
async fn shared_discovery_old_open_history_rotates_past_quiet_entries() {
    let api = Api::new((1..=12).map(|n| pull(n, "old")).collect()).await;
    let (_t, svc, _) = refresh_setup(StubForge::default(), "main", None, false).await;
    let sc = api.sc();
    let repo = RepoRef::new("o", "r");
    let mut pool = Some(
        (1..=12)
            .map(|n| {
                pool_entry(
                    n,
                    intent_core::PullRequestStatus::Open,
                    "2026-01-01T00:00:00Z",
                )
            })
            .collect(),
    );
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        for _ in 0..3 {
            let (_, limited) = crate::pr_ops::refresh_stale_pool_entries(
                &svc,
                sc.as_ref(),
                &repo,
                &mut pool,
                &mut Vec::new(),
                std::time::Duration::from_secs(5),
            )
            .await;
            assert!(limited.is_none());
        }
    })
    .await;
    assert_eq!(counts(&traffic), (0, 12, 0));
    for number in 1..=12 {
        assert!(api
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|p| p == &format!("/repos/o/r/pulls/{number}")));
    }
}
