//! Actual original UDS/WSS frames for scoped project reads and their private feed.
use super::*;

fn repository() -> Value {
    json!({"provider":"gitlab","instanceBaseUrl":INSTANCE,"projectPath":PROJECT})
}

fn list(capture: &Value, context: &Value) -> Value {
    json!({"context":context,"readLifetimeId":capture["readLifetimeId"],
        "repository":repository(),"kind":"merge-request",
        "query":{"state":"closed","filter":"all","search":"","limit":1}})
}

/// Only the legacy GitHub resolver is controlled here. GitLab policy and secret
/// delivery use the same real Services and original UDS/WSS listeners as pages.
#[derive(Default)]
struct HelperControl(AtomicUsize);
impl intent_transport::SystemControl for HelperControl {
    fn daemon_version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    fn status(&self) -> intent_transport::SystemStatus {
        panic!("helper control does not supply daemon status")
    }
    fn host_environment(&self) -> intent_transport::HostEnvironment {
        panic!("helper control does not supply host environment")
    }
    fn request_shutdown(&self) {
        panic!("helper control does not shut down the daemon")
    }
    fn request_update(&self) -> Result<(), String> {
        Err("helper control has no updater".into())
    }
    fn git_credential(
        &self,
        _: Option<u64>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<(String, String)>> + Send + '_>>
    {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Some(("x-access-token".into(), "github-synthetic".into())) })
    }
}

#[intent_test_macros::daemon_test]
async fn repository_child_git_real_uds_scope_policy_and_github_compatibility() {
    let control = Arc::new(HelperControl::default());
    let h = Harness::with_control(true, Server::new().await, Some(control.clone())).await;
    let provider_calls = h.server.as_ref().unwrap().count();
    let mut owner = h.wss(TOKEN).await;
    let mut helper = h.uds().await;
    let q = json!({"protocol":"https","host":"forge.test:8443",
        "path":"install/Team/Sub/Project.git","username":"oauth2","pid":17});
    let off = helper.rpc("system.gitCredential", q.clone()).await;
    assert_eq!(success(&off), &json!({"credential":null}));
    let original = owner
        .rpc("sourceControl.gitlab.childGit.get", json!({}))
        .await;
    let enabled = owner
        .rpc(
            "sourceControl.gitlab.childGit.set",
            json!({"binding":success(&original)["binding"],"enabled":true}),
        )
        .await;
    assert_eq!(success(&enabled)["effective"], true);
    for _ in 0..2 {
        let credential = helper.rpc("system.gitCredential", q.clone()).await;
        assert_eq!(
            success(&credential),
            &json!({"credential":{
            "username":"oauth2","password":"stored-pat"}})
        );
    }
    for (field, value) in [
        ("host", "forge.test"),
        ("host", "other.test:8443"),
        ("path", "elsewhere/Team/Sub/Project.git"),
        ("username", "another-account"),
        ("protocol", "http"),
    ] {
        let mut wrong = q.clone();
        wrong[field] = json!(value);
        let reply = helper.rpc("system.gitCredential", wrong).await;
        assert_eq!(success(&reply), &json!({"credential":null}));
    }
    let remote = owner.rpc("system.gitCredential", q.clone()).await;
    assert_eq!(remote["error"]["code"], -32001);
    for token in [MEMBER, GUEST] {
        let mut remote = h.wss(token).await;
        let reply = remote.rpc("system.gitCredential", q.clone()).await;
        assert_eq!(reply["error"]["code"], -32003);
        remote.close().await;
    }
    let disabled = owner
        .rpc(
            "sourceControl.gitlab.childGit.set",
            json!({"binding":success(&enabled)["binding"],"enabled":false}),
        )
        .await;
    assert_eq!(success(&disabled)["effective"], false);
    let off_again = helper.rpc("system.gitCredential", q).await;
    assert_eq!(
        success(&off_again),
        &json!({"credential":null}),
        "the existing helper connection observes the new disabled policy"
    );
    let github = helper
        .rpc(
            "system.gitCredential",
            json!({"protocol":"https","host":"github.com","pid":18}),
        )
        .await;
    assert_eq!(
        success(&github),
        &json!({"credential":{
        "username":"x-access-token","password":"github-synthetic"}})
    );
    assert_eq!(control.0.load(Ordering::SeqCst), 1);
    assert_eq!(h.server.as_ref().unwrap().count(), provider_calls);
    helper.close().await;
    owner.close().await;
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn repository_child_git_real_uds_partial_write_refuses_remainder_after_disable() {
    let control = Arc::new(HelperControl::default());
    let h = Harness::with_control(true, Server::new().await, Some(control.clone())).await;
    let provider_calls = h.server.as_ref().unwrap().count();
    let mut owner = h.wss(TOKEN).await;
    let original = owner
        .rpc("sourceControl.gitlab.childGit.get", json!({}))
        .await;
    let enabled = owner
        .rpc(
            "sourceControl.gitlab.childGit.set",
            json!({"binding":success(&original)["binding"],"enabled":true}),
        )
        .await;
    assert_eq!(success(&enabled)["effective"], true);

    // A bounded large request ID makes this ordinary secret response exceed
    // the socket buffer without changing the provider token or writer. Read
    // a success prefix to observe actual publication, then stop draining this socket.
    let id = "child-output-backpressure-".to_owned() + &"x".repeat(2 * 1024 * 1024);
    let query = json!({"protocol":"https","host":"forge.test:8443",
        "path":"install/Team/Sub/Project.git","username":"oauth2","pid":17});
    let request = json!({"jsonrpc":"2.0","id":id,"method":"system.gitCredential","params":query});
    let expected = json!({"jsonrpc":"2.0","id":id,"result":{"credential":{
        "username":"oauth2","password":"stored-pat"}}})
    .to_string();
    let mut helper = UnixStream::connect(h.dir.path().join("intentd.sock"))
        .await
        .unwrap();
    tokio::time::timeout(ACQUIRE, helper.write_all(format!("{request}\n").as_bytes()))
        .await
        .unwrap()
        .unwrap();
    let mut received = vec![0; 128];
    tokio::time::timeout(ACQUIRE, helper.read_exact(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert!(expected.as_bytes().starts_with(&received));

    // This plain response must not flush a retired private frame or continue
    // on a connection whose private writer has refused the remainder.
    let sentinel = json!({"jsonrpc":"2.0","id":"after-private-sentinel",
        "method":"test.publicSentinel","params":{}});
    tokio::time::timeout(
        ACQUIRE,
        helper.write_all(format!("{sentinel}\n").as_bytes()),
    )
    .await
    .unwrap()
    .unwrap();
    let disabled = owner
        .rpc(
            "sourceControl.gitlab.childGit.set",
            json!({"binding":success(&enabled)["binding"],"enabled":false}),
        )
        .await;
    assert_eq!(success(&disabled)["effective"], false);

    tokio::time::timeout(ACQUIRE, helper.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert!(
        received.len() < expected.len(),
        "retired private remainder was written"
    );
    assert!(expected.as_bytes().starts_with(&received));
    assert!(
        !received.contains(&b'\n'),
        "no complete private or later public frame"
    );
    assert_eq!(h.server.as_ref().unwrap().count(), provider_calls);
    assert_eq!(control.0.load(Ordering::SeqCst), 0);
    eprintln!(
        "child original UDS partial output: received={} expected={} then EOF after acknowledged disable",
        received.len(), expected.len()
    );
    drop(helper);
    owner.close().await;
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn repository_child_git_real_owner_policy_is_default_off_and_bound() {
    let h = Harness::new().await;
    let mut owner = h.wss(TOKEN).await;
    let hello = owner
        .rpc("client.hello", json!({"name":"child-policy-owner"}))
        .await;
    for capability in [
        "repositoryResourceRead",
        "repositoryResourcePages",
        "gitlabChildGitPolicy",
    ] {
        assert_eq!(success(&hello)["server"]["capabilities"][capability], 1);
    }
    let before = h.server.as_ref().unwrap().count();
    let first = owner
        .rpc("sourceControl.gitlab.childGit.get", json!({}))
        .await;
    let first = success(&first);
    assert_eq!(first["kind"], "connected");
    assert_eq!(first["enabled"], false);
    assert_eq!(first["effective"], false);
    assert_eq!(first["binding"]["instanceBaseUrl"], INSTANCE);
    assert!(first["binding"]["connection"]["connectionGeneration"].is_string());
    let binding = first["binding"].clone();
    let set = owner
        .rpc(
            "sourceControl.gitlab.childGit.set",
            json!({"binding":binding,"enabled":true}),
        )
        .await;
    let set = success(&set);
    assert_eq!(set["enabled"], true);
    assert_eq!(set["effective"], true);
    let changed = set["binding"].clone();
    let mut rewritten = changed.clone();
    rewritten["revision"] = json!(format!("0{}", changed["revision"].as_str().unwrap()));
    let rewritten = owner
        .rpc(
            "sourceControl.gitlab.childGit.set",
            json!({"binding":rewritten,"enabled":false}),
        )
        .await;
    assert!(
        rewritten.get("error").is_some(),
        "an opaque revision must match exactly"
    );
    let stale = owner
        .rpc(
            "sourceControl.gitlab.childGit.set",
            json!({"binding":binding,"enabled":false}),
        )
        .await;
    assert!(
        stale.get("error").is_some(),
        "old acknowledged policy revision must not overwrite a later choice"
    );
    for token in [MEMBER, GUEST] {
        let mut caller = h.wss(token).await;
        let get = caller
            .rpc("sourceControl.gitlab.childGit.get", json!({}))
            .await;
        assert_eq!(get["error"]["code"], -32003);
        let set = caller
            .rpc(
                "sourceControl.gitlab.childGit.set",
                json!({"binding":changed,"enabled":false}),
            )
            .await;
        assert_eq!(set["error"]["code"], -32003);
        caller.close().await;
    }
    let last = owner
        .rpc("sourceControl.gitlab.childGit.get", json!({}))
        .await;
    assert_eq!(success(&last)["enabled"], true);
    assert_eq!(
        h.server.as_ref().unwrap().count(),
        before,
        "local policy operations must not probe provider endpoints"
    );
    owner.close().await;
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn repository_pages_real_owner_and_member_wss_lists_discussions_and_release() {
    let h = Harness::new().await;
    for owner in [true, false] {
        let context = if owner {
            json!({"kind":"host"})
        } else {
            json!({"kind":"workspace","workspaceId":h.workspace})
        };
        let mut c = h.wss(if owner { TOKEN } else { MEMBER }).await;
        let capture = c
            .rpc(
                "sourceControl.read.captureScoped",
                json!({"context":context}),
            )
            .await;
        let capture = success(&capture).clone();
        assert_eq!(capture["context"], context);
        let provider = h.server.as_ref().unwrap();
        let path = "/api/v4/projects/Team%2FSub%2FProject/merge_requests";
        provider
            .state
            .replies
            .lock()
            .unwrap()
            .insert(path.into(), (200, json!([resource("merge_requests")])));
        provider
            .state
            .next_pages
            .lock()
            .unwrap()
            .insert(path.into(), "2".into());
        let before = provider.count();
        let mut q = list(&capture, &context);
        let first = c.rpc("sourceControl.read.list", q.clone()).await;
        let first = success(&first);
        assert_eq!(first["outcome"]["items"], json!([]));
        q["query"]["cursor"] = first["outcome"]["nextCursor"].clone();
        assert!(q["query"]["cursor"].is_string());
        let mut merged = resource("merge_requests");
        merged["state"] = json!("merged");
        provider
            .state
            .replies
            .lock()
            .unwrap()
            .insert(path.into(), (200, json!([merged])));
        provider
            .state
            .next_pages
            .lock()
            .unwrap()
            .insert(path.into(), String::new());
        let second = c.rpc("sourceControl.read.list", q).await;
        let second = success(&second);
        assert_eq!(second["outcome"]["items"][0]["state"], "merged");
        assert!(second["outcome"]["nextCursor"].is_null());
        assert_eq!(provider.count(), before + 2);
        let note = |id, body, resolvable| json!({"id":id,"body":body,"author":{"username":"fixture"},"created_at":"2026-01-01T00:00:00Z","resolvable":resolvable,"resolved":false});
        let mut diff = note(41, "diff", true);
        diff["position"] = json!({"new_path":"lib.rs","new_line":8,"head_sha":"0123456789012345678901234567890123456789"});
        provider.state.replies.lock().unwrap().insert(format!("{}/discussions",route("merge_requests")),(200,json!([
            {"id":"single","individual_note":true,"notes":[note(11,"single",false)]},
            {"id":"general","individual_note":false,"notes":[note(21,"general",false),note(22,"reply",false)]},
            {"id":"diff","individual_note":false,"notes":[diff]}
        ])));
        let target = json!({"repository":repository(),"kind":"merge-request","number":7});
        let discussions=c.rpc("sourceControl.read.discussions",json!({"context":context,"readLifetimeId":capture["readLifetimeId"],"target":target,"page":{"limit":50}})).await;
        let items = success(&discussions)["outcome"]["items"]
            .as_array()
            .unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0]["notes"][0]["resolvable"], false);
        assert!(items[0]["notes"][0]["resolved"].is_null());
        assert_eq!(
            items[1]["notes"][1]["inReplyToId"],
            items[1]["notes"][0]["id"]
        );
        assert_eq!(
            items[2]["notes"][0]["positionHeadSha"],
            "0123456789012345678901234567890123456789"
        );
        assert_eq!(
            provider.count(),
            before + 3,
            "one discussions endpoint and no notes or identity probe"
        );
        let detail=c.rpc("sourceControl.read.detailScoped",json!({"context":context,"readLifetimeId":capture["readLifetimeId"],"target":target})).await;
        assert_eq!(success(&detail)["outcome"]["kind"], "merge-request");
        let bound = json!({"context":context,"readLifetimeId":capture["readLifetimeId"]});
        let released = c
            .rpc("sourceControl.read.releaseScoped", bound.clone())
            .await;
        assert_eq!(success(&released)["released"], true);
        let repeated = c.rpc("sourceControl.read.releaseScoped", bound).await;
        assert_eq!(success(&repeated)["released"], true);
        let stale = c
            .rpc("sourceControl.read.list", list(&capture, &context))
            .await;
        assert!(stale.get("error").is_some());
        assert!(c
            .notices
            .iter()
            .any(|notice| notice["method"] == "sourceControl.read.retired"));
        c.close().await;
    }
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn repository_pages_real_wss_denial_feed_and_foreign_capture() {
    let h = Harness::new().await;
    let mut a = h.wss(MEMBER).await;
    let mut b = h.wss(MEMBER).await;
    let context = json!({"kind":"workspace","workspaceId":h.workspace});
    let capture = a
        .rpc(
            "sourceControl.read.captureScoped",
            json!({"context":context}),
        )
        .await;
    let capture = success(&capture).clone();
    let provider = h.server.as_ref().unwrap();
    let before = provider.count();
    let foreign = b
        .rpc("sourceControl.read.list", list(&capture, &context))
        .await;
    assert_eq!(foreign["error"]["code"], -32003);
    let host = b
        .rpc(
            "sourceControl.read.captureScoped",
            json!({"context":{"kind":"host"}}),
        )
        .await;
    assert_eq!(host["error"]["code"], -32003);
    assert_eq!(provider.count(), before);
    let path = "/api/v4/projects/Team%2FSub%2FProject/merge_requests";
    provider.set(path, 403);
    let failure = a
        .rpc("sourceControl.read.list", list(&capture, &context))
        .await;
    assert_eq!(
        success(&failure)["outcome"],
        json!({"kind":"failure","code":"resource-denied","status":403})
    );
    let detail=a.rpc("sourceControl.read.detailScoped",json!({"context":context,"readLifetimeId":capture["readLifetimeId"],"target":{"repository":repository(),"kind":"issue","number":7}})).await;
    assert_eq!(success(&detail)["outcome"]["kind"], "issue");
    let notice = loop {
        if let Some(v) = a
            .notices
            .iter()
            .find(|v| v["method"] == "sourceControl.read.invalidated")
        {
            break v.clone();
        }
        let v = a.next().await;
        a.notices.push(v);
    };
    assert_eq!(
        notice["params"]["readLifetimeIds"],
        json!([capture["readLifetimeId"]])
    );
    assert_eq!(notice["params"]["repository"], repository());
    assert_eq!(
        notice["params"]["coverage"],
        json!({"kind":"list","resourceKind":"merge-request"})
    );
    assert!(notice["params"]["sequence"].is_string());
    h.store.remove_host_member(&h.member).await.unwrap();
    let count = provider.count();
    let removed = a
        .rpc("sourceControl.read.list", list(&capture, &context))
        .await;
    assert!(removed.get("error").is_some());
    assert_eq!(provider.count(), count);
    a.close().await;
    b.close().await;
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn repository_pages_real_host_resolves_only_registered_local_identity() {
    let h = Harness::with_workspace(false).await;
    let repo = h.dir.path().join("registered");
    assert!(std::process::Command::new("git")
        .args(["init", "--quiet"])
        .arg(&repo)
        .status()
        .unwrap()
        .success());
    assert!(std::process::Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args([
            "remote",
            "add",
            "origin",
            &format!("{INSTANCE}/{PROJECT}.git")
        ])
        .status()
        .unwrap()
        .success());
    let path = repo.to_str().unwrap();
    h.store
        .upsert_known_repo(path, "display-only", Some("not-authority"))
        .await
        .unwrap();
    let mut owner = h.wss(TOKEN).await;
    let capture = owner
        .rpc(
            "sourceControl.read.captureScoped",
            json!({"context":{"kind":"host"}}),
        )
        .await;
    let capture = success(&capture);
    let before = h.server.as_ref().unwrap().count();
    let q =
        json!({"context":{"kind":"host"},"readLifetimeId":capture["readLifetimeId"],"path":path});
    let selected = owner
        .rpc("sourceControl.read.resolveKnownRepository", q.clone())
        .await;
    let selected = success(&selected);
    assert_eq!(selected["selection"]["outcome"]["state"], "resolved");
    assert_eq!(selected["selection"]["outcome"]["target"], repository());
    assert_eq!(h.server.as_ref().unwrap().count(), before);
    h.store.remove_known_repo(path).await.unwrap();
    let forgotten = owner
        .rpc("sourceControl.read.resolveKnownRepository", q)
        .await;
    assert!(forgotten.get("error").is_some());
    assert_eq!(h.server.as_ref().unwrap().count(), before);
    owner.close().await;
    h.finish().await;
}
