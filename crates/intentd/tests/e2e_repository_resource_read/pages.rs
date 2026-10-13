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
