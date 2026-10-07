//! The new pre-workspace producer through the original UDS and pinned TLS WSS.
use super::*;

const SHA: &str = "0123456789012345678901234567890123456789";
const BRANCHES: &str = "/api/v4/projects/Team%2FSub%2FProject/repository/branches";

fn repo() -> Value {
    json!({"id":42,"path_with_namespace":PROJECT,"web_url":format!("{INSTANCE}/{PROJECT}"),"default_branch":"main"})
}
fn branch(name: &str) -> Value {
    json!({"name":name,"commit":{"id":SHA},"protected":false})
}
fn ready(response: &Value) -> &Value {
    let result = success(response);
    assert_eq!(result["status"], "ready", "{response}");
    &result["value"]
}
fn bound(capture: &Value) -> Value {
    json!({"checkoutId":capture["checkoutId"],"revision":capture["revision"]})
}
fn project_query(capture: &Value) -> Value {
    let mut q = bound(capture);
    q["projectPath"] = json!(PROJECT);
    q
}
fn set(server: &Server, path: &str, body: Value, next: Option<&str>) {
    server
        .state
        .replies
        .lock()
        .unwrap()
        .insert(path.into(), (200, body));
    server
        .state
        .next_pages
        .lock()
        .unwrap()
        .insert(path.into(), next.unwrap_or_default().into());
}
async fn capture(client: &mut Client) -> Value {
    ready(
        &client
            .rpc(
                "sourceControl.checkout.capture",
                json!({"provider":"gitlab","instanceBaseUrl":INSTANCE}),
            )
            .await,
    )
    .clone()
}

#[intent_test_macros::daemon_test]
async fn checkout_default_provider_self_heal_keeps_original_pages_but_gitlab_settings_aba_retires()
{
    for remote in [false, true] {
        for heal_before_first_page in [true, false] {
            let h = Harness::with_workspace(false).await;
            let server = h.server.as_ref().unwrap();
            let route = "/api/v4/projects?membership=true&order_by=last_activity_at&simple=true";
            set(
                server,
                &format!("{route}&page=1&per_page=50"),
                json!([repo()]),
                Some("2"),
            );
            set(
                server,
                &format!("{route}&page=2&per_page=50"),
                json!([]),
                None,
            );
            let mut client = if remote {
                h.wss(TOKEN).await
            } else {
                h.uds().await
            };
            let original = capture(&mut client).await;
            let mut query = bound(&original);
            query["limit"] = json!(50);
            let mut first = None;
            if !heal_before_first_page {
                first = Some(
                    client
                        .rpc("sourceControl.checkout.projects", query.clone())
                        .await,
                );
                query["cursor"] = ready(first.as_ref().unwrap())["nextCursor"].clone();
            }
            // Invoke the same cache-only self-heal that discovery completed in
            // the retained Electron trace, without probing installed providers.
            let healed = h
                .services
                .heal_default_provider_settings(&["auggie".into()])
                .await
                .unwrap();
            assert_eq!(healed["healed"], true);
            assert_eq!(healed["provider"], "auggie");
            let response = client
                .rpc("sourceControl.checkout.projects", query.clone())
                .await;
            eprintln!("checkout self-heal original response remote={remote} before_first={heal_before_first_page} {response}");
            if heal_before_first_page {
                assert_eq!(ready(&response)["items"][0]["projectPath"], PROJECT);
                query["cursor"] = ready(&response)["nextCursor"].clone();
                first = Some(response);
                assert_eq!(
                    ready(&client.rpc("sourceControl.checkout.projects", query).await)["items"],
                    json!([])
                );
            } else {
                assert_eq!(ready(&response)["items"], json!([]));
            }
            assert!(ready(first.as_ref().unwrap())["nextCursor"].is_string());

            // A real GitLab settings change and restoration is a new authority
            // generation. Equality of the final settings cannot revive this lease.
            let mut owner = h.uds().await;
            for client_id in ["changed-client", "client"] {
                success(&owner.rpc("settings.update", json!({"changes":[{"path":"sourceControl.gitlab.oauthClientId","value":client_id}]})).await);
            }
            let before = server.count();
            let refused = client
                .rpc("sourceControl.checkout.projects", bound(&original))
                .await;
            assert!(
                refused.get("error").is_some(),
                "old settings generation revived: {refused}"
            );
            assert_eq!(
                server.count(),
                before,
                "retired settings cannot reach the provider"
            );
            let unverified = client
                .rpc(
                    "sourceControl.checkout.capture",
                    json!({"provider":"gitlab","instanceBaseUrl":INSTANCE}),
                )
                .await;
            assert_eq!(success(&unverified)["reason"], "not-connected");
            // Restoring settings is not verification. Recovery requires a new
            // public connection that verifies the original private credential.
            success(&owner.rpc("sourceControl.connect", json!({"provider":"gitlab","instanceBaseUrl":INSTANCE,"method":"pat","token":"stored-pat"})).await);
            let fresh = capture(&mut client).await;
            assert_ne!(fresh["revision"], original["revision"]);
            let mut query = bound(&fresh);
            query["limit"] = json!(50);
            assert_eq!(
                ready(&client.rpc("sourceControl.checkout.projects", query).await)["items"][0]
                    ["projectPath"],
                PROJECT
            );
            assert_eq!(h.store.list_workspaces(true).await.unwrap().len(), 0);
            owner.close().await;
            client.close().await;
            h.finish().await;
        }
    }
}

#[intent_test_macros::daemon_test]
async fn checkout_two_bound_hosts_and_workspace_guest_cannot_borrow_authority() {
    let first = Harness::new().await;
    let second = Harness::new().await;
    for h in [&first, &second] {
        set(
            h.server.as_ref().unwrap(),
            "/api/v4/projects",
            json!([repo()]),
            None,
        );
    }
    let mut first_owner = first.uds().await;
    let mut second_member = second.wss(MEMBER).await;
    let captured = capture(&mut first_owner).await;
    let before = second.server.as_ref().unwrap().count();
    let refused = second_member
        .rpc("sourceControl.checkout.projects", bound(&captured))
        .await;
    assert!(refused.get("error").is_some());
    assert_eq!(second.server.as_ref().unwrap().count(), before);
    let second_capture = capture(&mut second_member).await;
    assert_ne!(captured["checkoutId"], second_capture["checkoutId"]);
    assert_eq!(
        ready(
            &second_member
                .rpc("sourceControl.checkout.projects", bound(&second_capture))
                .await
        )["items"][0]["projectPath"],
        PROJECT
    );
    let mut guest = first.wss(GUEST).await;
    let before = first.server.as_ref().unwrap().count();
    let refused = guest
        .rpc(
            "sourceControl.checkout.capture",
            json!({"provider":"gitlab","instanceBaseUrl":INSTANCE}),
        )
        .await;
    assert!(
        refused.get("error").is_some(),
        "workspace collaboration never grants host checkout: {refused}"
    );
    assert_eq!(first.server.as_ref().unwrap().count(), before);
    assert_eq!(
        ready(
            &first_owner
                .rpc("sourceControl.checkout.projects", bound(&captured))
                .await
        )["items"][0]["projectPath"],
        PROJECT
    );
    guest.close().await;
    first_owner.close().await;
    second_member.close().await;
    first.finish().await;
    second.finish().await;
}

#[intent_test_macros::daemon_test]
async fn checkout_pagination_reaches_beyond_retained_history_without_losing_current_page() {
    let h = Harness::with_workspace(false).await;
    let server = h.server.as_ref().unwrap();
    set(
        server,
        "/api/v4/projects/Team%2FSub%2FProject",
        repo(),
        None,
    );
    for page in 1..=11 {
        let next = (page < 11).then(|| (page + 1).to_string());
        let names = ((page - 1) * 100..page * 100)
            .map(|i| branch(&format!("branch-{i:04}")))
            .collect::<Vec<_>>();
        set(
            server,
            &format!("{BRANCHES}?page={page}&per_page=100"),
            json!(names),
            next.as_deref(),
        );
        let projects = ((page - 1) * 100..page * 100)
            .map(|i| json!({"id":i+1000,"path_with_namespace":format!("Team/project-{i:04}"),"web_url":format!("{INSTANCE}/Team/project-{i:04}"),"default_branch":null}))
            .collect::<Vec<_>>();
        set(server, &format!("/api/v4/projects?membership=true&order_by=last_activity_at&simple=true&page={page}&per_page=100"), json!(projects), next.as_deref());
    }
    let mut client = h.wss(MEMBER).await;
    let original = capture(&mut client).await;
    for (method, field, expected, mut query) in [
        (
            "sourceControl.checkout.projects",
            "projectPath",
            "Team/project-1099",
            bound(&original),
        ),
        (
            "sourceControl.checkout.branches",
            "name",
            "branch-1099",
            project_query(&original),
        ),
    ] {
        query["limit"] = json!(100);
        for page in 1..=11 {
            let response = client.rpc(method, query.clone()).await;
            let data = ready(&response);
            assert_eq!(data["items"].as_array().unwrap().len(), 100);
            if page == 11 {
                assert_eq!(data["items"][99][field], expected);
                assert!(data.get("nextCursor").is_none());
            } else {
                query["cursor"] = data["nextCursor"].clone();
                assert!(query["cursor"].is_string());
            }
        }
    }
    client.close().await;
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn checkout_preworkspace_real_owner_and_member_page_two_restore_and_cursor_isolation() {
    let h = Harness::with_workspace(false).await;
    assert_eq!(
        h.store.list_workspaces(true).await.unwrap().len(),
        0,
        "browsing requires no workspace"
    );
    let server = h.server.as_ref().unwrap();
    set(
        server,
        "/api/v4/projects/Team%2FSub%2FProject",
        repo(),
        None,
    );
    set(
        server,
        "/api/v4/projects?membership=true&order_by=last_activity_at&simple=true&page=1&per_page=1",
        json!([repo()]),
        Some("2"),
    );
    set(
        server,
        "/api/v4/projects?membership=true&order_by=last_activity_at&simple=true&page=2&per_page=1",
        json!([]),
        None,
    );
    set(
        server,
        &format!("{BRANCHES}?page=1&per_page=1"),
        json!([branch("main")]),
        Some("2"),
    );
    set(
        server,
        &format!("{BRANCHES}?page=2&per_page=1"),
        json!([branch("release/later-page")]),
        None,
    );
    set(
        server,
        "/api/v4/projects/Other%2FSub%2FProject",
        json!({"id":43,"path_with_namespace":"Other/Sub/Project","web_url":format!("{INSTANCE}/Other/Sub/Project"),"default_branch":"stable"}),
        None,
    );
    for remote in [false, true] {
        let mut client = if remote {
            h.wss(MEMBER).await
        } else {
            h.uds().await
        };
        let original = capture(&mut client).await;
        assert_eq!(original["instanceBaseUrl"], INSTANCE);
        assert_eq!(original["expiresAfterMs"], 600_000);
        let mut query = bound(&original);
        query["limit"] = json!(1);
        let first = ready(
            &client
                .rpc("sourceControl.checkout.projects", query.clone())
                .await,
        )
        .clone();
        assert_eq!(first["items"][0]["projectPath"], PROJECT);
        let next = first["nextCursor"].clone();
        query["cursor"] = next.clone();
        assert_eq!(
            ready(
                &client
                    .rpc("sourceControl.checkout.projects", query.clone())
                    .await
            )["items"],
            json!([])
        );
        query["query"] = json!("different");
        assert_eq!(
            client.rpc("sourceControl.checkout.projects", query).await["error"]["code"],
            -32602
        );
        let mut b = project_query(&original);
        b["limit"] = json!(1);
        let page = ready(
            &client
                .rpc("sourceControl.checkout.branches", b.clone())
                .await,
        )
        .clone();
        b["cursor"] = page["nextCursor"].clone();
        let later = ready(
            &client
                .rpc("sourceControl.checkout.branches", b.clone())
                .await,
        )
        .clone();
        assert_eq!(later["items"][0]["name"], "release/later-page");
        assert_eq!(later["items"][0]["commitSha"], SHA);
        assert_eq!(later["defaultBranch"], "main");
        let another = capture(&mut client).await;
        b["checkoutId"] = another["checkoutId"].clone();
        b["revision"] = another["revision"].clone();
        assert_eq!(
            client.rpc("sourceControl.checkout.branches", b).await["error"]["code"],
            -32602
        );
        for suffix in [
            "/-/merge_requests/7?view=parallel#note_1",
            "/-/issues/7?x=y#comment",
        ] {
            let mut q = bound(&original);
            q["url"] = json!(format!("{INSTANCE}/{PROJECT}{suffix}"));
            let result = ready(
                &client
                    .rpc("sourceControl.checkout.project", q.clone())
                    .await,
            )
            .clone();
            assert_eq!(result["project"]["projectPath"], PROJECT);
            assert_eq!(result["project"]["defaultBranch"], "main");
            assert_eq!(result["contextUrl"], q["url"]);
        }
        let mut other = project_query(&original);
        other["projectPath"] = json!("Other/Sub/Project");
        let changed = ready(
            &client
                .rpc("sourceControl.checkout.project", other.clone())
                .await,
        )
        .clone();
        assert_eq!(changed["project"]["defaultBranch"], "stable");
        other["limit"] = json!(1);
        other["cursor"] = page["nextCursor"].clone();
        assert_eq!(
            client.rpc("sourceControl.checkout.branches", other).await["error"]["code"],
            -32602,
            "a branch cursor cannot move to an identically named project in another namespace"
        );
        assert_eq!(
            ready(
                &client
                    .rpc("sourceControl.checkout.project", project_query(&original))
                    .await
            )["project"]["projectPath"],
            PROJECT
        );
        let mut invalid = project_query(&original);
        invalid["branch"] = json!("unobserved");
        invalid["commitSha"] = json!(SHA);
        invalid["mode"] = json!("cached");
        assert_eq!(
            client.rpc("sourceControl.checkout.warm", invalid).await["error"]["code"],
            -32602
        );
        assert_eq!(
            success(
                &client
                    .rpc("sourceControl.checkout.release", bound(&original))
                    .await
            )["released"],
            true
        );
        assert!(client
            .rpc("sourceControl.checkout.project", project_query(&original))
            .await
            .get("error")
            .is_some());
        client.close().await;
    }
    let mut guest = h.wss(GUEST).await;
    assert_eq!(
        guest
            .rpc(
                "sourceControl.checkout.capture",
                json!({"provider":"gitlab"})
            )
            .await["error"]["code"],
        -32003
    );
    guest.close().await;
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn checkout_private_reply_after_host_revocation_is_denied_on_original_socket() {
    let h = Harness::with_workspace(false).await;
    let server = h.server.as_ref().unwrap();
    let route = "/api/v4/projects";
    set(server, route, json!([repo()]), None);
    let mut client = h.wss(MEMBER).await;
    let original = capture(&mut client).await;
    let mut foreign = h.wss(MEMBER).await;
    assert_eq!(
        foreign
            .rpc("sourceControl.checkout.projects", bound(&original))
            .await["error"]["code"],
        -32003
    );
    foreign.close().await;
    *server.state.pause.lock().unwrap() = Some(route.into());
    let query = bound(&original);
    let pending = tokio::spawn(async move {
        let result = client.rpc("sourceControl.checkout.projects", query).await;
        client.close().await;
        result
    });
    tokio::time::timeout(ACQUIRE, server.state.entered.notified())
        .await
        .unwrap();
    h.store.remove_host_member(&h.member).await.unwrap();
    server.state.release.notify_one();
    let response = tokio::time::timeout(ACQUIRE, pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response["error"]["code"], -32003);
    assert!(response.get("result").is_none());
    assert!(!response.to_string().contains(PROJECT));
    h.finish().await;
}

#[cfg(target_os = "linux")]
#[path = "checkout/native.rs"]
mod native;

#[path = "checkout/avatars.rs"]
mod avatars;

#[intent_test_macros::daemon_test]
async fn checkout_public_tls_auth_settings_transition_and_cancel_preserve_full_prefix() {
    let server = Server::new().await;
    let other = "https://forge.test:8443/Other";
    let fixtures = vec![
        server.descriptor.clone(),
        GitlabDescriptor::with_loopback_endpoint(
            GitlabInstance::parse(other).unwrap(),
            server.host.base_url(),
        )
        .unwrap(),
    ];
    let h = Harness::with_fixtures(false, server, fixtures).await;
    let server = h.server.as_ref().unwrap();
    let mut c = h.wss(TOKEN).await;
    let original = capture(&mut c).await;
    let connected=c.rpc("sourceControl.connect",json!({"provider":"gitlab","host":"forge.test:8443","instanceBaseUrl":other,"method":"pat","token":"stored-pat"})).await;
    assert!(connected.get("error").is_none(), "{connected}");
    let status = c
        .rpc(
            "sourceControl.authStatus",
            json!({"provider":"gitlab","instanceBaseUrl":other}),
        )
        .await;
    assert_eq!(success(&status)["instanceBaseUrl"], other);
    let captured = ready(
        &c.rpc(
            "sourceControl.checkout.capture",
            json!({"provider":"gitlab","instanceBaseUrl":other}),
        )
        .await,
    )
    .clone();
    assert_eq!(captured["instanceBaseUrl"], other);
    assert!(c
        .rpc("sourceControl.checkout.projects", bound(&original))
        .await
        .get("error")
        .is_some());
    assert!(c.rpc("sourceControl.connect",json!({"provider":"gitlab","host":"other.test","instanceBaseUrl":other,"method":"pat","token":"stored-pat"})).await.get("error").is_some());
    // Hold the real device startup response. Cancelling the old prefix must
    // leave this new-prefix flow intact; only its own scope may cancel it.
    set(
        server,
        "/oauth/authorize_device",
        json!({"device_code":"owned-device","user_code":"CODE","verification_uri":format!("{other}/oauth/device"),"expires_in":600,"interval":60}),
        None,
    );
    *server.state.pause.lock().unwrap() = Some("/oauth/authorize_device".into());
    let mut starter = h.wss(TOKEN).await;
    let pending = tokio::spawn(async move {
        let result = starter
            .rpc(
                "sourceControl.connect",
                json!({"provider":"gitlab","instanceBaseUrl":other,"method":"device"}),
            )
            .await;
        starter.close().await;
        result
    });
    tokio::time::timeout(ACQUIRE, server.state.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        success(
            &c.rpc(
                "sourceControl.cancelAuth",
                json!({"provider":"gitlab","instanceBaseUrl":INSTANCE})
            )
            .await
        )["cancelled"],
        false
    );
    assert_eq!(
        success(
            &c.rpc(
                "sourceControl.cancelAuth",
                json!({"provider":"gitlab","instanceBaseUrl":other})
            )
            .await
        )["cancelled"],
        true
    );
    server.state.release.notify_one();
    let cancelled = tokio::time::timeout(ACQUIRE, pending)
        .await
        .unwrap()
        .unwrap();
    assert!(
        cancelled.get("error").is_some(),
        "cancelled startup cannot publish a flow: {cancelled}"
    );
    c.close().await;
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn checkout_empty_missing_default_and_provider_failures_are_distinct() {
    let h = Harness::with_workspace(false).await;
    let server = h.server.as_ref().unwrap();
    let mut empty = repo();
    empty.as_object_mut().unwrap().remove("default_branch");
    set(server, "/api/v4/projects/Team%2FSub%2FProject", empty, None);
    set(server, BRANCHES, json!([]), None);
    let mut client = h.wss(TOKEN).await;
    let original = capture(&mut client).await;
    let project = ready(
        &client
            .rpc("sourceControl.checkout.project", project_query(&original))
            .await,
    )
    .clone();
    assert!(project["project"].get("defaultBranch").is_none());
    let page = ready(
        &client
            .rpc("sourceControl.checkout.branches", project_query(&original))
            .await,
    )
    .clone();
    assert_eq!(page["items"], json!([]));
    assert!(page.get("defaultBranch").is_none());
    server.set(BRANCHES, 429);
    let limited = client
        .rpc("sourceControl.checkout.branches", project_query(&original))
        .await;
    assert_eq!(success(&limited)["reason"], "rate-limited");
    assert!(success(&limited).get("value").is_none());
    client.close().await;
    h.finish().await;
    // A rate-limited original connection must keep its backoff. The denial
    // arm has an independent owner so it actually receives the original 403.
    let h = Harness::with_workspace(false).await;
    let server = h.server.as_ref().unwrap();
    set(
        server,
        "/api/v4/projects/Team%2FSub%2FProject",
        repo(),
        None,
    );
    let mut client = h.wss(TOKEN).await;
    let original = capture(&mut client).await;
    assert_eq!(
        ready(
            &client
                .rpc("sourceControl.checkout.project", project_query(&original))
                .await
        )["project"]["projectPath"],
        PROJECT
    );
    server.set(BRANCHES, 403);
    let denied = client
        .rpc("sourceControl.checkout.branches", project_query(&original))
        .await;
    assert_eq!(success(&denied)["reason"], "access-denied");
    assert!(success(&denied).get("value").is_none());
    // The known project denial overrides an earlier metadata observation.
    let stale = client
        .rpc("sourceControl.checkout.project", project_query(&original))
        .await;
    assert!(stale.get("error").is_some() || stale["result"]["status"] == "unavailable");
    assert!(!stale.to_string().contains(PROJECT));
    client.close().await;
    h.finish().await;
}

#[path = "checkout/config.rs"]
mod config;
