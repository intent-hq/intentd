//! Fixed configuration through the real native checkout WSS/UDS path.
use super::*;
use base64::Engine as _;

const FILE: &str = "/api/v4/projects/Team%2FSub%2FProject/repository/files/.intent%2Fconfig.json";
const EXACT_BRANCH: &str =
    "/api/v4/projects/Team%2FSub%2FProject/repository/branches/release%2Fconfig";
const PROJECT_ROUTE: &str = "/api/v4/projects/Team%2FSub%2FProject";

fn file(text: &str) -> Value {
    json!({"file_path":".intent/config.json","commit_id":SHA,"encoding":"base64",
        "content":base64::engine::general_purpose::STANDARD.encode(text)})
}
async fn selected(h: &Harness, client: &mut Client) -> Value {
    let server = h.server.as_ref().unwrap();
    set(server, PROJECT_ROUTE, repo(), None);
    set(server, BRANCHES, json!([branch("release/config")]), None);
    set(server, EXACT_BRANCH, branch("release/config"), None);
    let original = capture(client).await;
    ready(
        &client
            .rpc("sourceControl.checkout.branches", project_query(&original))
            .await,
    );
    let mut q = project_query(&original);
    q["branch"] = json!("release/config");
    q["commitSha"] = json!(SHA);
    q
}

#[intent_test_macros::daemon_test]
async fn checkout_config_real_transports_valid_absent_and_tolerant_present() {
    for remote in [true, false] {
        let h = Harness::with_workspace(false).await;
        let server = h.server.as_ref().unwrap();
        let mut client = if remote {
            h.wss(TOKEN).await
        } else {
            h.uds().await
        };
        let q = selected(&h, &mut client).await;
        let hello = client.rpc("client.hello", json!({})).await;
        assert_eq!(
            success(&hello)["server"]["capabilities"]["gitlabCheckoutRepoConfig"],
            1
        );
        for (text, expected) in [
            (
                r#"{"setupScript":"npm ci","extra":{"keep":true}}"#,
                json!({"setupScript":"npm ci","extra":{"keep":true}}),
            ),
            ("{bad", json!({})),
            ("[]", json!({})),
            ("null", json!({})),
            (r#"{"setupScript":42}"#, json!({})),
        ] {
            set(server, FILE, file(text), None);
            let response = client
                .rpc("sourceControl.checkout.repoConfig", q.clone())
                .await;
            assert_eq!(response["jsonrpc"], "2.0");
            assert!(response["id"].is_number());
            assert_eq!(
                ready(&response),
                &json!({"projectPath":PROJECT,"branch":"release/config","commitSha":SHA,"config":expected,"exists":true})
            );
        }
        for (encoding, content) in [("base64", "!"), ("base64", "/w=="), ("raw", "{}")] {
            let mut invalid = file("{}");
            invalid["encoding"] = json!(encoding);
            invalid["content"] = json!(content);
            set(server, FILE, invalid, None);
            let response = client
                .rpc("sourceControl.checkout.repoConfig", q.clone())
                .await;
            assert_eq!(ready(&response)["config"], json!({}));
            assert_eq!(ready(&response)["exists"], true);
        }
        server
            .state
            .replies
            .lock()
            .unwrap()
            .insert(FILE.into(), (404, json!({"message":"404 File Not Found"})));
        let response = client
            .rpc("sourceControl.checkout.repoConfig", q.clone())
            .await;
        assert_eq!(
            ready(&response),
            &json!({"projectPath":PROJECT,"branch":"release/config","commitSha":SHA,"config":null,"exists":false})
        );
        // A missing file must not retire the original project or require recapture.
        set(server, FILE, file("{}"), None);
        assert_eq!(
            ready(
                &client
                    .rpc("sourceControl.checkout.repoConfig", q.clone())
                    .await
            )["exists"],
            true
        );
        let routes = server.state.routes.lock().unwrap().clone();
        assert!(routes
            .iter()
            .filter(|(_, path)| path.starts_with(FILE))
            .all(|(method, path)| method == "GET" && path == &format!("{FILE}?ref={SHA}")));
        assert!(h.store.list_workspaces(true).await.unwrap().is_empty());
        client.close().await;
        h.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn checkout_config_wrong_binding_selection_and_arbitrary_inputs_do_not_read() {
    let h = Harness::with_workspace(false).await;
    let server = h.server.as_ref().unwrap();
    let mut client = h.wss(MEMBER).await;
    let q = selected(&h, &mut client).await;
    let before = server.count();
    let mut foreign = h.wss(MEMBER).await;
    assert_eq!(
        foreign
            .rpc("sourceControl.checkout.repoConfig", q.clone())
            .await["error"]["code"],
        -32003
    );
    for (field, value) in [
        ("revision", "other"),
        ("branch", "unobserved"),
        ("projectPath", "Other/Project"),
        ("commitSha", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        ("path", "secret"),
        ("url", "https://other"),
        ("mode", "cached"),
    ] {
        let mut invalid = q.clone();
        invalid[field] = json!(value);
        assert!(
            client
                .rpc("sourceControl.checkout.repoConfig", invalid)
                .await
                .get("error")
                .is_some(),
            "{field}"
        );
    }
    assert_eq!(server.count(), before);
    success(
        &client
            .rpc("sourceControl.checkout.release", bound(&q))
            .await,
    );
    assert!(client
        .rpc("sourceControl.checkout.repoConfig", q)
        .await
        .get("error")
        .is_some());
    assert_eq!(server.count(), before);
    foreign.close().await;
    client.close().await;
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn checkout_config_provider_failures_never_become_absent() {
    for (status, body, reason) in [
        (401, json!({}), "access-denied"),
        (403, json!({}), "access-denied"),
        (
            404,
            json!({"message":"404 Project Not Found"}),
            "access-denied",
        ),
        (
            404,
            json!({"message":"404 Commit Not Found"}),
            "access-denied",
        ),
        (429, json!({}), "rate-limited"),
        (500, json!({}), "unreachable"),
        (200, Value::Null, "unreachable"),
        (200, json!({"bad":"envelope"}), "retired"),
    ] {
        let h = Harness::with_workspace(false).await;
        let server = h.server.as_ref().unwrap();
        let mut client = h.wss(TOKEN).await;
        let q = selected(&h, &mut client).await;
        server
            .state
            .replies
            .lock()
            .unwrap()
            .insert(FILE.into(), (status, body));
        let response = client.rpc("sourceControl.checkout.repoConfig", q).await;
        assert_eq!(
            success(&response),
            &json!({"status":"unavailable","reason":reason})
        );
        client.close().await;
        h.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn checkout_config_rejects_branch_project_and_authority_changes_during_read() {
    for change in ["branch", "project", "caller", "credential", "disconnect"] {
        let h = Harness::with_workspace(false).await;
        let server = h.server.as_ref().unwrap();
        let mut client = h.wss(MEMBER).await;
        let q = selected(&h, &mut client).await;
        set(server, FILE, file(r#"{"setupScript":"private"}"#), None);
        *server.state.pause.lock().unwrap() = Some(FILE.into());
        let pending = tokio::spawn(async move {
            let response = client.rpc("sourceControl.checkout.repoConfig", q).await;
            client.close().await;
            response
        });
        tokio::time::timeout(ACQUIRE, server.state.entered.notified())
            .await
            .unwrap();
        match change {
            "branch" => {
                let mut b = branch("release/config");
                b["commit"]["id"] = json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                set(server, EXACT_BRANCH, b, None);
            }
            "project" => {
                let mut replacement = repo();
                replacement["id"] = json!(99);
                set(server, PROJECT_ROUTE, replacement, None);
            }
            "caller" => {
                h.store.remove_host_member(&h.member).await.unwrap();
            }
            "credential" | "disconnect" => {
                let mut owner = h.uds().await;
                success(
                    &owner
                        .rpc(
                            "sourceControl.revoke",
                            json!({"provider":"gitlab","instanceBaseUrl":INSTANCE}),
                        )
                        .await,
                );
                if change == "credential" {
                    success(&owner.rpc("sourceControl.connect",json!({"provider":"gitlab","instanceBaseUrl":INSTANCE,"method":"pat","token":"stored-pat"})).await);
                }
                owner.close().await;
            }
            _ => unreachable!(),
        }
        server.state.release.notify_one();
        let response = tokio::time::timeout(ACQUIRE, pending)
            .await
            .unwrap()
            .unwrap();
        if change == "branch" {
            assert_eq!(
                success(&response),
                &json!({"status":"unavailable","reason":"branch-changed"})
            );
        } else if change == "project" {
            assert_eq!(
                success(&response),
                &json!({"status":"unavailable","reason":"retired"})
            );
        } else {
            assert!(
                response.get("error").is_some() || response["result"]["status"] == "unavailable",
                "{response}"
            );
        }
        assert!(!response.to_string().contains("private"));
        h.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn checkout_config_changed_or_deleted_branch_prevents_file_request() {
    let h = Harness::with_workspace(false).await;
    let server = h.server.as_ref().unwrap();
    let mut client = h.wss(TOKEN).await;
    let q = selected(&h, &mut client).await;
    for (status, body) in [
        (404, json!({"message":"404 Branch Not Found"})),
        (
            200,
            json!({"name":"release/config","commit":{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}),
        ),
    ] {
        server
            .state
            .replies
            .lock()
            .unwrap()
            .insert(EXACT_BRANCH.into(), (status, body));
        let response = client
            .rpc("sourceControl.checkout.repoConfig", q.clone())
            .await;
        assert_eq!(
            success(&response),
            &json!({"status":"unavailable","reason":"branch-changed"})
        );
        assert!(!server
            .state
            .routes
            .lock()
            .unwrap()
            .iter()
            .any(|(_, path)| path.starts_with(FILE)));
    }
    set(server, EXACT_BRANCH, branch("release/config"), None);
    set(server, FILE, file("{}"), None);
    assert_eq!(
        ready(&client.rpc("sourceControl.checkout.repoConfig", q).await)["exists"],
        true
    );
    client.close().await;
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn checkout_config_literal_dollar_branch_remains_ready() {
    const SHA: &str = "0123456789012345678901234567890123456789";
    const NAME: &str = "release/cost$usd";
    const BRANCHES: &str = "/api/v4/projects/Team%2FSub%2FProject/repository/branches";
    const FILE: &str =
        "/api/v4/projects/Team%2FSub%2FProject/repository/files/.intent%2Fconfig.json";
    intent_git::native_checkout::NativeCheckoutSelection::new(NAME, SHA).unwrap();
    let h = Harness::with_workspace(false).await;
    let server = h.server.as_ref().unwrap();
    let observed = json!({"name": NAME, "commit": {"id": SHA}, "protected": false});
    server.state.replies.lock().unwrap().insert(
        "/api/v4/projects/Team%2FSub%2FProject".into(),
        (200, json!({"id":42,"path_with_namespace":PROJECT,"web_url":format!("{INSTANCE}/{PROJECT}"),"default_branch":NAME})),
    );
    server
        .state
        .replies
        .lock()
        .unwrap()
        .insert(BRANCHES.into(), (200, json!([observed])));
    server.state.replies.lock().unwrap().insert(FILE.into(), (200, json!({"file_path":".intent/config.json","commit_id":SHA,"encoding":"base64","content":"e30="})));
    let mut client = h.wss(TOKEN).await;
    let capture = client
        .rpc(
            "sourceControl.checkout.capture",
            json!({"provider":"gitlab","instanceBaseUrl":INSTANCE}),
        )
        .await;
    let original = &success(&capture)["value"];
    let mut q = json!({"checkoutId":original["checkoutId"],"revision":original["revision"],"projectPath":PROJECT});
    let branches = client
        .rpc("sourceControl.checkout.branches", q.clone())
        .await;
    assert_eq!(success(&branches)["value"]["items"][0]["name"], NAME);
    q["branch"] = json!(NAME);
    q["commitSha"] = json!(SHA);
    // GitLab GitRefsFinder escapes the term, then unescapes the FIRST dollar.
    // ^release/cost$usd$ becomes ^release/cost$usd\$ and cannot match this ref.
    // Exact route matching keeps ordinary branch observations unchanged.
    let search_route = format!("{BRANCHES}?search=%5Erelease%2Fcost%24usd%24&page=1&per_page=100");
    server
        .state
        .replies
        .lock()
        .unwrap()
        .insert(search_route.clone(), (200, json!([])));
    let exact_route = format!("{BRANCHES}/release%2Fcost%24usd");
    set(
        server,
        &exact_route,
        json!({"name":NAME,"commit":{"id":SHA}}),
        None,
    );
    let response = client.rpc("sourceControl.checkout.repoConfig", q).await;
    let routes = server.state.routes.lock().unwrap().clone();

    client.close().await;
    h.finish().await;
    assert_eq!(
        success(&response)["status"],
        "ready",
        "unchanged valid literal-dollar branch: {response}"
    );
    assert_eq!(
        routes
            .iter()
            .filter(|(method, path)| method == "GET" && path == &exact_route)
            .count(),
        2,
        "{routes:?}"
    );
    assert!(
        !routes.iter().any(|(_, path)| path == &search_route),
        "{routes:?}"
    );
}
