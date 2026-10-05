use super::*;

#[intent_test_macros::daemon_test]
async fn checkout_owner_avatar_is_opted_in_per_capture_without_extra_requests() {
    let h = Harness::with_workspace(false).await;
    let server = h.server.as_ref().unwrap();
    let mut upstream = repo();
    upstream["avatar_url"] = json!("https://images.example/project.png");
    upstream["namespace"] = json!({"full_path":"Team/Sub","avatar_url":"uploads/owner.png"});
    set(server, "/api/v4/projects", json!([upstream.clone()]), None);
    set(
        server,
        "/api/v4/projects/Team%2FSub%2FProject",
        upstream,
        None,
    );
    let expected = json!({
        "projectPath":PROJECT,"name":"Project","namespace":"Team/Sub",
        "webUrl":format!("{INSTANCE}/{PROJECT}"),
        "cloneUrl":format!("{INSTANCE}/{PROJECT}.git"),"defaultBranch":"main"
    });
    let mut client = h.wss(TOKEN).await;
    let legacy = capture(&mut client).await;
    let before = server.count();
    let page = client
        .rpc("sourceControl.checkout.projects", bound(&legacy))
        .await;
    assert_eq!(ready(&page)["items"], json!([expected.clone()]));
    assert_eq!(server.count(), before + 1);

    let opted = ready(
        &client
            .rpc(
                "sourceControl.checkout.capture",
                json!({
                    "provider":"gitlab","instanceBaseUrl":INSTANCE,"includeOwnerAvatar":true
                }),
            )
            .await,
    )
    .clone();
    let hello = client.rpc("client.hello", json!({})).await;
    assert_eq!(
        success(&hello)["server"]["capabilities"]["gitlabCheckoutOwnerAvatar"],
        1
    );
    let mut with_avatar = expected.clone();
    with_avatar["ownerAvatarUrl"] = json!(format!("{INSTANCE}/uploads/owner.png"));

    let before = server.count();
    // A detail lookup and a page each use their existing single project request.
    let detail = client
        .rpc("sourceControl.checkout.project", project_query(&opted))
        .await;
    assert_eq!(ready(&detail)["project"], with_avatar);
    assert_eq!(server.count(), before + 1);
    let page = client
        .rpc("sourceControl.checkout.projects", bound(&opted))
        .await;
    assert_eq!(ready(&page)["items"], json!([with_avatar.clone()]));
    assert_eq!(server.count(), before + 2);
    let cached = client
        .rpc("sourceControl.checkout.project", project_query(&opted))
        .await;
    assert_eq!(ready(&cached)["project"], with_avatar);
    assert_eq!(
        server.count(),
        before + 2,
        "avatar lookup must not add requests"
    );

    // Concurrent captures on the same connection retain their own projection.
    let detail = client
        .rpc("sourceControl.checkout.project", project_query(&legacy))
        .await;
    assert_eq!(ready(&detail)["project"], expected);
    let disabled = ready(
        &client
            .rpc(
                "sourceControl.checkout.capture",
                json!({
                    "provider":"gitlab","instanceBaseUrl":INSTANCE,"includeOwnerAvatar":false
                }),
            )
            .await,
    )
    .clone();
    let page = client
        .rpc("sourceControl.checkout.projects", bound(&disabled))
        .await;
    assert_eq!(ready(&page)["items"], json!([expected.clone()]));

    // A project image or a different namespace cannot stand in for the owner.
    let mut wrong = repo();
    wrong["avatar_url"] = json!("https://images.example/project.png");
    wrong["namespace"] = json!({"full_path":"Other/Sub","avatar_url":"uploads/other.png"});
    set(server, "/api/v4/projects", json!([wrong]), None);
    let before = server.count();
    let page = client
        .rpc("sourceControl.checkout.projects", bound(&opted))
        .await;
    assert_eq!(ready(&page)["items"], json!([expected]));
    assert_eq!(server.count(), before + 1);
    let invalid = client
        .rpc(
            "sourceControl.checkout.capture",
            json!({
                "provider":"gitlab","instanceBaseUrl":INSTANCE,"includeOwnerAvatar":"true"
            }),
        )
        .await;
    assert_eq!(invalid["error"]["code"], -32602);
    assert_eq!(server.count(), before + 1);
    assert!(h.store.list_workspaces(true).await.unwrap().is_empty());
    client.close().await;
    h.finish().await;
}
