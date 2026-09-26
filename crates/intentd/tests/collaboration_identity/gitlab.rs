use super::*;
use std::fmt::Write as _;

fn identity() -> Value {
    json!({"provider":"gitlab","host":HOST,"externalUserId":"4242"})
}

async fn connected(h: &Harness, rpc: &mut Ws) {
    let v = wss_rpc(
        rpc,
        1,
        "identity.connect",
        json!({
            "provider":"gitlab", "method":"pat", "token":PAT_TOKEN
        }),
    )
    .await;
    assert_eq!(
        v["result"],
        json!({"ok":true,"method":"pat","purpose":"collaboration"}),
        "{v}"
    );
    assert!(read_secrets(&h.secrets_file)["sourceControl.gitlab.token"].is_null());
}

#[tokio::test]
async fn collaboration_credential_isolated_from_repository_and_child_lookup_over_wss() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    connected(&h, &mut rpc).await;
    let v = wss_rpc(
        &mut rpc,
        2,
        "identity.authStatus",
        json!({"provider":"gitlab"}),
    )
    .await;
    assert_eq!(v["result"]["isConfigured"], true, "{v}");
    assert_eq!(v["result"]["purpose"], "collaboration");
    assert_eq!(v["result"]["requestedScopes"], json!(["api"]));
    assert!(v["result"].get("grantedScopes").is_some());
    let v = wss_rpc(
        &mut rpc,
        3,
        "sourceControl.authStatus",
        json!({"provider":"gitlab"}),
    )
    .await;
    assert_eq!(v["result"]["isConfigured"], false, "{v}");
    let v = wss_rpc(
        &mut rpc,
        4,
        "sourceControl.identityProof.create",
        json!({
            "provider":"gitlab", "nonce":"isolated", "hostLabel":"test"
        }),
    )
    .await;
    assert_eq!(v["error"]["data"]["code"], "gitlab-not-connected", "{v}");
    let v = wss_rpc(
        &mut rpc,
        5,
        "identity.getUser",
        json!({"provider":"gitlab"}),
    )
    .await;
    assert_eq!(v["result"]["user"]["id"], "4242", "{v}");
    let v = wss_rpc(
        &mut rpc,
        6,
        "sourceControl.identityProof.create",
        json!({
            "provider":"gitlab", "purpose":"collaboration", "expectedIdentity":identity(),
            "nonce":"isolated", "hostLabel":"test"
        }),
    )
    .await;
    let proof_id = v["result"]["proofId"]
        .as_str()
        .expect("collaboration proof")
        .to_owned();
    assert_eq!(mock.flags.snippets.lock().unwrap()[0].1, PAT_TOKEN);
    let v = wss_rpc(
        &mut rpc,
        7,
        "sourceControl.identityProof.delete",
        json!({
            "provider":"gitlab", "purpose":"collaboration", "proofId":proof_id
        }),
    )
    .await;
    assert_eq!(v["result"], json!({"ok":true}), "{v}");
    assert!(mock.flags.snippets.lock().unwrap().is_empty());
}

#[tokio::test]
async fn collaboration_selection_and_session_survive_repository_disconnect_over_wss() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    connected(&h, &mut rpc).await;
    let v = wss_rpc(&mut rpc, 2, "identity.select", identity()).await;
    let selected = v["result"]["principal"].clone();
    assert_eq!(selected["identity"], identity(), "{v}");
    assert_eq!(selected["hostRole"], "owner");
    let v = wss_rpc(
        &mut rpc,
        3,
        "sourceControl.connect",
        json!({
            "provider":"gitlab", "method":"pat", "token":ACCESS_TOKEN
        }),
    )
    .await;
    assert_eq!(v["result"]["ok"], true, "{v}");
    let before = read_secrets(&h.secrets_file);
    let v = wss_rpc(
        &mut rpc,
        4,
        "identity.connect",
        json!({
            "provider":"gitlab", "method":"pat", "token":PAT_TOKEN
        }),
    )
    .await;
    assert_eq!(v["result"]["ok"], true, "{v}");
    assert_eq!(
        read_secrets(&h.secrets_file),
        before,
        "repository secrets unchanged"
    );
    // GitHub is currently the only repository implementation. Applying that
    // default must not select it over a separately chosen GitLab identity.
    let v = wss_rpc(
        &mut rpc,
        40,
        "settings.update",
        json!({"changes":[{"path":"sourceControl.activeProvider","value":"github"}]}),
    )
    .await;
    assert!(v.get("error").is_none(), "{v}");
    let v = wss_rpc(&mut rpc, 41, "principal.me", json!({})).await;
    assert_eq!(
        v["result"]["identity"],
        identity(),
        "repository default cannot re-key identity: {v}"
    );
    let v = wss_rpc(
        &mut rpc,
        5,
        "sourceControl.revoke",
        json!({"provider":"gitlab"}),
    )
    .await;
    assert_eq!(v["result"], json!({"ok":true}), "{v}");
    let v = wss_rpc(&mut rpc, 6, "principal.me", json!({})).await;
    assert_eq!(v["result"]["identity"], identity(), "{v}");
    assert_eq!(v["result"]["id"], selected["id"]);
    let v = wss_rpc(&mut rpc, 7, "identity.revoke", json!({"provider":"gitlab"})).await;
    assert_eq!(v["result"], json!({"ok":true}), "{v}");
    let v = wss_rpc(&mut rpc, 8, "principal.me", json!({})).await;
    assert_eq!(v["result"]["identity"], identity(), "{v}");
    let mut second = connect_ws(h.port, h.cfg.clone()).await;
    let v = wss_rpc(&mut second, 9, "principal.me", json!({})).await;
    assert_eq!(
        v["result"]["identity"],
        identity(),
        "same bearer remains admitted: {v}"
    );
}

#[tokio::test]
async fn collaboration_cancel_is_flow_and_instance_scoped_over_wss() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let v = wss_rpc(
        &mut rpc,
        1,
        "identity.connect",
        json!({"provider":"gitlab"}),
    )
    .await;
    let flow = v["result"]["flowId"].as_str().expect("flowId").to_owned();
    let invalid = wss_rpc(
        &mut rpc,
        10,
        "identity.connect",
        json!({"provider":"gitlab","method":"pat"}),
    )
    .await;
    assert_eq!(invalid["error"]["code"], -32602, "{invalid}");
    let v = wss_rpc(
        &mut rpc,
        2,
        "identity.cancelAuth",
        json!({
            "provider":"gitlab", "host":"other.example", "flowId":flow
        }),
    )
    .await;
    assert_eq!(v["result"], json!({"ok":true,"cancelled":false}), "{v}");
    let v = wss_rpc(
        &mut rpc,
        3,
        "identity.cancelAuth",
        json!({
            "provider":"gitlab", "flowId":"stale"
        }),
    )
    .await;
    assert_eq!(v["result"], json!({"ok":true,"cancelled":false}), "{v}");
    let v = wss_rpc(
        &mut rpc,
        4,
        "identity.cancelAuth",
        json!({
            "provider":"gitlab", "flowId":flow
        }),
    )
    .await;
    assert_eq!(v["result"], json!({"ok":true,"cancelled":true}), "{v}");
    let v = wss_rpc(&mut rpc, 5, "identity.select", identity()).await;
    assert_eq!(v["error"]["data"]["code"], "gitlab-not-connected", "{v}");
    let v = wss_rpc(
        &mut rpc,
        6,
        "identity.getUser",
        json!({"provider":"gitlab"}),
    )
    .await;
    assert_eq!(v["result"], json!({"user":null}), "{v}");
    assert!(read_secrets(&h.secrets_file)["sourceControl.gitlab.token"].is_null());
}

async fn collaboration_subscriber(h: &Harness) -> Ws {
    let mut sub = connect_ws(h.port, h.cfg.clone()).await;
    let v = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({"eventTypes":["identity:auth-changed"]}),
    )
    .await;
    assert!(v.get("error").is_none(), "{v}");
    sub
}

#[tokio::test]
async fn collaboration_gitlab_refresh_is_serialized_and_disconnect_preserves_identity() {
    let mock = spawn_mock_gitlab().await;
    mock.flags.short_lived.store(true, Ordering::SeqCst);
    let h = boot(&mock).await;
    let mut sub = collaboration_subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let v = wss_rpc(
        &mut rpc,
        1,
        "identity.connect",
        json!({"provider":"gitlab"}),
    )
    .await;
    assert_eq!(v["result"]["purpose"], "collaboration", "{v}");
    mock.flags.authorize.store(true, Ordering::SeqCst);
    let event = super::collaboration_github::identity_event(&mut sub, "authorized").await;
    assert_eq!(event["flowId"], v["result"]["flowId"]);
    let mut second = connect_ws(h.port, h.cfg.clone()).await;
    let (a, b) = tokio::join!(
        wss_rpc(
            &mut rpc,
            2,
            "identity.authStatus",
            json!({"provider":"gitlab"})
        ),
        wss_rpc(
            &mut second,
            3,
            "identity.authStatus",
            json!({"provider":"gitlab"})
        )
    );
    for v in [a, b] {
        assert_eq!(v["result"]["isConfigured"], true, "{v}");
        assert_eq!(v["result"]["grantedScopes"], json!(["api"]), "{v}");
    }
    assert_eq!(mock.flags.refresh_exchanges.load(Ordering::SeqCst), 1);
    let v = wss_rpc(&mut rpc, 4, "identity.select", identity()).await;
    let principal = v["result"]["principal"]["id"]
        .as_str()
        .expect("selected primary")
        .to_owned();
    assert_eq!(v["result"]["principal"]["identity"], identity(), "{v}");
    let v=wss_rpc(&mut rpc,5,"sourceControl.identityProof.create",json!({"provider":"gitlab","purpose":"collaboration","expectedIdentity":identity(),"nonce":"refreshed","hostLabel":"test"})).await;
    assert!(v.get("error").is_none(), "{v}");
    assert_eq!(
        mock.flags.snippets.lock().unwrap()[0].1,
        ROTATED_ACCESS_TOKEN
    );
    mock.flags.reject_rotated.store(true, Ordering::SeqCst);
    let v = wss_rpc(
        &mut rpc,
        6,
        "identity.authStatus",
        json!({"provider":"gitlab"}),
    )
    .await;
    assert_eq!(v["result"]["isConfigured"], false, "{v}");
    let event = super::collaboration_github::identity_event(&mut sub, "expired").await;
    assert_eq!(
        event,
        json!({"provider":"gitlab","host":HOST,"purpose":"collaboration","status":"expired"})
    );
    let v = wss_rpc(&mut rpc, 7, "principal.me", json!({})).await;
    assert_eq!(v["result"]["id"], principal, "{v}");
    assert_eq!(v["result"]["identity"], identity());
    assert!(read_secrets(&h.secrets_file)["sourceControl.gitlab.token"].is_null());
}

#[tokio::test]
async fn collaboration_gitlab_cancel_inflight_grant_keeps_new_pat_and_repo_binding() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    mock.flags.authorize.store(true, Ordering::SeqCst);
    mock.flags.hold_authorize.store(true, Ordering::SeqCst);
    let v = wss_rpc(
        &mut rpc,
        1,
        "identity.connect",
        json!({"provider":"gitlab"}),
    )
    .await;
    await_latch(&mock.flags.authorize_held, "collaboration grant held").await;
    let v = wss_rpc(
        &mut rpc,
        2,
        "identity.cancelAuth",
        json!({"provider":"gitlab","flowId":v["result"]["flowId"]}),
    )
    .await;
    assert_eq!(v["result"]["cancelled"], true, "{v}");
    connected(&h, &mut rpc).await;
    mock.flags.release_authorize.notify_one();
    let v=wss_rpc(&mut rpc,3,"sourceControl.identityProof.create",json!({"provider":"gitlab","purpose":"collaboration","expectedIdentity":identity(),"nonce":"pat-wins","hostLabel":"h"})).await;
    assert!(v.get("error").is_none(), "{v}");
    assert_eq!(mock.flags.snippets.lock().unwrap()[0].1, PAT_TOKEN);
    let v = wss_rpc(
        &mut rpc,
        4,
        "settings.update",
        json!({"changes":[{"path":"sourceControl.gitlab.host","value":"other.example"},{"path":"sourceControl.gitlab.apiBaseUrl","value":mock.base_uri}]}),
    )
    .await;
    assert!(v.get("error").is_none(), "{v}");
    let v = wss_rpc(
        &mut rpc,
        5,
        "identity.authStatus",
        json!({"provider":"gitlab"}),
    )
    .await;
    assert_eq!(v["result"]["host"], HOST, "{v}");
    assert_eq!(v["result"]["isConfigured"], true);
    let v = wss_rpc(
        &mut rpc,
        6,
        "identity.authStatus",
        json!({"provider":"gitlab","host":"other.example"}),
    )
    .await;
    assert_eq!(v["result"]["isConfigured"], false, "{v}");
    assert_eq!(v["result"]["deviceGrantSupported"], false);
    let v = wss_rpc(
        &mut rpc,
        60,
        "identity.connect",
        json!({"provider":"gitlab","host":"OTHER.EXAMPLE","method":"pat","token":ACCESS_TOKEN}),
    )
    .await;
    assert_eq!(v["result"]["ok"], true, "{v}");
    let v = wss_rpc(
        &mut rpc,
        61,
        "identity.authStatus",
        json!({"provider":"gitlab","host":"other.example"}),
    )
    .await;
    assert_eq!(v["result"]["isConfigured"], true, "{v}");
    let v = wss_rpc(
        &mut rpc,
        7,
        "identity.revoke",
        json!({"provider":"gitlab","host":"other.example"}),
    )
    .await;
    assert_eq!(v["result"], json!({"ok":true}), "{v}");
    let v = wss_rpc(
        &mut rpc,
        8,
        "identity.getUser",
        json!({"provider":"gitlab"}),
    )
    .await;
    assert_eq!(v["result"]["user"]["id"], "4242", "{v}");
}

#[tokio::test]
async fn collaboration_gitlab_wrong_account_instance_and_missing_expected_identity_publish_nothing()
{
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    connected(&h, &mut rpc).await;
    let mut wrong = identity();
    wrong["externalUserId"] = json!("wrong");
    let mut other = identity();
    other["host"] = json!("other.example");
    for expected in [
        wrong,
        other,
        json!({"provider":"github","host":"github.com","externalUserId":"4242"}),
    ] {
        let v=wss_rpc(&mut rpc,2,"sourceControl.identityProof.create",json!({"provider":"gitlab","purpose":"collaboration","expectedIdentity":expected,"nonce":"wrong","hostLabel":"h"})).await;
        assert_eq!(v["error"]["data"]["code"], "identity-mismatch", "{v}");
    }
    let v = wss_rpc(
        &mut rpc,
        3,
        "sourceControl.identityProof.create",
        json!({"provider":"gitlab","purpose":"collaboration","nonce":"wrong","hostLabel":"h"}),
    )
    .await;
    assert_eq!(v["error"]["code"], -32602, "{v}");
    assert!(mock.flags.snippets.lock().unwrap().is_empty());
    let v = wss_rpc(
        &mut rpc,
        4,
        "identity.authStatus",
        json!({"provider":"gitlab"}),
    )
    .await;
    assert_eq!(
        v["result"]["grantedScopes"],
        Value::Null,
        "unreported PAT permissions are unknown: {v}"
    );
}

#[tokio::test]
async fn collaboration_auth_is_owner_only_and_repository_disconnect_keeps_member_bearers() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let store = intent_store::Store::open(&h.secrets_file.parent().unwrap().join("intentd.db"))
        .await
        .unwrap();
    let mut person = store.get_primary_principal().await.unwrap();
    person.id = intent_core::PrincipalId::new();
    person.is_primary = false;
    person.set_identity(serde_json::from_value(identity()).unwrap());
    store.upsert_principal(&person).await.unwrap();
    let bearer = "fe".repeat(32);
    let hash =
        Sha256::digest(bearer.as_bytes())
            .iter()
            .fold(String::with_capacity(64), |mut out, b| {
                let _ = write!(out, "{b:02x}");
                out
            });
    store
        .insert_principal_credential(&person.id, &hash)
        .await
        .unwrap();
    let url = format!("wss://localhost:{}/ws?token={bearer}", h.port);
    let mut guest = common::wss_connect_with_retry(h.port, h.cfg.clone(), &url).await;
    for member in [false, true] {
        if member {
            sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
                .bind(&person.id.0)
                .bind(intent_core::now_iso())
                .execute(store.write_pool())
                .await
                .unwrap();
        }
        for method in [
            "identity.authStatus",
            "identity.connect",
            "identity.cancelAuth",
            "identity.revoke",
            "identity.getUser",
            "identity.select",
        ] {
            let v = wss_rpc(
                &mut guest,
                1,
                method,
                json!({"provider":"gitlab","flowId":"f","externalUserId":"42"}),
            )
            .await;
            assert_eq!(v["error"]["code"], -32003, "{method}: {v}");
        }
    }
    let mut owner = connect_ws(h.port, h.cfg.clone()).await;
    connected(&h, &mut owner).await;
    let v = wss_rpc(&mut owner, 2, "identity.select", identity()).await;
    assert_eq!(v["error"]["data"]["code"], "identity-in-use", "{v}");
    assert_eq!(
        store.get_primary_principal().await.unwrap().identity_key(),
        None
    );
    for method in ["sourceControl.revoke", "identity.revoke"] {
        let v = wss_rpc(&mut owner, 2, method, json!({"provider":"gitlab"})).await;
        assert_eq!(v["result"], json!({"ok":true}), "{v}");
    }
    let v = wss_rpc(&mut guest, 3, "principal.me", json!({})).await;
    assert_eq!(v["result"]["id"], person.id.0, "{v}");
    assert_eq!(v["result"]["hostRole"], "member");
    let mut reconnected = common::wss_connect_with_retry(h.port, h.cfg.clone(), &url).await;
    let v = wss_rpc(&mut reconnected, 4, "principal.me", json!({})).await;
    assert_eq!(v["result"]["hostRole"], "member", "{v}");
    assert!(store
        .lookup_principal_credential(&hash)
        .await
        .unwrap()
        .unwrap()
        .is_active());
}
