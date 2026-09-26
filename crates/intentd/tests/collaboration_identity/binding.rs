use super::*;
use intent_core::FileSecretStore;

async fn settings(rpc: &mut Ws, changes: Value) {
    let response = wss_rpc(rpc, 800, "settings.update", json!({"changes":changes})).await;
    assert!(response.get("error").is_none(), "{response}");
}

fn credential_store(h: &Harness) -> FileSecretStore {
    let mut dir = h.secrets_file.as_os_str().to_os_string();
    dir.push(".collaboration");
    let path = std::fs::read_dir(std::path::PathBuf::from(dir))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "json"))
        .expect("one isolated collaboration credential");
    FileSecretStore::with_path(path)
}

async fn restart(h: &mut Harness, mock: &MockGitlab) {
    h.daemon.child.kill().unwrap();
    h.daemon.child.wait().unwrap();
    let secrets = h.secrets_file.to_string_lossy().to_string();
    h.daemon = Daemon {
        child: spawn_serve(
            h.data_dir.path(),
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("INTENTD_SECRETS_FILE", &secrets),
                ("INTENTD_GITLAB_API_BASE_URI", &mock.base_uri),
            ],
        ),
    };
    let socket = h.data_dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    h.port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    h.cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
}

#[derive(Clone, Copy)]
enum Change {
    Host,
    Client,
    Endpoint,
    Restart,
    Pat,
}

async fn binding_survives(change: Change) {
    let a = spawn_mock_gitlab().await;
    let b = spawn_mock_gitlab().await;
    let mut h = boot(&a).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let host = intent_sourcecontrol::gitlab_auth::GitlabHost::parse(&a.base_uri)
        .unwrap()
        .host()
        .to_owned();
    settings(
        &mut rpc,
        json!([
            {"path":"sourceControl.gitlab.host","value":host},
            {"path":"sourceControl.gitlab.oauthClientId","value":"application-a"},
            {"path":"sourceControl.gitlab.apiBaseUrl","value":a.base_uri}
        ]),
    )
    .await;
    let is_pat = matches!(change, Change::Pat);
    let params = json!({"provider":"gitlab","host":host});
    let identity = json!({"provider":"gitlab","host":host,"externalUserId":"4242"});
    if is_pat {
        let result = wss_rpc(
            &mut rpc,
            1,
            "identity.connect",
            json!({
                "provider":"gitlab","host":host,"method":"pat","token":PAT_TOKEN
            }),
        )
        .await;
        assert_eq!(result["result"]["ok"], true, "{result}");
    } else {
        let mut sub = super::collaboration_identity::collaboration_subscriber(&h).await;
        let result = wss_rpc(&mut rpc, 1, "identity.connect", params.clone()).await;
        assert!(result["result"]["flowId"].is_string(), "{result}");
        a.flags.authorize.store(true, Ordering::SeqCst);
        super::collaboration_github::identity_event(&mut sub, "authorized").await;
    }
    let selected = wss_rpc(&mut rpc, 2, "identity.select", identity.clone()).await;
    assert_eq!(
        selected["result"]["principal"]["identity"], identity,
        "{selected}"
    );
    let proof_params = json!({"provider":"gitlab","host":host,"purpose":"collaboration",
        "expectedIdentity":identity,"nonce":"binding","hostLabel":"fixture"});
    let created = wss_rpc(
        &mut rpc,
        3,
        "sourceControl.identityProof.create",
        proof_params.clone(),
    )
    .await;
    let proof_id = created["result"]["proofId"]
        .as_str()
        .expect("original proof")
        .to_owned();
    let credential = credential_store(&h);
    if !is_pat {
        credential
            .store("sourceControl.gitlab.tokenExpiresAt", "1")
            .unwrap();
    }
    let changes = match change {
        Change::Host | Change::Restart => json!([
            {"path":"sourceControl.gitlab.host","value":b.base_uri},
            {"path":"sourceControl.gitlab.oauthClientId","value":"application-b"},
            {"path":"sourceControl.gitlab.apiBaseUrl","value":b.base_uri}
        ]),
        Change::Client => json!([
            {"path":"sourceControl.gitlab.oauthClientId","value":"application-b"}
        ]),
        Change::Endpoint | Change::Pat => json!([
            {"path":"sourceControl.gitlab.apiBaseUrl","value":b.base_uri}
        ]),
    };
    settings(&mut rpc, changes).await;
    if matches!(change, Change::Restart) {
        drop(rpc);
        restart(&mut h, &b).await;
        rpc = connect_ws(h.port, h.cfg.clone()).await;
    }
    FileSecretStore::with_path(h.secrets_file.clone())
        .store("sourceControl.gitlab.token", "repository-binding-canary")
        .unwrap();
    let repository = read_secrets(&h.secrets_file);
    a.flags.requests.lock().unwrap().clear();
    b.flags.requests.lock().unwrap().clear();
    let status = wss_rpc(&mut rpc, 4, "identity.authStatus", params.clone()).await;
    assert_eq!(status["result"]["isConfigured"], true, "{status}");
    let user = wss_rpc(&mut rpc, 5, "identity.getUser", params).await;
    assert_eq!(user["result"]["user"]["id"], "4242", "{user}");
    let current = wss_rpc(&mut rpc, 6, "principal.me", json!({})).await;
    assert_eq!(current["result"]["identity"], identity, "{current}");
    assert_eq!(
        current["result"]["id"],
        selected["result"]["principal"]["id"]
    );
    let mut reconnected = connect_ws(h.port, h.cfg.clone()).await;
    let admitted = wss_rpc(&mut reconnected, 1, "principal.me", json!({})).await;
    assert_eq!(
        admitted["result"]["id"],
        selected["result"]["principal"]["id"]
    );
    let select = wss_rpc(&mut rpc, 7, "identity.select", identity.clone()).await;
    assert_eq!(
        select["result"]["principal"]["identity"], identity,
        "{select}"
    );
    let proof = wss_rpc(
        &mut rpc,
        8,
        "sourceControl.identityProof.create",
        proof_params,
    )
    .await;
    assert!(proof["result"]["proofId"].is_string(), "{proof}");
    let delete = wss_rpc(
        &mut rpc,
        9,
        "sourceControl.identityProof.delete",
        json!({
            "provider":"gitlab","host":host,"purpose":"collaboration","proofId":proof_id
        }),
    )
    .await;
    assert_eq!(delete["result"]["ok"], true, "{delete}");
    assert_eq!(
        read_secrets(&h.secrets_file),
        repository,
        "repository storage changed"
    );
    let requests_b = b.flags.requests.lock().unwrap().clone();
    assert!(
        requests_b.is_empty(),
        "credential reached the new repository endpoint: {requests_b:?}"
    );
    let requests = a.flags.requests.lock().unwrap();
    let refreshes: Vec<_> = requests
        .iter()
        .filter(|r| r["grantType"] == "refresh_token")
        .collect();
    if is_pat {
        assert!(refreshes.is_empty());
    } else {
        assert_eq!(refreshes.len(), 1);
        assert_eq!(
            refreshes[0]["clientId"], "application-a",
            "original OAuth application must survive: {refreshes:?}"
        );
    }
    assert!(requests
        .iter()
        .any(|r| r["method"] == "DELETE" && r["route"] == format!("/api/v4/snippets/{proof_id}")));
}

#[tokio::test]
async fn collaboration_gitlab_binding_survives_repository_rebind() {
    binding_survives(Change::Host).await;
}
#[tokio::test]
async fn collaboration_gitlab_binding_survives_client_id_change() {
    binding_survives(Change::Client).await;
}
#[tokio::test]
async fn collaboration_gitlab_binding_survives_api_override_change() {
    binding_survives(Change::Endpoint).await;
}
#[tokio::test]
async fn collaboration_gitlab_binding_survives_restart_before_refresh() {
    binding_survives(Change::Restart).await;
}
#[tokio::test]
async fn collaboration_gitlab_binding_preserves_pat_endpoint() {
    binding_survives(Change::Pat).await;
}

async fn invalid_binding(binding: Option<Value>, device: bool) {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    if device {
        let mut sub = super::collaboration_identity::collaboration_subscriber(&h).await;
        let result = wss_rpc(
            &mut rpc,
            1,
            "identity.connect",
            json!({"provider":"gitlab"}),
        )
        .await;
        assert!(result["result"]["flowId"].is_string(), "{result}");
        mock.flags.authorize.store(true, Ordering::SeqCst);
        super::collaboration_github::identity_event(&mut sub, "authorized").await;
    } else {
        super::collaboration_identity::connected(&h, &mut rpc).await;
    }
    let credential = credential_store(&h);
    let mut account: Value =
        serde_json::from_str(&credential.load("identity.account").unwrap().unwrap()).unwrap();
    if let Some(binding) = binding {
        account["gitlab_binding"] = binding;
    } else {
        account.as_object_mut().unwrap().remove("gitlab_binding");
    }
    credential
        .store("identity.account", &account.to_string())
        .unwrap();
    let before = std::fs::read(credential.path()).unwrap();
    mock.flags.requests.lock().unwrap().clear();
    for method in [
        "identity.authStatus",
        "identity.getUser",
        "identity.select",
        "sourceControl.identityProof.create",
        "sourceControl.identityProof.delete",
    ] {
        let result = wss_rpc(
            &mut rpc,
            1,
            method,
            json!({
                "provider":"gitlab","purpose":"collaboration","externalUserId":"4242",
                "expectedIdentity":{"provider":"gitlab","host":HOST,"externalUserId":"4242"},
                "nonce":"missing-binding","hostLabel":"fixture","proofId":"1"
            }),
        )
        .await;
        assert_eq!(
            result["error"]["data"]["code"], "identity-mismatch",
            "{method}: {result}"
        );
    }
    assert!(mock.flags.requests.lock().unwrap().is_empty());
    assert_eq!(
        std::fs::read(credential.path()).unwrap(),
        before,
        "do not erase incomplete metadata or credentials"
    );
    let revoked = wss_rpc(&mut rpc, 2, "identity.revoke", json!({"provider":"gitlab"})).await;
    assert_eq!(
        revoked["result"]["ok"], true,
        "explicit recovery stays available: {revoked}"
    );
}

#[tokio::test]
async fn collaboration_gitlab_binding_missing_fails_without_sending_or_deleting() {
    invalid_binding(None, false).await;
}
#[tokio::test]
async fn collaboration_gitlab_binding_invalid_fails_without_sending_or_deleting() {
    invalid_binding(
        Some(json!({"base_url":"http://unsafe.invalid","client_id":null})),
        false,
    )
    .await;
}

#[tokio::test]
async fn collaboration_gitlab_binding_missing_device_application_fails_without_network() {
    invalid_binding(
        Some(json!({"base_url":"https://gitlab.com","client_id":null})),
        true,
    )
    .await;
}

#[tokio::test]
async fn collaboration_gitlab_binding_malformed_fails_without_network() {
    invalid_binding(Some(json!({"base_url":42,"client_id":null})), false).await;
}
