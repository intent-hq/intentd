use super::*;
use std::io::Write as _;

#[derive(Default)]
struct GithubState {
    authorize: AtomicBool,
    account: AtomicUsize,
    hold_grant: AtomicBool,
    grant_held: Notify,
    release_grant: Notify,
    requests: Mutex<Vec<(String, String, String)>>,
    gist: Mutex<Option<Value>>,
    hold_proof: AtomicBool,
    proof_held: Notify,
    release_proof: Notify,
    hold_user: AtomicBool,
    user_held: Notify,
    release_user: Notify,
    scopes: Mutex<Option<String>>,
}

async fn mock_github() -> (String, Arc<GithubState>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let origin = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let state = Arc::new(GithubState::default());
    state.account.store(100, Ordering::SeqCst);
    *state.scopes.lock().unwrap() = Some("repo,read:org,workflow,gist".into());
    let shared = state.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let state = shared.clone();
            tokio::spawn(async move {
                github_request(stream, state).await.unwrap();
            });
        }
    });
    (origin, state)
}

async fn github_request(mut stream: TcpStream, state: Arc<GithubState>) -> std::io::Result<()> {
    let mut bytes = Vec::new();
    let mut buf = [0; 4096];
    let start = loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&buf[..n]);
        if let Some(i) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&bytes[..start]).into_owned();
    let header = |key: &str| {
        head.lines()
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.trim().to_owned())
    };
    let len = header("content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    while bytes.len() < start + len {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&buf[..n]);
    }
    let body = String::from_utf8_lossy(&bytes[start..]).into_owned();
    let mut line = head.lines().next().unwrap().split_whitespace();
    let method = line.next().unwrap();
    let route = line.next().unwrap();
    let bearer = header("authorization").unwrap_or_default();
    state
        .requests
        .lock()
        .unwrap()
        .push((route.into(), body.clone(), bearer.clone()));
    let granted = state.scopes.lock().unwrap().clone();
    let (code, payload) = match (method, route) {
        ("POST", "/login/device/code") => (
            200,
            json!({"device_code":"opaque", "user_code":"JOIN-1234","verification_uri":"https://github.com/login/device","expires_in":900,"interval":1}),
        ),
        ("POST", "/login/oauth/access_token") if state.authorize.load(Ordering::SeqCst) => {
            let id = state.account.load(Ordering::SeqCst);
            if state.hold_grant.swap(false, Ordering::SeqCst) {
                state.grant_held.notify_one();
                state.release_grant.notified().await;
            }
            (
                200,
                json!({"access_token":format!("collaboration-{id}"),"token_type":"bearer","scope":granted}),
            )
        }
        ("POST", "/login/oauth/access_token") => (200, json!({"error":"authorization_pending"})),
        ("GET", "/user") => {
            if state.hold_user.swap(false, Ordering::SeqCst) {
                state.user_held.notify_one();
                state.release_user.notified().await;
            }
            let id = bearer
                .rsplit('-')
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(900);
            (
                200,
                json!({"id":id,"login":format!("person{id}"),"name":"Person","avatar_url":"https://example.com/avatar"}),
            )
        }
        ("POST", "/gists") => {
            let posted: Value = serde_json::from_str(&body).unwrap();
            *state.gist.lock().unwrap() = Some(posted);
            if state.hold_proof.swap(false, Ordering::SeqCst) {
                state.proof_held.notify_one();
                state.release_proof.notified().await;
            }
            (201, json!({"id":"abcdef"}))
        }
        ("GET", "/gists/abcdef") => match state.gist.lock().unwrap().clone() {
            Some(v) => (200, v),
            None => (404, json!({"message":"Not Found"})),
        },
        ("DELETE", "/gists/abcdef") => {
            *state.gist.lock().unwrap() = None;
            (204, Value::Null)
        }
        _ => (404, json!({"message":"Not Found"})),
    };
    let body = if code == 204 {
        String::new()
    } else {
        payload.to_string()
    };
    let scope_header = granted
        .map(|s| format!("x-oauth-scopes: {s}\r\n"))
        .unwrap_or_default();
    let response=format!("HTTP/1.1 {code} OK\r\ncontent-type: application/json\r\n{scope_header}content-length: {}\r\nconnection: close\r\n\r\n{body}",body.len());
    stream.write_all(response.as_bytes()).await
}

async fn subscribe_identity(h: &Harness) -> Ws {
    let mut ws = connect_ws(h.port, h.cfg.clone()).await;
    let v = wss_rpc(
        &mut ws,
        1,
        "events.subscribe",
        json!({"eventTypes":["identity:auth-changed","principal:identity-changed"]}),
    )
    .await;
    assert!(v.get("error").is_none(), "{v}");
    ws
}

pub(super) async fn identity_event(ws: &mut Ws, wanted: &str) -> Value {
    timeout(Duration::from_secs(20), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(&t).unwrap();
                    if v["method"] == "events.event"
                        && v["params"]["event"]["type"] == "identity:auth-changed"
                        && v["params"]["event"]["data"]["status"] == wanted
                    {
                        assert_eq!(v["jsonrpc"], "2.0");
                        return v["params"]["event"]["data"].clone();
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    ws.send(Message::Pong(p)).await.unwrap();
                }
                _ => {}
            }
        }
    })
    .await
    .expect("identity event")
}

async fn github_boot() -> (Harness, Arc<GithubState>) {
    let (origin, state) = mock_github().await;
    let gitlab = spawn_mock_gitlab().await;
    let h = boot_with_env(
        &gitlab,
        &[
            ("INTENTD_GITHUB_LOGIN_BASE_URI", &origin),
            ("INTENTD_GITHUB_API_BASE_URI", &origin),
        ],
    )
    .await;
    (h, state)
}

async fn authorize(h: &Harness, state: &GithubState, rpc: &mut Ws, id: i64) -> Value {
    let mut sub = subscribe_identity(h).await;
    let v = wss_rpc(rpc, id, "identity.connect", json!({"provider":"github"})).await;
    assert_eq!(v["result"]["purpose"], "collaboration", "{v}");
    state.authorize.store(true, Ordering::SeqCst);
    let e = identity_event(&mut sub, "authorized").await;
    assert_eq!(
        e,
        json!({"provider":"github","host":"github.com","purpose":"collaboration","status":"authorized","flowId":v["result"]["flowId"]})
    );
    v
}

#[tokio::test]
async fn collaboration_github_requests_only_gist_reports_broader_grant_and_isolates_children() {
    let (h, state) = github_boot().await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let hello = wss_rpc(
        &mut rpc,
        0,
        "client.hello",
        json!({"clientId":"collaboration-test"}),
    )
    .await;
    assert_eq!(
        hello["result"]["server"]["capabilities"]["collaborationIdentity"], 1,
        "{hello}"
    );
    assert!(hello["result"]["server"]["capabilities"]
        .get("hostMembership")
        .is_none());
    authorize(&h, &state, &mut rpc, 2).await;
    {
        let req = state.requests.lock().unwrap();
        let body = &req.iter().find(|r| r.0 == "/login/device/code").unwrap().1;
        assert!(body.contains("gist"), "{body}");
        assert!(
            !body.contains("repo") && !body.contains("workflow"),
            "{body}"
        );
    }
    let v = wss_rpc(
        &mut rpc,
        3,
        "identity.authStatus",
        json!({"provider":"github"}),
    )
    .await;
    assert_eq!(v["result"]["requestedScopes"], json!(["gist"]), "{v}");
    assert_eq!(
        v["result"]["grantedScopes"],
        json!(["repo", "read:org", "workflow", "gist"])
    );
    assert!(read_secrets(&h.secrets_file)["sourceControl.github.token"].is_null());
    let v = wss_rpc(
        &mut rpc,
        4,
        "sourceControl.authStatus",
        json!({"provider":"github"}),
    )
    .await;
    assert_eq!(v["result"]["isConfigured"], false, "{v}");
    let v = wss_rpc(
        &mut rpc,
        5,
        "sourceControl.identityProof.create",
        json!({"provider":"github","nonce":"proof","hostLabel":"h"}),
    )
    .await;
    assert_eq!(v["error"]["data"]["code"], "github-not-connected", "{v}");
    // Exercise the real child-facing executable, not merely the secret file.
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["git-credential", "get"])
        .env("INTENTD_DATA_DIR", h.secrets_file.parent().unwrap())
        .env_remove("GITHUB_TOKEN")
        .env_remove("GH_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"protocol=https\nhost=github.com\n\n")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    let v = wss_rpc(
        &mut rpc,
        6,
        "identity.select",
        json!({"provider":"github","externalUserId":"999"}),
    )
    .await;
    assert_eq!(v["error"]["data"]["code"], "identity-mismatch", "{v}");
    let v = wss_rpc(
        &mut rpc,
        7,
        "identity.select",
        json!({"provider":"github","externalUserId":"100"}),
    )
    .await;
    assert_eq!(
        v["result"]["principal"]["identity"]["externalUserId"], "100",
        "{v}"
    );
    let v=wss_rpc(&mut rpc,8,"sourceControl.identityProof.create",json!({"provider":"github","purpose":"collaboration","expectedIdentity":{"provider":"github","host":"github.com","externalUserId":"100"},"nonce":"proof","hostLabel":"h"})).await;
    assert_eq!(v["result"]["gistId"], "abcdef", "{v}");
    assert_eq!(v["result"]["externalUserId"], Value::Null);
    let v = wss_rpc(
        &mut rpc,
        9,
        "sourceControl.identityProof.delete",
        json!({"provider":"github","purpose":"collaboration","proofId":"abcdef"}),
    )
    .await;
    assert_eq!(v["result"], json!({"ok":true}), "{v}");
}

#[tokio::test]
async fn collaboration_github_account_swap_preserves_selection_and_refuses_old_cleanup() {
    let (h, state) = github_boot().await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    authorize(&h, &state, &mut rpc, 1).await;
    let v = wss_rpc(
        &mut rpc,
        2,
        "identity.select",
        json!({"provider":"github","externalUserId":"100"}),
    )
    .await;
    assert_eq!(
        v["result"]["principal"]["identity"]["externalUserId"], "100",
        "{v}"
    );
    let v=wss_rpc(&mut rpc,3,"sourceControl.identityProof.create",json!({"provider":"github","purpose":"collaboration","expectedIdentity":{"provider":"github","host":"github.com","externalUserId":"100"},"nonce":"proof","hostLabel":"h"})).await;
    assert_eq!(v["result"]["gistId"], "abcdef", "{v}");
    state.account.store(200, Ordering::SeqCst);
    authorize(&h, &state, &mut rpc, 4).await;
    let v = wss_rpc(&mut rpc, 5, "principal.me", json!({})).await;
    assert_eq!(v["result"]["identity"]["externalUserId"], "100", "{v}");
    let v = wss_rpc(
        &mut rpc,
        6,
        "sourceControl.identityProof.delete",
        json!({"provider":"github","purpose":"collaboration","proofId":"abcdef"}),
    )
    .await;
    assert_eq!(v["error"]["data"]["code"], "identity-mismatch", "{v}");
    assert!(state.gist.lock().unwrap().is_some());
    let v = wss_rpc(
        &mut rpc,
        7,
        "identity.select",
        json!({"provider":"github","externalUserId":"200"}),
    )
    .await;
    assert_eq!(
        v["result"]["principal"]["identity"]["externalUserId"], "200",
        "{v}"
    );
}

#[tokio::test]
async fn collaboration_github_cancelled_grant_cannot_overwrite_newer_authorization() {
    let (h, state) = github_boot().await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    state.authorize.store(true, Ordering::SeqCst);
    state.hold_grant.store(true, Ordering::SeqCst);
    let v = wss_rpc(
        &mut rpc,
        1,
        "identity.connect",
        json!({"provider":"github"}),
    )
    .await;
    await_latch(&state.grant_held, "held GitHub grant").await;
    let flow = v["result"]["flowId"].clone();
    let v = wss_rpc(
        &mut rpc,
        2,
        "identity.cancelAuth",
        json!({"provider":"github","flowId":flow}),
    )
    .await;
    assert_eq!(v["result"]["cancelled"], true, "{v}");
    state.account.store(200, Ordering::SeqCst);
    authorize(&h, &state, &mut rpc, 3).await;
    state.hold_user.store(true, Ordering::SeqCst);
    state.release_grant.notify_one();
    // The stale completion now holds the persistence gate during its account
    // check. A following read can finish only after that completion is settled.
    await_latch(&state.user_held, "stale grant account check").await;
    state.release_user.notify_one();
    let v = wss_rpc(
        &mut rpc,
        4,
        "identity.getUser",
        json!({"provider":"github"}),
    )
    .await;
    assert_eq!(v["result"]["user"]["id"], "200", "{v}");
    let v = wss_rpc(
        &mut rpc,
        5,
        "identity.cancelAuth",
        json!({"provider":"github","flowId":flow}),
    )
    .await;
    assert_eq!(v["result"]["cancelled"], false, "{v}");
}

async fn select_gitlab(rpc: &mut Ws) -> Value {
    let v = wss_rpc(
        rpc,
        50,
        "identity.connect",
        json!({"provider":"gitlab","method":"pat","token":PAT_TOKEN}),
    )
    .await;
    assert_eq!(v["result"]["ok"], true, "{v}");
    let v = wss_rpc(
        rpc,
        51,
        "identity.select",
        json!({"provider":"gitlab","externalUserId":"4242"}),
    )
    .await;
    assert_eq!(
        v["result"]["principal"]["identity"]["provider"], "gitlab",
        "{v}"
    );
    v
}

#[tokio::test]
async fn collaboration_proof_superseded_by_selection_or_new_flow_is_cleaned_up() {
    for new_selection in [true, false] {
        let (h, state) = github_boot().await;
        let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
        authorize(&h, &state, &mut rpc, 1).await;
        state.hold_proof.store(true, Ordering::SeqCst);
        let mut proof_ws = connect_ws(h.port, h.cfg.clone()).await;
        let proof = tokio::spawn(async move {
            wss_rpc(&mut proof_ws, 2, "sourceControl.identityProof.create", json!({"provider":"github","purpose":"collaboration","expectedIdentity":{"provider":"github","host":"github.com","externalUserId":"100"},"nonce":"stale","hostLabel":"h"})).await
        });
        await_latch(&state.proof_held, "proof upload held").await;
        if new_selection {
            select_gitlab(&mut rpc).await;
        } else {
            // Starting and cancelling a newer flow invalidates the credential
            // epoch immediately, even while publication owns the IO gate.
            state.authorize.store(false, Ordering::SeqCst);
            let v = wss_rpc(
                &mut rpc,
                3,
                "identity.connect",
                json!({"provider":"github"}),
            )
            .await;
            assert!(v["result"]["flowId"].is_string(), "{v}");
            let v = wss_rpc(
                &mut rpc,
                4,
                "identity.cancelAuth",
                json!({"provider":"github","flowId":v["result"]["flowId"]}),
            )
            .await;
            assert_eq!(v["result"]["cancelled"], true, "{v}");
        }
        state.release_proof.notify_one();
        let v = proof.await.unwrap();
        assert_eq!(v["error"]["data"]["code"], "identity-mismatch", "{v}");
        assert!(
            state.gist.lock().unwrap().is_none(),
            "stale public proof was removed"
        );
    }
}

#[tokio::test]
async fn collaboration_select_cannot_overtake_newer_explicit_choice_and_emits_identity_event() {
    let (h, state) = github_boot().await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    authorize(&h, &state, &mut rpc, 1).await;
    let mut sub = subscribe_identity(&h).await;
    state.hold_user.store(true, Ordering::SeqCst);
    let mut old_ws = connect_ws(h.port, h.cfg.clone()).await;
    let old = tokio::spawn(async move {
        wss_rpc(
            &mut old_ws,
            2,
            "identity.select",
            json!({"provider":"github","externalUserId":"100"}),
        )
        .await
    });
    await_latch(&state.user_held, "older identity choice held").await;
    let selected = select_gitlab(&mut rpc).await;
    state.release_user.notify_one();
    let v = old.await.unwrap();
    assert_eq!(v["error"]["data"]["code"], "identity-mismatch", "{v}");
    let event = timeout(Duration::from_secs(10), async {
        loop {
            if let Some(Ok(Message::Text(t))) = sub.next().await {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["method"] == "events.event"
                    && v["params"]["event"]["type"] == "principal:identity-changed"
                {
                    assert_eq!(v["jsonrpc"], "2.0");
                    break v["params"]["event"]["data"].clone();
                }
            }
        }
    })
    .await
    .expect("selection event");
    assert_eq!(event["principalId"], selected["result"]["principal"]["id"]);
    assert_eq!(
        event["identity"],
        selected["result"]["principal"]["identity"]
    );
    let v = wss_rpc(&mut rpc, 3, "principal.me", json!({})).await;
    assert_eq!(v["result"]["identity"], event["identity"], "{v}");
}

#[tokio::test]
async fn collaboration_existing_repository_credential_proves_without_copy_or_overwrite() {
    let (h, state) = github_boot().await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    // Use the supported owner settings path for the existing repository slot.
    let v = wss_rpc(
        &mut rpc,
        1,
        "settings.update",
        json!({"changes":[{"path":"sourceControl.github.token","value":"repository-900"}]}),
    )
    .await;
    assert!(v.get("error").is_none(), "{v}");
    let before = read_secrets(&h.secrets_file);
    assert_eq!(before["sourceControl.github.token"], "repository-900");
    let v = wss_rpc(
        &mut rpc,
        2,
        "sourceControl.identityProof.create",
        json!({"provider":"github","nonce":"repo-proof","hostLabel":"h"}),
    )
    .await;
    assert_eq!(v["result"]["login"], "person900", "{v}");
    let v = wss_rpc(
        &mut rpc,
        3,
        "identity.getUser",
        json!({"provider":"github"}),
    )
    .await;
    assert_eq!(
        v["result"]["user"],
        Value::Null,
        "repository proof did not copy credentials: {v}"
    );
    authorize(&h, &state, &mut rpc, 4).await;
    assert_eq!(read_secrets(&h.secrets_file), before);
    let v = wss_rpc(&mut rpc, 5, "identity.revoke", json!({"provider":"github"})).await;
    assert_eq!(v["result"], json!({"ok":true}), "{v}");
    assert_eq!(read_secrets(&h.secrets_file), before);
    let v = wss_rpc(&mut rpc, 6, "github.getUser", json!({})).await;
    assert_eq!(v["result"]["user"]["login"], "person900", "{v}");
}

#[tokio::test]
async fn collaboration_github_unknown_grant_stays_unknown_and_missing_gist_is_refused() {
    let (h, state) = github_boot().await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    *state.scopes.lock().unwrap() = None;
    authorize(&h, &state, &mut rpc, 1).await;
    let v = wss_rpc(
        &mut rpc,
        2,
        "identity.authStatus",
        json!({"provider":"github"}),
    )
    .await;
    assert_eq!(v["result"]["grantedScopes"], Value::Null, "{v}");
    assert_eq!(v["result"]["requestedScopes"], json!(["gist"]));
    let v = wss_rpc(&mut rpc, 3, "identity.revoke", json!({"provider":"github"})).await;
    assert_eq!(v["result"]["ok"], true, "{v}");
    *state.scopes.lock().unwrap() = Some("repo workflow".into());
    let mut sub = subscribe_identity(&h).await;
    let v = wss_rpc(
        &mut rpc,
        4,
        "identity.connect",
        json!({"provider":"github"}),
    )
    .await;
    let event = identity_event(&mut sub, "error").await;
    assert_eq!(event["flowId"], v["result"]["flowId"]);
    let v = wss_rpc(
        &mut rpc,
        5,
        "identity.authStatus",
        json!({"provider":"github"}),
    )
    .await;
    assert_eq!(
        v["result"]["isConfigured"], false,
        "missing gist cannot complete sign-in: {v}"
    );
}
