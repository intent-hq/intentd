//! Real-process transport admission: member creation uses the primary owner.
use super::*;
use intent_core::{now_iso, Principal, PrincipalId, WorkspaceRole};
use intent_store::Store;
use std::fmt::Write as _;

async fn seed_person(store: &Store, token: &str, member: bool) -> PrincipalId {
    let principal = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: Some(if member { "member" } else { "guest" }.into()),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    store.upsert_principal(&principal).await.unwrap();
    let mut hash = String::with_capacity(64);
    for byte in Sha256::digest(token.as_bytes()) {
        write!(hash, "{byte:02x}").unwrap();
    }
    store
        .insert_principal_credential(&principal.id, &hash)
        .await
        .unwrap();
    if member {
        sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
            .bind(principal.id.as_str())
            .bind(now_iso())
            .execute(store.write_pool())
            .await
            .unwrap();
    }
    principal.id
}

async fn create_as_member(existing: bool, retained: bool) {
    let dir = temp_data_dir();
    let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
    let member_token = "b5".repeat(32);
    let guest_token = "c5".repeat(32);
    let member_id = seed_person(&store, &member_token, true).await;
    let guest_id = seed_person(&store, &guest_token, false).await;
    std::fs::write(
        dir.path().join("config.toml"),
        "[sourceControl.github]\ntokenSource = 'explicit'\napiBaseUrl = 'http://127.0.0.1:9'\n",
    )
    .unwrap();
    let _daemon = Daemon {
        child: spawn_serve(
            dir.path(),
            "both",
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("GH_TOKEN", ""),
                ("GITHUB_TOKEN", ""),
                ("GITLAB_TOKEN", ""),
                ("INTENTD_GITHUB_API_BASE_URI", "http://127.0.0.1:9"),
            ],
        ),
    };
    let socket = dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut owner = connect_ws(port, cfg.clone()).await;
    let owner_id = wss_rpc(&mut owner, 1, "principal.me", json!({})).await["id"].clone();
    let mut member = common::wss_connect_with_retry(
        port,
        cfg.clone(),
        &format!("wss://localhost:{port}/ws?token={member_token}"),
    )
    .await;
    let mut guest = common::wss_connect_with_retry(
        port,
        cfg.clone(),
        &format!("wss://localhost:{port}/ws?token={guest_token}"),
    )
    .await;
    assert!(
        wss_rpc(&mut member, 2, "workspace.list", json!({})).await["workspaces"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    if existing {
        let row = wss_rpc(
            &mut owner,
            2,
            "workspace.create",
            json!({"title":"Owner workspace"}),
        )
        .await;
        let id = intent_core::WorkspaceId::from(row["workspace"]["id"].as_str().unwrap());
        assert_eq!(row["workspace"]["ownerPrincipalId"], owner_id);
        store
            .add_workspace_member(&id, &guest_id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
        if retained {
            store
                .add_workspace_member(&id, &member_id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
        let read = wss_rpc(&mut member, 3, "workspace.get", json!({"workspaceId":id})).await;
        assert_eq!(read["workspace"]["canManage"], true);
        assert_eq!(read["workspace"]["myRole"], "collaborator");
    }
    let denied = wss_rpc_envelope(
        &mut guest,
        4,
        "workspace.create",
        json!({"title":"Forbidden guest"}),
    )
    .await;
    assert_eq!(denied["error"]["code"], -32003, "{denied}");
    let me = wss_rpc(&mut member, 4, "principal.me", json!({})).await;
    assert_eq!(me["hostRole"], "member");
    assert_eq!(me["isAdministrator"], false);
    let mut live = common::wss_connect_with_retry(
        port,
        cfg.clone(),
        &format!("wss://localhost:{port}/ws?token={member_token}"),
    )
    .await;
    wss_rpc(&mut live, 1, "workspace.subscribe", json!({})).await;
    let snapshot = wss_push(&mut live, 10).await;
    assert_eq!(snapshot["params"]["seq"], 0);
    assert_eq!(
        snapshot["params"]["snapshot"].as_array().unwrap().len(),
        usize::from(existing)
    );
    let created = wss_rpc_envelope(
        &mut member,
        5,
        "workspace.create",
        json!({"title":"Member workspace"}),
    )
    .await;
    assert_eq!(created["jsonrpc"], "2.0");
    assert_eq!(created["id"], 5);
    assert!(
        created.get("error").is_none(),
        "member workspace.create: {created}"
    );
    let row = &created["result"]["workspace"];
    assert_eq!(row["ownerPrincipalId"], owner_id);
    assert_eq!(row["canManage"], true);
    assert_eq!(row["myRole"], "collaborator");
    let delta = wss_push(&mut live, 10).await;
    assert_eq!(delta["params"]["seq"], 1);
    assert_eq!(delta["params"]["delta"]["added"][0]["id"], row["id"]);
    assert_eq!(delta["params"]["delta"]["added"][0]["canManage"], true);
    if !existing {
        let mut events = common::wss_connect_with_retry(
            port,
            cfg.clone(),
            &format!("wss://localhost:{port}/ws?token={member_token}"),
        )
        .await;
        wss_rpc(
            &mut events,
            1,
            "events.subscribe",
            json!({"workspaceId":row["id"],"eventTypes":["terminal:*","script:*"]}),
        )
        .await;
        let terminal = wss_rpc(&mut member, 11, "terminal.create", json!({"workspaceId":row["id"],"cols":80,"rows":24,"command":"env","env":{"MEMBER_TRANSPORT_MARKER":"terminal-live"}})).await;
        collect_tool_output(
            &mut events,
            "terminal",
            terminal["terminalId"].as_str().unwrap(),
            "MEMBER_TRANSPORT_MARKER=terminal-live",
        )
        .await;
        let script = wss_rpc(&mut member, 12, "script.create", json!({"workspaceId":row["id"],"name":"Member live output","mode":"command","command":"printf member-script-live"})).await;
        let script_id = script["id"].as_str().unwrap();
        wss_rpc(
            &mut member,
            13,
            "script.start",
            json!({"workspaceId":row["id"],"scriptId":script_id}),
        )
        .await;
        collect_tool_output(&mut events, "script", script_id, "member-script-live").await;
        for method in ["terminal.create", "script.create"] {
            assert_eq!(wss_rpc_envelope(&mut guest, 15, method, json!({"workspaceId":row["id"],"name":"Guest tool","command":"env","mode":"command"})).await["error"]["code"], -32003);
        }
    }
    drop(member);
    let mut member = common::wss_connect_with_retry(
        port,
        cfg,
        &format!("wss://localhost:{port}/ws?token={member_token}"),
    )
    .await;
    assert_eq!(
        wss_rpc(&mut member, 6, "principal.me", json!({})).await["hostRole"],
        "member"
    );
    assert_eq!(
        wss_rpc(
            &mut member,
            7,
            "workspace.get",
            json!({"workspaceId":row["id"]})
        )
        .await["workspace"]["canManage"],
        true
    );
    assert_eq!(
        wss_rpc_envelope(&mut member, 8, "settings.list", json!({})).await["error"]["code"],
        -32003
    );
    let hidden = wss_rpc_envelope(
        &mut guest,
        9,
        "workspace.get",
        json!({"workspaceId":row["id"]}),
    )
    .await;
    assert_eq!(hidden["error"]["data"]["code"], "not-found");
    assert!(wss_rpc(
        &mut owner,
        10,
        "workspace.create",
        json!({"title":"Owner control"})
    )
    .await["workspace"]["canManage"]
        .as_bool()
        .unwrap());
}

async fn collect_tool_output(events: &mut common::TlsWs, kind: &str, id: &str, marker: &str) {
    use base64::Engine as _;
    let mut output = Vec::new();
    let mut exited = false;
    tokio::time::timeout(Duration::from_secs(30), async {
        while !exited || !String::from_utf8_lossy(&output).contains(marker) {
            match events.next().await {
                Some(Ok(Message::Text(text))) => {
                    let frame: Value = serde_json::from_str(&text).unwrap();
                    let event = &frame["params"]["event"];
                    if event["data"][format!("{kind}Id")] != id {
                        continue;
                    }
                    if let Some(chunk) = event["data"]["chunk"].as_str() {
                        output.extend(
                            base64::engine::general_purpose::STANDARD
                                .decode(chunk)
                                .unwrap(),
                        );
                    }
                    exited |= event["type"] == "terminal:exit"
                        || (event["type"] == "script:state" && event["data"]["status"] == "exited");
                }
                Some(Ok(Message::Ping(bytes))) => events.send(Message::Pong(bytes)).await.unwrap(),
                other => panic!("live {kind} output: {other:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "missing {kind} output/exit; exited={exited}, output={:?}",
            String::from_utf8_lossy(&output)
        )
    });
}

#[tokio::test]
async fn member_transport_create_empty_host() {
    create_as_member(false, false).await;
}

#[tokio::test]
async fn member_transport_create_inherited_no_row() {
    create_as_member(true, false).await;
}

#[tokio::test]
async fn member_transport_create_retained_row() {
    create_as_member(true, true).await;
}
