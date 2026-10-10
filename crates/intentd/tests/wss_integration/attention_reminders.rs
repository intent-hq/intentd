//! Real authenticated RPC evidence for person-scoped, exact-reason acknowledgements.
use super::*;
use serde_json::json;

type Ws = common::TlsWs;

async fn rpc(ws: &mut Ws, id: u64, method: &str, params: Value) -> Value {
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    tokio::time::timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["id"] == id {
                        assert_eq!(v["jsonrpc"], "2.0");
                        assert!(v.get("error").is_none(), "{method}: {v}");
                        return v;
                    }
                }
                Some(Ok(Message::Ping(p))) => ws.send(Message::Pong(p)).await.unwrap(),
                Some(Ok(_)) => {}
                other => panic!("RPC connection closed: {other:?}"),
            }
        }
    })
    .await
    .expect("RPC deadline")
}

async fn get(ws: &mut Ws, id: u64, workspace: &WorkspaceId) -> Value {
    rpc(ws, id, "workspace.get", json!({"workspaceId":workspace})).await["result"]["workspace"]
        .clone()
}

async fn dismiss(ws: &mut Ws, id: u64, workspace: &WorkspaceId, reasons: Value) -> Value {
    rpc(
        ws,
        id,
        "workspace.dismissAttention",
        json!({"workspaceId":workspace,"reasons":reasons}),
    )
    .await["result"]["workspace"]
        .clone()
}

async fn seed(srv: &Server, workspace: &WorkspaceId, agent: &str, metadata: Value) {
    sqlx::query("INSERT INTO agent_session (id,workspace_id,name,status,created_at,updated_at,metadata) VALUES (?,?,'Reminder agent','idle',?,?,?)")
        .bind(agent).bind(workspace.as_str()).bind(now_iso()).bind(now_iso()).bind(metadata.to_string())
        .execute(srv.store.write_pool()).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn attention_reminders_exact_reasons_races_and_restart_over_wss() {
    let srv = start(WsOptions::default()).await;
    let workspace = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&workspace))
        .await
        .unwrap();
    seed(
        &srv,
        &workspace,
        "reminder-a",
        json!({"pendingQuestionsMessageId":"question-a"}),
    )
    .await;
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    let mut evidence = Vec::new();
    let before = get(&mut ws, 1, &workspace).await;
    assert_eq!(before["displayStatus"], "needs_attention");
    assert_eq!(
        before["attentionReminder"]["reasons"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let old = before["attentionReminder"]["reasons"].clone();
    let acknowledged = dismiss(&mut ws, 2, &workspace, old.clone()).await;
    assert_eq!(
        acknowledged["displayStatus"], "needs_attention",
        "raw state preserved"
    );
    assert_eq!(
        acknowledged["attentionReminder"]["displayStatus"],
        "waiting"
    );
    assert_eq!(acknowledged["attentionReminder"]["dismissed"], true);
    assert_eq!(acknowledged["updatedAt"], before["updatedAt"]);
    assert_eq!(acknowledged["lastActivity"], before["lastActivity"]);
    evidence.push(acknowledged);
    assert_eq!(
        get(&mut ws, 3, &workspace).await["attentionReminder"]["dismissed"],
        true
    );
    let snapshot = chat_subscribe_snapshot(
        srv.port,
        srv.cfg.clone(),
        &json!({"jsonrpc":"2.0","id":31,"method":"workspace.subscribe","params":{}}).to_string(),
        31,
    )
    .await;
    let subscribed = snapshot
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == workspace.as_str())
        .unwrap();
    assert_eq!(subscribed["attentionReminder"]["dismissed"], true);
    assert_eq!(subscribed["attentionReminder"]["displayStatus"], "waiting");
    evidence.push(subscribed.clone());
    let listed = rpc(&mut ws, 4, "workspace.list", json!({})).await;
    assert_eq!(
        listed["result"]["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["id"] == workspace.0)
            .unwrap()["attentionReminder"]["dismissed"],
        true
    );
    rpc(
        &mut ws,
        5,
        "workspace.update",
        json!({"workspaceId":workspace,"title":"Same reminder"}),
    )
    .await;
    assert_eq!(
        get(&mut ws, 6, &workspace).await["attentionReminder"]["dismissed"],
        true
    );

    // Another agent races a click on the already-open menu. Its reason survives.
    seed(
        &srv,
        &workspace,
        "reminder-b",
        json!({"pendingQuestionsMessageId":"question-b"}),
    )
    .await;
    let raced = dismiss(&mut ws, 7, &workspace, old).await;
    assert_eq!(raced["attentionReminder"]["dismissed"], false);
    assert_eq!(
        raced["attentionReminder"]["reasons"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let all = raced["attentionReminder"]["reasons"].clone();
    let acknowledged = dismiss(&mut ws, 8, &workspace, all.clone()).await;
    assert_eq!(acknowledged["attentionReminder"]["dismissed"], true);
    evidence.push(raced);
    // Resolving A must not resurface acknowledged B.
    sqlx::query("UPDATE agent_session SET metadata=json_set(metadata,'$.pendingQuestionsMessageId','') WHERE id='reminder-a'").execute(srv.store.write_pool()).await.unwrap();
    assert_eq!(
        get(&mut ws, 9, &workspace).await["attentionReminder"]["dismissed"],
        true
    );
    // A revised question is a new reason even on the same agent.
    sqlx::query("UPDATE agent_session SET metadata=json_set(metadata,'$.pendingQuestionsMessageId','question-b2') WHERE id='reminder-b'").execute(srv.store.write_pool()).await.unwrap();
    assert_eq!(
        dismiss(&mut ws, 10, &workspace, all).await["attentionReminder"]["dismissed"],
        false
    );
    let revised = get(&mut ws, 11, &workspace).await;
    dismiss(
        &mut ws,
        12,
        &workspace,
        revised["attentionReminder"]["reasons"].clone(),
    )
    .await;
    // Blocker/error still win over acknowledged human reminders.
    srv.store
        .set_attention_request(
            &workspace,
            &intent_core::AgentId::from("reminder-a"),
            "blocker",
            "Blocked",
            "same-time",
        )
        .await
        .unwrap();
    assert_eq!(
        get(&mut ws, 13, &workspace).await["attentionReminder"]["displayStatus"],
        "blocked"
    );
    sqlx::query("UPDATE agent_session SET status='error' WHERE id='reminder-a'")
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        get(&mut ws, 14, &workspace).await["attentionReminder"]["displayStatus"],
        "failed"
    );
    sqlx::query(
        "UPDATE agent_session SET status='idle',attention_request_kind=NULL WHERE id='reminder-a'",
    )
    .execute(srv.store.write_pool())
    .await
    .unwrap();
    // Reconstruct services and listener using the same durable database.
    drop(ws);
    srv.ws.stop().await;
    let reopened = Store::open(&srv.dir.path().join("intentd.db"))
        .await
        .unwrap();
    let bus = EventBus::new(reopened.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(Services::new(reopened).with_event_bus(bus.clone()));
    let tls = ensure_tls_certificate(srv.dir.path()).unwrap();
    let tokens = Arc::new(MemTokenStore::default());
    tokens.store_token(TOKEN).unwrap();
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..WsOptions::default()
    };
    let restarted = WsApiServer::new(
        api,
        bus,
        &tls,
        &Arc::new(AsyncTokenStore::new(tokens)),
        opts,
        None,
    )
    .unwrap();
    let port = restarted.start().await.unwrap();
    let mut ws = connect_ws(port, client_config(&tls.fingerprint256)).await;
    let after_restart = get(&mut ws, 15, &workspace).await;
    assert_eq!(after_restart["attentionReminder"]["dismissed"], true);
    evidence.push(after_restart);
    let artifact = srv.dir.path().join("attention-reminders-rpc.json");
    std::fs::write(&artifact, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    eprintln!(
        "attention reminder RPC evidence: {} (retain with INTENTD_TEST_KEEP_TMP=1)",
        artifact.display()
    );
    drop(ws);
    restarted.stop().await;
}

#[intent_test_macros::daemon_test]
async fn attention_reminders_principals_generations_and_legacy_over_wss() {
    let srv = start(WsOptions::default()).await;
    let workspace = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&workspace))
        .await
        .unwrap();
    seed(
        &srv,
        &workspace,
        "discussion-agent",
        json!({"pendingQuestionsMessageId":""}),
    )
    .await;
    srv.store
        .set_attention_request(
            &workspace,
            &intent_core::AgentId::from("discussion-agent"),
            "discussion",
            "Choose",
            "identical-time",
        )
        .await
        .unwrap();
    let mut owner = connect_ws(srv.port, srv.cfg.clone()).await;
    let token = "d3".repeat(32);
    let mut other = sharing::member(&srv, &token, "github", "github.com").await;
    let first = get(&mut owner, 1, &workspace).await;
    let old = first["attentionReminder"]["reasons"].clone();
    dismiss(&mut owner, 2, &workspace, old.clone()).await;
    let other_ws = other
        .call("workspace.get", json!({"workspaceId":workspace}))
        .await;
    assert_eq!(
        other_ws["result"]["workspace"]["attentionReminder"]["dismissed"],
        false
    );
    let mut same_person = connect_ws(srv.port, srv.cfg.clone()).await;
    assert_eq!(
        get(&mut same_person, 3, &workspace).await["attentionReminder"]["dismissed"],
        true
    );
    srv.store
        .set_attention_request(
            &workspace,
            &intent_core::AgentId::from("discussion-agent"),
            "discussion",
            "Choose",
            "identical-time",
        )
        .await
        .unwrap();
    let reraised = get(&mut owner, 4, &workspace).await;
    assert_ne!(reraised["attentionReminder"]["reasons"], old);
    assert_eq!(
        dismiss(&mut owner, 5, &workspace, old).await["attentionReminder"]["dismissed"],
        false
    );
    dismiss(
        &mut owner,
        6,
        &workspace,
        reraised["attentionReminder"]["reasons"].clone(),
    )
    .await;
    // Explicit repeated review_required updates each represent a fresh review request.
    rpc(
        &mut owner,
        7,
        "workspace.update",
        json!({"workspaceId":workspace,"attention":"review_required"}),
    )
    .await;
    let review = get(&mut owner, 8, &workspace).await;
    dismiss(
        &mut owner,
        9,
        &workspace,
        review["attentionReminder"]["reasons"].clone(),
    )
    .await;
    rpc(
        &mut owner,
        10,
        "workspace.update",
        json!({"workspaceId":workspace,"attention":"review_required"}),
    )
    .await;
    let fresh = get(&mut owner, 11, &workspace).await;
    assert_eq!(fresh["attentionReminder"]["dismissed"], false);
    assert_ne!(
        fresh["attentionReminder"]["reasons"],
        review["attentionReminder"]["reasons"]
    );
    assert_eq!(
        dismiss(&mut owner, 12, &workspace, json!([])).await["attentionReminder"]["dismissed"],
        false
    );
    // Acknowledgement has no question/request side effects; legacy omission still clears flag.
    rpc(
        &mut owner,
        13,
        "workspace.dismissAttention",
        json!({"workspaceId":workspace}),
    )
    .await;
    assert_eq!(
        srv.store.get_workspace(&workspace).await.unwrap().attention,
        WorkspaceAttention::None
    );
    assert_eq!(
        srv.store
            .get_agent_session(&intent_core::AgentId::from("discussion-agent"))
            .await
            .unwrap()
            .attention_request_kind
            .as_deref(),
        Some("discussion")
    );
    let events = rpc(
        &mut owner,
        14,
        "event.query",
        json!({"workspaceId":workspace,"eventType":"workspace:updated"}),
    )
    .await;
    let serialized = events.to_string();
    assert!(!serialized.contains("receipt"));
    assert!(!serialized.contains("attentionReminder\":{\"reasons"));
    let artifact = srv.dir.path().join("attention-reminders-principals.json");
    std::fs::write(
        &artifact,
        serde_json::to_vec_pretty(
            &json!({"first":first,"reraised":reraised,"fresh":fresh,"events":events}),
        )
        .unwrap(),
    )
    .unwrap();
    eprintln!(
        "attention reminder principal evidence: {} (retain with INTENTD_TEST_KEEP_TMP=1)",
        artifact.display()
    );
    drop((owner, same_person, other));
    srv.ws.stop().await;
}
