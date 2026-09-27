//! Incremental deletion through the authenticated, pinned TLS transport.

use super::*;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};

type Ws = tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;
const ROWS: i64 = 2_001;

async fn connect(srv: &Server) -> Ws {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let tls = common::tls_connect_with_retry(srv.port, srv.cfg.clone()).await;
    let mut req = format!("wss://localhost:{}/ws", srv.port)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("Authorization", format!("Bearer {TOKEN}").parse().unwrap());
    req.headers_mut()
        .insert("Origin", "http://localhost".parse().unwrap());
    tokio_tungstenite::client_async(req, tls)
        .await
        .expect("authenticated WSS upgrade")
        .0
}

async fn frame(ws: &mut Ws) -> Value {
    tokio::time::timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(s))) => return serde_json::from_str(&s).unwrap(),
                Some(Ok(Message::Ping(p))) => ws.send(Message::Pong(p)).await.unwrap(),
                Some(Ok(_)) => {}
                other => panic!("WebSocket closed: {other:?}"),
            }
        }
    })
    .await
    .expect("WSS frame deadline")
}

async fn rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    loop {
        let v = frame(ws).await;
        if v["id"] == id {
            assert_eq!(v["jsonrpc"], "2.0");
            assert_ne!(v.get("result").is_some(), v.get("error").is_some(), "{v}");
            return v;
        }
    }
}

async fn seed(srv: &Server, ws: &mut Ws, title: &str, agents: usize) -> String {
    let v = rpc(ws, 1, "workspace.create", json!({"title":title})).await;
    let id = v["result"]["workspace"]["id"].as_str().unwrap().to_string();
    for n in 0..agents {
        let v = rpc(
            ws,
            2,
            "agent.create",
            json!({"workspaceId":id, "name":format!("history-{n}")}),
        )
        .await;
        let agent = v["result"]["agent"]["id"].as_str().unwrap();
        let mut tx = srv.store.write_pool().begin().await.unwrap();
        sqlx::query("WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n < ?) \
            INSERT INTO agent_message (id,agent_id,seq,role,content,created_at) \
            SELECT ? || '-' || n, ?, n, 'assistant', '[{\"type\":\"text\",\"text\":\"searchable WSS history\"}]', 't0' FROM rows")
            .bind(ROWS).bind(agent).bind(agent).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO agent_message_payload (message_id,agent_id,block_ordinal,kind,encoding,body) \
            SELECT id,agent_id,0,'tool_result_output','none',zeroblob(8192) FROM agent_message WHERE agent_id=?")
            .bind(agent).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
    }
    id
}

#[tokio::test]
async fn loaded_bulk_delete_keeps_other_clients_writable_and_events_persisted() {
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", json!("auggie"));
    let mut deleting = connect(&srv).await;
    let mut probing = connect(&srv).await;
    let mut watching = connect(&srv).await;
    let failed = seed(&srv, &mut deleting, "failed deletion", 3).await;
    let a = seed(&srv, &mut deleting, "loaded deletion a", 3).await;
    let b = seed(&srv, &mut deleting, "loaded deletion b", 3).await;
    let keeper = seed(&srv, &mut probing, "surviving workspace", 0).await;
    // Seed as the owner, then exercise the same partial/retry/concurrency
    // lifecycle as an authorized member with no explicit workspace grant.
    let member = Guest::connect(&srv, &"c7".repeat(32)).await;
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(member.principal.id.as_str())
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    deleting = member.ws;
    let initial_note_events = srv
        .store
        .events_by_workspace(&WorkspaceId::from(keeper.as_str()), 100)
        .await
        .unwrap()
        .iter()
        .filter(|e| e.event_type == "note:created")
        .count();
    let sub = rpc(
        &mut watching,
        3,
        "events.subscribe",
        json!({"eventTypes":["agent:deleted", "workspace:deleted", "note:created"]}),
    )
    .await;
    assert!(sub.get("error").is_none(), "{sub}");

    // Instrument only the scratch database. Each audit row captures history
    // remaining INSIDE the unrelated write/event transaction, proving actual
    // writer interleaving rather than a response that arrives after deletion.
    sqlx::query("CREATE TABLE deletion_probes(kind TEXT, remaining INTEGER)")
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    for (table, condition, kind) in [
        ("note", format!("NEW.workspace_id='{keeper}'"), "write"),
        (
            "event",
            format!("NEW.workspace_id='{keeper}' AND NEW.event_type='note:created'"),
            "event",
        ),
    ] {
        sqlx::query(&format!(
            "CREATE TRIGGER probe_{kind} AFTER INSERT ON {table} WHEN {condition} BEGIN \
            INSERT INTO deletion_probes SELECT '{kind}', COALESCE(MAX(n),0) FROM \
            (SELECT COUNT(*) n FROM agent_message_payload p JOIN agent_session s ON s.id=p.agent_id \
            WHERE s.workspace_id IN ('{a}','{b}') GROUP BY s.workspace_id \
            HAVING COUNT(*) > 0 AND COUNT(*) < {}); END", 3 * ROWS
        ))
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    }
    sqlx::query(&format!("CREATE TRIGGER fail_partial_delete BEFORE DELETE ON agent_message \
        WHEN OLD.seq > 500 AND OLD.agent_id IN (SELECT id FROM agent_session WHERE workspace_id='{failed}') \
        BEGIN SELECT RAISE(ABORT, 'injected WSS cleanup failure'); END"))
        .execute(srv.store.write_pool()).await.unwrap();

    let done = AtomicBool::new(false);
    let deletion = async {
        // Match the existing client's sequential, best-effort bulk loop.
        let error = rpc(
            &mut deleting,
            10,
            "workspace.delete",
            json!({"workspaceId":failed}),
        )
        .await;
        assert_eq!(error["error"]["code"], -32603, "{error}");
        assert!(
            error.to_string().contains("injected WSS cleanup failure"),
            "{error}"
        );
        srv.store
            .get_workspace(&WorkspaceId::from(failed.as_str()))
            .await
            .expect("failed deletion retains row");
        let initial = 3 * ROWS;
        let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_message_payload p JOIN agent_session s ON s.id=p.agent_id WHERE s.workspace_id=?")
            .bind(&failed).fetch_one(srv.store.read_pool()).await.unwrap();
        assert!(left < initial, "failure happened after partial cleanup");
        for (id, workspace) in [(11, &a), (12, &b)] {
            let v = rpc(
                &mut deleting,
                id,
                "workspace.delete",
                json!({"workspaceId":workspace}),
            )
            .await;
            assert_eq!(
                v,
                json!({"jsonrpc":"2.0", "id":id, "result":{"success":true}})
            );
            assert!(srv
                .store
                .get_workspace(&WorkspaceId::from(workspace.as_str()))
                .await
                .is_err());
        }
        done.store(true, Ordering::SeqCst);
    };
    let probes = async {
        let mut n = 0;
        while !done.load(Ordering::SeqCst) {
            let read = rpc(
                &mut probing,
                20,
                "workspace.get",
                json!({"workspaceId":keeper}),
            )
            .await;
            assert_eq!(read["result"]["workspace"]["id"], keeper);
            let write = rpc(&mut probing, 21, "note.create", json!({"workspaceId":keeper, "title":format!("probe-{n}"), "content":"unrelated persisted write"})).await;
            assert!(write.get("error").is_none(), "{write}");
            n += 1;
        }
        n
    };
    let ((), probes) = tokio::time::timeout(common::test_timeout(Duration::from_secs(90)), async {
        tokio::join!(deletion, probes)
    })
    .await
    .expect("bulk deletion and probes finish");
    for kind in ["write", "event"] {
        let interleaved: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM deletion_probes WHERE kind=? AND remaining > 0 AND remaining < ?",
        )
        .bind(kind)
        .bind(3 * ROWS)
        .fetch_one(srv.store.read_pool())
        .await
        .unwrap();
        assert!(
            interleaved > 0,
            "{kind} never committed during partial history cleanup"
        );
    }
    let persisted = srv
        .store
        .events_by_workspace(&WorkspaceId::from(keeper.as_str()), 10_000)
        .await
        .unwrap();
    let queried = rpc(
        &mut probing,
        22,
        "event.query",
        json!({"workspaceId":keeper, "eventTypes":["note:created"], "limit":10000}),
    )
    .await;
    let queried = queried["result"]
        .as_array()
        .expect("persisted event.query envelope");
    assert_eq!(
        queried
            .iter()
            .filter(|e| e["type"] == "note:created")
            .count(),
        initial_note_events + probes
    );
    assert_eq!(
        persisted
            .iter()
            .filter(|e| e.event_type == "note:created")
            .count(),
        initial_note_events + probes
    );
    assert!(!srv
        .store
        .events_by_workspace(&WorkspaceId::from(failed.as_str()), 100)
        .await
        .unwrap()
        .iter()
        .any(|e| e.event_type == "workspace:deleted"));

    sqlx::query("DROP TRIGGER fail_partial_delete")
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    for id in [30, 31] {
        let v = rpc(
            &mut deleting,
            id,
            "workspace.delete",
            json!({"workspaceId":failed}),
        )
        .await;
        assert_eq!(
            v,
            json!({"jsonrpc":"2.0", "id":id, "result":{"success":true}})
        );
    }
    let mut agents = std::collections::HashMap::<String, std::collections::HashSet<String>>::new();
    let mut workspaces = std::collections::HashSet::new();
    while workspaces.len() < 3 {
        let v = frame(&mut watching).await;
        if v["method"] != "events.event" {
            continue;
        }
        assert_eq!(v["jsonrpc"], "2.0");
        let e = &v["params"]["event"];
        let ws = e["workspaceId"].as_str().unwrap();
        match e["type"].as_str().unwrap() {
            "agent:deleted" => {
                agents
                    .entry(ws.into())
                    .or_default()
                    .insert(e["data"]["agentId"].as_str().unwrap().into());
            }
            "workspace:deleted" => {
                assert_eq!(
                    agents.get(ws).map(std::collections::HashSet::len),
                    Some(3),
                    "all agent events precede workspace deletion: {v}"
                );
                assert_eq!(e["data"], json!({"workspaceId":ws}));
                workspaces.insert(ws.to_string());
            }
            _ => {}
        }
    }
    for ws in [&failed, &a, &b] {
        assert!(workspaces.contains(ws));
        assert_eq!(
            srv.store
                .events_by_workspace(&WorkspaceId::from(ws.as_str()), 100)
                .await
                .unwrap()
                .iter()
                .filter(|e| e.event_type == "workspace:deleted")
                .count(),
            1
        );
    }
    srv.ws.stop().await;
}
