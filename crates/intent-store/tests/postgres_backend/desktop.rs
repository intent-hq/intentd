//! Desktop-control journal and browser tab registry on both backends.
//!
//! Decoy rule: bodies use fixed ids and leave rows behind, so the decoy owns
//! every key the subject writes. Values carry the tenant, so a leaked row
//! never looks right. Ids the subject probes as absent (`ghost` agent,
//! `p-ghost` principal, `missing` session, `t-missing` tab, unknown wake ids
//! and outbox keys) are created by the decoy after its own probes. Tie tabs
//! share a `created_at` and have ids whose byte order differs from a locale
//! order, so `COLLATE "C"` decides the list order.

use intent_core::{
    AgentId, AgentSession, AgentStatus, BrowserTab, BrowserTabInput, BrowserTabSize,
    BrowserTabUpsertOutcome, BrowserTabVisibility, ClientId, Error, Principal, PrincipalId,
    PrincipalIdentity, Workspace, WorkspaceId, WorkspaceRole,
};
use intent_store::{AgentQueueRow, Store};
use serde_json::{json, Value};

use crate::harness::{parity_test, TestDb};

const T0: &str = "2026-01-01T00:00:00.000Z";
const TIE: &str = "2026-01-02T00:00:00.000Z";
const T1: &str = "2026-01-03T00:00:00.000Z";
const T2: &str = "2026-01-04T00:00:00.000Z";
const T3: &str = "2026-01-05T00:00:00.000Z";
const FAR: &str = "2099-01-01T00:00:00.000Z";
const PAST: &str = "2020-01-01T00:00:00.000Z";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

fn pid(id: &str) -> PrincipalId {
    PrincipalId(id.into())
}

fn cid(id: &str) -> ClientId {
    ClientId(id.into())
}

fn not_found<T: std::fmt::Debug>(r: &intent_core::Result<T>) -> bool {
    matches!(r, Err(Error::NotFound(_)))
}

fn invalid<T: std::fmt::Debug>(r: &intent_core::Result<T>) -> bool {
    matches!(r, Err(Error::InvalidParams(_)))
}

/// Raw SQL on this run's rows: `{w}` becomes the tenant filter on Postgres
/// (through the admin pool, which the leak trap does not apply to).
async fn sql(store: &Store, db: Option<&TestDb>, tenant: &str, stmt: &str) {
    match db {
        Some(db) => {
            sqlx::raw_sql(sqlx::AssertSqlSafe(
                stmt.replace("{w}", &format!("tenant_id = '{tenant}'")),
            ))
            .execute(&db.admin)
            .await
            .expect("setup statement");
        }
        None => {
            sqlx::raw_sql(sqlx::AssertSqlSafe(stmt.replace("{w}", "1 = 1")))
                .execute(store.write_pool())
                .await
                .expect("setup statement");
        }
    }
}

/// Clients are not on the Postgres store yet, so Postgres seeds them in SQL.
async fn client(store: &Store, db: Option<&TestDb>, tenant: &str, id: &str) {
    if db.is_none() {
        store.ensure_client(&cid(id)).await.unwrap();
    } else {
        sql(
            store,
            db,
            tenant,
            &format!(
                "INSERT INTO client (tenant_id, id, first_seen, last_seen) \
                 VALUES ('{tenant}', '{id}', 'x', 'x')"
            ),
        )
        .await;
    }
}

async fn workspace(store: &Store, id: &str, tenant: &str) {
    store
        .insert_workspace(&Workspace {
            id: wid(id),
            title: format!("{id} of {tenant}"),
            branch: "main".into(),
            created_at: T0.into(),
            updated_at: T0.into(),
            ..Default::default()
        })
        .await
        .unwrap();
}

async fn agent(store: &Store, id: &str, ws: &str, tenant: &str) {
    store
        .insert_agent_session(&AgentSession {
            id: aid(id),
            workspace_id: wid(ws),
            name: format!("{id} of {tenant}"),
            status: AgentStatus::Idle,
            created_at: T0.into(),
            updated_at: T0.into(),
            ..AgentSession::default()
        })
        .await
        .unwrap();
}

async fn principal(store: &Store, id: &str, github: i64, tenant: &str) {
    let identity = PrincipalIdentity::github(github);
    store
        .upsert_principal(&Principal {
            id: pid(id),
            github_user_id: identity.github_user_id(),
            identity: Some(identity),
            login: Some(format!("{id}-{tenant}")),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: T0.into(),
            updated_at: T0.into(),
        })
        .await
        .unwrap();
}

/// `ws-a` (owned by `p1`, agent `a1`), `ws-b` (agent `a2`), principal `p2`
/// and clients `c1`..`c3`.
async fn seed(store: &Store, db: Option<&TestDb>, tenant: &str) {
    workspace(store, "ws-a", tenant).await;
    workspace(store, "ws-b", tenant).await;
    agent(store, "a1", "ws-a", tenant).await;
    agent(store, "a2", "ws-b", tenant).await;
    principal(store, "p1", 1, tenant).await;
    principal(store, "p2", 2, tenant).await;
    // A new workspace is owned by the primary principal; hand it to p1.
    if let Some(owner) = store
        .get_workspace_owner_principal_id(&wid("ws-a"))
        .await
        .unwrap()
    {
        store
            .remove_workspace_member(&wid("ws-a"), &owner)
            .await
            .unwrap();
    }
    store
        .add_workspace_member(&wid("ws-a"), &pid("p1"), WorkspaceRole::Owner)
        .await
        .unwrap();
    for c in ["c1", "c2", "c3"] {
        client(store, db, tenant, c).await;
    }
}

/// Decoy-only targets the subject probes as absent.
async fn seed_ghosts(store: &Store, tenant: &str) {
    agent(store, "ghost", "ws-a", tenant).await;
    principal(store, "p-ghost", 9, tenant).await;
}

fn request_binding(generation: &str, expires: &str, tenant: &str) -> Value {
    json!({
        "principalId": "p1", "clientId": "c1", "computerId": "comp",
        "expiresAt": expires, "assignmentGeneration": generation, "tenant": tenant,
    })
}

fn claim(principal: &str, tenant: &str) -> Value {
    json!({
        "workspaceId": "ws-a", "agentId": "a1", "principalId": principal,
        "clientId": "c1", "computerId": "comp", "tenant": tenant,
    })
}

async fn outbox_keys(store: &Store) -> Vec<String> {
    store
        .desktop_outbox()
        .await
        .unwrap()
        .into_iter()
        .map(|(k, ..)| k)
        .collect()
}

parity_test!(desktop_consent_requests_and_claims, |store, tenant, db| {
    seed(&store, db, tenant).await;
    let (p1, wa, wb, a1) = (pid("p1"), wid("ws-a"), wid("ws-b"), aid("a1"));

    // Remembered consent.
    assert!(!store
        .desktop_permission(&p1, &wa, &a1, "comp")
        .await
        .unwrap());
    store
        .desktop_set_permission(&p1, &wa, &a1, "comp", true)
        .await
        .unwrap();
    assert!(store
        .desktop_permission(&p1, &wa, &a1, "comp")
        .await
        .unwrap());
    store
        .desktop_set_permission(&p1, &wa, &a1, "comp", false)
        .await
        .unwrap();
    assert!(!store
        .desktop_permission(&p1, &wa, &a1, "comp")
        .await
        .unwrap());
    store
        .desktop_set_permission(&p1, &wa, &a1, "comp", true)
        .await
        .unwrap();
    assert!(not_found(
        &store
            .desktop_set_permission(&p1, &wa, &aid("ghost"), "comp", true)
            .await
    ));
    assert!(not_found(
        &store
            .desktop_set_permission(&pid("p-ghost"), &wa, &a1, "comp", true)
            .await
    ));
    // The agent belongs to ws-a, not ws-b.
    assert!(not_found(
        &store
            .desktop_set_permission(&p1, &wb, &a1, "comp", true)
            .await
    ));

    // Requests.
    store
        .desktop_insert_request("r1", &wa, &a1, &request_binding("g1", FAR, tenant))
        .await
        .unwrap();
    assert!(matches!(
        store
            .desktop_insert_request("r1", &wa, &a1, &request_binding("g1", FAR, tenant))
            .await,
        Err(Error::Internal(_))
    ));
    assert!(not_found(
        &store
            .desktop_insert_request("r-wrong", &wb, &a1, &request_binding("g1", FAR, tenant))
            .await
    ));
    assert!(not_found(
        &store
            .desktop_insert_request(
                "r-ghost",
                &wa,
                &aid("ghost"),
                &request_binding("g1", FAR, tenant)
            )
            .await
    ));
    store
        .desktop_insert_request("r-old", &wa, &a1, &request_binding("g1", PAST, tenant))
        .await
        .unwrap();

    // Claims: every predicate must hold.
    let mut missing = claim("p1", tenant);
    missing.as_object_mut().unwrap().remove("clientId");
    assert!(invalid(
        &store
            .desktop_claim_primary("r1", &missing, "g1", true)
            .await
    ));
    assert!(!store
        .desktop_claim_primary("r1", &claim("p1", tenant), "g0", true)
        .await
        .unwrap());
    assert!(!store
        .desktop_claim_primary("r-old", &claim("p1", tenant), "g1", true)
        .await
        .unwrap());
    assert!(!store
        .desktop_claim_primary("r1", &claim("p2", tenant), "g1", true)
        .await
        .unwrap());
    assert!(!store
        .desktop_claim_primary("r-none", &claim("p1", tenant), "g1", true)
        .await
        .unwrap());
    // An open claimed tab in the workspace blocks the claim; its tombstone does not.
    store
        .upsert_browser_tab(
            &cid("c2"),
            BrowserTabInput {
                owner_agent_id: Some(aid("a1")),
                ..input("t-claimed", "ws-a", tenant)
            },
        )
        .await
        .unwrap();
    assert!(!store
        .desktop_claim_primary("r1", &claim("p1", tenant), "g1", true)
        .await
        .unwrap());
    store.close_browser_tab("t-claimed").await.unwrap();
    store
        .desktop_set_permission(&p1, &wa, &a1, "comp", false)
        .await
        .unwrap();
    assert!(store
        .desktop_claim_primary("r1", &claim("p1", tenant), "g1", true)
        .await
        .unwrap());
    assert!(store
        .desktop_permission(&p1, &wa, &a1, "comp")
        .await
        .unwrap());
    assert_eq!(
        store.workspace_browser_client(&wa).await.unwrap(),
        Some(cid("c1"))
    );
    // Claimed once; the workspace now has a browser client too.
    assert!(!store
        .desktop_claim_primary("r1", &claim("p1", tenant), "g1", false)
        .await
        .unwrap());

    // Outcomes go to the outbox exactly once.
    let payload = json!({"type": "desktop_control", "requestId": "r1", "tenant": tenant});
    assert!(!store
        .desktop_resolve_request("r1", &wb, &a1, "granted", &payload)
        .await
        .unwrap());
    assert!(!store
        .desktop_resolve_request("r1", &wa, &aid("a2"), "granted", &payload)
        .await
        .unwrap());
    assert!(store
        .desktop_resolve_request("r1", &wa, &a1, "granted", &payload)
        .await
        .unwrap());
    assert!(!store
        .desktop_resolve_request("r1", &wa, &a1, "denied", &payload)
        .await
        .unwrap());
    assert!(!store
        .desktop_resolve_request("r-none", &wa, &a1, "denied", &payload)
        .await
        .unwrap());
    let outbox = store.desktop_outbox().await.unwrap();
    assert_eq!(
        outbox,
        [(
            "desktop.v1/outbox/request:r1".to_owned(),
            wa.clone(),
            a1.clone(),
            payload.clone()
        )]
    );
    store
        .desktop_outbox_delivered("desktop.v1/outbox/none")
        .await
        .unwrap();
    // Only outbox keys are marked.
    store
        .desktop_outbox_delivered("desktop.v1/request/r-old")
        .await
        .unwrap();
    assert_eq!(outbox_keys(&store).await, ["desktop.v1/outbox/request:r1"]);
    store
        .desktop_outbox_delivered("desktop.v1/outbox/request:r1")
        .await
        .unwrap();
    assert!(outbox_keys(&store).await.is_empty());

    if tenant == "decoy" {
        seed_ghosts(&store, tenant).await;
        store
            .set_setting(
                "desktop.v1/outbox/none",
                &json!({"workspaceId": "ws-a", "agentId": "a1", "payload": {}, "delivered": false})
                    .to_string(),
            )
            .await
            .unwrap();
        store
            .desktop_insert_request("r-none", &wa, &a1, &request_binding("g1", FAR, tenant))
            .await
            .unwrap();
    }
});

parity_test!(desktop_sessions_end_once, |store, tenant, db| {
    seed(&store, db, tenant).await;
    let (wa, a1) = (wid("ws-a"), aid("a1"));
    let binding = |request: &str| json!({"principalId": "p1", "computerId": "comp", "requestId": request, "tenant": tenant});
    store
        .desktop_insert_request("r2", &wa, &a1, &request_binding("g1", FAR, tenant))
        .await
        .unwrap();
    store
        .desktop_insert_terminal("s1", &wa, &a1, &binding("r2"), "hash-1")
        .await
        .unwrap();
    let mut expected = binding("r2");
    expected["workspaceId"] = "ws-a".into();
    expected["agentId"] = "a1".into();
    expected["tokenHash"] = "hash-1".into();
    expected["reason"] = Value::Null;
    expected["reportId"] = Value::Null;
    assert_eq!(
        store.desktop_terminal("s1").await.unwrap(),
        Some(expected.clone())
    );
    assert_eq!(store.desktop_terminal("missing").await.unwrap(), None);
    assert!(not_found(
        &store
            .desktop_record_end("missing", "stopped", None, None)
            .await
    ));
    let mut ghost = binding("r2");
    ghost["principalId"] = "p-ghost".into();
    assert!(not_found(
        &store
            .desktop_insert_terminal("s-ghost", &wa, &a1, &ghost, "hash")
            .await
    ));
    assert!(not_found(
        &store
            .desktop_insert_terminal("s-ghost", &wa, &aid("ghost"), &binding("r2"), "hash")
            .await
    ));

    // An undelivered outcome of the request and of the session is superseded.
    let stale = json!({"sessionId": "s1", "tenant": tenant});
    assert!(store
        .desktop_resolve_request("r2", &wa, &a1, "granted", &stale)
        .await
        .unwrap());
    let end = json!({"type": "desktop_control", "sessionId": "s1", "tenant": tenant});
    assert!(store
        .desktop_record_end("s1", "stopped", None, Some(&end))
        .await
        .unwrap());
    assert_eq!(
        outbox_keys(&store).await,
        ["desktop.v1/outbox/session:s1:stopped"]
    );
    expected["reason"] = "stopped".into();
    assert_eq!(
        store.desktop_terminal("s1").await.unwrap(),
        Some(expected.clone())
    );
    // A reason is recorded once, but a Stop report still lands.
    assert!(!store
        .desktop_record_end("s1", "again", None, Some(&end))
        .await
        .unwrap());
    assert!(store
        .desktop_record_end("s1", "user", Some("rep-1"), Some(&end))
        .await
        .unwrap());
    assert!(!store
        .desktop_record_end("s1", "user", Some("rep-2"), None)
        .await
        .unwrap());
    expected["reason"] = "user".into();
    expected["reportId"] = "rep-1".into();
    assert_eq!(store.desktop_terminal("s1").await.unwrap(), Some(expected));
    // The new outcome supersedes the undelivered one of the same session.
    assert_eq!(
        outbox_keys(&store).await,
        ["desktop.v1/outbox/session:s1:user"]
    );

    // Report ids are unique across sessions; no payload means no outbox row.
    store
        .desktop_insert_terminal("s2", &wa, &a1, &binding("r-none"), "hash-2")
        .await
        .unwrap();
    assert!(invalid(
        &store
            .desktop_record_end("s2", "user", Some("rep-1"), None)
            .await
    ));
    assert!(store
        .desktop_record_end("s2", "done", None, None)
        .await
        .unwrap());
    assert_eq!(outbox_keys(&store).await.len(), 1);

    if tenant == "decoy" {
        seed_ghosts(&store, tenant).await;
        store
            .desktop_insert_terminal("missing", &wa, &a1, &binding("r2"), "hash")
            .await
            .unwrap();
    }
});

parity_test!(desktop_wake_probe, |store, tenant, db| {
    seed(&store, db, tenant).await;
    let (a1, a2) = (aid("a1"), aid("a2"));
    store
        .append_agent_message_with_metadata(
            &a1,
            "user",
            &json!(tenant),
            Some(&json!({"desktopWakeId": "w1"})),
            T0,
        )
        .await
        .unwrap();
    store
        .replace_agent_queue(
            &a1,
            &[AgentQueueRow {
                id: "q1".into(),
                agent_id: a1.clone(),
                position: 0,
                payload: json!({"messageMetadata": {"desktopWakeId": "w2"}, "tenant": tenant}),
                created_at: T0.into(),
                turn_id: "q1".into(),
            }],
        )
        .await
        .unwrap();
    assert!(store.desktop_wake_exists(&a1, "w1").await.unwrap());
    assert!(store.desktop_wake_exists(&a1, "w2").await.unwrap());
    assert!(!store.desktop_wake_exists(&a1, "w3").await.unwrap());
    assert!(!store.desktop_wake_exists(&a2, "w1").await.unwrap());
    assert!(!store.desktop_wake_exists(&a2, "w2").await.unwrap());
    if tenant == "decoy" {
        for (agent, wake) in [(&a1, "w3"), (&a2, "w1")] {
            store
                .append_agent_message_with_metadata(
                    agent,
                    "user",
                    &json!(tenant),
                    Some(&json!({"desktopWakeId": wake})),
                    T0,
                )
                .await
                .unwrap();
        }
        store
            .replace_agent_queue(
                &a2,
                &[AgentQueueRow {
                    id: "q2".into(),
                    agent_id: a2.clone(),
                    position: 0,
                    payload: json!({"messageMetadata": {"desktopWakeId": "w2"}}),
                    created_at: T0.into(),
                    turn_id: "q2".into(),
                }],
            )
            .await
            .unwrap();
    }
});

parity_test!(
    desktop_restart_invalidates_unfinished_work,
    |store, tenant, db| {
        seed(&store, db, tenant).await;
        let (wa, a1) = (wid("ws-a"), aid("a1"));
        // Inserted out of byte order, so the walk order shows in the outbox.
        for r in ["rB", "rA", "rC"] {
            store
                .desktop_insert_request(r, &wa, &a1, &request_binding("g1", FAR, tenant))
                .await
                .unwrap();
        }
        let session = |request: &str| json!({"principalId": "p1", "requestId": request});
        store
            .desktop_insert_terminal("sB", &wa, &a1, &session("r-x"), "h")
            .await
            .unwrap();
        store
            .desktop_insert_terminal("sZ", &wa, &a1, &session("rC"), "h")
            .await
            .unwrap();
        store
            .desktop_insert_terminal("sA", &wa, &a1, &json!({"principalId": "p1"}), "h")
            .await
            .unwrap();
        store
            .desktop_insert_terminal("s-done", &wa, &a1, &session("r-x"), "h")
            .await
            .unwrap();
        assert!(store
            .desktop_record_end("s-done", "stopped", None, None)
            .await
            .unwrap());
        // Orphans: a gone agent, a gone principal; and an undelivered sA outcome.
        let put = |k: &'static str, v: Value| {
            let store = &store;
            async move { store.set_setting(k, &v.to_string()).await.unwrap() }
        };
        put(
            "desktop.v1/request/orphan",
            json!({"workspaceId": "ws-a", "agentId": "nobody", "tenant": tenant}),
        )
        .await;
        put(
            "desktop.v1/permission/orphan",
            json!({"workspaceId": "ws-a", "agentId": "a1", "principalId": "nobody"}),
        )
        .await;
        put(
        "desktop.v1/outbox/stale",
        json!({"workspaceId": "ws-a", "agentId": "a1", "payload": {"sessionId": "sA"}, "delivered": false}),
    )
    .await;
        put("other/kept", json!({"agentId": "nobody"})).await;

        store.desktop_invalidate_restart().await.unwrap();

        let outbox = store.desktop_outbox().await.unwrap();
        let keys: Vec<&str> = outbox.iter().map(|(k, ..)| k.as_str()).collect();
        assert_eq!(
            keys,
            [
                "desktop.v1/outbox/request:rA",
                "desktop.v1/outbox/request:rB",
                "desktop.v1/outbox/session:sA:disconnected",
                "desktop.v1/outbox/session:sB:disconnected",
                "desktop.v1/outbox/session:sZ:disconnected",
            ]
        );
        assert_eq!(
            outbox[0].3,
            json!({"type":"desktop_control","requestId":"rA","outcome":"invalidated","state":{"status":"inactive"},"message":"Daemon restarted; desktop control is not active. Do not automatically restart."})
        );
        assert_eq!(outbox[4].3["requestId"], "rC");
        assert_eq!(outbox[2].3.get("requestId"), None);
        for k in ["desktop.v1/request/orphan", "desktop.v1/permission/orphan"] {
            assert_eq!(store.get_setting(k).await.unwrap(), None, "{k}");
        }
        assert!(store.get_setting("other/kept").await.unwrap().is_some());
        assert_eq!(
            store.desktop_terminal("sA").await.unwrap().unwrap()["reason"],
            "disconnected"
        );
        assert_eq!(
            store.desktop_terminal("s-done").await.unwrap().unwrap()["reason"],
            "stopped"
        );
        // Invalidated requests are terminal.
        assert!(!store
            .desktop_resolve_request("rA", &wa, &a1, "granted", &json!({}))
            .await
            .unwrap());
        // Idempotent.
        store.desktop_invalidate_restart().await.unwrap();
        assert_eq!(outbox_keys(&store).await.len(), 5);
        if tenant == "decoy" {
            // Rows the subject's sweep would wrongly delete if it read them.
            put(
                "desktop.v1/request/orphan",
                json!({"workspaceId": "ws-a", "agentId": "nobody", "tenant": tenant}),
            )
            .await;
            put(
                "desktop.v1/permission/orphan",
                json!({"workspaceId": "ws-a", "agentId": "a1", "principalId": "nobody"}),
            )
            .await;
            put(
            "desktop.v1/outbox/stale",
            json!({"workspaceId": "ws-a", "agentId": "a1", "payload": {"sessionId": "sA"}, "delivered": false}),
        )
        .await;
        }
    }
);

fn input(id: &str, ws: &str, tenant: &str) -> BrowserTabInput {
    BrowserTabInput {
        tab_id: id.into(),
        workspace_id: wid(ws),
        url: format!("https://{tenant}.example/{id}"),
        requested_url: None,
        title: Some(format!("{id} of {tenant}")),
        owner_agent_id: None,
        owner_agent_name: None,
        visibility: BrowserTabVisibility::Visible,
        navigation_only: false,
        emulated_size: None,
        displayed: None,
    }
}

fn ids(tabs: &[BrowserTab]) -> Vec<&str> {
    tabs.iter().map(|t| t.tab_id.as_str()).collect()
}

fn moved_ids(moved: &[(BrowserTab, Value)]) -> Vec<&str> {
    moved.iter().map(|(t, _)| t.tab_id.as_str()).collect()
}

async fn open_tab(store: &Store, host: &str, input: BrowserTabInput) -> BrowserTab {
    match store.upsert_browser_tab(&cid(host), input).await.unwrap() {
        BrowserTabUpsertOutcome::Opened(tab) => tab,
        other => panic!("expected opened, got {other:?}"),
    }
}

parity_test!(native_navigation_tabs_stay_on_host, |store, tenant, db| {
    seed(&store, db, tenant).await;
    let (c1, c2, wa) = (cid("c1"), cid("c2"), wid("ws-a"));
    let mut native = input("t-native", "ws-a", tenant);
    native.navigation_only = true;
    native.owner_agent_id = Some(aid("a1"));
    assert!(open_tab(&store, "c1", native.clone()).await.navigation_only);

    // A host refresh or reconnect snapshot cannot promote a native tab to automation.
    native.navigation_only = false;
    native.title = Some(format!("refreshed by {tenant}"));
    assert!(
        store
            .upsert_browser_tab(&c1, native.clone())
            .await
            .unwrap()
            .tab()
            .navigation_only
    );
    native.url = format!("https://{tenant}.example/reconnected");
    let synced = store.sync_browser_tabs(&c1, vec![native]).await.unwrap();
    assert_eq!(synced.updated.len(), 1);
    assert!(synced.updated[0].0.navigation_only);
    assert!(invalid(
        &store.reassign_browser_tab_host("t-native", &c2, None).await
    ));
    // Claiming ownership on the same device remains supported.
    assert!(
        store
            .reassign_browser_tab_host("t-native", &c1, Some(&aid("a2")))
            .await
            .unwrap()
            .unwrap()
            .0
            .navigation_only
    );

    let mut desktop = input("t-desktop", "ws-a", tenant);
    desktop.owner_agent_id = Some(aid("a1"));
    assert!(!open_tab(&store, "c1", desktop).await.navigation_only);
    assert_eq!(
        moved_ids(&store.reassign_claimed_browser_tabs(&wa, &c2).await.unwrap()),
        ["t-desktop"]
    );
    open_tab(&store, "c1", input("t-free", "ws-a", tenant)).await;
    assert_eq!(
        moved_ids(
            &store
                .reassign_browser_tabs_from_hosts(&c2, std::slice::from_ref(&c1), &|_| true)
                .await
                .unwrap()
        ),
        ["t-free"]
    );
    let persisted = store.get_browser_tab("t-native").await.unwrap().unwrap();
    assert!(persisted.navigation_only);
    assert_eq!(persisted.host_client_id, c1);
    assert_eq!(
        persisted.url,
        format!("https://{tenant}.example/reconnected")
    );
});

parity_test!(browser_tab_reports_and_closes, |store, tenant, db| {
    seed(&store, db, tenant).await;
    let (c1, c2, wa) = (cid("c1"), cid("c2"), wid("ws-a"));
    assert!(store.list_browser_tabs(&wa).await.unwrap().is_empty());
    assert!(store
        .list_browser_tabs_by_host(&c1)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.get_browser_tab("t-missing").await.unwrap(), None);
    assert_eq!(store.close_browser_tab("t-missing").await.unwrap(), None);
    assert_eq!(
        store.remove_browser_tab(&c1, "t-missing").await.unwrap(),
        None
    );

    let mut first = input("t1", "ws-a", tenant);
    first.emulated_size = Some(BrowserTabSize {
        width: 800,
        height: 600,
    });
    first.visibility = BrowserTabVisibility::Hidden;
    first.displayed = Some(true);
    let t1 = open_tab(&store, "c1", first.clone()).await;
    assert_eq!(t1.host_client_id, c1);
    assert_eq!(store.get_browser_tab("t1").await.unwrap(), Some(t1.clone()));
    assert!(matches!(
        store.upsert_browser_tab(&c1, first.clone()).await.unwrap(),
        BrowserTabUpsertOutcome::Unchanged(tab) if tab == t1
    ));
    let mut renamed = first.clone();
    renamed.title = None;
    renamed.owner_agent_id = Some(aid("a1"));
    renamed.displayed = Some(false);
    let BrowserTabUpsertOutcome::Updated { tab, changes } = store
        .upsert_browser_tab(&c1, renamed.clone())
        .await
        .unwrap()
    else {
        panic!("expected updated");
    };
    assert_eq!(
        changes,
        json!({"title": null, "ownerAgentId": "a1", "displayed": false})
    );
    assert_eq!(
        (tab.title, tab.displayed, tab.created_at),
        (None, Some(false), t1.created_at.clone())
    );
    assert!(invalid(
        &store.upsert_browser_tab(&c2, renamed.clone()).await
    ));
    let mut moved = renamed.clone();
    moved.workspace_id = wid("ws-b");
    assert!(invalid(&store.upsert_browser_tab(&c1, moved).await));
    assert!(invalid(&store.remove_browser_tab(&c2, "t1").await));

    // A daemon-side close leaves a tombstone only the host can purge.
    let closed = store.close_browser_tab("t1").await.unwrap().unwrap();
    assert_eq!(
        (closed.tab_id.as_str(), closed.displayed),
        ("t1", Some(false))
    );
    assert_eq!(store.close_browser_tab("t1").await.unwrap(), None);
    assert_eq!(store.get_browser_tab("t1").await.unwrap(), None);
    assert!(store.list_browser_tabs(&wa).await.unwrap().is_empty());
    assert!(invalid(
        &store.upsert_browser_tab(&c1, renamed.clone()).await
    ));
    assert!(invalid(
        &store.upsert_browser_tab(&c2, renamed.clone()).await
    ));
    assert!(invalid(&store.remove_browser_tab(&c2, "t1").await));
    assert_eq!(store.remove_browser_tab(&c1, "t1").await.unwrap(), None);
    let reopened = open_tab(&store, "c1", renamed.clone()).await;
    assert_eq!(reopened.displayed, Some(false));
    let removed = store.remove_browser_tab(&c1, "t1").await.unwrap().unwrap();
    assert_eq!(removed.tab_id, "t1");
    assert_eq!(store.remove_browser_tab(&c1, "t1").await.unwrap(), None);
    open_tab(&store, "c1", first).await;

    // Ties on created_at break by byte order of the id.
    for id in ["tc", "tB", "ta"] {
        open_tab(&store, "c2", input(id, "ws-b", tenant)).await;
    }
    open_tab(&store, "c2", input("t-other", "ws-a", tenant)).await;
    sql(
        &store,
        db,
        tenant,
        &format!("UPDATE browser_tab SET created_at = '{TIE}' WHERE {{w}} AND tab_id IN ('tc', 'tB', 'ta')"),
    )
    .await;
    sql(
        &store,
        db,
        tenant,
        &format!("UPDATE browser_tab SET created_at = '{T0}' WHERE {{w}} AND tab_id = 't-other'"),
    )
    .await;
    assert_eq!(
        ids(&store.list_browser_tabs(&wid("ws-b")).await.unwrap()),
        ["tB", "ta", "tc"]
    );
    assert_eq!(
        ids(&store.list_browser_tabs_by_host(&c2).await.unwrap()),
        ["t-other", "tB", "ta", "tc"]
    );
    assert_eq!(
        ids(&store.list_browser_tabs(&wa).await.unwrap()),
        ["t-other", "t1"]
    );

    if tenant == "decoy" {
        open_tab(&store, "c3", input("t-missing", "ws-a", tenant)).await;
    }
});

parity_test!(browser_tabs_rehome_and_sync, |store, tenant, db| {
    seed(&store, db, tenant).await;
    let (c1, c2, c3, wa) = (cid("c1"), cid("c2"), cid("c3"), wid("ws-a"));
    let claimed = |id: &str| BrowserTabInput {
        owner_agent_id: Some(aid("a1")),
        ..input(id, "ws-a", tenant)
    };
    let mut shown = claimed("tB");
    shown.displayed = Some(true);
    open_tab(&store, "c1", shown).await;
    open_tab(&store, "c1", claimed("ta")).await;
    open_tab(&store, "c1", input("t-free", "ws-a", tenant)).await;
    open_tab(&store, "c3", claimed("t-home")).await;
    open_tab(&store, "c1", claimed("t-b")).await;
    sql(
        &store,
        db,
        tenant,
        &format!(
            "UPDATE browser_tab SET created_at = CASE tab_id \
             WHEN 't-free' THEN '{T1}' WHEN 't-home' THEN '{T2}' WHEN 't-b' THEN '{T3}' \
             ELSE '{TIE}' END WHERE {{w}}"
        ),
    )
    .await;

    // One claim.
    assert_eq!(
        store
            .reassign_browser_tab_host("t-missing", &c2, Some(&aid("a2")))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .reassign_browser_tab_host("t-free", &c1, None)
            .await
            .unwrap(),
        None
    );
    let (tab, changes) = store
        .reassign_browser_tab_host("t-free", &c1, Some(&aid("a2")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(changes, json!({"ownerAgentId": "a2"}));
    assert_eq!(
        (tab.host_client_id, tab.owner_agent_id),
        (c1.clone(), Some(aid("a2")))
    );
    let (tab, changes) = store
        .reassign_browser_tab_host("t-free", &c2, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(changes, json!({"hostClientId": "c2"}));
    assert_eq!(tab.owner_agent_id, Some(aid("a2")));
    store.close_browser_tab("t-b").await.unwrap();
    assert_eq!(
        store
            .reassign_browser_tab_host("t-b", &c2, None)
            .await
            .unwrap(),
        None
    );

    // Driving-client switch: claimed open tabs not already on the host.
    let moved = store.reassign_claimed_browser_tabs(&wa, &c3).await.unwrap();
    assert_eq!(moved_ids(&moved), ["tB", "ta", "t-free"]);
    assert_eq!(moved[0].1, json!({"hostClientId": "c3", "displayed": null}));
    assert_eq!(moved[1].1, json!({"hostClientId": "c3"}));
    assert_eq!(moved[0].0.displayed, None);
    assert_eq!(
        store
            .get_browser_tab("tB")
            .await
            .unwrap()
            .unwrap()
            .displayed,
        None
    );
    assert!(store
        .reassign_claimed_browser_tabs(&wa, &c3)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .reassign_claimed_browser_tabs(&wid("ws-b"), &c1)
        .await
        .unwrap()
        .is_empty());

    // Superseded hosts.
    assert!(store
        .reassign_browser_tabs_from_hosts(&c2, &[], &|_| true)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .reassign_browser_tabs_from_hosts(&c2, std::slice::from_ref(&c3), &|_| false)
        .await
        .unwrap()
        .is_empty());
    let moved = store
        .reassign_browser_tabs_from_hosts(&c2, &[c1.clone(), c3.clone()], &|h| h.0 == "c3")
        .await
        .unwrap();
    assert_eq!(moved_ids(&moved), ["tB", "ta", "t-free", "t-home"]);
    assert_eq!(
        ids(&store.list_browser_tabs_by_host(&c3).await.unwrap()),
        Vec::<&str>::new()
    );
    assert_eq!(
        ids(&store.list_browser_tabs_by_host(&c2).await.unwrap()),
        ["tB", "ta", "t-free", "t-home"]
    );
    // The tombstone stays with its old host.
    assert!(invalid(&store.remove_browser_tab(&c2, "t-b").await));

    // Host snapshot: create, refresh, drop foreign and tombstoned ids,
    // close absent open rows and purge absent tombstones.
    open_tab(&store, "c1", input("t-gone", "ws-a", tenant)).await;
    let mut refreshed = input("t-c1", "ws-a", tenant);
    open_tab(&store, "c1", refreshed.clone()).await;
    refreshed.url = format!("https://{tenant}.example/next");
    refreshed.displayed = Some(true);
    let mut duplicate = input("t-new", "ws-a", tenant);
    duplicate.title = Some("second".into());
    let result = store
        .sync_browser_tabs(
            &c1,
            vec![
                input("t-new", "ws-b", tenant),
                duplicate,
                refreshed.clone(),
                input("tB", "ws-a", tenant),
                input("t-b", "ws-a", tenant),
            ],
        )
        .await
        .unwrap();
    assert_eq!(ids(&result.opened), ["t-new"]);
    assert_eq!(result.opened[0].workspace_id, wid("ws-b"));
    assert_eq!(moved_ids(&result.updated), ["t-c1"]);
    assert_eq!(
        result.updated[0].1,
        json!({"url": format!("https://{tenant}.example/next"), "displayed": true})
    );
    assert_eq!(result.drop, ["tB", "t-b"]);
    assert_eq!(ids(&result.closed), ["t-gone"]);
    assert_eq!(
        store
            .get_browser_tab("t-c1")
            .await
            .unwrap()
            .unwrap()
            .displayed,
        Some(true)
    );
    // The tombstone survives while still reported, and goes once omitted.
    assert!(invalid(&store.remove_browser_tab(&c2, "t-b").await));
    let result = store
        .sync_browser_tabs(&c1, vec![refreshed.clone(), input("t-new", "ws-b", tenant)])
        .await
        .unwrap();
    assert!(result.opened.is_empty() && result.updated.is_empty());
    assert!(result.drop.is_empty() && result.closed.is_empty());
    assert_eq!(store.remove_browser_tab(&c2, "t-b").await.unwrap(), None);

    // A workspace move rolls the whole snapshot back.
    assert!(invalid(
        &store
            .sync_browser_tabs(
                &c1,
                vec![
                    input("t-rollback", "ws-a", tenant),
                    input("t-c1", "ws-b", tenant)
                ],
            )
            .await
    ));
    assert_eq!(store.get_browser_tab("t-rollback").await.unwrap(), None);
    assert_eq!(
        ids(&store.list_browser_tabs_by_host(&c1).await.unwrap()),
        ["t-c1", "t-new"]
    );

    if tenant == "decoy" {
        open_tab(&store, "c3", input("t-missing", "ws-a", tenant)).await;
        open_tab(&store, "c3", input("t-rollback", "ws-a", tenant)).await;
        open_tab(&store, "c3", input("t-b", "ws-a", tenant)).await;
    }
});
