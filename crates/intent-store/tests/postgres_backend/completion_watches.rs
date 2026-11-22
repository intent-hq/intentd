//! Completion watch, delegation group and stop-redelivery store methods on
//! both backends. Tie rows share a `created_at` and have ids whose order
//! depends on collation (`B` < `a` < `c` in byte order only), so the
//! `row_seq` tie-break and `COLLATE "C"` decide results.
//!
//! Decoy rule: the decoy re-seeds every row after its own run, so the
//! subject's absent-id probes, writes and filtered reads all have decoy rows
//! behind them (plus a decoy-only workspace with an undelivered group).
//! Names, payloads and summaries carry the tenant, so a leaked row never
//! looks right.

use intent_core::{AgentId, AgentSession, AgentStatus, Workspace, WorkspaceId};
use intent_store::{PersistedCompletionWatch, PersistedDelegationGroup, Store};

use crate::harness::{parity_test, TestDb};

const CHIEF: &str = "__chief__";
const T0: &str = "2026-01-01T00:00:00.000Z";
const T1: &str = "2026-01-02T00:00:00.000Z";
const T2: &str = "2026-01-03T00:00:00.000Z";
const T3: &str = "2026-01-04T00:00:00.000Z";
const T4: &str = "2026-01-05T00:00:00.000Z";
const T5: &str = "2026-01-06T00:00:00.000Z";
const TIE: &str = "2026-01-07T00:00:00.000Z";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

fn workspace(id: &str) -> Workspace {
    Workspace {
        id: wid(id),
        title: format!("title {id}"),
        branch: "main".into(),
        created_at: T0.into(),
        updated_at: T0.into(),
        ..Default::default()
    }
}

async fn sessions(store: &Store, ids: &[&str], tenant: &str) {
    for id in ids {
        store
            .insert_agent_session(&AgentSession {
                id: aid(id),
                workspace_id: wid(CHIEF),
                name: format!("{id} of {tenant}"),
                status: AgentStatus::Idle,
                created_at: T0.into(),
                updated_at: T0.into(),
                ..AgentSession::default()
            })
            .await
            .unwrap();
    }
}

fn dbg<T: std::fmt::Debug>(rows: &[T]) -> Vec<String> {
    rows.iter().map(|r| format!("{r:?}")).collect()
}

// ---- completion watches ----------------------------------------------------------------

fn watch(
    id: &str,
    parent: &str,
    child: &str,
    group: Option<&str>,
    created_at: &str,
    tenant: &str,
) -> PersistedCompletionWatch {
    PersistedCompletionWatch {
        id: id.into(),
        parent_workspace_id: wid(CHIEF),
        child_workspace_id: wid("ws-child"),
        parent_agent_id: aid(parent),
        parent_agent_name: format!("{parent} of {tenant}"),
        child_agent_id: aid(child),
        group_id: group.map(Into::into),
        report_delivered: false,
        wake_on_attention: false,
        completion_only: false,
        created_at: created_at.into(),
    }
}

/// `w1` p1→c1 ungrouped; `w2` p1→c2, `w3` p2→c3, `w4` p2→c1 in group `g1`;
/// `w6`, `w7` in group `g2`; ties `wc`, `wB`, `wa` p2→c2 ungrouped.
fn watches(tenant: &str) -> Vec<PersistedCompletionWatch> {
    vec![
        watch("w1", "p1", "c1", None, T1, tenant),
        watch("w2", "p1", "c2", Some("g1"), T2, tenant),
        watch("w3", "p2", "c3", Some("g1"), T3, tenant),
        watch("w4", "p2", "c1", Some("g1"), T4, tenant),
        watch("w6", "p1", "c1", Some("g2"), T5, tenant),
        watch("w7", "p2", "c2", Some("g2"), T5, tenant),
        watch("wc", "p2", "c2", None, TIE, tenant),
        watch("wB", "p2", "c2", None, TIE, tenant),
        watch("wa", "p2", "c2", None, TIE, tenant),
    ]
}

fn group(
    id: &str,
    ws: &str,
    created_at: &str,
    delivered: bool,
    tenant: &str,
) -> PersistedDelegationGroup {
    PersistedDelegationGroup {
        group_id: id.into(),
        workspace_id: wid(ws),
        parent_agent_id: aid("p1"),
        await_mode: "after_all".into(),
        expected_agent_ids: vec![aid("c1"), aid("c2")],
        completed_agent_ids: vec![aid("c1")],
        deleted_agent_ids: vec![],
        sealed: false,
        delivered,
        event_summaries: vec![format!("summary of {tenant}")],
        raw_events_json: vec![format!("{{\"tenant\":\"{tenant}\"}}")],
        created_at: created_at.into(),
        updated_at: created_at.into(),
    }
}

async fn seed_watches(store: &Store, tenant: &str) -> Vec<PersistedCompletionWatch> {
    let all = watches(tenant);
    for w in &all {
        store.upsert_completion_watch(w).await.unwrap();
    }
    for g in ["g1", "g2"] {
        store
            .upsert_delegation_group(&group(g, CHIEF, T1, false, tenant))
            .await
            .unwrap();
    }
    all
}

parity_test!(completion_watch_writes_and_settlement, |store, tenant| {
    let (p1, p2, c1, c2, c3) = (aid("p1"), aid("p2"), aid("c1"), aid("c2"), aid("c3"));
    sessions(&store, &["p1", "p2", "c1", "c2", "c3"], tenant).await;

    assert!(store.list_completion_watches().await.unwrap().is_empty());
    store.delete_completion_watch("w1").await.unwrap();
    store
        .delete_completion_watches_for_parent(&p1)
        .await
        .unwrap();
    store
        .mark_completion_watch_report_delivered("w1")
        .await
        .unwrap();
    store
        .clear_completion_watch_report_delivered("w1")
        .await
        .unwrap();
    store.ungroup_completion_watch("w1").await.unwrap();
    store
        .update_completion_watch_parent("w1", "x", &wid("ws-x"))
        .await
        .unwrap();
    store
        .retire_completion_watch_after_delivery("w1", &p1, &c1, None, T5)
        .await
        .unwrap();
    store
        .settle_delegation_group_after_delivery("g1", std::slice::from_ref(&c3))
        .await
        .unwrap();
    assert!(store.list_completion_watches().await.unwrap().is_empty());
    assert_eq!(
        store.get_completion_wake_delivery(&p1, &c1).await.unwrap(),
        None
    );

    let mut expected = seed_watches(&store, tenant).await;
    assert_eq!(
        dbg(&store.list_completion_watches().await.unwrap()),
        dbg(&expected)
    );

    // Conflict: only the mutable columns change.
    let mut changed = watch("w1", "p9", "c9", Some("g2"), T5, "changed");
    changed.parent_workspace_id = wid("ws-new");
    changed.child_workspace_id = wid("ws-other");
    changed.report_delivered = true;
    changed.wake_on_attention = true;
    changed.completion_only = true;
    store.upsert_completion_watch(&changed).await.unwrap();
    let w1 = &mut expected[0];
    w1.parent_workspace_id = wid("ws-new");
    w1.parent_agent_name = changed.parent_agent_name.clone();
    w1.group_id = Some("g2".into());
    w1.report_delivered = true;
    w1.wake_on_attention = true;
    w1.completion_only = true;
    assert_eq!(
        dbg(&store.list_completion_watches().await.unwrap()),
        dbg(&expected)
    );

    store
        .clear_completion_watch_report_delivered("w1")
        .await
        .unwrap();
    store.ungroup_completion_watch("w1").await.unwrap();
    store
        .mark_completion_watch_report_delivered("wB")
        .await
        .unwrap();
    store
        .update_completion_watch_parent("wa", &format!("renamed by {tenant}"), &wid("ws-x"))
        .await
        .unwrap();
    expected[0].report_delivered = false;
    expected[0].group_id = None;
    expected[7].report_delivered = true;
    expected[8].parent_agent_name = format!("renamed by {tenant}");
    expected[8].parent_workspace_id = wid("ws-x");
    assert_eq!(
        dbg(&store.list_completion_watches().await.unwrap()),
        dbg(&expected)
    );

    // Retire with a delivery marker, then re-mark on an already-retired watch.
    let ident = format!("ident of {tenant}");
    store
        .retire_completion_watch_after_delivery("w2", &p1, &c2, Some(&ident), T5)
        .await
        .unwrap();
    assert_eq!(
        store.get_completion_wake_delivery(&p1, &c2).await.unwrap(),
        Some(ident)
    );
    store
        .retire_completion_watch_after_delivery("w2", &p1, &c2, Some("second"), T5)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_completion_wake_delivery(&p1, &c2)
            .await
            .unwrap()
            .as_deref(),
        Some("second")
    );
    store
        .retire_completion_watch_after_delivery("wc", &p2, &c2, None, T5)
        .await
        .unwrap();
    assert_eq!(
        store.get_completion_wake_delivery(&p2, &c2).await.unwrap(),
        None
    );
    expected.retain(|w| w.id != "w2" && w.id != "wc");

    // g1 keeps c3 (ungrouped) and a child it does not hold; w4 goes. g2 keeps none.
    store
        .settle_delegation_group_after_delivery("g1", &[c3.clone(), aid("c-none")])
        .await
        .unwrap();
    store
        .settle_delegation_group_after_delivery("g2", &[])
        .await
        .unwrap();
    expected.retain(|w| !["w4", "w6", "w7"].contains(&w.id.as_str()));
    expected
        .iter_mut()
        .filter(|w| w.id == "w3")
        .for_each(|w| w.group_id = None);
    assert_eq!(
        dbg(&store.list_completion_watches().await.unwrap()),
        dbg(&expected)
    );
    assert!(store
        .list_undelivered_groups(&wid(CHIEF))
        .await
        .unwrap()
        .is_empty());

    store
        .delete_completion_watches_for_parent(&p2)
        .await
        .unwrap();
    store.delete_completion_watch("w1").await.unwrap();
    assert!(store.list_completion_watches().await.unwrap().is_empty());

    if tenant == "decoy" {
        seed_watches(&store, tenant).await;
    }
});

// ---- delegation groups -----------------------------------------------------------------

/// `g1` (`ws-a`), `g2` delivered (`ws-a`), `g3` (`ws-B`), `g4` delivered
/// (`ws-B`), ties `gc`, `gB`, `ga` (`ws-a`).
fn groups(tenant: &str) -> Vec<PersistedDelegationGroup> {
    vec![
        group("g1", "ws-a", T1, false, tenant),
        group("g2", "ws-a", T2, true, tenant),
        group("g3", "ws-B", T3, false, tenant),
        group("g4", "ws-B", T4, true, tenant),
        group("gc", "ws-a", TIE, false, tenant),
        group("gB", "ws-a", TIE, false, tenant),
        group("ga", "ws-a", TIE, false, tenant),
    ]
}

async fn seed_groups(store: &Store, tenant: &str) -> Vec<PersistedDelegationGroup> {
    let all = groups(tenant);
    for g in &all {
        store.upsert_delegation_group(g).await.unwrap();
    }
    all
}

fn sorted(mut ids: Vec<WorkspaceId>) -> Vec<String> {
    ids.sort_by(|a, b| a.0.cmp(&b.0));
    ids.into_iter().map(|w| w.0).collect()
}

parity_test!(delegation_group_round_trip, |store, tenant| {
    let (wa, wb) = (wid("ws-a"), wid("ws-B"));
    for ws in ["ws-a", "ws-B"] {
        store.insert_workspace(&workspace(ws)).await.unwrap();
    }

    assert!(store.list_undelivered_groups(&wa).await.unwrap().is_empty());
    assert!(store
        .list_workspaces_with_undelivered_groups()
        .await
        .unwrap()
        .is_empty());
    store.delete_delegation_group("g1").await.unwrap();

    let all = seed_groups(&store, tenant).await;
    let pick = |ids: &[&str]| {
        ids.iter()
            .map(|id| all.iter().find(|g| g.group_id == *id).unwrap().clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        dbg(&store.list_undelivered_groups(&wa).await.unwrap()),
        dbg(&pick(&["g1", "gc", "gB", "ga"]))
    );
    assert_eq!(
        dbg(&store.list_undelivered_groups(&wb).await.unwrap()),
        dbg(&pick(&["g3"]))
    );
    assert_eq!(
        sorted(
            store
                .list_workspaces_with_undelivered_groups()
                .await
                .unwrap()
        ),
        ["ws-B", "ws-a"]
    );

    // Conflict: identity columns stay, the fan-in state is replaced.
    let mut changed = group("g1", "ws-B", T5, false, "changed");
    changed.parent_agent_id = aid("p9");
    changed.await_mode = "other".into();
    changed.expected_agent_ids = vec![aid("c3")];
    changed.completed_agent_ids = vec![aid("c3"), aid("c4")];
    changed.deleted_agent_ids = vec![aid("c5")];
    changed.sealed = true;
    changed.event_summaries = vec!["one".into(), "two".into()];
    changed.raw_events_json = vec![];
    store.upsert_delegation_group(&changed).await.unwrap();
    let mut g1 = pick(&["g1"]).remove(0);
    g1.expected_agent_ids = changed.expected_agent_ids.clone();
    g1.completed_agent_ids = changed.completed_agent_ids.clone();
    g1.deleted_agent_ids = changed.deleted_agent_ids.clone();
    g1.sealed = true;
    g1.event_summaries = changed.event_summaries.clone();
    g1.raw_events_json = vec![];
    g1.updated_at = T5.into();
    let mut want = pick(&["gc", "gB", "ga"]);
    want.insert(0, g1);
    assert_eq!(
        dbg(&store.list_undelivered_groups(&wa).await.unwrap()),
        dbg(&want)
    );
    assert_eq!(store.list_undelivered_groups(&wb).await.unwrap().len(), 1);

    let mut g3 = pick(&["g3"]).remove(0);
    g3.delivered = true;
    store.upsert_delegation_group(&g3).await.unwrap();
    assert!(store.list_undelivered_groups(&wb).await.unwrap().is_empty());
    assert_eq!(
        sorted(
            store
                .list_workspaces_with_undelivered_groups()
                .await
                .unwrap()
        ),
        ["ws-a"]
    );

    store.delete_delegation_group("g1").await.unwrap();
    store.delete_delegation_group("g-none").await.unwrap();
    assert_eq!(
        dbg(&store.list_undelivered_groups(&wa).await.unwrap()),
        dbg(&pick(&["gc", "gB", "ga"]))
    );

    if tenant == "decoy" {
        seed_groups(&store, tenant).await;
        store
            .insert_workspace(&workspace("ws-decoy"))
            .await
            .unwrap();
        store
            .upsert_delegation_group(&group("g-decoy", "ws-decoy", T0, false, tenant))
            .await
            .unwrap();
    }
});

// ---- stop redeliveries -----------------------------------------------------------------

fn payload(tenant: &str, n: u32) -> serde_json::Value {
    serde_json::json!({ "tenant": tenant, "n": n })
}

/// Overwrite one stored payload with text that is not JSON.
async fn corrupt_payload(store: &Store, db: Option<&TestDb>, tenant: &str, agent: &str) {
    match db {
        None => {
            sqlx::query("UPDATE agent_stop_redelivery SET payload = 'not json' WHERE agent_id = ?")
                .bind(agent)
                .execute(store.write_pool())
                .await
                .unwrap();
        }
        Some(db) => {
            sqlx::query(
                "UPDATE agent_stop_redelivery SET payload = 'not json' \
                 WHERE tenant_id = $1 AND agent_id = $2",
            )
            .bind(tenant)
            .bind(agent)
            .execute(&db.admin)
            .await
            .unwrap();
        }
    }
}

const REDELIVERY_AGENTS: [&str; 4] = ["ab", "aC", "a1", "a2"];

/// Every loaded payload as `(agent, payload, created_at)`.
async fn load(store: &Store) -> Vec<(String, serde_json::Value, String)> {
    store
        .load_all_stop_redeliveries()
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.agent_id.0, r.payload, r.created_at))
        .collect()
}

parity_test!(stop_redelivery_round_trip, |store, tenant, db| {
    sessions(&store, &REDELIVERY_AGENTS, tenant).await;
    let entry =
        |agent: &str, n: u32, at: &str| (agent.to_owned(), payload(tenant, n), at.to_owned());

    assert!(load(&store).await.is_empty());
    store.clear_stop_redelivery(&aid("ab")).await.unwrap();
    store.clear_stop_redeliveries(&[]).await.unwrap();
    store
        .clear_stop_redeliveries(&[aid("ab"), aid("a2")])
        .await
        .unwrap();

    for agent in ["ab", "aC", "a1"] {
        store
            .set_stop_redelivery(&aid(agent), &payload(tenant, 1), T1)
            .await
            .unwrap();
    }
    assert_eq!(
        load(&store).await,
        [entry("a1", 1, T1), entry("aC", 1, T1), entry("ab", 1, T1)]
    );

    store
        .set_stop_redelivery(&aid("ab"), &payload(tenant, 2), T2)
        .await
        .unwrap();
    corrupt_payload(&store, db, tenant, "a1").await;
    assert_eq!(
        load(&store).await,
        [
            ("a1".to_owned(), serde_json::Value::Null, T1.to_owned()),
            entry("aC", 1, T1),
            entry("ab", 2, T2),
        ]
    );

    store.clear_stop_redelivery(&aid("aC")).await.unwrap();
    store
        .clear_stop_redeliveries(&[aid("ab"), aid("a2")])
        .await
        .unwrap();
    assert_eq!(
        load(&store).await,
        [("a1".to_owned(), serde_json::Value::Null, T1.to_owned())]
    );

    if tenant == "decoy" {
        for agent in REDELIVERY_AGENTS {
            store
                .set_stop_redelivery(&aid(agent), &payload(tenant, 3), T3)
                .await
                .unwrap();
        }
    }
});

// ---- delivery claims (batch G 2c) --------------------------------------------------------

/// A store of `tenant` opened as if by another process named `process`: its
/// own process-local gates, so only the database orders it against others.
async fn open_as_process(db: &TestDb, tenant: &str, process: &str) -> Store {
    Store::open_postgres_as_process(
        db.app.clone(),
        intent_store::postgres::TenantId::new(tenant).unwrap(),
        intent_store::postgres::PostgresOptions::default(),
        process,
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open tenant store as another process")
}

/// Two processes that both see one child's completion (two delivery loops,
/// or a daemon still delivering while another rehydrated the watch) must
/// not both wake the parent. Each claims the watch (or the delegation
/// group) in the database before it delivers: the claim names the tenant's
/// epoch and the claiming process, so the other process skips until the
/// claim is released, a newer epoch (a takeover) makes it stale, or a new
/// process opens the tenant (a restart without a lease). A slow owner never
/// loses its claim to a timer. `SQLite` has one process and
/// always grants the claim.
#[tokio::test]
async fn a_completion_is_delivered_by_one_process_at_a_time() {
    let Some(db) = crate::harness::test_db(false).await else {
        let (store, _dir) = crate::harness::sqlite_store().await;
        assert!(store.claim_completion_watch("w1").await.unwrap());
        assert!(store.claim_completion_watch("w1").await.unwrap());
        assert!(store.claim_delegation_group("g1").await.unwrap());
        return;
    };
    let tenant = "claims";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;
    seed_watches(&a, tenant).await;

    for id in ["w1", "g1"] {
        let claim = |s: &Store| {
            let s = s.clone();
            async move {
                if id.starts_with('w') {
                    s.claim_completion_watch(id).await.unwrap()
                } else {
                    s.claim_delegation_group(id).await.unwrap()
                }
            }
        };
        let release = |s: &Store| {
            let s = s.clone();
            async move {
                if id.starts_with('w') {
                    s.release_completion_watch_claim(id).await.unwrap();
                } else {
                    s.release_delegation_group_claim(id).await.unwrap();
                }
            }
        };
        assert!(claim(&a).await, "{id}: the first process claims");
        assert!(claim(&a).await, "{id}: its own claim is re-entrant");
        assert!(!claim(&b).await, "{id}: the other process skips");
        release(&b).await;
        assert!(
            !claim(&b).await,
            "{id}: a release by a non-holder is a no-op"
        );
        release(&a).await;
        assert!(claim(&b).await, "{id}: a released claim can be taken");
        assert!(!claim(&a).await, "{id}: now held by the other process");
    }

    // A restarted daemon (a new process; the tenant never leased, so the
    // epoch is unchanged) takes over the claims the old process left: its
    // first open of the tenant clears them.
    let restarted = open_as_process(&db, tenant, "process-restarted").await;
    assert!(restarted.claim_completion_watch("w1").await.unwrap());
    assert!(restarted.claim_delegation_group("g1").await.unwrap());
    assert!(!b.claim_completion_watch("w1").await.unwrap());

    // No persisted row (the write-through has not landed): nothing to
    // order against, so the in-process claim alone decides.
    assert!(a.claim_completion_watch("never-persisted").await.unwrap());
    assert!(a.claim_delegation_group("never-persisted").await.unwrap());

    // A takeover bumps the tenant's epoch: claims from the old epoch are
    // stale, so the new owner may deliver what the old one left claimed.
    let (owner, _lease) = Store::open_postgres_owned_as_process(
        db.app.clone(),
        intent_store::postgres::TenantId::new(tenant).unwrap(),
        intent_store::postgres::PostgresOptions::default(),
        "process-c",
        "process-c",
        std::time::Duration::from_secs(60),
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("claim the tenant");
    // The leased open cleared nothing: the restarted process's claims are
    // still on the rows, only stale by epoch.
    for (table, key, id) in [
        ("completion_watch", "id", "w1"),
        ("delegation_group", "group_id", "g1"),
    ] {
        let (epoch, token): (Option<i64>, Option<String>) =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT claim_epoch, claim_token FROM {table} WHERE tenant_id = $1 AND {key} = $2"
            )))
            .bind(tenant)
            .bind(id)
            .fetch_one(&db.admin)
            .await
            .unwrap();
        assert_eq!(epoch, Some(0), "{table} {id}: claimed before any lease");
        assert!(
            token.is_some(),
            "{table} {id}: a leased open clears no claim"
        );
    }
    assert!(owner.claim_completion_watch("w1").await.unwrap());
    assert!(owner.claim_delegation_group("g1").await.unwrap());

    // Within the owner's epoch its claim holds against a later leased open
    // that fails (the lease is live) and against an unleased process,
    // whose writes are fenced.
    assert!(Store::open_postgres_owned_as_process(
        db.app.clone(),
        intent_store::postgres::TenantId::new(tenant).unwrap(),
        intent_store::postgres::PostgresOptions::default(),
        "process-d",
        "process-d",
        std::time::Duration::from_secs(60),
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .is_err());
    assert!(owner.claim_completion_watch("w1").await.unwrap());
    let (epoch, token): (Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT claim_epoch, claim_token FROM completion_watch WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant)
    .bind("w1")
    .fetch_one(&db.admin)
    .await
    .unwrap();
    assert!(
        epoch.is_some_and(|e| e > 0) && token.is_some(),
        "the owner's claim stands"
    );
    assert!(
        matches!(
            b.claim_completion_watch("w2").await,
            Err(intent_store::Error::NotOwner(_))
        ),
        "a process that lost the tenant cannot claim at all"
    );
}
