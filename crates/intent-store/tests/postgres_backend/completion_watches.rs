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
use intent_store::{LeaseKeeper, PersistedCompletionWatch, PersistedDelegationGroup, Store};

use crate::harness::{parity_test, TestDb};

const CHIEF: &str = "__chief__";
const T0: &str = "2026-01-01T00:00:00.000Z";
const T1: &str = "2026-01-02T00:00:00.000Z";
const T2: &str = "2026-01-03T00:00:00.000Z";
const T3: &str = "2026-01-04T00:00:00.000Z";
const T4: &str = "2026-01-05T00:00:00.000Z";
const T5: &str = "2026-01-06T00:00:00.000Z";
const TIE: &str = "2026-01-07T00:00:00.000Z";
const CLAIM_TTL: std::time::Duration = std::time::Duration::from_secs(60);

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

/// The delivery claim on watch `id`, as a delivery holds it.
async fn watch_claim(store: &Store, id: &str) -> LeaseKeeper {
    store
        .claim_completion_watch(id, CLAIM_TTL)
        .await
        .unwrap()
        .expect("the watch is unclaimed")
}

/// The delivery claim on group `id`, as a delivery holds it.
async fn group_claim(store: &Store, id: &str) -> LeaseKeeper {
    store
        .claim_delegation_group(id, CLAIM_TTL)
        .await
        .unwrap()
        .expect("the group is unclaimed")
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
        .retire_completion_watch_after_delivery(
            "w1",
            &p1,
            &c1,
            None,
            T5,
            &watch_claim(&store, "w1").await,
        )
        .await
        .unwrap();
    store
        .settle_delegation_group_after_delivery(
            "g1",
            std::slice::from_ref(&c3),
            &group_claim(&store, "g1").await,
        )
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
        .retire_completion_watch_after_delivery(
            "w2",
            &p1,
            &c2,
            Some(&ident),
            T5,
            &watch_claim(&store, "w2").await,
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_completion_wake_delivery(&p1, &c2).await.unwrap(),
        Some(ident)
    );
    store
        .retire_completion_watch_after_delivery(
            "w2",
            &p1,
            &c2,
            Some("second"),
            T5,
            &watch_claim(&store, "w2").await,
        )
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
        .retire_completion_watch_after_delivery(
            "wc",
            &p2,
            &c2,
            None,
            T5,
            &watch_claim(&store, "wc").await,
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_completion_wake_delivery(&p2, &c2).await.unwrap(),
        None
    );
    expected.retain(|w| w.id != "w2" && w.id != "wc");

    // g1 keeps c3 (ungrouped) and a child it does not hold; w4 goes. g2 keeps none.
    store
        .settle_delegation_group_after_delivery(
            "g1",
            &[c3.clone(), aid("c-none")],
            &group_claim(&store, "g1").await,
        )
        .await
        .unwrap();
    store
        .settle_delegation_group_after_delivery("g2", &[], &group_claim(&store, "g2").await)
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

// ---- delivery claims ----------------------------------------------------------------

/// A store of `tenant` opened as if by another process named `process`: its
/// own process token and gates, so only the database orders it against
/// others.
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

/// The stores a claim test contends with: two processes on Postgres, or two
/// clones of one `SQLite` store (one process per database file).
async fn contenders(tenant: &str) -> (Option<TestDb>, Store, Store, Option<tempfile::TempDir>) {
    if let Some(db) = crate::harness::test_db(false).await {
        let a = open_as_process(&db, tenant, "process-a").await;
        let b = open_as_process(&db, tenant, "process-b").await;
        (Some(db), a, b, None)
    } else {
        let (store, dir) = crate::harness::sqlite_store().await;
        (None, store.clone(), store, Some(dir))
    }
}

/// Claims `id` (a watch for a `w…` id, else a group).
async fn claim(store: &Store, id: &str, ttl: std::time::Duration) -> Option<LeaseKeeper> {
    if id.starts_with('w') {
        store.claim_completion_watch(id, ttl).await.unwrap()
    } else {
        store.claim_delegation_group(id, ttl).await.unwrap()
    }
}

/// Lets the delivery claim on `id` lapse by the database clock, as its TTL
/// passing would.
async fn expire_claim(db: &TestDb, tenant: &str, id: &str) {
    let scope = if id.starts_with('w') {
        "completion-watch"
    } else {
        "delegation-group"
    };
    let expired = sqlx::query(
        "UPDATE lease SET expires_at = clock_timestamp() \
         WHERE tenant_id = $1 AND scope = $2 AND lease_key = $3 AND lease_id IS NOT NULL",
    )
    .bind(tenant)
    .bind(scope)
    .bind(id)
    .execute(&db.admin)
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(expired, 1, "{scope}/{id} is held");
}

/// Every contender (half in each process) starts at one barrier on a
/// multi-thread runtime; exactly one claims each watch and group, and keeps
/// it: the claim is not re-entrant, a non-holder cannot take it, and the
/// holder's release hands it on at once. A watch and a group of one id are
/// separate claims, and a claim needs no persisted row (the write-through
/// may not have landed). Both backends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_contender_claims_a_delivery_until_it_releases() {
    const RACERS: usize = 8;
    let (db, a, b, _dir) = contenders("claims").await;
    if db.is_some() {
        seed_watches(&a, "claims").await;
    }

    for id in ["w1", "g1", "never-persisted"] {
        let start = std::sync::Arc::new(tokio::sync::Barrier::new(RACERS));
        let racers: Vec<_> = (0..RACERS)
            .map(|i| {
                let store = if i % 2 == 0 { a.clone() } else { b.clone() };
                let start = std::sync::Arc::clone(&start);
                tokio::spawn(async move {
                    start.wait().await;
                    (i % 2, claim(&store, id, CLAIM_TTL).await)
                })
            })
            .collect();
        let mut winners = Vec::new();
        for racer in racers {
            let (side, won) = racer.await.unwrap();
            if let Some(keeper) = won {
                winners.push((side, keeper));
            }
        }
        assert_eq!(winners.len(), 1, "{id}: exactly one contender claims");
        let (side, held) = winners.pop().unwrap();
        let (holder, other) = if side == 0 { (&a, &b) } else { (&b, &a) };
        assert!(
            claim(holder, id, CLAIM_TTL).await.is_none(),
            "{id}: not re-entrant"
        );
        assert!(
            claim(other, id, CLAIM_TTL).await.is_none(),
            "{id}: the other process waits"
        );
        held.release(holder).await;
        let next = claim(other, id, CLAIM_TTL)
            .await
            .unwrap_or_else(|| panic!("{id}: a released claim is free at once"));
        assert!(next.token() > 1, "{id}: the next holder's token is higher");
        next.release(other).await;
    }
    let watch = a.claim_completion_watch("x1", CLAIM_TTL).await.unwrap();
    let group = a.claim_delegation_group("x1", CLAIM_TTL).await.unwrap();
    assert!(
        watch.is_some() && group.is_some(),
        "a watch and a group of one id are separate claims"
    );
}

/// A holder that crashed mid-delivery (its keeper gone, nothing released)
/// keeps the claim until its TTL passes by the database clock: no other
/// process, not even a restarted daemon (whose first open of the tenant no
/// longer clears claims), can deliver meanwhile. Once it lapsed, the
/// processes that see the completion race for it and exactly one claims
/// it; that one retires the watch (and the group) under its claim, and the
/// claim's lease row goes with the work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_holders_claim_lapses_and_one_process_delivers() {
    const RACERS: usize = 6;
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "crashed-claims";
    let crashed = open_as_process(&db, tenant, "process-crashed").await;
    seed_watches(&crashed, tenant).await;
    sessions(&crashed, &["p1", "c1", "c3"], tenant).await;

    for id in ["w1", "g1"] {
        // The crash: the keeper (and its renewer) die without a release.
        drop(claim(&crashed, id, CLAIM_TTL).await.expect("claim"));
    }
    let restarted = open_as_process(&db, tenant, "process-restarted").await;
    for id in ["w1", "g1"] {
        assert!(
            claim(&restarted, id, CLAIM_TTL).await.is_none(),
            "{id}: a crashed holder's claim holds until its TTL passed"
        );
        expire_claim(&db, tenant, id).await;
    }

    let mut processes = vec![restarted];
    for i in 1..RACERS {
        processes.push(open_as_process(&db, tenant, &format!("process-{i}")).await);
    }
    for id in ["w1", "g1"] {
        let start = std::sync::Arc::new(tokio::sync::Barrier::new(RACERS));
        let racers: Vec<_> = processes
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, store)| {
                let start = std::sync::Arc::clone(&start);
                tokio::spawn(async move {
                    start.wait().await;
                    claim(&store, id, CLAIM_TTL).await.map(|keeper| (i, keeper))
                })
            })
            .collect();
        let mut winners = Vec::new();
        for racer in racers {
            winners.extend(racer.await.unwrap());
        }
        assert_eq!(winners.len(), 1, "{id}: exactly one process takes it over");
        let (i, keeper) = winners.pop().unwrap();
        assert!(
            keeper.token() >= 2,
            "{id}: a newer holder than the crashed one"
        );
        let delivered_by = format!("process-{i}");
        if id == "w1" {
            processes[i]
                .retire_completion_watch_after_delivery(
                    "w1",
                    &aid("p1"),
                    &aid("c1"),
                    Some(&delivered_by),
                    T5,
                    &keeper,
                )
                .await
                .unwrap();
        } else {
            processes[i]
                .settle_delegation_group_after_delivery("g1", &[], &keeper)
                .await
                .unwrap();
        }
    }
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM lease WHERE tenant_id = $1 \
         AND scope IN ('completion-watch', 'delegation-group')",
    )
    .bind(tenant)
    .fetch_one(&db.admin)
    .await
    .unwrap();
    assert_eq!(rows, 0, "settled work leaves no lease row behind");
    let store = &processes[0];
    assert!(store
        .get_completion_wake_delivery(&aid("p1"), &aid("c1"))
        .await
        .unwrap()
        .is_some_and(|by| by.starts_with("process-")));
    let left: Vec<String> = store
        .list_completion_watches()
        .await
        .unwrap()
        .into_iter()
        .map(|w| w.id)
        .collect();
    assert!(!left.contains(&"w1".to_owned()), "w1 retired: {left:?}");
    assert!(
        store
            .list_undelivered_groups(&wid(CHIEF))
            .await
            .unwrap()
            .iter()
            .all(|g| g.group_id != "g1"),
        "g1 settled"
    );
}

/// A holder whose claim lapsed and was taken over writes nothing: its
/// retirement and settlement fail `LeaseLost` and leave the watch, the
/// group and the delivery marker as they were, while the new holder's
/// succeed. A claim of other work does not cover the row at all.
#[tokio::test]
async fn a_holder_that_lost_its_claim_writes_nothing() {
    let Some(db) = crate::harness::test_db(false).await else {
        let (store, _dir) = crate::harness::sqlite_store().await;
        let other = watch_claim(&store, "w-other").await;
        assert!(matches!(
            store
                .retire_completion_watch_after_delivery(
                    "w1",
                    &aid("p1"),
                    &aid("c1"),
                    None,
                    T5,
                    &other
                )
                .await,
            Err(intent_store::Error::InvalidParams(_))
        ));
        return;
    };
    let tenant = "stale-claims";
    let stale = open_as_process(&db, tenant, "process-stale").await;
    let fresh = open_as_process(&db, tenant, "process-fresh").await;
    seed_watches(&stale, tenant).await;
    sessions(&stale, &["p1", "c1", "c3"], tenant).await;

    let stale_watch = watch_claim(&stale, "w1").await;
    let stale_group = group_claim(&stale, "g1").await;
    assert!(matches!(
        stale
            .retire_completion_watch_after_delivery(
                "w1",
                &aid("p1"),
                &aid("c1"),
                None,
                T5,
                &stale_group
            )
            .await,
        Err(intent_store::Error::InvalidParams(_))
    ));
    for id in ["w1", "g1"] {
        expire_claim(&db, tenant, id).await;
    }
    let fresh_watch = watch_claim(&fresh, "w1").await;
    let fresh_group = group_claim(&fresh, "g1").await;

    assert!(matches!(
        stale
            .retire_completion_watch_after_delivery(
                "w1",
                &aid("p1"),
                &aid("c1"),
                Some("stale"),
                T5,
                &stale_watch,
            )
            .await,
        Err(intent_store::Error::LeaseLost(_))
    ));
    assert!(matches!(
        stale
            .settle_delegation_group_after_delivery("g1", &[], &stale_group)
            .await,
        Err(intent_store::Error::LeaseLost(_))
    ));
    assert_eq!(
        fresh
            .get_completion_wake_delivery(&aid("p1"), &aid("c1"))
            .await
            .unwrap(),
        None,
        "the stale retirement wrote no marker"
    );
    assert!(fresh
        .list_completion_watches()
        .await
        .unwrap()
        .iter()
        .any(|w| w.id == "w1"));
    assert!(fresh
        .list_undelivered_groups(&wid(CHIEF))
        .await
        .unwrap()
        .iter()
        .any(|g| g.group_id == "g1"));

    fresh
        .retire_completion_watch_after_delivery(
            "w1",
            &aid("p1"),
            &aid("c1"),
            Some("fresh"),
            T5,
            &fresh_watch,
        )
        .await
        .unwrap();
    fresh
        .settle_delegation_group_after_delivery("g1", &[], &fresh_group)
        .await
        .unwrap();
    assert_eq!(
        fresh
            .get_completion_wake_delivery(&aid("p1"), &aid("c1"))
            .await
            .unwrap()
            .as_deref(),
        Some("fresh")
    );
}

fn wake_row(id: &str, agent: &str) -> intent_store::AgentQueueRow {
    intent_store::AgentQueueRow {
        id: id.into(),
        agent_id: aid(agent),
        position: 0,
        payload: serde_json::json!({ "content": format!("wake {id}") }),
        created_at: T5.into(),
        turn_id: id.into(),
    }
}

async fn wake_counts(store: &Store, parent: &str) -> (usize, usize) {
    let transcript = store
        .get_agent_session_with_transcript(&aid(parent))
        .await
        .unwrap()
        .messages
        .len();
    let queued = store
        .load_all_agent_queues()
        .await
        .unwrap()
        .iter()
        .filter(|row| row.agent_id == aid(parent))
        .count();
    (transcript, queued)
}

/// A parent wake sent under a delivery claim (its transcript row, or its
/// queue row for a busy parent) guards the claim's lease in its own
/// transaction: once the claim lapsed and another process took it over,
/// the stale holder's wake fails with `LeaseLost` and writes nothing, and
/// the new holder's lands. On `SQLite` (one process) a held claim writes.
#[tokio::test]
async fn a_wake_sent_under_a_lost_claim_writes_nothing() {
    let wake = serde_json::json!([{ "type": "text", "text": "child finished" }]);
    let Some(db) = crate::harness::test_db(false).await else {
        let (store, _dir) = crate::harness::sqlite_store().await;
        sessions(&store, &["p1"], "sqlite").await;
        let claim = watch_claim(&store, "w1").await;
        store
            .append_agent_message_guarded(
                &aid("p1"),
                "completion-wake:w1",
                "user",
                &wake,
                None,
                T5,
                intent_store::UsageMessageOrigin::Excluded,
                &claim,
            )
            .await
            .unwrap();
        store
            .sync_agent_queue_guarded(&aid("p1"), &[wake_row("q1", "p1")], &[], &claim)
            .await
            .unwrap();
        assert_eq!(wake_counts(&store, "p1").await, (1, 1));
        return;
    };
    let tenant = "stale-wakes";
    let stale = open_as_process(&db, tenant, "process-stale").await;
    let fresh = open_as_process(&db, tenant, "process-fresh").await;
    sessions(&stale, &["p1"], tenant).await;
    let stale_claim = watch_claim(&stale, "w1").await;
    expire_claim(&db, tenant, "w1").await;
    let fresh_claim = watch_claim(&fresh, "w1").await;

    assert!(matches!(
        stale
            .append_agent_message_guarded(
                &aid("p1"),
                "completion-wake:w1",
                "user",
                &wake,
                None,
                T5,
                intent_store::UsageMessageOrigin::Excluded,
                &stale_claim,
            )
            .await,
        Err(intent_store::Error::LeaseLost(_))
    ));
    assert!(matches!(
        stale
            .sync_agent_queue_guarded(&aid("p1"), &[wake_row("q1", "p1")], &[], &stale_claim)
            .await,
        Err(intent_store::Error::LeaseLost(_))
    ));
    assert_eq!(
        wake_counts(&fresh, "p1").await,
        (0, 0),
        "the stale holder wrote no wake"
    );

    fresh
        .append_agent_message_guarded(
            &aid("p1"),
            "completion-wake:w1",
            "user",
            &wake,
            None,
            T5,
            intent_store::UsageMessageOrigin::Excluded,
            &fresh_claim,
        )
        .await
        .unwrap();
    fresh
        .sync_agent_queue_guarded(&aid("p1"), &[wake_row("q1", "p1")], &[], &fresh_claim)
        .await
        .unwrap();
    assert_eq!(
        wake_counts(&stale, "p1").await,
        (1, 1),
        "the holder's wake lands"
    );
}

/// A tenant lease takeover leaves a live delivery claim alone: the new
/// tenant owner cannot claim the watch or group the old process is
/// delivering, and the old holder's keeper does not fence. The old
/// process's settlement still needs the tenant (`NotOwner`), so it
/// releases, and then the new owner claims and settles.
#[tokio::test]
async fn a_tenant_takeover_does_not_reset_a_live_delivery_claim() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "takeover-claims";
    let old = open_as_process(&db, tenant, "process-old").await;
    seed_watches(&old, tenant).await;
    sessions(&old, &["p1", "c1", "c3"], tenant).await;
    let old_watch = watch_claim(&old, "w1").await;
    let old_group = group_claim(&old, "g1").await;

    let (owner, _tenant_lease) = Store::open_postgres_owned_as_process(
        db.app.clone(),
        intent_store::postgres::TenantId::new(tenant).unwrap(),
        intent_store::postgres::PostgresOptions::default(),
        "process-new",
        "process-new",
        std::time::Duration::from_secs(60),
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("claim the tenant");
    for id in ["w1", "g1"] {
        assert!(
            claim(&owner, id, CLAIM_TTL).await.is_none(),
            "{id}: the takeover did not free the delivery claim"
        );
    }
    assert!(!old_watch.is_fired() && !old_group.is_fired());

    let refused = old
        .retire_completion_watch_after_delivery("w1", &aid("p1"), &aid("c1"), None, T5, &old_watch)
        .await;
    assert!(
        matches!(refused, Err(intent_store::Error::NotOwner(_))),
        "the old process lost the tenant: {refused:?}"
    );
    assert!(
        claim(&owner, "w1", CLAIM_TTL).await.is_none(),
        "a refused settlement keeps the claim"
    );
    old_watch.release(&old).await;
    old_group.release(&old).await;

    let watch = watch_claim(&owner, "w1").await;
    let group = group_claim(&owner, "g1").await;
    owner
        .retire_completion_watch_after_delivery("w1", &aid("p1"), &aid("c1"), None, T5, &watch)
        .await
        .unwrap();
    owner
        .settle_delegation_group_after_delivery("g1", &[], &group)
        .await
        .unwrap();
}
