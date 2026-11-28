//! One agent's send queue on a Postgres tenant shared by two processes
//! (stateless replicas groundwork): each `Services` runs on its own unclaimed
//! store of the tenant, as two daemons would. Entries enqueued on either are
//! seen by both in order, and draining from both delivers each entry once.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::collections::HashSet;
use std::sync::Arc;

use intent_core::{AgentId, AgentSession, AgentStatus, MessageOrigin, WorkspaceId};
use intent_services::events::EventBus;
use intent_services::{AgentManager, BusEventSink, CompletionClassifyPark, Services};
use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::{OrchestratorOnly, Store};

async fn pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("INTENT_STORE_TEST_PG_URL") else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_PG").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_PG=1 but INTENT_STORE_TEST_PG_URL is unset"
        );
        eprintln!("skipping: INTENT_STORE_TEST_PG_URL is unset");
        return None;
    };
    let pool = postgres::connect(&url, 8).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    Some(pool)
}

/// An unclaimed store of `tenant` with its own process-local state, as if
/// opened by the daemon `process`.
async fn open(pool: &PgPool, tenant: &str, process: &str) -> Store {
    Store::open_postgres_as_process(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        PostgresOptions::default(),
        process,
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open the tenant store")
}

async fn queue_contents(services: &Services, agent: &AgentId) -> Vec<String> {
    let queue = services
        .agent_get_queue_op_for_test(agent)
        .await
        .expect("agent.getQueue");
    queue["queue"]
        .as_array()
        .unwrap_or_else(|| panic!("no queue in {queue}"))
        .iter()
        .map(|entry| entry["content"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_share_one_agent_queue_and_drain_each_entry_once() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "shared-queue-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let (store_a, store_b) = (
        open(&pool, &tenant, "process-a").await,
        open(&pool, &tenant, "process-b").await,
    );
    let agent = AgentId("agent-shared".into());
    let now = intent_core::now_iso();
    store_a
        .insert_agent_session(&AgentSession {
            id: agent.clone(),
            workspace_id: WorkspaceId("__chief__".into()),
            name: "shared".into(),
            status: AgentStatus::Idle,
            created_at: now.clone(),
            updated_at: now,
            ..AgentSession::default()
        })
        .await
        .expect("seed the agent");
    let a = Services::new_isolated(store_a);
    let b = Services::new_isolated(store_b);

    // Enqueue alternately on both, each persisted by its write-through.
    let mut ids = Vec::new();
    for (services, text) in [(&a, "a1"), (&b, "b1"), (&a, "a2"), (&b, "b2")] {
        let (entry, _) = services.enqueue_message_for_test(
            &agent,
            text.into(),
            None,
            None,
            None,
            false,
            MessageOrigin::User,
        );
        services.persist_queue_snapshot_for_test(&agent).await;
        ids.push(entry.id);
    }
    for services in [&a, &b] {
        assert_eq!(
            queue_contents(services, &agent).await,
            ["a1", "b1", "a2", "b2"],
            "each process sees the whole queue in enqueue order"
        );
    }

    // Drain from both at once until both queues are empty: every entry is
    // delivered exactly once, whichever process pops it.
    let mut delivered = Vec::new();
    let mut lost = 0;
    loop {
        let (from_a, from_b) = tokio::join!(
            a.claim_next_queued_message_for_test(&agent),
            b.claim_next_queued_message_for_test(&agent),
        );
        if from_a.is_none() && from_b.is_none() {
            break;
        }
        for (id, claims_lost) in [from_a, from_b].into_iter().flatten() {
            if claims_lost.contains(&id) {
                lost += 1;
            } else {
                delivered.push(id);
            }
        }
    }
    let unique: HashSet<&String> = delivered.iter().collect();
    assert_eq!(
        unique.len(),
        delivered.len(),
        "an entry was delivered twice: {delivered:?}"
    );
    assert_eq!(
        unique,
        ids.iter().collect::<HashSet<_>>(),
        "every entry is delivered"
    );
    eprintln!("delivered {delivered:?}; {lost} pops lost to the other process");
    for services in [&a, &b] {
        assert!(queue_contents(services, &agent).await.is_empty());
    }
}

/// Runs `sql` on `pool`, again while Postgres reports a deadlock: tests in
/// other processes add and drop their own fault triggers on the same table.
async fn ddl(pool: &PgPool, sql: String, what: &str) {
    for _ in 0..20 {
        match sqlx::raw_sql(sqlx::AssertSqlSafe(sql.clone()))
            .execute(pool)
            .await
        {
            Ok(_) => return,
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("40P01") => {}
            Err(e) => panic!("{what}: {e:?}"),
        }
    }
    panic!("{what}: deadlocked 20 times");
}

/// Makes every agent queue write of `tenant` fail (the queue version row is
/// written by every sync), until the returned name is passed to `heal`.
async fn break_queue_writes(pool: &PgPool, tenant: &str) -> String {
    let name = format!("fail_queue_write_{}", uuid::Uuid::new_v4().simple());
    ddl(
        pool,
        format!(
            "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
             IF NEW.tenant_id = '{tenant}' THEN RAISE EXCEPTION 'injected queue write failure'; END IF; \
             RETURN NEW; END $$"
        ),
        "create the fault function",
    )
    .await;
    ddl(
        pool,
        format!(
            "CREATE TRIGGER {name} BEFORE INSERT OR UPDATE ON agent_queue_version \
             FOR EACH ROW EXECUTE FUNCTION {name}()"
        ),
        "create the fault trigger",
    )
    .await;
    name
}

async fn heal(pool: &PgPool, name: &str) {
    ddl(
        pool,
        format!("DROP TRIGGER {name} ON agent_queue_version"),
        "drop the fault trigger",
    )
    .await;
    ddl(
        pool,
        format!("DROP FUNCTION {name}()"),
        "drop the fault function",
    )
    .await;
}

/// A drain whose claim write fails delivers nothing: the entry stays queued
/// in memory and in the store (another process still sees it), and a later
/// drain delivers it exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_claim_write_delivers_nothing_and_a_later_drain_delivers_once() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "claim-fault-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let store_a = open(&pool, &tenant, "process-a").await;
    let agent = AgentId("agent-fault".into());
    let now = intent_core::now_iso();
    store_a
        .insert_agent_session(&AgentSession {
            id: agent.clone(),
            workspace_id: WorkspaceId("__chief__".into()),
            name: "fault".into(),
            status: AgentStatus::Idle,
            created_at: now.clone(),
            updated_at: now,
            ..AgentSession::default()
        })
        .await
        .expect("seed the agent");
    let a = Services::new_isolated(store_a);
    let (entry, _) = a.enqueue_message_for_test(
        &agent,
        "only".into(),
        None,
        None,
        None,
        false,
        MessageOrigin::User,
    );
    a.persist_queue_snapshot_for_test(&agent).await;

    let fault = break_queue_writes(&pool, &tenant).await;
    let claimed = a.claim_next_queued_message_for_test(&agent).await;
    heal(&pool, &fault).await;
    assert_eq!(claimed, None, "a failed claim must not be delivered");
    assert_eq!(
        queue_contents(&a, &agent).await,
        ["only"],
        "still queued here"
    );
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    assert_eq!(queue_contents(&b, &agent).await, ["only"], "still stored");

    assert_eq!(
        a.claim_next_queued_message_for_test(&agent).await,
        Some((entry.id, Vec::new())),
        "the next drain delivers it"
    );
    assert_eq!(
        b.claim_next_queued_message_for_test(&agent).await,
        None,
        "and nobody delivers it again"
    );
    for services in [&a, &b] {
        assert!(queue_contents(services, &agent).await.is_empty());
    }
}

/// An entry enqueued between the claim's snapshot and its batch pop (the
/// enqueue does not take the persist gate) survives a failed claim: the
/// batch is put back as popped, so `c` is restored with `a` and `b`, its
/// write-through persists all three, and draining delivers each once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_enqueued_during_a_failed_batch_claim_is_kept_and_drains_once() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "claim-race-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let store_a = open(&pool, &tenant, "process-a").await;
    let agent = AgentId("agent-race".into());
    let now = intent_core::now_iso();
    store_a
        .insert_agent_session(&AgentSession {
            id: agent.clone(),
            workspace_id: WorkspaceId("__chief__".into()),
            name: "race".into(),
            status: AgentStatus::Idle,
            created_at: now.clone(),
            updated_at: now,
            ..AgentSession::default()
        })
        .await
        .expect("seed the agent");
    let a = Services::new_isolated(store_a);
    let enqueue = |text: &str| {
        a.enqueue_message_for_test(
            &agent,
            text.into(),
            None,
            None,
            None,
            false,
            MessageOrigin::User,
        )
        .0
        .id
    };
    let mut ids = vec![enqueue("a"), enqueue("b")];
    a.persist_queue_snapshot_for_test(&agent).await;

    let fault = break_queue_writes(&pool, &tenant).await;
    let mut enqueued = None;
    let claimed = a
        .claim_queue_batch_for_test(&agent, 2, || enqueued = Some(enqueue("c")))
        .await;
    heal(&pool, &fault).await;
    assert_eq!(claimed, None, "a failed claim must not be delivered");
    ids.push(enqueued.expect("c was enqueued inside the claim window"));
    assert_eq!(
        queue_contents(&a, &agent).await,
        ["a", "b", "c"],
        "c kept, in place"
    );

    // c's own write-through, delayed past the failed claim, persists all.
    a.persist_queue_snapshot_for_test(&agent).await;
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    assert_eq!(
        queue_contents(&b, &agent).await,
        ["a", "b", "c"],
        "all stored"
    );

    let mut delivered = Vec::new();
    loop {
        let (from_a, from_b) = tokio::join!(
            a.claim_next_queued_message_for_test(&agent),
            b.claim_next_queued_message_for_test(&agent),
        );
        if from_a.is_none() && from_b.is_none() {
            break;
        }
        for (id, lost) in [from_a, from_b].into_iter().flatten() {
            if !lost.contains(&id) {
                delivered.push(id);
            }
        }
    }
    delivered.sort();
    ids.sort();
    assert_eq!(
        delivered, ids,
        "each entry, c included, is delivered exactly once"
    );
}

/// A fresh tenant name with `prefix`.
fn tenant_name(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// Seeds an idle agent session `agent` in the chief workspace.
async fn seed_agent(store: &Store, agent: &AgentId) {
    let now = intent_core::now_iso();
    store
        .insert_agent_session(&AgentSession {
            id: agent.clone(),
            workspace_id: WorkspaceId("__chief__".into()),
            name: agent.0.clone(),
            status: AgentStatus::Idle,
            created_at: now.clone(),
            updated_at: now,
            ..AgentSession::default()
        })
        .await
        .expect("seed the agent");
}

/// Enqueues `text` (user origin) on `services` and writes it through.
async fn enqueue(
    services: &Services,
    agent: &AgentId,
    text: &str,
    metadata: Option<serde_json::Value>,
) -> String {
    let (entry, _) = services.enqueue_message_for_test(
        agent,
        text.into(),
        None,
        None,
        metadata,
        false,
        MessageOrigin::User,
    );
    services.persist_queue_snapshot_for_test(agent).await;
    entry.id
}

fn wait() -> std::time::Duration {
    intentd_test_support::test_timeout(std::time::Duration::from_secs(30))
}

/// The user rows in `agent`'s transcript.
async fn user_rows(store: &Store, agent: &AgentId) -> usize {
    store
        .get_agent_session_with_transcript(agent)
        .await
        .expect("transcript")
        .messages
        .iter()
        .filter(|m| m.role == "user")
        .count()
}

/// A process that crashes between its drain's claim and the delivery does
/// not lose the entry: its claimed row stays stored, the restarted daemon
/// (an unleased open of the tenant) puts the claim back, and the entry is
/// then delivered exactly once by the processes left. Before claimed rows,
/// the claim deleted the row and the entry was gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_between_claim_and_send_redelivers_once() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant_name("claim-crash");
    let agent = AgentId("agent-crash".into());
    let store_a = open(&pool, &tenant, "process-a").await;
    seed_agent(&store_a, &agent).await;
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    let id = {
        let a = Services::new_isolated(store_a);
        let id = enqueue(&a, &agent, "only", None).await;
        let (claimed, lost, draining) = a
            .claim_next_queued_message_held_for_test(&agent)
            .await
            .expect("A claims the entry");
        assert_eq!((claimed.as_str(), lost.len()), (id.as_str(), 0));
        assert!(
            queue_contents(&b, &agent).await.is_empty(),
            "no one else sees an entry being delivered"
        );
        // A dies before its delivery: the claim is never ended.
        std::mem::forget(draining);
        id
    };

    let restarted = Services::new_isolated(open(&pool, &tenant, "process-a-restarted").await);
    assert_eq!(
        queue_contents(&restarted, &agent).await,
        ["only"],
        "the restarted daemon queues the entry again"
    );
    assert_eq!(
        queue_contents(&b, &agent).await,
        ["only"],
        "and so does the survivor"
    );
    let (from_b, from_restarted) = tokio::join!(
        b.claim_next_queued_message_for_test(&agent),
        restarted.claim_next_queued_message_for_test(&agent),
    );
    let delivered: Vec<String> = [from_b, from_restarted]
        .into_iter()
        .flatten()
        .filter(|(popped, lost)| !lost.contains(popped))
        .map(|(popped, _)| popped)
        .collect();
    assert_eq!(delivered, [id], "delivered exactly once");
    for services in [&b, &restarted] {
        assert!(queue_contents(services, &agent).await.is_empty());
    }
}

/// `agent.removeQueuedMessage` on process B removes an entry process A
/// enqueued, which B never held in memory: the removal is read from and
/// written to the store, so A no longer drains it. An agent's own removal
/// of an entry another process is delivering reads as not found.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_from_another_process_removes_the_entry() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant_name("cancel");
    let agent = AgentId("agent-cancel".into());
    let store_a = open(&pool, &tenant, "process-a").await;
    seed_agent(&store_a, &agent).await;
    let a = Services::new_isolated(store_a);
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    let first = enqueue(&a, &agent, "first", None).await;
    let second = enqueue(&a, &agent, "second", None).await;

    b.agent_remove_queued_message_op_for_test(agent.clone(), first.clone())
        .await
        .expect("B removes A's entry");
    for services in [&a, &b] {
        assert_eq!(queue_contents(services, &agent).await, ["second"]);
    }
    assert_eq!(
        a.claim_next_queued_message_for_test(&agent).await,
        Some((second, Vec::new())),
        "A drains only what is left"
    );

    // The MCP removal: an entry A is delivering is not B's to remove; one
    // only A held in memory is.
    let sender = AgentId("sender".into());
    let from_sender = Some(serde_json::json!({ "fromAgentId": sender.0 }));
    let busy = enqueue(&a, &agent, "busy", from_sender.clone()).await;
    let idle = enqueue(&a, &agent, "idle", from_sender).await;
    let (claimed, _, draining) = a
        .claim_next_queued_message_held_for_test(&agent)
        .await
        .expect("A claims the head");
    assert_eq!(claimed, busy);
    let refused = b
        .agent_remove_queued_message_owned_op_for_test(agent.clone(), busy, sender.clone())
        .await;
    assert!(
        matches!(refused, Err(intent_core::Error::NotFound(_))),
        "{refused:?}"
    );
    b.agent_remove_queued_message_owned_op_for_test(agent.clone(), idle, sender)
        .await
        .expect("B removes the waiting entry");
    drop(draining);
    for services in [&a, &b] {
        assert!(queue_contents(services, &agent).await.is_empty());
    }
}

/// Two processes enqueueing at once never assign one submission order
/// twice: each puts its own queue order tag under its count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_never_assign_the_same_order() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant_name("orders");
    let agent = AgentId("agent-orders".into());
    let store_a = open(&pool, &tenant, "process-a").await;
    seed_agent(&store_a, &agent).await;
    let a = Services::new_isolated(store_a);
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    let mut orders = Vec::new();
    for round in 0..4 {
        for services in [&a, &b] {
            let (entry, _) = services.enqueue_message_for_test(
                &agent,
                format!("message {round}"),
                None,
                None,
                None,
                false,
                MessageOrigin::User,
            );
            orders.push(entry.submission_order);
        }
    }
    let unique: HashSet<u64> = orders.iter().copied().collect();
    assert_eq!(unique.len(), orders.len(), "orders repeat: {orders:?}");
    // Each process's own orders still rise.
    for first in [0, 1] {
        let own: Vec<u64> = orders.iter().skip(first).step_by(2).copied().collect();
        assert!(own.windows(2).all(|w| w[0] < w[1]), "{own:?}");
    }
}

/// `agent.sendQueuedMessageNow` on process B sends an entry process A
/// enqueued (read from the store), once: A's drain no longer delivers it,
/// and an entry A is delivering reads as not found on B.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_now_from_another_process_claims_the_entry() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant_name("send-now");
    let agent = AgentId("agent-send-now".into());
    let store_a = open(&pool, &tenant, "process-a").await;
    seed_agent(&store_a, &agent).await;
    let a = Services::new_isolated(store_a.clone());
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    let first = enqueue(&a, &agent, "x", None).await;
    let second = enqueue(&a, &agent, "y", None).await;

    b.agent_send_queued_message_now_op_for_test(agent.clone(), first)
        .await
        .expect("B sends A's entry");
    assert_eq!(user_rows(&store_a, &agent).await, 1);
    assert_eq!(
        a.claim_next_queued_message_for_test(&agent).await,
        Some((second, Vec::new())),
        "A's drain skips the sent entry"
    );

    let busy = enqueue(&a, &agent, "z", None).await;
    let (_, _, draining) = a
        .claim_next_queued_message_held_for_test(&agent)
        .await
        .expect("A claims z");
    let refused = b
        .agent_send_queued_message_now_op_for_test(agent.clone(), busy)
        .await;
    assert!(
        matches!(refused, Err(intent_core::Error::InvalidParams(_))),
        "{refused:?}"
    );
    drop(draining);
    assert_eq!(user_rows(&store_a, &agent).await, 1, "nothing sent twice");
}

/// A drain whose claim write fails is retried after a backoff without any
/// other trigger: once the store is back, the retry delivers the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_claim_write_retries_the_drain() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant_name("claim-retry");
    let agent = AgentId("agent-retry".into());
    let store_a = open(&pool, &tenant, "process-a").await;
    seed_agent(&store_a, &agent).await;
    let park = Arc::new(CompletionClassifyPark::default());
    let a = Services::new_isolated(store_a.clone())
        .with_queue_claim_retry_park_for_test(std::time::Duration::from_millis(1), park.clone());
    let manager = Arc::new(AgentManager::new(
        a.clone(),
        Arc::new(BusEventSink::new(EventBus::new(store_a.clone()))),
        4,
    ));
    a.attach_agent_manager(&manager);
    enqueue(&a, &agent, "retry me", None).await;

    let fault = break_queue_writes(&pool, &tenant).await;
    assert_eq!(
        a.claim_next_queued_message_for_test(&agent).await,
        None,
        "the claim write fails"
    );
    tokio::time::timeout(wait(), park.entered.notified())
        .await
        .expect("a retry is scheduled");
    heal(&pool, &fault).await;
    park.release.notify_one();
    tokio::time::timeout(wait(), a.queue_claim_retried_for_test())
        .await
        .expect("the retry drains");
    assert_eq!(
        user_rows(&store_a, &agent).await,
        1,
        "the retried drain delivered the entry"
    );
}

/// A pop that is claimed only later (the recovery-send redrive, the
/// worker's raced pop) keeps its entry stored across a write-through of the
/// same queue in between: another process still sees it, and of the two
/// exactly one delivers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_between_a_pop_and_its_claim_keeps_the_entry_stored() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant_name("late-claim");
    let agent = AgentId("agent-late-claim".into());
    let store_a = open(&pool, &tenant, "process-a").await;
    seed_agent(&store_a, &agent).await;
    let a = Services::new_isolated(store_a);
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    let x = enqueue(&a, &agent, "x", None).await;

    let mut from_b = None;
    let from_a = a
        .pop_then_claim_for_test(&agent, async {
            // A writes the queue while x is popped but not yet claimed.
            enqueue(&a, &agent, "y", None).await;
            assert_eq!(
                queue_contents(&b, &agent).await,
                ["x", "y"],
                "x is still stored"
            );
            from_b = b.claim_next_queued_message_for_test(&agent).await;
        })
        .await
        .expect("A pops x");
    assert_eq!(from_b, Some((x.clone(), Vec::new())), "B claims x first");
    assert_eq!(from_a, (x.clone(), vec![x]), "so A's late claim is lost");
    assert_eq!(queue_contents(&a, &agent).await, ["y"]);
}

/// The other order of the window above: B claims x (and is still
/// delivering it) before A writes the queue. A's write merges x away and
/// moves A's view of the queue past B's claim, so A's late claim can no
/// longer see the claim through its own base; it must still lose x.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_claim_loses_an_entry_another_process_claimed_before_a_write() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant_name("late-claim-held");
    let agent = AgentId("agent-late-claim-held".into());
    let store_a = open(&pool, &tenant, "process-a").await;
    seed_agent(&store_a, &agent).await;
    let a = Services::new_isolated(store_a);
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    let x = enqueue(&a, &agent, "x", None).await;

    let mut held = None;
    let from_a = a
        .pop_then_claim_for_test(&agent, async {
            let (claimed, lost, draining) = b
                .claim_next_queued_message_held_for_test(&agent)
                .await
                .expect("B claims x");
            assert_eq!((claimed.as_str(), lost.len()), (x.as_str(), 0));
            held = Some(draining);
            // A writes the queue while B is still delivering x.
            enqueue(&a, &agent, "y", None).await;
        })
        .await
        .expect("A pops x");
    assert_eq!(from_a, (x.clone(), vec![x]), "A's late claim of x is lost");
    drop(held);
    assert_eq!(queue_contents(&a, &agent).await, ["y"]);
    assert_eq!(queue_contents(&b, &agent).await, ["y"]);
}
