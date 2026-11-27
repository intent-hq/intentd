//! Dedupe and idempotency gates on a Postgres tenant shared by two
//! processes: each `Services` runs on its own store of the tenant, as two
//! daemons would. A same-key call in the second process runs nothing a
//! second time: it waits for the first and replays its result.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::time::Duration;

use intent_core::{
    with_caller, AgentId, AgentSession, AgentStatus, Caller, Workspace, WorkspaceApi, WorkspaceId,
};
use intent_services::dedupe_keys::{
    attachment_reservation_key_for_test, park_dedupe_step_for_test,
    place_keyed_attachment_for_test, reservation_waits_for_test, steps,
};
use intent_services::Services;
use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::{OrchestratorOnly, Store};
use intentd_test_support::test_timeout;
use serde_json::json;

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

fn tenant(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

async fn seed_workspace(store: &Store, id: &str, root: Option<&std::path::Path>) -> WorkspaceId {
    let id = WorkspaceId::from(id);
    let now = intent_core::now_iso();
    store
        .insert_workspace(&Workspace {
            id: id.clone(),
            title: id.0.clone(),
            branch: "main".into(),
            worktree_path: root.map(|p| p.to_string_lossy().into_owned()),
            created_at: now.clone(),
            updated_at: now,
            ..Workspace::default()
        })
        .await
        .expect("seed the workspace");
    id
}

async fn seed_agent(store: &Store, ws: &WorkspaceId, id: &str) -> AgentId {
    let id = AgentId::from(id);
    let now = intent_core::now_iso();
    store
        .insert_agent_session(&AgentSession {
            id: id.clone(),
            workspace_id: ws.clone(),
            name: id.0.clone(),
            name_explicitly_set: true,
            status: AgentStatus::Idle,
            created_at: now.clone(),
            updated_at: now,
            ..AgentSession::default()
        })
        .await
        .expect("seed the agent");
    id
}

/// The questions-dismissed notices in `agent`'s transcript, by dismissed
/// message id.
async fn dismissal_notices(store: &Store, agent: &AgentId) -> Vec<serde_json::Value> {
    store
        .get_agent_messages(agent, None)
        .await
        .expect("messages")
        .into_iter()
        .filter_map(|m| {
            m.metadata
                .filter(|meta| meta["type"] == json!("questions_dismissed"))
                .map(|meta| meta["dismissedQuestionsMessageId"].clone())
        })
        .collect()
}

/// D1 + D2: dismissals of one message id from two processes notify once,
/// whether they race or interleave with a newer id (A -> B -> A).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dismissals_from_two_processes_notify_once_per_message_id() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("dedupe-dismiss");
    let store_a = open(&pool, &tenant, "process-a").await;
    let store_b = open(&pool, &tenant, "process-b").await;
    let a = Services::new_isolated(store_a.clone());
    let b = Services::new_isolated(store_b.clone());
    let ws = seed_workspace(&store_a, "ws-dismiss", None).await;
    let agent = seed_agent(&store_a, &ws, "agent-dismiss").await;
    let mut asked = Vec::new();
    for _ in 0..3 {
        let question = json!([{ "type": "text", "text": "Which one?" }]);
        let message = store_a
            .append_agent_message(&agent, "assistant", &question, &intent_core::now_iso())
            .await
            .expect("append question");
        asked.push(message.id);
    }
    let dismiss = |svc: &Services, id: &str| {
        let (svc, ws, agent, id) = (svc.clone(), ws.clone(), agent.clone(), id.to_owned());
        async move {
            with_caller(Caller::Daemon, svc.agent_dismiss_questions(ws, agent, id))
                .await
                .expect("dismiss")
        }
    };

    // Interleaved: A dismisses m0 then m1, B dismisses m0 again.
    dismiss(&a, &asked[0]).await;
    dismiss(&a, &asked[1]).await;
    dismiss(&b, &asked[0]).await;
    // Racing: both dismiss m2 at once.
    let (first, second) = tokio::time::timeout(test_timeout(Duration::from_secs(20)), async {
        tokio::join!(dismiss(&a, &asked[2]), dismiss(&b, &asked[2]))
    })
    .await
    .expect("both dismissals finish");
    assert_eq!(first["dismissedQuestionsMessageId"], json!(asked[2]));
    assert_eq!(second["dismissedQuestionsMessageId"], json!(asked[2]));

    assert_eq!(
        dismissal_notices(&store_b, &agent).await,
        vec![json!(asked[0]), json!(asked[1]), json!(asked[2])],
        "one notice per dismissed message id across both processes"
    );
}

/// A1: a keyed placement in another process while the first holds the key
/// waits, then replays the first's attachment instead of placing a second
/// copy (and failing "already bound"). This is the agent image path, which
/// holds no archive fence; `file.placeAttachment` also holds the
/// workspace's archive-fence lease, which orders it across processes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_keyed_placement_in_another_process_waits_and_replays() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("dedupe-attach");
    let root = tempfile::tempdir().expect("workspace root");
    let store_a = open(&pool, &tenant, "process-a").await;
    let store_b = open(&pool, &tenant, "process-b").await;
    let a = Services::new_isolated(store_a.clone());
    let b = Services::new_isolated(store_b.clone());
    let ws = seed_workspace(&store_a, "ws-attach", Some(root.path())).await;
    let key = format!("image-file-{}", uuid::Uuid::new_v4().simple());
    let place = |svc: &Services| {
        let (svc, ws, key) = (svc.clone(), ws.clone(), key.clone());
        async move {
            place_keyed_attachment_for_test(&svc, ws, "image.png", b"same payload", &key).await
        }
    };

    // A reserves the key and stops there.
    let park = park_dedupe_step_for_test(steps::ATTACHMENT_RESERVED, &key);
    let mut first = tokio::spawn(place(&a));
    tokio::time::timeout(test_timeout(Duration::from_secs(20)), async {
        tokio::select! {
            () = park.entered.notified() => {}
            ended = &mut first => panic!("A ended before its reservation: {ended:?}"),
        }
    })
    .await
    .expect("A reserves the key");

    // B's same-key placement finds the key reserved and waits.
    let store_key = attachment_reservation_key_for_test(&key);
    let mut second = tokio::spawn(place(&b));
    tokio::time::timeout(test_timeout(Duration::from_secs(20)), async {
        tokio::select! {
            () = reservation_waits_for_test(&ws.0, &store_key, 2) => {}
            ended = &mut second => panic!("B ended without waiting: {ended:?}"),
        }
    })
    .await
    .expect("B waits for A's reservation");
    assert!(!first.is_finished(), "A stays parked");

    park.release.notify_one();
    let (first, second) = tokio::time::timeout(test_timeout(Duration::from_secs(20)), async {
        (first.await.unwrap(), second.await.unwrap())
    })
    .await
    .expect("both placements finish");
    let first = first.expect("A places the attachment");
    let second = second.expect("B replays A's placement");
    assert_eq!(first["replayed"], serde_json::Value::Null, "A placed it");
    assert_eq!(second["replayed"], json!(true), "B replayed it");
    assert_eq!(second["attachmentId"], first["attachmentId"]);
    assert_eq!(second["path"], first["path"]);
    let placed = std::fs::read_dir(root.path().join(".intent").join("attachments"))
        .expect("the attachments folder")
        .filter(|entry| {
            !entry
                .as_ref()
                .expect("an attachments entry")
                .file_name()
                .to_string_lossy()
                .starts_with('.')
        })
        .count();
    assert_eq!(placed, 1, "one file placed for one key");
}
