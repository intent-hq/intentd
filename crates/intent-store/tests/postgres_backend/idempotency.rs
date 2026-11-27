//! Idempotency keys on both backends.
//!
//! Decoy rule: the decoy owns the same keys plus `missing`, and ends its run
//! with a row the subject's reap would reach without its tenant filter.
//! `created_at` is the store's clock, so the reap cutoffs are built from it;
//! a cutoff of `<date>t` sorts after `<date>T…` bytewise but before it under
//! a case-insensitive locale, so `COLLATE "C"` decides that reap.

use intent_store::{IdempotencyClaim, IdempotencyReservation, Store};

use crate::harness::parity_test;

parity_test!(idempotency_put_get_and_reap, |store, tenant| {
    assert_eq!(store.get_idempotent("", "missing").await.unwrap(), None);
    if tenant == "decoy" {
        assert!(store
            .put_idempotent("", "missing", "m", "\"decoy\"")
            .await
            .unwrap());
    }

    let result = format!("{{\"tenant\":\"{tenant}\"}}");
    assert!(store
        .put_idempotent("", "k1", "workspace.create", &result)
        .await
        .unwrap());
    assert!(!store
        .put_idempotent("", "k1", "workspace.create", "\"loser\"")
        .await
        .unwrap());
    assert_eq!(
        store.get_idempotent("", "k1").await.unwrap(),
        Some(result.clone())
    );
    // The same key in another workspace is a separate row.
    assert_eq!(store.get_idempotent("ws-1", "k1").await.unwrap(), None);
    assert!(store
        .put_idempotent("ws-1", "k1", "git.commit", "\"ws\"")
        .await
        .unwrap());
    assert_eq!(
        store.get_idempotent("ws-1", "k1").await.unwrap().as_deref(),
        Some("\"ws\"")
    );

    let date = intent_core::now_iso()[..10].to_owned();
    assert_eq!(store.reap_idempotent("2000-01-01").await.unwrap(), 0);
    let removed = store.reap_idempotent(&format!("{date}t")).await.unwrap();
    assert_eq!(removed, if tenant == "decoy" { 3 } else { 2 });
    assert_eq!(store.get_idempotent("", "k1").await.unwrap(), None);
    assert_eq!(store.reap_idempotent("9999").await.unwrap(), 0);

    assert!(store.put_idempotent("", "k1", "m", &result).await.unwrap());
    assert!(store
        .put_idempotent("ws-1", "k2", "m", &result)
        .await
        .unwrap());
    if tenant == "decoy" {
        assert!(store
            .put_idempotent("", "missing", "m", "\"decoy\"")
            .await
            .unwrap());
        assert!(store
            .put_idempotent("ws-1", "k1", "m", "\"decoy\"")
            .await
            .unwrap());
    } else {
        assert_eq!(store.reap_idempotent("9999").await.unwrap(), 2);
        assert!(store.put_idempotent("", "k1", "m", &result).await.unwrap());
        assert!(store
            .put_idempotent("ws-1", "k2", "m", &result)
            .await
            .unwrap());
    }
});

/// A store of `tenant` opened as if by another process named `process`.
async fn open_as_process(db: &crate::harness::TestDb, tenant: &str, process: &str) -> Store {
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

/// The claim of a `Reserved` reservation; panics on anything else.
fn reserved(reservation: IdempotencyReservation) -> IdempotencyClaim {
    match reservation {
        IdempotencyReservation::Reserved(claim) => claim,
        other => panic!("expected Reserved, got {other:?}"),
    }
}

/// Batch G 2b: a keyed operation reserves its key before it runs, so a
/// retry that reaches another process while the first still runs does not
/// run it again: it sees the key in progress, then replays the stored
/// result. A failed operation releases its key for a retry. A reservation
/// names itself and an expiry by the database clock; only its expiry frees
/// it for another reservation, never a takeover.
#[tokio::test]
async fn a_keyed_operation_runs_in_one_process_at_a_time() {
    let Some(db) = crate::harness::test_db(false).await else {
        let (store, _dir) = crate::harness::sqlite_store().await;
        let claim = reserved(store.reserve_idempotent("", "k", "m").await.unwrap());
        assert!(store
            .complete_idempotent("", "k", "m", &claim, "1")
            .await
            .unwrap());
        assert_eq!(
            store.reserve_idempotent("", "k", "m").await.unwrap(),
            IdempotencyReservation::Done("1".into())
        );
        return;
    };
    let tenant = "reservations";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;

    // A reserves and runs; B's retry waits, then replays A's result.
    let create = reserved(
        a.reserve_idempotent("", "create", "agent.create")
            .await
            .unwrap(),
    );
    assert_eq!(
        b.reserve_idempotent("", "create", "agent.create")
            .await
            .unwrap(),
        IdempotencyReservation::InProgress,
        "a retry in another process must not run the operation again"
    );
    assert_eq!(b.get_idempotent("", "create").await.unwrap(), None);
    assert!(a
        .complete_idempotent("", "create", "agent.create", &create, "{\"id\":\"a1\"}")
        .await
        .unwrap());
    assert_eq!(
        b.reserve_idempotent("", "create", "agent.create")
            .await
            .unwrap(),
        IdempotencyReservation::Done("{\"id\":\"a1\"}".into())
    );
    assert_eq!(
        b.get_idempotent("", "create").await.unwrap().as_deref(),
        Some("{\"id\":\"a1\"}")
    );

    // A failed operation releases its key; another claim's release is a
    // no-op.
    let commit = reserved(
        a.reserve_idempotent("ws-1", "commit", "git.commit")
            .await
            .unwrap(),
    );
    b.release_idempotent("ws-1", "commit", &create)
        .await
        .unwrap();
    assert_eq!(
        b.reserve_idempotent("ws-1", "commit", "git.commit")
            .await
            .unwrap(),
        IdempotencyReservation::InProgress
    );
    a.release_idempotent("ws-1", "commit", &commit)
        .await
        .unwrap();
    reserved(
        b.reserve_idempotent("ws-1", "commit", "git.commit")
            .await
            .unwrap(),
    );

    // A restarted daemon (a new process, the tenant never leased) may run
    // an operation the gone process left reserved: its first open of the
    // tenant drops other processes' reservations.
    reserved(a.reserve_idempotent("", "merge", "pr.merge").await.unwrap());
    let restarted = open_as_process(&db, tenant, "process-restarted").await;
    reserved(
        restarted
            .reserve_idempotent("", "merge", "pr.merge")
            .await
            .unwrap(),
    );

    // A takeover leaves a live reservation alone: only the reservation's
    // expiry frees it. (The restarted open above dropped B's reservation,
    // so B reserves the key again first.)
    reserved(
        b.reserve_idempotent("ws-1", "commit", "git.commit")
            .await
            .unwrap(),
    );
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
    assert_eq!(
        owner
            .reserve_idempotent("ws-1", "commit", "git.commit")
            .await
            .unwrap(),
        IdempotencyReservation::InProgress,
        "a takeover must not void a reservation whose holder still runs"
    );
    expire_reservation(&db, tenant, "ws-1", "commit").await;
    let taken = reserved(
        owner
            .reserve_idempotent("ws-1", "commit", "git.commit")
            .await
            .unwrap(),
    );
    assert!(owner
        .complete_idempotent("ws-1", "commit", "git.commit", &taken, "\"c\"")
        .await
        .unwrap());
    assert_eq!(
        owner
            .get_idempotent("ws-1", "commit")
            .await
            .unwrap()
            .as_deref(),
        Some("\"c\"")
    );
}

/// Moves the expiry of the pending reservation of (`workspace_id`, `key`)
/// to now, as if its holder stopped renewing it a TTL ago.
async fn expire_reservation(
    db: &crate::harness::TestDb,
    tenant: &str,
    workspace_id: &str,
    key: &str,
) {
    let expired = sqlx::query(
        "UPDATE idempotency_key SET claim_expires_at = clock_timestamp() \
         WHERE tenant_id = $1 AND workspace_id = $2 AND idempotency_key = $3 \
         AND result_json IS NULL",
    )
    .bind(tenant)
    .bind(workspace_id)
    .bind(key)
    .execute(&db.admin)
    .await
    .expect("expire the reservation")
    .rows_affected();
    assert_eq!(expired, 1, "a pending reservation to expire");
}

/// Request replicas (orchestrator mode): a process that reserved a key and
/// died leaves the tenant epoch unchanged, so the key must free itself by
/// its own expiry, not wait for a takeover or the 24 h reaper. A live
/// holder renews its reservation; a holder whose reservation another
/// process took over can no longer renew, release or complete it, and its
/// complete reports that another result stands.
#[tokio::test]
async fn a_dead_holders_reservation_lapses_and_a_live_one_renews() {
    let Some(db) = crate::harness::test_db(false).await else {
        let (store, _dir) = crate::harness::sqlite_store().await;
        let claim = reserved(store.reserve_idempotent("", "k", "m").await.unwrap());
        assert!(store.renew_idempotent("", "k", &claim).await.unwrap());
        return;
    };
    let tenant = "lapsing-reservations";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;

    // A reserves and renews while it runs: B keeps waiting, also past the
    // expiry A had before it renewed.
    let held_by_a = reserved(
        a.reserve_idempotent("", "create", "agent.create")
            .await
            .unwrap(),
    );
    expire_reservation(&db, tenant, "", "create").await;
    assert!(
        a.renew_idempotent("", "create", &held_by_a).await.unwrap(),
        "the holder renews its own lapsed reservation while nobody took it"
    );
    assert_eq!(
        b.reserve_idempotent("", "create", "agent.create")
            .await
            .unwrap(),
        IdempotencyReservation::InProgress,
        "a renewed reservation is live again"
    );

    // A dies (no release, no renewal). Its reservation lapses and B runs
    // the operation once, with no takeover and no new open.
    expire_reservation(&db, tenant, "", "create").await;
    let held_by_b = reserved(
        b.reserve_idempotent("", "create", "agent.create")
            .await
            .unwrap(),
    );
    // A, back from a stall, cannot renew, release or complete B's run.
    assert!(!a.renew_idempotent("", "create", &held_by_a).await.unwrap());
    a.release_idempotent("", "create", &held_by_a)
        .await
        .unwrap();
    assert!(
        !a.complete_idempotent("", "create", "agent.create", &held_by_a, "\"a\"")
            .await
            .unwrap(),
        "a lapsed reservation's result is not stored"
    );
    assert_eq!(
        a.reserve_idempotent("", "create", "agent.create")
            .await
            .unwrap(),
        IdempotencyReservation::InProgress,
        "B still holds the key"
    );
    assert!(b
        .complete_idempotent("", "create", "agent.create", &held_by_b, "\"b\"")
        .await
        .unwrap());
    assert_eq!(
        a.reserve_idempotent("", "create", "agent.create")
            .await
            .unwrap(),
        IdempotencyReservation::Done("\"b\"".into()),
        "every caller replays the result of the run that held the key"
    );
}

/// Each reservation has its own token, also in one process: a late release
/// of an earlier reservation (its spawned release ran after the next
/// caller in the same process reserved the key) leaves the later one alone.
#[tokio::test]
async fn a_late_release_leaves_a_later_reservation_of_the_same_process() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "late-release";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;

    let first = reserved(a.reserve_idempotent("", "place", "m").await.unwrap());
    expire_reservation(&db, tenant, "", "place").await;
    let second = reserved(a.reserve_idempotent("", "place", "m").await.unwrap());
    assert_ne!(first, second);
    a.release_idempotent("", "place", &first).await.unwrap();
    assert!(!a.renew_idempotent("", "place", &first).await.unwrap());
    assert_eq!(
        b.reserve_idempotent("", "place", "m").await.unwrap(),
        IdempotencyReservation::InProgress,
        "the later reservation survives the earlier one's release"
    );
    assert!(a.renew_idempotent("", "place", &second).await.unwrap());
}
