//! Idempotency keys on both backends.
//!
//! Decoy rule: the decoy owns the same keys plus `missing`, and ends its run
//! with a row the subject's reap would reach without its tenant filter.
//! `created_at` is the store's clock, so the reap cutoffs are built from it;
//! a cutoff of `<date>t` sorts after `<date>T…` bytewise but before it under
//! a case-insensitive locale, so `COLLATE "C"` decides that reap.

use intent_store::{IdempotencyReservation, Store};

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

/// Batch G 2b: a keyed operation reserves its key before it runs, so a
/// retry that reaches another process while the first still runs does not
/// run it again: it sees the key in progress, then replays the stored
/// result. A failed operation releases its key for a retry. A reservation
/// names the tenant's epoch and the process; only a newer epoch (a
/// takeover) may take it over, never elapsed time.
#[tokio::test]
async fn a_keyed_operation_runs_in_one_process_at_a_time() {
    let Some(db) = crate::harness::test_db(false).await else {
        let (store, _dir) = crate::harness::sqlite_store().await;
        assert_eq!(
            store.reserve_idempotent("", "k", "m").await.unwrap(),
            IdempotencyReservation::Reserved
        );
        store.complete_idempotent("", "k", "m", "1").await.unwrap();
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
    assert_eq!(
        a.reserve_idempotent("", "create", "agent.create")
            .await
            .unwrap(),
        IdempotencyReservation::Reserved
    );
    assert_eq!(
        b.reserve_idempotent("", "create", "agent.create")
            .await
            .unwrap(),
        IdempotencyReservation::InProgress,
        "a retry in another process must not run the operation again"
    );
    assert_eq!(b.get_idempotent("", "create").await.unwrap(), None);
    a.complete_idempotent("", "create", "agent.create", "{\"id\":\"a1\"}")
        .await
        .unwrap();
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

    // A failed operation releases its key; a non-holder's release is a no-op.
    assert_eq!(
        a.reserve_idempotent("ws-1", "commit", "git.commit")
            .await
            .unwrap(),
        IdempotencyReservation::Reserved
    );
    b.release_idempotent("ws-1", "commit").await.unwrap();
    assert_eq!(
        b.reserve_idempotent("ws-1", "commit", "git.commit")
            .await
            .unwrap(),
        IdempotencyReservation::InProgress
    );
    a.release_idempotent("ws-1", "commit").await.unwrap();
    assert_eq!(
        b.reserve_idempotent("ws-1", "commit", "git.commit")
            .await
            .unwrap(),
        IdempotencyReservation::Reserved
    );

    // A restarted daemon (a new process, the tenant never leased) may run
    // an operation the gone process left reserved: its first open of the
    // tenant drops other processes' reservations.
    assert_eq!(
        a.reserve_idempotent("", "merge", "pr.merge").await.unwrap(),
        IdempotencyReservation::Reserved
    );
    let restarted = open_as_process(&db, tenant, "process-restarted").await;
    assert_eq!(
        restarted
            .reserve_idempotent("", "merge", "pr.merge")
            .await
            .unwrap(),
        IdempotencyReservation::Reserved
    );

    // A takeover's newer epoch may take over a reservation left behind.
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
        IdempotencyReservation::Reserved
    );
    owner
        .complete_idempotent("ws-1", "commit", "git.commit", "\"c\"")
        .await
        .unwrap();
    assert_eq!(
        owner
            .get_idempotent("ws-1", "commit")
            .await
            .unwrap()
            .as_deref(),
        Some("\"c\"")
    );
}
