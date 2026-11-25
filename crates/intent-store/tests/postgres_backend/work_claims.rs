//! Work claims on Postgres: processes that share one tenant run a claimed
//! unit of work one at a time. See `work_claim_repo.rs` for the rules.

use std::time::Duration;

use intent_store::Store;

use crate::harness::TestDb;

const TTL: Duration = Duration::from_secs(60);
const SHORT_TTL: Duration = Duration::from_millis(400);
const KIND: &str = "token-usage-scan";

/// A store of `tenant` opened as if by another process named `process`: its
/// own process token and gates, so only the database orders it.
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

/// Two processes race one claim: exactly one wins; the holder renews and
/// the other keeps losing; release frees it at once; a claim is not
/// re-entrant; other keys and other tenants are other work.
#[tokio::test]
async fn two_processes_race_one_claim_and_release_frees_it() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "work-claims";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;

    let racers = (0..8).map(|i| {
        let s = if i % 2 == 0 { a.clone() } else { b.clone() };
        tokio::spawn(async move { (i % 2, s.claim_work(KIND, "", TTL).await.unwrap()) })
    });
    let mut winners = Vec::new();
    for racer in racers {
        let (side, claim) = racer.await.unwrap();
        if let Some(claim) = claim {
            winners.push((side, claim));
        }
    }
    assert_eq!(winners.len(), 1, "exactly one claimer wins");
    let (side, mut held) = winners.pop().unwrap();
    let (holder, other) = if side == 0 { (&a, &b) } else { (&b, &a) };

    assert!(
        holder.claim_work(KIND, "", TTL).await.unwrap().is_none(),
        "not re-entrant"
    );
    assert!(other.claim_work(KIND, "", TTL).await.unwrap().is_none());
    assert!(holder.renew_work_claim(&mut held).await.unwrap());
    assert!(other.claim_work(KIND, "", TTL).await.unwrap().is_none());

    let elsewhere = other.claim_work(KIND, "other-key", TTL).await.unwrap();
    assert!(elsewhere.is_some(), "another key is other work");
    let other_tenant = open_as_process(&db, "work-claims-other", "process-b").await;
    assert!(
        other_tenant
            .claim_work(KIND, "", TTL)
            .await
            .unwrap()
            .is_some(),
        "another tenant is other work"
    );

    holder.release_work_claim(held).await.unwrap();
    let mut taken = other
        .claim_work(KIND, "", TTL)
        .await
        .unwrap()
        .expect("release frees the claim at once");
    assert!(other.renew_work_claim(&mut taken).await.unwrap());
    assert!(holder.claim_work(KIND, "", TTL).await.unwrap().is_none());
}

/// A holder that stops renewing (crashed, or a run that outlived its TTL)
/// loses the claim by the database clock: another process takes it, the old
/// holder's renewal reports the loss, and its late release leaves the new
/// claim alone. A lapsed claim nobody took still renews.
#[tokio::test]
async fn expiry_hands_the_claim_over() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "work-claim-expiry";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;

    let mut lapsed = a.claim_work(KIND, "", SHORT_TTL).await.unwrap().unwrap();
    assert!(b.claim_work(KIND, "", TTL).await.unwrap().is_none());
    // timing-guard: outlive the short claim's TTL
    tokio::time::sleep(SHORT_TTL * 2).await;
    assert_eq!(lapsed.remaining(), Duration::ZERO);
    assert!(
        a.renew_work_claim(&mut lapsed).await.unwrap(),
        "lapsed but untaken: still this claim"
    );
    assert!(b.claim_work(KIND, "", TTL).await.unwrap().is_none());

    // timing-guard: outlive the renewed short TTL
    tokio::time::sleep(SHORT_TTL * 2).await;
    let mut taken = b
        .claim_work(KIND, "", TTL)
        .await
        .unwrap()
        .expect("expiry hands the claim over");
    assert!(
        !a.renew_work_claim(&mut lapsed).await.unwrap(),
        "the old holder lost it"
    );
    a.release_work_claim(lapsed).await.unwrap();
    assert!(
        b.renew_work_claim(&mut taken).await.unwrap(),
        "a late release by the old holder is a no-op"
    );
    assert!(a.claim_work(KIND, "", TTL).await.unwrap().is_none());
}

/// A takeover (a newer tenant epoch) makes every claim taken at the old
/// epoch stale at once, without waiting for its TTL; the old process can
/// no longer claim, renew or release at all.
#[tokio::test]
async fn a_takeover_frees_claims_of_the_old_epoch() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "work-claim-takeover";
    let old = open_as_process(&db, tenant, "process-old").await;
    let mut stale = old.claim_work(KIND, "", TTL).await.unwrap().unwrap();

    let (owner, _lease) = Store::open_postgres_owned_as_process(
        db.app.clone(),
        intent_store::postgres::TenantId::new(tenant).unwrap(),
        intent_store::postgres::PostgresOptions::default(),
        "process-new",
        "process-new",
        Duration::from_secs(60),
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("claim the tenant");
    let mut held = owner
        .claim_work(KIND, "", TTL)
        .await
        .unwrap()
        .expect("a claim from an older epoch is stale");
    assert!(owner.claim_work(KIND, "", TTL).await.unwrap().is_none());
    assert!(owner.renew_work_claim(&mut held).await.unwrap());

    assert!(matches!(
        old.renew_work_claim(&mut stale).await,
        Err(intent_store::Error::NotOwner(_))
    ));
    assert!(matches!(
        old.claim_work(KIND, "", TTL).await,
        Err(intent_store::Error::NotOwner(_))
    ));
}

/// The claim row's expiry comes from the database clock, not the caller's:
/// it lies within the TTL of the database's own `clock_timestamp()`.
#[tokio::test]
async fn expiry_is_set_by_the_database_clock() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "work-claim-clock";
    let a = open_as_process(&db, tenant, "process-a").await;
    let _held = a.claim_work(KIND, "", TTL).await.unwrap().unwrap();
    let (left_ms, epoch, token_set): (i64, i64, bool) = sqlx::query_as(
        "SELECT (extract(epoch FROM expires_at - clock_timestamp()) * 1000)::bigint, \
         claim_epoch, claim_token <> '' \
         FROM work_claim WHERE tenant_id = $1 AND kind = $2 AND work_key = ''",
    )
    .bind(tenant)
    .bind(KIND)
    .fetch_one(&db.admin)
    .await
    .unwrap();
    let ttl_ms = i64::try_from(TTL.as_millis()).unwrap();
    assert!(
        left_ms > 0 && left_ms <= ttl_ms,
        "{left_ms} ms left of {ttl_ms}"
    );
    assert_eq!(epoch, 0, "claimed before any lease");
    assert!(token_set);
}
