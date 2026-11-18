//! Idempotency keys on both backends.
//!
//! Decoy rule: the decoy owns the same keys plus `missing`, and ends its run
//! with a row the subject's reap would reach without its tenant filter.
//! `created_at` is the store's clock, so the reap cutoffs are built from it;
//! a cutoff of `<date>t` sorts after `<date>T…` bytewise but before it under
//! a case-insensitive locale, so `COLLATE "C"` decides that reap.

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
