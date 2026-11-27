//! Intent apps library and per-app storage on both backends.
//!
//! Decoy rule: the decoy writes the same app ids with its own records and
//! values, so a missing tenant filter would show the decoy's data or count
//! its bytes against the subject's quota.

use intent_store::AppStorageWrite;

use crate::harness::parity_test;

parity_test!(app_records_and_storage_round_trip, |store, tenant| {
    let rec = |id: &str| format!("{{\"id\":\"{id}\",\"tenant\":\"{tenant}\"}}");
    assert_eq!(store.get_app_record("a").await.unwrap(), None);
    for id in ["b", "B", "a"] {
        store.put_app_record(id, &rec(id), "t0").await.unwrap();
    }
    assert_eq!(
        store.list_app_records().await.unwrap(),
        [rec("B"), rec("a"), rec("b")]
    );
    assert_eq!(store.get_app_record("a").await.unwrap(), Some(rec("a")));

    assert_eq!(
        store.app_storage_set("a", "k1", "1", 3, 6).await.unwrap(),
        AppStorageWrite::Written { used_bytes: 3 }
    );
    assert_eq!(
        store.app_storage_set("a", "k2", "22", 4, 6).await.unwrap(),
        AppStorageWrite::OverQuota { used_bytes: 3 }
    );
    assert_eq!(
        store.app_storage_set("a", "k1", "333", 6, 6).await.unwrap(),
        AppStorageWrite::Written { used_bytes: 6 }
    );
    for key in ["p_", "pB", "pb", "q"] {
        store.app_storage_set("b", key, "0", 1, 100).await.unwrap();
    }
    assert_eq!(
        store.app_storage_list("b", "p", None, 2).await.unwrap(),
        ["pB", "p_"]
    );
    assert_eq!(
        store
            .app_storage_list("b", "p", Some("p_"), 2)
            .await
            .unwrap(),
        ["pb"]
    );
    assert_eq!(
        store.app_storage_get("a", "k1").await.unwrap().as_deref(),
        Some("333")
    );
    assert!(store.app_storage_delete("b", "q").await.unwrap());
    assert!(!store.app_storage_delete("b", "q").await.unwrap());

    assert!(store.delete_app("a").await.unwrap());
    assert_eq!(store.app_storage_get("a", "k1").await.unwrap(), None);
    assert!(!store.delete_app("a").await.unwrap());
    if tenant != "decoy" {
        store.delete_app("b").await.unwrap();
        store.delete_app("B").await.unwrap();
        assert!(store.list_app_records().await.unwrap().is_empty());
    }
});
