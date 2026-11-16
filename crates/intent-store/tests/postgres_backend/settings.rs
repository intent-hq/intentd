//! Settings and daemon id on both backends.

use crate::harness::parity_test;

// Leaves its rows behind and keeps a decoy-only row: on the subject run a
// read without its tenant filter sees the decoy's values, and a write
// without it changes the decoy's rows (the harness compares them).
parity_test!(settings_round_trip, |store, tenant| {
    assert_eq!(store.get_setting("userRules").await.unwrap(), None);
    store.set_setting("userRules", "\"a\"").await.unwrap();
    store
        .set_setting("userRules", &format!("\"{tenant}\""))
        .await
        .unwrap();
    assert_eq!(
        store.get_setting("userRules").await.unwrap(),
        Some(format!("\"{tenant}\""))
    );
    if tenant == "decoy" {
        store.set_setting("decoyOnly", "1").await.unwrap();
    } else {
        assert!(!store.delete_setting("decoyOnly").await.unwrap());
    }
    store.set_setting("other", "1").await.unwrap();
    assert!(store.delete_setting("other").await.unwrap());
});

parity_test!(daemon_id_is_minted_once, |store| {
    let id = store.daemon_id().await.unwrap();
    assert!(!id.is_empty());
    assert_eq!(store.daemon_id().await.unwrap(), id);
});

parity_test!(agent_creation_preferences_default_to_empty, |store| {
    let ws = intent_core::WorkspaceId("ws-1".into());
    assert_eq!(
        store.get_agent_creation_preferences(&ws).await.unwrap(),
        serde_json::json!({})
    );
});
