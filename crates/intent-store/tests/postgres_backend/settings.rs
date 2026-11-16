//! Settings and daemon id on both backends.

use crate::harness::parity_test;

// Leaves its rows behind: on the subject run, a statement without its
// tenant filter would see the decoy's values.
parity_test!(settings_round_trip, |store| {
    assert_eq!(store.get_setting("userRules").await.unwrap(), None);
    store.set_setting("userRules", "\"a\"").await.unwrap();
    store.set_setting("userRules", "\"b\"").await.unwrap();
    assert_eq!(
        store.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"b\"")
    );
    assert!(!store.delete_setting("other").await.unwrap());
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
