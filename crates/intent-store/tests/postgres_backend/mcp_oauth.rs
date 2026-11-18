//! MCP OAuth token bags on both backends.
//!
//! Decoy rule: the decoy owns the same server ids (bags carry the tenant)
//! plus `decoy-only`, which the subject reads and deletes as absent. Server
//! ids `b`, `B`, `a` sort differently bytewise and under a locale, so the
//! list order needs `COLLATE "C"`.

use crate::harness::parity_test;

parity_test!(mcp_oauth_tokens_round_trip, |store, tenant| {
    assert_eq!(store.get_mcp_oauth_token("decoy-only").await.unwrap(), None);
    assert!(!store.delete_mcp_oauth_token("decoy-only").await.unwrap());
    assert!(store.list_mcp_oauth_server_ids().await.unwrap().is_empty());

    let bag = |v: &str| format!("{{\"tenant\":\"{tenant}\",\"v\":\"{v}\"}}");
    for id in ["b", "B", "a"] {
        store
            .set_mcp_oauth_token(id, &bag(id), "2026-01-01T00:00:00.000Z")
            .await
            .unwrap();
    }
    store
        .set_mcp_oauth_token("a", &bag("a2"), "2026-01-02T00:00:00.000Z")
        .await
        .unwrap();
    assert_eq!(
        store.get_mcp_oauth_token("a").await.unwrap(),
        Some(bag("a2"))
    );
    assert_eq!(
        store.list_mcp_oauth_server_ids().await.unwrap(),
        ["B", "a", "b"]
    );

    assert!(store.delete_mcp_oauth_token("b").await.unwrap());
    assert!(!store.delete_mcp_oauth_token("b").await.unwrap());
    assert_eq!(store.get_mcp_oauth_token("b").await.unwrap(), None);
    store
        .set_mcp_oauth_token("b", &bag("b"), "2026-01-03T00:00:00.000Z")
        .await
        .unwrap();
    if tenant == "decoy" {
        store
            .set_mcp_oauth_token("decoy-only", &bag("x"), "2026-01-01T00:00:00.000Z")
            .await
            .unwrap();
    } else {
        assert_eq!(
            store.list_mcp_oauth_server_ids().await.unwrap(),
            ["B", "a", "b"]
        );
    }
});
