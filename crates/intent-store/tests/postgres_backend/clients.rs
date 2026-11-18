//! Logical clients on both backends.
//!
//! Decoy rule: the decoy owns the same client ids (names carry the tenant)
//! plus `missing`, the id the subject probes as absent, so a read without its
//! tenant filter returns a decoy row.

use intent_core::{ClientHostInfo, ClientId};
use serde_json::json;

use crate::harness::parity_test;

fn cid(id: &str) -> ClientId {
    ClientId(id.into())
}

fn host(name: &str) -> ClientHostInfo {
    ClientHostInfo {
        hostname: Some(name.into()),
        pretty_hostname: Some(format!("{name}'s Mac")),
        device_kind: Some("desktop".into()),
    }
}

parity_test!(clients_upsert_ensure_and_get, |store, tenant| {
    assert!(store.get_client(&cid("missing")).await.unwrap().is_none());
    if tenant == "decoy" {
        store.ensure_client(&cid("missing")).await.unwrap();
    }

    let caps = json!({"tenant": tenant, "browser": true});
    store
        .upsert_client(&cid("c1"), Some(tenant), Some(&caps), &host(tenant))
        .await
        .unwrap();
    let first = store.get_client(&cid("c1")).await.unwrap().unwrap();
    assert_eq!(first.id, cid("c1"));
    assert_eq!(first.name.as_deref(), Some(tenant));
    assert_eq!(first.capabilities, caps);
    assert_eq!(first.host, host(tenant));
    assert_eq!(first.first_seen, first.last_seen);
    assert_eq!(
        first.last_hello_at.as_deref(),
        Some(first.last_seen.as_str())
    );

    // Re-hello: name, capabilities and host replaced wholesale, first_seen kept.
    let bare = ClientHostInfo {
        hostname: Some(format!("{tenant}-2")),
        pretty_hostname: None,
        device_kind: None,
    };
    store
        .upsert_client(&cid("c1"), None, None, &bare)
        .await
        .unwrap();
    let again = store.get_client(&cid("c1")).await.unwrap().unwrap();
    assert_eq!(again.name, None);
    assert_eq!(again.capabilities, json!({}));
    assert_eq!(again.host, bare);
    assert_eq!(again.first_seen, first.first_seen);
    assert!(again.last_hello_at.is_some());

    // ensure_client never clobbers a hello'd client.
    store.ensure_client(&cid("c1")).await.unwrap();
    assert_eq!(store.get_client(&cid("c1")).await.unwrap().unwrap(), again);

    // A placeholder has no hello, empty capabilities and no host.
    store.ensure_client(&cid("draft")).await.unwrap();
    let draft = store.get_client(&cid("draft")).await.unwrap().unwrap();
    assert_eq!(draft.name, None);
    assert_eq!(draft.capabilities, json!({}));
    assert_eq!(draft.host, ClientHostInfo::default());
    assert_eq!(draft.last_hello_at, None);
    store.ensure_client(&cid("draft")).await.unwrap();
    assert_eq!(
        store.get_client(&cid("draft")).await.unwrap().unwrap(),
        draft
    );

    // A hello upgrades the placeholder in place.
    store
        .upsert_client(&cid("draft"), Some("named"), None, &host(tenant))
        .await
        .unwrap();
    let hello = store.get_client(&cid("draft")).await.unwrap().unwrap();
    assert_eq!(hello.first_seen, draft.first_seen);
    assert_eq!(hello.name.as_deref(), Some("named"));
    assert!(hello.last_hello_at.is_some());
});
