//! Repository consumers receive the authentic opaque admission object. These
//! tests exercise that private transport producer without exporting bearer
//! constructors or starting a listener; service integration is tested separately.

use futures::FutureExt as _;
use intent_core::caller::WireCredential;
use intent_core::{HostRole, PrincipalId};

use super::*;

fn store() -> (tempfile::TempDir, AsyncTokenStore) {
    let mut dir = tempfile::Builder::new()
        .prefix("repository-transport-authority-")
        .tempdir()
        .unwrap();
    if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|value| !value.is_empty()) {
        dir.disable_cleanup(true);
    }
    let backing = FileTokenStore {
        secrets: intent_core::FileSecretStore::with_path(dir.path().join("secrets.json")),
    };
    (dir, AsyncTokenStore::new(Arc::new(backing)))
}

fn admitted(store: &AsyncTokenStore, token: &str) -> AdmittedCredential {
    AdmittedCredential::new(
        ResolvedCredential::Legacy,
        token.into(),
        LegacyRotation::new(store, token),
    )
}

fn owner() -> Caller {
    Caller::Wire {
        principal_id: PrincipalId::from("original-owner"),
        host_role: HostRole::Owner,
    }
}

#[tokio::test]
async fn original_binding_lease_fences_rotation_then_rejects_the_original_secret() {
    let (_dir, store) = store();
    store
        .store_token("original-disposable-token")
        .await
        .unwrap();
    let binding = admitted(&store, "original-disposable-token")
        .binding(&store, &owner())
        .unwrap();
    let WireCredential::Legacy {
        principal_id,
        authority,
    } = binding
    else {
        panic!("expected legacy binding");
    };
    assert_eq!(principal_id, PrincipalId::from("original-owner"));
    let lease = authority.authorize().await.unwrap();
    let mut replace = Box::pin(store.store_token("replacement-disposable-token"));
    assert!((&mut replace).now_or_never().is_none());
    drop(lease);
    replace.await.unwrap();
    assert!(authority.authorize().await.is_err());
    let fresh = admitted(&store, "replacement-disposable-token")
        .binding(&store, &owner())
        .unwrap();
    let WireCredential::Legacy {
        authority: replacement,
        ..
    } = fresh
    else {
        panic!("expected replacement binding");
    };
    drop(replacement.authorize().await.unwrap());
    assert!(authority.authorize().await.is_err());
}

#[tokio::test]
async fn absent_cleared_or_foreign_bearer_never_uses_the_current_token() {
    let (_dir, store) = store();
    let original = admitted(&store, "original-disposable-token");
    let WireCredential::Legacy { authority, .. } = original.binding(&store, &owner()).unwrap()
    else {
        panic!("expected binding");
    };
    assert!(authority.authorize().await.is_err());
    store
        .store_token("different-disposable-token")
        .await
        .unwrap();
    assert!(authority.authorize().await.is_err());
    store
        .store_token("original-disposable-token")
        .await
        .unwrap();
    drop(authority.authorize().await.unwrap());
    store.store_token("").await.unwrap();
    assert!(authority.authorize().await.is_err());
    assert!(original.binding(&store, &Caller::Daemon).is_none());
}

#[tokio::test]
async fn personal_binding_preserves_its_admitted_principal_and_original_hash() {
    let (_dir, store) = store();
    store.store_token("unrelated-legacy-token").await.unwrap();
    let principal = PrincipalId::from("member-A");
    let original = AdmittedCredential::new(
        ResolvedCredential::Principal(principal.clone()),
        "personal-token".into(),
        LegacyRotation::new(&store, "personal-token"),
    );
    let binding = original
        .binding(
            &store,
            &Caller::Wire {
                principal_id: principal.clone(),
                host_role: HostRole::Member,
            },
        )
        .unwrap();
    let WireCredential::Principal {
        principal_id,
        token_hash,
    } = binding
    else {
        panic!("expected personal binding");
    };
    assert_eq!(principal_id, principal);
    assert_eq!(token_hash, hash_token("personal-token"));
    assert_ne!(token_hash, hash_token("unrelated-legacy-token"));
}
