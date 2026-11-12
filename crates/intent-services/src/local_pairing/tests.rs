use std::time::Duration;

use intent_core::local_pairing::{
    LocalPairingCancelReason, LocalPairingCodeState, LocalPairingError, LocalPairingService,
};
use sha2::{Digest as _, Sha256};

use super::{code_lifetime_from, LocalPairing, CODE_LIFETIME};
use crate::{EventBus, SubscriptionFilter};

struct Fixture {
    _dir: tempfile::TempDir,
    pairing: std::sync::Arc<LocalPairing>,
    bus: EventBus,
}

async fn fixture(lifetime: Duration) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = intent_store::Store::open(&dir.path().join("pairing.db"))
        .await
        .unwrap();
    let bus = EventBus::new(store.clone());
    let pairing = LocalPairing::with_lifetime(store, Some(bus.clone()), lifetime).unwrap();
    Fixture {
        _dir: dir,
        pairing,
        bus,
    }
}

async fn fixture_default() -> Fixture {
    fixture(intent_core::local_pairing::CODE_LIFETIME).await
}

/// A well-formed code that is not `code`.
fn wrong(code: &str) -> &'static str {
    if code == "AAA-AAA" {
        "BBB-BBB"
    } else {
        "AAA-AAA"
    }
}

fn hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

#[tokio::test]
async fn right_code_mints_a_credential_once() {
    let f = fixture_default().await;
    let created = f.pairing.create_code().await.unwrap();
    assert_eq!(created.code.len(), 7);
    assert_eq!(&created.code[3..4], "-");
    assert_eq!(created.local_code.state, LocalPairingCodeState::Active);
    assert_eq!(created.local_code.attempts_remaining, 5);

    let typed = created.code.to_lowercase().replace('-', " ");
    let grant = f.pairing.redeem(&typed, " Intent Alpha ").await.unwrap();
    assert_eq!(grant.app_name, "Intent Alpha");
    assert_eq!(grant.token.len(), 64);
    assert!(grant.token.bytes().all(|b| b.is_ascii_hexdigit()));

    assert_eq!(
        f.pairing.resolve_credential(hash(&grant.token)).await,
        Some(grant.credential_id.clone())
    );
    let listed = f.pairing.list_credentials().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].credential_id, grant.credential_id);
    assert_eq!(listed[0].app_name, "Intent Alpha");
    assert!(listed[0].last_used_at.is_some());
    assert!(!listed[0].connected);

    // A used code is gone: `local-code-expired`.
    assert_eq!(
        f.pairing.redeem(&created.code, "Intent").await.err(),
        Some(LocalPairingError::Expired)
    );
}

#[tokio::test]
async fn wrong_tries_count_down_then_lock() {
    let f = fixture_default().await;
    let created = f.pairing.create_code().await.unwrap();
    let bad = wrong(&created.code);
    for remaining in [4, 3, 2, 1] {
        assert_eq!(
            f.pairing.redeem(bad, "Intent").await.err(),
            Some(LocalPairingError::Invalid {
                attempts_remaining: remaining
            })
        );
    }
    assert_eq!(
        f.pairing.redeem(bad, "Intent").await.err(),
        Some(LocalPairingError::Locked)
    );
    // The right code after the lock is refused as locked.
    assert_eq!(
        f.pairing.redeem(&created.code, "Intent").await.err(),
        Some(LocalPairingError::Locked)
    );
    assert!(f.pairing.list_credentials().await.unwrap().is_empty());
}

#[tokio::test]
async fn malformed_input_does_not_count_as_a_try() {
    let f = fixture_default().await;
    let created = f.pairing.create_code().await.unwrap();
    for malformed in ["", "ABC", "ABC-DEFG", "AB0-DEF", "ABC_DEF"] {
        assert!(matches!(
            f.pairing.redeem(malformed, "Intent").await,
            Err(LocalPairingError::InvalidParams(_))
        ));
    }
    assert!(matches!(
        f.pairing.redeem(&created.code, "  ").await,
        Err(LocalPairingError::InvalidParams(_))
    ));
    assert_eq!(
        f.pairing.redeem(wrong(&created.code), "Intent").await.err(),
        Some(LocalPairingError::Invalid {
            attempts_remaining: 4
        })
    );
    assert!(f.pairing.redeem(&created.code, "Intent").await.is_ok());
}

#[tokio::test]
async fn no_code_is_expired() {
    let f = fixture_default().await;
    assert_eq!(
        f.pairing.redeem("ABC-DEF", "Intent").await.err(),
        Some(LocalPairingError::Expired)
    );
}

#[tokio::test]
async fn code_expires_and_emits_expired() {
    let f = fixture(Duration::from_millis(300)).await;
    let mut events = f.bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::PAIRING_LOCAL_CODE_CHANGED.into()],
        ..Default::default()
    });
    let created = f.pairing.create_code().await.unwrap();
    let first = events.recv().await.unwrap();
    assert_eq!(first[0].data["localCode"]["state"], "active");
    // The timer emits `expired` on its own, without an attempt.
    let expired = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        events.recv(),
    )
    .await
    .expect("expiry event")
    .unwrap();
    assert_eq!(expired[0].data["localCode"]["state"], "expired");
    assert_eq!(
        expired[0].data["localCode"]["codeId"],
        created.local_code.code_id.as_str()
    );
    assert_eq!(
        f.pairing.redeem(&created.code, "Intent").await.err(),
        Some(LocalPairingError::Expired)
    );
}

#[tokio::test]
async fn locked_becomes_expired_after_original_expiry() {
    let f = fixture(Duration::from_millis(400)).await;
    let created = f.pairing.create_code().await.unwrap();
    for _ in 0..5 {
        let _ = f.pairing.redeem(wrong(&created.code), "Intent").await;
    }
    assert_eq!(
        f.pairing.redeem(&created.code, "Intent").await.err(),
        Some(LocalPairingError::Locked)
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        f.pairing.redeem(&created.code, "Intent").await.err(),
        Some(LocalPairingError::Expired)
    );
}

#[tokio::test]
async fn concurrent_right_attempts_mint_exactly_once() {
    let f = fixture_default().await;
    let created = f.pairing.create_code().await.unwrap();
    let attempts: Vec<_> = (0..8)
        .map(|_| {
            let pairing = f.pairing.clone();
            let code = created.code.clone();
            tokio::spawn(async move { pairing.redeem(&code, "Intent").await })
        })
        .collect();
    let mut granted = 0;
    for attempt in attempts {
        match attempt.await.unwrap() {
            Ok(_) => granted += 1,
            Err(error) => assert_eq!(error, LocalPairingError::Expired),
        }
    }
    assert_eq!(granted, 1);
    assert_eq!(f.pairing.list_credentials().await.unwrap().len(), 1);
}

#[tokio::test]
async fn new_code_replaces_the_active_one() {
    let f = fixture_default().await;
    let mut events = f.bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::PAIRING_LOCAL_CODE_CHANGED.into()],
        ..Default::default()
    });
    let first = f.pairing.create_code().await.unwrap();
    let second = f.pairing.create_code().await.unwrap();
    assert_ne!(first.local_code.code_id, second.local_code.code_id);
    let mut seen = Vec::new();
    while seen.len() < 3 {
        seen.extend(events.recv().await.unwrap());
    }
    assert_eq!(
        seen[1].data["localCode"]["codeId"],
        first.local_code.code_id.as_str()
    );
    assert_eq!(seen[1].data["localCode"]["state"], "cancelled");
    assert_eq!(seen[1].data["localCode"]["cancelReason"], "replaced");
    assert_eq!(seen[2].data["localCode"]["state"], "active");
    for event in &seen {
        let text = event.data.to_string();
        assert!(!text.contains(&first.code) && !text.contains(&second.code));
    }
    if first.code != second.code {
        assert_eq!(
            f.pairing.redeem(&first.code, "Intent").await.err(),
            Some(LocalPairingError::Invalid {
                attempts_remaining: 4
            })
        );
    }
    assert!(f.pairing.redeem(&second.code, "Intent").await.is_ok());
}

#[tokio::test]
async fn cancel_by_user_and_by_id() {
    let f = fixture_default().await;
    assert!(!f.pairing.cancel_code(None).await);
    let created = f.pairing.create_code().await.unwrap();
    assert!(!f.pairing.cancel_code(Some("other".into())).await);
    assert!(
        f.pairing
            .cancel_code(Some(created.local_code.code_id.clone()))
            .await
    );
    assert!(!f.pairing.cancel_code(None).await);
    assert_eq!(
        f.pairing.redeem(&created.code, "Intent").await.err(),
        Some(LocalPairingError::Expired)
    );
    let slot = f.pairing.slot.lock().await;
    let code = &slot.as_ref().unwrap().code;
    assert_eq!(code.state, LocalPairingCodeState::Cancelled);
    assert_eq!(code.cancel_reason, Some(LocalPairingCancelReason::User));
}

#[tokio::test]
async fn revoke_refuses_and_broadcasts() {
    let f = fixture_default().await;
    let mut revocations = f.pairing.subscribe_revocations();
    let mut events = f.bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::PAIRING_LOCAL_CREDENTIALS_CHANGED.into()],
        ..Default::default()
    });
    let created = f.pairing.create_code().await.unwrap();
    let grant = f.pairing.redeem(&created.code, "Intent").await.unwrap();
    let minted = events.recv().await.unwrap();
    assert_eq!(
        minted[0].data["credential"]["credentialId"],
        grant.credential_id.as_str()
    );
    assert!(!minted[0].data.to_string().contains(&grant.token));

    assert!(
        f.pairing
            .credential_active(grant.credential_id.clone())
            .await
    );
    assert!(f
        .pairing
        .revoke_credential(grant.credential_id.clone())
        .await
        .unwrap());
    assert_eq!(revocations.recv().await.unwrap(), grant.credential_id);
    let revoked = events.recv().await.unwrap();
    assert_eq!(revoked[0].data["revoked"], true);
    assert_eq!(
        revoked[0].data["credentialId"],
        grant.credential_id.as_str()
    );

    assert!(
        !f.pairing
            .credential_active(grant.credential_id.clone())
            .await
    );
    assert_eq!(f.pairing.resolve_credential(hash(&grant.token)).await, None);
    assert!(!f
        .pairing
        .revoke_credential(grant.credential_id.clone())
        .await
        .unwrap());
    assert_eq!(
        f.pairing.revoke_credential("unknown".into()).await.err(),
        Some(LocalPairingError::NotFound)
    );
    assert!(f.pairing.list_credentials().await.unwrap().is_empty());
}

#[tokio::test]
async fn connected_flag_follows_live_connections() {
    let f = fixture_default().await;
    let mut events = f.bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::PAIRING_LOCAL_CREDENTIALS_CHANGED.into()],
        ..Default::default()
    });
    let created = f.pairing.create_code().await.unwrap();
    let grant = f.pairing.redeem(&created.code, "Intent").await.unwrap();
    let _minted = events.recv().await.unwrap();

    f.pairing.connection_opened(&grant.credential_id);
    f.pairing.connection_opened(&grant.credential_id);
    let opened = events.recv().await.unwrap();
    assert_eq!(opened[0].data["credential"]["connected"], true);
    assert!(f.pairing.list_credentials().await.unwrap()[0].connected);

    f.pairing.connection_closed(&grant.credential_id);
    assert!(f.pairing.list_credentials().await.unwrap()[0].connected);
    f.pairing.connection_closed(&grant.credential_id);
    let closed = events.recv().await.unwrap();
    assert_eq!(closed[0].data["credential"]["connected"], false);
    assert!(!f.pairing.list_credentials().await.unwrap()[0].connected);
}

#[test]
fn drawn_codes_use_the_alphabet() {
    for _ in 0..200 {
        let code = super::draw_code().unwrap();
        assert!(
            intent_core::local_pairing::normalize_code(&code).as_deref() == Some(code.as_str())
        );
    }
}

#[test]
fn lifetime_seam_only_shortens() {
    assert_eq!(code_lifetime_from(None), CODE_LIFETIME);
    assert_eq!(
        code_lifetime_from(Some("1500")),
        std::time::Duration::from_millis(1500)
    );
    assert_eq!(code_lifetime_from(Some("50")), CODE_LIFETIME);
    assert_eq!(code_lifetime_from(Some("junk")), CODE_LIFETIME);
    assert_eq!(code_lifetime_from(Some("3600000")), CODE_LIFETIME);
}
