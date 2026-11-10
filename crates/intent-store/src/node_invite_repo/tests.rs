use super::*;
use intent_core::ExecutorCredentialId;

async fn open() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("invites.db")).await.unwrap();
    (dir, store)
}

fn invite(id: &str, expires_at: &str) -> NodeInvite {
    NodeInvite {
        id: id.into(),
        label: None,
        created_at: now_iso(),
        expires_at: expires_at.into(),
        used_at: None,
        credential_id: None,
        revoked_at: None,
    }
}

fn credential(id: &str, backend: &str) -> ExecutorCredential {
    ExecutorCredential {
        id: ExecutorCredentialId::from(id.to_owned()),
        backend_id: backend.into(),
        backend_name: format!("{backend} name"),
        backend_fingerprint: None,
        created_at: now_iso(),
        last_used_at: None,
        revoked_at: None,
    }
}

async fn claim(store: &Store, n: usize) -> std::result::Result<(), NodeInviteRefusal> {
    store
        .claim_node_invite(
            "ni-1",
            "hash",
            &format!("b-{n}"),
            &credential(&format!("c-{n}"), &format!("b-{n}")),
            &format!("token-{n}"),
            None,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn racing_claims_spend_an_invite_once() {
    let (_dir, store) = open().await;
    store
        .insert_node_invite(&invite("ni-1", "2999-01-01T00:00:00Z"), "hash")
        .await
        .unwrap();
    let outcomes = futures_util::future::join_all((0..8).map(|n| claim(&store, n))).await;
    assert_eq!(
        outcomes.iter().filter(|o| o.is_ok()).count(),
        1,
        "{outcomes:?}"
    );
    assert!(outcomes
        .iter()
        .all(|o| o.is_ok() || *o == Err(NodeInviteRefusal::Used)));
    assert_eq!(
        store
            .list_active_executor_credentials()
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(store.claimed_node_invites().await.unwrap().len(), 1);
    assert!(store.list_open_node_invites().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_claim_racing_a_revoke_has_one_winner() {
    let (_dir, store) = open().await;
    store
        .insert_node_invite(&invite("ni-1", "2999-01-01T00:00:00Z"), "hash")
        .await
        .unwrap();
    let (claimed, revoked) = tokio::join!(claim(&store, 1), store.revoke_node_invite("ni-1"));
    let revoked = revoked.unwrap();
    let won = claimed.is_ok();
    match claimed {
        Ok(()) => assert_eq!(revoked, NodeInviteRevocation::Closed),
        Err(refusal) => {
            assert_eq!(refusal, NodeInviteRefusal::Revoked);
            assert_eq!(revoked, NodeInviteRevocation::Revoked);
        }
    }
    let minted = store
        .list_active_executor_credentials()
        .await
        .unwrap()
        .len();
    assert_eq!(minted, usize::from(won));
}

#[tokio::test]
async fn refusals_spend_nothing() {
    let (_dir, store) = open().await;
    store
        .insert_node_invite(&invite("ni-1", "2000-01-01T00:00:00Z"), "hash")
        .await
        .unwrap();
    assert_eq!(
        claim(&store, 1).await,
        Err(NodeInviteRefusal::Expired {
            expires_at: "2000-01-01T00:00:00Z".into()
        })
    );
    let wrong = store
        .claim_node_invite("ni-1", "other", "b", &credential("c-x", "b"), "t", None)
        .await
        .unwrap();
    assert_eq!(wrong, Err(NodeInviteRefusal::Invalid));
    store
        .insert_node_invite(&invite("ni-2", "2999-01-01T00:00:00Z"), "hash-2")
        .await
        .unwrap();
    let late = store
        .claim_node_invite(
            "ni-2",
            "hash-2",
            "b",
            &credential("c-y", "b"),
            "t2",
            Some("node-invite-self"),
        )
        .await
        .unwrap();
    assert_eq!(late, Err(NodeInviteRefusal::Late("node-invite-self")));
    assert!(store
        .list_active_executor_credentials()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.list_open_node_invites().await.unwrap().len(), 1);
}
