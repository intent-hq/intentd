//! Unique claims under concurrent claimers (spec §6.5.3).
//!
//! Invariant: a single-use node invite is spent exactly once. When N
//! backends race to claim it, from several handles, exactly one gets `Ok`,
//! every other one gets `Used`, and only the winner's executor credential
//! exists afterwards (a loser's credential insert rolls back with its
//! claim).

use intent_core::{machines::ExecutorCredential, ExecutorCredentialId};
use intent_store::{NodeInvite, NodeInviteRefusal};

use crate::concurrency_support::{handles, race, WRITERS};
use crate::harness::parity_test;

const T0: &str = "2026-01-01T00:00:00.000Z";
const FUTURE: &str = "2999-01-01T00:00:00.000Z";

fn invite(tenant: &str) -> NodeInvite {
    NodeInvite {
        id: "inv-race".into(),
        label: Some(format!("{tenant} race")),
        created_at: T0.into(),
        expires_at: FUTURE.into(),
        used_at: None,
        credential_id: None,
        revoked_at: None,
    }
}

/// A claimer's credential. The id holds a UUID, so the decoy coverage check
/// skips it: which claimer wins differs between runs.
fn credential(tenant: &str, writer: usize) -> ExecutorCredential {
    ExecutorCredential {
        id: ExecutorCredentialId::from(format!("cred-{}", uuid::Uuid::new_v4())),
        backend_id: format!("backend-{writer}"),
        backend_name: format!("{tenant} backend {writer}"),
        backend_fingerprint: None,
        created_at: T0.into(),
        last_used_at: None,
        revoked_at: None,
    }
}

parity_test!(
    concurrent_claimers_spend_a_node_invite_once,
    |store, tenant, db| {
        let secret = format!("{tenant}-secret");
        store
            .insert_node_invite(&invite(tenant), &secret)
            .await
            .unwrap();
        let all = handles(&store, db, tenant).await;
        let owned = tenant.to_owned();
        let outcomes = race(&all, WRITERS, |writer, store| {
            let credential = credential(&owned, writer);
            let secret = secret.clone();
            let token = format!("{}-token-{}", owned, uuid::Uuid::new_v4());
            async move {
                let outcome = store
                    .claim_node_invite(
                        "inv-race",
                        &secret,
                        &credential.backend_id,
                        &credential,
                        &token,
                        None,
                    )
                    .await
                    .unwrap();
                (outcome, credential.id)
            }
        })
        .await;

        let winners: Vec<&ExecutorCredentialId> = outcomes
            .iter()
            .filter(|(outcome, _)| outcome.is_ok())
            .map(|(_, id)| id)
            .collect();
        assert_eq!(winners.len(), 1, "exactly one claim wins: {outcomes:?}");
        for (outcome, _) in &outcomes {
            assert!(
                matches!(outcome, Ok(()) | Err(NodeInviteRefusal::Used)),
                "a losing claim is refused as used: {outcome:?}"
            );
        }
        assert_eq!(
            store.claimed_node_invites().await.unwrap(),
            vec![(winners[0].as_str().to_owned(), "inv-race".to_owned())]
        );
        for (outcome, id) in &outcomes {
            assert_eq!(
                store.get_executor_credential(id).await.unwrap().is_some(),
                outcome.is_ok(),
                "only the winner's credential is kept"
            );
        }
    }
);
