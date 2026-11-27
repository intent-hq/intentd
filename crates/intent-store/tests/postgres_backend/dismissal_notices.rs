//! Questions-dismissed notice claims (`agent.dismissQuestions`): one notice
//! per (agent, dismissed message id), however many processes dismiss it.
//!
//! Invariant: when N callers race to claim the same id, from several
//! handles (two processes on Postgres), exactly one wins. An id claimed
//! earlier stays claimed after other ids (A -> B -> A). A released claim
//! can be won again, and deleting the agent drops its claims.

use intent_core::{AgentId, AgentSession, WorkspaceId};

use crate::concurrency_support::{handles, race, WRITERS};
use crate::harness::parity_test;

async fn insert_agent(store: &intent_store::Store, id: &AgentId) {
    store
        .insert_agent_session(&AgentSession {
            id: id.clone(),
            workspace_id: WorkspaceId("__chief__".into()),
            name: "Dismissal test".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            ..Default::default()
        })
        .await
        .unwrap();
}

parity_test!(
    a_dismissal_notice_is_claimed_once_across_handles,
    |store, tenant, db| {
        let agent = AgentId("dismissing-agent".into());
        let other = AgentId("other-agent".into());
        insert_agent(&store, &agent).await;
        insert_agent(&store, &other).await;
        if tenant == "decoy" {
            // Rows with the subject's keys, so a missing tenant filter in
            // the subject's run shows up.
            for (who, id) in [(&agent, "m1"), (&agent, "m2"), (&other, "m1")] {
                assert!(store.claim_dismissal_notice(who, id).await.unwrap());
            }
            return;
        }

        let all = handles(&store, db, tenant).await;
        let wins = race(&all, WRITERS, |_, handle| {
            let agent = agent.clone();
            async move { handle.claim_dismissal_notice(&agent, "m1").await.unwrap() }
        })
        .await;
        assert_eq!(
            wins.iter().filter(|won| **won).count(),
            1,
            "exactly one concurrent dismissal of m1 claims its notice: {wins:?}"
        );

        // A -> B -> A, the second A from the other handle (process).
        let second = &all[all.len() - 1];
        assert!(store.claim_dismissal_notice(&agent, "m2").await.unwrap());
        assert!(
            !second.claim_dismissal_notice(&agent, "m1").await.unwrap(),
            "an older id stays claimed after a newer dismissal"
        );
        // Claims are per agent.
        assert!(second.claim_dismissal_notice(&other, "m1").await.unwrap());

        // A released claim (the marker write failed) is won by the retry.
        second.release_dismissal_notice(&agent, "m1").await.unwrap();
        assert!(store.claim_dismissal_notice(&agent, "m1").await.unwrap());
        assert!(!second.claim_dismissal_notice(&agent, "m1").await.unwrap());

        // Deleting the agent drops its claims.
        assert!(store
            .delete_agent_session(&WorkspaceId("__chief__".into()), &agent)
            .await
            .unwrap());
        insert_agent(&store, &agent).await;
        assert!(store.claim_dismissal_notice(&agent, "m1").await.unwrap());
        assert!(!store.claim_dismissal_notice(&other, "m1").await.unwrap());
    }
);
