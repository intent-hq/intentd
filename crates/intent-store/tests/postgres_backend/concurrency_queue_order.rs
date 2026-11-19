//! Queue order under concurrent appends (spec §6.5.3).
//!
//! Invariant: an agent's transcript is a gapless queue. When appends race
//! on one agent, from several handles, the committed messages hold the
//! sequence numbers `0..committed` exactly once each (no duplicate, no gap),
//! and each writer's committed appends keep the order it made them in.
//!
//! The backends differ in how a race ends:
//! - Postgres reads `MAX(seq) + 1` and inserts inside one writer-locked
//!   transaction, so every append commits: the test requires that. Without
//!   the lock two appends read the same `MAX(seq)` and
//!   `UNIQUE(tenant_id, agent_id, seq)` refuses one of them.
//! - `SQLite` reads `MAX(seq)` on the read pool before its write
//!   transaction, and `UNIQUE(agent_id, seq)` refuses the loser of a race
//!   (documented on `append_agent_message_with_id`; the agent manager
//!   serializes appends per agent). A refused append commits nothing.

use intent_core::{AgentId, AgentSession, AgentStatus, Error, WorkspaceId};
use serde_json::json;

use crate::concurrency_support::{handles, race, WRITERS};
use crate::harness::parity_test;

const AT: &str = "2026-03-01T00:00:00.000Z";
/// Appends per writer, made one after another.
const PER_WRITER: usize = 4;

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

fn session(id: &str, tenant: &str) -> AgentSession {
    AgentSession {
        id: aid(id),
        workspace_id: WorkspaceId("__chief__".into()),
        name: format!("{id} of {tenant}"),
        status: AgentStatus::Idle,
        created_at: AT.into(),
        updated_at: AT.into(),
        ..AgentSession::default()
    }
}

parity_test!(
    concurrent_appends_keep_one_agent_queue_gapless,
    |store, tenant, db| {
        store
            .insert_agent_session(&session("q-agent", tenant))
            .await
            .unwrap();
        let all = handles(&store, db, tenant).await;
        let owned = tenant.to_owned();
        let refused: Vec<Vec<String>> = race(&all, WRITERS, |writer, store| {
            let tenant = owned.clone();
            async move {
                let mut refused = Vec::new();
                for n in 0..PER_WRITER {
                    let appended = store
                        .append_agent_message(
                            &aid("q-agent"),
                            "user",
                            &json!([{"type": "text", "text": format!("{tenant} w{writer} n{n}")}]),
                            AT,
                        )
                        .await;
                    match appended {
                        Ok(_) => {}
                        Err(Error::Internal(e)) if e.contains("UNIQUE constraint failed") => {
                            refused.push(format!("{tenant} w{writer} n{n}"));
                        }
                        Err(e) => panic!("writer {writer} append {n}: {e}"),
                    }
                }
                refused
            }
        })
        .await;
        let refused: Vec<String> = refused.into_iter().flatten().collect();
        if db.is_some() {
            assert_eq!(
                refused,
                Vec::<String>::new(),
                "the writer lock admits every append"
            );
        }

        let messages = store
            .get_agent_messages(&aid("q-agent"), None)
            .await
            .unwrap();
        let mut seqs: Vec<i64> = messages.iter().map(|m| m.seq).collect();
        seqs.sort_unstable();
        let total = i64::try_from(WRITERS * PER_WRITER - refused.len()).unwrap();
        assert_eq!(
            seqs,
            (0..total).collect::<Vec<_>>(),
            "every append gets its own sequence number, with no gap"
        );
        let mut by_seq = messages;
        by_seq.sort_by_key(|m| m.seq);
        for writer in 0..WRITERS {
            let order: Vec<String> = by_seq
                .iter()
                .map(|m| m.content[0]["text"].as_str().unwrap().to_owned())
                .filter(|text| text.starts_with(&format!("{tenant} w{writer} ")))
                .collect();
            let made: Vec<String> = (0..PER_WRITER)
                .map(|n| format!("{tenant} w{writer} n{n}"))
                .filter(|text| !refused.contains(text))
                .collect();
            assert_eq!(order, made, "writer {writer}'s appends keep their order");
        }
    }
);
