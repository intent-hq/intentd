//! Workspace delete against concurrent desktop writes (spec §6.5.3).
//!
//! Invariant: no desktop record outlives its workspace. While one task
//! deletes a workspace, writers on several handles record desktop sessions
//! for it and for a second workspace. Afterwards:
//! - the delete succeeded;
//! - every write for the deleted workspace either committed (and the delete
//!   then removed it), was refused by the handle's teardown gate
//!   (`Forbidden`), or found its agent gone (`NotFound`), and none of its
//!   records is left;
//! - every write for the other workspace committed and its record is kept.
//!
//! Leftovers are counted as raw rows: the read API hides a record whose
//! workspace is gone, so it could not show one that outlived the delete.
//!
//! A second handle's teardown gate does not see the delete (audit finding
//! 3), so for its writes the invariant rests on the database alone: the
//! writer lock orders the write against the delete, and the insert checks
//! inside it that the agent and workspace still exist.

use intent_core::{AgentId, AgentSession, AgentStatus, Error, Workspace, WorkspaceId};
use intent_store::Store;
use serde_json::json;

use crate::concurrency_support::{handles, race, WRITERS};
use crate::harness::{count_rows, parity_test};

const AT: &str = "2026-04-01T00:00:00.000Z";
const DOOMED: &str = "ws-doomed";
const KEPT: &str = "__chief__";

fn session(id: &str, workspace: &str, tenant: &str) -> AgentSession {
    AgentSession {
        id: AgentId(id.into()),
        workspace_id: WorkspaceId(workspace.into()),
        name: format!("{id} of {tenant}"),
        status: AgentStatus::Idle,
        created_at: AT.into(),
        updated_at: AT.into(),
        ..AgentSession::default()
    }
}

/// Records each writer makes, one after another, so the writes spread over
/// the whole delete.
const PER_WRITER: usize = 6;

/// Task 0 deletes. Writers alternate in pairs between the doomed and the
/// kept workspace, so each target gets writers on every handle (task `i`
/// runs on handle `i % 2`).
fn target(task: usize) -> (&'static str, &'static str) {
    if task % 4 < 2 {
        (DOOMED, "a-doomed")
    } else {
        (KEPT, "a-kept")
    }
}

fn record_id(task: usize, n: usize) -> String {
    format!("s-{task}-{n}")
}

#[derive(Debug)]
enum Outcome {
    Deleted,
    Wrote(Vec<Result<(), Error>>),
}

async fn write(store: &Store, tenant: &str, task: usize) -> Vec<Result<(), Error>> {
    let (workspace, agent) = target(task);
    let principal = store.get_primary_principal().await.unwrap().id;
    let mut results = Vec::new();
    for n in 0..PER_WRITER {
        let id = record_id(task, n);
        results.push(
            store
                .desktop_insert_terminal(
                    &id,
                    &WorkspaceId(workspace.into()),
                    &AgentId(agent.into()),
                    &json!({
                        "principalId": principal, "computerId": "comp",
                        "requestId": format!("r-{id}"), "tenant": tenant,
                    }),
                    &format!("hash-{id}"),
                )
                .await,
        );
    }
    results
}

parity_test!(
    no_desktop_record_outlives_a_concurrent_workspace_delete,
    |store, tenant, db| {
        store
            .insert_workspace(&Workspace {
                id: WorkspaceId(DOOMED.into()),
                title: format!("doomed of {tenant}"),
                branch: "main".into(),
                created_at: AT.into(),
                updated_at: AT.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .insert_agent_session(&session("a-doomed", DOOMED, tenant))
            .await
            .unwrap();
        store
            .insert_agent_session(&session("a-kept", KEPT, tenant))
            .await
            .unwrap();
        let all = handles(&store, db, tenant).await;
        let owned = tenant.to_owned();
        let tasks = WRITERS * 2 + 1;
        let outcomes = race(&all, tasks, |task, store| {
            let tenant = owned.clone();
            async move {
                if task == 0 {
                    store
                        .delete_workspace(&WorkspaceId(DOOMED.into()))
                        .await
                        .unwrap();
                    Outcome::Deleted
                } else {
                    Outcome::Wrote(write(&store, &tenant, task).await)
                }
            }
        })
        .await;

        assert!(matches!(outcomes[0], Outcome::Deleted));
        // Raw rows: `desktop_terminal` hides a record whose workspace is
        // gone, so it cannot show one that outlived the delete. The kept
        // workspace's count is the positive control: it proves the pattern
        // matches the stored records, so the zero is not vacuous.
        let rows_of = |workspace: &'static str| {
            let filter = format!(
                "key LIKE 'desktop.v1/%' AND value LIKE '%\"workspaceId\":\"{workspace}\"%'"
            );
            let store = store.clone();
            async move { count_rows(&store, db, tenant, "settings", &filter).await }
        };
        let kept_writes = (1..tasks).filter(|&t| target(t).0 == KEPT).count() * PER_WRITER;
        assert_eq!(kept_writes, 48, "8 kept writers x {PER_WRITER} writes");
        assert_eq!(
            rows_of(KEPT).await,
            i64::try_from(kept_writes).unwrap(),
            "the row pattern matches every kept record"
        );
        assert_eq!(
            rows_of(DOOMED).await,
            0,
            "a desktop record outlived its workspace"
        );
        // How the doomed writes ended varies run to run; print it, do not
        // assert it.
        let (mut removed, mut not_found, mut forbidden) = (0, 0, 0);
        for outcome in outcomes
            .iter()
            .enumerate()
            .skip(1)
            .filter(|(t, _)| target(*t).0 == DOOMED)
        {
            if let (_, Outcome::Wrote(results)) = outcome {
                for result in results {
                    match result {
                        Ok(()) => removed += 1,
                        Err(Error::NotFound(_)) => not_found += 1,
                        Err(Error::Forbidden(_)) => forbidden += 1,
                        Err(_) => {}
                    }
                }
            }
        }
        eprintln!(
            "[{tenant}] doomed writes: {removed} committed then removed, \
             {not_found} NotFound, {forbidden} Forbidden"
        );
        for (task, outcome) in outcomes.iter().enumerate().skip(1) {
            let Outcome::Wrote(results) = outcome else {
                panic!("task {task} is a writer");
            };
            for (n, result) in results.iter().enumerate() {
                let left = store.desktop_terminal(&record_id(task, n)).await.unwrap();
                if target(task).0 == DOOMED {
                    assert!(
                        matches!(
                            result,
                            Ok(()) | Err(Error::Forbidden(_) | Error::NotFound(_))
                        ),
                        "task {task} write {n}: a write racing the delete commits or is \
                         refused: {result:?}"
                    );
                    assert_eq!(
                        left, None,
                        "task {task} write {n}: the deleted record reads"
                    );
                } else {
                    assert!(result.is_ok(), "task {task} write {n}: {result:?}");
                    assert!(
                        left.is_some(),
                        "task {task} write {n}: the other workspace's record is kept"
                    );
                }
            }
        }
    }
);
