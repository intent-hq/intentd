use super::*;
use intent_core::{Workspace, WorkspaceGitRoot, WorkspaceGitRootId};

struct Fixture {
    store: Store,
    root: RepositoryRootId,
    dir: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("selection.db")).await.unwrap();
        let root = RepositoryRootId {
            workspace_id: WorkspaceId::from("selection-workspace"),
            kind: RepositoryRootKind::Primary,
        };
        workspace(&store, &root.workspace_id, None).await;
        Self { store, root, dir }
    }
    async fn read(&self) -> RepositorySelectionSnapshot {
        self.store
            .repository_selection_snapshot(&self.root)
            .await
            .unwrap()
    }
    async fn set(&self, name: &str) -> RepositorySelectionSnapshot {
        let old = self.read().await;
        applied(
            self.store
                .write_repository_selection(&old, remote(name))
                .await,
        )
    }
    async fn registered(&self, id: &str, path: &str) -> RepositoryRootId {
        let row: WorkspaceGitRoot = serde_json::from_value(serde_json::json!({
            "id":id,"workspaceId":self.root.workspace_id,"path":path,"source":"agent",
            "registeredByAgentIds":[],"createdAt":"same-time","updatedAt":"same-time"
        }))
        .unwrap();
        self.store.upsert_workspace_git_root(&row).await.unwrap();
        RepositoryRootId {
            workspace_id: self.root.workspace_id.clone(),
            kind: RepositoryRootKind::Registered {
                git_root_id: WorkspaceGitRootId::from(id),
            },
        }
    }
}
async fn workspace(store: &Store, id: &WorkspaceId, hint: Option<&str>) {
    let row:Workspace=serde_json::from_value(serde_json::json!({
        "id":id,"title":"Selection","branch":"main","status":"Active","activity":"idle","attention":"none",
        "createdAt":"same-time","updatedAt":"same-time","tags":[],"skipWorktree":false,"isRemote":false,"archived":false,
        "repositoryPath":"/original","repositoryOwner":hint
    })).unwrap();
    store.insert_workspace(&row).await.unwrap();
}
fn remote(s: &str) -> RepositorySelectionChange {
    RepositorySelectionChange::ExplicitRemote {
        remote_name: s.into(),
    }
}
fn applied(out: RepositorySelectionWriteOutcome) -> RepositorySelectionSnapshot {
    assert!(matches!(
        out.persistence,
        RepositorySelectionPersistence::Committed { .. }
    ));
    match out.result.unwrap() {
        RepositorySelectionWriteResult::Applied(s) => s,
        _ => panic!("expected original commit"),
    }
}
fn saved(s: &RepositorySelectionSnapshot) -> &SavedReviewSelection {
    match s.selection().unwrap() {
        RepositoryStoredSelection::Saved(s) => s,
        _ => panic!("expected saved intent"),
    }
}
fn unresolved(s: &RepositorySelectionSnapshot) {
    assert!(matches!(
        saved(s),
        SavedReviewSelection::UnresolvedHistorical { .. }
    ));
}

#[tokio::test]
async fn absence_automatic_remote_reset_are_distinct_and_reopen_keeps_reset() {
    let f = Fixture::new().await;
    let initial = f.read().await;
    assert_eq!(
        initial.selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
    let auto = applied(
        f.store
            .write_repository_selection(&initial, RepositorySelectionChange::Automatic)
            .await,
    );
    assert_eq!(saved(&auto), &SavedReviewSelection::Automatic);
    let selected = applied(
        f.store
            .write_repository_selection(&auto, remote("upstream"))
            .await,
    );
    assert_eq!(
        saved(&selected),
        &SavedReviewSelection::ExplicitRemote {
            remote_name: "upstream".into()
        }
    );
    let reset = applied(f.store.reset_repository_selection(&selected).await);
    assert_eq!(reset.selection(), Some(&RepositoryStoredSelection::Reset));
    assert_eq!(reset.root_incarnation(), initial.root_incarnation());
    assert!(
        reset.selection_revision().unwrap().get() > initial.selection_revision().unwrap().get()
    );
    sqlx::query(
        "UPDATE workspace SET repository_owner='unproved',repository_name='hint' WHERE id=?",
    )
    .bind(f.root.workspace_id.as_str())
    .execute(f.store.write_pool())
    .await
    .unwrap();
    let reopened = Store::open(&f.dir.path().join("selection.db"))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .repository_selection_snapshot(&f.root)
            .await
            .unwrap()
            .selection(),
        Some(&RepositoryStoredSelection::Reset)
    );
}

#[tokio::test]
async fn no_op_and_stale_aba_comparisons_do_not_write() {
    let f = Fixture::new().await;
    let original = f.set("one").await;
    let noop = f
        .store
        .write_repository_selection(&original, remote("one"))
        .await;
    assert_eq!(noop.persistence, RepositorySelectionPersistence::NoEffect);
    assert!(matches!(
        noop.result,
        Ok(RepositorySelectionWriteResult::Unchanged(_))
    ));
    f.set("two").await;
    f.set("one").await;
    let conflict = f
        .store
        .write_repository_selection(&original, remote("three"))
        .await;
    assert_eq!(
        conflict.persistence,
        RepositorySelectionPersistence::NoEffect
    );
    assert!(matches!(
        conflict.result,
        Ok(RepositorySelectionWriteResult::Conflict(_))
    ));
    assert_eq!(saved(&f.read().await), saved(&original));
    assert!(
        f.read().await.selection_revision().unwrap().get()
            > original.selection_revision().unwrap().get()
    );
}

#[tokio::test]
async fn multiple_roots_workspaces_and_foreign_domains_never_alias() {
    let f = Fixture::new().await;
    let a = f.registered("root-a", "/a").await;
    let b = f.registered("root-b", "/b").await;
    let old = f.store.repository_selection_snapshot(&a).await.unwrap();
    applied(
        f.store
            .write_repository_selection(&old, remote("origin-a"))
            .await,
    );
    assert_eq!(
        f.store
            .repository_selection_snapshot(&b)
            .await
            .unwrap()
            .selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
    assert_eq!(
        f.read().await.selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
    let missing = RepositoryRootId {
        workspace_id: WorkspaceId::from("foreign-workspace"),
        kind: a.kind.clone(),
    };
    workspace(&f.store, &missing.workspace_id, None).await;
    let missing = f
        .store
        .repository_selection_snapshot(&missing)
        .await
        .unwrap();
    assert!(missing.binding().is_none());
    assert!(matches!(
        f.store
            .write_repository_selection(&missing, remote("x"))
            .await
            .result,
        Ok(RepositorySelectionWriteResult::MissingRoot(_))
    ));
    let other = Fixture::new().await;
    let refused = other
        .store
        .write_repository_selection(&old, remote("bad"))
        .await;
    assert_eq!(
        refused.persistence,
        RepositorySelectionPersistence::NotAttempted
    );
    assert!(refused.result.is_err());
}

#[tokio::test]
async fn registered_delete_recreate_equal_ids_and_paths_retains_unresolved_intent() {
    let f = Fixture::new().await;
    let root = f.registered("root", "/same").await;
    let original = f.store.repository_selection_snapshot(&root).await.unwrap();
    let selected = applied(
        f.store
            .write_repository_selection(&original, remote("old"))
            .await,
    );
    f.store
        .delete_workspace_git_root(&WorkspaceGitRootId::from("root"))
        .await
        .unwrap();
    let deleted = f.store.repository_selection_snapshot(&root).await.unwrap();
    assert!(deleted.binding().is_none());
    unresolved(&deleted);
    f.registered("root", "/same").await;
    let recreated = f.store.repository_selection_snapshot(&root).await.unwrap();
    unresolved(&recreated);
    assert!(
        recreated.root_incarnation().unwrap().get() > selected.root_incarnation().unwrap().get()
    );
    assert!(matches!(
        f.store
            .write_repository_selection(&selected, remote("old"))
            .await
            .result,
        Ok(RepositorySelectionWriteResult::Conflict(_))
    ));
    let reset = applied(f.store.reset_repository_selection(&recreated).await);
    assert_eq!(reset.selection(), Some(&RepositoryStoredSelection::Reset));
}

#[tokio::test]
async fn primary_binding_aba_and_workspace_recreation_never_restore_a_choice() {
    let f = Fixture::new().await;
    let selected = f.set("old").await;
    for path in ["/other", "/original"] {
        sqlx::query("UPDATE workspace SET repository_path=? WHERE id=?")
            .bind(path)
            .bind(f.root.workspace_id.as_str())
            .execute(f.store.write_pool())
            .await
            .unwrap();
    }
    unresolved(&f.read().await);
    assert_eq!(f.read().await.binding(), selected.binding());
    f.store
        .delete_workspace(&f.root.workspace_id)
        .await
        .unwrap();
    workspace(&f.store, &f.root.workspace_id, None).await;
    unresolved(&f.read().await);
    assert!(
        f.read().await.root_incarnation().unwrap().get()
            > selected.root_incarnation().unwrap().get()
    );
}

#[tokio::test]
async fn registered_moves_advance_old_and_new_keys_and_cascade_preserves_tombstones() {
    let f = Fixture::new().await;
    let root = f.registered("old-root", "/old").await;
    let old = f.store.repository_selection_snapshot(&root).await.unwrap();
    applied(
        f.store
            .write_repository_selection(&old, remote("old"))
            .await,
    );
    let other = WorkspaceId::from("other");
    workspace(&f.store, &other, None).await;
    sqlx::query("UPDATE workspace_git_root SET id='new-root',workspace_id=?,path='/new' WHERE id='old-root'")
        .bind(other.as_str()).execute(f.store.write_pool()).await.unwrap();
    let gone = f.store.repository_selection_snapshot(&root).await.unwrap();
    assert!(gone.binding().is_none());
    unresolved(&gone);
    let moved = RepositoryRootId {
        workspace_id: other.clone(),
        kind: RepositoryRootKind::Registered {
            git_root_id: WorkspaceGitRootId::from("new-root"),
        },
    };
    let next = f.store.repository_selection_snapshot(&moved).await.unwrap();
    assert!(next.binding().is_some());
    sqlx::query("DELETE FROM workspace WHERE id=?")
        .bind(other.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let after = f.store.repository_selection_snapshot(&moved).await.unwrap();
    assert!(after.binding().is_none());
    assert!(after.root_incarnation().unwrap().get() > next.root_incarnation().unwrap().get());
}

#[tokio::test]
async fn harmless_metadata_and_failed_or_rolled_back_source_writes_preserve_facts() {
    let f = Fixture::new().await;
    let root = f.registered("root", "/root").await;
    let before = f.store.repository_selection_snapshot(&root).await.unwrap();
    let primary = f.read().await;
    sqlx::query("UPDATE workspace SET branch='other',title='renamed',updated_at='later',pr_number=123 WHERE id=?")
        .bind(f.root.workspace_id.as_str()).execute(f.store.write_pool()).await.unwrap();
    sqlx::query("UPDATE workspace_git_root SET registered_by_agent_ids='[\"a\"]',registered_commit_sha='sha',pr_number=123,updated_at='later' WHERE id='root'")
        .execute(f.store.write_pool()).await.unwrap();
    assert!(primary.matches(&f.read().await));
    assert!(before.matches(&f.store.repository_selection_snapshot(&root).await.unwrap()));
    let mut tx = f.store.write_pool().begin().await.unwrap();
    sqlx::query("UPDATE workspace_git_root SET path='/other' WHERE id='root'")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(before.matches(&f.store.repository_selection_snapshot(&root).await.unwrap()));
    assert!(
        sqlx::query("UPDATE workspace_git_root SET workspace_id='missing' WHERE id='root'")
            .execute(f.store.write_pool())
            .await
            .is_err()
    );
    assert!(before.matches(&f.store.repository_selection_snapshot(&root).await.unwrap()));
}

#[tokio::test]
async fn overflow_aborts_source_write_even_with_ignore_and_tombstones_cannot_replace() {
    let f = Fixture::new().await;
    sqlx::query("UPDATE repository_selection_state SET root_incarnation=9223372036854775807,selection_revision=9223372036854775807 WHERE workspace_id=?")
        .bind(f.root.workspace_id.as_str()).execute(f.store.write_pool()).await.unwrap();
    let before = f.read().await;
    assert!(
        sqlx::query("UPDATE OR IGNORE workspace SET repository_path='/overflow' WHERE id=?")
            .bind(f.root.workspace_id.as_str())
            .execute(f.store.write_pool())
            .await
            .is_err()
    );
    assert!(before.matches(&f.read().await));
    assert!(sqlx::query("DELETE FROM repository_selection_state")
        .execute(f.store.write_pool())
        .await
        .is_err());
    assert!(sqlx::query("INSERT OR REPLACE INTO repository_selection_state SELECT * FROM repository_selection_state")
        .execute(f.store.write_pool()).await.is_err());
    assert!(
        sqlx::query("UPDATE OR IGNORE repository_selection_state SET selection_revision=1")
            .execute(f.store.write_pool())
            .await
            .is_err()
    );
    assert!(before.matches(&f.read().await));
}

#[tokio::test]
async fn strict_new_format_refuses_partial_canonical_and_missing_provenance() {
    let f = Fixture::new().await;
    sqlx::query("DROP TRIGGER repository_selection_validate_update")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    sqlx::query("UPDATE repository_selection_state SET selection_revision=selection_revision+1,choice_mode='canonical'")
        .execute(f.store.write_pool()).await.unwrap();
    assert!(f
        .store
        .repository_selection_snapshot(&f.root)
        .await
        .is_err());
    sqlx::query("UPDATE repository_selection_state SET selection_revision=selection_revision+1,choice_mode='unresolved',historical_source='workspace-metadata'")
        .execute(f.store.write_pool()).await.unwrap();
    assert!(f
        .store
        .repository_selection_snapshot(&f.root)
        .await
        .is_err());
    sqlx::query("DROP TRIGGER repository_selection_no_delete")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM repository_selection_state")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    assert!(f
        .store
        .repository_selection_snapshot(&f.root)
        .await
        .is_err());
}

#[tokio::test]
async fn migration_bootstrap_retains_unproved_hints_without_copying_them() {
    let f = Fixture::new().await;
    let triggers:Vec<String>=sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'repository_selection_%'")
        .fetch_all(f.store.read_pool()).await.unwrap();
    for trigger in triggers {
        sqlx::query(&format!("DROP TRIGGER {trigger}"))
            .execute(f.store.write_pool())
            .await
            .unwrap();
    }
    sqlx::query("DROP TABLE repository_selection_state")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let hinted = WorkspaceId::from("hinted");
    workspace(
        &f.store,
        &hinted,
        Some("https://sensitive@legacy.invalid/bad"),
    )
    .await;
    let root = f.registered("hint-root", "/hint").await;
    sqlx::query(
        "UPDATE workspace_git_root SET repo_name='malformed legacy value' WHERE id='hint-root'",
    )
    .execute(f.store.write_pool())
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/0138_repository_context_selection.sql"
    ))
    .execute(f.store.write_pool())
    .await
    .unwrap();
    assert_eq!(
        f.read().await.selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
    let h = RepositoryRootId {
        workspace_id: hinted.clone(),
        kind: RepositoryRootKind::Primary,
    };
    let selected = f.store.repository_selection_snapshot(&h).await.unwrap();
    assert_eq!(
        saved(&selected),
        &SavedReviewSelection::UnresolvedHistorical {
            source: Some(HistoricalTargetSource::WorkspaceMetadata),
            record_id: Some(hinted.0)
        }
    );
    unresolved(&f.store.repository_selection_snapshot(&root).await.unwrap());
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM repository_selection_state WHERE remote_name IS NOT NULL",
    )
    .fetch_one(f.store.read_pool())
    .await
    .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn snapshot_reads_source_and_counter_from_same_sqlite_snapshot() {
    let f = Fixture::new().await;
    let initial = f.read().await;
    let mut read = f.store.read_pool().begin().await.unwrap();
    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM workspace")
        .fetch_one(&mut *read)
        .await
        .unwrap();
    sqlx::query("UPDATE workspace SET repository_path='/changed' WHERE id=?")
        .bind(f.root.workspace_id.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let captured = read_at(&f.store, &f.root, &mut read).await.unwrap();
    assert!(initial.matches(&captured));
    read.commit().await.unwrap();
    assert!(!initial.matches(&f.read().await));
}

#[tokio::test]
async fn concurrent_clones_and_independent_opens_have_one_cas_winner() {
    let f = Fixture::new().await;
    let other = Store::open(&f.dir.path().join("selection.db"))
        .await
        .unwrap();
    let a = f.read().await;
    let b = other.repository_selection_snapshot(&f.root).await.unwrap();
    let (a, b) = tokio::join!(
        f.store.write_repository_selection(&a, remote("a")),
        other.write_repository_selection(&b, remote("b"))
    );
    let outcomes = [a, b];
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o.result, Ok(RepositorySelectionWriteResult::Applied(_))))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o.result, Ok(RepositorySelectionWriteResult::Conflict(_))))
            .count(),
        1
    );
}

#[tokio::test]
async fn acknowledged_rollback_of_trigger_abort_changes_no_selection() {
    let f = Fixture::new().await;
    let old = f.read().await;
    sqlx::query("CREATE TRIGGER selection_abort BEFORE UPDATE ON repository_selection_state BEGIN SELECT RAISE(ABORT,'fixture'); END")
        .execute(f.store.write_pool()).await.unwrap();
    let outcome = f
        .store
        .write_repository_selection(&old, remote("fail"))
        .await;
    // Production does not claim the asynchronous rollback was observed by its
    // original owner. A later read is evidence of rows, not barrier settlement.
    assert_eq!(outcome.persistence, RepositorySelectionPersistence::Unknown);
    assert!(outcome.result.is_err());
    assert!(old.matches(&f.read().await));
}

async fn replay_selection_migration(store: &Store) {
    let triggers:Vec<String>=sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'repository_selection_%'")
        .fetch_all(store.read_pool()).await.unwrap();
    for trigger in triggers {
        sqlx::query(&format!("DROP TRIGGER {trigger}"))
            .execute(store.write_pool())
            .await
            .unwrap();
    }
    sqlx::query("DROP TABLE repository_selection_state")
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/0138_repository_context_selection.sql"
    ))
    .execute(store.write_pool())
    .await
    .unwrap();
}
#[tokio::test]
async fn legacy_present_empty_hint_is_unresolved_not_proven_absence() {
    let f = Fixture::new().await;
    sqlx::query("UPDATE workspace SET repository_owner='' WHERE id=?")
        .bind(f.root.workspace_id.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    replay_selection_migration(&f.store).await;
    unresolved(&f.read().await);
}
#[tokio::test]
async fn legacy_present_registration_record_is_unresolved_without_target_proof() {
    let f = Fixture::new().await;
    let root = f.registered("historical-root", "/historical").await;
    sqlx::query("UPDATE workspace_git_root SET registered_commit_sha='old-registration' WHERE id='historical-root'").execute(f.store.write_pool()).await.unwrap();
    replay_selection_migration(&f.store).await;
    unresolved(&f.store.repository_selection_snapshot(&root).await.unwrap());
}

async fn selection_authority(
    f: &Fixture,
    token: Option<&str>,
) -> crate::RepositoryAuthoritySnapshot {
    let owner = f.store.get_primary_principal().await.unwrap();
    f.store
        .repository_authority_snapshot(&f.root.workspace_id, &owner.id, token)
        .await
        .unwrap()
}
#[tokio::test]
async fn admitted_selection_cas_and_original_noop_order_call_admission_once() {
    let f = Fixture::new().await;
    let before = f.read().await;
    let authority = selection_authority(&f, None).await;
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let next = applied(
        f.store
            .write_repository_selection_admitted(
                &before,
                remote("explicit"),
                &authority,
                None,
                || {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                },
            )
            .await,
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    for (original, expected_conflict) in [(&before, true), (&next, false)] {
        let outcome = f
            .store
            .write_repository_selection_admitted(
                original,
                remote("explicit"),
                &authority,
                None,
                || panic!("no-op must never reach effect admission"),
            )
            .await;
        assert_eq!(
            outcome.persistence,
            RepositorySelectionPersistence::NoEffect
        );
        assert_eq!(
            matches!(
                outcome.result,
                Ok(RepositorySelectionWriteResult::Conflict(_))
            ),
            expected_conflict
        );
    }
    let denied = f
        .store
        .write_repository_selection_admitted(&next, remote("denied"), &authority, None, || {
            Err(admission_denied())
        })
        .await;
    assert_eq!(denied.persistence, RepositorySelectionPersistence::NoEffect);
    assert!(denied.result.is_err());
    assert!(next.matches(&f.read().await));
}
#[tokio::test]
async fn admitted_selection_initial_authority_and_foreign_domain_refuse_before_effect() {
    let f = Fixture::new().await;
    let owner = f.store.get_primary_principal().await.unwrap();
    f.store
        .insert_principal_credential(&owner.id, "admission-fixture")
        .await
        .unwrap();
    let authority = selection_authority(&f, Some("admission-fixture")).await;
    let before = f.read().await;
    f.store
        .revoke_principal_credential("admission-fixture")
        .await
        .unwrap();
    let denied = f
        .store
        .write_repository_selection_admitted(
            &before,
            remote("denied"),
            &authority,
            Some("admission-fixture"),
            || panic!("stale authority reached update"),
        )
        .await;
    assert_eq!(denied.persistence, RepositorySelectionPersistence::NoEffect);
    assert!(denied.result.is_err());
    assert!(before.matches(&f.read().await));
    let other = Fixture::new().await;
    let denied = other
        .store
        .write_repository_selection_admitted(&before, remote("foreign"), &authority, None, || {
            panic!("foreign snapshot reached update")
        })
        .await;
    assert_eq!(
        denied.persistence,
        RepositorySelectionPersistence::NotAttempted
    );
}
struct SelectionDrain {
    entered: tokio::sync::Notify,
    released: (std::sync::Mutex<bool>, std::sync::Condvar),
    panic_settlement: bool,
}
impl crate::RepositoryLifecycleObserver for SelectionDrain {
    fn begin_mutation(
        &self,
        keys: &[crate::RepositoryLifecycleKey],
    ) -> Result<Box<dyn crate::RepositoryLifecycleMutationTicket>> {
        let selected = keys
            .iter()
            .any(|k| matches!(k, crate::RepositoryLifecycleKey::Selection { .. }));
        if selected && !self.panic_settlement {
            self.entered.notify_one();
            let (ready, signal) = &self.released;
            let waited = signal
                .wait_timeout_while(
                    ready.lock().unwrap(),
                    std::time::Duration::from_secs(5),
                    |ready| !*ready,
                )
                .unwrap();
            assert!(*waited.0, "original selection drain never released");
        }
        Ok(Box::new(SelectionSettled {
            panic: selected && self.panic_settlement,
        }))
    }
}
struct SelectionSettled {
    panic: bool,
}
impl crate::RepositoryLifecycleMutationTicket for SelectionSettled {
    fn settle_confirmed(self: Box<Self>) {
        assert!(!self.panic, "fixture settlement failure after commit");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admitted_selection_final_transaction_rechecks_authority_after_real_drain() {
    let f = Fixture::new().await;
    let owner = f.store.get_primary_principal().await.unwrap();
    f.store
        .insert_principal_credential(&owner.id, "drain-fixture")
        .await
        .unwrap();
    let authority = selection_authority(&f, Some("drain-fixture")).await;
    let before = f.read().await;
    let drain = Arc::new(SelectionDrain {
        entered: tokio::sync::Notify::new(),
        released: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        panic_settlement: false,
    });
    f.store
        .install_repository_lifecycle_observer(drain.clone())
        .await
        .unwrap();
    let store = f.store.clone();
    let handle = tokio::runtime::Handle::current();
    let worker = tokio::task::spawn_blocking(move || {
        handle.block_on(store.write_repository_selection_admitted(
            &before,
            remote("denied"),
            &authority,
            Some("drain-fixture"),
            || panic!("changed authority reached UPDATE"),
        ))
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), drain.entered.notified())
        .await
        .unwrap();
    f.store
        .revoke_principal_credential("drain-fixture")
        .await
        .unwrap();
    *drain.released.0.lock().unwrap() = true;
    drain.released.1.notify_all();
    let result = worker.await.unwrap();
    assert_eq!(result.persistence, RepositorySelectionPersistence::NoEffect);
    assert!(result.result.is_err());
    assert_eq!(
        f.read().await.selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
}
#[tokio::test]
async fn admitted_selection_commit_survives_settlement_failure_and_abort_stays_unknown() {
    let f = Fixture::new().await;
    let before = f.read().await;
    let authority = selection_authority(&f, None).await;
    let observer = Arc::new(SelectionDrain {
        entered: tokio::sync::Notify::new(),
        released: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        panic_settlement: true,
    });
    f.store
        .install_repository_lifecycle_observer(observer)
        .await
        .unwrap();
    let outcome = f
        .store
        .write_repository_selection_admitted(&before, remote("committed"), &authority, None, || {
            Ok(())
        })
        .await;
    assert!(matches!(
        outcome.persistence,
        RepositorySelectionPersistence::Committed { .. }
    ));
    assert!(outcome.result.is_err());
    let name: String = sqlx::query_scalar(
        "SELECT remote_name FROM repository_selection_state WHERE workspace_id=?",
    )
    .bind(f.root.workspace_id.as_str())
    .fetch_one(f.store.read_pool())
    .await
    .unwrap();
    assert_eq!(name, "committed");
    let f = Fixture::new().await;
    let before = f.read().await;
    let authority = selection_authority(&f, None).await;
    sqlx::query("CREATE TRIGGER native_selection_abort BEFORE UPDATE ON repository_selection_state BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(f.store.write_pool()).await.unwrap();
    let outcome = f
        .store
        .write_repository_selection_admitted(
            &before,
            remote("aborted"),
            &authority,
            None,
            || Ok(()),
        )
        .await;
    assert_eq!(outcome.persistence, RepositorySelectionPersistence::Unknown);
    assert!(outcome.result.is_err());
    assert!(before.matches(&f.read().await));
}
