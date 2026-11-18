//! Repository lifecycle, repository authority snapshots, known repos, saved
//! repository selections and workspace transfer rows on both backends.
//!
//! Bodies use fixed ids and leave their rows behind. Ids the subject probes
//! as absent are created by the decoy after its own probes, so a statement
//! without its tenant filter reaches a decoy row on the subject run.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use intent_core::SavedReviewSelection;
use intent_core::{
    AgentId, AgentSession, AgentStatus, Error, PrincipalId, RepositoryRootId, RepositoryRootKind,
    Workspace, WorkspaceGitRoot, WorkspaceGitRootId, WorkspaceGitRootSource, WorkspaceId,
};
use intent_store::{
    RepositoryAcpCompatibilityEffect, RepositoryAcpCompatibilityPersistence,
    RepositoryAcpCompatibilityResult, RepositoryAcpInitialization, RepositoryInitializationBinding,
    RepositoryLifecycleKey, RepositoryLifecycleMutationTicket, RepositoryLifecycleObserver,
    RepositorySelectionChange, RepositorySelectionPersistence, RepositorySelectionWriteResult,
    RepositoryStoredSelection, Store, TRANSFER_TABLES,
};

use crate::harness::{open_trapped, parity_test, sqlite_store, test_db};

const T0: &str = "2026-03-01T00:00:00.000Z";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

async fn workspace(store: &Store, id: &str, tenant: &str) {
    store
        .insert_workspace(&Workspace {
            id: wid(id),
            title: format!("{id} of {tenant}"),
            branch: "main".into(),
            repository_path: Some(format!("/repos/{id}")),
            created_at: T0.into(),
            updated_at: T0.into(),
            ..Default::default()
        })
        .await
        .unwrap();
}

async fn session(store: &Store, ws: &str, id: &str, tenant: &str) {
    store
        .insert_agent_session(&AgentSession {
            id: AgentId(id.into()),
            workspace_id: wid(ws),
            name: format!("{id} of {tenant}"),
            status: AgentStatus::Idle,
            created_at: T0.into(),
            updated_at: T0.into(),
            ..AgentSession::default()
        })
        .await
        .unwrap();
}

fn git_root(id: &str, ws: &str, path: &str) -> WorkspaceGitRoot {
    WorkspaceGitRoot {
        id: WorkspaceGitRootId(id.into()),
        workspace_id: wid(ws),
        path: path.into(),
        source: WorkspaceGitRootSource::Auto,
        repo_owner: None,
        repo_name: None,
        registered_by_agent_ids: Vec::new(),
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: None,
        created_at: T0.into(),
        updated_at: T0.into(),
    }
}

fn primary(ws: &str) -> RepositoryRootId {
    RepositoryRootId {
        workspace_id: wid(ws),
        kind: RepositoryRootKind::Primary,
    }
}

fn registered(ws: &str, id: &str) -> RepositoryRootId {
    RepositoryRootId {
        workspace_id: wid(ws),
        kind: RepositoryRootKind::Registered {
            git_root_id: WorkspaceGitRootId(id.into()),
        },
    }
}

fn remote(name: &str) -> RepositorySelectionChange {
    RepositorySelectionChange::ExplicitRemote {
        remote_name: name.into(),
    }
}

#[track_caller]
fn outcome_kind(result: &intent_core::Result<RepositorySelectionWriteResult>) -> &'static str {
    match result.as_ref().expect("selection write") {
        RepositorySelectionWriteResult::Applied(_) => "applied",
        RepositorySelectionWriteResult::Unchanged(_) => "unchanged",
        RepositorySelectionWriteResult::Conflict(_) => "conflict",
        RepositorySelectionWriteResult::MissingRoot(_) => "missing-root",
    }
}

parity_test!(known_repos_upsert_list_and_remove, |store, tenant| {
    assert!(store.list_known_repos().await.unwrap().is_empty());
    store
        .upsert_known_repo("/r/a", &format!("a-{tenant}"), None)
        .await
        .unwrap();
    store
        .upsert_known_repo("/r/b", "b", Some("owner-b"))
        .await
        .unwrap();
    assert!(store.known_repo_exists("/r/a").await.unwrap());
    assert!(!store.known_repo_exists("/r/absent").await.unwrap());

    // An empty name and no owner keep the stored ones.
    store.upsert_known_repo("/r/b", "", None).await.unwrap();
    store
        .upsert_known_repo("/r/a", "a2", Some("owner-a"))
        .await
        .unwrap();
    let mut repos = store.list_known_repos().await.unwrap();
    assert!(repos
        .windows(2)
        .all(|w| w[0].last_used_at >= w[1].last_used_at));
    repos.sort_by(|x, y| x.path.cmp(&y.path));
    let got: Vec<(&str, &str, Option<&str>)> = repos
        .iter()
        .map(|r| (r.path.as_str(), r.name.as_str(), r.owner.as_deref()))
        .collect();
    assert_eq!(
        got,
        [
            ("/r/a", "a2", Some("owner-a")),
            ("/r/b", "b", Some("owner-b"))
        ]
    );
    assert!(repos.iter().all(|r| r.added_at <= r.last_used_at));

    store.upsert_known_repo("/r/gone", "g", None).await.unwrap();
    assert!(store.remove_known_repo("/r/gone").await.unwrap());
    assert!(!store.remove_known_repo("/r/absent").await.unwrap());
    assert!(!store.known_repo_exists("/r/gone").await.unwrap());
    assert_eq!(store.list_known_repos().await.unwrap().len(), 2);
    if tenant == "decoy" {
        store
            .upsert_known_repo("/r/absent", "x", None)
            .await
            .unwrap();
        store.upsert_known_repo("/r/gone", "x", None).await.unwrap();
    }
});

parity_test!(
    repository_authority_snapshots_read_one_transaction,
    |store, tenant| {
        workspace(&store, "ws-auth", tenant).await;
        let primary_id = store.get_primary_principal().await.unwrap().id;
        store
            .insert_principal_credential(&primary_id, "hash-auth")
            .await
            .unwrap();

        let ws_snapshot = store
            .repository_workspace_authority_snapshot(&wid("ws-auth"))
            .await
            .unwrap();
        assert_eq!(
            ws_snapshot
                .workspace
                .value
                .as_ref()
                .unwrap()
                .owner_principal_id,
            Some(primary_id.clone())
        );
        assert!(ws_snapshot.workspace.revision.is_some());

        let full = store
            .repository_authority_snapshot(&wid("ws-auth"), &primary_id, Some("hash-auth"))
            .await
            .unwrap();
        assert_eq!(full.workspace, ws_snapshot.workspace);
        assert_eq!(
            full.host_authorization_generation,
            ws_snapshot.host_authorization_generation
        );
        let me = full.principal.value.as_ref().unwrap();
        assert!(me.is_primary);
        assert_eq!(full.principal, full.primary_principal);
        let credential = full.credential.as_ref().unwrap();
        assert_eq!(credential.value.as_ref().unwrap().principal_id, primary_id);
        assert!(!credential.value.as_ref().unwrap().revoked);
        assert!(credential.revision.is_some());
        assert_eq!(
            full.workspace_grant.value,
            Some(intent_core::WorkspaceRole::Owner)
        );
        assert!(full.principal_revocation_generation.is_none());

        let host = store
            .repository_host_authority_snapshot(&primary_id, Some("hash-auth"))
            .await
            .unwrap();
        assert_eq!(host.principal, full.principal);
        assert_eq!(host.host_member, full.host_member);
        assert_eq!(host.credential, full.credential);

        // Absent keys stay explicit absence, without a revision.
        let absent = store
            .repository_authority_snapshot(
                &wid("ws-auth-absent"),
                &PrincipalId("p-absent".into()),
                Some("hash-absent"),
            )
            .await
            .unwrap();
        assert_eq!(absent.workspace.value, None);
        assert_eq!(absent.workspace.revision, None);
        assert_eq!(absent.principal.value, None);
        assert_eq!(absent.workspace_grant.value, None);
        assert_eq!(absent.credential.unwrap().value, None);
        assert!(store
            .repository_authority_snapshot(&wid("ws-auth"), &primary_id, None)
            .await
            .unwrap()
            .credential
            .is_none());

        // Revoking the credential bumps its own revision.
        store
            .revoke_principal_credential("hash-auth")
            .await
            .unwrap();
        let revoked = store
            .repository_host_authority_snapshot(&primary_id, Some("hash-auth"))
            .await
            .unwrap()
            .credential
            .unwrap();
        assert!(revoked.value.unwrap().revoked);
        assert!(revoked.revision.unwrap().get() > credential.revision.unwrap().get());
        if tenant == "decoy" {
            workspace(&store, "ws-auth-absent", tenant).await;
            store
                .insert_principal_credential(&primary_id, "hash-absent")
                .await
                .unwrap();
        }
    }
);

parity_test!(repository_selections_compare_and_write, |store, tenant| {
    workspace(&store, "ws-sel", tenant).await;
    store
        .upsert_workspace_git_root(&git_root("gr-sel", "ws-sel", "/src/sel"))
        .await
        .unwrap();

    let first = store
        .repository_selection_snapshot(&primary("ws-sel"))
        .await
        .unwrap();
    assert_eq!(
        first.selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
    assert!(first.binding().is_some());
    let revision = first.selection_revision().unwrap().get();

    let applied = store
        .write_repository_selection(&first, remote("origin"))
        .await;
    assert_eq!(outcome_kind(&applied.result), "applied");
    let RepositorySelectionPersistence::Committed { revision: next } = applied.persistence else {
        panic!("not committed: {:?}", applied.persistence);
    };
    assert_eq!(next.get(), revision + 1);
    let now = store
        .repository_selection_snapshot(&primary("ws-sel"))
        .await
        .unwrap();
    assert_eq!(
        now.selection(),
        Some(&RepositoryStoredSelection::Saved(
            SavedReviewSelection::ExplicitRemote {
                remote_name: "origin".into()
            }
        ))
    );

    // The old snapshot no longer matches; the fresh one is unchanged.
    let stale = store
        .write_repository_selection(&first, RepositorySelectionChange::Automatic)
        .await;
    assert_eq!(outcome_kind(&stale.result), "conflict");
    assert_eq!(stale.persistence, RepositorySelectionPersistence::NoEffect);
    let same = store
        .write_repository_selection(&now, remote("origin"))
        .await;
    assert_eq!(outcome_kind(&same.result), "unchanged");
    let bad = store.write_repository_selection(&now, remote(" x")).await;
    assert!(matches!(bad.result, Err(Error::InvalidParams(_))));
    assert_eq!(
        bad.persistence,
        RepositorySelectionPersistence::NotAttempted
    );

    let reset = store.reset_repository_selection(&now).await;
    assert_eq!(outcome_kind(&reset.result), "applied");
    assert_eq!(
        store
            .repository_selection_snapshot(&primary("ws-sel"))
            .await
            .unwrap()
            .selection(),
        Some(&RepositoryStoredSelection::Reset)
    );

    // A registered root has its own state.
    let root = store
        .repository_selection_snapshot(&registered("ws-sel", "gr-sel"))
        .await
        .unwrap();
    assert_eq!(
        root.selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
    let written = store
        .write_repository_selection(&root, RepositorySelectionChange::Automatic)
        .await;
    assert_eq!(outcome_kind(&written.result), "applied");

    // A root that is not registered has neither binding nor state.
    let missing = store
        .repository_selection_snapshot(&registered("ws-sel", "gr-absent"))
        .await
        .unwrap();
    assert!(missing.binding().is_none() && missing.selection().is_none());
    let missing = store
        .write_repository_selection(&missing, remote("origin"))
        .await;
    assert_eq!(outcome_kind(&missing.result), "missing-root");

    // Admitted writes compare the authority under both snapshots.
    let primary_id = store.get_primary_principal().await.unwrap().id;
    let authority = store
        .repository_authority_snapshot(&wid("ws-sel"), &primary_id, None)
        .await
        .unwrap();
    let admitted = AtomicBool::new(false);
    let current = store
        .repository_selection_snapshot(&primary("ws-sel"))
        .await
        .unwrap();
    let written = store
        .write_repository_selection_admitted(&current, remote("upstream"), &authority, None, || {
            admitted.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await;
    assert_eq!(outcome_kind(&written.result), "applied");
    assert!(admitted.load(Ordering::SeqCst));
    let mut stale_authority = authority.clone();
    stale_authority.host_authorization_generation += 1;
    let current = store
        .repository_selection_snapshot(&primary("ws-sel"))
        .await
        .unwrap();
    let denied = store
        .write_repository_selection_admitted(
            &current,
            remote("other"),
            &stale_authority,
            None,
            || panic!("a stale authority must not admit"),
        )
        .await;
    assert!(matches!(denied.result, Err(Error::Forbidden(_))));
    assert_eq!(denied.persistence, RepositorySelectionPersistence::NoEffect);
    if tenant == "decoy" {
        store
            .upsert_workspace_git_root(&git_root("gr-absent", "ws-sel", "/src/absent"))
            .await
            .unwrap();
    }
});

#[tokio::test]
async fn a_selection_snapshot_belongs_to_its_tenant_domain() {
    let Some(db) = crate::harness::trapped_db().await else {
        return;
    };
    let a = open_trapped(&db, "tenant-a").await;
    let a_again = open_trapped(&db, "tenant-a").await;
    let b = open_trapped(&db, "tenant-b").await;
    assert!(a.shares_repository_lifecycle_domain(&a.clone()));
    assert!(a.shares_repository_lifecycle_domain(&a_again));
    assert!(!a.shares_repository_lifecycle_domain(&b));
    let (sqlite, _dir) = sqlite_store().await;
    assert!(!a.shares_repository_lifecycle_domain(&sqlite));
    assert!(!sqlite.shares_repository_lifecycle_domain(&a));

    workspace(&a, "ws-dom", "tenant-a").await;
    workspace(&b, "ws-dom", "tenant-b").await;
    let snapshot = a
        .repository_selection_snapshot(&primary("ws-dom"))
        .await
        .unwrap();
    let foreign = b
        .write_repository_selection(&snapshot, remote("origin"))
        .await;
    assert!(matches!(foreign.result, Err(Error::InvalidParams(_))));
    let own = a_again
        .write_repository_selection(&snapshot, remote("origin"))
        .await;
    assert_eq!(outcome_kind(&own.result), "applied");
}

struct NoopTicket;

impl RepositoryLifecycleMutationTicket for NoopTicket {
    fn settle_confirmed(self: Box<Self>) {}
}

struct NoopObserver;

impl RepositoryLifecycleObserver for NoopObserver {
    fn begin_mutation(
        &self,
        _keys: &[RepositoryLifecycleKey],
    ) -> intent_core::Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        Ok(Box::new(NoopTicket))
    }
}

fn binding(
    ws: &str,
    agent: &str,
    action: RepositoryAcpInitialization,
) -> RepositoryInitializationBinding {
    RepositoryInitializationBinding {
        workspace_id: wid(ws),
        agent_id: AgentId(agent.into()),
        action,
    }
}

fn first_set(id: &str) -> RepositoryAcpInitialization {
    RepositoryAcpInitialization::FirstSet {
        session_id: id.into(),
    }
}

async fn acp_session(store: &Store, agent: &str) -> Option<String> {
    store
        .get_agent_session_without_messages(&AgentId(agent.into()))
        .await
        .unwrap()
        .acp_session_id
}

parity_test!(
    repository_lifecycle_brackets_and_compatible_acp_writes,
    |store, tenant| {
        workspace(&store, "ws-life", tenant).await;
        session(&store, "ws-life", "ag-life", tenant).await;

        assert!(store.begin_repository_pending_delete(&[]).await.is_err());
        let guard = store
            .begin_repository_pending_delete(&[RepositoryLifecycleKey::Workspace(wid("ws-life"))])
            .await
            .unwrap();
        guard.settle_confirmed().await.unwrap();

        let observer: Arc<dyn RepositoryLifecycleObserver> = Arc::new(NoopObserver);
        assert!(!store.has_repository_lifecycle_observer(&observer));
        assert!(store
            .bind_repository_initialization_claim(observer.clone(), Box::new(()))
            .is_err());

        let set = store
            .initialize_repository_acp_session_compatible(
                None,
                binding("ws-life", "ag-life", first_set("acp-1")),
            )
            .await;
        assert_eq!(
            set.result.unwrap(),
            RepositoryAcpCompatibilityResult::Committed {
                session_id: "acp-1".into(),
                effect: RepositoryAcpCompatibilityEffect::FirstSet,
            }
        );
        assert!(matches!(
            set.persistence,
            RepositoryAcpCompatibilityPersistence::Committed {
                affected_rows: 1,
                ..
            }
        ));
        assert!(set.confirmation.is_err());
        assert_eq!(
            acp_session(&store, "ag-life").await.as_deref(),
            Some("acp-1")
        );

        let again = store
            .initialize_repository_acp_session_compatible(
                None,
                binding("ws-life", "ag-life", first_set("acp-1")),
            )
            .await;
        assert_eq!(
            again.result.unwrap(),
            RepositoryAcpCompatibilityResult::Observed {
                session_id: "acp-1".into()
            }
        );
        let conflicting = store
            .initialize_repository_acp_session_compatible(
                None,
                binding("ws-life", "ag-life", first_set("acp-other")),
            )
            .await;
        assert!(conflicting.result.is_err());

        let replaced = store
            .initialize_repository_acp_session_compatible(
                None,
                binding(
                    "ws-life",
                    "ag-life",
                    RepositoryAcpInitialization::Replace {
                        expected: Some("acp-1".into()),
                        session_id: "acp-2".into(),
                    },
                ),
            )
            .await;
        assert_eq!(
            replaced.result.unwrap(),
            RepositoryAcpCompatibilityResult::Committed {
                session_id: "acp-2".into(),
                effect: RepositoryAcpCompatibilityEffect::Replace {
                    previous: Some("acp-1".into())
                },
            }
        );
        assert_eq!(
            acp_session(&store, "ag-life").await.as_deref(),
            Some("acp-2")
        );
        let stale = store
            .initialize_repository_acp_session_compatible(
                None,
                binding(
                    "ws-life",
                    "ag-life",
                    RepositoryAcpInitialization::Replace {
                        expected: Some("acp-1".into()),
                        session_id: "acp-3".into(),
                    },
                ),
            )
            .await;
        assert_eq!(
            stale.result.unwrap(),
            RepositoryAcpCompatibilityResult::Observed {
                session_id: "acp-2".into()
            }
        );
        assert_eq!(
            acp_session(&store, "ag-life").await.as_deref(),
            Some("acp-2")
        );
        let loaded = store
            .initialize_repository_acp_session_compatible(
                None,
                binding(
                    "ws-life",
                    "ag-life",
                    RepositoryAcpInitialization::Loaded {
                        session_id: "acp-2".into(),
                    },
                ),
            )
            .await;
        assert!(matches!(
            loaded.persistence,
            RepositoryAcpCompatibilityPersistence::NoEffect { .. }
        ));
        let missing = store
            .initialize_repository_acp_session_compatible(
                None,
                binding("ws-life", "ag-absent", first_set("acp-1")),
            )
            .await;
        assert!(matches!(missing.result, Err(Error::NotFound(_))));

        // SQLite installs an observer; Postgres writers carry no brackets, so
        // it refuses one and no claim can bind.
        let installed = store
            .install_repository_lifecycle_observer(observer.clone())
            .await;
        assert_eq!(installed.is_ok(), tenant == "sqlite", "{installed:?}");
        assert_eq!(
            store.has_repository_lifecycle_observer(&observer),
            tenant == "sqlite"
        );
        assert_eq!(
            store
                .bind_repository_initialization_claim(observer.clone(), Box::new(()))
                .is_ok(),
            tenant == "sqlite"
        );
        if tenant == "decoy" {
            session(&store, "ws-life", "ag-absent", tenant).await;
        }
    }
);

/// The archive's JSON with every `from` id replaced by `to`.
fn renamed(
    rows: &[(String, Vec<serde_json::Value>)],
    pairs: &[(&str, &str)],
) -> Vec<(String, Vec<serde_json::Value>)> {
    let mut text = serde_json::to_string(rows).unwrap();
    for (from, to) in pairs {
        text = text.replace(&format!("\"{from}\""), &format!("\"{to}\""));
    }
    serde_json::from_str(&text).unwrap()
}

#[track_caller]
fn count(stats: &[intent_core::transfer::TransferTableStat], table: &str) -> i64 {
    stats
        .iter()
        .find(|s| s.name == table)
        .unwrap_or_else(|| panic!("no {table} stat"))
        .row_count
}

parity_test!(transfer_rows_export_and_import, |store, tenant| {
    workspace(&store, "ws-tr", tenant).await;
    session(&store, "ws-tr", "ag-tr", tenant).await;

    let stats = store.transfer_table_stats(&wid("ws-tr")).await.unwrap();
    let names: Vec<&str> = stats.iter().map(|s| s.name.as_str()).collect();
    let expected: Vec<&str> = TRANSFER_TABLES.iter().map(|(t, _)| *t).collect();
    assert_eq!(names, expected);
    assert_eq!(count(&stats, "workspace"), 1);
    assert_eq!(count(&stats, "agent_session"), 1);
    assert!(
        stats
            .iter()
            .find(|s| s.name == "workspace")
            .unwrap()
            .approx_bytes
            > 0
    );
    assert_eq!(count(&stats, "note"), 0);

    let rows = store.transfer_export_rows(&wid("ws-tr")).await.unwrap();
    assert_eq!(
        rows.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>(),
        expected
    );
    let ws_rows = &rows.iter().find(|(t, _)| t == "workspace").unwrap().1;
    assert_eq!(ws_rows.len(), 1);
    assert_eq!(ws_rows[0]["title"], format!("ws-tr of {tenant}"));
    assert!(ws_rows[0].get("tenant_id").is_none() && ws_rows[0].get("row_seq").is_none());
    let monitor = store.transfer_monitor_rows(&wid("ws-tr")).await.unwrap();
    assert_eq!(
        monitor.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>(),
        ["agent_message", "pr_monitor"]
    );

    let imported = renamed(&rows, &[("ws-tr", "ws-tr2"), ("ag-tr", "ag-tr2")]);
    let inserted = store.transfer_import_rows(&imported).await.unwrap();
    assert!(inserted >= 2, "{inserted}");
    let copy = store.get_workspace(&wid("ws-tr2")).await.unwrap();
    assert_eq!(copy.title, format!("ws-tr of {tenant}"));
    let copied = store
        .get_agent_session_without_messages(&AgentId("ag-tr2".into()))
        .await
        .unwrap();
    assert_eq!(copied.name, format!("ag-tr of {tenant}"));
    let stats2 = store.transfer_table_stats(&wid("ws-tr2")).await.unwrap();
    assert_eq!(count(&stats2, "workspace"), 1);
    assert_eq!(count(&stats2, "agent_session"), 1);
    // The imported primary root's choice is unresolved, never invented.
    assert!(matches!(
        store
            .repository_selection_snapshot(&primary("ws-tr2"))
            .await
            .unwrap()
            .selection(),
        Some(RepositoryStoredSelection::Saved(
            SavedReviewSelection::UnresolvedHistorical { .. }
        ))
    ));

    let event = vec![("event".to_owned(), vec![serde_json::json!({"id": "e"})])];
    assert!(matches!(
        store.transfer_import_rows(&event).await,
        Err(Error::InvalidParams(_))
    ));
    // A bad row rolls back the whole batch, the workspace included.
    let mut bad = renamed(&rows, &[("ws-tr", "ws-tr-bad"), ("ag-tr", "ag-tr-bad")]);
    for (table, objects) in &mut bad {
        if table == "agent_session" {
            objects[0]["no_such_column"] = serde_json::json!(1);
        }
    }
    assert!(matches!(
        store.transfer_import_rows(&bad).await,
        Err(Error::InvalidParams(_))
    ));
    assert!(matches!(
        store.get_workspace(&wid("ws-tr-bad")).await,
        Err(Error::NotFound(_))
    ));
    if tenant == "decoy" {
        workspace(&store, "ws-tr-bad", tenant).await;
    }
});

/// Archives move between backends, so each transfer table exports the same
/// columns on both.
#[tokio::test]
async fn transfer_tables_export_the_same_columns_on_both_backends() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let (sqlite, _dir) = sqlite_store().await;
    for table in TRANSFER_TABLES.iter().map(|(t, _)| *t) {
        let mut lite: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT name FROM pragma_table_info('{table}')"
        )))
        .fetch_all(sqlite.read_pool())
        .await
        .unwrap();
        let mut pg: Vec<String> = sqlx::query_scalar(
            "SELECT column_name::text FROM information_schema.columns \
             WHERE table_schema = current_schema() AND table_name = $1 \
             AND is_generated = 'NEVER' AND column_name NOT IN ('tenant_id', 'row_seq')",
        )
        .bind(table)
        .fetch_all(&db.admin)
        .await
        .unwrap();
        lite.sort();
        pg.sort();
        assert_eq!(lite, pg, "{table}");
    }
}
