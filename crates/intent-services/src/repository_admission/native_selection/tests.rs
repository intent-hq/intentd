//! These controls use actual Store authority and CAS. Socket fixtures supply
//! explicit native task locals; the daemon target proves UDS/WSS dispatch.
use super::*;
use intent_core::repository_request::{
    RepositoryReadConnection, RepositoryReadRetirements, RepositoryWireEntry,
};
use intent_core::{
    HostRole, Principal, PrincipalId, Workspace, WorkspaceApi, WorkspaceId, WorkspaceRole,
};
use intent_store::Store;

struct Fixture {
    dir: tempfile::TempDir,
    services: Arc<Services>,
    workspace: Workspace,
    caller: Caller,
}
impl Fixture {
    async fn new() -> Self {
        let dir = crate::test_support::test_tempdir("repository-selection-");
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let mut workspace = intent_core::chief_workspace();
        workspace.id = WorkspaceId::new();
        workspace.repository_path = Some(dir.path().join("root").to_string_lossy().into_owned());
        std::fs::create_dir(dir.path().join("root")).unwrap();
        store.insert_workspace(&workspace).await.unwrap();
        let services = Arc::new(Services::new_repository_fixture(
            store,
            intent_core::FileSecretStore::with_path(dir.path().join("secrets.json")),
            None,
        ));
        services.initialize_repository_wire().await.unwrap();
        let principal = services.store.get_primary_principal().await.unwrap();
        Self {
            dir,
            services,
            workspace,
            caller: Caller::Wire {
                principal_id: principal.id,
                host_role: HostRole::Owner,
            },
        }
    }
    fn query(&self) -> Query {
        Query {
            workspace_id: self.workspace.id.clone(),
            git_root_id: None,
        }
    }
    async fn socket(&self) -> Socket {
        self.socket_as(self.caller.clone(), None).await
    }
    async fn socket_as(&self, caller: Caller, credential: Option<WireCredential>) -> Socket {
        let entry = if credential.is_some() {
            RepositoryWireEntry::Bearer
        } else {
            RepositoryWireEntry::AdmittedLocal
        };
        let owner = with_caller(
            caller.clone(),
            with_wire_credential(credential.clone(), async {
                super::super::connection(&self.services, entry).unwrap()
            }),
        )
        .await;
        let read = owner.take_retirements().unwrap();
        let selection = owner.take_selection_retirements().unwrap();
        Socket {
            owner,
            caller,
            credential,
            _read: Mutex::new(read),
            selection: Mutex::new(Some(selection)),
        }
    }
    async fn guest(&self, role: Option<WorkspaceRole>) -> Socket {
        let person = Principal {
            id: PrincipalId::new(),
            identity: None,
            github_user_id: Some(704),
            login: Some("selection-fixture".into()),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: intent_core::now_iso(),
            updated_at: intent_core::now_iso(),
        };
        self.services.store.upsert_principal(&person).await.unwrap();
        self.services
            .store
            .insert_principal_credential(&person.id, "selection-fixture-token")
            .await
            .unwrap();
        if let Some(role) = role {
            self.services
                .store
                .add_workspace_member(&self.workspace.id, &person.id, role)
                .await
                .unwrap();
        }
        self.socket_as(
            Caller::Wire {
                principal_id: person.id.clone(),
                host_role: HostRole::Guest,
            },
            Some(WireCredential::Principal {
                principal_id: person.id,
                token_hash: "selection-fixture-token".into(),
            }),
        )
        .await
    }
}
struct Socket {
    owner: Arc<dyn RepositoryReadConnection>,
    caller: Caller,
    credential: Option<WireCredential>,
    _read: Mutex<Box<dyn RepositoryReadRetirements>>,
    selection: Mutex<Option<Box<dyn RepositorySelectionRetirements>>>,
}
impl Socket {
    async fn entered<T>(&self, body: impl Future<Output = T>) -> T {
        with_caller(
            self.caller.clone(),
            with_wire_credential(self.credential.clone(), body),
        )
        .await
    }
    async fn request<T: Send>(
        &self,
        frame: Frame,
        body: impl Future<Output = Result<T>> + Send,
    ) -> Result<T> {
        self.entered(async {
            let scope = self.owner.capture_selection(&frame).unwrap();
            let _guard = RetireFrame(scope.clone());
            let mut answer = None;
            scope
                .scope(Box::pin(async {
                    let value = body.await;
                    let mut count = 0;
                    let sent = scope
                        .deliver(
                            if value.is_ok() {
                                RepositoryReadReplyKind::Result
                            } else {
                                RepositoryReadReplyKind::ServiceError
                            },
                            &mut || {
                                count += 1;
                                Ok(())
                            },
                        )
                        .await;
                    assert!(count <= 1);
                    answer = Some(sent.and(value));
                }))
                .await;
            scope.retire();
            answer.unwrap()
        })
        .await
    }
    async fn capture(&self, f: &Fixture) -> Result<Capture> {
        self.request(Frame::Capture(f.query()), async {
            f.services.repository_selection_capture(f.query()).await
        })
        .await
    }
    async fn save(&self, f: &Fixture, capture: &Capture, choice: Choice) -> Result<Attempt> {
        let query = SaveQuery {
            workspace_id: capture.root.workspace_id.clone(),
            git_root_id: match &capture.root.kind {
                RepositoryRootKind::Primary => None,
                RepositoryRootKind::Registered { git_root_id } => Some(git_root_id.clone()),
            },
            selection_id: capture.selection_id.clone(),
            choice,
        };
        self.request(Frame::Save(query.clone()), async {
            f.services.repository_selection_save(query).await
        })
        .await
    }
    async fn reconcile(&self, f: &Fixture, capture: &Capture) -> Result<Attempt> {
        let q = bound(capture);
        self.request(Frame::Reconcile(q.clone()), async {
            f.services.repository_selection_reconcile(q).await
        })
        .await
    }
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.owner.retire();
    }
}
fn bound(capture: &Capture) -> BoundQuery {
    BoundQuery {
        workspace_id: capture.root.workspace_id.clone(),
        git_root_id: match &capture.root.kind {
            RepositoryRootKind::Primary => None,
            RepositoryRootKind::Registered { git_root_id } => Some(git_root_id.clone()),
        },
        selection_id: capture.selection_id.clone(),
    }
}
fn receipt(attempt: &Attempt) -> &Receipt {
    match &attempt.attempt {
        AttemptState::Settled { receipt } => receipt,
        _ => panic!("expected settled original operation"),
    }
}

#[intent_test_macros::daemon_test]
async fn native_selection_original_save_reset_history_and_identical_receipt() {
    let f = Fixture::new().await;
    let mut s = f.socket().await;
    let first = s.capture(&f).await.unwrap();
    assert_eq!(first.snapshot.selection, SelectionState::NeverSaved);
    assert_eq!(first.expires_after_ms, 300_000);
    let saved = s
        .save(
            &f,
            &first,
            Choice::ExplicitRemote {
                remote_name: "not-current-git".into(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        receipt(&saved).persistence,
        Persistence::Committed { .. }
    ));
    assert_eq!(
        s.save(
            &f,
            &first,
            Choice::ExplicitRemote {
                remote_name: "not-current-git".into()
            }
        )
        .await
        .unwrap(),
        saved
    );
    assert_eq!(s.reconcile(&f, &first).await.unwrap(), saved);
    assert!(s.save(&f, &first, Choice::Automatic {}).await.is_err());
    let event = s
        .selection
        .get_mut()
        .unwrap()
        .as_mut()
        .unwrap()
        .next()
        .await
        .unwrap();
    assert_eq!(event.selection_ids, vec![first.selection_id.clone()]);
    let reset = s.capture(&f).await.unwrap();
    let q = bound(&reset);
    let result = s
        .request(Frame::Reset(q.clone()), async {
            f.services.repository_selection_reset(q).await
        })
        .await
        .unwrap();
    assert!(
        matches!(receipt(&result).result, Outcome::Applied { ref snapshot } if snapshot.selection == SelectionState::Reset)
    );
    assert_eq!(
        s.reconcile(&f, &first).await.unwrap(),
        saved,
        "receipt survives another committed selection"
    );
    assert_eq!(
        std::fs::read_dir(f.dir.path().join("root"))
            .unwrap()
            .count(),
        0,
        "intent persistence runs no Git action"
    );
}

#[intent_test_macros::daemon_test]
async fn native_selection_actual_conflict_consumes_old_snapshot() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let old = s.capture(&f).await.unwrap();
    let newer = s.capture(&f).await.unwrap();
    s.save(&f, &newer, Choice::Automatic {}).await.unwrap();
    let conflict = s.save(&f, &old, Choice::Automatic {}).await.unwrap();
    assert!(matches!(
        receipt(&conflict).result,
        Outcome::Conflict { .. }
    ));
    assert_eq!(receipt(&conflict).persistence, Persistence::NoEffect);
    assert_eq!(
        s.save(&f, &old, Choice::Automatic {}).await.unwrap(),
        conflict
    );
    assert!(s
        .save(
            &f,
            &old,
            Choice::ExplicitRemote {
                remote_name: "new".into()
            }
        )
        .await
        .is_err());
}

#[intent_test_macros::daemon_test]
async fn native_selection_durable_manager_permission_and_credential_retirement() {
    let f = Fixture::new().await;
    let collaborator = f.guest(Some(WorkspaceRole::Collaborator)).await;
    assert!(collaborator.capture(&f).await.is_err());
    let Caller::Wire { principal_id, .. } = &collaborator.caller else {
        unreachable!()
    };
    let primary = f.services.store.get_primary_principal().await.unwrap();
    f.services
        .store
        .set_workspace_member_role(&f.workspace.id, &primary.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    f.services
        .store
        .set_workspace_member_role(&f.workspace.id, principal_id, WorkspaceRole::Owner)
        .await
        .unwrap();
    let owner = f
        .socket_as(collaborator.caller.clone(), collaborator.credential.clone())
        .await;
    let edit = owner.capture(&f).await.unwrap();
    f.services
        .store
        .set_workspace_member_role(&f.workspace.id, principal_id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    f.services
        .store
        .set_workspace_member_role(&f.workspace.id, principal_id, WorkspaceRole::Owner)
        .await
        .unwrap();
    assert!(owner.save(&f, &edit, Choice::Automatic {}).await.is_err());
    let current = f
        .socket_as(owner.caller.clone(), owner.credential.clone())
        .await;
    let edit = current.capture(&f).await.unwrap();
    f.services
        .store
        .revoke_principal_credential("selection-fixture-token")
        .await
        .unwrap();
    assert!(current.save(&f, &edit, Choice::Automatic {}).await.is_err());
}

#[intent_test_macros::daemon_test]
async fn native_selection_foreign_socket_read_lease_and_frame_command_never_grant() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let other = f.socket().await;
    let edit = s.capture(&f).await.unwrap();
    assert!(other.save(&f, &edit, Choice::Automatic {}).await.is_err());
    assert!(f
        .services
        .repository_selection_capture(f.query())
        .await
        .is_err());
    s.entered(async {
        let q = SaveQuery {
            workspace_id: f.workspace.id.clone(),
            git_root_id: None,
            selection_id: edit.selection_id.clone(),
            choice: Choice::Automatic {},
        };
        let frame = s.owner.capture_selection(&Frame::Save(q.clone())).unwrap();
        frame
            .scope(Box::pin(async {
                let mut changed = q.clone();
                changed.choice = Choice::ExplicitRemote {
                    remote_name: "forged".into(),
                };
                assert!(f.services.repository_selection_save(changed).await.is_err());
            }))
            .await;
        frame.retire();
    })
    .await;
    let result = s.reconcile(&f, &edit).await.unwrap();
    assert_eq!(receipt(&result).persistence, Persistence::NotAttempted);
}

struct RetireFrame(Arc<dyn RepositoryReadRequestScope>);
impl Drop for RetireFrame {
    fn drop(&mut self) {
        self.0.retire();
    }
}
#[derive(Default)]
pub(super) struct WorkerGate {
    entered: Notify,
    release: Notify,
}
impl WorkerGate {
    pub(super) async fn wait(&self) {
        self.entered.notify_one();
        self.release.notified().await;
    }
}
async fn until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
impl Socket {
    async fn original(&self, query: Query) -> Arc<Connection> {
        self.entered(async {
            let scope = self
                .owner
                .capture_selection(&Frame::Capture(query))
                .unwrap();
            let guard = RetireFrame(scope.clone());
            let mut connection = None;
            scope
                .scope(Box::pin(async {
                    connection = Some(SELECTION_REQUEST.with(|r| r.connection.clone()));
                }))
                .await;
            drop(guard);
            connection.unwrap()
        })
        .await
    }
}
fn save_query(capture: &Capture) -> SaveQuery {
    let b = bound(capture);
    SaveQuery {
        workspace_id: b.workspace_id,
        git_root_id: b.git_root_id,
        selection_id: b.selection_id,
        choice: Choice::Automatic {},
    }
}
fn record(c: &Connection, id: &str) -> Arc<Operation> {
    c.selection
        .feed
        .lock()
        .unwrap()
        .records
        .get(id)
        .unwrap()
        .clone()
}

#[intent_test_macros::daemon_test]
async fn native_selection_before_queue_unpolled_and_duplicate_lane_have_no_effect() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let edit = s.capture(&f).await.unwrap();
    let c = s.original(f.query()).await;
    let op = record(&c, &edit.selection_id);
    let q = save_query(&edit);
    s.entered(async {
        let scope = s.owner.capture_selection(&Frame::Save(q.clone())).unwrap();
        assert!(op.progress.lock().unwrap().command.is_some());
        assert!(!op.progress.lock().unwrap().worker_started);
        let duplicate = s.owner.capture_selection(&Frame::Save(q.clone())).unwrap();
        duplicate
            .scope(Box::pin(async {
                assert!(f
                    .services
                    .repository_selection_save(q.clone())
                    .await
                    .is_err());
            }))
            .await;
        duplicate.retire();
        drop(scope); // An unpolled queued command is permanently claimed and retired.
    })
    .await;
    assert_eq!(
        receipt(&s.reconcile(&f, &edit).await.unwrap()).persistence,
        Persistence::NotAttempted
    );
    assert_eq!(
        f.services
            .store
            .repository_selection_snapshot(&edit.root)
            .await
            .unwrap()
            .selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
    assert_eq!(
        receipt(&s.save(&f, &edit, Choice::Automatic {}).await.unwrap()).persistence,
        Persistence::NotAttempted
    );
}

#[intent_test_macros::daemon_test]
async fn native_selection_cancelled_worker_holds_capacity_until_real_store_exit() {
    let f = Arc::new(Fixture::new().await);
    let s = Arc::new(f.socket().await);
    let edit = s.capture(&f).await.unwrap();
    let c = s.original(f.query()).await;
    let op = record(&c, &edit.selection_id);
    let write_connection = f.services.store.write_pool().acquire().await.unwrap();
    let worker_f = f.clone();
    let worker_s = s.clone();
    let worker_edit = edit.clone();
    let task = tokio::spawn(async move {
        worker_s
            .save(&worker_f, &worker_edit, Choice::Automatic {})
            .await
    });
    until(|| op.progress.lock().unwrap().worker_started).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        f.services
            .repository_selection_capacity
            .workers
            .available_permits(),
        WORKER_LIMIT - 1
    );
    assert!(op.progress.lock().unwrap().receipt.is_none());
    drop(write_connection);
    until(|| op.progress.lock().unwrap().receipt.is_some()).await;
    until(|| {
        f.services
            .repository_selection_capacity
            .workers
            .available_permits()
            == WORKER_LIMIT
    })
    .await;
    assert_eq!(
        receipt(&op.attempt().unwrap()).persistence,
        Persistence::NoEffect
    );
    assert_eq!(
        f.services
            .store
            .repository_selection_snapshot(&edit.root)
            .await
            .unwrap()
            .selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
    assert_eq!(s.reconcile(&f, &edit).await.unwrap(), op.attempt().unwrap());
}

#[intent_test_macros::daemon_test]
async fn native_selection_observed_commit_survives_waiter_cancel_before_retention() {
    let f = Arc::new(Fixture::new().await);
    let s = Arc::new(f.socket().await);
    let edit = s.capture(&f).await.unwrap();
    let c = s.original(f.query()).await;
    let op = record(&c, &edit.selection_id);
    let gate = Arc::new(WorkerGate::default());
    *c.selection.after_store.lock().unwrap() = Some(gate.clone());
    let worker_f = f.clone();
    let worker_s = s.clone();
    let worker_edit = edit.clone();
    let task = tokio::spawn(async move {
        worker_s
            .save(&worker_f, &worker_edit, Choice::Automatic {})
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        f.services
            .store
            .repository_selection_snapshot(&edit.root)
            .await
            .unwrap()
            .selection(),
        Some(&RepositoryStoredSelection::Saved(
            intent_core::SavedReviewSelection::Automatic
        ))
    );
    assert!(op.progress.lock().unwrap().admitted);
    assert!(op.progress.lock().unwrap().receipt.is_none());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        f.services
            .repository_selection_capacity
            .workers
            .available_permits(),
        WORKER_LIMIT - 1
    );
    gate.release.notify_one();
    until(|| op.progress.lock().unwrap().receipt.is_some()).await;
    until(|| {
        f.services
            .repository_selection_capacity
            .workers
            .available_permits()
            == WORKER_LIMIT
    })
    .await;
    assert!(matches!(
        receipt(&op.attempt().unwrap()).persistence,
        Persistence::Committed { .. }
    ));
    assert_eq!(s.reconcile(&f, &edit).await.unwrap(), op.attempt().unwrap());
}

#[intent_test_macros::daemon_test]
async fn native_selection_committed_receipt_precedes_final_disclosure_and_never_replays() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let edit = s.capture(&f).await.unwrap();
    let c = s.original(f.query()).await;
    let op = record(&c, &edit.selection_id);
    s.entered(async {
        let q = save_query(&edit);
        let scope = s.owner.capture_selection(&Frame::Save(q.clone())).unwrap();
        let guard = RetireFrame(scope.clone());
        scope
            .scope(Box::pin(async {
                let result = f.services.repository_selection_save(q).await.unwrap();
                assert!(matches!(
                    receipt(&result).persistence,
                    Persistence::Committed { .. }
                ));
                let mut effects = 0;
                assert!(scope
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        effects += 1;
                        Err(unavailable())
                    })
                    .await
                    .is_err());
                assert!(scope
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        effects += 1;
                        Ok(())
                    })
                    .await
                    .is_err());
                assert_eq!(effects, 1);
                assert_eq!(op.attempt().unwrap(), result);
            }))
            .await;
        drop(guard);
    })
    .await;
    assert_eq!(s.reconcile(&f, &edit).await.unwrap(), op.attempt().unwrap());
    // A later authority retirement suppresses disclosure, never the original commit.
    let q = bound(&edit);
    s.entered(async {
        let scope = s
            .owner
            .capture_selection(&Frame::Reconcile(q.clone()))
            .unwrap();
        scope
            .scope(Box::pin(async {
                let result = f.services.repository_selection_reconcile(q).await.unwrap();
                c.close();
                let mut effects = 0;
                assert!(scope
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        effects += 1;
                        Ok(())
                    })
                    .await
                    .is_err());
                assert_eq!(effects, 0);
                assert_eq!(op.attempt().unwrap(), result);
            }))
            .await;
        scope.retire();
    })
    .await;
    assert!(f.socket().await.reconcile(&f, &edit).await.is_err());
}

#[intent_test_macros::daemon_test]
async fn native_selection_pending_delete_and_equal_root_recreation_do_not_repair() {
    use intent_store::RepositoryLifecycleObserver;
    let f = Fixture::new().await;
    let s = f.socket().await;
    let edit = s.capture(&f).await.unwrap();
    let registry = f.services.repository_lifecycle_registry.clone();
    let pending = registry
        .begin_pending_delete(&[RepositoryLifecycleKey::Workspace(f.workspace.id.clone())])
        .unwrap();
    assert!(s.save(&f, &edit, Choice::Automatic {}).await.is_err());
    pending.settle_confirmed();
    assert!(s.save(&f, &edit, Choice::Automatic {}).await.is_err());
    let current = s.capture(&f).await.unwrap();
    let mut changed = f.workspace.clone();
    changed.repository_path = Some(
        f.dir
            .path()
            .join("different")
            .to_string_lossy()
            .into_owned(),
    );
    f.services.store.update_workspace(&changed).await.unwrap();
    f.services
        .store
        .update_workspace(&f.workspace)
        .await
        .unwrap();
    assert!(s.save(&f, &current, Choice::Automatic {}).await.is_err());
    let fresh = s.capture(&f).await.unwrap();
    assert!(
        fresh.snapshot.root_incarnation.parse::<u64>().unwrap()
            > current.snapshot.root_incarnation.parse::<u64>().unwrap()
    );
}

#[intent_test_macros::daemon_test]
async fn native_selection_actual_sql_abort_is_retained_unknown_without_retry() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let edit = s.capture(&f).await.unwrap();
    sqlx::query("CREATE TRIGGER selection_fail BEFORE UPDATE ON repository_selection_state BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(f.services.store.write_pool()).await.unwrap();
    let result = s.save(&f, &edit, Choice::Automatic {}).await.unwrap();
    assert_eq!(receipt(&result).persistence, Persistence::Unknown);
    assert_eq!(
        receipt(&result).result,
        Outcome::Failed {
            code: Failure::StorageFailed
        }
    );
    sqlx::query("DROP TRIGGER selection_fail")
        .execute(f.services.store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        s.save(&f, &edit, Choice::Automatic {}).await.unwrap(),
        result
    );
    assert_eq!(
        f.services
            .store
            .repository_selection_snapshot(&edit.root)
            .await
            .unwrap()
            .selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
}

#[intent_test_macros::daemon_test]
async fn native_selection_private_feed_overflow_loss_and_reconcile_budget_close() {
    let f = Fixture::new().await;
    let mut s = f.socket().await;
    let edit = s.capture(&f).await.unwrap();
    let c = s.original(f.query()).await;
    let op = record(&c, &edit.selection_id);
    for _ in 0..RECONCILE_LIMIT {
        s.reconcile(&f, &edit).await.unwrap();
    }
    assert!(s.reconcile(&f, &edit).await.is_err());
    assert_eq!(op.reconciles.load(Ordering::Acquire), RECONCILE_LIMIT);
    c.selection.feed.lock().unwrap().sequence = u64::MAX;
    op.retire_write();
    let terminal = s
        .selection
        .get_mut()
        .unwrap()
        .as_mut()
        .unwrap()
        .next()
        .await
        .unwrap();
    assert!(terminal.terminal && terminal.all_retired);
    assert_eq!(terminal.sequence, u64::MAX.to_string());
    assert!(s.capture(&f).await.is_err());
    let mut other = f.socket().await;
    let next = other.capture(&f).await.unwrap();
    drop(other.selection.get_mut().unwrap().take());
    assert!(other.save(&f, &next, Choice::Automatic {}).await.is_err());
    assert!(other.capture(&f).await.is_err());
    let fresh = f.socket().await;
    let c = fresh.original(f.query()).await;
    for i in 0..=NOTICE_LIMIT {
        c.selection.notice(&format!("only-original-{i}"));
    }
    assert!(c.selection.feed.lock().unwrap().closed);
}

#[tokio::test]
async fn native_selection_virtual_deadlines_retire_unclaimed_frame_and_receipt() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let edit = s.capture(&f).await.unwrap();
    let c = s.original(f.query()).await;
    let expiring = record(&c, &edit.selection_id);
    tokio::time::pause();
    tokio::time::advance(LEASE_TTL + Duration::from_secs(1)).await;
    assert!(expiring.write_current().is_err());
    tokio::time::resume();
    assert!(s.save(&f, &edit, Choice::Automatic {}).await.is_err());
    let edit = s.capture(&f).await.unwrap();
    let q = save_query(&edit);
    let op = record(&c, &edit.selection_id);
    let scope = s
        .entered(async { s.owner.capture_selection(&Frame::Save(q)).unwrap() })
        .await;
    tokio::time::pause();
    tokio::time::advance(FRAME_TTL + Duration::from_secs(1)).await;
    tokio::time::resume();
    scope.retire();
    assert_eq!(
        receipt(&op.attempt().unwrap()).persistence,
        Persistence::NotAttempted
    );
    let edit = s.capture(&f).await.unwrap();
    s.save(&f, &edit, Choice::Automatic {}).await.unwrap();
    let op = record(&c, &edit.selection_id);
    tokio::time::pause();
    tokio::time::advance(RECEIPT_TTL + Duration::from_secs(1)).await;
    assert!(op.disclosure_current().is_err());
    tokio::time::resume();
    assert!(s.reconcile(&f, &edit).await.is_err());
}

#[intent_test_macros::daemon_test]
async fn native_selection_record_and_worker_limits_charge_original_completion() {
    let f = Arc::new(Fixture::new().await);
    let mut sockets = Vec::new();
    let mut records = Vec::new();
    for _ in 0..(GLOBAL_LIMIT / CONNECTION_LIMIT) {
        let s = Arc::new(f.socket().await);
        for _ in 0..CONNECTION_LIMIT {
            records.push(s.capture(&f).await.unwrap());
        }
        assert!(s.capture(&f).await.is_err());
        sockets.push(s);
    }
    assert_eq!(
        f.services
            .repository_selection_capacity
            .records
            .available_permits(),
        0
    );
    assert!(f.socket().await.capture(&f).await.is_err());
    for s in &sockets {
        s.owner.retire();
    }
    until(|| {
        f.services
            .repository_selection_capacity
            .records
            .available_permits()
            == GLOBAL_LIMIT
    })
    .await;
    let s = Arc::new(f.socket().await);
    let c = s.original(f.query()).await;
    let gate = Arc::new(WorkerGate::default());
    *c.selection.after_store.lock().unwrap() = Some(gate.clone());
    let mut tasks = Vec::new();
    let mut ops = Vec::new();
    for _ in 0..WORKER_LIMIT {
        let edit = s.capture(&f).await.unwrap();
        ops.push(record(&c, &edit.selection_id));
        let f = f.clone();
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            s.save(&f, &edit, Choice::Automatic {}).await
        }));
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
    }
    assert_eq!(
        f.services
            .repository_selection_capacity
            .workers
            .available_permits(),
        0
    );
    let refused = s.capture(&f).await.unwrap();
    assert!(s.save(&f, &refused, Choice::Automatic {}).await.is_err());
    assert_eq!(
        receipt(&s.reconcile(&f, &refused).await.unwrap()).persistence,
        Persistence::NotAttempted
    );
    for task in tasks {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
    assert_eq!(
        f.services
            .repository_selection_capacity
            .workers
            .available_permits(),
        0
    );
    gate.release.notify_waiters();
    until(|| {
        f.services
            .repository_selection_capacity
            .workers
            .available_permits()
            == WORKER_LIMIT
    })
    .await;
    assert!(ops
        .iter()
        .all(|op| op.progress.lock().unwrap().receipt.is_some()));
}

#[intent_test_macros::daemon_test]
async fn native_selection_construction_poll_and_nested_scope_never_restore_caller() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    for changed_credential in [false, true] {
        let edit = s.capture(&f).await.unwrap();
        s.entered(async {
            let q = save_query(&edit);
            let scope = s.owner.capture_selection(&Frame::Save(q.clone())).unwrap();
            let guard = RetireFrame(scope.clone());
            scope
                .scope(Box::pin(async {
                    let nested = s
                        .owner
                        .capture_selection(&Frame::Capture(f.query()))
                        .unwrap();
                    nested
                        .scope(Box::pin(async {
                            assert!(f
                                .services
                                .repository_selection_capture(f.query())
                                .await
                                .is_err());
                        }))
                        .await;
                    nested.retire();
                    let future = f.services.repository_selection_save(q);
                    let result = if changed_credential {
                        with_wire_credential(
                            Some(WireCredential::Principal {
                                principal_id: PrincipalId::new(),
                                token_hash: "foreign-fixture".into(),
                            }),
                            future,
                        )
                        .await
                    } else {
                        with_caller(Caller::Daemon, future).await
                    };
                    assert!(result.is_err());
                    assert_eq!(intent_core::current_caller(), Some(s.caller.clone()));
                }))
                .await;
            drop(guard);
        })
        .await;
        assert_eq!(
            receipt(&s.reconcile(&f, &edit).await.unwrap()).persistence,
            Persistence::NotAttempted
        );
    }
    let copied = f.services.as_ref().clone();
    s.entered(async {
        assert!(super::super::connection(&copied, RepositoryWireEntry::AdmittedLocal).is_none());
    })
    .await;
}

#[intent_test_macros::daemon_test]
async fn native_selection_registered_exact_root_keeps_unresolved_history() {
    use intent_core::{WorkspaceGitRoot, WorkspaceGitRootId};
    let f = Fixture::new().await;
    let s = f.socket().await;
    let id = WorkspaceGitRootId::new();
    let row:WorkspaceGitRoot=serde_json::from_value(serde_json::json!({"id":id,"workspaceId":f.workspace.id,"path":f.dir.path().join("registered"),"source":"agent","registeredByAgentIds":[],"createdAt":intent_core::now_iso(),"updatedAt":intent_core::now_iso()})).unwrap();
    f.services
        .store
        .upsert_workspace_git_root(&row)
        .await
        .unwrap();
    let q = Query {
        workspace_id: f.workspace.id.clone(),
        git_root_id: Some(id.clone()),
    };
    let edit = s
        .request(Frame::Capture(q.clone()), async {
            f.services.repository_selection_capture(q.clone()).await
        })
        .await
        .unwrap();
    s.save(
        &f,
        &edit,
        Choice::ExplicitRemote {
            remote_name: "original".into(),
        },
    )
    .await
    .unwrap();
    f.services
        .store
        .delete_workspace_git_root(&id)
        .await
        .unwrap();
    f.services
        .store
        .upsert_workspace_git_root(&row)
        .await
        .unwrap();
    assert!(s.reconcile(&f, &edit).await.is_err());
    let current = s
        .request(Frame::Capture(q.clone()), async {
            f.services.repository_selection_capture(q).await
        })
        .await
        .unwrap();
    assert!(matches!(
        current.snapshot.selection,
        SelectionState::Saved {
            value: intent_core::SavedReviewSelection::UnresolvedHistorical { .. }
        }
    ));
    assert_eq!(
        s.capture(&f).await.unwrap().snapshot.selection,
        SelectionState::NeverSaved
    );
}

#[intent_test_macros::daemon_test]
async fn native_selection_actual_member_chief_and_durable_host_removal() {
    let f = Fixture::new().await;
    let guest = f.guest(None).await;
    let person = f
        .services
        .store
        .get_principal(guest.caller.principal_id().unwrap())
        .await
        .unwrap();
    let owner = f.services.store.get_primary_principal().await.unwrap();
    let invite = intent_core::HostInvite::new(
        "selection-member".into(),
        owner.id,
        person.identity_key().unwrap(),
        person.login.clone().unwrap(),
        "selection-invite-proof".into(),
        None,
    )
    .unwrap();
    f.services.store.insert_host_invite(&invite).await.unwrap();
    let generation = f
        .services
        .store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    f.services
        .store
        .join_host_by_invite(
            &invite.id,
            &person,
            intent_store::HostJoinCredential::Proof {
                token_hash: "selection-member-credential",
                authorization_generation: generation,
            },
        )
        .await
        .unwrap();
    let member = f
        .socket_as(
            Caller::Wire {
                principal_id: person.id.clone(),
                host_role: HostRole::Member,
            },
            Some(WireCredential::Principal {
                principal_id: person.id.clone(),
                token_hash: "selection-member-credential".into(),
            }),
        )
        .await;
    let edit = member.capture(&f).await.unwrap();
    let outcome = member.save(&f, &edit, Choice::Automatic {}).await.unwrap();
    assert!(matches!(
        receipt(&outcome).persistence,
        Persistence::Committed { .. }
    ));
    assert!(guest.capture(&f).await.is_err());
    if f.services
        .store
        .get_workspace(&WorkspaceId::chief())
        .await
        .is_err()
    {
        f.services
            .store
            .insert_workspace(&intent_core::chief_workspace())
            .await
            .unwrap();
    }
    let chief = Query {
        workspace_id: WorkspaceId::chief(),
        git_root_id: None,
    };
    assert!(member
        .request(Frame::Capture(chief.clone()), async {
            f.services.repository_selection_capture(chief).await
        })
        .await
        .is_err());
    let pending = member.capture(&f).await.unwrap();
    f.services
        .store
        .remove_host_member(&person.id)
        .await
        .unwrap();
    assert!(member
        .save(&f, &pending, Choice::Automatic {})
        .await
        .is_err());
    assert!(member.reconcile(&f, &edit).await.is_err());
    assert!(member.capture(&f).await.is_err());
    assert!(matches!(
        receipt(&outcome).persistence,
        Persistence::Committed { .. }
    ));
}

#[tokio::test]
async fn native_selection_expired_worker_retains_capacity_and_observed_commit() {
    let f = Arc::new(Fixture::new().await);
    let s = Arc::new(f.socket().await);
    let edit = s.capture(&f).await.unwrap();
    let c = s.original(f.query()).await;
    let op = record(&c, &edit.selection_id);
    let gate = Arc::new(WorkerGate::default());
    *c.selection.after_store.lock().unwrap() = Some(gate.clone());
    let worker_f = f.clone();
    let worker_s = s.clone();
    let worker_edit = edit.clone();
    let waiter = tokio::spawn(async move {
        worker_s
            .save(&worker_f, &worker_edit, Choice::Automatic {})
            .await
    });
    gate.entered.notified().await;
    tokio::time::pause();
    tokio::time::advance(LEASE_TTL + Duration::from_secs(1)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        f.services
            .repository_selection_capacity
            .workers
            .available_permits(),
        WORKER_LIMIT - 1
    );
    assert_eq!(
        c.selection.permits.available_permits(),
        CONNECTION_LIMIT - 1
    );
    assert!(op.progress.lock().unwrap().receipt.is_none());
    assert!(c
        .selection
        .feed
        .lock()
        .unwrap()
        .records
        .contains_key(&edit.selection_id));
    tokio::time::resume();
    waiter.abort();
    let _ = waiter.await;
    gate.release.notify_one();
    until(|| op.progress.lock().unwrap().receipt.is_some()).await;
    until(|| {
        f.services
            .repository_selection_capacity
            .workers
            .available_permits()
            == WORKER_LIMIT
    })
    .await;
    assert!(matches!(
        receipt(&op.attempt().unwrap()).persistence,
        Persistence::Committed { .. }
    ));
    assert_eq!(s.reconcile(&f, &edit).await.unwrap(), op.attempt().unwrap());
}

#[intent_test_macros::daemon_test]
async fn native_selection_unpublished_acquisition_drop_closes_original_record() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let c = s.original(f.query()).await;
    s.entered(async {
        let scope = s
            .owner
            .capture_selection(&Frame::Capture(f.query()))
            .unwrap();
        let mut captured = None;
        scope
            .scope(Box::pin(async {
                captured = Some(
                    f.services
                        .repository_selection_capture(f.query())
                        .await
                        .unwrap(),
                );
            }))
            .await;
        let captured = captured.unwrap();
        let op = record(&c, &captured.selection_id);
        assert!(!op.published.load(Ordering::Acquire));
        scope.retire();
        drop(scope);
        assert!(op.disclosure_current().is_err());
        drop(op);
        until(|| {
            !c.selection
                .feed
                .lock()
                .unwrap()
                .records
                .contains_key(&captured.selection_id)
        })
        .await;
    })
    .await;
    until(|| c.selection.permits.available_permits() == CONNECTION_LIMIT).await;
    assert_eq!(
        f.services
            .repository_selection_capacity
            .records
            .available_permits(),
        GLOBAL_LIMIT
    );
}

#[intent_test_macros::daemon_test]
async fn native_selection_cancelled_acquisition_cannot_publish_late_success() {
    let f = Arc::new(Fixture::new().await);
    let s = Arc::new(f.socket().await);
    let c = s.original(f.query()).await;
    let pool = f.services.store.read_pool();
    let mut held = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        held.push(pool.acquire().await.unwrap());
    }
    let pending_f = f.clone();
    let pending_s = s.clone();
    let task = tokio::spawn(async move { pending_s.capture(&pending_f).await });
    until(|| c.selection.permits.available_permits() == CONNECTION_LIMIT - 1).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    until(|| c.selection.permits.available_permits() == CONNECTION_LIMIT).await;
    assert!(c.selection.feed.lock().unwrap().records.is_empty());
    assert_eq!(
        f.services
            .repository_selection_capacity
            .records
            .available_permits(),
        GLOBAL_LIMIT
    );
    drop(held);
    let fresh = s.capture(&f).await.unwrap();
    assert_eq!(fresh.snapshot.selection, SelectionState::NeverSaved);
    assert_eq!(c.selection.feed.lock().unwrap().records.len(), 1);
}
