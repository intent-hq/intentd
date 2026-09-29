//! Real Store/Git/native owners. Unit callers are explicit transport fixtures;
//! the separate daemon target establishes the actual UDS/WSS entry.
use super::*;
use crate::repository_admission_source_tests::fixtures::Fixture as GitFixture;
use intent_core::{
    now_iso, HostRole, Principal, PrincipalId, WorkspaceApi, WorkspaceGitRoot, WorkspaceGitRootId,
    WorkspaceRole,
};
use serde_json::json;
use std::future::Future;

struct Fixture {
    git: GitFixture,
    services: Arc<Services>,
    caller: Caller,
}
impl Fixture {
    async fn new() -> Self {
        let git = GitFixture::new().await;
        let services = Arc::new(
            Services::new_repository_fixture(
                git.store.clone(),
                intent_core::FileSecretStore::with_path(git.dir.path().join("secrets.json")),
                None,
            )
            .with_workspaces_root(git.dir.path().join("workspaces")),
        );
        services.initialize_repository_wire().await.unwrap();
        let owner = services.store.get_primary_principal().await.unwrap();
        Self {
            git,
            services,
            caller: Caller::Wire {
                principal_id: owner.id,
                host_role: HostRole::Owner,
            },
        }
    }
    fn query(&self) -> RepositoryContextQuery {
        RepositoryContextQuery {
            workspace_id: self.git.workspace.id.clone(),
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
        let mut socket = Socket {
            owner: with_caller(
                caller.clone(),
                with_wire_credential(credential.clone(), async {
                    connection(&self.services, entry).unwrap()
                }),
            )
            .await,
            caller,
            credential,
            receiver: Mutex::new(None),
        };
        *socket.receiver.get_mut().unwrap() = socket.owner.take_retirements();
        assert!(socket.receiver.get_mut().unwrap().is_some());
        socket
    }
    async fn registered(&self, name: &str) -> WorkspaceGitRoot {
        let path = self.git.dir.path().join(name);
        let repo = git2::Repository::init(&path).unwrap();
        let tree_id = repo.index().unwrap().write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let sig = git2::Signature::now("Fixture", "fixture@example.invalid").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "fixture", &tree, &[])
            .unwrap();
        let row:WorkspaceGitRoot=serde_json::from_value(json!({"id":WorkspaceGitRootId::new(),"workspaceId":self.git.workspace.id,"path":path,"source":"auto","registeredByAgentIds":[],"createdAt":now_iso(),"updatedAt":now_iso()})).unwrap();
        self.services
            .store
            .upsert_workspace_git_root(&row)
            .await
            .unwrap()
            .0
    }
    async fn guest(&self, hash: &str, member: bool) -> Socket {
        let person = Principal {
            id: PrincipalId::new(),
            identity: None,
            github_user_id: Some(42),
            login: Some("guest".into()),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        self.services.store.upsert_principal(&person).await.unwrap();
        self.services
            .store
            .insert_principal_credential(&person.id, hash)
            .await
            .unwrap();
        if member {
            self.services
                .store
                .add_workspace_member(
                    &self.git.workspace.id,
                    &person.id,
                    WorkspaceRole::Collaborator,
                )
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
                token_hash: hash.into(),
            }),
        )
        .await
    }
}
struct Socket {
    owner: Arc<dyn RepositoryReadConnection>,
    caller: Caller,
    credential: Option<WireCredential>,
    receiver: Mutex<Option<Box<dyn RepositoryReadRetirements>>>,
}
impl Socket {
    async fn entered<T>(&self, body: impl Future<Output = T>) -> T {
        with_caller(
            self.caller.clone(),
            with_wire_credential(self.credential.clone(), body),
        )
        .await
    }
    async fn request<T>(&self, body: impl Future<Output = Result<T>> + Send) -> Result<T>
    where
        T: Send,
    {
        self.entered(async {
            let scope = self.owner.capture();
            let mut answer = None;
            scope
                .scope(Box::pin(async {
                    let value = body.await;
                    let mut effects = 0;
                    let delivered = scope
                        .deliver(
                            if value.is_ok() {
                                RepositoryReadReplyKind::Result
                            } else {
                                RepositoryReadReplyKind::ServiceError
                            },
                            &mut || {
                                effects += 1;
                                Ok(())
                            },
                        )
                        .await;
                    assert!(effects <= 1);
                    answer = Some(delivered.and(value));
                }))
                .await;
            scope.retire();
            answer.unwrap()
        })
        .await
    }
    async fn capture(&self, f: &Fixture) -> Result<RepositoryContextCapture> {
        self.request(async { f.services.repository_context_capture(f.query()).await })
            .await
    }
    async fn read(&self, f: &Fixture, c: &RepositoryContextCapture) -> Result<RepositoryContext> {
        self.request(async {
            f.services
                .repository_context(bound(f.query(), &c.lifetime_id))
                .await
        })
        .await
    }
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.owner.retire();
    }
}
fn bound(q: RepositoryContextQuery, id: &str) -> RepositoryContextBoundQuery {
    RepositoryContextBoundQuery {
        workspace_id: q.workspace_id,
        git_root_id: q.git_root_id,
        repository_lifetime_id: id.into(),
    }
}

#[intent_test_macros::daemon_test]
async fn native_wire_original_cold_owner_inventory_and_exact_root() {
    let f = Fixture::new().await;
    let root = f.registered("registered").await;
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/team/repo.git",
        ],
    );
    let s = f.socket().await;
    let c = s.capture(&f).await.unwrap();
    let first = s.read(&f, &c).await.unwrap();
    let second = s.read(&f, &c).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(first.roots.len(), 2);
    assert_eq!(first.scope, c.scope);
    assert_eq!(c.retirement_sequence, "0");
    assert!(matches!(
        c.coverage,
        RepositoryContextCoverage::WorkspaceInventory { .. }
    ));
    assert!(first
        .roots
        .iter()
        .flat_map(|r| &r.targets)
        .all(|t| t.connection.is_none()));
    let query = RepositoryContextQuery {
        workspace_id: f.git.workspace.id.clone(),
        git_root_id: Some(root.id.clone()),
    };
    let exact = s
        .request(async { f.services.repository_context_capture(query.clone()).await })
        .await
        .unwrap();
    let one = s
        .request(async {
            f.services
                .repository_context(bound(query.clone(), &exact.lifetime_id))
                .await
        })
        .await
        .unwrap();
    assert_eq!(one.roots.len(), 1);
    assert!(
        matches!(&one.roots[0].root.kind,RepositoryRootKind::Registered{git_root_id} if *git_root_id==root.id)
    );
    assert!(s
        .request(async {
            f.services
                .repository_context(bound(query, &c.lifetime_id))
                .await
        })
        .await
        .is_err());
    let other = f.socket().await;
    assert!(other.read(&f, &c).await.is_err());
    assert!(f
        .services
        .repository_context_capture(f.query())
        .await
        .is_err());
}

#[intent_test_macros::daemon_test]
async fn native_wire_real_membership_revoke_and_readd_never_repairs_lease() {
    let f = Fixture::new().await;
    let s = f.guest("guest-hash", false).await;
    assert!(s.capture(&f).await.is_err());
    let Caller::Wire { principal_id, .. } = &s.caller else {
        panic!()
    };
    f.services
        .store
        .add_workspace_member(
            &f.git.workspace.id,
            principal_id,
            WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    let c = s.capture(&f).await.unwrap();
    assert!(s.read(&f, &c).await.is_ok());
    f.services
        .store
        .remove_workspace_member(&f.git.workspace.id, principal_id)
        .await
        .unwrap();
    f.services
        .store
        .add_workspace_member(
            &f.git.workspace.id,
            principal_id,
            WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    assert!(s.read(&f, &c).await.is_err());
    let fresh = s.capture(&f).await.unwrap();
    assert_ne!(fresh.lifetime_id, c.lifetime_id);
    f.services
        .store
        .revoke_principal_credential("guest-hash")
        .await
        .unwrap();
    assert!(s.read(&f, &fresh).await.is_err());
    assert!(s.capture(&f).await.is_err());
}

#[intent_test_macros::daemon_test]
async fn native_wire_pending_root_change_and_private_retirement_cursor() {
    let f = Fixture::new().await;
    let mut s = f.socket().await;
    let c = s.capture(&f).await.unwrap();
    let pending = f
        .services
        .repository_lifecycle_registry
        .begin_pending_delete(&[RepositoryLifecycleKey::Workspace(
            f.git.workspace.id.clone(),
        )])
        .unwrap();
    let event = tokio::time::timeout(
        Duration::from_secs(2),
        s.receiver.get_mut().unwrap().as_mut().unwrap().next(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(event.lifetime_ids, vec![c.lifetime_id.clone()]);
    assert_eq!(event.sequence, "1");
    assert!(!event.all_retired);
    assert!(s.capture(&f).await.is_err());
    pending.settle_confirmed();
    assert!(s.read(&f, &c).await.is_err());
    let fresh = s.capture(&f).await.unwrap();
    f.registered("new-root").await;
    assert!(s.read(&f, &fresh).await.is_err());
    let again = s.capture(&f).await.unwrap();
    let release = bound(f.query(), &again.lifetime_id);
    assert!(
        s.request(async { f.services.repository_context_release(release.clone()).await })
            .await
            .unwrap()
            .released
    );
    assert!(
        s.request(async { f.services.repository_context_release(release).await })
            .await
            .unwrap()
            .released
    );
}

#[intent_test_macros::daemon_test]
async fn native_wire_final_git_change_and_consumed_failure_never_replay() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let c = s.capture(&f).await.unwrap();
    s.entered(async {
        let scope = s.owner.capture();
        scope
            .scope(Box::pin(async {
                f.services
                    .repository_context(bound(f.query(), &c.lifetime_id))
                    .await
                    .unwrap();
                f.git.git(
                    &f.git.path,
                    &["remote", "add", "late", "https://github.com/team/late.git"],
                );
                let mut effects = 0;
                assert!(scope
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        effects += 1;
                        Ok(())
                    })
                    .await
                    .is_err());
                assert_eq!(effects, 0);
            }))
            .await;
        scope.retire();
    })
    .await;
    let fresh = s.capture(&f).await.unwrap();
    s.entered(async {
        let scope = s.owner.capture();
        scope
            .scope(Box::pin(async {
                f.services
                    .repository_context(bound(f.query(), &fresh.lifetime_id))
                    .await
                    .unwrap();
                let mut effects = 0;
                assert!(scope
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        effects += 1;
                        Err(Error::Internal("consumer failure".into()))
                    })
                    .await
                    .is_err());
                assert_eq!(effects, 1);
            }))
            .await;
        scope.retire();
    })
    .await;
}

#[intent_test_macros::daemon_test]
async fn native_wire_unpublished_capture_cancel_and_receiver_loss_close_original_only() {
    let f = Fixture::new().await;
    let mut s = f.socket().await;
    let capture = s
        .entered(async {
            let scope = s.owner.capture();
            let mut captured = None;
            scope
                .scope(Box::pin(async {
                    captured = Some(
                        f.services
                            .repository_context_capture(f.query())
                            .await
                            .unwrap(),
                    );
                }))
                .await;
            scope.retire();
            captured.unwrap()
        })
        .await;
    assert!(s.read(&f, &capture).await.is_err());
    let fresh = s.capture(&f).await.unwrap();
    drop(s.receiver.get_mut().unwrap().take());
    assert!(s.read(&f, &fresh).await.is_err());
    assert!(s.capture(&f).await.is_err());
    let other = f.socket().await;
    assert!(other.capture(&f).await.is_ok());
}

impl Socket {
    async fn concrete(&self) -> Arc<Connection> {
        self.entered(async {
            let request = self.owner.capture();
            let mut found = None;
            request
                .scope(Box::pin(async {
                    found = Some(NATIVE_REQUEST.with(|r| r.connection.clone()));
                }))
                .await;
            request.retire();
            found.unwrap()
        })
        .await
    }
}

#[intent_test_macros::daemon_test]
async fn native_wire_bounds_overflow_expiry_and_same_id_foreign_caller() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let connection = s.concrete().await;
    let c = s.capture(&f).await.unwrap();
    with_caller(Caller::Daemon, async {
        let request = s.owner.capture();
        request
            .scope(Box::pin(async {
                assert!(f
                    .services
                    .repository_context(bound(f.query(), &c.lifetime_id))
                    .await
                    .is_err());
            }))
            .await;
        request.retire();
    })
    .await;
    let other = WireCredential::Principal {
        principal_id: s.caller.principal_id().unwrap().clone(),
        token_hash: "changed-credential".into(),
    };
    s.entered(with_wire_credential(Some(other), async {
        let request = s.owner.capture();
        request
            .scope(Box::pin(async {
                assert!(f
                    .services
                    .repository_context(bound(f.query(), &c.lifetime_id))
                    .await
                    .is_err());
            }))
            .await;
        request.retire();
    }))
    .await;
    assert!(s.read(&f, &c).await.is_ok());
    tokio::time::timeout(Duration::from_secs(5), async {
        while Arc::strong_count(
            connection
                .state
                .lock()
                .unwrap()
                .leases
                .get(&c.lifetime_id)
                .unwrap(),
        ) != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let mut state = connection.state.lock().unwrap();
        Arc::get_mut(state.leases.get_mut(&c.lifetime_id).unwrap())
            .unwrap()
            .deadline = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
    }
    assert!(s.read(&f, &c).await.is_err());
    let fresh = s.capture(&f).await.unwrap();
    connection.state.lock().unwrap().sequence = u64::MAX;
    connection.retire_id(&fresh.lifetime_id);
    assert!(s.capture(&f).await.is_err());
    assert!(connection.state.lock().unwrap().closed);
    let other = f.socket().await;
    let mut ids = Vec::new();
    for _ in 0..LEASE_LIMIT {
        ids.push(other.capture(&f).await.unwrap());
    }
    assert!(other.capture(&f).await.is_err());
    for c in &ids {
        other
            .request(async {
                f.services
                    .repository_context_release(bound(f.query(), &c.lifetime_id))
                    .await
            })
            .await
            .unwrap();
    }
    // The next notification exceeds the original undrained bounded feed.
    let one = other.capture(&f).await.unwrap();
    other.concrete().await.retire_id(&one.lifetime_id);
    assert!(other.capture(&f).await.is_err());
}

#[intent_test_macros::daemon_test]
async fn native_wire_actual_settings_and_provider_projection_stay_original() {
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
        Fixture as AuthFixture, Server,
    };
    let server = Server::new().await;
    let auth = AuthFixture::new(&server).await;
    let mut git = GitFixture::new().await;
    auth.service
        .store
        .insert_workspace(&git.workspace)
        .await
        .unwrap();
    git.store = auth.service.store.clone();
    git.git(
        &git.path,
        &[
            "remote",
            "add",
            "origin",
            &format!(
                "{}/group/project.git",
                server.descriptor.instance().as_str()
            ),
        ],
    );
    auth.service.initialize_repository_wire().await.unwrap();
    let owner = auth.service.store.get_primary_principal().await.unwrap();
    let f = Fixture {
        git,
        services: auth.service.clone(),
        caller: Caller::Wire {
            principal_id: owner.id,
            host_role: HostRole::Owner,
        },
    };
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "add",
            "local-github",
            "https://github.com/team/local.git",
        ],
    );
    let guest = f.guest("projection-guest", true).await;
    let owner = f.socket().await;
    let calls = server.control.requests.lock().unwrap().len();
    let c = owner.capture(&f).await.unwrap();
    let value = owner.read(&f, &c).await.unwrap();
    assert!(value.roots[0]
        .targets
        .iter()
        .any(
            |target| target.target.provider == intent_core::RepositoryProvider::Gitlab
                && target.connection.is_some()
        ));
    let gc = guest.capture(&f).await.unwrap();
    let local = guest.read(&f, &gc).await.unwrap();
    assert!(!local.roots[0].remotes.is_empty());
    assert!(!local.roots[0].targets.is_empty());
    assert!(local.roots[0].targets.iter().all(|t| t.connection.is_none()
        && t.availability == intent_core::RepositoryAvailability::Unknown));
    assert_eq!(
        server.control.requests.lock().unwrap().len(),
        calls,
        "native context performs no provider request"
    );
    f.services
        .gitlab_connect_pat(server.host.clone(), "pat-second".into())
        .await
        .unwrap();
    assert!(owner.read(&f, &c).await.is_err());
    assert!(guest.read(&f, &gc).await.is_ok());
    let next = owner.capture(&f).await.unwrap();
    auth.registry
        .apply(&[("git.autoCommit".into(), json!(true))])
        .unwrap();
    assert!(owner.read(&f, &next).await.is_err());
    let current = owner.capture(&f).await.unwrap();
    assert!(owner.read(&f, &current).await.is_ok());
}

#[intent_test_macros::daemon_test]
async fn native_wire_final_transfer_joins_writer_and_keeps_completed_effect() {
    let f = Fixture::new().await;
    let s = Arc::new(f.socket().await);
    let c = s.capture(&f).await.unwrap();
    let service = f.services.clone();
    let query = bound(f.query(), &c.lifetime_id);
    let (entered, admitted) = tokio::sync::oneshot::channel();
    let (resume, held) = std::sync::mpsc::channel();
    let held = Mutex::new(held);
    let owner = s.clone();
    let send = tokio::task::spawn_blocking(move || {
        tokio::runtime::Handle::current().block_on(async move {
            owner
                .entered(async {
                    let scope = owner.owner.capture();
                    scope
                        .scope(Box::pin(async {
                            service.repository_context(query).await.unwrap();
                            let mut entered = Some(entered);
                            let mut effects = 0;
                            scope
                                .deliver(RepositoryReadReplyKind::Result, &mut || {
                                    effects += 1;
                                    entered.take().unwrap().send(()).unwrap();
                                    held.lock()
                                        .unwrap()
                                        .recv_timeout(Duration::from_secs(5))
                                        .unwrap();
                                    Ok(())
                                })
                                .await
                                .unwrap();
                            assert_eq!(effects, 1);
                        }))
                        .await;
                    scope.retire();
                })
                .await;
        });
    });
    admitted.await.unwrap();
    let registry = f.services.repository_lifecycle_registry.clone();
    let key = RepositoryLifecycleKey::Workspace(f.git.workspace.id.clone());
    let (started, waiting) = tokio::sync::oneshot::channel();
    let close = tokio::task::spawn_blocking(move || {
        started.send(()).unwrap();
        registry.begin_mutation(&[key]).unwrap()
    });
    waiting.await.unwrap();
    assert!(!close.is_finished());
    resume.send(()).unwrap();
    send.await.unwrap();
    close.await.unwrap().settle_confirmed();
    assert!(s.read(&f, &c).await.is_err());
}

#[derive(Default)]
struct BlockingHold {
    entered: Notify,
    released: Mutex<bool>,
    wake: std::sync::Condvar,
}
impl BlockingHold {
    fn hold(&self) {
        self.entered.notify_one();
        let (guard, expired) = self
            .wake
            .wait_timeout_while(
                self.released.lock().unwrap(),
                Duration::from_secs(15),
                |released| !*released,
            )
            .unwrap();
        assert!(!expired.timed_out());
        assert!(*guard);
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

#[intent_test_macros::daemon_test]
async fn native_wire_cancelled_blocking_job_keeps_actual_worktree_lock_until_exit() {
    let f = Fixture::new().await;
    let s = Arc::new(f.socket().await);
    let c = s.capture(&f).await.unwrap();
    let connection = s.concrete().await;
    let hold = Arc::new(BlockingHold::default());
    let probe = hold.clone();
    *connection.git_probe.lock().unwrap() = Some(Arc::new(move || probe.hold()));
    let scope = s.entered(async { s.owner.capture() }).await;
    let original = scope.clone();
    let owner = s.clone();
    let service = f.services.clone();
    let query = bound(f.query(), &c.lifetime_id);
    let task = tokio::spawn(async move {
        owner
            .entered(async {
                original
                    .scope(Box::pin(async {
                        let _ = service.repository_context(query).await;
                    }))
                    .await;
            })
            .await;
    });
    tokio::time::timeout(Duration::from_secs(5), hold.entered.notified())
        .await
        .unwrap();
    scope.retire();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(connection.active_jobs.load(Ordering::Acquire), 1);
    assert!(f
        .services
        .worktree_locks
        .try_with_lock(&f.git.path, || async {})
        .await
        .is_none());
    hold.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let changed = connection.jobs_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if connection.active_jobs.load(Ordering::Acquire) == 0 {
                break;
            }
            changed.await;
        }
    })
    .await
    .unwrap();
    assert!(f
        .services
        .worktree_locks
        .try_with_lock(&f.git.path, || async {})
        .await
        .is_some());
    assert!(s.read(&f, &c).await.is_err());
    *connection.git_probe.lock().unwrap() = None;
    let fresh = s.capture(&f).await.unwrap();
    assert!(s.read(&f, &fresh).await.is_ok());
}

#[intent_test_macros::daemon_test]
async fn native_wire_queued_worktree_cancellation_closes_without_waiting_for_lock() {
    let f = Fixture::new().await;
    let s = Arc::new(f.socket().await);
    let c = s.capture(&f).await.unwrap();
    let connection = s.concrete().await;
    let (entered, held) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let services = f.services.clone();
    let path = f.git.path.clone();
    let holder = tokio::spawn(async move {
        services
            .worktree_locks
            .with_lock(&path, || async {
                entered.send(()).unwrap();
                wait.await.unwrap();
            })
            .await;
    });
    held.await.unwrap();
    let scope = s.entered(async { s.owner.capture() }).await;
    let original = scope.clone();
    let owner = s.clone();
    let service = f.services.clone();
    let query = bound(f.query(), &c.lifetime_id);
    let read = tokio::spawn(async move {
        owner
            .entered(async {
                original
                    .scope(Box::pin(async {
                        assert!(service.repository_context(query).await.is_err());
                    }))
                    .await;
            })
            .await;
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while connection.active_jobs.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    scope.retire();
    tokio::time::timeout(Duration::from_secs(2), read)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(connection.active_jobs.load(Ordering::Acquire), 0);
    release.send(()).unwrap();
    holder.await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn native_wire_saved_selection_historical_and_exact_root_incarnation() {
    use intent_core::{
        ReviewSelectionOutcome, ReviewSelectionRequiredReason, SavedReviewSelection,
    };
    use intent_store::{RepositorySelectionChange, RepositorySelectionWriteResult};
    let mut f = Fixture::new().await;
    f.services
        .store
        .delete_workspace(&f.git.workspace.id)
        .await
        .unwrap();
    f.git.workspace.repository_owner = Some("historical".into());
    f.git.workspace.repository_name = Some("old".into());
    f.services
        .store
        .insert_workspace(&f.git.workspace)
        .await
        .unwrap();
    f.git.git(
        &f.git.path,
        &["remote", "add", "origin", "https://github.com/team/one.git"],
    );
    f.git.git(
        &f.git.path,
        &["remote", "add", "second", "https://github.com/team/two.git"],
    );
    let s = f.socket().await;
    let old = s.capture(&f).await.unwrap();
    let context = s.read(&f, &old).await.unwrap();
    assert!(matches!(
        context.roots[0].review_selection.saved,
        SavedReviewSelection::UnresolvedHistorical { .. }
    ));
    let root = RepositoryRootId {
        workspace_id: f.git.workspace.id.clone(),
        kind: RepositoryRootKind::Primary,
    };
    let before = f
        .services
        .store
        .repository_selection_snapshot(&root)
        .await
        .unwrap();
    let revision = before.selection_revision();
    let _ = s.read(&f, &old).await.unwrap();
    assert_eq!(
        f.services
            .store
            .repository_selection_snapshot(&root)
            .await
            .unwrap()
            .selection_revision(),
        revision,
        "read does not persist Automatic"
    );
    assert!(matches!(
        f.services
            .store
            .write_repository_selection(&before, RepositorySelectionChange::Automatic)
            .await
            .result
            .unwrap(),
        RepositorySelectionWriteResult::Applied(_)
    ));
    assert!(s.read(&f, &old).await.is_err());
    let automatic = s.capture(&f).await.unwrap();
    let context = s.read(&f, &automatic).await.unwrap();
    assert!(matches!(
        context.roots[0].review_selection.outcome,
        ReviewSelectionOutcome::SelectionRequired {
            reason: ReviewSelectionRequiredReason::AmbiguousTargets,
            ..
        }
    ));
    let before = f
        .services
        .store
        .repository_selection_snapshot(&root)
        .await
        .unwrap();
    assert!(matches!(
        f.services
            .store
            .write_repository_selection(
                &before,
                RepositorySelectionChange::ExplicitRemote {
                    remote_name: "second".into()
                }
            )
            .await
            .result
            .unwrap(),
        RepositorySelectionWriteResult::Applied(_)
    ));
    let explicit = s.capture(&f).await.unwrap();
    assert!(
        matches!(s.read(&f,&explicit).await.unwrap().roots[0].review_selection.saved,SavedReviewSelection::ExplicitRemote{ref remote_name} if remote_name=="second")
    );
    let first = f.registered("exact-one").await;
    let second = f.registered("exact-two").await;
    let query = RepositoryContextQuery {
        workspace_id: f.git.workspace.id.clone(),
        git_root_id: Some(first.id.clone()),
    };
    let exact = s
        .request(async { f.services.repository_context_capture(query.clone()).await })
        .await
        .unwrap();
    f.services
        .store
        .delete_workspace_git_root(&second.id)
        .await
        .unwrap();
    assert!(s
        .request(async {
            f.services
                .repository_context(bound(query.clone(), &exact.lifetime_id))
                .await
        })
        .await
        .is_ok());
    f.services
        .store
        .delete_workspace_git_root(&first.id)
        .await
        .unwrap();
    f.services
        .store
        .upsert_workspace_git_root(&first)
        .await
        .unwrap();
    assert!(
        s.request(async {
            f.services
                .repository_context(bound(query, &exact.lifetime_id))
                .await
        })
        .await
        .is_err(),
        "same root ID cannot repair the original incarnation"
    );
}

#[intent_test_macros::daemon_test]
async fn native_wire_actual_host_member_chief_boundary_and_durable_role_change() {
    let f = Fixture::new().await;
    let guest = f.guest("host-member", false).await;
    let person = f
        .services
        .store
        .get_principal(guest.caller.principal_id().unwrap())
        .await
        .unwrap();
    let owner = f.services.store.get_primary_principal().await.unwrap();
    let invite = intent_core::HostInvite::new(
        "native-member".into(),
        owner.id,
        person.identity_key().unwrap(),
        person.login.clone().unwrap(),
        "invite-proof".into(),
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
                token_hash: "native-member-credential",
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
                token_hash: "native-member-credential".into(),
            }),
        )
        .await;
    let captured = member.capture(&f).await.unwrap();
    let value = member.read(&f, &captured).await.unwrap();
    assert!(value
        .roots
        .iter()
        .flat_map(|r| &r.targets)
        .all(|t| t.connection.is_none()));
    assert!(
        guest.capture(&f).await.is_err(),
        "old Guest socket cannot adopt changed durable host role"
    );
    if f.services
        .store
        .get_workspace(&intent_core::WorkspaceId::chief())
        .await
        .is_err()
    {
        f.services
            .store
            .insert_workspace(&intent_core::chief_workspace())
            .await
            .unwrap();
    }
    assert!(member
        .request(async {
            f.services
                .repository_context_capture(RepositoryContextQuery {
                    workspace_id: intent_core::WorkspaceId::chief(),
                    git_root_id: None,
                })
                .await
        })
        .await
        .is_err());
    f.services
        .store
        .remove_host_member(&person.id)
        .await
        .unwrap();
    assert!(member.read(&f, &captured).await.is_err());
    assert!(member.capture(&f).await.is_err());
}

#[intent_test_macros::daemon_test]
async fn native_wire_failed_api_foreign_allocation_nested_capture_and_queued_retirement() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let copied = f.services.as_ref().clone();
    s.entered(async {
        assert!(connection(&copied, RepositoryWireEntry::AdmittedLocal).is_none());
    })
    .await;
    assert!(Arc::new(copied).initialize_repository_wire().await.is_err());
    for caller in [
        Caller::Daemon,
        Caller::Agent {
            agent_id: intent_core::AgentId::new(),
        },
    ] {
        with_caller(caller, async {
            assert!(connection(&f.services, RepositoryWireEntry::AdmittedLocal).is_none());
        })
        .await;
    }
    let request = s.entered(async { s.owner.capture() }).await;
    s.entered(request.scope(Box::pin(async {
        let nested = s.owner.capture();
        nested
            .scope(Box::pin(async {
                assert!(f
                    .services
                    .repository_context_capture(f.query())
                    .await
                    .is_err());
            }))
            .await;
        nested.retire();
    })))
    .await;
    request.retire();
    let queued = s.entered(async { s.owner.capture() }).await;
    let mutation = f
        .services
        .repository_lifecycle_registry
        .begin_mutation(&[RepositoryLifecycleKey::Database])
        .unwrap();
    mutation.settle_confirmed();
    s.entered(queued.scope(Box::pin(async {
        assert!(f
            .services
            .repository_context_capture(f.query())
            .await
            .is_err());
    })))
    .await;
    queued.retire();
    assert!(
        s.capture(&f).await.is_err(),
        "database retirement cannot repair original connection"
    );
    let fresh = f.socket().await;
    assert!(fresh.capture(&f).await.is_ok());
}

#[intent_test_macros::daemon_test]
async fn native_wire_final_permission_retirement_and_consumer_panic_leave_no_replay() {
    let f = Fixture::new().await;
    let s = f.guest("final-guest", true).await;
    let c = s.capture(&f).await.unwrap();
    let principal = s.caller.principal_id().unwrap().clone();
    s.entered(async {
        let scope = s.owner.capture();
        scope
            .scope(Box::pin(async {
                f.services
                    .repository_context(bound(f.query(), &c.lifetime_id))
                    .await
                    .unwrap();
                f.services
                    .store
                    .remove_workspace_member(&f.git.workspace.id, &principal)
                    .await
                    .unwrap();
                let mut effects = 0;
                assert!(scope
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        effects += 1;
                        Ok(())
                    })
                    .await
                    .is_err());
                assert_eq!(effects, 0);
            }))
            .await;
        scope.retire();
    })
    .await;
    let owner = f.socket().await;
    let fresh = owner.capture(&f).await.unwrap();
    owner
        .entered(async {
            let scope = owner.owner.capture();
            scope
                .scope(Box::pin(async {
                    f.services
                        .repository_context(bound(f.query(), &fresh.lifetime_id))
                        .await
                        .unwrap();
                    let mut effects = 0;
                    let mut action = || -> Result<()> {
                        effects += 1;
                        panic!("native consuming fixture")
                    };
                    let panicked = {
                        let mut future =
                            scope.deliver(RepositoryReadReplyKind::Result, &mut action);
                        std::future::poll_fn(|cx| {
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                future.as_mut().poll(cx)
                            })) {
                                Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
                                Ok(std::task::Poll::Ready(value)) => {
                                    std::task::Poll::Ready(Ok(value))
                                }
                                Err(error) => std::task::Poll::Ready(Err(error)),
                            }
                        })
                        .await
                    };
                    assert!(panicked.is_err());
                    assert_eq!(effects, 1);
                    assert!(scope
                        .deliver(RepositoryReadReplyKind::Result, &mut || {
                            effects += 1;
                            Ok(())
                        })
                        .await
                        .is_err());
                    assert_eq!(effects, 1);
                }))
                .await;
            scope.retire();
        })
        .await;
}

#[intent_test_macros::daemon_test]
async fn native_wire_final_actual_settings_reload_and_provider_replacement_refuse() {
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
        Fixture as AuthFixture, Server,
    };
    let server = Server::new().await;
    let auth = AuthFixture::new(&server).await;
    let mut git = GitFixture::new().await;
    auth.service
        .store
        .insert_workspace(&git.workspace)
        .await
        .unwrap();
    git.store = auth.service.store.clone();
    git.git(
        &git.path,
        &[
            "remote",
            "add",
            "origin",
            &format!(
                "{}/group/project.git",
                server.descriptor.instance().as_str()
            ),
        ],
    );
    auth.service.initialize_repository_wire().await.unwrap();
    let owner = auth.service.store.get_primary_principal().await.unwrap();
    let f = Fixture {
        git,
        services: auth.service.clone(),
        caller: Caller::Wire {
            principal_id: owner.id,
            host_role: HostRole::Owner,
        },
    };
    let socket = f.socket().await;
    for stage in 0..3 {
        let c = socket.capture(&f).await.unwrap();
        let calls = server.control.requests.lock().unwrap().len();
        socket
            .entered(async {
                let scope = socket.owner.capture();
                scope
                    .scope(Box::pin(async {
                        let context = f
                            .services
                            .repository_context(bound(f.query(), &c.lifetime_id))
                            .await
                            .unwrap();
                        assert!(!context.roots.is_empty());
                        assert_eq!(server.control.requests.lock().unwrap().len(), calls);
                        match stage {
                            0 => {
                                auth.registry
                                    .apply(&[("git.autoCommit".into(), json!(true))])
                                    .unwrap();
                            }
                            1 => {
                                f.services
                                    .gitlab_connect_pat(server.host.clone(), "pat-second".into())
                                    .await
                                    .unwrap();
                            }
                            _ => {
                                let text =
                                    std::fs::read_to_string(auth.registry.config_path()).unwrap();
                                let replacement =
                                    text.replace("autoCommit = true", "autoCommit = false");
                                assert_ne!(text, replacement);
                                auth.registry.reload(&replacement).unwrap();
                            }
                        }
                        let mut effects = 0;
                        assert!(scope
                            .deliver(RepositoryReadReplyKind::Result, &mut || {
                                effects += 1;
                                Ok(())
                            })
                            .await
                            .is_err());
                        assert_eq!(effects, 0);
                    }))
                    .await;
                scope.retire();
            })
            .await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_wire_original_absent_provider_never_inherits_late_installation() {
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::Server;
    let f = Fixture::new().await;
    let s = f.socket().await;
    let c = s.capture(&f).await.unwrap();
    let before = s.read(&f, &c).await.unwrap();
    assert!(before
        .roots
        .iter()
        .flat_map(|r| &r.targets)
        .all(|t| t.connection.is_none()));
    let server = Server::new().await;
    let registry = SettingsRegistry::load(f.git.dir.path().join("late-settings.toml")).unwrap();
    let guard = f.services.gitlab_credential_gate.lock().await;
    f.services
        .gitlab_credential_gate
        .install_settings_boundary(
            &registry,
            &f.services.secrets,
            &f.services.gitlab_secret_store,
            Some(server.descriptor.clone()),
        )
        .unwrap();
    drop(guard);
    let after = s.read(&f, &c).await.unwrap();
    assert_eq!(before, after);
    assert!(server.control.requests.lock().unwrap().is_empty());
}

#[intent_test_macros::daemon_test]
async fn native_wire_actual_event_receiver_gap_closes_only_original_feed() {
    let git = GitFixture::new().await;
    let bus = crate::EventBus::new(git.store.clone());
    let services = Arc::new(
        Services::new_repository_fixture(
            git.store.clone(),
            intent_core::FileSecretStore::with_path(git.dir.path().join("gap-secrets.json")),
            None,
        )
        .with_event_bus(bus.clone()),
    );
    services.initialize_repository_wire().await.unwrap();
    let owner = services.store.get_primary_principal().await.unwrap();
    let f = Fixture {
        git,
        services,
        caller: Caller::Wire {
            principal_id: owner.id,
            host_role: HostRole::Owner,
        },
    };
    let mut socket = f.socket().await;
    let c = socket.capture(&f).await.unwrap();
    let event = intent_store::NewEvent {
        workspace_id: f.git.workspace.id.clone(),
        timestamp: now_iso(),
        event_type: "git:changed".into(),
        actor: intent_core::EventActor::default(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({}),
    };
    // No await: the real broadcast receiver cannot drain this bounded burst.
    for _ in 0..2048 {
        let _ = bus.publish_transient(&event);
    }
    let notice = tokio::time::timeout(
        Duration::from_secs(3),
        socket.receiver.get_mut().unwrap().as_mut().unwrap().next(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(notice.terminal && notice.all_retired);
    assert!(notice.lifetime_ids.is_empty());
    assert!(socket.read(&f, &c).await.is_err());
    assert!(socket.capture(&f).await.is_err());
    let fresh = f.socket().await;
    assert!(fresh.capture(&f).await.is_ok());
}

#[intent_test_macros::daemon_test]
async fn native_wire_read_budget_and_checked_revision_overflow_never_repair() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let c = s.capture(&f).await.unwrap();
    let connection = s.concrete().await;
    connection
        .state
        .lock()
        .unwrap()
        .leases
        .get(&c.lifetime_id)
        .unwrap()
        .reads
        .store(READ_LIMIT - 1, Ordering::Release);
    assert!(s.read(&f, &c).await.is_ok());
    assert!(s.read(&f, &c).await.is_err());
    let next = s.capture(&f).await.unwrap();
    connection
        .state
        .lock()
        .unwrap()
        .leases
        .get(&next.lifetime_id)
        .unwrap()
        .state
        .lock()
        .unwrap()
        .sequence = u64::MAX;
    assert!(s.read(&f, &next).await.is_err());
    let fresh = s.capture(&f).await.unwrap();
    assert!(s.read(&f, &fresh).await.is_ok());
}

#[intent_test_macros::daemon_test]
async fn native_wire_frame_root_is_owned_before_queue_and_never_rebound() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    for key in [
        RepositoryLifecycleKey::WireAuthority,
        RepositoryLifecycleKey::Workspace(f.git.workspace.id.clone()),
        RepositoryLifecycleKey::RootInventory(f.git.workspace.id.clone()),
    ] {
        let frame = s
            .entered(async { s.owner.capture_context(&f.query()) })
            .await;
        let ticket = f
            .services
            .repository_lifecycle_registry
            .begin_mutation(&[key])
            .unwrap();
        ticket.settle_confirmed();
        s.entered(frame.scope(Box::pin(async {
            assert!(f
                .services
                .repository_context_capture(f.query())
                .await
                .is_err());
        })))
        .await;
        frame.retire();
    }
    let root = f.registered("queued-original").await;
    let query = RepositoryContextQuery {
        workspace_id: f.git.workspace.id.clone(),
        git_root_id: Some(root.id.clone()),
    };
    let frame = s.entered(async { s.owner.capture_context(&query) }).await;
    f.services
        .store
        .delete_workspace_git_root(&root.id)
        .await
        .unwrap();
    f.services
        .store
        .upsert_workspace_git_root(&root)
        .await
        .unwrap();
    s.entered(frame.scope(Box::pin(async {
        assert!(f
            .services
            .repository_context_capture(query.clone())
            .await
            .is_err());
    })))
    .await;
    frame.retire();
    let frame = s.entered(async { s.owner.capture_context(&query) }).await;
    s.entered(frame.scope(Box::pin(async {
        assert!(
            f.services
                .repository_context_capture(f.query())
                .await
                .is_err(),
            "queued root cannot widen to inventory"
        );
    })))
    .await;
    frame.retire();
    assert!(s.capture(&f).await.is_ok());
}

#[intent_test_macros::daemon_test]
async fn native_wire_cancelled_acquisition_closes_before_late_metadata_returns() {
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
        Fixture as AuthFixture, Server,
    };
    let server = Server::new().await;
    let auth = AuthFixture::new(&server).await;
    let mut git = GitFixture::new().await;
    auth.service
        .store
        .insert_workspace(&git.workspace)
        .await
        .unwrap();
    git.store = auth.service.store.clone();
    auth.service.initialize_repository_wire().await.unwrap();
    let owner = auth.service.store.get_primary_principal().await.unwrap();
    let f = Fixture {
        git,
        services: auth.service.clone(),
        caller: Caller::Wire {
            principal_id: owner.id,
            host_role: HostRole::Owner,
        },
    };
    let s = Arc::new(f.socket().await);
    let connection = s.concrete().await;
    let hold = Arc::new(BlockingHold::default());
    let held = hold.clone();
    let registry = auth.registry.clone();
    let writer = tokio::task::spawn_blocking(move || {
        registry.context_hold_snapshot_for_test(|| held.hold());
    });
    hold.entered.notified().await;
    let scope = s
        .entered(async { s.owner.capture_context(&f.query()) })
        .await;
    let retained = scope.clone();
    let socket = s.clone();
    let service = f.services.clone();
    let q = f.query();
    let capture = tokio::spawn(async move {
        socket
            .entered(retained.scope(Box::pin(async {
                let _ = service.repository_context_capture(q).await;
            })))
            .await;
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while connection.active_jobs.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    scope.retire();
    capture.abort();
    assert!(capture.await.unwrap_err().is_cancelled());
    assert_eq!(connection.permits.available_permits(), LEASE_LIMIT);
    assert!(connection.state.lock().unwrap().leases.is_empty());
    assert_eq!(connection.active_jobs.load(Ordering::Acquire), 1);
    hold.release();
    writer.await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while connection.active_jobs.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(connection.state.lock().unwrap().leases.is_empty());
    assert!(s.capture(&f).await.is_ok());
}

#[intent_test_macros::daemon_test]
async fn native_wire_final_future_moved_after_worker_start_cannot_restore_foreign_caller() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let connection = s.concrete().await;
    for credential_change in [false, true] {
        let c = s.capture(&f).await.unwrap();
        s.entered(async{let scope=s.owner.capture_context(&f.query());scope.scope(Box::pin(async{
            f.services.repository_context(bound(f.query(),&c.lifetime_id)).await.unwrap();
            let hold=Arc::new(BlockingHold::default());let hook=hold.clone();*connection.git_probe.lock().unwrap()=Some(Arc::new(move||hook.hold()));
            let mut effects=0;let mut action=||{effects+=1;Ok(())};let mut future=scope.deliver(RepositoryReadReplyKind::Result,&mut action);
            tokio::select! { result=&mut future=>panic!("delivery completed before held worker: {result:?}"), ()=hold.entered.notified()=>{} }
            hold.release();
            let result=if credential_change { with_wire_credential(Some(WireCredential::Principal{principal_id:s.caller.principal_id().unwrap().clone(),token_hash:"foreign-poll".into()}),&mut future).await } else { with_caller(Caller::Daemon,&mut future).await };
            assert!(result.is_err());drop(future);assert_eq!(effects,0);*connection.git_probe.lock().unwrap()=None;
        })).await;scope.retire();}).await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_wire_queued_frame_expires_without_slot_or_service_entry() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let scope = s
        .entered(async { s.owner.capture_context(&f.query()) })
        .await;
    let mut request = None;
    s.entered(scope.scope(Box::pin(async {
        request = Some(NATIVE_REQUEST.with(Clone::clone));
    })))
    .await;
    let request = request.unwrap();
    tokio::time::timeout(
        FRAME_TTL + Duration::from_secs(2),
        request
            .lifetime
            .as_ref()
            .unwrap()
            .retirement()
            .native_cancelled(),
    )
    .await
    .unwrap();
    assert!(request.completed.load(Ordering::Acquire));
    assert!(request.connection.state.lock().unwrap().leases.is_empty());
    s.entered(scope.scope(Box::pin(async {
        assert!(f
            .services
            .repository_context_capture(f.query())
            .await
            .is_err());
    })))
    .await;
    scope.retire();
    assert!(s.capture(&f).await.is_ok());
}

#[intent_test_macros::daemon_test]
async fn native_wire_final_construction_and_failed_acquisition_transfer_do_not_publish() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let connection = s.concrete().await;
    for foreign in [false, true] {
        s.entered(async {
            let scope = s.owner.capture_context(&f.query());
            scope
                .scope(Box::pin(async {
                    let captured = f
                        .services
                        .repository_context_capture(f.query())
                        .await
                        .unwrap();
                    let mut effects = 0;
                    let mut action = || {
                        effects += 1;
                        Err(Error::Internal("owned slot closed".into()))
                    };
                    if foreign {
                        let (future,) = with_caller(Caller::Daemon, async {
                            (scope.deliver(RepositoryReadReplyKind::Result, &mut action),)
                        })
                        .await;
                        assert!(future.await.is_err());
                        assert_eq!(effects, 0);
                    } else {
                        assert!(scope
                            .deliver(RepositoryReadReplyKind::Result, &mut action)
                            .await
                            .is_err());
                        assert_eq!(effects, 1);
                    }
                    assert!(!connection
                        .state
                        .lock()
                        .unwrap()
                        .leases
                        .get(&captured.lifetime_id)
                        .unwrap()
                        .published
                        .load(Ordering::Acquire));
                }))
                .await;
            scope.retire();
        })
        .await;
        assert!(connection.state.lock().unwrap().leases.is_empty());
        assert_eq!(connection.permits.available_permits(), LEASE_LIMIT);
    }
    assert!(s.capture(&f).await.is_ok());
}

#[intent_test_macros::daemon_test]
async fn native_wire_actual_disconnected_and_mixed_root_facts_need_no_provider_read() {
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
        Fixture as AuthFixture, Server,
    };
    use intent_core::{RepositoryAvailability, RepositoryProvider};
    let server = Server::new().await;
    let auth = AuthFixture::new(&server).await;
    let mut git = GitFixture::new().await;
    auth.service
        .store
        .insert_workspace(&git.workspace)
        .await
        .unwrap();
    git.store = auth.service.store.clone();
    git.git(
        &git.path,
        &[
            "remote",
            "add",
            "forge",
            &format!(
                "{}/group/project.git",
                server.descriptor.instance().as_str()
            ),
        ],
    );
    git.git(
        &git.path,
        &[
            "remote",
            "add",
            "github",
            "https://github.com/team/public.git",
        ],
    );
    auth.service.initialize_repository_wire().await.unwrap();
    let owner = auth.service.store.get_primary_principal().await.unwrap();
    let f = Fixture {
        git,
        services: auth.service.clone(),
        caller: Caller::Wire {
            principal_id: owner.id,
            host_role: HostRole::Owner,
        },
    };
    f.registered("local-only").await;
    let s = f.socket().await;
    let c = s.capture(&f).await.unwrap();
    let before = s.read(&f, &c).await.unwrap();
    assert_eq!(before.roots.len(), 2);
    assert!(before.roots[1].remotes.is_empty());
    assert!(before.roots[0]
        .targets
        .iter()
        .any(|t| t.target.provider == RepositoryProvider::Gitlab
            && t.availability == RepositoryAvailability::Connected
            && t.connection.is_some()));
    assert!(before.roots[0]
        .targets
        .iter()
        .any(|t| t.target.provider == RepositoryProvider::Github
            && t.availability == RepositoryAvailability::Unknown
            && t.connection.is_none()));
    let calls = server.control.requests.lock().unwrap().len();
    f.services
        .gitlab_revoke_owned(server.host.clone())
        .await
        .unwrap();
    assert!(s.read(&f, &c).await.is_err());
    let fresh = s.capture(&f).await.unwrap();
    let disconnected = s.read(&f, &fresh).await.unwrap();
    assert!(disconnected.roots[0]
        .targets
        .iter()
        .any(|t| t.target.provider == RepositoryProvider::Gitlab
            && t.availability == RepositoryAvailability::Disconnected
            && t.connection.is_none()));
    assert!(disconnected.roots[1].remotes.is_empty());
    assert_eq!(server.control.requests.lock().unwrap().len(), calls);
}

#[intent_test_macros::daemon_test]
async fn native_wire_final_provider_refresh_busy_and_poison_refuse_original_context() {
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
        Fixture as AuthFixture, Server,
    };
    use intent_sourcecontrol::gitlab_token::{EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT};
    for mode in ["refresh", "busy", "poison"] {
        let server = Server::new().await;
        let auth = AuthFixture::new(&server).await;
        if mode == "refresh" {
            auth.service
                .gitlab_secret_store
                .store(REFRESH_SECRET_ACCOUNT, "refresh-old")
                .unwrap();
            let expiry = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 7200;
            auth.service
                .gitlab_secret_store
                .store(EXPIRES_AT_SECRET_ACCOUNT, &expiry.to_string())
                .unwrap();
            auth.service
                .reconcile_gitlab_repository_binding()
                .await
                .unwrap();
        }
        let mut git = GitFixture::new().await;
        auth.service
            .store
            .insert_workspace(&git.workspace)
            .await
            .unwrap();
        git.store = auth.service.store.clone();
        git.git(
            &git.path,
            &[
                "remote",
                "add",
                "forge",
                &format!(
                    "{}/group/project.git",
                    server.descriptor.instance().as_str()
                ),
            ],
        );
        auth.service.initialize_repository_wire().await.unwrap();
        let owner = auth.service.store.get_primary_principal().await.unwrap();
        let f = Fixture {
            git,
            services: auth.service.clone(),
            caller: Caller::Wire {
                principal_id: owner.id,
                host_role: HostRole::Owner,
            },
        };
        let s = f.socket().await;
        let c = s.capture(&f).await.unwrap();
        s.entered(async {
            let scope = s.owner.capture_context(&f.query());
            scope
                .scope(Box::pin(async {
                    let context = f
                        .services
                        .repository_context(bound(f.query(), &c.lifetime_id))
                        .await
                        .unwrap();
                    assert!(context.roots[0].targets[0].connection.is_some());
                    let facts = f.services.gitlab_repository_connection_facts().unwrap();
                    let mut held = None;
                    if mode == "refresh" {
                        let original = auth.request();
                        f.services
                            .gitlab_secret_store
                            .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
                            .unwrap();
                        f.services
                            .stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab {
                                host: server.host.clone(),
                            })
                            .await
                            .unwrap();
                        let current = auth.request();
                        assert_eq!(current.binding, original.binding);
                        assert!(current.secret_revision > original.secret_revision);
                    } else if mode == "poison" {
                        assert!(std::thread::spawn(move || {
                            RepositoryConnectionFacts::with_native_context_current(
                                Some(&facts),
                                |current| {
                                    assert!(current);
                                    panic!("native metadata poison fixture");
                                },
                            )
                            .unwrap();
                        })
                        .join()
                        .is_err());
                    } else {
                        let hold = Arc::new(BlockingHold::default());
                        let blocked = hold.clone();
                        let task = tokio::task::spawn_blocking(move || {
                            RepositoryConnectionFacts::with_native_context_current(
                                Some(&facts),
                                |current| {
                                    assert!(current);
                                    blocked.hold();
                                    Ok(())
                                },
                            )
                        });
                        hold.entered.notified().await;
                        held = Some((hold, task));
                    }
                    let calls = server.control.requests.lock().unwrap().len();
                    let mut effects = 0;
                    assert!(scope
                        .deliver(RepositoryReadReplyKind::Result, &mut || {
                            effects += 1;
                            Ok(())
                        })
                        .await
                        .is_err());
                    assert_eq!(effects, 0);
                    assert_eq!(server.control.requests.lock().unwrap().len(), calls);
                    if let Some((hold, task)) = held {
                        hold.release();
                        task.await.unwrap().unwrap();
                    }
                }))
                .await;
            scope.retire();
        })
        .await;
    }
}

#[intent_test_macros::daemon_test]
async fn native_wire_disabled_child_capability_is_metadata_and_never_a_grant() {
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
        Fixture as AuthFixture, Server,
    };
    use intent_core::{RepositoryCapabilityState, RepositoryOperation};
    let server = Server::new().await;
    let auth = AuthFixture::new(&server).await;
    let mut git = GitFixture::new().await;
    auth.service
        .store
        .insert_workspace(&git.workspace)
        .await
        .unwrap();
    git.store = auth.service.store.clone();
    git.git(
        &git.path,
        &[
            "remote",
            "add",
            "forge",
            &format!(
                "{}/group/project.git",
                server.descriptor.instance().as_str()
            ),
        ],
    );
    let binding = auth.request().binding;
    auth.service
        .repository_connection_directory
        .set_child_policy(&binding, false)
        .unwrap();
    auth.service.initialize_repository_wire().await.unwrap();
    let owner = auth.service.store.get_primary_principal().await.unwrap();
    let f = Fixture {
        git,
        services: auth.service.clone(),
        caller: Caller::Wire {
            principal_id: owner.id,
            host_role: HostRole::Owner,
        },
    };
    let s = f.socket().await;
    let calls = server.control.requests.lock().unwrap().len();
    let c = s.capture(&f).await.unwrap();
    let context = s.read(&f, &c).await.unwrap();
    let target = &context.roots[0].targets[0];
    assert!(target.connection.is_some());
    for cap in &target.capabilities {
        assert_eq!(
            cap.state,
            if matches!(
                cap.operation,
                RepositoryOperation::Clone | RepositoryOperation::Fetch | RepositoryOperation::Push
            ) {
                RepositoryCapabilityState::Unavailable
            } else {
                RepositoryCapabilityState::Unknown
            }
        );
    }
    assert_eq!(server.control.requests.lock().unwrap().len(), calls);
}

// Member target qualification: real original owners, private facts and redacted output.
use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
    Fixture as MemberAuthFixture, Server as MemberServer,
};

struct MemberTargetFixture {
    auth: MemberAuthFixture,
    server: MemberServer,
    native: Fixture,
}
impl MemberTargetFixture {
    async fn new() -> Self {
        let server = MemberServer::new().await;
        let auth = MemberAuthFixture::new(&server).await;
        let mut git = GitFixture::new().await;
        auth.service
            .store
            .insert_workspace(&git.workspace)
            .await
            .unwrap();
        git.store = auth.service.store.clone();
        git.git(
            &git.path,
            &[
                "remote",
                "add",
                "forge",
                &format!(
                    "{}/group/project.git",
                    server.descriptor.instance().as_str()
                ),
            ],
        );
        auth.service.initialize_repository_wire().await.unwrap();
        let principal = auth.service.store.get_primary_principal().await.unwrap();
        let native = Fixture {
            git,
            services: auth.service.clone(),
            caller: Caller::Wire {
                principal_id: principal.id,
                host_role: HostRole::Owner,
            },
        };
        Self {
            auth,
            server,
            native,
        }
    }
}

async fn target_member(f: &Fixture) -> (Socket, Principal) {
    let person = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: Some(4306),
        login: Some("target-member".into()),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    f.services.store.upsert_principal(&person).await.unwrap();
    let owner = f.services.store.get_primary_principal().await.unwrap();
    let invite = intent_core::HostInvite::new(
        "target-invite".into(),
        owner.id,
        person.identity_key().unwrap(),
        person.login.clone().unwrap(),
        "target-proof".into(),
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
                token_hash: "target-member-token",
                authorization_generation: generation,
            },
        )
        .await
        .unwrap();
    let socket = f
        .socket_as(
            Caller::Wire {
                principal_id: person.id.clone(),
                host_role: HostRole::Member,
            },
            Some(WireCredential::Principal {
                principal_id: person.id.clone(),
                token_hash: "target-member-token".into(),
            }),
        )
        .await;
    (socket, person)
}

fn assert_target_redaction(context: &RepositoryContext) {
    for target in context.roots.iter().flat_map(|r| &r.targets) {
        assert!(target.connection.is_none());
        assert!(target.provider_project_id.is_none());
        assert_eq!(
            target.availability,
            intent_core::RepositoryAvailability::Unknown
        );
        assert!(target
            .capabilities
            .iter()
            .all(|c| c.state == intent_core::RepositoryCapabilityState::Unknown));
    }
    let encoded = serde_json::to_string(context).unwrap();
    for field in [
        "connectionId",
        "connectionGeneration",
        "accountId",
        "providerProjectId",
        "stored-pat",
        "target-member-token",
    ] {
        assert!(!encoded.contains(field), "public context contains {field}");
    }
    assert_eq!(
        serde_json::from_str::<RepositoryContext>(&encoded).unwrap(),
        *context
    );
}

#[intent_test_macros::daemon_test]
async fn native_wire_member_qualification_projection() {
    let fixture = MemberTargetFixture::new().await;
    let f = &fixture.native;
    let registered = f.registered("member-github-root").await;
    f.git.git(
        std::path::Path::new(&registered.path),
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/team/ordinary.git",
        ],
    );
    let (member, _) = target_member(f).await;
    let guest = f.guest("target-guest-token", true).await;
    let owner = f.socket().await;
    let calls = fixture.server.control.requests.lock().unwrap().len();
    for (label, socket) in [("owner", &owner), ("member", &member), ("guest", &guest)] {
        let capture = socket.capture(f).await.unwrap();
        let value = socket.read(f, &capture).await.unwrap();
        assert_eq!(value.roots.len(), 2);
        let primary = value
            .roots
            .iter()
            .find(|r| matches!(r.root.kind, RepositoryRootKind::Primary))
            .unwrap();
        let gitlab = primary
            .targets
            .iter()
            .find(|t| t.target.provider == intent_core::RepositoryProvider::Gitlab);
        if label == "guest" {
            assert!(gitlab.is_none());
            assert!(matches!(
                primary.review_selection.outcome,
                intent_core::ReviewSelectionOutcome::SelectionRequired { .. }
            ));
        } else {
            let gitlab = gitlab.unwrap_or_else(|| {
                panic!("{label} must resolve original configured GitLab: {value:?}")
            });
            assert_eq!(
                gitlab.target.instance_base_url,
                fixture.server.descriptor.instance().as_str()
            );
            assert_eq!(gitlab.target.project_path, "group/project");
            assert!(matches!(
                primary.review_selection.outcome,
                intent_core::ReviewSelectionOutcome::Resolved { .. }
            ));
            assert_eq!(gitlab.connection.is_some(), label == "owner");
        }
        assert!(value
            .roots
            .iter()
            .flat_map(|r| &r.targets)
            .any(
                |t| t.target.provider == intent_core::RepositoryProvider::Github
                    && t.target.project_path == "team/ordinary"
            ));
        if label != "owner" {
            assert_target_redaction(&value);
        }
        assert_eq!(
            fixture.server.control.requests.lock().unwrap().len(),
            calls,
            "context performs no provider HTTP"
        );
        eprintln!(
            "member qualification projection {label}: {}",
            serde_json::to_string(&value).unwrap()
        );
    }
}

#[intent_test_macros::daemon_test]
async fn native_wire_member_qualification_final_transfer() {
    for mode in [
        "role",
        "credential",
        "workspace",
        "root",
        "selection",
        "provider",
        "settings",
    ] {
        let fixture = MemberTargetFixture::new().await;
        let f = &fixture.native;
        let registered = f.registered("member-original-root").await;
        f.git.git(
            std::path::Path::new(&registered.path),
            &[
                "remote",
                "add",
                "forge",
                &format!(
                    "{}/group/project.git",
                    fixture.server.descriptor.instance().as_str()
                ),
            ],
        );
        let (s, person) = target_member(f).await;
        let capture = s.capture(f).await.unwrap();
        s.entered(async {
            let scope = s.owner.capture_context(&f.query());
            scope
                .scope(Box::pin(async {
                    let body = f
                        .services
                        .repository_context(bound(f.query(), &capture.lifetime_id))
                        .await
                        .unwrap();
                    assert_target_redaction(&body);
                    assert!(body
                        .roots
                        .iter()
                        .flat_map(|r| &r.targets)
                        .any(|t| t.target.provider == intent_core::RepositoryProvider::Gitlab));
                    match mode {
                        "role" => {
                            f.services
                                .store
                                .remove_host_member(&person.id)
                                .await
                                .unwrap();
                        }
                        "credential" => {
                            assert!(f
                                .services
                                .store
                                .revoke_principal_credential("target-member-token")
                                .await
                                .unwrap());
                        }
                        "workspace" => {
                            f.services
                                .store
                                .delete_workspace(&f.git.workspace.id)
                                .await
                                .unwrap();
                        }
                        "root" => {
                            f.services
                                .store
                                .delete_workspace_git_root(&registered.id)
                                .await
                                .unwrap();
                        }
                        "selection" => {
                            let root = RepositoryRootId {
                                workspace_id: f.git.workspace.id.clone(),
                                kind: RepositoryRootKind::Primary,
                            };
                            let original = f
                                .services
                                .store
                                .repository_selection_snapshot(&root)
                                .await
                                .unwrap();
                            f.services
                                .store
                                .write_repository_selection(
                                    &original,
                                    intent_store::RepositorySelectionChange::Automatic,
                                )
                                .await
                                .result
                                .unwrap();
                        }
                        "provider" => {
                            f.services
                                .gitlab_connect_pat(
                                    fixture.server.host.clone(),
                                    "replacement-pat".into(),
                                )
                                .await
                                .unwrap();
                        }
                        "settings" => {
                            fixture
                                .auth
                                .registry
                                .apply(&[("git.autoCommit".into(), json!(true))])
                                .unwrap();
                        }
                        _ => unreachable!(),
                    }
                    let calls = fixture.server.control.requests.lock().unwrap().len();
                    let mut transfers = 0;
                    let delivered = scope
                        .deliver(RepositoryReadReplyKind::Result, &mut || {
                            transfers += 1;
                            Ok(())
                        })
                        .await;
                    assert!(delivered.is_err(), "{mode}");
                    assert_eq!(transfers, 0, "{mode}");
                    assert_eq!(fixture.server.control.requests.lock().unwrap().len(), calls);
                    eprintln!(
                        "member original final transfer {mode}: refused; transfers={transfers}"
                    );
                }))
                .await;
            scope.retire();
        })
        .await;
        match mode {
            "workspace" => {
                f.services
                    .store
                    .insert_workspace(&f.git.workspace)
                    .await
                    .unwrap();
            }
            "root" => {
                f.services
                    .store
                    .upsert_workspace_git_root(&registered)
                    .await
                    .unwrap();
            }
            "provider" => {
                f.services
                    .gitlab_connect_pat(fixture.server.host.clone(), "stored-pat".into())
                    .await
                    .unwrap();
            }
            "settings" => {
                fixture
                    .auth
                    .registry
                    .apply(&[("git.autoCommit".into(), json!(false))])
                    .unwrap();
            }
            _ => {}
        }
        assert!(
            s.read(f, &capture).await.is_err(),
            "restoration cannot repair {mode}"
        );
    }
    let fixture = MemberTargetFixture::new().await;
    let f = &fixture.native;
    let (s, _) = target_member(f).await;
    let capture = s.capture(f).await.unwrap();
    let second = f.socket_as(s.caller.clone(), s.credential.clone()).await;
    assert!(second.read(f, &capture).await.is_err());
    let other = Fixture::new().await;
    other
        .services
        .store
        .insert_workspace(&f.git.workspace)
        .await
        .unwrap();
    s.entered(async {
        let scope = s.owner.capture_context(&f.query());
        scope
            .scope(Box::pin(async {
                assert!(
                    other
                        .services
                        .repository_context(bound(f.query(), &capture.lifetime_id))
                        .await
                        .is_err(),
                    "equal public workspace ID cannot change original Services host"
                );
            }))
            .await;
        scope.retire();
    })
    .await;
    assert_target_redaction(&s.read(f, &capture).await.unwrap());
}

#[intent_test_macros::daemon_test]
async fn native_wire_member_qualification_unknown_and_busy() {
    use crate::source_control_auth_ops::repository_owner::RepositoryDescriptorState;
    // Absent metadata cannot be filled by a later installation, even on this Services.
    let f = Fixture::new().await;
    let server = MemberServer::new().await;
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "add",
            "forge",
            &format!(
                "{}/group/project.git",
                server.descriptor.instance().as_str()
            ),
        ],
    );
    let (s, _) = target_member(&f).await;
    let absent = s.capture(&f).await.unwrap();
    let before = s.read(&f, &absent).await.unwrap();
    assert!(before.roots[0].targets.is_empty());
    let registry =
        SettingsRegistry::load(f.git.dir.path().join("member-late-settings.toml")).unwrap();
    registry
        .apply(&[
            ("sourceControl.gitlab.host".into(), json!("gitlab.test")),
            (
                "sourceControl.gitlab.instanceBaseUrl".into(),
                json!(server.descriptor.instance().as_str()),
            ),
            (
                "sourceControl.gitlab.apiBaseUrl".into(),
                json!(server.host.base_url()),
            ),
        ])
        .unwrap();
    let guard = f.services.gitlab_credential_gate.lock().await;
    f.services
        .gitlab_credential_gate
        .install_settings_boundary(
            &registry,
            &f.services.secrets,
            &f.services.gitlab_secret_store,
            None,
        )
        .unwrap();
    drop(guard);
    assert!(matches!(
        f.services
            .gitlab_repository_connection_facts()
            .unwrap()
            .approval(),
        RepositoryDescriptorState::Unapproved
    ));
    assert_eq!(s.read(&f, &absent).await.unwrap(), before);
    let unapproved = s.capture(&f).await.unwrap();
    let value = s.read(&f, &unapproved).await.unwrap();
    assert!(value.roots[0].targets.is_empty());
    assert_target_redaction(&value);
    assert!(server.control.requests.lock().unwrap().is_empty());

    let fixture = MemberTargetFixture::new().await;
    let f = &fixture.native;
    let unknown = f.registered("member-unknown-instance").await;
    f.git.git(
        std::path::Path::new(&unknown.path),
        &[
            "remote",
            "add",
            "forge",
            "https://unapproved.invalid/group/project.git",
        ],
    );
    let (s, _) = target_member(f).await;
    let calls = fixture.server.control.requests.lock().unwrap().len();
    let current = s.capture(f).await.unwrap();
    let value = s.read(f, &current).await.unwrap();
    assert_target_redaction(&value);
    assert_eq!(
        value.roots.iter().filter(|r| !r.targets.is_empty()).count(),
        1
    );
    f.services
        .gitlab_revoke_owned(fixture.server.host.clone())
        .await
        .unwrap();
    assert!(s.read(f, &current).await.is_err());
    let disconnected = s.capture(f).await.unwrap();
    let value = s.read(f, &disconnected).await.unwrap();
    assert_eq!(
        value
            .roots
            .iter()
            .flat_map(|r| &r.targets)
            .filter(|t| t.target.provider == intent_core::RepositoryProvider::Gitlab)
            .count(),
        1
    );
    assert_target_redaction(&value);
    assert_eq!(fixture.server.control.requests.lock().unwrap().len(), calls);

    let fixture = MemberTargetFixture::new().await;
    let f = &fixture.native;
    let (s, _) = target_member(f).await;
    let capture = s.capture(f).await.unwrap();
    s.entered(async {
        let scope = s.owner.capture_context(&f.query());
        scope
            .scope(Box::pin(async {
                let value = f
                    .services
                    .repository_context(bound(f.query(), &capture.lifetime_id))
                    .await
                    .unwrap();
                assert_target_redaction(&value);
                let facts = f.services.gitlab_repository_connection_facts().unwrap();
                let hold = Arc::new(BlockingHold::default());
                let blocked = hold.clone();
                let task = tokio::task::spawn_blocking(move || {
                    RepositoryConnectionFacts::with_native_context_current(
                        Some(&facts),
                        |current| {
                            assert!(current);
                            blocked.hold();
                            Ok(())
                        },
                    )
                });
                hold.entered.notified().await;
                let calls = fixture.server.control.requests.lock().unwrap().len();
                let mut transfers = 0;
                let result = scope
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        transfers += 1;
                        Ok(())
                    })
                    .await;
                hold.release();
                task.await.unwrap().unwrap();
                assert!(result.is_err());
                assert_eq!(transfers, 0);
                assert_eq!(fixture.server.control.requests.lock().unwrap().len(), calls);
            }))
            .await;
        scope.retire();
    })
    .await;
    assert!(s.read(f, &capture).await.is_err());
    let fresh = s.capture(f).await.unwrap();
    assert_target_redaction(&s.read(f, &fresh).await.unwrap());
}
