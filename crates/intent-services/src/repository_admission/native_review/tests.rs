//! Actual Store/Git/P effects under explicit original transport fixtures. The
//! daemon target separately proves real sockets; these callers do not claim it.
use super::credential_tests::{services, Server};
use super::*;
use crate::repository_admission_source_tests::fixtures::Fixture as Git;
use intent_core::repository_request::{
    RepositoryReadConnection, RepositoryReadRetirements, RepositoryWireEntry,
};
use intent_core::{HostRole, WorkspaceApi};

struct Fixture {
    git: Git,
    services: Arc<Services>,
    registry: Arc<crate::SettingsRegistry>,
    server: Server,
}
impl Fixture {
    async fn new() -> Self {
        let git = Git::new().await;
        git.git(&git.path, &["config", "user.name", "Fixture"]);
        git.git(
            &git.path,
            &["config", "user.email", "fixture@example.invalid"],
        );
        let server = Server::new().await;
        *server.control.sha.lock().unwrap() =
            git.git(&git.path, &["rev-parse", "HEAD"]).trim().into();
        let (services, registry) = services(&server, &git).await;
        Self {
            git,
            services,
            registry,
            server,
        }
    }
    fn query(&self, action: Stage) -> Prepare {
        serde_json::from_value(json!({"workspaceId":self.git.workspace.id,"action":action,"review":{"root":self.git.root(),"choice":{"kind":"explicitTarget","target":{"provider":"gitlab","instanceBaseUrl":"https://gitlab.test/forge","projectPath":"group/project"}},"targetBranch":"trunk"}})).unwrap()
    }
    async fn socket(&self) -> Socket {
        let p = self.services.store.get_primary_principal().await.unwrap();
        Socket::new(
            &self.services,
            Caller::Wire {
                principal_id: p.id,
                host_role: HostRole::Owner,
            },
            None,
        )
        .await
    }
    fn stage(&self, name: &str) {
        std::fs::write(self.git.path.join(name), name).unwrap();
        self.git.git(&self.git.path, &["add", name]);
    }
}
struct Socket {
    owner: Arc<dyn RepositoryReadConnection>,
    caller: Caller,
    wire: Option<WireCredential>,
    _context: Mutex<Box<dyn RepositoryReadRetirements>>,
    receiver: Mutex<Option<Box<dyn NativeReviewRetirements>>>,
}
impl Socket {
    async fn new(s: &Arc<Services>, caller: Caller, wire: Option<WireCredential>) -> Self {
        let owner = with_caller(
            caller.clone(),
            with_wire_credential(wire.clone(), async {
                super::super::connection(
                    s,
                    if wire.is_some() {
                        RepositoryWireEntry::Bearer
                    } else {
                        RepositoryWireEntry::AdmittedLocal
                    },
                )
                .unwrap()
            }),
        )
        .await;
        let context = owner.take_retirements().unwrap();
        let receiver = owner.take_review_retirements().unwrap();
        Self {
            owner,
            caller,
            wire,
            _context: Mutex::new(context),
            receiver: Mutex::new(Some(receiver)),
        }
    }
    async fn entered<T>(&self, f: impl Future<Output = T>) -> T {
        with_caller(
            self.caller.clone(),
            with_wire_credential(self.wire.clone(), f),
        )
        .await
    }
    async fn request(&self, s: &Services, f: Frame) -> Result<Value> {
        self.entered(async {
            let scope = self.owner.capture_review(&f).unwrap();
            let mut result = None;
            scope
                .scope(Box::pin(async {
                    let reply = match f {
                        Frame::Prepare(q) => s.native_review_prepare(q).await,
                        Frame::Execute(q) => s.native_review_execute(q).await,
                        Frame::Reconcile(q) => s.native_review_reconcile(q).await,
                        Frame::Release(q) => s.native_review_release(q).await,
                    };
                    let mut count = 0;
                    let delivered = scope
                        .deliver(
                            if reply.is_ok() {
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
                    result = Some(delivered.and(reply));
                }))
                .await;
            scope.retire();
            result.unwrap()
        })
        .await
    }
    async fn prepare(&self, f: &Fixture, q: Prepare) -> Value {
        self.request(&f.services, Frame::Prepare(q)).await.unwrap()
    }
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.owner.retire();
    }
}
fn command(f: &Fixture, p: &Value, stage: Stage) -> Execute {
    serde_json::from_value(json!({"workspaceId":f.git.workspace.id,"action":stage,"review":{"operationId":p["reviewOperation"]["operationId"],"root":p["reviewOperation"]["root"]},"commitMessage":"owned commit","prTitle":"owned ready MR","prBody":"body"})).unwrap()
}
fn bound(q: &Execute) -> Bound {
    Bound {
        workspace_id: q.workspace_id.clone(),
        operation_id: q.review.operation_id.clone(),
        root: q.review.root.clone(),
    }
}

#[intent_test_macros::daemon_test]
async fn native_review_commit_and_separate_create_retain_original_effects() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("staged.txt");
    std::fs::write(f.git.path.join("unstaged.txt"), "later").unwrap();
    let before = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let value = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(
        value["reviewExecution"]["gitReceipts"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "{value}"
    );
    assert!(value["success"].as_bool().unwrap(), "{value}");
    let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    assert_eq!(
        value["reviewExecution"]["gitReceipts"][0]["commitHash"],
        head.trim()
    );
    assert_eq!(
        f.git
            .git(&f.git.path, &["show", "--pretty=", "--name-only", "HEAD"])
            .trim(),
        "staged.txt"
    );
    let again = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(value, again);
    let mut changed = q.clone();
    changed.commit_message = Some("different".into());
    assert!(s
        .request(&f.services, Frame::Execute(changed))
        .await
        .is_err());
    let history = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(history["reviewExecution"], value["reviewExecution"]);
    let create = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let create = command(&f, &create, Stage::CreatePr);
    let result = s
        .request(&f.services, Frame::Execute(create))
        .await
        .unwrap();
    assert_eq!(
        result["reviewExecution"]["outcome"]["status"], "created",
        "{result}"
    );
    assert_eq!(result["reviewExecution"]["gitReceipts"], json!([]));
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), head);
    let after = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    assert_eq!(before.selection_revision(), after.selection_revision());
    assert_eq!(before.selection(), after.selection());
}
#[intent_test_macros::daemon_test]
async fn native_review_repeated_create_reuses_exact_and_uncertain_never_repairs() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.server.control.lost_post.store(true, Ordering::SeqCst);
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    let lost = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(
        lost["reviewExecution"]["outcome"]["status"], "uncertain",
        "{lost}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    f.server.control.lost_post.store(false, Ordering::SeqCst);
    let again = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(again["reviewExecution"], lost["reviewExecution"]);
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let newer = s
        .request(
            &f.services,
            Frame::Execute(command(&f, &p, Stage::CreatePr)),
        )
        .await
        .unwrap();
    assert_eq!(
        newer["reviewExecution"]["outcome"]["status"], "reused",
        "{newer}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
}
#[intent_test_macros::daemon_test]
async fn native_review_confirmation_changes_release_and_foreign_socket_refuse() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let other = f.socket().await;
    f.stage("initial");
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    assert!(other
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .is_err());
    f.stage("changed-index");
    let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    let result = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(result["success"], false, "{result}");
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), head);
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    s.request(&f.services, Frame::Release(bound(&q)))
        .await
        .unwrap();
    assert!(s.request(&f.services, Frame::Execute(q)).await.is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}
#[intent_test_macros::daemon_test]
async fn native_review_unsupported_provider_ssh_and_changed_settings_do_not_fallback() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let mut q = f.query(Stage::CreatePr);
    if let Choice::ExplicitTarget { target } = &mut q.review.choice {
        target.provider = RepositoryProvider::Github;
        target.instance_base_url = "https://github.com".into();
    }
    assert!(s.request(&f.services, Frame::Prepare(q)).await.is_err());
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "add",
            "origin",
            "git@gitlab.test:group/project.git",
        ],
    );
    let mut q = f.query(Stage::Push);
    q.review.push_remote = Some("origin".into());
    assert!(s.request(&f.services, Frame::Prepare(q)).await.is_err());
    f.git.git(&f.git.path, &["remote", "remove", "origin"]);
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    f.registry
        .apply(&[("git.autoCommit".into(), json!(true))])
        .unwrap();
    assert!(s.request(&f.services, Frame::Execute(q)).await.is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_primitive_commit_observation_precedes_panic_and_never_infers_head() {
    let f = Fixture::new().await;
    f.stage("first");
    let seen = Mutex::new(Vec::new());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        intent_git::commit::commit_observed(&f.git.path, "observed", |sha| {
            seen.lock().unwrap().push(sha.to_string());
            panic!("after original primitive observation");
        })
    }));
    assert!(panic.is_err());
    let seen = seen.into_inner().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(
        f.git.git(&f.git.path, &["rev-parse", "HEAD"]).trim(),
        seen[0]
    );
    let calls = AtomicUsize::new(0);
    assert!(
        intent_git::commit::commit_observed(&f.git.path, "empty", |_| {
            calls.fetch_add(1, Ordering::SeqCst);
        })
        .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[intent_test_macros::daemon_test]
async fn native_review_publication_uses_confirmed_source_and_actual_ancestry() {
    let f = Fixture::new().await;
    let original = f
        .git
        .git(&f.git.path, &["rev-parse", "HEAD"])
        .trim()
        .to_string();
    f.stage("ahead");
    let next = intent_git::commit::commit(&f.git.path, "ahead")
        .unwrap()
        .hash;
    assert!(matches!(
        publication(&f.git.path, Some(next.clone()), Ok(Some(original.clone()))),
        Publication::LocalAhead { .. }
    ));
    assert!(matches!(
        publication(&f.git.path, Some(original.clone()), Ok(Some(next.clone()))),
        Publication::Included { .. }
    ));
    assert!(matches!(
        publication(&f.git.path, Some(next.clone()), Ok(None)),
        Publication::RemoteBranchMissing { .. }
    ));
    assert!(matches!(
        publication(&f.git.path, Some(next.clone()), Err(unavailable())),
        Publication::Unknown {
            remote_source_sha: None,
            ..
        }
    ));
    assert!(matches!(
        publication(
            &f.git.path,
            Some(next.clone()),
            Ok(Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()))
        ),
        Publication::Unknown { .. }
    ));
    f.git
        .git(&f.git.path, &["checkout", "-b", "other", &original]);
    f.stage("other");
    let other = intent_git::commit::commit(&f.git.path, "other")
        .unwrap()
        .hash;
    assert!(matches!(
        publication(&f.git.path, Some(next), Ok(Some(other))),
        Publication::Diverged { .. }
    ));
}
#[intent_test_macros::daemon_test]
async fn native_review_construction_poll_and_unpolled_command_keep_original_owner() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let q = f.query(Stage::CreatePr);
    assert!(f.services.native_review_prepare(q.clone()).await.is_err());
    let scope = s
        .entered(async { s.owner.capture_review(&Frame::Prepare(q.clone())).unwrap() })
        .await;
    let mut denied = false;
    s.entered(scope.scope(Box::pin(async {
        let constructed = f.services.native_review_prepare(q.clone());
        denied = with_caller(Caller::Daemon, constructed).await.is_err();
    })))
    .await;
    assert!(denied);
    scope.retire();
    let p = s.prepare(&f, q).await;
    let command = command(&f, &p, Stage::CreatePr);
    let unpolled = s
        .entered(async {
            s.owner
                .capture_review(&Frame::Execute(command.clone()))
                .unwrap()
        })
        .await;
    unpolled.retire();
    drop(unpolled);
    let state = s
        .request(&f.services, Frame::Reconcile(bound(&command)))
        .await
        .unwrap();
    assert_eq!(state["state"], "settled");
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

impl Socket {
    async fn concrete(&self, f: &Fixture) -> Arc<Connection> {
        self.entered(async {
            let frame = self
                .owner
                .capture_review(&Frame::Prepare(f.query(Stage::Commit)))
                .unwrap();
            let mut out = None;
            frame
                .scope(Box::pin(async {
                    out = Some(REVIEW_REQUEST.with(|r| r.connection.clone()));
                }))
                .await;
            frame.retire();
            out.unwrap()
        })
        .await
    }
}
async fn wait_until(check: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !check() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[intent_test_macros::daemon_test]
async fn native_review_actual_post_completion_after_cancel_retains_receipt_and_capacity() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    f.server.control.pause_post.store(true, Ordering::SeqCst);
    s.entered(async {
        let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
        let call = frame.scope(Box::pin(async { let _ = f.services.native_review_execute(q.clone()).await; }));
        tokio::pin!(call);
        tokio::select! {()=&mut call=>panic!("response preceded fixture effect barrier"),()=f.server.control.post_entered.notified()=>{}}
        assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
        frame.retire();
        assert_eq!(c.review.workers.available_permits(), WORKERS-1);
        assert!(op.progress.lock().unwrap().settled.is_none());
        assert!(f.services.worktree_locks.try_with_lock(&f.git.path, || async {}).await.is_none());
        f.server.control.post_release.notify_one();
        call.await;
    }).await;
    wait_until(|| op.progress.lock().unwrap().settled.is_some()).await;
    wait_until(|| c.review.workers.available_permits() == WORKERS).await;
    let result = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(
        result["reviewExecution"]["outcome"]["status"], "created",
        "{result}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    assert_eq!(
        s.request(&f.services, Frame::Execute(q)).await.unwrap()["reviewExecution"],
        result["reviewExecution"]
    );
}
#[intent_test_macros::daemon_test]
async fn native_review_cancelled_lock_wait_never_observes_or_changes_git() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("owned.txt");
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    let before = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    f.services.worktree_locks.with_lock(&f.git.path, || async {
        s.entered(async {
            let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
            let call=frame.scope(Box::pin(async { let _=f.services.native_review_execute(q.clone()).await; }));
            tokio::pin!(call);
            tokio::select! {()=&mut call=>panic!("held lock operation finished early"),()=wait_until(|| c.review.workers.available_permits()==WORKERS-1)=>{}}
            frame.retire(); call.await;
            wait_until(|| c.review.workers.available_permits()==WORKERS).await;
            assert!(op.progress.lock().unwrap().engine.is_none());
        }).await;
    }).await;
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), before);
    let state = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(state["reviewExecution"]["gitReceipts"], json!([]));
    assert_eq!(state["reviewExecution"]["outcome"]["status"], "failed");
}
#[intent_test_macros::daemon_test]
async fn native_review_final_reply_error_keeps_original_effect_and_receiver_loss_refuses() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("effect.txt");
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    s.entered(async {
        let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
        frame
            .scope(Box::pin(async {
                let reply = f.services.native_review_execute(q.clone()).await.unwrap();
                assert_eq!(
                    reply["reviewExecution"]["gitReceipts"]
                        .as_array()
                        .unwrap()
                        .len(),
                    1
                );
                let mut effects = 0;
                assert!(frame
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        effects += 1;
                        Err(Error::Internal("fixture consumed".into()))
                    })
                    .await
                    .is_err());
                assert!(frame
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        effects += 1;
                        Ok(())
                    })
                    .await
                    .is_err());
                assert_eq!(effects, 1);
            }))
            .await;
        frame.retire();
    })
    .await;
    let result = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    assert_eq!(
        result["reviewExecution"]["gitReceipts"][0]["commitHash"],
        head.trim()
    );
    s.receiver.lock().unwrap().take();
    assert!(s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .is_err());
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), head);
}
#[intent_test_macros::daemon_test]
async fn native_review_private_feed_overflow_and_original_observation_budget() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    for _ in 0..OBSERVATIONS {
        assert_eq!(
            s.request(&f.services, Frame::Reconcile(bound(&q)))
                .await
                .unwrap()["state"],
            "prepared"
        );
    }
    assert!(s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .is_err());
    let c = s.concrete(&f).await;
    for _ in 0..=NOTICES {
        c.review.notice(&q.review.operation_id);
    }
    let mut receiver = s.receiver.lock().unwrap().take().unwrap();
    let event = receiver.next().await.unwrap();
    assert!(event.terminal && event.all_retired);
    assert!(event.operation_ids.is_empty());
    assert!(event.sequence.parse::<u64>().is_ok());
    assert!(receiver.next().await.is_none());
    assert!(s
        .request(&f.services, Frame::Prepare(f.query(Stage::CreatePr)))
        .await
        .is_err());
}

async fn member(f: &Fixture) -> (Socket, intent_core::Principal) {
    use intent_core::{now_iso, HostInvite, Principal, PrincipalId};
    let person = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: Some(8217),
        login: Some("review-member".into()),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    f.services.store.upsert_principal(&person).await.unwrap();
    let owner = f.services.store.get_primary_principal().await.unwrap();
    let invite = HostInvite::new(
        "native-review-member".into(),
        owner.id,
        person.identity_key().unwrap(),
        person.login.clone().unwrap(),
        "review-proof".into(),
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
                token_hash: "review-member-credential",
                authorization_generation: generation,
            },
        )
        .await
        .unwrap();
    let s = Socket::new(
        &f.services,
        Caller::Wire {
            principal_id: person.id.clone(),
            host_role: HostRole::Member,
        },
        Some(WireCredential::Principal {
            principal_id: person.id.clone(),
            token_hash: "review-member-credential".into(),
        }),
    )
    .await;
    (s, person)
}
#[intent_test_macros::daemon_test]
async fn native_review_actual_member_projection_and_durable_revocation() {
    let f = Fixture::new().await;
    let (s, person) = member(&f).await;
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    assert!(p["reviewPreparation"]["source"].get("connection").is_none());
    assert!(p["reviewPreparation"]["target"].get("connection").is_none());
    let q = command(&f, &p, Stage::CreatePr);
    let value = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(
        value["reviewExecution"]["outcome"]["status"], "created",
        "{value}"
    );
    assert!(value["reviewExecution"]["preparation"]["target"]
        .get("connection")
        .is_none());
    f.services
        .store
        .remove_host_member(&person.id)
        .await
        .unwrap();
    assert!(s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
}
#[intent_test_macros::daemon_test]
async fn native_review_actual_selection_change_and_pending_delete_refuse_original() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let snapshot = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    f.services
        .store
        .write_repository_selection(
            &snapshot,
            intent_store::RepositorySelectionChange::Automatic,
        )
        .await
        .result
        .unwrap();
    assert!(s.request(&f.services, Frame::Execute(q)).await.is_err());
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    use intent_store::RepositoryLifecycleObserver;
    let pending = f
        .services
        .repository_lifecycle_registry
        .begin_pending_delete(&[RepositoryLifecycleKey::Workspace(
            f.git.workspace.id.clone(),
        )])
        .unwrap();
    assert!(s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .is_err());
    pending.settle_confirmed();
    assert!(s.request(&f.services, Frame::Execute(q)).await.is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

fn source_input(op: &Operation) -> RepositorySourceInput {
    RepositorySourceInput {
        facts: op.facts.clone(),
        context: RepositoryContextInput {
            scope: op.facts.preparation.scope.clone(),
            revision: op.facts.preparation.context_revision.clone(),
            roots: vec![AdmittedRepositoryRoot {
                root: op.query.review.root.clone(),
                path: op.metadata.root.path().to_path_buf(),
                saved_selection: saved(&op.metadata.selection).unwrap(),
                explicit_target: explicit(&op.query).cloned(),
                targets: vec![target_context(
                    &op.facts.preparation.source.repository,
                    Some(&op.metadata.provider),
                )],
            }],
        },
        resolver: resolver(Some(&op.metadata.provider)).unwrap(),
        environment: GitConfigEnvironment::default(),
        before_lock: None,
    }
}

// Direct original source-entry proof: the callback marker is later than lock
// acquisition/root validation, and earlier than the engine's first observation.
#[intent_test_macros::daemon_test]
async fn native_review_observation_entry_precedes_early_source_error_and_false_drop_is_final() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    let before = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    s.entered(async {
        let (life,_registration)=c.new_lifetime().unwrap();
        let retired=life.retirement();
        let entered=AtomicBool::new(false); let action=AtomicBool::new(false);
        let mut input=source_input(&op);
        input.facts.source_ref="refs/heads/not-the-original".into();
        let result=with_repository_lifecycle_source_observed(&f.services,original(&c).unwrap(),"early-source-error".into(),vec![Stage::Commit],input,(life,&entered), |_| async {action.store(true,Ordering::Release); Ok(())}).await;
        assert!(result.is_err()); assert!(entered.load(Ordering::Acquire));
        assert!(!action.load(Ordering::Acquire)); assert!(retired.check_current().is_err());
        let (life,_registration)=c.new_lifetime().unwrap(); let retired=life.retirement();
        let entered=AtomicBool::new(false); let action=AtomicBool::new(false);
        let ready=Arc::new(Notify::new()); let mut input=source_input(&op); input.before_lock=Some(ready.clone());
        f.services.worktree_locks.with_lock(&f.git.path,|| async {
            let capacity=job(&c).unwrap();
            {
                let work=with_repository_lifecycle_source_observed(&f.services,original(&c).unwrap(),"drop-unentered".into(),vec![Stage::Commit],input,(life,&entered), |_| async {action.store(true,Ordering::Release); Ok(())});
                tokio::pin!(work);
                tokio::select! {r=&mut work=>panic!("held lock source ended: {r:?}"),()=ready.notified()=>{}}
                retired.end_scope(); assert!(!entered.load(Ordering::Acquire));
                // This future has exactly one poller. No task can start an
                // observation between this false check and its lexical drop.
            }
            assert!(retired.check_current().is_err());
            drop(capacity);
            assert_eq!(c.review.workers.available_permits(),WORKERS);
        }).await;
        tokio::task::yield_now().await;
        assert!(!entered.load(Ordering::Acquire)); assert!(!action.load(Ordering::Acquire));
    }).await;
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), before);
}

#[intent_test_macros::daemon_test]
async fn native_review_owned_primitive_sink_survives_post_observation_worker_panic() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("primitive.txt");
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    s.entered(async {
        // Claim the actual original command before running this deliberate
        // callback-panic schedule in the original source/owned worker.
        let frame = s.owner.capture_review(&Frame::Execute(q.clone())).unwrap();
        let capacity = job(&c).unwrap();
        op.progress.lock().unwrap().started = true;
        let owned = op.clone();
        let connection = c.clone();
        let task = tokio::spawn(with_caller(
            c.caller.caller().clone(),
            with_wire_credential(c.caller.wire_credential().cloned(), async move {
                let _capacity = capacity;
                let _finish = Completion(owned.clone());
                let (life, _registration) = connection.new_lifetime().unwrap();
                let flag = AtomicBool::new(false);
                let result = with_repository_lifecycle_source_observed(
                    &connection.services,
                    original(&connection).unwrap(),
                    owned.id.clone(),
                    vec![Stage::Commit],
                    source_input(&owned),
                    (life, &flag),
                    |admission| async move {
                        owned.progress.lock().unwrap().engine = Some(admission.clone());
                        let checked =
                            engine::revalidate_repository_stage(&admission, Stage::Commit).await?;
                        let stamp = engine::begin_native_repository_stage(checked, |claim| {
                            owned.metadata.with_metadata(claim)
                        })?;
                        let sink = owned.clone();
                        let joined = tokio::task::spawn_blocking(move || {
                            let _stamp = stamp;
                            intent_git::commit::commit_observed(
                                sink.metadata.root.path(),
                                "primitive observed",
                                |sha| {
                                    sink.primitive(GitReceipt::Commit {
                                        commit_hash: sha.into(),
                                    });
                                    panic!(
                                        "deliberate callback failure after retained original SHA"
                                    );
                                },
                            )
                        })
                        .await;
                        assert!(joined.unwrap_err().is_panic());
                        Err::<(), _>(AdmissionError::Unavailable)
                    },
                )
                .await;
                assert!(flag.load(Ordering::Acquire));
                assert!(result.is_err());
            }),
        ));
        task.await.unwrap();
        frame.retire();
    })
    .await;
    assert_eq!(c.review.workers.available_permits(), WORKERS);
    let head = f.git.git(&f.git.path, &["rev-parse", "HEAD"]);
    let state = s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .unwrap();
    assert_eq!(
        state["reviewExecution"]["gitReceipts"],
        json!([{"stage":"commit","commitHash":head.trim()}])
    );
    assert_eq!(state["reviewExecution"]["outcome"]["status"], "uncertain");
    let repeated = s.request(&f.services, Frame::Execute(q)).await.unwrap();
    assert_eq!(repeated["reviewExecution"], state["reviewExecution"]);
    assert_eq!(f.git.git(&f.git.path, &["rev-parse", "HEAD"]), head);
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_concurrent_registered_roots_coordinate_complete_branch_pair() {
    let f = Arc::new(Fixture::new().await);
    let s = Arc::new(f.socket().await);
    let alternate = f.git.dir.path().join("second-repo");
    f.git.git(
        f.git.dir.path(),
        &[
            "clone",
            f.git.path.to_str().unwrap(),
            alternate.to_str().unwrap(),
        ],
    );
    f.git.git(&alternate, &["remote", "remove", "origin"]);
    let id = intent_core::WorkspaceGitRootId::new();
    let row=serde_json::from_value(json!({"id":id,"workspaceId":f.git.workspace.id,"path":alternate,"source":"auto","registeredByAgentIds":[],"createdAt":intent_core::now_iso(),"updatedAt":intent_core::now_iso()})).unwrap();
    f.services
        .store
        .upsert_workspace_git_root(&row)
        .await
        .unwrap();
    let first = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let first = command(&f, &first, Stage::CreatePr);
    let mut query = f.query(Stage::CreatePr);
    query.review.root.kind = RepositoryRootKind::Registered { git_root_id: id };
    let second = s.prepare(&f, query).await;
    let second = command(&f, &second, Stage::CreatePr);
    let c = s.concrete(&f).await;
    f.server.control.pause_post.store(true, Ordering::SeqCst);
    let first_task = {
        let f = f.clone();
        let s = s.clone();
        tokio::spawn(async move { s.request(&f.services, Frame::Execute(first)).await })
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        f.server.control.post_entered.notified(),
    )
    .await
    .unwrap();
    let second_task = {
        let f = f.clone();
        let s = s.clone();
        tokio::spawn(async move { s.request(&f.services, Frame::Execute(second)).await })
    };
    wait_until(|| {
        c.services
            .repository_review_capacity
            .creates
            .lock()
            .unwrap()
            .values()
            .any(|lock| lock.strong_count() >= 2)
    })
    .await;
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    f.server.control.post_release.notify_one();
    let first = first_task.await.unwrap().unwrap();
    let second = second_task.await.unwrap().unwrap();
    assert_eq!(
        first["reviewExecution"]["outcome"]["status"], "created",
        "{first}"
    );
    assert_eq!(
        second["reviewExecution"]["outcome"]["status"], "reused",
        "{second}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    assert_eq!(first["reviewExecution"]["gitReceipts"], json!([]));
    assert_eq!(second["reviewExecution"]["gitReceipts"], json!([]));
}

#[intent_test_macros::daemon_test]
async fn native_review_ambiguous_and_malformed_provider_results_never_guess_or_retry() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let sha = f.server.control.sha.lock().unwrap().clone();
    let review = super::credential_tests::review(&sha);
    let mut other = review.clone();
    other["iid"] = json!(8);
    *f.server.control.reviews.lock().unwrap() = vec![review, other];
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let refused = s
        .request(
            &f.services,
            Frame::Execute(command(&f, &p, Stage::CreatePr)),
        )
        .await
        .unwrap();
    assert_eq!(
        refused["reviewExecution"]["outcome"]["status"], "failed",
        "{refused}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
    f.server.control.reviews.lock().unwrap().clear();
    f.server
        .control
        .malformed_post
        .store(true, Ordering::SeqCst);
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    let uncertain = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(
        uncertain["reviewExecution"]["outcome"]["status"], "uncertain",
        "{uncertain}"
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
    assert_eq!(
        s.request(&f.services, Frame::Reconcile(bound(&q)))
            .await
            .unwrap()["reviewExecution"],
        uncertain["reviewExecution"]
    );
    assert_eq!(
        s.request(&f.services, Frame::Execute(q)).await.unwrap()["reviewExecution"],
        uncertain["reviewExecution"]
    );
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
}

#[intent_test_macros::daemon_test]
async fn native_review_prepare_subscription_outlives_helper_and_denies_final_root_change() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    s.entered(async {
        let q = f.query(Stage::Commit);
        let frame = s.owner.capture_review(&Frame::Prepare(q.clone())).unwrap();
        frame
            .scope(Box::pin(async {
                let p = f.services.native_review_prepare(q).await.unwrap();
                let original = REVIEW_REQUEST.with(Clone::clone);
                original.check().unwrap(); // acquisition returned with root subscription retained
                let op = original.operation().unwrap();
                assert!(!op.published.load(Ordering::Acquire));
                f.git.git(
                    &f.git.path,
                    &["checkout", "-b", "changed-before-disclosure"],
                );
                let mut sent = 0;
                assert!(frame
                    .deliver(RepositoryReadReplyKind::Result, &mut || {
                        sent += 1;
                        Ok(())
                    })
                    .await
                    .is_err());
                assert_eq!(sent, 0);
                assert!(!op.published.load(Ordering::Acquire));
                assert_eq!(p["reviewOperation"]["operationId"], op.id);
            }))
            .await;
        frame.retire();
    })
    .await;
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_virtual_bounds_and_checked_cursor_never_revive_original() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    tokio::time::pause();
    tokio::time::advance(LEASE_TTL + Duration::from_secs(1)).await;
    assert!(op.write_current().is_err());
    tokio::time::resume();
    assert!(s.request(&f.services, Frame::Execute(q)).await.is_err());
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    let frame = s
        .entered(async { s.owner.capture_review(&Frame::Execute(q.clone())).unwrap() })
        .await;
    tokio::time::pause();
    tokio::time::advance(FRAME_TTL + Duration::from_secs(1)).await;
    tokio::time::resume();
    frame.retire();
    assert_eq!(
        s.request(&f.services, Frame::Reconcile(bound(&q)))
            .await
            .unwrap()["reviewExecution"]["outcome"]["status"],
        "failed"
    );
    let p = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let q = command(&f, &p, Stage::CreatePr);
    s.request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    tokio::time::pause();
    tokio::time::advance(RECEIPT_TTL + Duration::from_secs(1)).await;
    assert!(op.disclosure_current().is_err());
    tokio::time::resume();
    assert!(s
        .request(&f.services, Frame::Reconcile(bound(&q)))
        .await
        .is_err());
    c.review.feed.lock().unwrap().sequence = u64::MAX;
    c.review.notice("original-only");
    let mut feed = s.receiver.lock().unwrap().take().unwrap();
    let notice = feed.next().await.unwrap();
    assert_eq!(notice.sequence, u64::MAX.to_string());
    assert!(notice.terminal && notice.all_retired);
    assert!(feed.next().await.is_none());
    assert!(s
        .request(&f.services, Frame::Prepare(f.query(Stage::CreatePr)))
        .await
        .is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 1);
}

#[intent_test_macros::daemon_test]
async fn native_review_cancelled_acquisition_keeps_worker_until_original_http_finishes() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    let c = s.concrete(&f).await;
    *f.server.control.pause.lock().unwrap() = Some("/projects/".into());
    s.entered(async {
        let q=f.query(Stage::CreatePr);let frame=s.owner.capture_review(&Frame::Prepare(q.clone())).unwrap();
        let work=frame.scope(Box::pin(async {assert!(f.services.native_review_prepare(q).await.is_err());}));
        tokio::pin!(work);
        tokio::select! {()=&mut work=>panic!("acquisition completed before fixture response"),()=f.server.control.entered.notified()=>{}}
        frame.retire();work.await;
        assert_eq!(c.review.workers.available_permits(),WORKERS-1);
        assert!(c.review.feed.lock().unwrap().records.is_empty());
        f.server.control.release.notify_one();
    }).await;
    wait_until(|| c.review.workers.available_permits() == WORKERS).await;
    assert_eq!(c.review.records.available_permits(), RECORDS);
    assert_eq!(
        f.services
            .repository_review_capacity
            .records
            .available_permits(),
        GLOBAL_RECORDS
    );
    assert!(c.review.feed.lock().unwrap().records.is_empty());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_saved_automatic_reset_historical_and_explicit_choice_preserve_store_history()
{
    use intent_store::RepositorySelectionChange as Change;
    let mut f = Fixture::new().await;
    let s = f.socket().await;
    let mut saved_query = f.query(Stage::CreatePr);
    saved_query.review.choice = Choice::Saved;
    assert!(s
        .request(&f.services, Frame::Prepare(saved_query.clone()))
        .await
        .is_err());
    let before = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    assert_eq!(
        before.selection(),
        Some(&RepositoryStoredSelection::NeverSaved)
    );
    let _ = s.prepare(&f, f.query(Stage::CreatePr)).await;
    assert_eq!(
        f.services
            .store
            .repository_selection_snapshot(&f.git.root())
            .await
            .unwrap()
            .selection(),
        before.selection()
    );
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "add",
            "forge",
            "https://gitlab.test/forge/group/project.git",
        ],
    );
    for change in [
        Change::Automatic,
        Change::Reset,
        Change::ExplicitRemote {
            remote_name: "forge".into(),
        },
    ] {
        let snapshot = f
            .services
            .store
            .repository_selection_snapshot(&f.git.root())
            .await
            .unwrap();
        f.services
            .store
            .write_repository_selection(&snapshot, change)
            .await
            .result
            .unwrap();
        let snapshot = f
            .services
            .store
            .repository_selection_snapshot(&f.git.root())
            .await
            .unwrap();
        let p = s.prepare(&f, saved_query.clone()).await;
        assert_eq!(
            p["reviewPreparation"]["target"]["repository"]["projectPath"],
            "group/project"
        );
        let after = f
            .services
            .store
            .repository_selection_snapshot(&f.git.root())
            .await
            .unwrap();
        assert_eq!(snapshot.selection(), after.selection());
        assert_eq!(snapshot.selection_revision(), after.selection_revision());
    }
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "add",
            "other",
            "https://gitlab.test/forge/group/other.git",
        ],
    );
    let snapshot = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    f.services
        .store
        .write_repository_selection(&snapshot, Change::Automatic)
        .await
        .result
        .unwrap();
    assert!(s
        .request(&f.services, Frame::Prepare(saved_query.clone()))
        .await
        .is_err());
    let before = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    let _ = s.prepare(&f, f.query(Stage::CreatePr)).await;
    assert_eq!(
        f.services
            .store
            .repository_selection_snapshot(&f.git.root())
            .await
            .unwrap()
            .selection(),
        before.selection()
    );
    // A new incarnation with legacy intent remains unresolved, not migrated by preparation.
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
    let fresh = f.socket().await;
    assert!(fresh
        .request(&f.services, Frame::Prepare(saved_query))
        .await
        .is_err());
    let before = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    assert!(matches!(
        before.selection(),
        Some(RepositoryStoredSelection::Saved(
            intent_core::SavedReviewSelection::UnresolvedHistorical { .. }
        ))
    ));
    let _ = fresh.prepare(&f, f.query(Stage::CreatePr)).await;
    let after = f
        .services
        .store
        .repository_selection_snapshot(&f.git.root())
        .await
        .unwrap();
    assert_eq!(before.selection(), after.selection());
    assert_eq!(before.selection_revision(), after.selection_revision());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_record_and_worker_limits_release_only_original_owners() {
    let f = Fixture::new().await;
    let mut sockets = Vec::new();
    // This schedule isolates record capacity from the separately tested periodic
    // metadata observation. Strict consuming admission may legitimately refuse
    // on contention; filling the quota must not rely on retrying that refusal.
    // Keep the executor runnable so paused time cannot auto-advance during real
    // Store/provider I/O. All preparations still use the original full path.
    tokio::time::pause();
    let frozen = Instant::now();
    let extra = tokio::select! {
        socket = async {
            for _ in 0..GLOBAL_RECORDS / RECORDS {
                let s = f.socket().await;
                for _ in 0..RECORDS {
                    let _ = s.prepare(&f, f.query(Stage::CreatePr)).await;
                }
                assert!(s
                    .request(&f.services, Frame::Prepare(f.query(Stage::CreatePr)))
                    .await
                    .is_err());
                sockets.push(s);
            }
            assert_eq!(
                f.services
                    .repository_review_capacity
                    .records
                    .available_permits(),
                0
            );
            let extra = f.socket().await;
            assert!(extra
                .request(&f.services, Frame::Prepare(f.query(Stage::CreatePr)))
                .await
                .is_err());
            extra
        } => socket,
        () = async { loop { tokio::task::yield_now().await; } } => unreachable!(),
    };
    assert_eq!(Instant::now(), frozen);
    tokio::time::resume();
    for s in &sockets {
        s.owner.retire();
    }
    wait_until(|| {
        f.services
            .repository_review_capacity
            .records
            .available_permits()
            == GLOBAL_RECORDS
    })
    .await;
    let mut held = Vec::new();
    let mut connections = Vec::new();
    for _ in 0..GLOBAL_WORKERS / WORKERS {
        let s = f.socket().await;
        let c = s.concrete(&f).await;
        for _ in 0..WORKERS {
            held.push(job(&c).unwrap());
        }
        assert!(job(&c).is_err());
        connections.push((s, c));
    }
    assert_eq!(
        f.services
            .repository_review_capacity
            .workers
            .available_permits(),
        0
    );
    assert!(job(extra.concrete(&f).await.as_ref()).is_err());
    drop(held);
    assert_eq!(
        f.services
            .repository_review_capacity
            .workers
            .available_permits(),
        GLOBAL_WORKERS
    );
    assert!(connections
        .iter()
        .all(|(_, c)| c.review.workers.available_permits() == WORKERS));
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}

#[intent_test_macros::daemon_test]
async fn native_review_background_lock_contention_preserves_original_until_real_change() {
    let f = Fixture::new().await;
    let s = f.socket().await;
    f.stage("monitor.txt");
    let p = s.prepare(&f, f.query(Stage::Commit)).await;
    let q = command(&f, &p, Stage::Commit);
    let c = s.concrete(&f).await;
    let op = c.review.feed.lock().unwrap().records[&q.review.operation_id].clone();
    let baseline = Arc::strong_count(&op.metadata);
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let facts = op.metadata.provider.clone();
    let holder =
        std::thread::spawn(move || facts.hold_native_review_metadata_for_test(entered, released));
    waiting.await.unwrap();
    // Wait for the actual monitor to retain its original metadata for the blocking
    // observation. The real config lock is held; no synthetic admission is used.
    wait_until(|| Arc::strong_count(&op.metadata) > baseline).await;
    let mut consumed = 0;
    assert!(op
        .metadata
        .with_metadata(|| {
            consumed += 1;
            Ok(())
        })
        .is_err());
    assert_eq!(consumed, 0);
    assert!(op.write_current().is_ok());
    release.send(()).unwrap();
    holder.join().unwrap();
    wait_until(|| Arc::strong_count(&op.metadata) == baseline).await;
    assert!(op.write_current().is_ok());
    let result = s
        .request(&f.services, Frame::Execute(q.clone()))
        .await
        .unwrap();
    assert_eq!(
        result["reviewExecution"]["gitReceipts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let before = op.progress.lock().unwrap().effects.clone();
    let prepared = s.prepare(&f, f.query(Stage::CreatePr)).await;
    let create = command(&f, &prepared, Stage::CreatePr);
    let next = c.review.feed.lock().unwrap().records[&create.review.operation_id].clone();
    f.services
        .gitlab_connect_pat(f.server.host.clone(), "stored-pat".into())
        .await
        .unwrap();
    wait_until(|| next.write_current().is_err()).await;
    assert_eq!(op.progress.lock().unwrap().effects, before);
    assert!(s
        .request(&f.services, Frame::Execute(create))
        .await
        .is_err());
    assert_eq!(f.server.control.posts.load(Ordering::SeqCst), 0);
}
