//! Real Store writers with an invalidation-only observer/leaf fixture. These
//! tests do not install R `ReadRequest` subscriptions or a transport reply fence.

use std::sync::atomic::{AtomicBool, Ordering};

use intent_core::{
    now_iso, HostInvite, Principal, PrincipalId, Workspace, WorkspaceInvite, WorkspaceRole,
};

use super::*;
use crate::{CollaboratorAddOutcome, HostInviteJoinOutcome, HostJoinCredential, InviteJoinOutcome};

type Leaf = Arc<AtomicBool>;

#[derive(Default)]
struct State {
    pending: HashMap<RepositoryLifecycleKey, usize>,
    leaves: Vec<(Vec<RepositoryLifecycleKey>, Leaf)>,
    starts: usize,
}

#[derive(Default)]
struct Probe {
    state: Arc<Mutex<State>>,
    reject: AtomicBool,
}

impl Probe {
    fn capture(&self, mut keys: Vec<RepositoryLifecycleKey>) -> Option<Leaf> {
        keys.push(RepositoryLifecycleKey::Database);
        let mut state = self.state.lock().unwrap();
        if keys.iter().any(|key| state.pending.contains_key(key)) {
            return None;
        }
        let leaf = Arc::new(AtomicBool::new(true));
        state.leaves.push((keys, leaf.clone()));
        Some(leaf)
    }

    fn wire(&self) -> Option<Leaf> {
        self.capture(vec![RepositoryLifecycleKey::WireAuthority])
    }

    fn starts(&self) -> usize {
        self.state.lock().unwrap().starts
    }
}

struct Ticket {
    state: Arc<Mutex<State>>,
    keys: Vec<RepositoryLifecycleKey>,
}

impl RepositoryLifecycleMutationTicket for Ticket {
    fn settle_confirmed(self: Box<Self>) {
        let mut state = self.state.lock().unwrap();
        for key in &self.keys {
            let count = state.pending.get_mut(key).unwrap();
            *count -= 1;
            if *count == 0 {
                state.pending.remove(key);
            }
        }
    }
}

impl RepositoryLifecycleObserver for Probe {
    fn begin_mutation(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        if self.reject.load(Ordering::SeqCst) {
            return Err(lifecycle_error("fixture refused before SQL"));
        }
        let mut state = self.state.lock().unwrap();
        state.starts += 1;
        for key in keys {
            *state.pending.entry(key.clone()).or_default() += 1;
        }
        for (captured, leaf) in &state.leaves {
            if keys.iter().any(|key| captured.contains(key)) {
                leaf.store(false, Ordering::SeqCst);
            }
        }
        Ok(Box::new(Ticket {
            state: self.state.clone(),
            keys: keys.to_vec(),
        }))
    }
}

fn person(id: i64) -> Principal {
    Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: Some(id),
        login: Some(format!("person-{id}")),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    }
}

struct Fixture {
    store: Store,
    workspace: Workspace,
    person: Principal,
    probe: Arc<Probe>,
    dir: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("wire.db")).await.unwrap();
        let workspace: Workspace = serde_json::from_value(serde_json::json!({
            "id":"wire-workspace", "title":"Wire", "branch":"main", "status":"Active",
            "activity":"idle", "attention":"none", "createdAt":"same-time", "updatedAt":"same-time",
            "tags":[], "skipWorktree":false, "isRemote":false, "archived":false
        }))
        .unwrap();
        store.insert_workspace(&workspace).await.unwrap();
        let person = person(42);
        store.upsert_principal(&person).await.unwrap();
        store
            .insert_principal_credential(&person.id, "original-hash")
            .await
            .unwrap();
        let probe = Arc::new(Probe::default());
        store
            .install_repository_lifecycle_observer(probe.clone())
            .await
            .unwrap();
        Self {
            store,
            workspace,
            person,
            probe,
            dir,
        }
    }

    fn wire(&self) -> Leaf {
        self.probe.wire().unwrap()
    }

    fn agent(&self) -> Leaf {
        self.probe
            .capture(vec![RepositoryLifecycleKey::Agent(AgentId(
                "unrelated-agent".into(),
            ))])
            .unwrap()
    }

    fn retired(&self, leaf: &Leaf) {
        assert!(
            !leaf.load(Ordering::SeqCst),
            "the original Wire leaf must retire"
        );
        assert!(
            self.probe.wire().is_some(),
            "only a known completed owner unblocks fresh captures"
        );
    }

    async fn add(&self) {
        self.store
            .add_workspace_member(
                &self.workspace.id,
                &self.person.id,
                WorkspaceRole::Collaborator,
            )
            .await
            .unwrap();
    }

    async fn invite(&self) {
        let owner = self.store.get_primary_principal().await.unwrap();
        self.store
            .insert_workspace_invite(&WorkspaceInvite {
                id: "workspace-invite".into(),
                workspace_id: self.workspace.id.clone(),
                secret_hash: "workspace-invite-hash".into(),
                secret: None,
                created_by_principal_id: owner.id,
                pin_identity: None,
                pin_github_user_id: None,
                pin_login: None,
                created_at: now_iso(),
                expires_at: "2999-01-01T00:00:00Z".into(),
                redeemed_at: None,
                redeemed_by_principal_id: None,
                revoked_at: None,
                redemption_count: 0,
            })
            .await
            .unwrap();
    }

    async fn host_invite(&self, id: &str) {
        let owner = self.store.get_primary_principal().await.unwrap();
        self.store
            .insert_host_invite(
                &HostInvite::new(
                    id.into(),
                    owner.id,
                    self.person.identity_key().unwrap(),
                    "person".into(),
                    format!("hash-{id}"),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn principal_insert_and_identity_aba_retire_but_profile_does_not() {
    let f = Fixture::new().await;
    let agent = f.agent();
    let old = f.wire();
    f.store.upsert_principal(&person(99)).await.unwrap();
    f.retired(&old);
    let original = f.wire();
    let mut changed = f.person.clone();
    changed.github_user_id = Some(43);
    f.store.upsert_principal(&changed).await.unwrap();
    f.store.upsert_principal(&f.person).await.unwrap();
    f.retired(&original);
    let profile = f.wire();
    changed = f.person.clone();
    changed.login = Some("renamed".into());
    changed.display_name = Some("New profile".into());
    changed.avatar_url = Some("https://example.invalid/avatar".into());
    changed.is_primary = true; // ON CONFLICT deliberately never changes this flag.
    changed.updated_at = now_iso();
    let starts = f.probe.starts();
    f.store.upsert_principal(&changed).await.unwrap();
    assert_eq!(f.probe.starts(), starts);
    assert!(profile.load(Ordering::SeqCst));
    assert!(
        !f.store
            .get_principal(&f.person.id)
            .await
            .unwrap()
            .is_primary
    );
    assert!(agent.load(Ordering::SeqCst));
}

#[tokio::test]
async fn primary_identity_edit_retires_original_read() {
    let f = Fixture::new().await;
    let mut primary = f.store.get_primary_principal().await.unwrap();
    let old = f.wire();
    primary.github_user_id = Some(123);
    f.store.upsert_principal(&primary).await.unwrap();
    f.retired(&old);
}

#[tokio::test]
async fn direct_grant_add_remove_readd_preserves_unrelated_agent() {
    let f = Fixture::new().await;
    let agent = f.agent();
    let old = f.wire();
    f.add().await;
    f.retired(&old);
    let old = f.wire();
    assert!(f
        .store
        .remove_workspace_member(&f.workspace.id, &f.person.id)
        .await
        .unwrap());
    f.add().await;
    f.retired(&old);
    let no_op = f.wire();
    assert!(!f
        .store
        .add_workspace_member(&f.workspace.id, &f.person.id, WorkspaceRole::Owner)
        .await
        .unwrap());
    assert!(!f
        .store
        .remove_workspace_member(&f.workspace.id, &PrincipalId::new())
        .await
        .unwrap());
    assert!(no_op.load(Ordering::SeqCst));
    assert!(agent.load(Ordering::SeqCst));
}

#[tokio::test]
async fn role_aba_and_owner_sync_retire_original() {
    let f = Fixture::new().await;
    let owner = f.store.get_primary_principal().await.unwrap();
    let old = f.wire();
    f.store
        .set_workspace_member_role(&f.workspace.id, &owner.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    f.store
        .set_workspace_member_role(&f.workspace.id, &owner.id, WorkspaceRole::Owner)
        .await
        .unwrap();
    f.retired(&old);
    let no_op = f.wire();
    f.store
        .set_workspace_member_role(&f.workspace.id, &owner.id, WorkspaceRole::Owner)
        .await
        .unwrap();
    assert!(no_op.load(Ordering::SeqCst));
}

#[tokio::test]
async fn owner_sync_repair_is_not_a_noop_in_add_set_or_remove() {
    for action in 0..3 {
        let f = Fixture::new().await;
        let owner = f.store.get_primary_principal().await.unwrap();
        // Deliberate malformed preimage fixture, not claimed raw-SQL coverage.
        sqlx::query("UPDATE workspace SET owner_principal_id=NULL WHERE id=?")
            .bind(&f.workspace.id.0)
            .execute(f.store.write_pool())
            .await
            .unwrap();
        let old = f.wire();
        match action {
            0 => {
                assert!(!f
                    .store
                    .add_workspace_member(&f.workspace.id, &owner.id, WorkspaceRole::Owner)
                    .await
                    .unwrap());
            }
            1 => {
                f.store
                    .set_workspace_member_role(&f.workspace.id, &owner.id, WorkspaceRole::Owner)
                    .await
                    .unwrap();
            }
            _ => {
                assert!(!f
                    .store
                    .remove_workspace_member(&f.workspace.id, &PrincipalId::new())
                    .await
                    .unwrap());
            }
        }
        f.retired(&old);
        assert_eq!(
            f.store
                .get_workspace_owner_principal_id(&f.workspace.id)
                .await
                .unwrap(),
            Some(owner.id)
        );
    }
}

#[tokio::test]
async fn capped_add_and_guest_removal_retire_only_on_effect() {
    let f = Fixture::new().await;
    let denied = f.wire();
    assert_eq!(
        f.store
            .add_workspace_collaborator_within_cap(&f.workspace.id, &f.person.id, 0)
            .await
            .unwrap(),
        CollaboratorAddOutcome::WorkspaceFull
    );
    assert!(denied.load(Ordering::SeqCst));
    assert_eq!(
        f.store
            .add_workspace_collaborator_within_cap(&f.workspace.id, &f.person.id, 4)
            .await
            .unwrap(),
        CollaboratorAddOutcome::Added
    );
    f.retired(&denied);
    let repeated = f.wire();
    assert_eq!(
        f.store
            .add_workspace_collaborator_within_cap(&f.workspace.id, &f.person.id, 4)
            .await
            .unwrap(),
        CollaboratorAddOutcome::AlreadyMember
    );
    assert!(repeated.load(Ordering::SeqCst));
    assert!(f
        .store
        .remove_workspace_guest(&f.workspace.id, &f.person.id)
        .await
        .unwrap());
    f.retired(&repeated);
    let absent = f.wire();
    assert!(!f
        .store
        .remove_workspace_guest(&f.workspace.id, &f.person.id)
        .await
        .unwrap());
    assert!(absent.load(Ordering::SeqCst));
}

#[tokio::test]
async fn credential_insert_and_revoke_retire_but_touches_and_repeats_do_not() {
    let f = Fixture::new().await;
    let agent = f.agent();
    let old = f.wire();
    f.store
        .insert_principal_credential(&f.person.id, "new-hash")
        .await
        .unwrap();
    f.retired(&old);
    let touched = f.wire();
    assert!(f
        .store
        .touch_principal_credential("new-hash")
        .await
        .unwrap());
    assert_eq!(
        f.store
            .resolve_active_principal_credential("new-hash")
            .await
            .unwrap(),
        Some(f.person.id.clone())
    );
    assert!(touched.load(Ordering::SeqCst));
    assert!(f
        .store
        .revoke_principal_credential("new-hash")
        .await
        .unwrap());
    f.retired(&touched);
    let no_op = f.wire();
    assert!(!f
        .store
        .revoke_principal_credential("new-hash")
        .await
        .unwrap());
    assert!(!f
        .store
        .revoke_principal_credential("missing")
        .await
        .unwrap());
    assert!(no_op.load(Ordering::SeqCst));
    assert!(agent.load(Ordering::SeqCst));
}

#[tokio::test]
async fn revoke_all_preserves_count_and_zero_row_noop() {
    let f = Fixture::new().await;
    f.store
        .insert_principal_credential(&f.person.id, "second")
        .await
        .unwrap();
    let old = f.wire();
    assert_eq!(
        f.store
            .revoke_all_principal_credentials(&f.person.id)
            .await
            .unwrap(),
        2
    );
    f.retired(&old);
    let no_op = f.wire();
    assert_eq!(
        f.store
            .revoke_all_principal_credentials(&f.person.id)
            .await
            .unwrap(),
        0
    );
    assert!(no_op.load(Ordering::SeqCst));
    assert!(f
        .store
        .lookup_principal_credential("original-hash")
        .await
        .unwrap()
        .unwrap()
        .revoked_at
        .is_some());
}

#[tokio::test]
async fn workspace_join_and_new_proof_retire_but_existing_rejoin_is_profile_only() {
    let f = Fixture::new().await;
    let invitation = f.wire();
    f.invite().await;
    assert!(invitation.load(Ordering::SeqCst));
    let result = f
        .store
        .join_workspace_by_invite(
            "workspace-invite",
            &f.workspace.id,
            &f.person,
            HostJoinCredential::Existing {
                token_hash: "original-hash",
            },
            8,
        )
        .await
        .unwrap();
    assert!(matches!(result, InviteJoinOutcome::Joined(_)));
    f.retired(&invitation);
    let rejoin = f.wire();
    let mut renamed = f.person.clone();
    renamed.login = Some("renamed".into());
    assert!(matches!(
        f.store
            .join_workspace_by_invite(
                "workspace-invite",
                &f.workspace.id,
                &renamed,
                HostJoinCredential::Existing {
                    token_hash: "original-hash"
                },
                8
            )
            .await
            .unwrap(),
        InviteJoinOutcome::Rejoined(_)
    ));
    assert!(rejoin.load(Ordering::SeqCst));
    let generation = f
        .store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    f.store
        .join_workspace_by_invite(
            "workspace-invite",
            &f.workspace.id,
            &renamed,
            HostJoinCredential::Proof {
                token_hash: "proof",
                authorization_generation: generation,
            },
            8,
        )
        .await
        .unwrap();
    f.retired(&rejoin);
}

#[tokio::test]
async fn host_join_remove_and_same_identity_rejoin_retire_original() {
    let f = Fixture::new().await;
    f.host_invite("host").await;
    let agent = f.agent();
    let old = f.wire();
    assert!(matches!(
        f.store
            .join_host_by_invite(
                "host",
                &f.person,
                HostJoinCredential::Existing {
                    token_hash: "original-hash"
                }
            )
            .await
            .unwrap(),
        HostInviteJoinOutcome::Joined {
            membership_added: true,
            ..
        }
    ));
    f.retired(&old);
    f.host_invite("profile").await;
    let profile = f.wire();
    assert!(matches!(
        f.store
            .join_host_by_invite(
                "profile",
                &f.person,
                HostJoinCredential::Existing {
                    token_hash: "original-hash"
                }
            )
            .await
            .unwrap(),
        HostInviteJoinOutcome::Joined {
            membership_added: false,
            ..
        }
    ));
    assert!(profile.load(Ordering::SeqCst));
    assert!(
        f.store
            .remove_host_member(&f.person.id)
            .await
            .unwrap()
            .removed
    );
    f.retired(&profile);
    f.host_invite("back").await;
    let generation = f
        .store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    let removed = f.wire();
    f.store
        .join_host_by_invite(
            "back",
            &f.person,
            HostJoinCredential::Proof {
                token_hash: "back-hash",
                authorization_generation: generation,
            },
        )
        .await
        .unwrap();
    f.retired(&removed);
    assert!(agent.load(Ordering::SeqCst));
}

#[tokio::test]
async fn host_removal_noop_and_guest_revocation_generation_are_distinct() {
    let f = Fixture::new().await;
    let old = f.wire();
    assert!(
        !f.store
            .remove_host_member(&f.person.id)
            .await
            .unwrap()
            .removed
    );
    assert!(old.load(Ordering::SeqCst));
    f.store
        .revoke_all_principal_credentials(&f.person.id)
        .await
        .unwrap();
    let old = f.wire();
    let before = f
        .store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    let removed = f.store.revoke_principal_access(&f.person.id).await.unwrap();
    assert!(!removed.removed);
    assert_eq!(removed.credentials, 0);
    assert!(removed.workspaces.is_empty());
    assert!(
        f.store
            .host_membership_state()
            .await
            .unwrap()
            .authorization_generation
            > before
    );
    f.retired(&old);
}

#[tokio::test]
async fn repeated_archive_sweep_retires_wire_without_repeating_workspace_retirement() {
    let f = Fixture::new().await;
    f.store
        .archive_workspace_detaching_guests(&f.workspace.id, &now_iso())
        .await
        .unwrap();
    f.add().await; // Direct Store API permits a row in an already archived workspace.
    let old = f.wire();
    let scoped = f
        .probe
        .capture(vec![RepositoryLifecycleKey::Workspace(
            f.workspace.id.clone(),
        )])
        .unwrap();
    let removed = f
        .store
        .archive_workspace_detaching_guests(&f.workspace.id, &now_iso())
        .await
        .unwrap();
    assert_eq!(removed.removed_collaborators, vec![f.person.id.clone()]);
    f.retired(&old);
    assert!(scoped.load(Ordering::SeqCst));
    let no_op = f.wire();
    f.store
        .archive_workspace_detaching_guests(&f.workspace.id, &now_iso())
        .await
        .unwrap();
    assert!(no_op.load(Ordering::SeqCst));
}

#[tokio::test]
async fn import_uses_one_database_barrier_including_owner_trigger() {
    let f = Fixture::new().await;
    let old = f.wire();
    let agent = f.agent();
    let starts = f.probe.starts();
    assert_eq!(f.store.transfer_import_rows(&[]).await.unwrap(), 0);
    assert!(old.load(Ordering::SeqCst));
    let rows = vec![(
        "workspace".into(),
        vec![serde_json::json!({
            "id":"imported", "title":"Imported", "branch":"main", "created_at":"now", "updated_at":"now"
        })],
    )];
    assert_eq!(f.store.transfer_import_rows(&rows).await.unwrap(), 1);
    f.retired(&old);
    assert!(!agent.load(Ordering::SeqCst));
    assert_eq!(f.probe.starts(), starts + 1);
    let primary = f.store.get_primary_principal().await.unwrap();
    assert_eq!(
        f.store
            .get_workspace_owner_principal_id(&WorkspaceId("imported".into()))
            .await
            .unwrap(),
        Some(primary.id)
    );
}

#[tokio::test]
async fn observer_refusal_precedes_credential_write() {
    let f = Fixture::new().await;
    f.probe.reject.store(true, Ordering::SeqCst);
    assert!(f
        .store
        .revoke_principal_credential("original-hash")
        .await
        .is_err());
    assert!(f
        .store
        .lookup_principal_credential("original-hash")
        .await
        .unwrap()
        .unwrap()
        .is_active());
}

#[tokio::test]
async fn actual_sql_hook_observes_retirement_before_permission_dml() {
    let f = Fixture::new().await;
    let old = f.wire();
    let observed = Arc::new(AtomicBool::new(false));
    let flag = observed.clone();
    let leaf = old.clone();
    let mut connection = f.store.write_pool().acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |update| {
            if update.table == "principal_credential" {
                flag.store(!leaf.load(Ordering::SeqCst), Ordering::SeqCst);
            }
        });
    drop(connection);
    f.store
        .revoke_principal_credential("original-hash")
        .await
        .unwrap();
    let mut connection = f.store.write_pool().acquire().await.unwrap();
    connection.lock_handle().await.unwrap().remove_update_hook();
    assert!(
        observed.load(Ordering::SeqCst),
        "SQL executed before the retirement barrier"
    );
}

#[tokio::test]
async fn failed_write_keeps_its_unknown_barrier_after_later_known_commit() {
    let f = Fixture::new().await;
    sqlx::query("CREATE TEMP TRIGGER refuse_wire_credential BEFORE INSERT ON principal_credential BEGIN SELECT RAISE(ABORT, 'fixture failure'); END")
        .execute(f.store.write_pool()).await.unwrap();
    let old = f.wire();
    assert!(f
        .store
        .insert_principal_credential(&f.person.id, "failed")
        .await
        .is_err());
    assert!(!old.load(Ordering::SeqCst));
    assert!(f.probe.wire().is_none());
    sqlx::query("DROP TRIGGER refuse_wire_credential")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    f.store
        .insert_principal_credential(&f.person.id, "later")
        .await
        .unwrap();
    assert!(
        f.probe.wire().is_none(),
        "a later known owner cannot settle the failed original owner"
    );
    assert!(f
        .store
        .lookup_principal_credential("failed")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn clone_and_independent_store_share_wire_retirement() {
    let f = Fixture::new().await;
    let second = Store::open(&f.dir.path().join("wire.db")).await.unwrap();
    let old = f.wire();
    second
        .clone()
        .revoke_principal_credential("original-hash")
        .await
        .unwrap();
    f.retired(&old);
}

#[tokio::test]
async fn canceled_actual_credential_worker_survives_last_store_without_unblocking() {
    for installed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("late.db");
        let store = Store::open(&path).await.unwrap();
        let person = person(42);
        store.upsert_principal(&person).await.unwrap();
        let probe = Arc::new(Probe::default());
        if installed {
            store
                .install_repository_lifecycle_observer(probe.clone())
                .await
                .unwrap();
        }
        let pool = store.write_pool().clone();
        let started = Arc::new(tokio::sync::Notify::new());
        let notify = started.clone();
        let (release, blocked) = std::sync::mpsc::sync_channel(1);
        let mut blocked = Some(blocked);
        let mut connection = pool.acquire().await.unwrap();
        connection
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |update| {
                if update.table == "principal_credential" {
                    if let Some(blocked) = blocked.take() {
                        notify.notify_one();
                        blocked
                            .recv_timeout(std::time::Duration::from_secs(20))
                            .unwrap();
                    }
                }
            });
        drop(connection);
        let task =
            tokio::spawn(
                async move { store.insert_principal_credential(&person.id, "late").await },
            );
        started.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        let mut connection = pool.acquire().await.unwrap();
        connection.lock_handle().await.unwrap().remove_update_hook();
        drop(connection);
        let reopened = Store::open(&path).await.unwrap();
        assert!(reopened
            .lookup_principal_credential("late")
            .await
            .unwrap()
            .is_some());
        if installed {
            assert!(
                probe.wire().is_none(),
                "reopen must not settle the canceled credential owner"
            );
        } else {
            assert!(reopened
                .install_repository_lifecycle_observer(probe)
                .await
                .is_err());
        }
    }
}

#[tokio::test]
async fn independent_writer_classifies_noop_only_after_original_settles() {
    use std::future::Future;
    use std::task::Poll;

    let f = Fixture::new().await;
    let second = Store::open(&f.dir.path().join("wire.db")).await.unwrap();
    let original = f.wire();
    let started = Arc::new(tokio::sync::Notify::new());
    let notify = started.clone();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let mut blocked = Some(blocked);
    let mut connection = f.store.write_pool().acquire().await.unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |update| {
            if update.table == "principal_credential" {
                if let Some(blocked) = blocked.take() {
                    notify.notify_one();
                    blocked
                        .recv_timeout(std::time::Duration::from_secs(20))
                        .unwrap();
                }
            }
        });
    drop(connection);
    let store = f.store.clone();
    let first =
        tokio::spawn(async move { store.revoke_principal_credential("original-hash").await });
    started.notified().await;
    assert!(!original.load(Ordering::SeqCst));
    assert!(f.probe.wire().is_none());
    let starts = f.probe.starts();
    let mut later = Box::pin(second.revoke_principal_credential("original-hash"));
    std::future::poll_fn(|cx| {
        assert!(later.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(f.probe.starts(), starts);
    release.send(()).unwrap();
    assert!(first.await.unwrap().unwrap());
    assert!(!later.await.unwrap());
    let mut connection = f.store.write_pool().acquire().await.unwrap();
    connection.lock_handle().await.unwrap().remove_update_hook();
    assert_eq!(
        f.probe.starts(),
        starts,
        "second writer must see the committed no-op"
    );
    assert!(f.probe.wire().is_some());
}

#[tokio::test]
async fn failed_invite_transaction_rolls_back_without_claiming_owner_settlement() {
    let f = Fixture::new().await;
    f.invite().await;
    let before = f
        .store
        .get_workspace_invite("workspace-invite")
        .await
        .unwrap();
    let old = f.wire();
    let agent = f.agent();
    let generation = f
        .store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    // Actual duplicate credential error after principal/grant/invite DML in
    // the original transaction. A later read of rollback is not its ticket.
    assert!(f
        .store
        .join_workspace_by_invite(
            "workspace-invite",
            &f.workspace.id,
            &f.person,
            HostJoinCredential::Proof {
                token_hash: "original-hash",
                authorization_generation: generation
            },
            8
        )
        .await
        .is_err());
    assert!(!old.load(Ordering::SeqCst));
    assert!(f.probe.wire().is_none());
    assert_eq!(
        f.store
            .get_workspace_invite("workspace-invite")
            .await
            .unwrap(),
        before
    );
    assert!(f
        .store
        .get_workspace_member_role(&f.workspace.id, &f.person.id)
        .await
        .unwrap()
        .is_none());
    assert!(agent.load(Ordering::SeqCst));
    // A later original successful transaction owns a distinct ticket.
    f.store
        .join_workspace_by_invite(
            "workspace-invite",
            &f.workspace.id,
            &f.person,
            HostJoinCredential::Existing {
                token_hash: "original-hash",
            },
            8,
        )
        .await
        .unwrap();
    assert!(f.probe.wire().is_none());
}
