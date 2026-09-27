use super::*;
use crate::{CollaboratorAddOutcome, HostInviteJoinOutcome, HostJoinCredential, InviteJoinOutcome};
use intent_core::{now_iso, HostInvite, WorkspaceInvite};

struct Fixture {
    store: Store,
    workspace: WorkspaceId,
    person: PrincipalId,
    _dir: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("authority.db")).await.unwrap();
        let workspace = WorkspaceId::new();
        let person = PrincipalId::new();
        insert_workspace(&store, &workspace).await;
        insert_person(&store, &person).await;
        Self {
            store,
            workspace,
            person,
            _dir: dir,
        }
    }

    async fn snapshot(&self) -> RepositoryAuthoritySnapshot {
        self.store
            .repository_authority_snapshot(&self.workspace, &self.person, Some("original-hash"))
            .await
            .unwrap()
    }

    async fn add(&self) {
        assert!(self
            .store
            .add_workspace_member(&self.workspace, &self.person, WorkspaceRole::Collaborator)
            .await
            .unwrap());
    }
}

async fn insert_workspace(store: &Store, id: &WorkspaceId) {
    sqlx::query("INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES(?, 'Test', 'main', 'same-time', 'same-time')")
        .bind(id.as_str()).execute(store.write_pool()).await.unwrap();
}

async fn insert_person(store: &Store, id: &PrincipalId) {
    sqlx::query("INSERT INTO principal(id,is_primary,created_at,updated_at) VALUES(?,0,'same-time','same-time')")
        .bind(id.as_str()).execute(store.write_pool()).await.unwrap();
}

#[tokio::test]
async fn grant_delete_readd_and_role_aba_retire_original_revision() {
    let f = Fixture::new().await;
    f.add().await;
    let admitted = f.snapshot().await;
    assert!(f
        .store
        .remove_workspace_member(&f.workspace, &f.person)
        .await
        .unwrap());
    let removed = f.snapshot().await;
    assert!(removed.workspace_grant.value.is_none());
    assert!(
        removed.workspace_grant.revision.unwrap().get()
            > admitted.workspace_grant.revision.unwrap().get()
    );
    f.add().await;
    let readded = f.snapshot().await;
    assert_eq!(
        readded.workspace_grant.value,
        admitted.workspace_grant.value
    );
    assert!(
        readded.workspace_grant.revision.unwrap().get()
            > removed.workspace_grant.revision.unwrap().get()
    );
    let owner = f.store.get_primary_principal().await.unwrap();
    f.store
        .set_workspace_member_role(&f.workspace, &owner.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    f.store
        .set_workspace_member_role(&f.workspace, &f.person, WorkspaceRole::Owner)
        .await
        .unwrap();
    f.store
        .set_workspace_member_role(&f.workspace, &f.person, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let returned = f.snapshot().await;
    assert_eq!(
        returned.workspace_grant.value,
        readded.workspace_grant.value
    );
    assert!(
        returned.workspace_grant.revision.unwrap().get()
            > readded.workspace_grant.revision.unwrap().get()
    );
    assert_eq!(
        returned.host_authorization_generation,
        admitted.host_authorization_generation
    );
}

#[tokio::test]
async fn workspace_and_principal_same_id_recreation_keep_tombstones() {
    let f = Fixture::new().await;
    f.add().await;
    let admitted = f.snapshot().await;
    sqlx::query("DELETE FROM workspace WHERE id=?")
        .bind(f.workspace.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let deleted = f.snapshot().await;
    assert!(deleted.workspace.value.is_none());
    assert!(deleted.workspace_grant.value.is_none());
    insert_workspace(&f.store, &f.workspace).await;
    f.add().await;
    let recreated = f.snapshot().await;
    assert!(
        recreated.workspace.revision.unwrap().get() > admitted.workspace.revision.unwrap().get()
    );
    assert!(
        recreated.workspace_grant.revision.unwrap().get()
            > admitted.workspace_grant.revision.unwrap().get()
    );
    sqlx::query("DELETE FROM principal WHERE id=?")
        .bind(f.person.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    assert!(f.snapshot().await.principal.value.is_none());
    insert_person(&f.store, &f.person).await;
    f.add().await;
    let returned = f.snapshot().await;
    assert_eq!(returned.principal.value, admitted.principal.value);
    assert!(
        returned.principal.revision.unwrap().get() > admitted.principal.revision.unwrap().get()
    );
    assert!(
        returned.workspace_grant.revision.unwrap().get()
            > recreated.workspace_grant.revision.unwrap().get()
    );
}

#[tokio::test]
async fn no_op_failed_and_rolled_back_writes_preserve_snapshot() {
    let f = Fixture::new().await;
    f.add().await;
    f.store
        .insert_principal_credential(&f.person, "original-hash")
        .await
        .unwrap();
    let before = f.snapshot().await;
    assert!(!f
        .store
        .add_workspace_member(&f.workspace, &f.person, WorkspaceRole::Collaborator)
        .await
        .unwrap());
    f.store
        .set_workspace_member_role(&f.workspace, &f.person, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    assert!(f
        .store
        .set_workspace_member_role(&f.workspace, &f.person, WorkspaceRole::Owner)
        .await
        .is_err());
    assert!(f
        .store
        .add_workspace_member(
            &f.workspace,
            &PrincipalId::new(),
            WorkspaceRole::Collaborator
        )
        .await
        .is_err());
    f.store
        .resolve_active_principal_credential("original-hash")
        .await
        .unwrap();
    sqlx::query("UPDATE principal SET display_name='new profile',updated_at='later' WHERE id=?")
        .bind(f.person.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    sqlx::query("UPDATE workspace SET title='New title' WHERE id=?")
        .bind(f.workspace.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let mut tx = f.store.write_pool().begin().await.unwrap();
    sqlx::query("DELETE FROM workspace_member WHERE workspace_id=? AND principal_id=?")
        .bind(f.workspace.as_str())
        .bind(f.person.as_str())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(f.snapshot().await, before);
}

#[tokio::test]
async fn membership_movement_retires_both_old_and_new_keys() {
    let f = Fixture::new().await;
    let other_workspace = WorkspaceId::new();
    let other_person = PrincipalId::new();
    insert_workspace(&f.store, &other_workspace).await;
    insert_person(&f.store, &other_person).await;
    f.add().await;
    let original = f.snapshot().await;
    sqlx::query("UPDATE workspace_member SET workspace_id=?,principal_id=? WHERE workspace_id=? AND principal_id=?")
        .bind(other_workspace.as_str()).bind(other_person.as_str()).bind(f.workspace.as_str()).bind(f.person.as_str())
        .execute(f.store.write_pool()).await.unwrap();
    let moved = f
        .store
        .repository_authority_snapshot(&other_workspace, &other_person, None)
        .await
        .unwrap();
    assert_eq!(
        moved.workspace_grant.value,
        Some(WorkspaceRole::Collaborator)
    );
    assert!(f.snapshot().await.workspace_grant.value.is_none());
    sqlx::query("UPDATE workspace_member SET workspace_id=?,principal_id=? WHERE workspace_id=? AND principal_id=?")
        .bind(f.workspace.as_str()).bind(f.person.as_str()).bind(other_workspace.as_str()).bind(other_person.as_str())
        .execute(f.store.write_pool()).await.unwrap();
    let returned = f.snapshot().await;
    assert_eq!(
        returned.workspace_grant.value,
        original.workspace_grant.value
    );
    assert!(
        returned.workspace_grant.revision.unwrap().get()
            > original.workspace_grant.revision.unwrap().get()
    );
    let old = f
        .store
        .repository_authority_snapshot(&other_workspace, &other_person, None)
        .await
        .unwrap();
    assert!(old.workspace_grant.value.is_none());
    assert!(
        old.workspace_grant.revision.unwrap().get() > moved.workspace_grant.revision.unwrap().get()
    );
}

#[tokio::test]
async fn host_and_credential_revocation_recreation_have_independent_continuity() {
    let f = Fixture::new().await;
    f.add().await;
    f.store
        .insert_principal_credential(&f.person, "original-hash")
        .await
        .unwrap();
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES(?,'same-time')")
        .bind(f.person.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let original = f.snapshot().await;
    f.store.remove_host_member(&f.person).await.unwrap();
    let revoked = f.snapshot().await;
    assert!(revoked.host_member.value.is_none());
    assert!(revoked.workspace_grant.value.is_none());
    assert!(
        revoked
            .credential
            .as_ref()
            .unwrap()
            .value
            .as_ref()
            .unwrap()
            .revoked
    );
    assert!(revoked.host_authorization_generation > original.host_authorization_generation);
    assert_eq!(
        revoked.principal_revocation_generation,
        Some(revoked.host_authorization_generation)
    );
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES(?,'same-time')")
        .bind(f.person.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM principal_credential WHERE token_hash='original-hash'")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    f.store
        .insert_principal_credential(&f.person, "original-hash")
        .await
        .unwrap();
    let readded = f.snapshot().await;
    assert_eq!(readded.host_member.value, original.host_member.value);
    assert!(
        readded.host_member.revision.unwrap().get() > original.host_member.revision.unwrap().get()
    );
    assert_eq!(
        readded.credential.as_ref().unwrap().value,
        original.credential.as_ref().unwrap().value
    );
    assert!(
        readded.credential.unwrap().revision.unwrap().get()
            > original.credential.unwrap().revision.unwrap().get()
    );
}

#[tokio::test]
async fn principal_identity_and_primary_aba_retire_original_evidence() {
    let f = Fixture::new().await;
    let original = f.snapshot().await;
    sqlx::query("UPDATE principal SET identity_provider='gitlab',instance_host='forge.example/prefix',external_user_id='42' WHERE id=?")
        .bind(f.person.as_str()).execute(f.store.write_pool()).await.unwrap();
    sqlx::query("UPDATE principal SET identity_provider=NULL,instance_host=NULL,external_user_id=NULL WHERE id=?")
        .bind(f.person.as_str()).execute(f.store.write_pool()).await.unwrap();
    let primary = original.primary_principal.value.as_ref().unwrap();
    for (id, flag) in [
        (&primary.id, 0),
        (&f.person, 1),
        (&f.person, 0),
        (&primary.id, 1),
    ] {
        sqlx::query("UPDATE principal SET is_primary=? WHERE id=?")
            .bind(flag)
            .bind(id.as_str())
            .execute(f.store.write_pool())
            .await
            .unwrap();
    }
    let returned = f.snapshot().await;
    assert_eq!(returned.principal.value, original.principal.value);
    assert_eq!(
        returned.primary_principal.value,
        original.primary_principal.value
    );
    assert!(
        returned.principal.revision.unwrap().get() > original.principal.revision.unwrap().get()
    );
    assert!(
        returned.primary_principal.revision.unwrap().get()
            > original.primary_principal.revision.unwrap().get()
    );
}

#[tokio::test]
async fn credential_subject_mismatch_and_missing_hash_are_not_grants() {
    let f = Fixture::new().await;
    let owner = f.store.get_primary_principal().await.unwrap();
    f.store
        .insert_principal_credential(&owner.id, "original-hash")
        .await
        .unwrap();
    let snapshot = f.snapshot().await;
    assert_eq!(
        snapshot.credential.unwrap().value.unwrap().principal_id,
        owner.id
    );
    let absent = f
        .store
        .repository_authority_snapshot(&f.workspace, &f.person, None)
        .await
        .unwrap();
    assert!(absent.credential.is_none());
    assert!(absent.workspace_grant.value.is_none());
    assert!(absent.host_member.value.is_none());
    assert!(!format!("{absent:?}").contains("original-hash"));
}

#[tokio::test]
async fn overflow_aborts_authority_effect_even_with_outer_ignore() {
    let f = Fixture::new().await;
    f.add().await;
    sqlx::query("UPDATE repository_authority_revision SET revision=9223372036854775807 WHERE kind='workspace_member' AND subject_id=? AND member_id=?")
        .bind(f.workspace.as_str()).bind(f.person.as_str()).execute(f.store.write_pool()).await.unwrap();
    let before = f.snapshot().await;
    assert!(f
        .store
        .remove_workspace_member(&f.workspace, &f.person)
        .await
        .is_err());
    assert_eq!(f.snapshot().await, before);
    let failed = sqlx::query("UPDATE OR IGNORE workspace_member SET principal_id=? WHERE workspace_id=? AND principal_id=?")
        .bind(before.primary_principal.value.as_ref().unwrap().id.as_str()).bind(f.workspace.as_str()).bind(f.person.as_str())
        .execute(f.store.write_pool()).await;
    // The existing primary key conflict is a harmless ignored source write.
    assert!(failed.is_ok());
    assert_eq!(f.snapshot().await, before);
    let other = PrincipalId::new();
    insert_person(&f.store, &other).await;
    assert!(sqlx::query("UPDATE OR IGNORE workspace_member SET principal_id=? WHERE workspace_id=? AND principal_id=?")
        .bind(other.as_str()).bind(f.workspace.as_str()).bind(f.person.as_str()).execute(f.store.write_pool()).await.is_err());
    assert_eq!(f.snapshot().await, before);
    assert!(sqlx::query("DELETE FROM workspace WHERE id=?")
        .bind(f.workspace.as_str())
        .execute(f.store.write_pool())
        .await
        .is_err());
    assert_eq!(
        f.snapshot().await,
        before,
        "cascade failure rolls back workspace and grant together"
    );
}

#[tokio::test]
async fn missing_or_invalid_provenance_fails_closed_and_tombstones_cannot_reset() {
    let f = Fixture::new().await;
    f.add().await;
    for sql in [
        "DELETE FROM repository_authority_revision WHERE kind='workspace_member'",
        "UPDATE repository_authority_revision SET revision=0 WHERE kind='workspace_member'",
        "INSERT OR REPLACE INTO repository_authority_revision SELECT kind,subject_id,member_id,1 FROM repository_authority_revision WHERE kind='workspace_member'",
    ] {
        assert!(sqlx::query(sql).execute(f.store.write_pool()).await.is_err());
    }
    sqlx::query("DROP TRIGGER repository_authority_revision_no_delete")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    sqlx::query(
        "DELETE FROM repository_authority_revision WHERE kind='principal' AND subject_id=?",
    )
    .bind(f.person.as_str())
    .execute(f.store.write_pool())
    .await
    .unwrap();
    assert!(f
        .store
        .repository_authority_snapshot(&f.workspace, &f.person, None)
        .await
        .is_err());
    assert!(counter(-1, false).is_err());
    assert!(counter(0, true).is_err());
}

#[tokio::test]
async fn read_transaction_keeps_grant_and_credential_from_one_snapshot() {
    let f = Fixture::new().await;
    f.add().await;
    f.store
        .insert_principal_credential(&f.person, "original-hash")
        .await
        .unwrap();
    let original = f.snapshot().await;
    let mut reader = f.store.read_pool().begin().await.unwrap();
    // Establish the actual SQLite snapshot, then commit a competing writer.
    sqlx::query("SELECT id FROM workspace")
        .fetch_all(&mut *reader)
        .await
        .unwrap();
    f.store.revoke_principal_access(&f.person).await.unwrap();
    let same_transaction =
        read_snapshot(&mut reader, &f.workspace, &f.person, Some("original-hash"))
            .await
            .unwrap();
    assert_eq!(same_transaction, original);
    reader.commit().await.unwrap();
    let next = f.snapshot().await;
    assert!(next.workspace_grant.value.is_none());
    assert!(next.credential.unwrap().value.unwrap().revoked);
    assert_ne!(
        next.workspace_grant.revision,
        original.workspace_grant.revision
    );
}

#[tokio::test]
async fn capped_and_invite_routes_update_the_same_durable_grant() {
    let f = Fixture::new().await;
    f.store
        .insert_principal_credential(&f.person, "original-hash")
        .await
        .unwrap();
    let before = f.snapshot().await;
    assert_eq!(
        f.store
            .add_workspace_collaborator_within_cap(&f.workspace, &f.person, 0)
            .await
            .unwrap(),
        CollaboratorAddOutcome::WorkspaceFull
    );
    assert_eq!(f.snapshot().await, before);
    assert_eq!(
        f.store
            .add_workspace_collaborator_within_cap(&f.workspace, &f.person, 1)
            .await
            .unwrap(),
        CollaboratorAddOutcome::Added
    );
    let added = f.snapshot().await;
    assert_eq!(
        f.store
            .add_workspace_collaborator_within_cap(&f.workspace, &f.person, 1)
            .await
            .unwrap(),
        CollaboratorAddOutcome::AlreadyMember
    );
    assert_eq!(f.snapshot().await, added);
    f.store
        .remove_workspace_guest(&f.workspace, &f.person)
        .await
        .unwrap();
    let mut person = f.store.get_principal(&f.person).await.unwrap();
    person.github_user_id = Some(12345);
    person.login = Some("guest".into());
    f.store.upsert_principal(&person).await.unwrap();
    let owner = f.store.get_primary_principal().await.unwrap();
    let invite = WorkspaceInvite {
        id: "grant-invite".into(),
        workspace_id: f.workspace.clone(),
        secret_hash: "invite-hash".into(),
        secret: None,
        created_by_principal_id: owner.id,
        pin_identity: None,
        pin_github_user_id: None,
        pin_login: None,
        created_at: now_iso(),
        expires_at: "2999-01-01T00:00:00.000Z".into(),
        redeemed_at: None,
        redeemed_by_principal_id: None,
        revoked_at: None,
        redemption_count: 0,
    };
    f.store.insert_workspace_invite(&invite).await.unwrap();
    let original_generation = f.snapshot().await.host_authorization_generation;
    let outcome = f
        .store
        .join_workspace_by_invite(
            &invite.id,
            &f.workspace,
            &person,
            HostJoinCredential::Existing {
                token_hash: "original-hash",
            },
            2,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, InviteJoinOutcome::Joined(_)));
    let joined = f.snapshot().await;
    assert_eq!(
        joined.workspace_grant.value,
        Some(WorkspaceRole::Collaborator)
    );
    assert!(
        joined.workspace_grant.revision.unwrap().get()
            > added.workspace_grant.revision.unwrap().get()
    );
    assert_eq!(joined.host_authorization_generation, original_generation);
    assert!(matches!(
        f.store
            .join_workspace_by_invite(
                &invite.id,
                &f.workspace,
                &person,
                HostJoinCredential::Existing {
                    token_hash: "original-hash"
                },
                2
            )
            .await
            .unwrap(),
        InviteJoinOutcome::Rejoined(_)
    ));
    assert_eq!(f.snapshot().await, joined);
    assert_eq!(
        f.store
            .count_workspace_guests(&f.workspace)
            .await
            .unwrap()
            .collaborators,
        1
    );
}

#[tokio::test]
async fn host_join_revoke_rejoin_preserves_host_and_workspace_semantics() {
    let f = Fixture::new().await;
    let owner = f.store.get_primary_principal().await.unwrap();
    let mut person = f.store.get_principal(&f.person).await.unwrap();
    person.github_user_id = Some(54321);
    person.login = Some("member".into());
    f.store.upsert_principal(&person).await.unwrap();
    f.add().await;
    let original = f.snapshot().await;
    for index in 0..2 {
        let invite = HostInvite::new(
            format!("host-{index}"),
            owner.id.clone(),
            person.identity_key().unwrap(),
            "member".into(),
            format!("host-hash-{index}"),
            None,
        )
        .unwrap();
        f.store.insert_host_invite(&invite).await.unwrap();
        let generation = f.snapshot().await.host_authorization_generation;
        assert!(matches!(
            f.store
                .join_host_by_invite(
                    &invite.id,
                    &person,
                    HostJoinCredential::Proof {
                        token_hash: &format!("member-{index}"),
                        authorization_generation: generation
                    }
                )
                .await
                .unwrap(),
            HostInviteJoinOutcome::Joined { .. }
        ));
        assert_eq!(
            f.store.host_membership_state().await.unwrap().member_count,
            1
        );
        if index == 0 {
            f.store.remove_host_member(&f.person).await.unwrap();
        }
    }
    let rejoined = f.snapshot().await;
    assert_eq!(rejoined.host_member.value, Some(()));
    assert!(rejoined.host_member.revision.unwrap().get() >= 3);
    assert!(
        rejoined.workspace_grant.value.is_none(),
        "host rejoin does not restore revoked direct grant"
    );
    assert!(
        rejoined.workspace_grant.revision.unwrap().get()
            > original.workspace_grant.revision.unwrap().get()
    );
    assert_eq!(f.store.get_primary_principal().await.unwrap(), owner);
}

#[tokio::test]
async fn migration_seeds_existing_rows_without_rewriting_authority_and_restart_keeps_revisions() {
    use std::borrow::Cow;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("upgrade.db");
    let pool = crate::connect_write(&path).await.unwrap();
    sqlx::migrate::Migrator {
        migrations: Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version < 136)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    }
    .run(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES('workspace','Test','main','same-time','same-time')").execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO principal(id,created_at,updated_at) VALUES('person','same-time','same-time')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO workspace_member VALUES('workspace','person','collaborator','same-time')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO host_member VALUES('person','same-time')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO principal_credential(token_hash,principal_id,created_at) VALUES('original-hash','person','same-time')").execute(&pool).await.unwrap();
    let workspace = WorkspaceId("workspace".into());
    let person = PrincipalId("person".into());
    let host: (i64, i64, i64) = sqlx::query_as(
        "SELECT revision, member_count, authorization_generation FROM host_membership_state",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    pool.close().await;
    let store = Store::open(&path).await.unwrap();
    let before = store
        .repository_authority_snapshot(&workspace, &person, Some("original-hash"))
        .await
        .unwrap();
    for revision in [
        before.workspace.revision,
        before.principal.revision,
        before.host_member.revision,
        before.workspace_grant.revision,
        before.credential.as_ref().unwrap().revision,
    ] {
        assert_eq!(revision.unwrap().get(), 1);
    }
    assert_eq!(
        before.workspace_grant.value,
        Some(WorkspaceRole::Collaborator)
    );
    assert_eq!(before.host_member.value, Some(()));
    assert!(
        !before
            .credential
            .as_ref()
            .unwrap()
            .value
            .as_ref()
            .unwrap()
            .revoked
    );
    let after: (i64, i64, i64) = sqlx::query_as(
        "SELECT revision, member_count, authorization_generation FROM host_membership_state",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert_eq!(host, after);
    store.read_pool().close().await;
    store.write_pool().close().await;
    let reopened = Store::open(&path).await.unwrap();
    assert_eq!(
        reopened
            .repository_authority_snapshot(&workspace, &person, Some("original-hash"))
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn exhausted_destination_rolls_back_both_movement_and_insert() {
    let f = Fixture::new().await;
    f.add().await;
    let other = PrincipalId::new();
    insert_person(&f.store, &other).await;
    sqlx::query("INSERT INTO repository_authority_revision VALUES('workspace_member',?,?,9223372036854775807)")
        .bind(f.workspace.as_str()).bind(other.as_str()).execute(f.store.write_pool()).await.unwrap();
    let before = f.snapshot().await;
    assert!(sqlx::query("UPDATE OR IGNORE workspace_member SET principal_id=? WHERE workspace_id=? AND principal_id=?")
        .bind(other.as_str()).bind(f.workspace.as_str()).bind(f.person.as_str()).execute(f.store.write_pool()).await.is_err());
    assert_eq!(
        f.snapshot().await,
        before,
        "destination exhaustion restores the earlier old-key bump"
    );
    assert!(sqlx::query(
        "INSERT OR IGNORE INTO workspace_member VALUES(?,?,'collaborator','same-time')"
    )
    .bind(f.workspace.as_str())
    .bind(other.as_str())
    .execute(f.store.write_pool())
    .await
    .is_err());
    let dest = f
        .store
        .repository_authority_snapshot(&f.workspace, &other, None)
        .await
        .unwrap();
    assert!(dest.workspace_grant.value.is_none());
    assert_eq!(
        dest.workspace_grant.revision.unwrap().get(),
        u64::try_from(i64::MAX).unwrap()
    );
}

#[tokio::test]
async fn credential_rebinding_and_principal_cascades_retire_all_affected_evidence() {
    let f = Fixture::new().await;
    let other = PrincipalId::new();
    insert_person(&f.store, &other).await;
    f.store
        .insert_principal_credential(&f.person, "original-hash")
        .await
        .unwrap();
    sqlx::query("INSERT INTO host_member VALUES(?,'same-time')")
        .bind(f.person.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let original = f.snapshot().await;
    for id in [&other, &f.person] {
        sqlx::query(
            "UPDATE principal_credential SET principal_id=? WHERE token_hash='original-hash'",
        )
        .bind(id.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
        sqlx::query("UPDATE host_member SET principal_id=?")
            .bind(id.as_str())
            .execute(f.store.write_pool())
            .await
            .unwrap();
    }
    let rebound = f.snapshot().await;
    assert_eq!(
        rebound.credential.as_ref().unwrap().value,
        original.credential.as_ref().unwrap().value
    );
    assert!(
        rebound.credential.as_ref().unwrap().revision.unwrap().get()
            > original.credential.unwrap().revision.unwrap().get()
    );
    assert!(
        rebound.host_member.revision.unwrap().get() > original.host_member.revision.unwrap().get()
    );
    sqlx::query("DELETE FROM principal WHERE id=?")
        .bind(f.person.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let removed = f.snapshot().await;
    assert!(removed.host_member.value.is_none());
    assert!(removed.credential.as_ref().unwrap().value.is_none());
    assert!(
        removed.host_member.revision.unwrap().get() > rebound.host_member.revision.unwrap().get()
    );
    assert!(
        removed.credential.unwrap().revision.unwrap().get()
            > rebound.credential.unwrap().revision.unwrap().get()
    );
}

#[tokio::test]
async fn corrupted_durable_counter_is_rejected_by_actual_snapshot_reader() {
    let f = Fixture::new().await;
    // Deliberately bypass schema guards only in this disposable corruption fixture.
    let mut writer = f.store.write_pool().acquire().await.unwrap();
    sqlx::query("DROP TRIGGER repository_authority_revision_monotonic")
        .execute(&mut *writer)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints=ON")
        .execute(&mut *writer)
        .await
        .unwrap();
    sqlx::query("UPDATE repository_authority_revision SET revision=-1 WHERE kind='principal' AND subject_id=?")
        .bind(f.person.as_str()).execute(&mut *writer).await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints=OFF")
        .execute(&mut *writer)
        .await
        .unwrap();
    drop(writer);
    assert!(f
        .store
        .repository_authority_snapshot(&f.workspace, &f.person, None)
        .await
        .is_err());
}

#[tokio::test]
async fn workspace_only_tracks_absence_owner_aba_and_recreation() {
    let f = Fixture::new().await;
    let read = || {
        f.store
            .repository_workspace_authority_snapshot(&f.workspace)
    };
    let original = read().await.unwrap();
    let human = f.snapshot().await;
    assert_eq!(original.workspace, human.workspace);
    assert_eq!(
        original.host_authorization_generation,
        human.host_authorization_generation
    );
    let unknown = WorkspaceId::new();
    let absent = f
        .store
        .repository_workspace_authority_snapshot(&unknown)
        .await
        .unwrap();
    assert_eq!(absent.workspace_id, unknown);
    assert!(absent.workspace.value.is_none());
    assert!(absent.workspace.revision.is_none());
    let owner = f.store.get_primary_principal().await.unwrap();
    f.store
        .set_workspace_member_role(&f.workspace, &owner.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    f.store
        .add_workspace_member(&f.workspace, &f.person, WorkspaceRole::Owner)
        .await
        .unwrap();
    let changed = read().await.unwrap();
    assert_eq!(
        changed.workspace.value.unwrap().owner_principal_id,
        Some(f.person.clone())
    );
    f.store
        .set_workspace_member_role(&f.workspace, &f.person, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    f.store
        .set_workspace_member_role(&f.workspace, &owner.id, WorkspaceRole::Owner)
        .await
        .unwrap();
    let returned = read().await.unwrap();
    assert_eq!(returned.workspace.value, original.workspace.value);
    assert!(
        returned.workspace.revision.unwrap().get() > original.workspace.revision.unwrap().get()
    );
    assert_eq!(returned.workspace, f.snapshot().await.workspace);
    sqlx::query("DELETE FROM workspace WHERE id=?")
        .bind(f.workspace.as_str())
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let deleted = read().await.unwrap();
    assert!(deleted.workspace.value.is_none());
    assert!(deleted.workspace.revision.unwrap().get() > returned.workspace.revision.unwrap().get());
    insert_workspace(&f.store, &f.workspace).await;
    let recreated = read().await.unwrap();
    assert_eq!(recreated.workspace.value, original.workspace.value);
    assert!(
        recreated.workspace.revision.unwrap().get() > deleted.workspace.revision.unwrap().get()
    );
    assert_eq!(recreated.workspace, f.snapshot().await.workspace);
}

#[tokio::test]
async fn workspace_only_has_no_human_or_primary_requirement() {
    let f = Fixture::new().await;
    sqlx::query("DELETE FROM principal")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM principal")
        .fetch_one(f.store.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    let snapshot = f
        .store
        .repository_workspace_authority_snapshot(&f.workspace)
        .await
        .unwrap();
    assert_eq!(snapshot.workspace_id, f.workspace);
    assert!(snapshot.workspace.value.is_some());
    // A dangling historical owner column stays only a fact; it is never replaced
    // by a fabricated principal or used here to authorize an internal caller.
    let human = f.snapshot().await;
    assert!(human.principal.value.is_none());
    assert!(human.primary_principal.value.is_none());
    assert_eq!(snapshot.workspace, human.workspace);
}

#[tokio::test]
async fn workspace_only_does_not_read_or_default_missing_human_provenance() {
    let f = Fixture::new().await;
    let before = f
        .store
        .repository_workspace_authority_snapshot(&f.workspace)
        .await
        .unwrap();
    // Corrupt only human provenance in a disposable fixture. A hidden primary
    // lookup would fail; an optional-human fallback would wrongly hide it.
    sqlx::query("DROP TRIGGER repository_authority_revision_no_delete")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM repository_authority_revision WHERE kind='principal'")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        f.store
            .repository_workspace_authority_snapshot(&f.workspace)
            .await
            .unwrap(),
        before
    );
    assert!(f
        .store
        .repository_authority_snapshot(&f.workspace, &f.person, None)
        .await
        .is_err());
}

#[tokio::test]
async fn workspace_only_and_human_helpers_read_the_same_closed_snapshot() {
    let f = Fixture::new().await;
    f.add().await;
    f.store
        .insert_principal_credential(&f.person, "original-hash")
        .await
        .unwrap();
    let before = f
        .store
        .repository_workspace_authority_snapshot(&f.workspace)
        .await
        .unwrap();
    let human_before = f.snapshot().await;
    let owner = f.store.get_primary_principal().await.unwrap();
    let mut tx = f.store.read_pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM workspace")
        .fetch_all(&mut *tx)
        .await
        .unwrap();
    f.store
        .set_workspace_member_role(&f.workspace, &owner.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    f.store
        .set_workspace_member_role(&f.workspace, &f.person, WorkspaceRole::Owner)
        .await
        .unwrap();
    f.store.revoke_principal_access(&f.person).await.unwrap();
    let common = read_workspace_snapshot(&mut tx, &f.workspace)
        .await
        .unwrap();
    let human = read_snapshot(&mut tx, &f.workspace, &f.person, Some("original-hash"))
        .await
        .unwrap();
    assert_eq!(common, before);
    assert_eq!(human, human_before);
    assert_eq!(common.workspace, human.workspace);
    assert_eq!(
        common.host_authorization_generation,
        human.host_authorization_generation
    );
    tx.commit().await.unwrap();
    let now = f
        .store
        .repository_workspace_authority_snapshot(&f.workspace)
        .await
        .unwrap();
    assert_ne!(now.workspace, before.workspace);
    assert!(now.host_authorization_generation > before.host_authorization_generation);
    let human_now = f.snapshot().await;
    assert_eq!(now.workspace, human_now.workspace);
    assert_eq!(
        now.host_authorization_generation,
        human_now.host_authorization_generation
    );
    assert!(human_now.credential.unwrap().value.unwrap().revoked);
}

#[tokio::test]
async fn workspace_only_rejects_missing_and_invalid_workspace_provenance() {
    for missing in [true, false] {
        let f = Fixture::new().await;
        let mut writer = f.store.write_pool().acquire().await.unwrap();
        if missing {
            sqlx::query("DROP TRIGGER repository_authority_revision_no_delete")
                .execute(&mut *writer)
                .await
                .unwrap();
            sqlx::query(
                "DELETE FROM repository_authority_revision WHERE kind='workspace' AND subject_id=?",
            )
            .bind(f.workspace.as_str())
            .execute(&mut *writer)
            .await
            .unwrap();
        } else {
            sqlx::query("DROP TRIGGER repository_authority_revision_monotonic")
                .execute(&mut *writer)
                .await
                .unwrap();
            sqlx::query("PRAGMA ignore_check_constraints=ON")
                .execute(&mut *writer)
                .await
                .unwrap();
            sqlx::query("UPDATE repository_authority_revision SET revision=-1 WHERE kind='workspace' AND subject_id=?")
                .bind(f.workspace.as_str()).execute(&mut *writer).await.unwrap();
            sqlx::query("PRAGMA ignore_check_constraints=OFF")
                .execute(&mut *writer)
                .await
                .unwrap();
        }
        drop(writer);
        assert!(f
            .store
            .repository_workspace_authority_snapshot(&f.workspace)
            .await
            .is_err());
        assert!(f
            .store
            .repository_authority_snapshot(&f.workspace, &f.person, None)
            .await
            .is_err());
    }
}

#[tokio::test]
async fn workspace_only_does_not_invent_missing_host_generation() {
    let f = Fixture::new().await;
    sqlx::query("DELETE FROM host_membership_state")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    assert!(f
        .store
        .repository_workspace_authority_snapshot(&f.workspace)
        .await
        .is_err());
    assert!(f
        .store
        .repository_authority_snapshot(&f.workspace, &f.person, None)
        .await
        .is_err());
}
