use std::borrow::Cow;
use std::sync::Arc;

use intent_core::{parse_iso, HostInvite, HostMembershipState, HostRole};
use sqlx::{migrate::Migrator, sqlite::SqliteConnectOptions, SqlitePool};
use tokio::sync::Barrier;

use super::*;
use crate::{HostInviteJoinOutcome as Join, HostJoinCredential as Credential};

fn invitation(id: &str, owner: &PrincipalId, person: &Principal) -> HostInvite {
    HostInvite::new(
        id.into(),
        owner.clone(),
        person.identity_key().expect("identity"),
        person.login.clone().expect("login"),
        format!("invite-hash-{id}"),
        Some(format!("invite-secret-{id}")),
    )
    .expect("new invite")
}

fn proof(hash: &str, generation: u64) -> Credential<'_> {
    Credential::Proof {
        token_hash: hash,
        authorization_generation: generation,
    }
}

async fn join(store: &Store, id: &str, person: &Principal, hash: &str) -> Join {
    let owner = store.get_primary_principal().await.expect("primary");
    store
        .insert_host_invite(&invitation(id, &owner.id, person))
        .await
        .expect("invite");
    let generation = store
        .host_membership_state()
        .await
        .expect("state")
        .authorization_generation;
    store
        .join_host_by_invite(id, person, proof(hash, generation))
        .await
        .expect("join")
}

#[tokio::test]
async fn upgrade_preserves_legacy_identity_grants_credentials_invites_and_owner_defaults() {
    let tmp = TempDb::new();
    let pool = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&tmp.path)
            .create_if_missing(true),
    )
    .await
    .expect("legacy pool");
    let migrator = Migrator {
        migrations: Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|migration| migration.version <= 129)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    migrator.run(&pool).await.expect("real pre-identity schema");
    let owner: String = sqlx::query_scalar("SELECT id FROM principal WHERE is_primary = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspace (id, title, branch, status, created_at, updated_at) VALUES ('old-workspace','Legacy','main','Active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO principal (id,github_user_id,login,is_primary,created_at,updated_at) VALUES ('old-guest',42,'before-rename',0,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO workspace_member VALUES ('old-workspace','old-guest','collaborator','2026-01-01T00:00:00Z')").execute(&pool).await.unwrap();
    for (hash, revoked) in [("active", None), ("revoked", Some("2026-02-01T00:00:00Z"))] {
        sqlx::query("INSERT INTO principal_credential (token_hash,principal_id,created_at,last_used_at,revoked_at) VALUES (?,'old-guest','2026-01-01T00:00:00Z','2026-01-02T00:00:00Z',?)")
            .bind(hash).bind(revoked).execute(&pool).await.unwrap();
    }
    for (id, redeemed, revoked, count) in [
        ("open", None, None, 0),
        ("used", Some("2026-01-02T00:00:00Z"), None, 1),
        ("closed", None, Some("2026-01-03T00:00:00Z"), 0),
    ] {
        sqlx::query("INSERT INTO workspace_invite (id,workspace_id,secret_hash,secret,created_by_principal_id,pin_github_user_id,pin_login,created_at,expires_at,redeemed_at,redeemed_by_principal_id,revoked_at,redemption_count) VALUES (?,'old-workspace',?,'saved-secret',?,42,'before-rename','2026-01-01T00:00:00Z','2099-01-01T00:00:00Z',?,?,?,?)")
            .bind(id).bind(format!("hash-{id}")).bind(&owner).bind(redeemed)
            .bind(redeemed.map(|_| "old-guest")).bind(revoked).bind(count).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO settings (key,value) VALUES ('preserve-me','owner-setting')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO note (id,workspace_id,title,content,created_at,updated_at) VALUES ('preserved-note','old-workspace','Keep','Shared work','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").execute(&pool).await.unwrap();
    pool.close().await;

    let store = Store::open(&tmp.path)
        .await
        .expect("upgrade through 0130-0132");
    let primary = store.get_primary_principal().await.unwrap();
    assert_eq!(primary.id.0, owner);
    assert!(
        primary.identity.is_none(),
        "an unlinked owner remains owner"
    );
    let guest = store
        .find_principal_by_identity(&PrincipalIdentity::github(42))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(guest.id.0, "old-guest");
    assert_eq!(
        store.get_host_role(&guest.id).await.unwrap(),
        HostRole::Guest
    );
    assert_eq!(
        store.host_membership_state().await.unwrap(),
        HostMembershipState::default()
    );
    assert_eq!(store.list_host_members().await.unwrap().members.len(), 1);
    assert_eq!(
        store
            .list_workspace_members(&WorkspaceId::from("old-workspace"))
            .await
            .unwrap()
            .len(),
        2
    );
    let credentials = store.list_principal_credentials(&guest.id).await.unwrap();
    assert_eq!(credentials.len(), 2);
    assert_eq!(credentials.iter().filter(|c| c.is_active()).count(), 1);
    assert!(credentials
        .iter()
        .all(|c| c.last_used_at.as_deref() == Some("2026-01-02T00:00:00Z")));
    for id in ["open", "used", "closed"] {
        let row = store.get_workspace_invite(id).await.unwrap().unwrap();
        assert_eq!(row.pin_identity, Some(PrincipalIdentity::github(42)));
        assert_eq!(row.pin_github_user_id, Some(42));
        assert_eq!(row.secret.as_deref(), Some("saved-secret"));
        assert_eq!(row.redemption_count, u64::from(id == "used"));
        assert_eq!(row.revoked_at.is_some(), id == "closed");
        assert_eq!(row.redeemed_at.is_some(), id == "used");
    }
    assert_eq!(
        store.get_setting("preserve-me").await.unwrap().as_deref(),
        Some("owner-setting")
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT content FROM note WHERE id = 'preserved-note'")
            .fetch_one(store.read_pool())
            .await
            .unwrap(),
        "Shared work"
    );
    let future = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&future, "Future", false))
        .await
        .unwrap();
    assert_eq!(
        store
            .get_workspace_owner_principal_id(&future)
            .await
            .unwrap(),
        Some(primary.id.clone())
    );
    store.close().await;
    let restarted = Store::open(&tmp.path).await.unwrap();
    assert_eq!(restarted.get_primary_principal().await.unwrap(), primary);
    assert_eq!(
        restarted
            .list_principal_credentials(&guest.id)
            .await
            .unwrap(),
        credentials
    );
    assert!(restarted.migration_status().await.unwrap().is_current());
}

#[tokio::test]
async fn empty_host_join_survives_restart_without_workspace_fanout_or_repository_profile() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    assert!(owner.identity.is_none());
    let member = guest_identity(10);
    let outcome = join(&store, "empty", &member, "member-token").await;
    assert!(matches!(
        outcome,
        Join::Joined {
            membership_added: true,
            revision: 1,
            ..
        }
    ));
    assert_eq!(
        store.get_host_role(&member.id).await.unwrap(),
        HostRole::Member
    );
    assert_eq!(
        store.get_host_role(&owner.id).await.unwrap(),
        HostRole::Owner
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workspace_member WHERE principal_id = ?"
        )
        .bind(&member.id.0)
        .fetch_one(store.read_pool())
        .await
        .unwrap(),
        0
    );
    let future = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&future, "Created later", false))
        .await
        .unwrap();
    assert_eq!(
        store.list_workspace_members(&future).await.unwrap().len(),
        1
    );
    assert_eq!(
        store
            .get_workspace_owner_principal_id(&future)
            .await
            .unwrap(),
        Some(owner.id.clone())
    );
    let before = store.list_host_members().await.unwrap();
    assert_eq!(before.members.len(), 2);
    assert_eq!(before.members[0].principal_id, owner.id);
    assert_eq!(before.members[0].added_at, owner.created_at);
    assert_eq!(before.members[1].host_role, HostRole::Member);
    let invite = store.get_host_invite("empty").await.unwrap().unwrap();
    assert_eq!(invite.redemption_count, 1);
    assert_eq!(invite.redeemed_by_principal_id, Some(member.id.clone()));
    assert!(store.list_open_host_invites().await.unwrap().is_empty());
    store.close().await;
    let store = Store::open(&tmp.path).await.unwrap();
    assert_eq!(store.list_host_members().await.unwrap(), before);
    assert_eq!(store.get_host_invite("empty").await.unwrap(), Some(invite));
    assert_eq!(
        store
            .resolve_active_principal_credential("member-token")
            .await
            .unwrap(),
        Some(member.id)
    );
    assert_eq!(store.get_primary_principal().await.unwrap(), owner);
}

#[tokio::test]
async fn guest_upgrade_keeps_identity_old_grants_and_reuses_the_presented_credential() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Old share", false))
        .await
        .unwrap();
    let person = guest_identity(42);
    store.upsert_principal(&person).await.unwrap();
    store
        .add_workspace_member(&ws, &person.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    store
        .insert_principal_credential(&person.id, "existing-token")
        .await
        .unwrap();
    let original_members = store.list_workspace_members(&ws).await.unwrap();
    let credentials = store.list_principal_credentials(&person.id).await.unwrap();
    let mut renamed = guest_identity(42);
    renamed.login = Some("renamed-account".into());
    store
        .insert_host_invite(&invitation("upgrade", &owner.id, &person))
        .await
        .unwrap();
    let outcome = store
        .join_host_by_invite(
            "upgrade",
            &renamed,
            Credential::Existing {
                token_hash: "existing-token",
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, Join::Joined { principal, membership_added: true, revision: 1 } if principal.id == person.id && principal.login == renamed.login)
    );
    assert_eq!(
        store.list_workspace_members(&ws).await.unwrap(),
        original_members
    );
    assert_eq!(
        store.list_principal_credentials(&person.id).await.unwrap(),
        credentials
    );
    let before = store.list_host_members().await.unwrap();
    store
        .insert_host_invite(&invitation("already-member", &owner.id, &person))
        .await
        .unwrap();
    assert!(matches!(
        store
            .join_host_by_invite(
                "already-member",
                &renamed,
                Credential::Existing {
                    token_hash: "existing-token"
                }
            )
            .await
            .unwrap(),
        Join::Joined {
            membership_added: false,
            revision: 1,
            ..
        }
    ));
    assert_eq!(store.list_host_members().await.unwrap(), before);
    assert_eq!(
        store
            .get_host_invite("already-member")
            .await
            .unwrap()
            .unwrap()
            .redemption_count,
        1
    );
    // Repository account changes only refresh the owner's profile; neither
    // membership nor the invited principal's credential is derived from it.
    let mut changed_owner = owner.clone();
    changed_owner.set_identity(PrincipalIdentity::github(999));
    store.upsert_principal(&changed_owner).await.unwrap();
    changed_owner.set_github_user_id(None);
    store.upsert_principal(&changed_owner).await.unwrap();
    assert_eq!(
        store.get_host_role(&person.id).await.unwrap(),
        HostRole::Member
    );
    assert_eq!(
        store.list_principal_credentials(&person.id).await.unwrap(),
        credentials
    );
    assert_eq!(
        store.get_workspace_owner_principal_id(&ws).await.unwrap(),
        Some(owner.id)
    );
}

#[tokio::test]
async fn stable_pin_isolates_providers_instances_and_ids_and_ignores_login_renames() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let mut target = guest_identity(42);
    target.set_identity(PrincipalIdentity {
        provider: "gitlab".into(),
        host: "gitlab.example:8443".into(),
        external_user_id: "42".into(),
    });
    store
        .insert_host_invite(&invitation("pinned", &owner.id, &target))
        .await
        .unwrap();
    let mut others = vec![guest_identity(42)];
    for (host, external_user_id) in [
        ("gitlab.com", "42"),
        ("gitlab.example", "42"),
        ("gitlab.example:8443", "43"),
    ] {
        let mut other = guest_identity(100);
        other.set_identity(PrincipalIdentity {
            provider: "gitlab".into(),
            host: host.into(),
            external_user_id: external_user_id.into(),
        });
        others.push(other);
    }
    for (i, other) in others.iter().enumerate() {
        store.upsert_principal(other).await.unwrap();
        assert_eq!(
            store
                .join_host_by_invite("pinned", other, proof(&format!("wrong-{i}"), 0))
                .await
                .unwrap(),
            Join::PinMismatch
        );
        assert_eq!(
            store.get_host_role(&other.id).await.unwrap(),
            HostRole::Guest
        );
        assert!(store
            .lookup_principal_credential(&format!("wrong-{i}"))
            .await
            .unwrap()
            .is_none());
    }
    target.login = Some("new-login".into());
    assert!(matches!(
        store
            .join_host_by_invite("pinned", &target, proof("right", 0))
            .await
            .unwrap(),
        Join::Joined { .. }
    ));
    assert_eq!(
        store
            .get_host_invite("pinned")
            .await
            .unwrap()
            .unwrap()
            .pin_login,
        "guest-42"
    );
    assert_eq!(
        store
            .find_principal_by_identity(&target.identity_key().unwrap())
            .await
            .unwrap()
            .unwrap()
            .id,
        target.id
    );
    assert_eq!(store.count_principals().await.unwrap(), 6);
}

#[tokio::test]
async fn expiry_revocation_scope_and_owner_refusals_leave_no_grant_or_credential() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let mut owner = store.get_primary_principal().await.unwrap();
    let person = guest_identity(20);
    let mut expired = invitation("expired", &owner.id, &person);
    assert_eq!(
        parse_iso(&expired.expires_at).unwrap() - parse_iso(&expired.created_at).unwrap(),
        std::time::Duration::from_secs(604_800)
    );
    assert!(
        !expired.is_open_at(&expired.expires_at),
        "expiry boundary is exclusive"
    );
    assert!(!expired.is_open_at("invalid"));
    expired.created_at = "2020-01-01T00:00:00Z".into();
    expired.expires_at = "2020-01-08T00:00:00Z".into();
    store.insert_host_invite(&expired).await.unwrap();
    assert_eq!(
        store
            .join_host_by_invite("expired", &person, proof("expired-cred", 0))
            .await
            .unwrap(),
        Join::Expired
    );
    store
        .insert_host_invite(&invitation("revoked", &owner.id, &person))
        .await
        .unwrap();
    assert!(store.revoke_host_invite("revoked").await.unwrap());
    assert!(!store.revoke_host_invite("revoked").await.unwrap());
    assert_eq!(
        store
            .join_host_by_invite("revoked", &person, proof("revoked-cred", 0))
            .await
            .unwrap(),
        Join::Revoked
    );
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Scope", false))
        .await
        .unwrap();
    store
        .insert_workspace_invite(&guest_invite("workspace-only", &ws, &owner.id))
        .await
        .unwrap();
    assert!(matches!(
        store.revoke_host_invite("workspace-only").await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        store
            .join_host_by_invite("workspace-only", &person, proof("wrong-scope", 0))
            .await
            .unwrap(),
        Join::NotFound
    );
    assert!(store
        .get_workspace_invite("revoked")
        .await
        .unwrap()
        .is_none());
    owner.set_identity(PrincipalIdentity::github(20));
    store.upsert_principal(&owner).await.unwrap();
    store
        .insert_host_invite(&invitation("owner", &owner.id, &person))
        .await
        .unwrap();
    assert_eq!(
        store
            .join_host_by_invite("owner", &person, proof("owner-cred", 0))
            .await
            .unwrap(),
        Join::OwnerSelfJoin
    );
    assert!(matches!(
        store.remove_host_member(&owner.id).await,
        Err(Error::InvalidInput(_))
    ));
    assert!(
        sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?,?)")
            .bind(&owner.id.0)
            .bind(now_iso())
            .execute(store.write_pool())
            .await
            .is_err()
    );
    assert_eq!(
        store.host_membership_state().await.unwrap(),
        HostMembershipState::default()
    );
    assert!(store
        .list_principal_credentials(&owner.id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.count_principals().await.unwrap(), 1);
    let mut invalid = invitation("invalid", &owner.id, &person);
    invalid.pin_login = " ".into();
    assert!(store.insert_host_invite(&invalid).await.is_err());
    invalid.pin_login = "valid".into();
    invalid.expires_at.clone_from(&invalid.created_at);
    assert!(store.insert_host_invite(&invalid).await.is_err());
}

#[tokio::test]
async fn concurrent_redemption_across_independent_pools_has_exactly_one_winner() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let other_store = Store::open(&tmp.path).await.unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let person = guest_identity(42);
    store
        .insert_host_invite(&invitation("race", &owner.id, &person))
        .await
        .unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let mut handles = Vec::new();
    for i in 0..8 {
        let pool = if i % 2 == 0 {
            store.clone()
        } else {
            other_store.clone()
        };
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            pool.join_host_by_invite(
                "race",
                &guest_identity(42),
                proof(&format!("race-token-{i}"), 0),
            )
            .await
            .unwrap()
        }));
    }
    let mut winners = 0;
    for handle in handles {
        match handle.await.unwrap() {
            Join::Joined {
                membership_added: true,
                revision: 1,
                ..
            } => winners += 1,
            Join::Redeemed => {}
            other => panic!("unexpected join outcome: {other:?}"),
        }
    }
    assert_eq!(winners, 1);
    assert_eq!(store.host_membership_state().await.unwrap().member_count, 1);
    assert_eq!(
        store
            .get_host_invite("race")
            .await
            .unwrap()
            .unwrap()
            .redemption_count,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM principal_credential")
            .fetch_one(store.read_pool())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn failed_mint_rolls_back_every_join_write_and_rejects_foreign_or_revoked_bearers() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let other = guest_identity(1);
    store.upsert_principal(&other).await.unwrap();
    store
        .insert_principal_credential(&other.id, "collision")
        .await
        .unwrap();
    let person = guest_identity(2);
    store
        .insert_host_invite(&invitation("rollback", &owner.id, &person))
        .await
        .unwrap();
    assert!(store
        .join_host_by_invite("rollback", &person, proof("collision", 0))
        .await
        .is_err());
    assert!(store
        .find_principal_by_identity(&PrincipalIdentity::github(2))
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_host_invite("rollback")
        .await
        .unwrap()
        .unwrap()
        .is_open_at(&now_iso()));
    assert_eq!(
        store.host_membership_state().await.unwrap(),
        HostMembershipState::default()
    );
    store.upsert_principal(&person).await.unwrap();
    store
        .insert_principal_credential(&person.id, "revoked")
        .await
        .unwrap();
    store.revoke_principal_credential("revoked").await.unwrap();
    for hash in ["collision", "revoked", "unknown"] {
        assert_eq!(
            store
                .join_host_by_invite(
                    "rollback",
                    &person,
                    Credential::Existing { token_hash: hash }
                )
                .await
                .unwrap(),
            Join::CredentialInvalid
        );
    }
    assert!(store
        .insert_host_invite(&invitation("guest-issued", &person.id, &other))
        .await
        .is_err());
    assert_eq!(
        store.host_membership_state().await.unwrap(),
        HostMembershipState::default()
    );
    assert!(store
        .get_host_invite("rollback")
        .await
        .unwrap()
        .unwrap()
        .is_open_at(&now_iso()));
    assert!(store
        .lookup_principal_credential("collision")
        .await
        .unwrap()
        .unwrap()
        .is_active());
    assert!(matches!(
        store
            .join_host_by_invite("rollback", &person, proof("valid", 0))
            .await
            .unwrap(),
        Join::Joined { .. }
    ));
}

#[tokio::test]
async fn removal_preserves_other_people_and_history_and_fences_stale_proofs_across_restart() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let member = guest_identity(42);
    join(&store, "first", &member, "member-one").await;
    store
        .insert_principal_credential(&member.id, "member-two")
        .await
        .unwrap();
    let other = guest_identity(43);
    join(&store, "other-member", &other, "other-token").await;
    let guest = guest_identity(44);
    store.upsert_principal(&guest).await.unwrap();
    store
        .insert_principal_credential(&guest.id, "guest-token")
        .await
        .unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Shared work", false))
        .await
        .unwrap();
    for person in [&member, &other, &guest] {
        store
            .add_workspace_member(&ws, &person.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
    }
    for id in [
        "usable",
        "used-reusable",
        "used-pinned",
        "expired",
        "closed",
        "other-issuer",
    ] {
        let issuer = if id == "other-issuer" {
            &other.id
        } else {
            &member.id
        };
        let mut invite = guest_invite(id, &ws, issuer);
        if id == "used-pinned" {
            invite.pin_github_user_id = Some(44);
        }
        if id == "expired" {
            invite.expires_at = "2020-01-01T00:00:00Z".into();
        }
        store.insert_workspace_invite(&invite).await.unwrap();
        if id.starts_with("used") {
            store.redeem_workspace_invite(id, &guest.id).await.unwrap();
        }
        if id == "closed" {
            store.revoke_workspace_invite(id).await.unwrap();
        }
    }
    let history: Vec<_> = {
        let mut rows = Vec::new();
        for id in ["used-pinned", "expired", "closed", "other-issuer"] {
            rows.push(store.get_workspace_invite(id).await.unwrap().unwrap());
        }
        rows
    };
    store
        .insert_host_invite(&invitation("fresh-invitation", &owner.id, &member))
        .await
        .unwrap();
    let stale_generation = store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    let removal = store.remove_host_member(&member.id).await.unwrap();
    assert!(removal.removed);
    assert_eq!(removal.revision, 3);
    assert_eq!(removal.credentials, 2);
    assert_eq!(removal.workspaces, vec![ws.clone()]);
    assert_eq!(removal.revoked_invites, vec!["usable", "used-reusable"]);
    assert_eq!(
        store.get_host_role(&member.id).await.unwrap(),
        HostRole::Guest
    );
    assert_eq!(
        store.get_host_role(&other.id).await.unwrap(),
        HostRole::Member
    );
    assert!(store
        .get_workspace_member_role(&ws, &guest.id)
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        store.get_workspace_owner_principal_id(&ws).await.unwrap(),
        Some(owner.id)
    );
    for row in history {
        assert_eq!(
            store.get_workspace_invite(&row.id).await.unwrap(),
            Some(row)
        );
    }
    let state = store.host_membership_state().await.unwrap();
    for id in [&member.id, &guest.id, &PrincipalId::from("unknown")] {
        assert!(!store.remove_host_member(id).await.unwrap().removed);
    }
    assert_eq!(store.host_membership_state().await.unwrap(), state);
    store.close().await;
    let store = Store::open(&tmp.path).await.unwrap();
    assert_eq!(store.host_membership_state().await.unwrap(), state);
    assert_eq!(
        store
            .join_host_by_invite(
                "fresh-invitation",
                &member,
                proof("stale-proof", stale_generation)
            )
            .await
            .unwrap(),
        Join::AccessRevoked
    );
    assert_eq!(
        store
            .join_host_by_invite(
                "fresh-invitation",
                &member,
                Credential::Existing {
                    token_hash: "member-one"
                }
            )
            .await
            .unwrap(),
        Join::CredentialInvalid
    );
    for hash in ["member-one", "member-two"] {
        assert!(store
            .resolve_active_principal_credential(hash)
            .await
            .unwrap()
            .is_none());
    }
    for hash in ["other-token", "guest-token"] {
        assert!(store
            .resolve_active_principal_credential(hash)
            .await
            .unwrap()
            .is_some());
    }
    assert!(matches!(
        store
            .join_host_by_invite(
                "fresh-invitation",
                &member,
                proof("fresh-proof", state.authorization_generation)
            )
            .await
            .unwrap(),
        Join::Joined {
            membership_added: true,
            revision: 4,
            ..
        }
    ));
    assert!(
        store
            .get_workspace_member_role(&ws, &member.id)
            .await
            .unwrap()
            .is_none(),
        "fresh host grant has no direct workspace fanout"
    );
}

#[tokio::test]
async fn removal_failure_rolls_back_membership_revision_generation_and_credentials() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let member = guest_identity(42);
    assert!(matches!(
        join(&store, "joined", &member, "member-token").await,
        Join::Joined { .. }
    ));
    let before = store.host_membership_state().await.unwrap();
    sqlx::query("CREATE TRIGGER fail_revoke BEFORE UPDATE ON principal_credential BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
        .execute(store.write_pool()).await.unwrap();
    assert!(store.remove_host_member(&member.id).await.is_err());
    assert_eq!(store.host_membership_state().await.unwrap(), before);
    assert_eq!(
        store.get_host_role(&member.id).await.unwrap(),
        HostRole::Member
    );
    assert!(store
        .lookup_principal_credential("member-token")
        .await
        .unwrap()
        .unwrap()
        .is_active());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM principal_revocation")
            .fetch_one(store.read_pool())
            .await
            .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_revoke")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(store.remove_host_member(&member.id).await.unwrap().removed);
}

#[tokio::test]
async fn redemption_racing_removal_cannot_resurrect_membership_or_credentials() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let other_store = Store::open(&tmp.path).await.unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    for i in 0..8 {
        let member = guest_identity(100 + i);
        assert!(matches!(
            join(
                &store,
                &format!("first-{i}"),
                &member,
                &format!("first-token-{i}")
            )
            .await,
            Join::Joined { .. }
        ));
        let invite_id = format!("race-removal-{i}");
        store
            .insert_host_invite(&invitation(&invite_id, &owner.id, &member))
            .await
            .unwrap();
        let generation = store
            .host_membership_state()
            .await
            .unwrap()
            .authorization_generation;
        let barrier = Arc::new(Barrier::new(2));
        let join_task = {
            let store = other_store.clone();
            let barrier = barrier.clone();
            let member = member.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                store
                    .join_host_by_invite(
                        &invite_id,
                        &member,
                        proof(&format!("racing-token-{i}"), generation),
                    )
                    .await
                    .unwrap()
            })
        };
        barrier.wait().await;
        assert!(store.remove_host_member(&member.id).await.unwrap().removed);
        assert!(matches!(
            join_task.await.unwrap(),
            Join::Joined {
                membership_added: false,
                ..
            } | Join::AccessRevoked
        ));
        assert_eq!(
            store.get_host_role(&member.id).await.unwrap(),
            HostRole::Guest
        );
        assert!(store
            .resolve_active_principal_credential(&format!("racing-token-{i}"))
            .await
            .unwrap()
            .is_none());
        assert_eq!(store.host_membership_state().await.unwrap().member_count, 0);
    }
}

#[tokio::test]
async fn open_invite_list_preserves_nanosecond_expiry_and_orders_equal_creation_times() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    for id in ["b", "a"] {
        let mut row = invitation(id, &owner.id, &guest_identity(42));
        row.created_at = "2026-01-01T00:00:00.000000001Z".into();
        row.expires_at = "2026-01-08T00:00:00.000000001Z".into();
        store.insert_host_invite(&row).await.unwrap();
    }
    assert_eq!(
        store
            .list_open_host_invites_at("2026-01-08T00:00:00Z")
            .await
            .unwrap()
            .iter()
            .map(|i| i.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert!(store
        .list_open_host_invites_at("2026-01-08T00:00:00.000000001Z")
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .list_open_host_invites_at("2026-01-08T00:00:00.000000002Z")
        .await
        .unwrap()
        .is_empty());
}
