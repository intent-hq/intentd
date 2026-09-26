use super::tests::{fixture, guest, id_of, invite_kind, nonce_of, wire, with_forge};
use super::*;
use crate::tests::TempDb;
use intent_core::with_caller;

async fn create(f: &super::tests::Fixture, login: &str) -> Value {
    with_caller(
        Caller::Daemon,
        f.services.host_invite_create_op(InvitePin {
            login: login.into(),
            provider: Some("github".into()),
            host: None,
        }),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn host_invitation_owner_administration_and_scope_privacy() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    let owner = f.store.get_primary_principal().await.unwrap();
    assert!(owner.identity_key().is_none());
    let row = create(&f, "guest").await;
    let id = id_of(&row);
    let secret = row["secret"].as_str().unwrap();
    assert_eq!(row["invite"]["scope"], "host");
    assert_eq!(row["invite"]["role"], "member");
    assert_eq!(row["invite"]["reusable"], false);
    assert!(row["invite"].get("secret").is_none());
    let created = intent_core::parse_iso(row["invite"]["createdAt"].as_str().unwrap()).unwrap();
    let expires = intent_core::parse_iso(row["invite"]["expiresAt"].as_str().unwrap()).unwrap();
    assert_eq!(
        (expires - created).whole_seconds(),
        HostInvite::LIFETIME_SECONDS
    );
    let preview = f
        .services
        .invite_inspect_op(&id, secret, InviteScope::Host)
        .await
        .unwrap();
    assert_eq!(
        preview,
        json!({"scope":"host","role":"member","pinIdentity":PrincipalIdentity::github(4242)})
    );
    assert_eq!(
        invite_kind(
            &f.services
                .invite_inspect_op(&id, "wrong", InviteScope::Workspace)
                .await
        ),
        InviteErrorKind::NotFound
    );
    assert_eq!(
        invite_kind(
            &f.services
                .invite_inspect_op(&id, secret, InviteScope::Workspace)
                .await
        ),
        InviteErrorKind::ScopeMismatch
    );
    assert_eq!(
        invite_kind(
            &f.services
                .invite_challenge_op(&id, secret, InviteScope::Workspace)
                .await
        ),
        InviteErrorKind::ScopeMismatch
    );
    assert!(f.services.invite_nonces.lock().await.is_empty());
    let members = with_caller(Caller::Daemon, f.services.host_members_list_op())
        .await
        .unwrap();
    assert_eq!(
        members,
        json!({"members":[{"principalId":owner.id,"hostRole":"owner","login":null,"displayName":null,"avatarUrl":null,"addedAt":owner.created_at}],"revision":0})
    );
    for role in [HostRole::Guest, HostRole::Member] {
        let caller = Caller::Wire {
            principal_id: f.collaborator.clone(),
            host_role: role,
        };
        for r in [
            with_caller(caller.clone(), f.services.host_members_list_op()).await,
            with_caller(caller.clone(), f.services.host_invite_list_op()).await,
            with_caller(caller.clone(), f.services.host_invite_revoke_op(&id)).await,
            with_caller(
                caller,
                f.services.host_invite_create_op(InvitePin::login("guest")),
            )
            .await,
        ] {
            assert!(matches!(r, Err(Error::Forbidden(_))), "{r:?}");
        }
    }
    assert_eq!(
        with_caller(Caller::Daemon, f.services.host_invite_list_op())
            .await
            .unwrap()["invites"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        with_caller(Caller::Daemon, f.services.host_invite_revoke_op(&id))
            .await
            .unwrap(),
        json!({"revoked":true})
    );
    assert_eq!(
        with_caller(Caller::Daemon, f.services.host_invite_revoke_op(&id))
            .await
            .unwrap(),
        json!({"revoked":false})
    );
    assert_eq!(
        invite_kind(
            &f.services
                .invite_inspect_op(&id, secret, InviteScope::Workspace)
                .await
        ),
        InviteErrorKind::Revoked
    );
    assert_eq!(f.store.host_membership_state().await.unwrap().revision, 0);
}

#[tokio::test]
async fn host_proof_upgrades_existing_guest_without_rotating_other_credentials() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    let (person, old_token) = f.first_join(&guest("guest", 4242)).await;
    let invitation = create(&f, "guest").await;
    let id = id_of(&invitation);
    let secret = invitation["secret"].as_str().unwrap();
    let challenge = f
        .services
        .invite_challenge_op(&id, secret, InviteScope::Host)
        .await
        .unwrap();
    let nonce = nonce_of(&challenge);
    with_forge(
        &mut f,
        vec![("hostproof", Ok(super::tests::gist("guest", Some(&nonce))))],
    );
    let joined = f
        .services
        .invite_prove_op(
            &id,
            secret,
            &nonce,
            InviteProofClaim::github("hostproof", "guest"),
            InviteScope::Host,
        )
        .await
        .unwrap();
    assert_eq!(joined["principalId"], person.0);
    assert_eq!(joined["hostRole"], "member");
    assert_eq!(joined["scope"], "host");
    assert!(joined.get("workspaceId").is_none());
    assert_eq!(joined["identity"], json!(PrincipalIdentity::github(4242)));
    assert_eq!(
        f.store
            .resolve_active_principal_credential(&hash_secret(&old_token))
            .await
            .unwrap(),
        Some(person.clone())
    );
    assert_eq!(
        f.store
            .get_workspace_member_role(&f.ws, &person)
            .await
            .unwrap(),
        Some(WorkspaceRole::Collaborator)
    );
    let second = create(&f, "guest").await;
    let accepted = f
        .services
        .invite_accept_op(
            &id_of(&second),
            second["secret"].as_str().unwrap(),
            &old_token,
            InviteScope::Host,
        )
        .await
        .unwrap();
    assert_eq!(accepted["token"], old_token);
    assert_eq!(f.store.host_membership_state().await.unwrap().revision, 1);
    assert_eq!(
        f.store
            .list_principal_credentials(&person)
            .await
            .unwrap()
            .len(),
        2
    );
    let fresh = crate::tests::workspace(&WorkspaceId::new());
    f.store.insert_workspace(&fresh).await.unwrap();
    let ws_invite = with_caller(
        wire(&person),
        f.services.workspace_invite_create_op(&fresh.id, None, None),
    )
    .await
    .unwrap();
    let joined = f
        .services
        .invite_accept_op(
            &id_of(&ws_invite),
            ws_invite["secret"].as_str().unwrap(),
            &old_token,
            InviteScope::Workspace,
        )
        .await
        .unwrap();
    assert_eq!(joined["hostRole"], "member");
    assert_eq!(joined["token"], old_token);
    assert_eq!(
        f.store
            .get_workspace_member_role(&fresh.id, &person)
            .await
            .unwrap(),
        None,
        "effective members need no direct grant"
    );
    let reopened = intent_store::Store::open(&tmp.path).await.unwrap();
    assert_eq!(
        reopened.get_host_role(&person).await.unwrap(),
        HostRole::Member
    );
    assert_eq!(reopened.list_host_members().await.unwrap().members.len(), 2);
}

#[tokio::test]
async fn host_single_use_accept_race_has_one_winner_and_preserves_bearer() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    let (person, token) = f.first_join(&guest("guest", 4242)).await;
    let row = create(&f, "guest").await;
    let id = id_of(&row);
    let secret = row["secret"].as_str().unwrap();
    let (a, b) = tokio::join!(
        f.services
            .invite_accept_op(&id, secret, &token, InviteScope::Host),
        f.services
            .invite_accept_op(&id, secret, &token, InviteScope::Host)
    );
    let (winner, loser) = if a.is_ok() { (a, b) } else { (b, a) };
    assert_eq!(winner.unwrap()["token"], token);
    assert_eq!(invite_kind(&loser), InviteErrorKind::Redeemed);
    assert_eq!(
        f.store
            .list_principal_credentials(&person)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        f.store.host_membership_state().await.unwrap().member_count,
        1
    );
}

#[tokio::test]
async fn host_pins_refuse_wrong_triples_and_invalid_credentials_without_writes() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    let row = create(&f, "guest").await;
    let id = id_of(&row);
    let secret = row["secret"].as_str().unwrap();
    for other in [
        guest("other", 4343),
        super::tests::gitlab_guest("gitlab.com", "guest", 4242),
        super::tests::gitlab_guest("other.example", "guest", 4242),
    ] {
        assert_eq!(
            invite_kind(
                &f.services
                    .complete_invite_join(&id, &other, InviteScope::Host, 0)
                    .await
            ),
            InviteErrorKind::PinMismatch
        );
    }
    assert_eq!(
        invite_kind(
            &f.services
                .invite_accept_op(&id, secret, "invalid", InviteScope::Host)
                .await
        ),
        InviteErrorKind::CredentialInvalid
    );
    assert_eq!(
        f.store.host_membership_state().await.unwrap().member_count,
        0
    );
    assert!(f
        .store
        .find_principal_by_identity(&PrincipalIdentity::github(4242))
        .await
        .unwrap()
        .is_none());
    assert!(f
        .store
        .get_host_invite(&id)
        .await
        .unwrap()
        .unwrap()
        .redeemed_at
        .is_none());
}

#[tokio::test]
async fn proof_challenge_generation_prevents_removal_from_resurrecting_either_scope() {
    for scope in [InviteScope::Host, InviteScope::Workspace] {
        let tmp = TempDb::new();
        let mut f = fixture(&tmp).await;
        with_forge(&mut f, vec![]);
        let joined = create(&f, "guest").await;
        let person = f
            .services
            .complete_invite_join(&id_of(&joined), &guest("guest", 4242), InviteScope::Host, 0)
            .await
            .unwrap();
        let person = PrincipalId(person["principalId"].as_str().unwrap().into());
        let row = if scope == InviteScope::Host {
            create(&f, "guest").await
        } else {
            f.create_invite(None).await
        };
        let id = id_of(&row);
        let secret = row["secret"].as_str().unwrap();
        let challenge = f
            .services
            .invite_challenge_op(&id, secret, scope)
            .await
            .unwrap();
        let nonce = nonce_of(&challenge);
        with_forge(
            &mut f,
            vec![("stale", Ok(super::tests::gist("guest", Some(&nonce))))],
        );
        f.store.remove_host_member(&person).await.unwrap();
        let result = f
            .services
            .invite_prove_op(
                &id,
                secret,
                &nonce,
                InviteProofClaim::github("stale", "guest"),
                scope,
            )
            .await;
        assert_eq!(invite_kind(&result), InviteErrorKind::AccessRevoked);
        assert_eq!(
            f.store.get_host_role(&person).await.unwrap(),
            HostRole::Guest
        );
        assert!(f
            .store
            .list_principal_credentials(&person)
            .await
            .unwrap()
            .iter()
            .all(|c| !c.is_active()));
        assert!(f
            .services
            .stored_invite(&id)
            .await
            .unwrap()
            .check_open(scope)
            .is_ok());
    }
}

#[tokio::test]
async fn workspace_pin_without_primary_identity_requires_explicit_provider() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    let result = with_caller(
        wire(&f.owner),
        f.services
            .workspace_invite_create_op(&f.ws, Some(InvitePin::login("guest")), None),
    )
    .await;
    assert!(matches!(result, Err(Error::InvalidParams(_))), "{result:?}");
    let row = with_caller(
        wire(&f.owner),
        f.services.workspace_invite_create_op(
            &f.ws,
            Some(InvitePin {
                login: "guest".into(),
                provider: Some("github".into()),
                host: None,
            }),
            None,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        row["invite"]["pinIdentity"],
        json!(PrincipalIdentity::github(4242))
    );
    let mut owner = f.store.get_principal(&f.primary).await.unwrap();
    owner.set_identity(PrincipalIdentity::github(101));
    f.store.upsert_principal(&owner).await.unwrap();
    assert!(with_caller(
        wire(&f.owner),
        f.services
            .workspace_invite_create_op(&f.ws, Some(InvitePin::login("guest")), None)
    )
    .await
    .is_ok());
}

#[tokio::test]
async fn host_expired_revoked_and_replayed_links_never_mint_credentials() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    for (name, kind) in [
        ("expired", InviteErrorKind::Expired),
        ("revoked", InviteErrorKind::Revoked),
        ("replayed", InviteErrorKind::Redeemed),
    ] {
        let row = create(&f, "guest").await;
        let id = id_of(&row);
        let secret = row["secret"].as_str().unwrap();
        match name {
            "expired" => {
                sqlx::query("UPDATE host_invite SET created_at = '2000-01-01T00:00:00Z', expires_at = '2000-01-08T00:00:00Z' WHERE id = ?").bind(&id).execute(f.store.write_pool()).await.unwrap();
            }
            "revoked" => {
                f.store.revoke_host_invite(&id).await.unwrap();
            }
            _ => {
                f.services
                    .complete_invite_join(&id, &guest("guest", 4242), InviteScope::Host, 0)
                    .await
                    .unwrap();
            }
        }
        let before = f.store.list_principals().await.unwrap();
        let state = f.store.host_membership_state().await.unwrap();
        assert_eq!(
            invite_kind(
                &f.services
                    .invite_inspect_op(&id, secret, InviteScope::Host)
                    .await
            ),
            kind
        );
        assert_eq!(
            invite_kind(
                &f.services
                    .invite_challenge_op(&id, secret, InviteScope::Host)
                    .await
            ),
            kind
        );
        assert_eq!(
            invite_kind(
                &f.services
                    .complete_invite_join(&id, &guest("guest", 4242), InviteScope::Host, 0)
                    .await
            ),
            kind
        );
        assert_eq!(f.store.list_principals().await.unwrap(), before);
        assert_eq!(f.store.host_membership_state().await.unwrap(), state);
    }
}

#[tokio::test]
async fn revoked_host_issuer_cannot_create_a_workspace_invitation() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    let row = create(&f, "guest").await;
    let join = f
        .services
        .complete_invite_join(&id_of(&row), &guest("guest", 4242), InviteScope::Host, 0)
        .await
        .unwrap();
    let person = PrincipalId(join["principalId"].as_str().unwrap().into());
    // A removed host member never remains a legacy workspace owner in this fixture.
    let staged = f
        .store
        .get_workspace_invite(&id_of(&f.create_invite(None).await))
        .await
        .unwrap()
        .unwrap();
    let mut staged = staged;
    staged.id = "after-removal".into();
    staged.created_by_principal_id = person.clone();
    f.store.remove_host_member(&person).await.unwrap();
    assert_eq!(
        f.store.insert_workspace_invite(&staged).await.unwrap(),
        InviteInsertOutcome::IssuerForbidden
    );
    assert!(f
        .store
        .get_workspace_invite(&staged.id)
        .await
        .unwrap()
        .is_none());
}
