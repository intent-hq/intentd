//! Sharing requires a connected owner forge: with neither GitHub nor GitLab
//! connected on the host, no invite is minted (open or pinned, workspace or
//! host scope) and no open invite can be redeemed. Existing guests keep
//! their access, and the unused invites work again once a forge reconnects.

use super::tests::{fixture, gist, guest, id_of, invite_kind, nonce_of, wire, with_forge, Fixture};
use super::*;
use crate::tests::pr::StubForge;
use crate::tests::TempDb;
use intent_core::with_caller;

/// The host's GitHub credential is rejected and no GitLab instance is
/// connected (the fixture's GitLab secret store is an empty scratch file).
fn disconnect(f: &mut Fixture) {
    f.services = f
        .services
        .clone()
        .with_source_control(Arc::new(StubForge::unauthenticated()));
}

fn pin(login: &str) -> InvitePin {
    InvitePin {
        login: login.into(),
        provider: Some("github".into()),
        host: None,
    }
}

#[tokio::test]
async fn workspace_invite_create_refuses_open_and_pinned_without_a_forge() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    disconnect(&mut f);
    for pin in [None, Some(pin("guest"))] {
        let r = with_caller(
            wire(&f.owner),
            f.services.workspace_invite_create_op(&f.ws, pin, None),
        )
        .await;
        assert_eq!(invite_kind(&r), InviteErrorKind::GithubIdentityRequired);
    }
    let r = with_caller(
        Caller::Daemon,
        f.services.workspace_invite_create_op(&f.ws, None, None),
    )
    .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubIdentityRequired);
    assert_eq!(f.store.count_open_workspace_invites().await.unwrap(), 0);
}

/// A cached primary identity is not a connection: the live credential is.
#[tokio::test]
async fn cached_primary_identity_without_a_live_credential_cannot_invite() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    let mut primary = f.store.get_primary_principal().await.unwrap();
    primary.github_user_id = Some(10);
    f.store.upsert_principal(&primary).await.unwrap();
    disconnect(&mut f);
    let r = with_caller(
        Caller::Daemon,
        f.services.workspace_invite_create_op(&f.ws, None, None),
    )
    .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubIdentityRequired);
    assert_eq!(f.store.count_open_workspace_invites().await.unwrap(), 0);
}

#[tokio::test]
async fn host_invite_create_refuses_without_a_forge() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    disconnect(&mut f);
    let r = with_caller(
        Caller::Daemon,
        f.services.host_invite_create_op(pin("guest")),
    )
    .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubIdentityRequired);
    assert!(f.store.list_open_host_invites().await.unwrap().is_empty());
}

#[tokio::test]
async fn forge_connected_invites_are_unchanged() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    for pin in [None, Some(pin("guest"))] {
        with_caller(
            wire(&f.owner),
            f.services.workspace_invite_create_op(&f.ws, pin, None),
        )
        .await
        .expect("connected owner mints");
    }
    with_caller(
        Caller::Daemon,
        f.services.host_invite_create_op(pin("guest")),
    )
    .await
    .expect("connected owner mints a host invite");
}

/// An invite minted while connected cannot be redeemed after the forge is
/// disconnected — challenge, prove and accept all refuse and leave it open —
/// while the guest who already joined keeps access. Reconnecting reopens it.
#[tokio::test]
async fn disconnect_blocks_redemption_but_keeps_existing_guests() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    let (member, token) = f.first_join(&guest("returning", 4242)).await;
    let created = f.create_invite(None).await;
    let (invite_id, secret) = (
        id_of(&created),
        created["secret"].as_str().unwrap().to_string(),
    );
    let nonce = nonce_of(
        &f.services
            .invite_challenge_op(&invite_id, &secret, InviteScope::Workspace)
            .await
            .expect("challenge while connected"),
    );

    let open_before = f.store.count_open_workspace_invites().await.unwrap();
    disconnect(&mut f);
    let r = f
        .services
        .invite_challenge_op(&invite_id, &secret, InviteScope::Workspace)
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubIdentityRequired);
    let r = f
        .services
        .invite_prove_op(
            &invite_id,
            &secret,
            &nonce,
            InviteProofClaim::github("g1", "guest"),
            InviteScope::Workspace,
        )
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubIdentityRequired);
    let r = f
        .services
        .invite_accept_op(&invite_id, &secret, &token, InviteScope::Workspace)
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubIdentityRequired);

    assert_eq!(
        f.store.count_open_workspace_invites().await.unwrap(),
        open_before,
        "refused redemptions leave the invites open"
    );
    assert_eq!(
        f.store
            .get_workspace_member_role(&f.ws, &member)
            .await
            .unwrap(),
        Some(WorkspaceRole::Collaborator),
        "existing guests keep access"
    );
    assert!(f
        .store
        .resolve_active_principal_credential(&hash_secret(&token))
        .await
        .unwrap()
        .is_some());

    with_forge(&mut f, vec![("g1", Ok(gist("other", Some(&nonce))))]);
    f.services
        .invite_challenge_op(&invite_id, &secret, InviteScope::Workspace)
        .await
        .expect("reconnected forge reopens redemption");
    let joined = f
        .services
        .invite_prove_op(
            &invite_id,
            &secret,
            &nonce,
            InviteProofClaim::github("g1", "other"),
            InviteScope::Workspace,
        )
        .await
        .expect("the nonce a refused prove presented is still redeemable");
    assert_eq!(joined["status"], json!("authorized"));
    assert_eq!(joined["login"], json!("other"));
}

/// The owner-forge check runs only after the invite and secret validate:
/// a wrong secret is still `NotFound` and never reaches a forge.
#[tokio::test]
async fn wrong_secret_is_not_found_without_a_forge_call() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    let invite_id = id_of(&f.create_invite(None).await);
    let forge = Arc::new(StubForge::unauthenticated());
    f.services = f.services.clone().with_source_control(forge.clone());
    let r = f
        .services
        .invite_challenge_op(&invite_id, "wrong", InviteScope::Workspace)
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::NotFound);
    let r = f
        .services
        .invite_prove_op(
            &invite_id,
            "wrong",
            "nonce",
            InviteProofClaim::github("g1", "guest"),
            InviteScope::Workspace,
        )
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::NotFound);
    assert_eq!(
        forge
            .get_user_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

/// Host-scope invites are gated the same way: minted while connected, they
/// cannot be challenged once the owner's forge is disconnected.
#[tokio::test]
async fn host_invite_redemption_refuses_without_a_forge() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    let created = with_caller(
        Caller::Daemon,
        f.services.host_invite_create_op(pin("guest")),
    )
    .await
    .expect("connected owner mints a host invite");
    let (invite_id, secret) = (
        id_of(&created),
        created["secret"].as_str().unwrap().to_string(),
    );
    disconnect(&mut f);
    let r = f
        .services
        .invite_challenge_op(&invite_id, &secret, InviteScope::Host)
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubIdentityRequired);
    assert_eq!(f.store.list_open_host_invites().await.unwrap().len(), 1);
}
