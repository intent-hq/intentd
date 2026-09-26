use std::future::Future;

use super::tests::{fixture, guest, id_of, invite_kind, wire, with_forge, Fixture};
use super::*;
use crate::tests::TempDb;
use intent_core::{with_caller, AgentId, WorkspaceApi};

async fn member(f: &mut Fixture) -> (PrincipalId, String) {
    with_forge(f, vec![]);
    let (person, token) = f.first_join(&guest("guest", 4242)).await;
    let invite = with_caller(
        Caller::Daemon,
        f.services.host_invite_create_op(InvitePin {
            login: "guest".into(),
            provider: Some("github".into()),
            host: None,
        }),
    )
    .await
    .unwrap();
    let joined = f
        .services
        .invite_accept_op(
            &id_of(&invite),
            invite["secret"].as_str().unwrap(),
            &token,
            InviteScope::Host,
        )
        .await
        .unwrap();
    assert_eq!(joined["hostRole"], "member");
    (person, token)
}

async fn issue(f: &Fixture, issuer: &PrincipalId, pinned: bool) -> Value {
    with_caller(
        wire(issuer),
        f.services.workspace_invite_create_op(
            &f.ws,
            pinned.then(|| InvitePin {
                login: "guest".into(),
                provider: Some("github".into()),
                host: None,
            }),
            None,
        ),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn member_removal_real_service_sweeps_usable_links_preserves_history_and_guests() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    f.services = f
        .services
        .with_event_bus(crate::events::EventBus::new(f.store.clone()));
    let (person, token) = member(&mut f).await;
    let unused = issue(&f, &person, false).await;
    let pinned = issue(&f, &person, true).await;
    let used = issue(&f, &person, false).await;
    let admitted = f
        .services
        .complete_invite_join(
            &id_of(&used),
            &guest("earlier", 7001),
            InviteScope::Workspace,
            0,
        )
        .await
        .unwrap();
    let earlier = PrincipalId(admitted["principalId"].as_str().unwrap().into());
    let expired = issue(&f, &person, false).await;
    sqlx::query("UPDATE workspace_invite SET expires_at = '2000-01-01T00:00:00Z' WHERE id = ?")
        .bind(id_of(&expired))
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let closed = issue(&f, &person, true).await;
    f.services
        .invite_accept_op(
            &id_of(&closed),
            closed["secret"].as_str().unwrap(),
            &token,
            InviteScope::Workspace,
        )
        .await
        .unwrap();
    let revoked = issue(&f, &person, false).await;
    with_caller(
        wire(&person),
        f.services
            .workspace_invite_revoke_op(&f.ws, &id_of(&revoked)),
    )
    .await
    .unwrap();
    // Retained direct grants on every workspace must also be removed.
    let extra_workspace = WorkspaceId::new();
    f.store
        .insert_workspace(&crate::tests::workspace(&extra_workspace))
        .await
        .unwrap();
    f.store
        .add_workspace_member(&extra_workspace, &person, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let other = issue(&f, &f.owner, false).await;
    let history = [id_of(&expired), id_of(&closed), id_of(&revoked)];
    let mut before = Vec::new();
    for id in &history {
        before.push(f.store.get_workspace_invite(id).await.unwrap());
    }
    let reserved_before = f.store.count_workspace_guests(&f.ws).await.unwrap();
    let mut revocations = f.services.subscribe_principal_revocations().unwrap();
    let removed = with_caller(
        Caller::Daemon,
        f.services.host_members_remove(person.clone()),
    )
    .await
    .unwrap();
    assert_eq!(removed, json!({"removed":true}));
    assert_eq!(revocations.try_recv().unwrap().principal_id, person);
    let reserved_after = f.store.count_workspace_guests(&f.ws).await.unwrap();
    assert_eq!(
        reserved_before.open_invites - reserved_after.open_invites,
        3
    );
    assert_eq!(reserved_before.collaborators, reserved_after.collaborators);
    for row in [&unused, &pinned, &used] {
        assert_eq!(
            invite_kind(
                &f.services
                    .invite_inspect_op(
                        &id_of(row),
                        row["secret"].as_str().unwrap(),
                        InviteScope::Workspace
                    )
                    .await
            ),
            InviteErrorKind::Revoked
        );
    }
    for (id, old) in history.iter().zip(before) {
        let new = f.store.get_workspace_invite(id).await.unwrap();
        assert_eq!(
            serde_json::to_value(old).unwrap(),
            serde_json::to_value(new).unwrap()
        );
    }
    assert!(f
        .services
        .invite_inspect_op(
            &id_of(&other),
            other["secret"].as_str().unwrap(),
            InviteScope::Workspace
        )
        .await
        .is_ok());
    assert_eq!(
        f.store
            .get_workspace_member_role(&f.ws, &earlier)
            .await
            .unwrap(),
        Some(WorkspaceRole::Collaborator)
    );
    assert!(f
        .store
        .resolve_active_principal_credential(&hash_secret(admitted["token"].as_str().unwrap()))
        .await
        .unwrap()
        .is_some());
    assert!(f
        .store
        .resolve_active_principal_credential(&hash_secret(&token))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        f.store.get_host_role(&person).await.unwrap(),
        HostRole::Guest
    );
    assert_eq!(
        f.store
            .get_workspace_member_role(&f.ws, &person)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        f.store
            .get_workspace_member_role(&extra_workspace, &person)
            .await
            .unwrap(),
        None
    );
    assert!(f.store.get_workspace(&extra_workspace).await.is_ok());
    assert!(f.store.get_workspace(&f.ws).await.is_ok());
    let events = f
        .store
        .query_events(&intent_store::EventQuery {
            event_types: vec![intent_core::events::HOST_MEMBERS_CHANGED.into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(events
        .iter()
        .any(|e| e.data["principalId"] == person.0 && e.data["action"] == "removed"));
    assert_eq!(
        with_caller(
            Caller::Daemon,
            f.services.host_members_remove(person.clone())
        )
        .await
        .unwrap(),
        json!({"removed":false})
    );
    assert!(revocations.try_recv().is_err());
    // A fresh proof on somebody else's invitation grants only that workspace.
    let generation = f
        .store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    let joined = f
        .services
        .complete_invite_join(
            &id_of(&other),
            &guest("guest", 4242),
            InviteScope::Workspace,
            generation,
        )
        .await
        .unwrap();
    assert_eq!(joined["principalId"], person.0);
    assert_eq!(joined["hostRole"], "guest");
    assert!(f
        .store
        .resolve_active_principal_credential(&hash_secret(&token))
        .await
        .unwrap()
        .is_none());
    // A stale client profile with a newer clock has no bearer authority.
    let mut stale = f.store.get_principal(&person).await.unwrap();
    stale.updated_at = "2099-01-01T00:00:00Z".into();
    f.store.upsert_principal(&stale).await.unwrap();
    assert!(f
        .store
        .insert_principal_credential(&person, &hash_secret(&token))
        .await
        .is_err());
    let reopened = intent_store::Store::open(&tmp.path).await.unwrap();
    assert!(reopened
        .resolve_active_principal_credential(&hash_secret(&token))
        .await
        .unwrap()
        .is_none());
    assert!(reopened
        .get_workspace_invite(&id_of(&used))
        .await
        .unwrap()
        .unwrap()
        .revoked_at
        .is_some());
}

#[tokio::test]
async fn member_removal_authority_rollback_and_noop_publish_nothing() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    let (person, token) = member(&mut f).await;
    let mut feed = f.services.subscribe_principal_revocations().unwrap();
    for caller in [
        wire(&person),
        Caller::Wire {
            principal_id: person.clone(),
            host_role: HostRole::Owner,
        },
        Caller::Agent {
            agent_id: AgentId::new(),
        },
    ] {
        assert!(matches!(
            with_caller(caller, f.services.host_members_remove(person.clone())).await,
            Err(Error::Forbidden(_))
        ));
    }
    assert!(matches!(
        with_caller(
            Caller::Daemon,
            f.services.host_members_remove(f.primary.clone())
        )
        .await,
        Err(Error::InvalidInput(_))
    ));
    assert_eq!(
        with_caller(
            Caller::Daemon,
            f.services.host_members_remove(f.collaborator.clone())
        )
        .await
        .unwrap(),
        json!({"removed":false})
    );
    let before = f.store.host_membership_state().await.unwrap();
    sqlx::query("CREATE TRIGGER fail_removal BEFORE UPDATE OF revoked_at ON principal_credential BEGIN SELECT RAISE(ABORT,'removal test rollback'); END").execute(f.store.write_pool()).await.unwrap();
    assert!(with_caller(
        Caller::Daemon,
        f.services.host_members_remove(person.clone())
    )
    .await
    .is_err());
    assert_eq!(f.store.host_membership_state().await.unwrap(), before);
    assert_eq!(
        f.store.get_host_role(&person).await.unwrap(),
        HostRole::Member
    );
    assert!(f
        .store
        .resolve_active_principal_credential(&hash_secret(&token))
        .await
        .unwrap()
        .is_some());
    assert!(feed.try_recv().is_err());
}

#[tokio::test]
async fn member_removal_stale_creation_and_redemption_cannot_reopen_access() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    let (person, token) = member(&mut f).await;
    let link = issue(&f, &person, false).await;
    let future_link = issue(&f, &f.owner, false).await;
    let generation = f
        .store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    // Stop a real create after the initial role check, before its insert.
    let (reached, release) = f.services.invite_create_commit_pause.arm();
    let services = f.services.clone();
    let workspace = f.ws.clone();
    let issuer = person.clone();
    let create = tokio::spawn(with_caller(wire(&issuer), async move {
        services
            .workspace_invite_create_op(&workspace, None, None)
            .await
    }));
    reached.await.unwrap();
    with_caller(
        Caller::Daemon,
        f.services.host_members_remove(person.clone()),
    )
    .await
    .unwrap();
    release.send(()).unwrap();
    assert!(create.await.unwrap().is_err());
    assert_eq!(
        invite_kind(
            &f.services
                .invite_accept_op(
                    &id_of(&link),
                    link["secret"].as_str().unwrap(),
                    &token,
                    InviteScope::Workspace
                )
                .await
        ),
        InviteErrorKind::CredentialInvalid
    );
    assert_eq!(
        invite_kind(
            &f.services
                .complete_invite_join(
                    &id_of(&future_link),
                    &guest("guest", 4242),
                    InviteScope::Workspace,
                    generation
                )
                .await
        ),
        InviteErrorKind::AccessRevoked
    );
    assert!(with_caller(
        wire(&person),
        f.services.workspace_invite_create_op(&f.ws, None, None)
    )
    .await
    .is_err());
}

#[tokio::test]
async fn member_removal_serializes_human_queue_admission_and_preserves_automation() {
    for removal_first in [false, true] {
        let tmp = TempDb::new();
        let mut f = fixture(&tmp).await;
        let (person, _) = member(&mut f).await;
        let id = AgentId::new();
        let session:intent_core::AgentSession=serde_json::from_value(json!({"id":id,"workspaceId":f.ws,"name":"Running work","status":"active","createdAt":now_iso(),"updatedAt":now_iso()})).unwrap();
        f.store.insert_agent_session(&session).await.unwrap();
        with_caller(
            Caller::Daemon,
            f.services.agent_queue_message(
                id.clone(),
                "automation survives".into(),
                None,
                None,
                Some(json!({"source":"system","fromPrincipalId":person})),
            ),
        )
        .await
        .unwrap();
        let queue = with_caller(
            wire(&person),
            f.services.agent_queue_message(
                id.clone(),
                "removed human".into(),
                None,
                None,
                Some(json!({"source":"system","fromPrincipalId":f.primary})),
            ),
        );
        tokio::pin!(queue);
        if removal_first {
            let (reached, release) = f.services.member_removal_commit_pause.arm();
            let services = f.services.clone();
            let target = person.clone();
            let remove = tokio::spawn(with_caller(Caller::Daemon, async move {
                services.host_members_remove(target).await
            }));
            reached.await.unwrap();
            let pending =
                std::future::poll_fn(|cx| std::task::Poll::Ready(queue.as_mut().poll(cx))).await;
            assert!(pending.is_pending());
            release.send(()).unwrap();
            remove.await.unwrap().unwrap();
            assert!(queue.await.is_err());
        } else {
            queue.await.unwrap();
            with_caller(
                Caller::Daemon,
                f.services.host_members_remove(person.clone()),
            )
            .await
            .unwrap();
        }
        let queue = with_caller(
            Caller::Daemon,
            f.services.agent_get_queue(id.clone(), Some(f.ws.clone())),
        )
        .await
        .unwrap();
        assert_eq!(queue["queue"].as_array().unwrap().len(), 1);
        assert_eq!(queue["queue"][0]["content"], "automation survives");
        assert_eq!(
            f.store.get_agent_session(&id).await.unwrap().status,
            session.status
        );
        let rows = f.store.load_all_agent_queues().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].payload["content"], "automation survives");
        assert!(f.store.get_workspace(&f.ws).await.is_ok());
    }
}

#[tokio::test]
async fn member_removal_orders_creation_redemption_and_proof_issuance_both_ways() {
    for removal_first in [false, true] {
        for subject_removed in [false, true] {
            let tmp = TempDb::new();
            let mut f = fixture(&tmp).await;
            let (person, old_token) = member(&mut f).await;
            let issuer = if subject_removed { &f.owner } else { &person };
            let link = issue(&f, issuer, false).await;
            let user = if subject_removed {
                guest("guest", 4242)
            } else {
                guest("newcomer", 8123)
            };
            let generation = f
                .store
                .host_membership_state()
                .await
                .unwrap()
                .authorization_generation;
            let (reached, release) = f.services.invite_join_commit_pause.arm();
            let service = f.services.clone();
            let invite = id_of(&link);
            let join = tokio::spawn(async move {
                service
                    .complete_invite_join(&invite, &user, InviteScope::Workspace, generation)
                    .await
            });
            reached.await.unwrap();
            if removal_first {
                with_caller(
                    Caller::Daemon,
                    f.services.host_members_remove(person.clone()),
                )
                .await
                .unwrap();
            }
            release.send(()).unwrap();
            let joined = join.await.unwrap();
            if removal_first {
                assert_eq!(
                    invite_kind(&joined),
                    if subject_removed {
                        InviteErrorKind::AccessRevoked
                    } else {
                        InviteErrorKind::Revoked
                    }
                );
            } else {
                let joined = joined.unwrap();
                // A create committed before removal is swept as well, including
                // an already-redeemed reusable invitation.
                let before = issue(&f, &person, false).await;
                with_caller(
                    Caller::Daemon,
                    f.services.host_members_remove(person.clone()),
                )
                .await
                .unwrap();
                assert_eq!(
                    invite_kind(
                        &f.services
                            .invite_inspect_op(
                                &id_of(&before),
                                before["secret"].as_str().unwrap(),
                                InviteScope::Workspace
                            )
                            .await
                    ),
                    InviteErrorKind::Revoked
                );
                let credential = f
                    .store
                    .resolve_active_principal_credential(&hash_secret(
                        joined["token"].as_str().unwrap(),
                    ))
                    .await
                    .unwrap();
                assert_eq!(credential.is_none(), subject_removed);
                if !subject_removed {
                    assert_eq!(
                        f.store
                            .get_workspace_member_role(
                                &f.ws,
                                &PrincipalId(joined["principalId"].as_str().unwrap().into())
                            )
                            .await
                            .unwrap(),
                        Some(WorkspaceRole::Collaborator)
                    );
                }
            }
            assert!(f
                .store
                .resolve_active_principal_credential(&hash_secret(&old_token))
                .await
                .unwrap()
                .is_none());
        }
    }
}

#[tokio::test]
async fn member_removal_accept_rechecks_bearer_at_commit() {
    for removal_first in [false, true] {
        let tmp = TempDb::new();
        let mut f = fixture(&tmp).await;
        let (person, token) = member(&mut f).await;
        let link = issue(&f, &f.owner, false).await;
        let token_hash = hash_secret(&token);
        let barrier = removal_first.then(|| f.services.invite_join_commit_pause.arm());
        let service = f.services.clone();
        let join = tokio::spawn(async move {
            service
                .invite_accept_op(
                    &id_of(&link),
                    link["secret"].as_str().unwrap(),
                    &token,
                    InviteScope::Workspace,
                )
                .await
        });
        if let Some((reached, release)) = barrier {
            reached.await.unwrap();
            with_caller(
                Caller::Daemon,
                f.services.host_members_remove(person.clone()),
            )
            .await
            .unwrap();
            release.send(()).unwrap();
            assert_eq!(
                invite_kind(&join.await.unwrap()),
                InviteErrorKind::CredentialInvalid
            );
        } else {
            let admitted = join.await.unwrap().unwrap();
            assert_eq!(hash_secret(admitted["token"].as_str().unwrap()), token_hash);
            with_caller(
                Caller::Daemon,
                f.services.host_members_remove(person.clone()),
            )
            .await
            .unwrap();
        }
        assert!(f
            .store
            .resolve_active_principal_credential(&token_hash)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            f.store
                .get_workspace_member_role(&f.ws, &person)
                .await
                .unwrap(),
            None
        );
    }
}
