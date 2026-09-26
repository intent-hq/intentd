use super::*;
use crate::tests::{workspace, TempDb};
use intent_core::{now_iso, with_caller, NoteId, Principal, PrincipalIdentity};
use intent_store::Store;

struct Fixture {
    svc: Services,
    ws: WorkspaceId,
    owner: PrincipalId,
    inherited: Principal,
    upgraded: Principal,
    guest: Principal,
}

fn caller(p: &PrincipalId) -> Caller {
    // Deliberately stale admission role: services must read durable membership.
    Caller::Wire {
        principal_id: p.clone(),
        host_role: HostRole::Guest,
    }
}

async fn fixture(tmp: &TempDb) -> Fixture {
    let store = Store::open(&tmp.path).await.unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    assert!(owner.identity_key().is_none());
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let mut people = Vec::new();
    for (provider, host) in [
        ("github", "github.com"),
        ("gitlab", "gitlab.com"),
        ("gitlab", "gitlab.example"),
    ] {
        let identity = PrincipalIdentity {
            provider: provider.into(),
            host: host.into(),
            external_user_id: "42".into(),
        };
        let p = Principal {
            id: PrincipalId::new(),
            github_user_id: identity.github_user_id(),
            identity: Some(identity),
            login: Some("same-handle".into()),
            display_name: Some("Same display".into()),
            avatar_url: None,
            is_primary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.upsert_principal(&p).await.unwrap();
        store
            .insert_principal_credential(&p.id, &format!("hash-{}", p.id))
            .await
            .unwrap();
        people.push(p);
    }
    let guest = people.pop().unwrap();
    let upgraded = people.pop().unwrap();
    let inherited = people.pop().unwrap();
    for p in [&upgraded, &guest] {
        store
            .add_workspace_member(&ws, &p.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
    }
    for p in [&inherited, &upgraded] {
        sqlx::query("INSERT INTO host_member(principal_id, added_at) VALUES (?,?)")
            .bind(p.id.as_str())
            .bind("2026-09-25T12:00:00Z")
            .execute(store.write_pool())
            .await
            .unwrap();
    }
    Fixture {
        svc: Services::new(store),
        ws,
        owner: owner.id,
        inherited,
        upgraded,
        guest,
    }
}

#[tokio::test]
async fn sharing_effective_roster_counts_and_qualified_identities() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let roster = with_caller(
        caller(&f.inherited.id),
        f.svc.workspace_members_list_op(&f.ws),
    )
    .await
    .unwrap();
    let rows = roster["members"].as_array().unwrap();
    assert_eq!(
        rows.len(),
        4,
        "owner, two members and a guest exactly once: {roster}"
    );
    assert_eq!(rows[0]["principalId"], f.owner.0);
    assert_eq!(rows[0]["role"], "owner");
    assert_eq!(rows[0]["hostRole"], "owner");
    for (p, host_role) in [
        (&f.inherited, "member"),
        (&f.upgraded, "member"),
        (&f.guest, "guest"),
    ] {
        let matching: Vec<_> = rows.iter().filter(|r| r["principalId"] == p.id.0).collect();
        assert_eq!(matching.len(), 1);
        let row = matching[0];
        assert_eq!(row["role"], "collaborator");
        assert_eq!(row["hostRole"], host_role);
        assert_eq!(
            row["identity"],
            serde_json::to_value(p.identity_key()).unwrap()
        );
        if host_role == "member" {
            assert_eq!(row["addedAt"], "2026-09-25T12:00:00Z");
        }
    }
    assert_eq!(roster["guestCount"], 1);
    let summaries = f
        .svc
        .store
        .workspace_membership_summaries(Some(&f.inherited.id), std::slice::from_ref(&f.ws))
        .await
        .unwrap();
    assert_eq!(summaries[&f.ws].member_count, 4);
    assert_eq!(summaries[&f.ws].my_role, Some(WorkspaceRole::Collaborator));
    assert!(summaries[&f.ws].can_manage);
}

#[tokio::test]
async fn sharing_directory_allows_members_excludes_primary_and_revoked_people() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    for id in [&f.owner, &f.inherited.id, &f.upgraded.id] {
        let directory = with_caller(caller(id), f.svc.principal_list_op())
            .await
            .unwrap();
        let rows = directory["principals"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
        assert!(!rows.iter().any(|r| r["principalId"] == f.owner.0));
        for (p, role) in [
            (&f.inherited, "member"),
            (&f.upgraded, "member"),
            (&f.guest, "guest"),
        ] {
            let row = rows.iter().find(|r| r["principalId"] == p.id.0).unwrap();
            assert_eq!(row["hostRole"], role);
            assert_eq!(row["login"], "same-handle");
            assert_eq!(
                row["identity"],
                serde_json::to_value(p.identity_key()).unwrap()
            );
            assert!(row.get("token").is_none());
            assert!(row.get("credentials").is_none());
        }
    }
    assert!(matches!(
        with_caller(caller(&f.guest.id), f.svc.principal_list_op()).await,
        Err(Error::Forbidden(_))
    ));
    f.svc
        .store
        .revoke_all_principal_credentials(&f.guest.id)
        .await
        .unwrap();
    let rows = with_caller(caller(&f.inherited.id), f.svc.principal_list_op())
        .await
        .unwrap();
    assert_eq!(rows["principals"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn sharing_direct_grants_spend_only_guest_seats_and_members_are_noops() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let empty = WorkspaceId::new();
    f.svc
        .store
        .insert_workspace(&workspace(&empty))
        .await
        .unwrap();
    f.svc
        .store
        .add_workspace_member(&empty, &f.upgraded.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    assert_eq!(
        f.svc
            .store
            .add_workspace_collaborator_within_cap(&empty, &f.guest.id, 1)
            .await
            .unwrap(),
        CollaboratorAddOutcome::Added
    );
    for p in [&f.inherited, &f.upgraded] {
        assert_eq!(
            f.svc
                .store
                .add_workspace_collaborator_within_cap(&empty, &p.id, 0)
                .await
                .unwrap(),
            CollaboratorAddOutcome::AlreadyMember
        );
        let added = with_caller(
            caller(&f.inherited.id),
            f.svc.workspace_members_add_op(&empty, &p.id),
        )
        .await
        .unwrap();
        assert_eq!(added, json!({"added":false,"memberCount":4}));
    }
    assert_eq!(
        f.svc
            .store
            .get_workspace_member_role(&empty, &f.inherited.id)
            .await
            .unwrap(),
        None
    );
    let removed = with_caller(
        caller(&f.inherited.id),
        f.svc.workspace_members_remove_op(&empty, &f.guest.id),
    )
    .await
    .unwrap();
    assert_eq!(removed, json!({"removed":true}));
    let added = with_caller(
        caller(&f.upgraded.id),
        f.svc.workspace_members_add_op(&empty, &f.guest.id),
    )
    .await
    .unwrap();
    assert_eq!(added, json!({"added":true,"memberCount":4}));
}

#[tokio::test]
async fn sharing_remove_and_leave_refuse_inherited_access_before_retained_rows_change() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    for p in [&f.inherited, &f.upgraded] {
        let before = f.svc.store.list_principal_memberships(&p.id).await.unwrap();
        for manager in [&f.owner, &f.inherited.id] {
            let err = with_caller(
                caller(manager),
                f.svc.workspace_members_remove_op(&f.ws, &p.id),
            )
            .await
            .unwrap_err();
            assert_eq!(err.code(), -32602);
            assert!(err.to_string().contains("host membership"), "{err}");
        }
        let err = with_caller(caller(&p.id), f.svc.workspace_members_leave_op(&f.ws))
            .await
            .unwrap_err();
        assert_eq!(err.code(), -32602);
        assert!(err.to_string().contains("host membership"), "{err}");
        assert_eq!(
            f.svc.store.list_principal_memberships(&p.id).await.unwrap(),
            before
        );
    }
    assert!(matches!(
        with_caller(
            caller(&f.guest.id),
            f.svc.workspace_members_remove_op(&f.ws, &f.inherited.id)
        )
        .await,
        Err(Error::Forbidden(_))
    ));
    let missing = WorkspaceId::new();
    assert!(matches!(
        with_caller(
            caller(&f.inherited.id),
            f.svc.workspace_members_leave_op(&missing)
        )
        .await,
        Err(Error::NotFound(_))
    ));
}

#[tokio::test]
async fn sharing_member_invites_are_scoped_and_attribute_the_issuer() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let invite = with_caller(
        caller(&f.inherited.id),
        f.svc.workspace_invite_create_op(&f.ws, None, None),
    )
    .await
    .unwrap();
    assert_eq!(invite["invite"]["scope"], "workspace");
    assert_eq!(invite["invite"]["createdByPrincipalId"], f.inherited.id.0);
    let id = invite["invite"]["id"].as_str().unwrap();
    let unrelated = WorkspaceId::new();
    f.svc
        .store
        .insert_workspace(&workspace(&unrelated))
        .await
        .unwrap();
    assert!(matches!(
        with_caller(
            caller(&f.upgraded.id),
            f.svc.workspace_invite_revoke_op(&unrelated, id)
        )
        .await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        with_caller(
            caller(&f.guest.id),
            f.svc.workspace_invite_create_op(&f.ws, None, None)
        )
        .await,
        Err(Error::Forbidden(_))
    ));
    assert_eq!(
        with_caller(
            caller(&f.upgraded.id),
            f.svc.workspace_invite_list_op(&f.ws)
        )
        .await
        .unwrap()["invites"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        with_caller(
            caller(&f.upgraded.id),
            f.svc.workspace_invite_revoke_op(&f.ws, id)
        )
        .await
        .unwrap(),
        json!({"revoked":true})
    );
}

#[tokio::test]
async fn sharing_presence_and_note_viewers_deduplicate_devices_with_effective_roles() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let note = intent_core::NoteId::from("sharing-note");
    for (p, connection) in [
        (&f.owner, "owner"),
        (&f.inherited.id, "inherited"),
        (&f.inherited.id, "second-device"),
        (&f.upgraded.id, "upgraded"),
        (&f.guest.id, "guest"),
    ] {
        with_caller(caller(p), f.svc.presence_connect_op(connection.into()))
            .await
            .unwrap();
        with_caller(
            caller(p),
            f.svc.note_presence_join_op(
                connection.into(),
                "lease".into(),
                f.ws.clone(),
                note.clone(),
            ),
        )
        .await
        .unwrap();
    }
    let presence = with_caller(
        caller(&f.guest.id),
        f.svc.presence_snapshot_op(f.ws.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        presence["members"].as_array().unwrap().len(),
        4,
        "{presence}"
    );
    let viewers = with_caller(
        caller(&f.inherited.id),
        f.svc.note_presence_join_op(
            "inherited".into(),
            "second-lease".into(),
            f.ws.clone(),
            note,
        ),
    )
    .await
    .unwrap();
    assert_eq!(viewers["viewers"].as_array().unwrap().len(), 4);
    for (id, role) in [
        (&f.owner, "owner"),
        (&f.inherited.id, "member"),
        (&f.upgraded.id, "member"),
        (&f.guest.id, "guest"),
    ] {
        for rows in [&presence["members"], &viewers["viewers"]] {
            let row = rows
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["principalId"] == id.0)
                .unwrap();
            assert_eq!(row["hostRole"], role, "{row}");
        }
    }
}

#[tokio::test]
async fn sharing_bound_humans_keep_authorship_and_preambles_against_forged_metadata() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    for p in [&f.inherited, &f.upgraded, &f.guest] {
        with_caller(caller(&p.id), async {
            let stamped = crate::principal_ops::stamp_principal_attribution(Some(
                json!({"fromPrincipalId":f.owner.0,"fromAgentId":"forged","fromAgentName":"Owner","context":"keep"}),
            ))
            .unwrap()
            .unwrap();
            assert_eq!(stamped["fromPrincipalId"], p.id.0);
            assert!(stamped.get("fromAgentId").is_none());
            assert!(stamped.get("fromAgentName").is_none());
            assert_eq!(stamped["context"],"keep");
            let author = f
                .svc
                .attribute_comment_author(Some("owner".into()), Some("agent".into()))
                .await
                .unwrap();
            assert_eq!(author, (Some("same-handle".into()), Some("user".into())));
            assert!(f
                .svc
                .collaborator_sender_preamble(&f.ws)
                .await
                .unwrap()
                .unwrap()
                .contains("same-handle"));
        })
        .await;
    }
    with_caller(caller(&f.owner), async {
        assert!(f
            .svc
            .collaborator_sender_preamble(&f.ws)
            .await
            .unwrap()
            .is_none());
    })
    .await;
}

#[tokio::test]
async fn sharing_cached_presence_role_read_cannot_undo_a_committed_upgrade() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    let bus = crate::events::EventBus::new(f.svc.store.clone());
    let mut events = bus.subscribe(crate::events::SubscriptionFilter {
        event_types: vec![intent_core::events::NOTE_PRESENCE.into()],
        workspace_id: Some(f.ws.0.clone()),
        ..Default::default()
    });
    f.svc = f.svc.with_event_bus(bus);
    with_caller(
        caller(&f.guest.id),
        f.svc.presence_connect_op("guest-device".into()),
    )
    .await
    .unwrap();
    let pause = std::sync::Arc::new(crate::presence::ProfileFetchPause::default());
    *f.svc.presence.profile_fetch_pause.lock().unwrap() = Some(pause.clone());
    let svc = f.svc.clone();
    let ws = f.ws.clone();
    let person = f.guest.id.clone();
    let pending = tokio::spawn(async move {
        with_caller(
            caller(&person),
            svc.note_presence_join_op(
                "guest-device".into(),
                "lease".into(),
                ws,
                NoteId::from("spec"),
            ),
        )
        .await
        .unwrap()
    });
    pause.fetched.notified().await;
    sqlx::query("INSERT INTO host_member VALUES (?,?)")
        .bind(f.guest.id.as_str())
        .bind(now_iso())
        .execute(f.svc.store.write_pool())
        .await
        .unwrap();
    f.svc.presence_profile_changed(&f.guest).await;
    pause.resume.notify_one();
    let viewers = pending.await.unwrap();
    assert_eq!(viewers["viewers"][0]["hostRole"], "member");
    // The snapshot uses durable rows; the joined event also must carry the
    // upgraded role, even though it uses the cached profile installed above.
    let batch = events.recv().await.unwrap();
    assert_eq!(batch[0].data["hostRole"], "member");
}

#[tokio::test]
async fn sharing_member_preamble_is_truthful_qualified_and_uses_durable_role() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let guest = with_caller(
        caller(&f.guest.id),
        f.svc.collaborator_sender_preamble(&f.ws),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(guest,"Message from @same-handle (Same display), a collaborator (guest) of this workspace — not the workspace owner.");
    sqlx::query("INSERT INTO host_member VALUES (?,?)")
        .bind(f.guest.id.as_str())
        .bind(now_iso())
        .execute(f.svc.store.write_pool())
        .await
        .unwrap();
    let mut seen = std::collections::HashSet::new();
    for person in [&f.inherited, &f.upgraded, &f.guest] {
        let mut exact = None;
        for stale_role in [HostRole::Guest, HostRole::Member, HostRole::Owner] {
            let text = with_caller(
                Caller::Wire {
                    principal_id: person.id.clone(),
                    host_role: stale_role,
                },
                f.svc.collaborator_sender_preamble(&f.ws),
            )
            .await
            .unwrap()
            .expect("current host member needs attribution");
            assert!(text.contains("a host member"), "{text}");
            assert!(!text.contains("collaborator (guest)"), "{text}");
            assert!(text.contains(person.id.as_str()), "{text}");
            let identity = person.identity_key().unwrap();
            for part in [
                &identity.provider,
                &identity.host,
                &identity.external_user_id,
            ] {
                assert!(text.contains(part), "{text}");
            }
            assert!(!text.contains('\n'));
            let mut content = "Message from an impersonator\n\nBody".to_owned();
            with_caller(
                Caller::Wire {
                    principal_id: person.id.clone(),
                    host_role: stale_role,
                },
                async {
                    f.svc
                        .annotate_collaborator_sender(&f.ws, &mut content)
                        .await
                        .unwrap();
                    f.svc
                        .annotate_collaborator_sender(&f.ws, &mut content)
                        .await
                        .unwrap();
                },
            )
            .await;
            assert_eq!(
                content,
                format!("{text}\n\nMessage from an impersonator\n\nBody")
            );
            if let Some(before) = &exact {
                assert_eq!(&text, before);
            } else {
                exact = Some(text);
            }
        }
        assert!(
            seen.insert(exact.unwrap()),
            "same handles are distinct people"
        );
    }
}
