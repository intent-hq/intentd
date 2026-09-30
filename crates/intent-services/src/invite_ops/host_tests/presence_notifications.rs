use super::*;
use crate::events::{EventBus, Subscription, SubscriptionFilter};
use intent_core::events::{NOTE_PRESENCE, PRESENCE_CHANGED, WORKSPACE_UPDATED};

/// The marker shares the subscription's FIFO with presence. Once observed,
/// absence assertions cover every emission completed by the preceding action.
async fn through_marker(
    bus: &EventBus,
    events: &mut Subscription,
    workspace: &WorkspaceId,
) -> Vec<intent_core::Event> {
    bus.publish(&crate::workspace_updated_event(
        workspace,
        &json!({"title":"presence notification barrier"}),
    ))
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut received = Vec::new();
        loop {
            for event in events.recv().await.expect("subscription open") {
                if event.event_type == WORKSPACE_UPDATED
                    && event.data["changes"]["title"] == "presence notification barrier"
                {
                    return received;
                }
                received.push(event);
            }
        }
    })
    .await
    .expect("event delivery barrier")
}

async fn membership_addition_presence(cached: bool, online: bool, self_removal: bool) {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    let bus = EventBus::new(f.store.clone());
    f.services = f.services.with_event_bus(bus.clone());
    let (person, token) = f.first_join(&guest("guest", 4242)).await;
    let inherited = crate::tests::workspace(&WorkspaceId::new());
    f.store.insert_workspace(&inherited).await.unwrap();
    let chief = crate::tests::workspace(&WorkspaceId::from(intent_core::CHIEF_WORKSPACE_ID));
    f.store.insert_workspace(&chief).await.unwrap();
    with_caller(
        wire(&f.collaborator),
        f.services.presence_connect_op("remaining-guest".into()),
    )
    .await
    .unwrap();
    let note = intent_core::NoteId::from("spec");
    if cached && !online {
        // A non-hello note lease keeps the profile cached without making
        // its principal online, including after the hello connection closes.
        let viewers = with_caller(
            wire(&person),
            f.services.note_presence_join_op(
                "cached-note".into(),
                "cache-lease".into(),
                f.ws.clone(),
                note.clone(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(viewers["viewers"][0]["principalId"], person.0);
    }
    if cached {
        with_caller(
            wire(&person),
            f.services.presence_connect_op("promoted-person".into()),
        )
        .await
        .unwrap();
        if !online {
            f.services
                .presence_disconnect_op("promoted-person".into())
                .await;
        }
    }
    let before_presence = with_caller(
        wire(&f.collaborator),
        f.services.presence_snapshot_op(f.ws.clone()),
    )
    .await
    .unwrap();
    let before_members = with_caller(
        wire(&f.collaborator),
        f.services.workspace_members_list_op(&f.ws),
    )
    .await
    .unwrap();
    let retained = f.store.list_workspace_members(&f.ws).await.unwrap();
    let count = f.services.member_count(&f.ws).await.unwrap();
    let mut events = bus.subscribe(SubscriptionFilter {
        event_types: vec![
            PRESENCE_CHANGED.into(),
            WORKSPACE_UPDATED.into(),
            NOTE_PRESENCE.into(),
        ],
        batch_window: None,
        ..Default::default()
    });

    let link = create(&f, "guest").await;
    if cached && !online {
        with_caller(
            wire(&person),
            f.services.note_presence_update_op(
                "cached-note",
                f.ws.clone(),
                note,
                &json!({"rev":1,"anchor":0,"head":0}),
            ),
        )
        .await
        .unwrap();
        let proof = through_marker(&bus, &mut events, &f.ws).await;
        let update = proof
            .iter()
            .find(|event| event.event_type == NOTE_PRESENCE && event.data["kind"] == "updated")
            .expect("live note lease publishes its cached profile before acceptance");
        assert_eq!(update.data["principalId"], person.0);
        assert_eq!(update.data["login"], "guest");
        assert_eq!(update.data["hostRole"], "guest");
    }
    let immediately_before = with_caller(
        wire(&f.collaborator),
        f.services.presence_snapshot_op(f.ws.clone()),
    )
    .await
    .unwrap();
    assert_eq!(immediately_before, before_presence);
    assert_eq!(
        immediately_before["members"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["principalId"] == person.0),
        online,
        "retaining a note lease must not mark its principal online"
    );
    let joined = f
        .services
        .invite_accept_op(
            &id_of(&link),
            link["secret"].as_str().unwrap(),
            &token,
            InviteScope::Host,
        )
        .await
        .unwrap();
    assert_eq!(joined["hostRole"], "member");
    let received = through_marker(&bus, &mut events, &f.ws).await;
    for workspace in [&f.ws, &inherited.id] {
        let presence: Vec<_> = received
            .iter()
            .filter(|event| {
                event.event_type == PRESENCE_CHANGED && &event.workspace_id == workspace
            })
            .collect();
        assert!(
            !presence.is_empty(),
            "committed addition must notify {workspace}; cached={cached}, online={online}: {received:?}"
        );
        let expected = with_caller(
            Caller::Daemon,
            f.services.presence_snapshot_op(workspace.clone()),
        )
        .await
        .unwrap();
        assert!(presence.iter().all(|event| event.data == expected));
        let promoted: Vec<_> = expected["members"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["principalId"] == person.0)
            .collect();
        assert_eq!(promoted.len(), usize::from(online));
        if online {
            assert_eq!(promoted[0]["hostRole"], "member");
        }
        if !online && workspace == &f.ws {
            assert_eq!(expected, before_presence, "online roster is unchanged");
        }
    }
    assert!(received.iter().all(|event| !event.workspace_id.is_chief()));
    assert_eq!(f.services.member_count(&f.ws).await.unwrap(), count);
    assert_eq!(
        f.store.list_workspace_members(&f.ws).await.unwrap(),
        retained
    );
    assert!(f
        .store
        .get_workspace_member_role(&inherited.id, &person)
        .await
        .unwrap()
        .is_none());
    let after_members = with_caller(
        wire(&f.collaborator),
        f.services.workspace_members_list_op(&f.ws),
    )
    .await
    .unwrap();
    let promoted: Vec<_> = after_members["members"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["principalId"] == person.0)
        .collect();
    assert_eq!(promoted.len(), 1);
    assert_eq!(promoted[0]["hostRole"], "member");
    assert_eq!(
        after_members["guestCount"].as_u64().unwrap() + 1,
        before_members["guestCount"].as_u64().unwrap()
    );

    // Redeeming a fresh invite as an existing member is not another addition.
    let revision = f.store.host_membership_state().await.unwrap().revision;
    let again = create(&f, "guest").await;
    f.services
        .invite_accept_op(
            &id_of(&again),
            again["secret"].as_str().unwrap(),
            &token,
            InviteScope::Host,
        )
        .await
        .unwrap();
    assert!(through_marker(&bus, &mut events, &f.ws)
        .await
        .iter()
        .all(|event| event.event_type != PRESENCE_CHANGED));
    assert_eq!(
        f.store.host_membership_state().await.unwrap().revision,
        revision
    );

    if cached && !online {
        f.services
            .note_presence_leave_op("cached-note", "cache-lease");
        let left = through_marker(&bus, &mut events, &f.ws).await;
        assert!(left.iter().any(|event| {
            event.event_type == NOTE_PRESENCE
                && event.data["kind"] == "left"
                && event.data["principalId"] == person.0
        }));
    }
    if self_removal {
        with_caller(
            Caller::Wire {
                principal_id: person.clone(),
                host_role: HostRole::Member,
            },
            f.services.principal_revoke_self_op(),
        )
        .await
        .unwrap();
    } else {
        with_caller(Caller::Daemon, f.services.host_members_remove_op(&person))
            .await
            .unwrap();
    }
    let removed = through_marker(&bus, &mut events, &f.ws).await;
    for workspace in [&f.ws, &inherited.id] {
        assert!(removed.iter().any(|event| {
            event.event_type == PRESENCE_CHANGED && &event.workspace_id == workspace
        }));
    }
    assert!(f
        .store
        .get_workspace_member_role(&f.ws, &person)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn host_membership_presence_notifies_for_uncached_offline_addition() {
    membership_addition_presence(false, false, false).await;
}

#[tokio::test]
async fn host_membership_presence_notifies_for_cached_offline_addition() {
    membership_addition_presence(true, false, true).await;
}

#[tokio::test]
async fn host_membership_presence_preserves_online_and_noop_behavior() {
    membership_addition_presence(true, true, false).await;
}
