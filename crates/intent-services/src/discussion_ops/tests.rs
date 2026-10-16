use super::*;
use crate::tests::{workspace, TempDb};
use intent_core::{with_caller, AgentId, EventQueryParams, HostRole, WorkspaceApi, WorkspaceRole};
use intent_store::Store;

fn human(id: &PrincipalId) -> Caller {
    Caller::Wire {
        principal_id: id.clone(),
        host_role: HostRole::Guest,
    }
}

#[tokio::test]
async fn discussion_history_is_filtered_from_agent_event_queries_before_paging() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    // Seed the legacy event log directly: changing the publish path must not
    // expose messages already written by an older daemon.
    for (i, kind) in [
        "note:updated",
        "discussion:message",
        "agent:idle",
        "discussion:future-type",
        "file:changed",
        "discussion:message",
    ]
    .iter()
    .enumerate()
    {
        store
            .insert_event(&NewEvent {
                workspace_id: ws.clone(),
                timestamp: format!("2026-01-01T00:00:0{i}Z"),
                event_type: (*kind).into(),
                actor: EventActor::default(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: json!({"body":"legacy"}),
            })
            .await
            .unwrap();
    }
    let svc = Services::new(store);
    let caller = Caller::Agent {
        agent_id: AgentId::new(),
    };
    for kind in [
        "discussion:message",
        "discussion:*",
        "discussion:future-type",
    ] {
        for paginate in [false, true] {
            let result = with_caller(
                caller.clone(),
                svc.event_query(
                    ws.clone(),
                    EventQueryParams {
                        event_type: Some(kind.into()),
                        paginate: Some(paginate),
                        limit: Some(1),
                        ..Default::default()
                    },
                ),
            )
            .await
            .unwrap();
            assert_eq!(
                result,
                if paginate {
                    json!({"items":[],"nextToken":null})
                } else {
                    json!([])
                }
            );
        }
    }
    for event_type in [None, Some("*".into())] {
        let legacy = with_caller(
            caller.clone(),
            svc.event_query(
                ws.clone(),
                EventQueryParams {
                    event_type: event_type.clone(),
                    limit: Some(2),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            legacy
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["file:changed", "agent:idle"]
        );
        let page = with_caller(
            caller.clone(),
            svc.event_query(
                ws.clone(),
                EventQueryParams {
                    event_type: event_type.clone(),
                    limit: Some(2),
                    paginate: Some(true),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(page["items"], legacy);
        let next = with_caller(
            caller.clone(),
            svc.event_query(
                ws.clone(),
                EventQueryParams {
                    event_type,
                    limit: Some(2),
                    page_token: Some(page["nextToken"].as_str().unwrap().into()),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(next["items"].as_array().unwrap().len(), 1);
        assert_eq!(next["items"][0]["type"], "note:updated");
        assert!(next["nextToken"].is_null());
    }
    let unrelated = with_caller(
        caller,
        svc.event_query(
            ws.clone(),
            EventQueryParams {
                event_type: Some("agent:*".into()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    assert_eq!(unrelated.as_array().unwrap().len(), 1);
    assert_eq!(unrelated[0]["type"], "agent:idle");
    let primary = svc.store.get_primary_principal().await.unwrap();
    let human_history = with_caller(
        Caller::Wire {
            principal_id: primary.id,
            host_role: HostRole::Owner,
        },
        svc.event_query(
            ws,
            EventQueryParams {
                event_type: Some("discussion:*".into()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    assert_eq!(human_history.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn discussion_agent_subscription_admission_rejects_exact_wildcard_and_mixed_lists() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let svc = Services::new(store);
    let agent = AgentId::new();
    for types in [
        vec!["discussion:message".into()],
        vec!["discussion:*".into()],
        vec!["file:*".into(), "discussion:message".into()],
    ] {
        let caller = Caller::Agent {
            agent_id: agent.clone(),
        };
        let error = with_caller(
            caller.clone(),
            svc.event_subscribe(ws.clone(), Some(agent.clone()), types.clone(), None, None),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&error, Error::InvalidParams(m) if m.contains("human-only")),
            "{error:?}"
        );
        let error = with_caller(
            caller,
            svc.agent_subscribe(ws.clone(), Some(agent.clone()), types, None, None),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&error, Error::InvalidParams(m) if m.contains("human-only")),
            "{error:?}"
        );
    }
    assert!(svc
        .store
        .list_event_subscriptions()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn discussion_requires_humans_and_current_membership() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let mut guest = store.get_primary_principal().await.unwrap();
    guest.id = PrincipalId::new();
    guest.is_primary = false;
    guest.login = Some("member".into());
    store.upsert_principal(&guest).await.unwrap();
    let svc = Services::new(store.clone());
    for caller in [
        Caller::Daemon,
        Caller::Agent {
            agent_id: AgentId::new(),
        },
    ] {
        assert!(matches!(
            with_caller(caller.clone(), svc.discussion_send_op(&ws, "id", "hello")).await,
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            with_caller(caller, svc.discussion_list_op(&ws, None, None, 50)).await,
            Err(Error::Forbidden(_))
        ));
    }
    assert!(matches!(
        svc.discussion_send_op(&ws, "id", "hello").await,
        Err(Error::Forbidden(_))
    ));
    assert!(matches!(
        with_caller(
            human(&guest.id),
            svc.discussion_list_op(&ws, None, None, 50)
        )
        .await,
        Err(Error::NotFound(_))
    ));
    store
        .add_workspace_member(&ws, &guest.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let first = with_caller(
        human(&guest.id),
        svc.discussion_send_op(&ws, "id", " hello\nthere "),
    )
    .await
    .unwrap();
    assert_eq!(first.author.principal_id, guest.id);
    assert_eq!(first.author.login.as_deref(), Some("member"));
    assert_eq!(first.body, " hello\nthere ");
    for (id, body) in [
        ("", "hello"),
        ("id", " \n\t"),
        ("id", &"é".repeat(8193)),
        (&"x".repeat(129), "hello"),
    ] {
        assert!(matches!(
            with_caller(human(&guest.id), svc.discussion_send_op(&ws, id, body)).await,
            Err(Error::InvalidParams(_))
        ));
    }
    let full = "é".repeat(8192);
    assert!(
        with_caller(human(&guest.id), svc.discussion_send_op(&ws, "full", &full))
            .await
            .is_ok()
    );
    store.remove_workspace_member(&ws, &guest.id).await.unwrap();
    assert!(matches!(
        with_caller(human(&guest.id), svc.discussion_send_op(&ws, "id", "hello")).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        with_caller(
            human(&guest.id),
            svc.discussion_list_op(&ws, None, None, 50)
        )
        .await,
        Err(Error::NotFound(_))
    ));
}
