use super::*;
use crate::tests::{workspace, TempDb};
use intent_core::{with_caller, AgentId, HostRole, WorkspaceRole};
use intent_store::Store;

fn human(id: &PrincipalId) -> Caller {
    Caller::Wire {
        principal_id: id.clone(),
        host_role: HostRole::Guest,
    }
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
            with_caller(caller, svc.discussion_list_op(&ws, None, 50)).await,
            Err(Error::Forbidden(_))
        ));
    }
    assert!(matches!(
        svc.discussion_send_op(&ws, "id", "hello").await,
        Err(Error::Forbidden(_))
    ));
    assert!(matches!(
        with_caller(human(&guest.id), svc.discussion_list_op(&ws, None, 50)).await,
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
        with_caller(human(&guest.id), svc.discussion_list_op(&ws, None, 50)).await,
        Err(Error::NotFound(_))
    ));
}
