//! The shared module also compiles in the standalone admission harness.
use super::*;
use crate::repository_admission::lifecycle::physical_owner::{
    RepositoryCreationIntent, RepositoryCreationOwner,
};
use crate::repository_admission::request_context::current_read_request;
use intent_acp::mcp_server::request_context::McpRequestContext;
use intent_core::caller::{with_caller, Caller};
use intent_core::{chief_workspace, AgentId, AgentSession, WorkspaceId};

#[tokio::test]
async fn read_child_cleanup_and_last_scope_drop_preserve_only_original_siblings() {
    let dir = tempfile::Builder::new()
        .prefix("original-read-child-")
        .tempdir()
        .unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let mut workspace = chief_workspace();
    workspace.id = WorkspaceId::new();
    store.insert_workspace(&workspace).await.unwrap();
    let agent = AgentId::new();
    let row: AgentSession = serde_json::from_value(serde_json::json!({
        "id":agent,"workspaceId":workspace.id,"name":"read child", "status":"active",
        "createdAt":"2026-09-28T00:00:00Z","updatedAt":"2026-09-28T00:00:00Z"
    }))
    .unwrap();
    store.insert_agent_session(&row).await.unwrap();
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    registry.install(&store).await.unwrap();
    let physical = RepositoryCreationOwner::allocate(
        &registry,
        &store,
        workspace.id.clone(),
        agent.clone(),
        RepositoryCreationIntent::FirstSet,
    )
    .unwrap()
    .initialize(&store, || async {
        Ok("original fixture completion".into())
    })
    .await
    .unwrap();
    let retained = Arc::new(17_u8);
    let owner = RepositoryReadOwner::retain_original(retained.clone(), store, registry).unwrap();
    let context = physical.callback().with_read_owner(Ok(owner));
    let scope = McpRequestContext::capture(&context);
    let sibling = McpRequestContext::capture(&context);
    let caller = Caller::Agent { agent_id: agent };
    let mut escaped = None;
    with_caller(
        caller.clone(),
        scope.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            assert!(read.retains(retained.as_ref()));
            assert!(!read.retains(&17_u8));
            let mut child = read.child().unwrap();
            child
                .subscribe(&[
                    RepositoryLifecycleKey::Database,
                    RepositoryLifecycleKey::Workspace(workspace.id.clone()),
                ])
                .unwrap();
            assert_eq!(child.transfer(|| Ok(23)), Ok(23));
            let ended = child.retirement();
            drop(child);
            assert_eq!(ended.check_current(), Err(AdmissionError::Retired));
            let fresh = read.child().unwrap();
            assert_eq!(fresh.transfer(|| Ok(29)), Ok(29));
            escaped = Some((read, fresh));
        })),
    )
    .await;
    with_caller(
        caller.clone(),
        scope.scope(Box::pin(async {
            assert!(current_read_request().is_ok());
        })),
    )
    .await;
    let intermediate = scope.clone();
    drop(intermediate);
    with_caller(caller.clone(), async {
        assert!(escaped.as_ref().unwrap().0.check_current().is_ok());
    })
    .await;
    drop(scope);
    with_caller(caller.clone(), async {
        let (read, child) = escaped.as_ref().unwrap();
        assert_eq!(read.check_current(), Err(AdmissionError::Retired));
        assert_eq!(child.transfer(|| Ok(())), Err(AdmissionError::Retired));
    })
    .await;
    with_caller(
        caller,
        sibling.scope(Box::pin(async {
            let read = current_read_request().unwrap();
            assert_ne!(
                read.correlation(),
                escaped.as_ref().unwrap().0.correlation()
            );
            assert!(read.child().unwrap().transfer(|| Ok(())).is_ok());
        })),
    )
    .await;
}
