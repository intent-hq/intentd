//! Current authorization for bounded subscription state, without Note hydration.
use crate::Services;
use intent_core::{Error, NoteId, Result, WorkspaceId};
use serde_json::Value;

impl Services {
    pub(crate) async fn read_page_state(
        &self,
        workspace: WorkspaceId,
        note: NoteId,
        incarnation: Option<String>,
    ) -> Result<Value> {
        self.require_member(&workspace).await?;
        if intent_core::current_caller().is_none() {
            return Err(Error::Forbidden("Caller required".into()));
        }
        // A retained incarnation is not permission to read a deleted workspace.
        self.store.get_workspace(&workspace).await?;
        let result = self
            .store
            .read_note_page_state(&workspace, &note, incarnation.as_deref())
            .await;
        #[cfg(test)]
        tests::pause_after_read(&workspace, &result).await;
        self.require_member(&workspace).await?;
        self.store.get_workspace(&workspace).await?;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::setup;
    use intent_core::{with_caller, Caller, HostRole, Principal, PrincipalId, WorkspaceRole};

    #[tokio::test]
    async fn page_state_rechecks_current_membership_for_retained_incarnation() {
        let (_temporary, services, workspace, note) = setup("source").await;
        let principal = Principal {
            id: PrincipalId::new(),
            identity: None,
            github_user_id: None,
            login: None,
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: intent_core::now_iso(),
            updated_at: intent_core::now_iso(),
        };
        services.store.upsert_principal(&principal).await.unwrap();
        let caller = Caller::Wire {
            principal_id: principal.id.clone(),
            host_role: HostRole::Guest,
        };
        assert!(matches!(
            with_caller(
                caller.clone(),
                services.read_page_state(workspace.clone(), note.clone(), None)
            )
            .await,
            Err(Error::NotFound(_))
        ));
        services
            .store
            .add_workspace_member(&workspace, &principal.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
        let state = with_caller(
            caller.clone(),
            services.read_page_state(workspace.clone(), note.clone(), None),
        )
        .await
        .unwrap();
        let instance = state["scope"]["noteInstanceId"]
            .as_str()
            .unwrap()
            .to_owned();
        services
            .store
            .remove_workspace_member(&workspace, &principal.id)
            .await
            .unwrap();
        assert!(matches!(
            with_caller(
                caller,
                services.read_page_state(workspace, note, Some(instance))
            )
            .await,
            Err(Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn page_state_reads_original_incarnation_without_loading_recreated_note() {
        let (_temporary, services, workspace, note) = setup("original").await;
        let first = with_caller(
            Caller::Daemon,
            services.read_page_state(workspace.clone(), note.clone(), None),
        )
        .await
        .unwrap();
        let incarnation = first["scope"]["noteInstanceId"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut recreated = services.store.get_note(&workspace, &note).await.unwrap();
        // The ordinary deletion path publishes a durable per-incarnation tuple.
        services.store.delete_note(&workspace, &note).await.unwrap();
        let deleted = with_caller(
            Caller::Daemon,
            services.read_page_state(workspace.clone(), note.clone(), Some(incarnation.clone())),
        )
        .await
        .unwrap();
        assert_eq!(deleted["deleted"], true);
        assert_eq!(deleted["scope"], first["scope"]);
        assert!(deleted.get("content").is_none());
        assert!(matches!(
            with_caller(
                Caller::Daemon,
                services.read_page_state(workspace.clone(), note.clone(), None)
            )
            .await,
            Err(Error::NotFound(_))
        ));
        recreated.content = "new incarnation".into();
        services.store.insert_note(&recreated).await.unwrap();
        let current = with_caller(
            Caller::Daemon,
            services.read_page_state(workspace.clone(), note.clone(), None),
        )
        .await
        .unwrap();
        assert_ne!(current["scope"]["noteInstanceId"], incarnation);
        assert_eq!(current["deleted"], false);
        let retained = with_caller(
            Caller::Daemon,
            services.read_page_state(workspace.clone(), note.clone(), Some(incarnation.clone())),
        )
        .await
        .unwrap();
        assert_eq!(
            retained, deleted,
            "old subscription must not adopt a recreated note"
        );
        services.store.delete_workspace(&workspace).await.unwrap();
        assert!(matches!(
            with_caller(
                Caller::Daemon,
                services.read_page_state(workspace, note, Some(incarnation))
            )
            .await,
            Err(Error::NotFound(_))
        ));
    }
    struct ReadBoundary {
        outcome: std::sync::atomic::AtomicU8,
        reached: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    static READ_BOUNDARIES: std::sync::Mutex<Vec<(WorkspaceId, std::sync::Arc<ReadBoundary>)>> =
        std::sync::Mutex::new(Vec::new());

    pub(super) async fn pause_after_read(workspace: &WorkspaceId, result: &Result<Value>) {
        let boundary = {
            let mut boundaries = READ_BOUNDARIES.lock().unwrap();
            boundaries
                .iter()
                .position(|(id, _)| id == workspace)
                .map(|index| boundaries.swap_remove(index).1)
        };
        if let Some(boundary) = boundary {
            let outcome = match result {
                Ok(_) => 1,
                Err(Error::NotFound(resource)) if resource == "note page state" => 2,
                Err(_) => 3,
            };
            boundary
                .outcome
                .store(outcome, std::sync::atomic::Ordering::SeqCst);
            boundary.reached.notify_one();
            boundary.release.notified().await;
        }
    }

    #[tokio::test]
    async fn page_state_revocation_after_store_result_withholds_success_and_errors() {
        let (_temporary, services, workspace, note) = setup("source").await;
        let principal = Principal {
            id: PrincipalId::new(),
            identity: None,
            github_user_id: None,
            login: None,
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: intent_core::now_iso(),
            updated_at: intent_core::now_iso(),
        };
        services.store.upsert_principal(&principal).await.unwrap();
        let caller = Caller::Wire {
            principal_id: principal.id.clone(),
            host_role: HostRole::Guest,
        };
        for missing in [false, true] {
            services
                .store
                .add_workspace_member(&workspace, &principal.id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
            let boundary = std::sync::Arc::new(ReadBoundary {
                outcome: std::sync::atomic::AtomicU8::new(0),
                reached: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            READ_BOUNDARIES
                .lock()
                .unwrap()
                .push((workspace.clone(), boundary.clone()));
            let read = with_caller(
                caller.clone(),
                services.read_page_state(
                    workspace.clone(),
                    note.clone(),
                    missing.then(|| "missing-incarnation".into()),
                ),
            );
            let revoke = async {
                boundary.reached.notified().await;
                assert_eq!(
                    boundary.outcome.load(std::sync::atomic::Ordering::SeqCst),
                    if missing { 2 } else { 1 },
                    "exact Store outcome before revocation"
                );
                services
                    .store
                    .remove_workspace_member(&workspace, &principal.id)
                    .await
                    .unwrap();
                boundary.release.notify_one();
            };
            let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::join!(read, revoke)
            })
            .await
            .unwrap();
            assert!(matches!(result, Err(Error::NotFound(ref resource))
                if resource == &format!("workspace {workspace}")),
                "post-read authorization overrides Store success and missing incarnation: {result:?}");
        }
    }
}
