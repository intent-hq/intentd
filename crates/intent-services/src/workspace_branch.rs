use intent_core::Workspace;

use crate::{events::bus::EventBus, workspace_updated_event};

pub(crate) async fn reconcile_workspace_branch(bus: &EventBus, ws: &mut Workspace, branch: &str) {
    if branch.is_empty() || branch == ws.branch {
        return;
    }
    match bus.store().reconcile_workspace_branch(ws, branch).await {
        Ok(true) => {
            ws.branch = branch.to_string();
            if let Err(error) = bus
                .publish(&workspace_updated_event(
                    &ws.id,
                    &serde_json::json!({ "branch": branch }),
                ))
                .await
            {
                tracing::warn!(workspace = %ws.id, %error, "failed to publish workspace branch update");
            }
        }
        Ok(false) => {
            if let Ok(current) = bus.store().get_workspace(&ws.id).await {
                *ws = current;
            }
        }
        Err(error) => tracing::warn!(
            workspace = %ws.id,
            %error,
            "failed to reconcile workspace branch"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use intent_core::events::WORKSPACE_UPDATED;
    use intent_core::{WorkspaceApi, WorkspaceId};
    use intent_store::Store;

    use super::*;
    use crate::events::bus::EventBus;
    use crate::events::filter::SubscriptionFilter;
    use crate::events::git_status_refresher::GitStatusRefresher;
    use crate::tests::{test_tempdir, workspace, TempDb};
    use crate::Services;

    async fn refresh(svc: &Services, ws: &Workspace) {
        crate::events::git_status_refresher::refresh_workspace(
            svc.event_bus.as_ref().unwrap(),
            svc,
            svc.git_status_cache().as_ref(),
            &ws.id,
        )
        .await;
    }

    fn init_repo(path: &Path) -> git2::Repository {
        let repo = git2::Repository::init(path).unwrap();
        repo.set_head("refs/heads/original").unwrap();
        let tree_id = repo.index().unwrap().write_tree().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        repo.commit(
            Some("HEAD"),
            &sig,
            &sig,
            "initial",
            &repo.find_tree(tree_id).unwrap(),
            &[],
        )
        .unwrap();
        repo
    }

    async fn setup(path: &Path) -> (TempDb, Services, Workspace) {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.unwrap();
        let mut ws = workspace(&WorkspaceId::new());
        ws.branch = "original".into();
        ws.worktree_path = Some(path.to_string_lossy().into_owned());
        store.insert_workspace(&ws).await.unwrap();
        store
            .set_workspace_branch_auto_generated(&ws.id, true)
            .await
            .unwrap();
        let bus = EventBus::new(store.clone());
        (db, Services::new(store).with_event_bus(bus), ws)
    }

    #[tokio::test]
    async fn refresh_repairs_external_rename_and_publishes_once() {
        let dir = test_tempdir("branch-read-");
        let repo = init_repo(dir.path());
        let (_db, svc, ws) = setup(dir.path()).await;
        let mut sub = svc
            .event_bus
            .as_ref()
            .unwrap()
            .subscribe(SubscriptionFilter {
                event_types: vec![WORKSPACE_UPDATED.into()],
                ..SubscriptionFilter::default()
            });
        repo.find_branch("original", git2::BranchType::Local)
            .unwrap()
            .rename("feat/renamed", false)
            .unwrap();

        refresh(&svc, &ws).await;
        assert_eq!(
            svc.get_workspace(ws.id.clone()).await.unwrap().branch,
            "feat/renamed"
        );
        assert_eq!(
            svc.store.get_workspace(&ws.id).await.unwrap().branch,
            "feat/renamed"
        );
        assert!(!svc
            .store
            .workspace_branch_auto_generated(&ws.id)
            .await
            .unwrap());
        let events = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].data["changes"],
            serde_json::json!({ "branch": "feat/renamed" })
        );
        assert_eq!(
            svc.list_workspaces_lite(false).await.unwrap()[0].branch,
            "feat/renamed"
        );
        refresh(&svc, &ws).await;
        assert!(sub.try_recv_delivery().is_none());
    }

    #[tokio::test]
    async fn metadata_refresh_repairs_linked_worktree_branch() {
        let dir = test_tempdir("branch-worktree-");
        let repo = init_repo(&dir.path().join("repo"));
        let path = dir.path().join("linked");
        repo.worktree("linked", &path, None).unwrap();
        let linked = git2::Repository::open(&path).unwrap();
        let (_db, svc, mut ws) = setup(&path).await;
        ws.branch = "linked".into();
        ws.repository_path = Some(dir.path().join("repo").to_string_lossy().into_owned());
        svc.store.update_workspace(&ws).await.unwrap();
        let bus = svc.event_bus.as_ref().unwrap().clone();
        let mut sub = bus.subscribe(SubscriptionFilter {
            event_types: vec![WORKSPACE_UPDATED.into()],
            ..SubscriptionFilter::default()
        });
        let svc = Arc::new(svc);
        let refresher = GitStatusRefresher::start(bus, svc.clone(), svc.git_status_cache());
        linked
            .find_branch("linked", git2::BranchType::Local)
            .unwrap()
            .rename("fix/linked", false)
            .unwrap();
        refresher.trigger(ws.id.clone());

        let events = tokio::time::timeout(Duration::from_secs(10), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(events[0].data["changes"]["branch"], "fix/linked");
        assert_eq!(
            svc.store.get_workspace(&ws.id).await.unwrap().branch,
            "fix/linked"
        );
        assert_eq!(
            intent_git::status::current_branch_at(repo.workdir().unwrap()).as_deref(),
            Some("original")
        );
    }

    #[tokio::test]
    async fn detached_missing_and_remote_repositories_preserve_metadata() {
        let dir = test_tempdir("branch-fallback-");
        let repo = init_repo(dir.path());
        let (_db, svc, mut ws) = setup(dir.path()).await;
        repo.set_head_detached(repo.head().unwrap().target().unwrap())
            .unwrap();
        refresh(&svc, &ws).await;
        assert_eq!(
            svc.get_workspace(ws.id.clone()).await.unwrap().branch,
            "original"
        );
        repo.set_head("refs/heads/original").unwrap();
        repo.find_branch("original", git2::BranchType::Local)
            .unwrap()
            .rename("external", false)
            .unwrap();
        ws.is_remote = true;
        svc.store.update_workspace(&ws).await.unwrap();
        refresh(&svc, &ws).await;
        assert_eq!(
            svc.get_workspace(ws.id.clone()).await.unwrap().branch,
            "original"
        );
        ws.is_remote = false;
        ws.worktree_path = Some(dir.path().join("missing").to_string_lossy().into_owned());
        svc.store.update_workspace(&ws).await.unwrap();
        refresh(&svc, &ws).await;
        assert_eq!(
            svc.get_workspace(ws.id.clone()).await.unwrap().branch,
            "original"
        );
        assert!(svc
            .store
            .workspace_branch_auto_generated(&ws.id)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn reconciliation_preserves_concurrent_edits_and_rejects_stale_branch_or_path() {
        let dir = test_tempdir("branch-concurrency-");
        let (_db, svc, ws) = setup(dir.path()).await;
        let mut edited = ws.clone();
        edited.title = "New title".into();
        edited.status_message = Some("Keep this status".into());
        svc.store.update_workspace(&edited).await.unwrap();
        assert!(svc
            .store
            .reconcile_workspace_branch(&ws, "renamed")
            .await
            .unwrap());
        let current = svc.store.get_workspace(&ws.id).await.unwrap();
        assert_eq!(current.title, edited.title);
        assert_eq!(current.status_message, edited.status_message);
        assert_eq!(current.updated_at, ws.updated_at);
        assert!(!svc
            .store
            .reconcile_workspace_branch(&ws, "stale")
            .await
            .unwrap());
        let mut moved = current.clone();
        moved.worktree_path = Some(dir.path().join("other").to_string_lossy().into_owned());
        svc.store.update_workspace(&moved).await.unwrap();
        assert!(!svc
            .store
            .reconcile_workspace_branch(&current, "wrong-repo")
            .await
            .unwrap());
    }
}
