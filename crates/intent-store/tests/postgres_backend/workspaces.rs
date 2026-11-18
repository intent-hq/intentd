//! Workspace store methods on both backends: workspaces, git roots, homes,
//! context items, UI context and MCP disables. Bodies use fixed ids and
//! leave their rows behind, so a Postgres statement without its tenant
//! filter sees the decoy tenant's rows on the subject run.

use intent_core::node_link::HomeStopReason;
use intent_core::{
    AgentId, AgentSession, AgentStatus, BrowserTabInput, BrowserTabVisibility, ClientId,
    ContextItem, Error, HomeId, HomeState, PullRequestInfo, PullRequestStatus, TokenUsage,
    TokenUsageTotals, Workspace, WorkspaceAttention, WorkspaceGitRoot, WorkspaceGitRootId,
    WorkspaceGitRootSource, WorkspaceHome, WorkspaceId, WorkspaceListFilter, WorkspaceStatus,
};
use intent_store::{RepositoryWorkspaceDeleteDisposition, Store};
use serde_json::json;

use crate::harness::{count_rows as count, parity_test, seed_sql as sql, TestDb};

const T0: &str = "2026-01-01T00:00:00.000Z";
const T1: &str = "2026-01-02T00:00:00.000Z";
const T2: &str = "2026-01-03T00:00:00.000Z";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn workspace(id: &str, created_at: &str) -> Workspace {
    Workspace {
        id: wid(id),
        title: format!("title {id}"),
        branch: "main".into(),
        created_at: created_at.into(),
        updated_at: created_at.into(),
        tags: vec!["t".into()],
        ..Default::default()
    }
}

fn pr(url: &str, status: PullRequestStatus) -> PullRequestInfo {
    serde_json::from_value(serde_json::json!({
        "id": url, "number": 7, "url": url, "title": "pr",
        "status": status, "createdAt": T0, "updatedAt": T0,
    }))
    .unwrap()
}

fn root(id: &str, ws: &str, path: &str, agents: &[&str]) -> WorkspaceGitRoot {
    WorkspaceGitRoot {
        id: WorkspaceGitRootId(id.into()),
        workspace_id: wid(ws),
        path: path.into(),
        source: WorkspaceGitRootSource::Auto,
        repo_owner: None,
        repo_name: None,
        registered_by_agent_ids: agents.iter().map(|a| AgentId((*a).into())).collect(),
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: None,
        created_at: T0.into(),
        updated_at: T0.into(),
    }
}

fn ids(list: &[Workspace]) -> Vec<&str> {
    list.iter().map(|w| w.id.0.as_str()).collect()
}

async fn list(store: &Store, include_archived: bool, include_retired: bool) -> Vec<Workspace> {
    store
        .list_workspaces(WorkspaceListFilter {
            include_archived,
            include_retired,
        })
        .await
        .unwrap()
}

parity_test!(workspaces_insert_get_and_list, |store| {
    assert!(list(&store, true, true).await.is_empty());
    assert!(store.ordinary_workspace_ids().await.unwrap().is_empty());
    assert!(store.plain_folder_paths().await.unwrap().is_empty());
    assert!(!store.workspace_id_ever_used(&wid("ws-a")).await.unwrap());

    let a = workspace("ws-a", T0);
    store.insert_workspace(&a).await.unwrap();
    let mut b = workspace("ws-b", T1);
    b.plain_folder = true;
    b.repository_path = Some("/srv/plain".into());
    b.last_content_activity = Some(T2.into());
    store
        .insert_workspace_with_auto_commit(&b, Some(true))
        .await
        .unwrap();
    let mut c = workspace("ws-c", T2);
    c.archived = true;
    c.status = WorkspaceStatus::Archived;
    store.insert_workspace(&c).await.unwrap();
    let mut home = workspace("__home__:n1", T0);
    home.title = "home".into();
    store.insert_workspace(&home).await.unwrap();

    assert_eq!(store.get_workspace(&wid("ws-a")).await.unwrap(), a);
    assert_eq!(store.get_workspace(&wid("ws-b")).await.unwrap(), b);
    assert!(matches!(
        store.get_workspace(&wid("missing")).await,
        Err(Error::NotFound(_))
    ));
    assert!(store.workspace_id_ever_used(&wid("ws-a")).await.unwrap());
    assert_eq!(ids(&list(&store, false, false).await), ["ws-a", "ws-b"]);
    assert_eq!(
        ids(&list(&store, true, false).await),
        ["ws-a", "ws-b", "ws-c"]
    );
    let page = store
        .list_workspaces_page(WorkspaceListFilter::default(), None, 1)
        .await
        .unwrap();
    assert_eq!(ids(&page), ["ws-a"]);
    let page = store
        .list_workspaces_page(WorkspaceListFilter::default(), Some((T0, "ws-a")), 5)
        .await
        .unwrap();
    assert_eq!(ids(&page), ["ws-b"]);
    let mut ordinary = store.ordinary_workspace_ids().await.unwrap();
    ordinary.sort_by(|x, y| x.0.cmp(&y.0));
    assert_eq!(ordinary, [wid("ws-a"), wid("ws-b"), wid("ws-c")]);
    assert_eq!(store.plain_folder_paths().await.unwrap(), ["/srv/plain"]);
    assert_eq!(
        store.workspace_auto_commit(&wid("ws-a")).await.unwrap(),
        None
    );
    assert_eq!(
        store.workspace_auto_commit(&wid("ws-b")).await.unwrap(),
        Some(true)
    );

    let clocks = store
        .workspace_content_clocks(&[wid("ws-b"), wid("ws-b"), wid("missing")])
        .await
        .unwrap();
    assert_eq!(clocks.len(), 1);
    assert_eq!(clocks[&wid("ws-b")].created_at, T1);
    assert_eq!(
        clocks[&wid("ws-b")].last_content_activity.as_deref(),
        Some(T2)
    );

    assert_eq!(store.home_record_ids().await.unwrap(), [wid("__home__:n1")]);
    assert!(matches!(
        store.delete_home_record(&wid("ws-a")).await,
        Err(Error::InvalidParams(_))
    ));
    store.delete_home_record(&wid("__home__:n1")).await.unwrap();
    assert!(store.home_record_ids().await.unwrap().is_empty());
    assert!(!store
        .workspace_id_ever_used(&wid("__home__:n1"))
        .await
        .unwrap());
});

parity_test!(workspace_scoped_column_writes, |store, tenant| {
    let id = wid("ws-s");
    let missing = wid("missing");
    store
        .insert_workspace(&workspace("ws-s", T0))
        .await
        .unwrap();

    assert!(!store.workspace_branch_auto_generated(&id).await.unwrap());
    store
        .set_workspace_branch_auto_generated(&id, true)
        .await
        .unwrap();
    assert!(store.workspace_branch_auto_generated(&id).await.unwrap());
    store.set_workspace_auto_commit(&id, false).await.unwrap();
    assert_eq!(store.workspace_auto_commit(&id).await.unwrap(), Some(false));
    store.set_workspace_label(&id, Some(tenant)).await.unwrap();
    store
        .set_workspace_auto_archive_disabled(&id, true)
        .await
        .unwrap();
    assert_eq!(store.workspace_browser_client(&id).await.unwrap(), None);
    store
        .set_workspace_browser_client(&id, Some(&ClientId(tenant.into())))
        .await
        .unwrap();
    assert_eq!(
        store.workspace_browser_client(&id).await.unwrap(),
        Some(ClientId(tenant.into()))
    );
    let got = store.get_workspace(&id).await.unwrap();
    assert_eq!(got.label.as_deref(), Some(tenant));
    assert!(got.auto_archive_disabled);
    for err in [
        store.set_workspace_label(&missing, None).await,
        store.set_workspace_auto_commit(&missing, true).await,
        store
            .set_workspace_branch_auto_generated(&missing, true)
            .await,
        store
            .set_workspace_auto_archive_disabled(&missing, true)
            .await,
        store.set_workspace_browser_client(&missing, None).await,
    ] {
        assert!(matches!(err, Err(Error::NotFound(_))), "{err:?}");
    }
    assert!(matches!(
        store.workspace_browser_client(&missing).await,
        Err(Error::NotFound(_))
    ));

    // Attention: guarded writes and NotFound.
    assert!(store
        .set_workspace_attention(&id, WorkspaceAttention::Unread, Some(T1), None)
        .await
        .unwrap());
    assert!(!store
        .set_workspace_attention(&id, WorkspaceAttention::Unread, None, None)
        .await
        .unwrap());
    assert!(!store
        .set_workspace_attention(
            &id,
            WorkspaceAttention::None,
            None,
            Some(WorkspaceAttention::ReviewRequired)
        )
        .await
        .unwrap());
    assert_eq!(store.get_workspace(&id).await.unwrap().updated_at, T1);
    assert!(matches!(
        store
            .set_workspace_attention(&missing, WorkspaceAttention::Unread, None, None)
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(store.clear_workspace_unread_if_all_seen(&id).await.unwrap());
    assert!(!store.clear_workspace_unread_if_all_seen(&id).await.unwrap());
    assert!(!store
        .clear_workspace_unread_if_all_seen(&missing)
        .await
        .unwrap());

    // Monotonic last_activity, compared as times not text.
    assert!(store
        .bump_workspace_last_activity(&id, "2026-01-02T00:00:00Z")
        .await
        .unwrap());
    assert!(!store
        .bump_workspace_last_activity(&id, "2026-01-01T23:59:59.5Z")
        .await
        .unwrap());
    assert!(!store
        .bump_workspace_last_activity(&id, "not a time")
        .await
        .unwrap());
    assert!(store
        .bump_workspace_last_activity(&id, "2026-01-02T00:00:00.5Z")
        .await
        .unwrap());
    assert!(matches!(
        store.bump_workspace_last_activity(&missing, T1).await,
        Err(Error::NotFound(_))
    ));
    if tenant == "decoy" {
        // Same id the subject writes, in a state its guarded writes match.
        store
            .insert_workspace(&workspace("missing", T0))
            .await
            .unwrap();
        store
            .set_workspace_label(&missing, Some("decoy"))
            .await
            .unwrap();
        store
            .set_workspace_attention(&missing, WorkspaceAttention::Unread, None, None)
            .await
            .unwrap();
    }
});

parity_test!(
    workspace_full_updates_and_branch_reconcile,
    |store, tenant| {
        let id = wid("ws-u");
        let mut ws = workspace("ws-u", T0);
        ws.last_activity = Some(T2.into());
        store.insert_workspace(&ws).await.unwrap();

        let mut edit = ws.clone();
        edit.title = format!("renamed {tenant}");
        edit.branch = "ignored".into();
        edit.last_activity = Some(T1.into());
        edit.status = WorkspaceStatus::Archived;
        assert_eq!(store.update_workspace(&edit).await.unwrap(), "main");
        let got = store.get_workspace(&id).await.unwrap();
        assert_eq!(got.title, format!("renamed {tenant}"));
        assert_eq!(
            got.last_activity.as_deref(),
            Some(T2),
            "older value ignored"
        );
        assert_eq!(
            got.status,
            WorkspaceStatus::Active,
            "archive moves elsewhere"
        );
        assert_eq!(
            store
                .update_workspace_with_branch(&edit, Some("feat/x"))
                .await
                .unwrap(),
            "feat/x"
        );
        assert!(matches!(
            store.update_workspace(&workspace("missing", T0)).await,
            Err(Error::NotFound(_))
        ));

        let current = store.get_workspace(&id).await.unwrap();
        assert!(!store
            .reconcile_workspace_branch(&current, "feat/x")
            .await
            .unwrap());
        store
            .set_workspace_branch_auto_generated(&id, true)
            .await
            .unwrap();
        assert!(store
            .reconcile_workspace_branch(&current, "feat/y")
            .await
            .unwrap());
        assert!(!store
            .reconcile_workspace_branch(&current, "feat/z")
            .await
            .unwrap());
        assert_eq!(store.get_workspace(&id).await.unwrap().branch, "feat/y");
        assert!(!store.workspace_branch_auto_generated(&id).await.unwrap());
    }
);

parity_test!(workspace_archive_retire_and_revive, |store, tenant| {
    let id = wid("ws-r");
    let (r2, r3, r5) = (wid("ws-r2"), wid("ws-r3"), wid("ws-r5"));
    let missing = wid("missing");
    store
        .insert_workspace(&workspace("ws-r", T0))
        .await
        .unwrap();
    assert!(!store
        .unarchive_workspace_if_archived(&id, T1)
        .await
        .unwrap());
    assert!(!store.retire_workspace_if_archived(&id, T1).await.unwrap());
    // Archive columns move only through the scoped flips, so stage archived
    // rows by inserting them archived.
    for staged in ["ws-r2", "ws-r3", "ws-r5"] {
        let mut ws = workspace(staged, T0);
        ws.archived = true;
        ws.archived_at = Some(T0.into());
        ws.status = WorkspaceStatus::Archived;
        store.insert_workspace(&ws).await.unwrap();
    }
    assert_eq!(
        store.retired_workspace_for(Some(&r5), None).await.unwrap(),
        None
    );
    assert!(store.retire_workspace_if_archived(&r5, T1).await.unwrap());

    // The decoy stops here: ws-r2 and ws-r3 stay archived and ws-r5 stays
    // retired, so the subject's retire, unarchive and revive of the same ids
    // would match them without the tenant filter.
    if tenant != "decoy" {
        assert!(store.retire_workspace_if_archived(&r2, T1).await.unwrap());
        assert!(!store.retire_workspace_if_archived(&r2, T1).await.unwrap());
        let retired = store.get_workspace(&r2).await.unwrap();
        assert_eq!(retired.status, WorkspaceStatus::Retired);
        assert!(retired.archived);
        assert_eq!(retired.retired_at.as_deref(), Some(T1));
        assert_eq!(
            retired.status_message.as_deref(),
            Some(intent_core::WORKSPACE_PURGED_STATUS_MESSAGE)
        );
        assert_eq!(
            store.retired_workspace_for(Some(&r2), None).await.unwrap(),
            Some(r2.clone())
        );
        assert_eq!(
            store
                .retired_workspace_for(None, Some(&AgentId("no-agent".into())))
                .await
                .unwrap(),
            None
        );
        assert_eq!(ids(&list(&store, true, false).await), ["ws-r", "ws-r3"]);
        assert_eq!(
            ids(&list(&store, false, true).await),
            ["ws-r", "ws-r2", "ws-r5"]
        );
        assert!(!store
            .unarchive_workspace_if_archived(&r2, T1)
            .await
            .unwrap());

        assert!(store
            .unarchive_workspace_if_archived(&r3, T1)
            .await
            .unwrap());
        let unarchived = store.get_workspace(&r3).await.unwrap();
        assert_eq!(unarchived.status, WorkspaceStatus::Active);
        assert!(!unarchived.archived);
        assert_eq!(unarchived.archived_at, None);
        assert_eq!(unarchived.updated_at, T1);

        assert!(!store
            .revive_workspace_if_retired(&id, "b", None, T2)
            .await
            .unwrap());
        assert!(store
            .revive_workspace_if_retired_with_notice(
                &r5,
                "revived",
                Some(tenant),
                T2,
                Some(("notice", "text", None))
            )
            .await
            .unwrap());
        let revived = store.get_workspace(&r5).await.unwrap();
        assert_eq!(revived.status, WorkspaceStatus::Active);
        assert!(!revived.archived);
        assert_eq!(revived.retired_at, None);
        assert_eq!(revived.branch, "revived");
        assert_eq!(revived.base_commit_sha.as_deref(), Some(tenant));
        assert_eq!(revived.status_message, None);
    }
    for err in [
        store.unarchive_workspace_if_archived(&missing, T1).await,
        store.retire_workspace_if_archived(&missing, T1).await,
        store
            .revive_workspace_if_retired(&missing, "b", None, T1)
            .await,
    ] {
        assert!(matches!(err, Err(Error::NotFound(_))), "{err:?}");
    }
});

parity_test!(workspace_pull_request_links, |store, tenant| {
    let url = "https://github.com/acme/app/pull/7";
    let key = "github.com/acme/app/pull/7";
    let id = wid("ws-p");
    assert!(store
        .list_pull_request_workspaces(key, true)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .list_workspaces_referencing_pr_url(url)
        .await
        .unwrap()
        .is_empty());
    store
        .insert_workspace(&workspace("ws-p", T0))
        .await
        .unwrap();
    store
        .insert_workspace(&workspace("ws-q", T1))
        .await
        .unwrap();
    assert!(store
        .workspaces_with_settled_pull_requests(std::slice::from_ref(&id))
        .await
        .unwrap()
        .is_empty());

    let mut ws = store.get_workspace(&id).await.unwrap();
    ws.pr_number = Some(7);
    ws.pr_url = Some(url.into());
    ws.pr_status = Some(PullRequestStatus::Open);
    ws.updated_at = T1.into();
    store.update_workspace_pr_linkage(&ws).await.unwrap();
    assert!(matches!(
        store
            .update_workspace_pr_linkage(&workspace("missing", T0))
            .await,
        Err(Error::NotFound(_))
    ));
    let found = store
        .list_workspaces_referencing_pr_url(&url.to_uppercase())
        .await
        .unwrap();
    assert_eq!(ids(&found), ["ws-p"]);
    let linked = store.list_pull_request_workspaces(key, true).await.unwrap();
    assert_eq!(linked.len(), 1);
    assert_eq!(linked[0].id, id);
    let links = store
        .list_pull_request_links_for_workspaces(&[id.clone(), wid("ws-q")])
        .await
        .unwrap();
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].pr_key, key);
    assert!(store
        .list_pull_request_links_for_workspaces(&[])
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .workspaces_with_settled_pull_requests(std::slice::from_ref(&id))
        .await
        .unwrap()
        .is_empty());

    let mut ws = store.get_workspace(&id).await.unwrap();
    store
        .update_workspace_pr_linkage_rebased(&mut ws, |ws, stored| {
            assert_eq!(stored, None);
            ws.pr_status = Some(PullRequestStatus::Merged);
            ws.pull_requests = Some(vec![pr(url, PullRequestStatus::Merged)]);
        })
        .await
        .unwrap();
    let projected = store
        .project_workspace_pr_snapshots(&id, T2, |pool, active| {
            assert_eq!(pool.as_ref().map(Vec::len), Some(1));
            let mut merged = pr(url, PullRequestStatus::Merged);
            merged.title = tenant.into();
            *active = Some(merged);
            true
        })
        .await
        .unwrap();
    assert!(projected.is_some_and(|(_, active)| active.is_some()));
    assert_eq!(
        store
            .project_workspace_pr_snapshots(&id, T2, |_, _| false)
            .await
            .unwrap(),
        None
    );
    let stored = store.get_workspace(&id).await.unwrap();
    assert_eq!(stored.updated_at, T2);
    assert_eq!(stored.pr_status, Some(PullRequestStatus::Merged));
    assert_eq!(stored.active_pull_request.unwrap().title, tenant);
    let settled = store
        .workspaces_with_settled_pull_requests(&[id.clone(), wid("ws-q")])
        .await
        .unwrap();
    assert_eq!(
        settled.into_iter().collect::<Vec<_>>(),
        std::slice::from_ref(&id)
    );
});

parity_test!(workspace_git_roots, |store, tenant| {
    let ws = wid("ws-g");
    let rid = WorkspaceGitRootId("root-1".into());
    store
        .insert_workspace(&workspace("ws-g", T0))
        .await
        .unwrap();
    assert!(store
        .list_workspace_git_roots(&ws)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .list_workspace_git_roots_with_prs(true)
        .await
        .unwrap()
        .is_empty());

    let first = root("root-1", "ws-g", "/src/a", &["a1", "a1"]);
    let (stored, inserted) = store.upsert_workspace_git_root(&first).await.unwrap();
    assert!(inserted);
    assert_eq!(stored.registered_by_agent_ids, [AgentId("a1".into())]);
    let mut again = root("root-other", "ws-g", "/src/a", &["a2"]);
    again.source = WorkspaceGitRootSource::Agent;
    again.repo_owner = Some(tenant.into());
    again.updated_at = T1.into();
    let (merged, inserted) = store.upsert_workspace_git_root(&again).await.unwrap();
    assert!(!inserted);
    assert_eq!(merged.id, rid);
    assert_eq!(merged.source, WorkspaceGitRootSource::Agent);
    assert_eq!(
        merged.registered_by_agent_ids,
        [AgentId("a1".into()), AgentId("a2".into())]
    );
    assert_eq!(store.get_workspace_git_root(&rid).await.unwrap(), merged);
    assert_eq!(
        store
            .find_workspace_git_root_by_path(&ws, "/src/a")
            .await
            .unwrap(),
        Some(merged.clone())
    );
    assert_eq!(
        store
            .find_workspace_git_root_by_path(&ws, "/src/none")
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.list_workspace_git_roots(&ws).await.unwrap(), [merged]);

    assert!(store
        .backfill_workspace_git_root_commit_sha(&rid, "sha1", T1)
        .await
        .unwrap());
    assert!(!store
        .backfill_workspace_git_root_commit_sha(&rid, "sha2", T1)
        .await
        .unwrap());
    let url = "https://github.com/acme/lib/pull/3";
    let mut with_pr = store.get_workspace_git_root(&rid).await.unwrap();
    with_pr.pr_url = Some(url.into());
    with_pr.pr_number = Some(3);
    store.update_workspace_git_root_pr(&with_pr).await.unwrap();
    store
        .update_workspace_git_root_pr_rebased(&mut with_pr, |root, stored| {
            assert_eq!(stored, None);
            root.pull_requests = Some(vec![pr(url, PullRequestStatus::Open)]);
        })
        .await
        .unwrap();
    let pool = store
        .project_workspace_git_root_pull_requests(&rid, T2, |pool| {
            pool.as_mut().unwrap()[0].status = PullRequestStatus::Closed;
            true
        })
        .await
        .unwrap();
    assert_eq!(pool.flatten().unwrap()[0].status, PullRequestStatus::Closed);
    assert_eq!(
        store
            .project_workspace_git_root_pull_requests(&rid, T2, |_| false)
            .await
            .unwrap(),
        None
    );
    let got = store.get_workspace_git_root(&rid).await.unwrap();
    assert_eq!(got.registered_commit_sha.as_deref(), Some("sha1"));
    assert_eq!(got.updated_at, T2);
    assert_eq!(
        store
            .list_workspace_git_roots_with_prs(false)
            .await
            .unwrap(),
        std::slice::from_ref(&got)
    );
    assert_eq!(
        store
            .list_workspace_git_roots_referencing_pr_url(&url.to_uppercase())
            .await
            .unwrap(),
        [got]
    );

    store.delete_workspace_git_root(&rid).await.unwrap();
    let gone = WorkspaceGitRootId("root-1".into());
    for err in [
        store.delete_workspace_git_root(&gone).await,
        store
            .update_workspace_git_root_pr(&root("root-1", "ws-g", "/x", &[]))
            .await,
        store
            .backfill_workspace_git_root_commit_sha(&gone, "s", T1)
            .await
            .map(|_| ()),
        store.get_workspace_git_root(&gone).await.map(|_| ()),
    ] {
        assert!(matches!(err, Err(Error::NotFound(_))), "{err:?}");
    }
    // Leave a root behind for the next tenant's run.
    store.upsert_workspace_git_root(&first).await.unwrap();
});

fn home(home_id: &str, node: Option<&str>, cloud: Option<&str>, state: HomeState) -> WorkspaceHome {
    WorkspaceHome {
        home_id: HomeId(home_id.into()),
        node_id: node.map(str::to_owned),
        cloud_machine_id: cloud.map(str::to_owned),
        name: "machine".into(),
        state,
        since: T0.into(),
        last_backup_at: None,
        backup_warning: false,
        setup: None,
    }
}

parity_test!(workspace_homes, |store, tenant| {
    let ws = wid("ws-h");
    let cloud = wid("ws-c");
    let h1 = HomeId("h1".into());
    let h2 = HomeId("h2".into());
    store
        .insert_workspace(&workspace("ws-h", T0))
        .await
        .unwrap();
    store
        .insert_workspace(&workspace("ws-c", T0))
        .await
        .unwrap();
    assert!(store.list_workspace_homes().await.unwrap().is_empty());
    assert!(store.homes_on_machine("n1").await.unwrap().is_empty());
    assert_eq!(store.get_workspace_home(&ws).await.unwrap(), None);

    let mut attached = home("h1", Some("n1"), None, HomeState::Attached);
    attached.name = tenant.into();
    store.set_workspace_home(&ws, &attached).await.unwrap();
    assert_eq!(store.get_workspace_home(&ws).await.unwrap(), Some(attached));
    assert!(store
        .set_workspace_home(&wid("missing"), &home("h", None, None, HomeState::Attached))
        .await
        .is_err());
    assert!(!store
        .set_workspace_home_state(&ws, &h1, HomeState::Attached, T1)
        .await
        .unwrap());
    assert!(store
        .set_workspace_home_state(&ws, &h1, HomeState::Moving, T1)
        .await
        .unwrap());
    assert!(store
        .move_workspace_home_state(&ws, &h1, HomeState::Moving, HomeState::Attached, T1)
        .await
        .unwrap());
    assert!(!store
        .move_workspace_home_state(&ws, &h1, HomeState::Moving, HomeState::Attached, T1)
        .await
        .unwrap());

    assert_eq!(
        store
            .record_workspace_home_backup_failure(&ws, &h1, T1)
            .await
            .unwrap()
            .as_deref(),
        Some(T1)
    );
    assert_eq!(
        store
            .record_workspace_home_backup_failure(&ws, &h1, T2)
            .await
            .unwrap()
            .as_deref(),
        Some(T1)
    );
    assert_eq!(
        store
            .record_workspace_home_backup_failure(&ws, &h2, T2)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .workspace_home_backup_failing_since(&ws, &h1)
            .await
            .unwrap()
            .as_deref(),
        Some(T1)
    );
    assert!(
        store
            .get_workspace_home(&ws)
            .await
            .unwrap()
            .unwrap()
            .backup_warning
    );
    assert!(!store
        .record_workspace_home_backup(&ws, &h1, "n2", T2)
        .await
        .unwrap());
    assert!(store
        .record_workspace_home_backup(&ws, &h1, "n1", T2)
        .await
        .unwrap());
    assert_eq!(
        store
            .workspace_home_backup_failing_since(&ws, &h1)
            .await
            .unwrap(),
        None
    );

    assert!(store
        .claim_workspace_home_auto_start(&ws, &h1, "n1", "lease", 1)
        .await
        .unwrap());
    assert!(!store
        .claim_workspace_home_auto_start(&ws, &h1, "n1", "lease", 1)
        .await
        .unwrap());
    assert!(store
        .claim_workspace_home_auto_start(&ws, &h1, "n1", "lease", 2)
        .await
        .unwrap());

    store
        .set_workspace_home(&cloud, &home("h2", None, Some("m1"), HomeState::Attached))
        .await
        .unwrap();
    assert!(store
        .mark_cloud_home_starting(&cloud, &h2, T1)
        .await
        .unwrap());
    assert_eq!(
        store.reset_unstarted_cloud_homes(T2).await.unwrap(),
        std::slice::from_ref(&cloud)
    );
    assert!(store
        .reset_unstarted_cloud_homes(T2)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .claim_cloud_home(&cloud, &h2, "m1", "n9")
        .await
        .unwrap());
    assert!(!store
        .claim_cloud_home(&cloud, &h2, "m1", "n8")
        .await
        .unwrap());
    assert!(!store
        .detach_cloud_home(&cloud, &h2, Some("n8"), T2)
        .await
        .unwrap());
    assert!(store
        .detach_cloud_home(&cloud, &h2, Some("n9"), T2)
        .await
        .unwrap());

    let on_n1 = store.homes_on_machine("n1").await.unwrap();
    assert_eq!(on_n1.len(), 1);
    assert_eq!(on_n1[0].0, ws);
    let all = store.list_workspace_homes().await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[&cloud].state, HomeState::Detached);
    assert_eq!(all[&cloud].node_id, None);
    if tenant == "decoy" {
        // A starting cloud home the subject's reset must not touch.
        let other = wid("ws-decoy-only");
        store
            .insert_workspace(&workspace("ws-decoy-only", T0))
            .await
            .unwrap();
        store
            .set_workspace_home(&other, &home("h3", None, Some("m3"), HomeState::Detached))
            .await
            .unwrap();
        assert!(store
            .mark_cloud_home_starting(&other, &HomeId("h3".into()), T1)
            .await
            .unwrap());
    }
});

parity_test!(workspace_context_ui_and_mcp, |store, tenant| {
    let ws = wid("ws-x");
    store
        .insert_workspace(&workspace("ws-x", T0))
        .await
        .unwrap();
    assert!(store
        .list_workspace_context_items(&ws)
        .await
        .unwrap()
        .is_empty());
    let item = |id: &str| -> ContextItem {
        serde_json::from_value(serde_json::json!({ "id": id, "kind": "note" })).unwrap()
    };
    let saved = store
        .replace_workspace_context_items(&ws, &[item("b"), item("a")])
        .await
        .unwrap();
    assert_eq!(
        saved.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["b", "a"]
    );
    let saved = store
        .replace_workspace_context_items(&ws, &[item("c")])
        .await
        .unwrap();
    assert_eq!(saved, [item("c")]);
    assert_eq!(
        store.list_workspace_context_items(&ws).await.unwrap(),
        saved
    );

    assert_eq!(store.get_workspace_ui_context(&ws).await.unwrap(), None);
    let ui = serde_json::json!({ "panes": [1, null], "x": { "y": "z" } });
    assert_eq!(
        store.update_workspace_ui_context(&ws, &ui).await.unwrap(),
        ui
    );
    let ui2 = serde_json::json!({ "panes": [], "tenant": tenant });
    store.update_workspace_ui_context(&ws, &ui2).await.unwrap();
    assert_eq!(
        store.get_workspace_ui_context(&ws).await.unwrap(),
        Some(ui2)
    );

    assert!(store
        .workspace_mcp_disabled_servers(&ws)
        .await
        .unwrap()
        .is_empty());
    for server in ["zeta", "Alpha", "beta", "beta"] {
        store
            .set_workspace_mcp_server_disabled(&ws, server, true)
            .await
            .unwrap();
    }
    store
        .set_workspace_mcp_server_disabled(&ws, "zeta", false)
        .await
        .unwrap();
    assert_eq!(
        store.workspace_mcp_disabled_servers(&ws).await.unwrap(),
        ["Alpha", "beta"]
    );
    assert!(store
        .workspace_mcp_server_disabled(&ws, "beta")
        .await
        .unwrap());
    assert!(!store
        .workspace_mcp_server_disabled(&ws, "zeta")
        .await
        .unwrap());
    assert!(matches!(
        store
            .set_workspace_mcp_server_disabled(&wid("missing"), "x", true)
            .await,
        Err(Error::NotFound(_))
    ));
    if tenant == "decoy" {
        store
            .set_workspace_mcp_server_disabled(&ws, "zeta", true)
            .await
            .unwrap();
    }
});

// ---- deletion cascade, token usage and host-admitted insert ----------------

fn agent(id: &str, ws: &str, tenant: &str) -> AgentSession {
    AgentSession {
        id: AgentId(id.into()),
        workspace_id: wid(ws),
        name: format!("{id} of {tenant}"),
        model: Some("opus".into()),
        status: AgentStatus::Idle,
        created_at: T0.into(),
        updated_at: T0.into(),
        ..AgentSession::default()
    }
}

fn machine_home(home_id: &str, node: &str) -> WorkspaceHome {
    WorkspaceHome {
        home_id: HomeId(home_id.into()),
        node_id: Some(node.into()),
        cloud_machine_id: None,
        name: "Mac".into(),
        state: HomeState::Attached,
        since: T0.into(),
        last_backup_at: None,
        backup_warning: false,
        setup: None,
    }
}

/// `ws-del` (a plain folder) with a row in every table its deletion touches:
/// agents `a1` and `a2` with their messages, payloads, queue, hooks,
/// monitors, wake markers and recovery history; scripts, attachments, notes
/// with their paging, comments, versions and attributions; registries,
/// drafts, a browser tab, private desktop records, agent preferences and a
/// moved home with a move in progress. `ws-keep` and its agent `k1` hold the
/// rows the deletion must leave: `k1`'s recovery row naming `ws-del`, its
/// wake markers with `ws-del`'s agents, its desktop record, and a comment of
/// `ws-del` with no note.
async fn seed_cascade(store: &Store, db: Option<&TestDb>, tenant: &str) {
    let mut del = workspace("ws-del", T0);
    del.plain_folder = true;
    del.repository_path = Some(format!("/folders/{tenant}"));
    store.insert_workspace(&del).await.unwrap();
    if store.get_workspace(&wid("ws-keep")).await.is_err() {
        store
            .insert_workspace(&workspace("ws-keep", T0))
            .await
            .unwrap();
        store
            .insert_agent_session(&agent("k1", "ws-keep", tenant))
            .await
            .unwrap();
        store.ensure_client(&ClientId("c1".into())).await.unwrap();
        // Rows of ws-del the deletion leaves: k1's recovery row, k1's desktop
        // record and a comment with no note.
        sql(
            store,
            db,
            tenant,
            &format!(
                "INSERT INTO interrupted_agent ({{t}}agent_id, workspace_id, prev_status, \
                 interrupted_at) VALUES ({{v}}'k1', 'ws-del', 'active', 'T0'); \
                 INSERT INTO settings ({{t}}key, value) \
                 VALUES ({{v}}'desktop.v1/request/r-keep', '{}'); \
                 INSERT INTO comment ({{t}}id, thread_id, note_id, workspace_id, kind, content, \
                 author, author_type, anchor_json, created_at, updated_at) VALUES \
                 ({{v}}'cm-loose', 'th', NULL, 'ws-del', 'note', 'c', 'a', 'human', '{{}}', \
                 'T0', 'T0')",
                json!({"workspaceId": "ws-keep", "agentId": "k1", "tenant": tenant})
            ),
        )
        .await;
    }
    for id in ["a1", "a2"] {
        store
            .insert_agent_session(&agent(id, "ws-del", tenant))
            .await
            .unwrap();
    }
    let heavy = json!([{
        "type": "tool_use", "id": "tu1", "name": "write",
        "input": { "body": format!("{tenant} {}", "x".repeat(20_000)) },
    }]);
    store
        .append_agent_message(&AgentId("a1".into()), "assistant", &heavy, T0)
        .await
        .unwrap();
    store
        .append_agent_message(
            &AgentId("a2".into()),
            "user",
            &json!([{"type": "text", "text": tenant}]),
            T0,
        )
        .await
        .unwrap();
    let owner = store.get_primary_principal().await.unwrap().id;
    let desktop = |agent: &str, ws: &str| {
        json!({"workspaceId": ws, "agentId": agent, "tenant": tenant}).to_string()
    };
    let statements = [
        "INSERT INTO interrupted_agent ({t}agent_id, workspace_id, prev_status, interrupted_at) \
         VALUES ({v}'a1', 'ws-del', 'active', 'T0'), ({v}'a-gone', 'ws-del', 'active', 'T0')"
            .to_owned(),
        format!(
            "INSERT INTO settings ({{t}}key, value) VALUES \
             ({{v}}'desktop.v1/request/r-del', '{}'), ({{v}}'desktop.v1/request/r-a2', '{}'), \
             ({{v}}'workspace.agentCreationPreferences:ws-del', '{{}}')",
            desktop("a1", "ws-del"),
            desktop("a2", "ws-del"),
        ),
        "INSERT INTO agent_queue ({t}id, agent_id, position, payload, created_at) \
         VALUES ({v}'q1', 'a1', 0, '{}', 'T0')"
            .to_owned(),
        "INSERT INTO hook ({t}hook_id, workspace_id, agent_id, name, code, delay_ms, state, \
         created_at) VALUES ({v}'hk1', 'ws-del', 'a1', 'n', 'c', 1, 'scheduled', 'T0')"
            .to_owned(),
        "INSERT INTO pr_monitor ({t}monitor_id, workspace_id, agent_id, repo_owner, repo_name, \
         pr_number, state, created_at, updated_at) \
         VALUES ({v}'m1', 'ws-del', 'a1', 'o', 'r', 1, 'active', 'T0', 'T0')"
            .to_owned(),
        "INSERT INTO completion_wake_delivery ({t}parent_agent_id, child_agent_id, \
         completion_identity, delivered_at) \
         VALUES ({v}'a1', 'a2', 'c', 'T0'), ({v}'k1', 'a2', 'c', 'T0')"
            .to_owned(),
        "INSERT INTO advisory_wake_delivery ({t}parent_agent_id, child_agent_id, delivered_at) \
         VALUES ({v}'a1', 'a2', 'T0'), ({v}'k1', 'a1', 'T0')"
            .to_owned(),
        "INSERT INTO script ({t}id, workspace_id, name, command, mode, source, created_at) \
         VALUES ({v}'s1', 'ws-del', 'n', 'c', 'command', 'agent', 'T0')"
            .to_owned(),
        "INSERT INTO attachments ({t}id, workspace_id, file_name, size, uploaded_at, \
         stored_path) VALUES ({v}'att1', 'ws-del', 'f', 1, 'T0', '/p')"
            .to_owned(),
        "INSERT INTO attachment_idempotency_keys ({t}workspace_id, key, attachment_id, \
         fingerprint, created_at) VALUES ({v}'ws-del', 'k1', 'att1', 'f', 'T0')"
            .to_owned(),
        "INSERT INTO note ({t}id, workspace_id, title, content, created_at, updated_at) \
         VALUES ({v}'n1', 'ws-del', 't', 'c', 'T0', 'T0')"
            .to_owned(),
        "INSERT INTO note ({t}id, workspace_id, title, content, created_at, updated_at, \
         parent_id) VALUES ({v}'n2', 'ws-del', 't', 'c', 'T0', 'T0', 'n1')"
            .to_owned(),
        "INSERT INTO comment ({t}id, thread_id, note_id, workspace_id, kind, content, author, \
         author_type, anchor_json, created_at, updated_at) VALUES \
         ({v}'cm1', 'th', 'n1', 'ws-del', 'note', 'c', 'a', 'human', '{}', 'T0', 'T0')"
            .to_owned(),
        "INSERT INTO note_version ({t}note_id, workspace_id, v, date, author_id, author_name, \
         author_type, title, content) VALUES ({v}'n1', 'ws-del', 1, 'T0', 'a', 'a', 'human', \
         't', 'c')"
            .to_owned(),
        "INSERT INTO note_line_attribution ({t}note_id, workspace_id, computed_at, \
         attributions_json) VALUES ({v}'n1', 'ws-del', 'T0', '[]')"
            .to_owned(),
        "INSERT INTO tracked_changes ({t}id, workspace_id, path, stage, status, created_at, \
         updated_at) VALUES ({v}'tc1', 'ws-del', 'p', 'unstaged', 'modified', 'T0', 'T0')"
            .to_owned(),
        "INSERT INTO diffs ({t}id, workspace_id, file_path, created_at, updated_at) \
         VALUES ({v}'d1', 'ws-del', 'p', 'T0', 'T0')"
            .to_owned(),
        "INSERT INTO delegation_group ({t}group_id, workspace_id, parent_agent_id, await_mode, \
         expected_agent_ids, created_at, updated_at) \
         VALUES ({v}'g1', 'ws-del', 'a1', 'all', '[]', 'T0', 'T0')"
            .to_owned(),
        "INSERT INTO task_agent_link ({t}workspace_id, note_id, task_key, task_text, agent_id, \
         created_at) VALUES ({v}'ws-del', 'n1', 'k', 't', 'a1', 0)"
            .to_owned(),
        "INSERT INTO agent_metrics ({t}agent_id, workspace_id, updated_at) \
         VALUES ({v}'a1', 'ws-del', 'T0')"
            .to_owned(),
        "INSERT INTO workspace_context_item ({t}workspace_id, id, ordinal, payload) \
         VALUES ({v}'ws-del', 'ci1', 0, '{}')"
            .to_owned(),
        "INSERT INTO workspace_git_root ({t}id, workspace_id, path, source, created_at, \
         updated_at) VALUES ({v}'gr1', 'ws-del', '/r', 'auto', 'T0', 'T0')"
            .to_owned(),
        "INSERT INTO pull_request_workspace ({t}pr_key, workspace_id, source, linked_at) \
         VALUES ({v}'pk1', 'ws-del', 'workspace', 'T0')"
            .to_owned(),
        format!(
            "INSERT INTO workspace_invite ({{t}}id, workspace_id, secret_hash, \
             created_by_principal_id, created_at, expires_at) \
             VALUES ({{v}}'inv1', 'ws-del', 'h', '{}', 'T0', 'T9')",
            owner.0
        ),
        "INSERT INTO workspace_mcp_disabled_server ({t}workspace_id, server_id, created_at) \
         VALUES ({v}'ws-del', 'srv', 'T0')"
            .to_owned(),
        "INSERT INTO draft ({t}workspace_id, agent_id, client_id, text, updated_at) \
         VALUES ({v}'ws-del', 'a1', 'c1', 'd', 'T0')"
            .to_owned(),
        "INSERT INTO home_move ({t}workspace_id, home_id, node_id, name, from_home_id, \
         from_state, from_since, started_at) \
         VALUES ({v}'ws-del', 'h-t', 'n3', 'Mac', 'h-b', 'attached', 'T0', 'T1')"
            .to_owned(),
    ];
    for stmt in &statements {
        sql(store, db, tenant, stmt).await;
    }
    if db.is_some() {
        // Retired from SQLite (dropped after startup); still a Postgres table.
        sql(
            store,
            db,
            tenant,
            "INSERT INTO note_page_head ({t}workspace_id, note_id, instance_id, current_rev, \
             generation) VALUES ({v}'ws-del', 'n1', 'i', 0, 'g'); \
             INSERT INTO note_page_entry ({t}workspace_id, note_id, collection, position, value) \
             VALUES ({v}'ws-del', 'n1', 'c', 0, 'v')",
        )
        .await;
    }
    let mut tab = BrowserTabInput {
        tab_id: "t1".into(),
        workspace_id: wid("ws-del"),
        url: format!("https://{tenant}.example/"),
        requested_url: None,
        title: None,
        owner_agent_id: None,
        owner_agent_name: None,
        visibility: BrowserTabVisibility::Visible,
        emulated_size: None,
        displayed: None,
    };
    tab.displayed = Some(true);
    store
        .upsert_browser_tab(&ClientId("c1".into()), tab)
        .await
        .unwrap();
    let ws = wid("ws-del");
    store
        .set_workspace_home(&ws, &machine_home("h-a", "n1"))
        .await
        .unwrap();
    store
        .switch_workspace_home(&ws, &machine_home("h-b", "n2"), T1)
        .await
        .unwrap();
}

parity_test!(workspace_delete_cascades, |store, tenant, db| {
    seed_cascade(&store, db, tenant).await;
    let ws = wid("ws-del");
    assert_eq!(
        store
            .get_home_stop(&HomeId("h-a".into()))
            .await
            .unwrap()
            .unwrap()
            .reason,
        HomeStopReason::Moved
    );

    let outcome = store.delete_workspace_with_outcome(&ws).await;
    outcome.result.unwrap();
    assert_eq!(
        outcome.disposition,
        RepositoryWorkspaceDeleteDisposition::Committed
    );
    let again = store.delete_workspace_with_outcome(&ws).await;
    assert!(matches!(again.result, Err(Error::NotFound(_))));
    assert_eq!(
        again.disposition,
        RepositoryWorkspaceDeleteDisposition::NoEffect
    );
    assert!(matches!(
        store.delete_workspace(&ws).await,
        Err(Error::NotFound(_))
    ));

    assert!(matches!(
        store.get_workspace(&ws).await,
        Err(Error::NotFound(_))
    ));
    assert!(store.workspace_id_ever_used(&ws).await.unwrap());
    assert!(store
        .plain_folder_paths()
        .await
        .unwrap()
        .contains(&format!("/folders/{tenant}")));
    for table in [
        "agent_session",
        "hook",
        "pr_monitor",
        "script",
        "attachments",
        "attachment_idempotency_keys",
        "note",
        "note_version",
        "note_line_attribution",
        "tracked_changes",
        "diffs",
        "delegation_group",
        "task_agent_link",
        "agent_metrics",
        "workspace_context_item",
        "workspace_git_root",
        "pull_request_workspace",
        "workspace_invite",
        "workspace_mcp_disabled_server",
        "draft",
        "browser_tab",
        "workspace_home",
        "home_move",
    ] {
        assert_eq!(
            count(&store, db, tenant, table, "workspace_id = 'ws-del'").await,
            0,
            "{table}"
        );
    }
    if db.is_some() {
        for table in ["note_page_head", "note_page_entry"] {
            assert_eq!(
                count(&store, db, tenant, table, "workspace_id = 'ws-del'").await,
                0,
                "{table}"
            );
        }
    }
    for (table, filter) in [
        ("agent_message", "agent_id IN ('a1', 'a2')"),
        ("agent_message_payload", "agent_id IN ('a1', 'a2')"),
        ("agent_queue", "agent_id = 'a1'"),
        ("completion_wake_delivery", "1 = 1"),
        ("advisory_wake_delivery", "1 = 1"),
        (
            "settings",
            "key = 'workspace.agentCreationPreferences:ws-del'",
        ),
    ] {
        assert_eq!(count(&store, db, tenant, table, filter).await, 0, "{table}");
    }
    // What stays: k1 and its recovery row naming ws-del, its desktop record,
    // the comment with no note, and the tombstone.
    assert_eq!(
        count(&store, db, tenant, "agent_session", "id = 'k1'").await,
        1
    );
    assert_eq!(
        count(
            &store,
            db,
            tenant,
            "interrupted_agent",
            "workspace_id = 'ws-del'"
        )
        .await,
        1
    );
    assert_eq!(
        count(&store, db, tenant, "interrupted_agent", "agent_id = 'k1'").await,
        1
    );
    assert_eq!(
        count(&store, db, tenant, "settings", "key LIKE 'desktop.v1/%'").await,
        1
    );
    assert_eq!(
        count(&store, db, tenant, "comment", "workspace_id = 'ws-del'").await,
        1
    );
    assert_eq!(
        count(&store, db, tenant, "comment", "id = 'cm-loose'").await,
        1
    );
    // The home retires: the current home and the move target get stops, and
    // the moved home's stop becomes a retirement.
    for home in ["h-a", "h-b", "h-t"] {
        let stop = store
            .get_home_stop(&HomeId(home.into()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (stop.reason, stop.orphaned_at),
            (HomeStopReason::Retired, None),
            "{home}"
        );
    }
    // The tab's displayed overlay goes with its row.
    sql(
        &store,
        db,
        tenant,
        "INSERT INTO browser_tab ({t}tab_id, workspace_id, host_client_id, url, created_at, \
         updated_at) VALUES ({v}'t1', 'ws-keep', 'c1', 'u', 'T0', 'T0')",
    )
    .await;
    assert_eq!(
        store
            .get_browser_tab("t1")
            .await
            .unwrap()
            .unwrap()
            .displayed,
        None
    );
    sql(
        &store,
        db,
        tenant,
        "DELETE FROM browser_tab WHERE {w} AND tab_id = 't1'",
    )
    .await;

    if tenant == "decoy" {
        // A full copy of every row the subject's deletion reaches, and a
        // moved stop for its retirement update to reach.
        seed_cascade(&store, db, tenant).await;
        sql(
            &store,
            db,
            tenant,
            "INSERT INTO home_stop ({t}home_id, workspace_id, node_id, stop_id, reason, \
             orphaned_at, recorded_at) \
             VALUES ({v}'h-moved', 'ws-del', 'n1', 's-m', 'moved', 'T0', 'T0')",
        )
        .await;
    }
});

fn usage(input: u64) -> TokenUsage {
    TokenUsage {
        totals: TokenUsageTotals {
            input_tokens: input,
            ..TokenUsageTotals::default()
        },
        ..TokenUsage::default()
    }
}

parity_test!(workspace_token_usage_recompute, |store, tenant| {
    let weight: u64 = if tenant == "decoy" { 1000 } else { 1 };
    let mut ws = workspace("ws-u", T0);
    ws.token_usage = Some(usage(weight));
    store.insert_workspace(&ws).await.unwrap();
    for id in ["u1", "u2"] {
        store
            .insert_agent_session(&agent(id, "ws-u", tenant))
            .await
            .unwrap();
    }
    let id = wid("ws-u");
    let written = store
        .update_workspace_token_usage(&id, |rows, current| {
            let mut names: Vec<&str> = rows.iter().map(|r| r.6.as_str()).collect();
            names.sort_unstable();
            assert_eq!(
                names,
                [format!("u1 of {tenant}"), format!("u2 of {tenant}")]
            );
            assert_eq!(current, Some(&usage(weight)));
            Some(usage(weight + 1))
        })
        .await
        .unwrap();
    assert_eq!(written, Some(usage(weight + 1)));
    assert_eq!(
        store.get_workspace(&id).await.unwrap().token_usage,
        Some(usage(weight + 1))
    );
    let declined = store
        .update_workspace_token_usage(&id, |_, current| {
            assert_eq!(current, Some(&usage(weight + 1)));
            None
        })
        .await
        .unwrap();
    assert_eq!(declined, None);
    assert_eq!(
        store.get_workspace(&id).await.unwrap().token_usage,
        Some(usage(weight + 1))
    );
    assert!(matches!(
        store
            .update_workspace_token_usage(&wid("ws-ghost"), |_, _| Some(usage(9)))
            .await,
        Err(Error::NotFound(_))
    ));
    if tenant == "decoy" {
        store
            .insert_workspace(&workspace("ws-ghost", T0))
            .await
            .unwrap();
        let mut reset = store.get_workspace(&id).await.unwrap();
        reset.token_usage = Some(usage(weight));
        store.update_workspace(&reset).await.unwrap();
    }
});

parity_test!(workspace_insert_with_host_admission, |store, tenant| {
    let owner = store.get_primary_principal().await.unwrap().id;
    if store
        .insert_principal_credential(&owner, "hash-h")
        .await
        .is_err()
    {
        panic!("credential insert failed");
    }
    let expected = store
        .repository_host_authority_snapshot(&owner, Some("hash-h"))
        .await
        .unwrap();

    let denied = store
        .insert_workspace_with_host_admission(
            &workspace("ws-h0", T0),
            None,
            &expected,
            Some("hash-h"),
            || Err(Error::Forbidden("retired".into())),
        )
        .await;
    assert!(matches!(denied, Err(Error::Forbidden(_))));
    assert!(matches!(
        store.get_workspace(&wid("ws-h0")).await,
        Err(Error::NotFound(_))
    ));

    let admitted = std::sync::atomic::AtomicBool::new(false);
    store
        .insert_workspace_with_host_admission(
            &workspace("ws-h1", T0),
            Some(true),
            &expected,
            Some("hash-h"),
            || {
                admitted.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .unwrap();
    assert!(admitted.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        store.get_workspace(&wid("ws-h1")).await.unwrap().title,
        "title ws-h1"
    );
    assert_eq!(
        store.workspace_auto_commit(&wid("ws-h1")).await.unwrap(),
        Some(true)
    );

    // Changed authority refuses before the callback runs.
    assert!(store.revoke_principal_credential("hash-h").await.unwrap());
    let stale = store
        .insert_workspace_with_host_admission(
            &workspace("ws-h2", T0),
            None,
            &expected,
            Some("hash-h"),
            || panic!("admitted on changed authority"),
        )
        .await;
    assert!(matches!(stale, Err(Error::Forbidden(_))));
    assert!(matches!(
        store.get_workspace(&wid("ws-h2")).await,
        Err(Error::NotFound(_))
    ));
    if tenant == "decoy" {
        store
            .insert_workspace(&workspace("ws-h0", T0))
            .await
            .unwrap();
        store
            .insert_workspace(&workspace("ws-h2", T0))
            .await
            .unwrap();
    }
});
