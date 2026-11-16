//! Workspace store methods on both backends: workspaces, git roots, homes,
//! context items, UI context and MCP disables. Bodies use fixed ids and
//! leave their rows behind, so a Postgres statement without its tenant
//! filter sees the decoy tenant's rows on the subject run.

use intent_core::{
    AgentId, ClientId, ContextItem, Error, HomeId, HomeState, PullRequestInfo, PullRequestStatus,
    Workspace, WorkspaceAttention, WorkspaceGitRoot, WorkspaceGitRootId, WorkspaceGitRootSource,
    WorkspaceHome, WorkspaceId, WorkspaceListFilter, WorkspaceStatus,
};
use intent_store::Store;

use crate::harness::parity_test;

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

parity_test!(workspace_scoped_column_writes, |store| {
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
    store.set_workspace_label(&id, Some("lbl")).await.unwrap();
    store
        .set_workspace_auto_archive_disabled(&id, true)
        .await
        .unwrap();
    assert_eq!(store.workspace_browser_client(&id).await.unwrap(), None);
    store
        .set_workspace_browser_client(&id, Some(&ClientId("c1".into())))
        .await
        .unwrap();
    assert_eq!(
        store.workspace_browser_client(&id).await.unwrap(),
        Some(ClientId("c1".into()))
    );
    let got = store.get_workspace(&id).await.unwrap();
    assert_eq!(got.label.as_deref(), Some("lbl"));
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
});

parity_test!(workspace_full_updates_and_branch_reconcile, |store| {
    let id = wid("ws-u");
    let mut ws = workspace("ws-u", T0);
    ws.last_activity = Some(T2.into());
    store.insert_workspace(&ws).await.unwrap();

    let mut edit = ws.clone();
    edit.title = "renamed".into();
    edit.branch = "ignored".into();
    edit.last_activity = Some(T1.into());
    edit.status = WorkspaceStatus::Archived;
    assert_eq!(store.update_workspace(&edit).await.unwrap(), "main");
    let got = store.get_workspace(&id).await.unwrap();
    assert_eq!(got.title, "renamed");
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
});

parity_test!(workspace_archive_retire_and_revive, |store| {
    let id = wid("ws-r");
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
    let mut archived = store.get_workspace(&id).await.unwrap();
    archived.archived = true;
    archived.status = WorkspaceStatus::Archived;
    // Archive columns move only through the scoped flips, so stage the
    // archived row by re-inserting it under a second id.
    let mut staged = archived.clone();
    staged.id = wid("ws-r2");
    store.insert_workspace(&staged).await.unwrap();
    let id2 = wid("ws-r2");

    assert_eq!(
        store.retired_workspace_for(Some(&id2), None).await.unwrap(),
        None
    );
    assert!(store.retire_workspace_if_archived(&id2, T1).await.unwrap());
    assert!(!store.retire_workspace_if_archived(&id2, T1).await.unwrap());
    let retired = store.get_workspace(&id2).await.unwrap();
    assert_eq!(retired.status, WorkspaceStatus::Retired);
    assert_eq!(retired.retired_at.as_deref(), Some(T1));
    assert_eq!(
        store.retired_workspace_for(Some(&id2), None).await.unwrap(),
        Some(id2.clone())
    );
    assert_eq!(
        store
            .retired_workspace_for(None, Some(&AgentId("no-agent".into())))
            .await
            .unwrap(),
        None
    );
    assert_eq!(ids(&list(&store, true, false).await), ["ws-r"]);
    assert_eq!(ids(&list(&store, false, true).await), ["ws-r", "ws-r2"]);
    assert!(!store
        .unarchive_workspace_if_archived(&id2, T1)
        .await
        .unwrap());

    assert!(!store
        .revive_workspace_if_retired(&id, "b", None, T2)
        .await
        .unwrap());
    assert!(store
        .revive_workspace_if_retired_with_notice(
            &id2,
            "revived",
            Some("abc"),
            T2,
            Some(("notice", "text", None))
        )
        .await
        .unwrap());
    let revived = store.get_workspace(&id2).await.unwrap();
    assert_eq!(revived.status, WorkspaceStatus::Active);
    assert!(!revived.archived);
    assert_eq!(revived.branch, "revived");
    assert_eq!(revived.base_commit_sha.as_deref(), Some("abc"));
    assert_eq!(revived.status_message, None);
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

parity_test!(workspace_pull_request_links, |store| {
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
            *active = Some(pr(url, PullRequestStatus::Merged));
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
    let settled = store
        .workspaces_with_settled_pull_requests(&[id.clone(), wid("ws-q")])
        .await
        .unwrap();
    assert_eq!(
        settled.into_iter().collect::<Vec<_>>(),
        std::slice::from_ref(&id)
    );
});

parity_test!(workspace_git_roots, |store| {
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
    again.repo_owner = Some("acme".into());
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
    }
}

parity_test!(workspace_homes, |store| {
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

    let attached = home("h1", Some("n1"), None, HomeState::Attached);
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
});

parity_test!(workspace_context_ui_and_mcp, |store| {
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
    let ui2 = serde_json::json!({ "panes": [] });
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
});
