//! `ws.crossWorkspace.search`: sibling-scoped workspace search by free text
//! (metadata + note FTS) and by PR reference (PR <-> workspace link index).

use intent_core::{
    CrossWorkspaceSearch, PullRequestSelector, WorkspaceApi, WorkspaceId, WorkspaceStatus,
};
use intent_store::Store;
use serde_json::Value;

use super::{note, workspace, TempDb};
use crate::Services;

const INTENTD_7: &str = "https://github.com/intent-hq/intentd/pull/7";
const INTENT_7: &str = "https://github.com/intent-hq/intent/pull/7";

struct Harness {
    _tmp: TempDb,
    svc: Services,
    caller: WorkspaceId,
}

/// Caller and siblings in `/repo/a`; `ws-other` is in another repository and
/// matches every probe, so its absence proves the sibling scope.
async fn harness() -> Harness {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let mk = |id: &str, title: &str, repo: &str, pr: Option<&str>| {
        let mut w = workspace(&WorkspaceId::from(id));
        w.title = title.to_string();
        w.repository_path = Some(repo.to_string());
        w.pr_url = pr.map(str::to_string);
        w
    };
    let caller = mk("ws-caller", "Fix login caller", "/repo/a", Some(INTENTD_7));
    let login = {
        let mut w = mk("ws-login", "Fix login bug", "/repo/a", Some(INTENTD_7));
        w.branch = "fix/login-redirect".to_string();
        w
    };
    let dark = {
        let mut w = mk("ws-dark", "Dark launch", "/repo/a", Some(INTENT_7));
        w.status_message = Some("Waiting for review".to_string());
        w
    };
    let notes_only = mk("ws-notes", "Settings polish", "/repo/a", None);
    let retired = {
        let mut w = mk("ws-retired", "Old login spike", "/repo/a", None);
        w.status = WorkspaceStatus::Retired;
        w
    };
    let other = mk(
        "ws-other",
        "Fix login elsewhere",
        "/repo/b",
        Some(INTENTD_7),
    );
    for w in [&caller, &login, &dark, &notes_only, &retired, &other] {
        store.insert_workspace(w).await.expect("insert workspace");
    }
    store
        .insert_note(&note(
            &WorkspaceId::from("ws-notes"),
            "n-1",
            "Investigated the login redirect loop",
        ))
        .await
        .expect("note");
    store
        .insert_note(&note(
            &WorkspaceId::from("ws-other"),
            "n-2",
            "login redirect loop too",
        ))
        .await
        .expect("other note");
    Harness {
        _tmp: tmp,
        svc: Services::new_isolated(store),
        caller: caller.id,
    }
}

fn search(query: Option<&str>, pr: Option<&str>) -> CrossWorkspaceSearch {
    CrossWorkspaceSearch {
        query: query.map(str::to_string),
        pr: pr.map(|p| PullRequestSelector::parse(p).expect("selector")),
        include_retired: false,
        limit: 20,
    }
}

async fn ids(h: &Harness, s: CrossWorkspaceSearch) -> Vec<String> {
    let v = h
        .svc
        .cross_workspace_search(h.caller.clone(), s)
        .await
        .expect("search");
    v["matches"]
        .as_array()
        .expect("matches")
        .iter()
        .map(|m| m["id"].as_str().unwrap().to_string())
        .collect()
}

#[intent_test_macros::daemon_test]
async fn free_text_ranks_metadata_above_notes_and_stays_in_the_repository() {
    let h = harness().await;
    let v = h
        .svc
        .cross_workspace_search(h.caller.clone(), search(Some("login"), None))
        .await
        .expect("search");
    let matches = v["matches"].as_array().unwrap();
    let got: Vec<&str> = matches.iter().map(|m| m["id"].as_str().unwrap()).collect();
    assert_eq!(got, ["ws-login", "ws-notes"]);
    assert_eq!(v["truncated"], false);
    let on: Vec<&str> = matches[0]["matchedOn"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect();
    assert_eq!(on, ["title", "branch"]);
    assert_eq!(matches[1]["matchedOn"][0], "notes");
    assert_eq!(matches[1]["notes"][0]["noteId"], "n-1");
    assert!(matches[1]["notes"][0]["preview"]
        .as_str()
        .unwrap()
        .contains("login"));

    // Every term must match; terms may span fields.
    assert_eq!(
        ids(&h, search(Some("dark review"), None)).await,
        ["ws-dark"]
    );
    assert!(ids(&h, search(Some("dark nonexistentterm"), None))
        .await
        .is_empty());
}

#[intent_test_macros::daemon_test]
async fn pr_reference_forms_resolve_through_the_link_index() {
    let h = harness().await;
    assert_eq!(ids(&h, search(None, Some("7"))).await.len(), 2);
    assert_eq!(
        ids(&h, search(None, Some("intent-hq/intentd#7"))).await,
        ["ws-login"]
    );
    assert_eq!(ids(&h, search(None, Some(INTENT_7))).await, ["ws-dark"]);
    assert!(ids(&h, search(None, Some("#8"))).await.is_empty());

    let v = h
        .svc
        .cross_workspace_search(h.caller.clone(), search(None, Some("#7")))
        .await
        .unwrap();
    let pr = &v["matches"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["id"] == "ws-dark")
        .unwrap()["pullRequests"][0];
    assert_eq!(pr["key"], "github.com/intent-hq/intent/pull/7");
    assert_eq!(pr["url"], "https://github.com/intent-hq/intent/pull/7");
    assert_eq!(pr["number"], 7);

    // Query and PR together must both match.
    assert_eq!(ids(&h, search(Some("dark"), Some("#7"))).await, ["ws-dark"]);
    assert!(ids(&h, search(Some("settings"), Some("#7")))
        .await
        .is_empty());
}

#[intent_test_macros::daemon_test]
async fn retired_siblings_are_opt_in_and_limit_reports_truncation() {
    let h = harness().await;
    assert!(!ids(&h, search(Some("spike"), None))
        .await
        .contains(&"ws-retired".to_string()));
    let mut s = search(Some("spike"), None);
    s.include_retired = true;
    assert_eq!(ids(&h, s).await, ["ws-retired"]);

    let mut s = search(None, Some("7"));
    s.limit = 1;
    let v = h
        .svc
        .cross_workspace_search(h.caller.clone(), s)
        .await
        .unwrap();
    assert_eq!(v["matches"].as_array().map(Vec::len), Some(1));
    assert_eq!(v["truncated"], Value::Bool(true));
}

#[intent_test_macros::daemon_test]
async fn search_requires_a_criterion_and_a_repository() {
    let h = harness().await;
    let err = h
        .svc
        .cross_workspace_search(h.caller.clone(), search(Some("   "), None))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("non-empty query"), "{err}");

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let lone = WorkspaceId::from("ws-lone");
    store.insert_workspace(&workspace(&lone)).await.unwrap();
    let svc = Services::new_isolated(store);
    let err = svc
        .cross_workspace_search(lone, search(Some("x"), None))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not associated with a repository"),
        "{err}"
    );
}
