//! GraphQL envelope regression coverage against a mock GitHub host, plus the
//! REST `/search/issues` multi-repo scope tolerance that needs the same
//! loopback HTTP stub (status codes + request recording).
//!
//! `octocrab::Octocrab::graphql` already unwraps the GraphQL envelope and
//! returns its `data` payload, so a second unwrap in this crate turned every
//! successful GraphQL read into an error (`graphql response returned no data`)
//! and silently degraded `pr.snapshot` to its REST fallback, where thread
//! resolution state is unavailable and every thread counts as unresolved
//! (intent-hq/monorepo#1533).

use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use intent_sourcecontrol::{
    Error, GitHubSourceControl, IssueQuery, PageParams, PrQuery, RepoRef, SourceControl,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A mock GitHub API host that answers every `POST /graphql` with `body`.
struct MockGraphql {
    base_uri: String,
}

/// Per-request responder: raw request text (head + body) → `(status, body)`.
type Responder = Arc<dyn Fn(&str) -> (u16, String) + Send + Sync>;

async fn spawn_mock_graphql(body: Value) -> MockGraphql {
    let body = serde_json::to_string(&body).expect("serialize mock body");
    spawn_mock_graphql_with(Arc::new(move |_| body.clone())).await
}

/// Like [`spawn_mock_graphql`], but the response is computed per request from
/// the raw request text (head + body) — lets a test answer the primary and
/// fallback shapes of a retried query differently. Always `200 OK`.
async fn spawn_mock_graphql_with(
    respond: Arc<dyn Fn(&str) -> String + Send + Sync>,
) -> MockGraphql {
    spawn_mock_with(Arc::new(move |request| (200, respond(request)))).await
}

/// Loopback HTTP stub whose responder picks the status code too — the REST
/// error paths (422 / 404 / 403) need it.
async fn spawn_mock_with(respond: Responder) -> MockGraphql {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind mock graphql host");
    let port = listener.local_addr().expect("mock addr").port();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let respond = respond.clone();
            tokio::spawn(async move {
                let _ = serve_conn(stream, respond.as_ref()).await;
            });
        }
    });
    MockGraphql {
        base_uri: format!("http://127.0.0.1:{port}"),
    }
}

/// Minimal HTTP/1.1 handler: read one request (headers + content-length body),
/// answer with the responder's status + JSON, and close.
async fn serve_conn(
    mut stream: TcpStream,
    respond: &(dyn Fn(&str) -> (u16, String) + Send + Sync),
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    let (head_end, body_start) = loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break (pos, pos + 4);
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let content_length = head
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    while buf.len() < body_start + content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let request = String::from_utf8_lossy(&buf).to_string();
    let (status, body) = respond(&request);
    let reason = match status {
        200 => "OK",
        403 => "Forbidden",
        404 => "Not Found",
        422 => "Unprocessable Entity",
        _ => "Status",
    };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await
}

// ---------------------------------------------------------------------------
// REST `/search/issues` multi-repo scope: unreadable-repo tolerance.
// ---------------------------------------------------------------------------

/// The request target (`GET <path?query>`) with the query percent-decoded, so
/// assertions can read the `q=` search string as GitHub would.
fn request_target(request: &str) -> String {
    let target = request
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .unwrap_or_default();
    let bytes = target.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("00");
                out.push(u8::from_str_radix(hex, 16).unwrap_or(b'%'));
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

/// GitHub's whole-search rejection when a `repo:` qualifier names something
/// the token cannot read (422 → [`Error::Conflict`]).
fn unsearchable_422() -> (u16, String) {
    (
        422,
        json!({
            "message": "Validation Failed",
            "errors": [{
                "message": "The listed users and repositories cannot be searched either because the resources do not exist or you do not have permission to view them.",
                "resource": "Search",
                "field": "q",
                "code": "invalid"
            }],
            "documentation_url": "https://docs.github.com/v3/search/"
        })
        .to_string(),
    )
}

fn search_hit(owner: &str, repo: &str, number: u64) -> Value {
    json!({
        "number": number,
        "title": format!("{owner}/{repo} #{number}"),
        "state": "open",
        "html_url": format!("https://github.com/{owner}/{repo}/pull/{number}"),
        "user": { "login": "octocat" },
        "pull_request": { "url": format!("https://api.github.com/repos/{owner}/{repo}/pulls/{number}") },
        "created_at": "2026-09-01T00:00:00Z",
        "updated_at": "2026-09-02T00:00:00Z",
    })
}

/// Records every request target and answers from `respond`.
fn recording(
    seen: Arc<Mutex<Vec<String>>>,
    respond: impl Fn(&str) -> (u16, String) + Send + Sync + 'static,
) -> Responder {
    Arc::new(move |request: &str| {
        let target = request_target(request);
        seen.lock().unwrap().push(target.clone());
        respond(&target)
    })
}

/// A multi-repo scope naming repos the token cannot read: the first search
/// (`repo:a/b repo:c/d repo:e/f`) is rejected 422, each scoped repo is probed
/// once, the not-found (`c/d`) and forbidden (`e/f`) ones are dropped, and the
/// search is retried ONCE over the readable remainder (`repo:a/b` only) —
/// returning results, not an error.
#[tokio::test]
async fn multi_repo_search_drops_unreadable_repos_and_retries_once() {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let mock = spawn_mock_with(recording(seen.clone(), |target| {
        if target.starts_with("/search/issues") {
            if target.contains("repo:c/d") || target.contains("repo:e/f") {
                return unsearchable_422();
            }
            return (
                200,
                json!({ "total_count": 1, "incomplete_results": false,
                        "items": [search_hit("a", "b", 7)] })
                .to_string(),
            );
        }
        match target {
            "/repos/a/b" => (200, json!({ "full_name": "a/b" }).to_string()),
            "/repos/c/d" => (404, json!({ "message": "Not Found" }).to_string()),
            "/repos/e/f" => (
                403,
                json!({ "message": "Resource not accessible by personal access token" })
                    .to_string(),
            ),
            _ => (
                404,
                json!({ "message": format!("unexpected {target}") }).to_string(),
            ),
        }
    }))
    .await;
    let sc = GitHubSourceControl::new("token-not-a-real-secret", Some(&mock.base_uri))
        .expect("build github client");

    let page = sc
        .list_prs(
            &RepoRef::new("a", "b"),
            PrQuery {
                extra_repos: vec![RepoRef::new("c", "d"), RepoRef::new("e", "f")],
                ..PrQuery::default()
            },
        )
        .await
        .expect("retry over the readable remainder must succeed");

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].number, 7);
    assert_eq!(page.items[0].url, "https://github.com/a/b/pull/7");

    let seen = seen.lock().unwrap().clone();
    let searches: Vec<&String> = seen
        .iter()
        .filter(|t| t.starts_with("/search/issues"))
        .collect();
    assert_eq!(
        searches.len(),
        2,
        "one rejected search + one retry: {seen:?}"
    );
    assert!(
        searches[0].contains("q=is:pr repo:a/b repo:c/d repo:e/f is:open"),
        "first search spans the whole scope: {}",
        searches[0]
    );
    assert!(
        searches[1].contains("q=is:pr repo:a/b is:open"),
        "retry spans only the readable remainder: {}",
        searches[1]
    );
    assert!(
        !searches[1].contains("repo:c/d") && !searches[1].contains("repo:e/f"),
        "dropped repos must not be retried: {}",
        searches[1]
    );
    let probes: Vec<&String> = seen.iter().filter(|t| t.starts_with("/repos/")).collect();
    assert_eq!(
        probes,
        vec!["/repos/a/b", "/repos/c/d", "/repos/e/f"],
        "each scoped repo is probed exactly once, in scope order"
    );
}

/// The negative paths: when the retry over the readable remainder is still
/// rejected, the 422 surfaces as [`Error::Conflict`]; and when every probe
/// says readable (nothing droppable) the original rejection surfaces with NO
/// retry at all.
#[tokio::test]
async fn multi_repo_search_surfaces_rejection_when_nothing_helps() {
    // Retry still 422s → error surfaces (issues surface, same helper).
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let mock = spawn_mock_with(recording(seen.clone(), |target| {
        if target.starts_with("/search/issues") {
            return unsearchable_422();
        }
        match target {
            "/repos/a/b" => (200, json!({ "full_name": "a/b" }).to_string()),
            _ => (404, json!({ "message": "Not Found" }).to_string()),
        }
    }))
    .await;
    let sc = GitHubSourceControl::new("token-not-a-real-secret", Some(&mock.base_uri))
        .expect("build github client");
    let err = sc
        .list_issues(
            &RepoRef::new("a", "b"),
            IssueQuery {
                extra_repos: vec![RepoRef::new("c", "d")],
                ..IssueQuery::default()
            },
        )
        .await
        .expect_err("a retry that is still rejected must fail");
    assert!(matches!(err, Error::Conflict(_)), "{err:?}");
    let seen = seen.lock().unwrap().clone();
    let searches = seen
        .iter()
        .filter(|t| t.starts_with("/search/issues"))
        .count();
    assert_eq!(searches, 2, "exactly one retry, never more: {seen:?}");
    assert!(
        seen.iter().any(|t| t == "/repos/c/d"),
        "the unreadable repo was probed: {seen:?}"
    );

    // Nothing droppable (every probe readable) → original error, no retry.
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let mock = spawn_mock_with(recording(seen.clone(), |target| {
        if target.starts_with("/search/issues") {
            return unsearchable_422();
        }
        (200, json!({ "full_name": target }).to_string())
    }))
    .await;
    let sc = GitHubSourceControl::new("token-not-a-real-secret", Some(&mock.base_uri))
        .expect("build github client");
    let err = sc
        .list_prs(
            &RepoRef::new("a", "b"),
            PrQuery {
                extra_repos: vec![RepoRef::new("c", "d")],
                ..PrQuery::default()
            },
        )
        .await
        .expect_err("nothing droppable surfaces the original rejection");
    assert!(matches!(err, Error::Conflict(_)), "{err:?}");
    let seen = seen.lock().unwrap().clone();
    let searches = seen
        .iter()
        .filter(|t| t.starts_with("/search/issues"))
        .count();
    assert_eq!(
        searches, 1,
        "no retry when no repo can be dropped: {seen:?}"
    );
    assert_eq!(
        seen.iter().filter(|t| t.starts_with("/repos/")).count(),
        2,
        "both scoped repos were probed: {seen:?}"
    );

    // A single-repo scope never probes: the 422 surfaces straight away.
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let mock = spawn_mock_with(recording(seen.clone(), |_| unsearchable_422())).await;
    let sc = GitHubSourceControl::new("token-not-a-real-secret", Some(&mock.base_uri))
        .expect("build github client");
    let err = sc
        .list_prs(
            &RepoRef::new("a", "b"),
            PrQuery {
                search: Some("needle".into()),
                ..PrQuery::default()
            },
        )
        .await
        .expect_err("single-repo rejection surfaces unchanged");
    assert!(matches!(err, Error::Conflict(_)), "{err:?}");
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "one search, no probes: {seen:?}");
    assert!(seen[0].starts_with("/search/issues"), "{seen:?}");

    // A 422 unrelated to scope readability (here: a page past GitHub's
    // 1000-result window) on a multi-repo scope surfaces as-is — no probes,
    // no retry — and its detail is carried on the error message.
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let mock = spawn_mock_with(recording(seen.clone(), |_| {
        (
            422,
            json!({
                "message": "Validation Failed",
                "errors": [{
                    "message": "Only the first 1000 search results are available",
                    "resource": "Search",
                    "field": "q",
                    "code": "invalid"
                }],
                "documentation_url": "https://docs.github.com/v3/search/"
            })
            .to_string(),
        )
    }))
    .await;
    let sc = GitHubSourceControl::new("token-not-a-real-secret", Some(&mock.base_uri))
        .expect("build github client");
    let err = sc
        .list_prs(
            &RepoRef::new("a", "b"),
            PrQuery {
                extra_repos: vec![RepoRef::new("c", "d")],
                ..PrQuery::default()
            },
        )
        .await
        .expect_err("an unrelated 422 surfaces without probing");
    match &err {
        Error::Conflict(msg) => assert!(
            msg.contains("Only the first 1000 search results are available"),
            "422 detail folded into the message: {msg}"
        ),
        other => panic!("expected Conflict, got {other:?}"),
    }
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "one search, no probes, no retry: {seen:?}");
    assert!(seen[0].starts_with("/search/issues"), "{seen:?}");
}

fn review_threads_envelope() -> Value {
    json!({
        "data": {
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "pageInfo": { "hasNextPage": false, "endCursor": "Y3Vyc29yOjI=" },
                        "nodes": [
                            {
                                "id": "PRRT_resolved",
                                "isResolved": true,
                                "comments": { "nodes": [{
                                    "id": "PRRC_1", "body": "nit", "author": { "login": "octocat" },
                                    "path": "src/lib.rs", "line": 12, "createdAt": "2026-08-06T00:00:00Z"
                                }] }
                            },
                            {
                                "id": "PRRT_open",
                                "isResolved": false,
                                "comments": { "nodes": [{
                                    "id": "PRRC_2", "body": "please fix", "author": { "login": "octocat" },
                                    "path": "src/lib.rs", "line": 34, "createdAt": "2026-08-06T00:01:00Z"
                                }] }
                            }
                        ]
                    }
                }
            }
        }
    })
}

/// The success path: a real GitHub `{ "data": ... }` envelope must yield the
/// review threads with their `isResolved` state, not a decode error.
#[tokio::test]
async fn get_review_threads_parses_graphql_data_envelope() {
    let mock = spawn_mock_graphql(review_threads_envelope()).await;
    let sc = GitHubSourceControl::new("token-not-a-real-secret", Some(&mock.base_uri))
        .expect("build github client");

    let page = sc
        .get_review_threads(
            &RepoRef::new("intent-hq", "intentd"),
            928,
            PageParams::first(100),
        )
        .await
        .expect("graphql review threads");

    assert_eq!(page.next_cursor, None);
    let resolution: Vec<(String, bool)> = page
        .items
        .iter()
        .map(|t| (t.id.clone(), t.is_resolved))
        .collect();
    assert_eq!(
        resolution,
        vec![
            ("PRRT_resolved".to_string(), true),
            ("PRRT_open".to_string(), false)
        ]
    );
    assert_eq!(page.items[0].comments.len(), 1);
    assert_eq!(page.items[0].comments[0].path, "src/lib.rs");
}

/// A GraphQL error envelope must still surface as an error.
#[tokio::test]
async fn get_review_threads_surfaces_graphql_errors() {
    let mock = spawn_mock_graphql(json!({
        "data": null,
        "errors": [{ "message": "Could not resolve to a Repository" }]
    }))
    .await;
    let sc = GitHubSourceControl::new("token-not-a-real-secret", Some(&mock.base_uri))
        .expect("build github client");

    let err = sc
        .get_review_threads(
            &RepoRef::new("intent-hq", "nope"),
            928,
            PageParams::first(100),
        )
        .await
        .expect_err("graphql error envelope must fail");
    assert!(
        err.to_string()
            .contains("Could not resolve to a Repository"),
        "error should carry the GraphQL message: {err}"
    );
}

/// Schema tolerance for hosts that predate merge queues (older GHES): the
/// host rejects the WHOLE merge-requirements query over the unknown
/// `isInMergeQueue` field, and the probe retries once without that selection —
/// the signal degrades to `None` instead of failing the entire checklist.
#[tokio::test]
async fn merge_requirements_retries_without_is_in_merge_queue_on_old_schemas() {
    let mock = spawn_mock_graphql_with(Arc::new(|request: &str| {
        if request.contains("isInMergeQueue") {
            // The primary query names the field the schema lacks.
            json!({
                "data": null,
                "errors": [{
                    "message": "Field 'isInMergeQueue' doesn't exist on type 'PullRequest'"
                }]
            })
            .to_string()
        } else {
            // The degraded retry succeeds with the remaining signals.
            json!({
                "data": {
                    "repository": {
                        "pullRequest": {
                            "mergeStateStatus": "CLEAN",
                            "reviewDecision": "APPROVED",
                            "commits": { "nodes": [{ "commit": { "statusCheckRollup": {
                                "contexts": { "nodes": [] }
                            } } }] }
                        }
                    }
                }
            })
            .to_string()
        }
    }))
    .await;
    let sc = GitHubSourceControl::new("token-not-a-real-secret", Some(&mock.base_uri))
        .expect("build github client");

    let signals = sc
        .merge_requirements(&RepoRef::new("intent-hq", "intentd"), 928)
        .await
        .expect("degraded retry must succeed");
    assert_eq!(signals.is_in_merge_queue, None, "signal degrades to None");
    assert_eq!(
        signals.merge_queue_removal, None,
        "removal event degrades to None alongside the flag"
    );
    assert_eq!(signals.merge_state_status.as_deref(), Some("CLEAN"));
    assert!(signals.checks_known, "the rollup survived the retry");
}

/// The success path fetches the PR's latest merge-queue removal event in the
/// same round trip and surfaces it as `merge_queue_removal`; an empty
/// timeline window (never ejected) yields `None`.
#[tokio::test]
async fn merge_requirements_parses_merge_queue_removal_event() {
    let envelope = |timeline_nodes: Value| {
        json!({
            "data": {
                "repository": {
                    "pullRequest": {
                        "mergeStateStatus": "CLEAN",
                        "isInMergeQueue": false,
                        "timelineItems": { "nodes": timeline_nodes },
                        "reviewDecision": "APPROVED",
                        "commits": { "nodes": [{ "commit": { "statusCheckRollup": {
                            "contexts": { "nodes": [] }
                        } } }] }
                    }
                }
            }
        })
    };

    let mock = spawn_mock_graphql(envelope(json!([{
        "createdAt": "2026-08-26T22:26:36Z",
        "reason": "failed_checks"
    }])))
    .await;
    let sc = GitHubSourceControl::new("token-not-a-real-secret", Some(&mock.base_uri))
        .expect("build github client");
    let signals = sc
        .merge_requirements(&RepoRef::new("intent-hq", "intentd"), 1517)
        .await
        .expect("merge requirements");
    let removal = signals.merge_queue_removal.expect("removal event surfaced");
    assert_eq!(removal.at, "2026-08-26T22:26:36Z");
    assert_eq!(removal.reason.as_deref(), Some("failed_checks"));
    assert_eq!(signals.is_in_merge_queue, Some(false));

    let mock = spawn_mock_graphql(envelope(json!([]))).await;
    let sc = GitHubSourceControl::new("token-not-a-real-secret", Some(&mock.base_uri))
        .expect("build github client");
    let signals = sc
        .merge_requirements(&RepoRef::new("intent-hq", "intentd"), 1517)
        .await
        .expect("merge requirements");
    assert_eq!(signals.merge_queue_removal, None, "never ejected");
}
