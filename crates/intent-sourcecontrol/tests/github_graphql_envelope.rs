//! GraphQL envelope regression coverage against a mock GitHub host.
//!
//! `octocrab::Octocrab::graphql` already unwraps the GraphQL envelope and
//! returns its `data` payload, so a second unwrap in this crate turned every
//! successful GraphQL read into an error (`graphql response returned no data`)
//! and silently degraded `pr.snapshot` to its REST fallback, where thread
//! resolution state is unavailable and every thread counts as unresolved
//! (intent-hq/monorepo#1533).

use std::net::Ipv4Addr;
use std::sync::Arc;

use intent_sourcecontrol::{GitHubSourceControl, PageParams, RepoRef, SourceControl};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A mock GitHub API host that answers every `POST /graphql` with `body`.
struct MockGraphql {
    base_uri: String,
}

async fn spawn_mock_graphql(body: Value) -> MockGraphql {
    let body = serde_json::to_string(&body).expect("serialize mock body");
    spawn_mock_graphql_with(Arc::new(move |_| body.clone())).await
}

/// Like [`spawn_mock_graphql`], but the response is computed per request from
/// the raw request text (head + body) — lets a test answer the primary and
/// fallback shapes of a retried query differently.
async fn spawn_mock_graphql_with(
    respond: Arc<dyn Fn(&str) -> String + Send + Sync>,
) -> MockGraphql {
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
/// answer with the responder's JSON, and close.
async fn serve_conn(
    mut stream: TcpStream,
    respond: &(dyn Fn(&str) -> String + Send + Sync),
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
    let body = respond(&request);
    let resp = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await
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
    assert_eq!(signals.merge_state_status.as_deref(), Some("CLEAN"));
    assert!(signals.checks_known, "the rollup survived the retry");
}
