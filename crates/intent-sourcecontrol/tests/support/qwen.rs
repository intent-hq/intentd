//! Public check payloads captured for intent#5881. Only check nodes, head SHA,
//! state and updatedAt are captured. Surrounding PR/review/rule fields and REST
//! projections are synthetic controls, not evidence from the affected daemon.
//! Shared with the service tests so they exercise the real GitHub adapter.

// Each integration suite exercises a different subset of this shared fixture.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use intent_sourcecontrol::GitHubSourceControl;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadMode {
    Folded,
    Standalone,
    Rest,
    Degraded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckFault {
    ContinuationError,
    RateLimit,
    MissingCursor,
    RepeatedCursor,
    MissingPageInfo,
    HeadChanged,
    CommitChanged,
    CountChanged,
    NullNodes,
    Endless,
}

pub struct State {
    pub pr: Value,
    pub nodes: Vec<Value>,
    pub mode: ReadMode,
    pub fault: Option<CheckFault>,
    pub rest_unreadable: bool,
    pub rest_fault: Option<CheckFault>,
    pub rest_head: Option<String>,
}

impl State {
    pub fn captured(number: u64) -> Self {
        let capture: Value =
            serde_json::from_str(include_str!("../fixtures/qwen_checks_5881.json")).unwrap();
        let pr = capture["firstPages"][format!("p{number}")].clone();
        let pointer = "/commits/nodes/0/commit/statusCheckRollup/contexts/nodes";
        let mut nodes = pr.pointer(pointer).unwrap().as_array().unwrap().clone();
        if number == 11506 {
            assert_eq!(pr["headRefOid"], capture["remaining11506"]["headRefOid"]);
            nodes.extend(
                capture["remaining11506"]
                    .pointer(pointer)
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .clone(),
            );
        }
        assert_eq!(nodes.len(), if number == 10978 { 40 } else { 136 });
        Self {
            pr,
            nodes,
            mode: ReadMode::Folded,
            fault: None,
            rest_unreadable: false,
            rest_fault: None,
            rest_head: None,
        }
    }

    fn graphql_pr(&self, offset: usize, query: &str) -> Value {
        let mut pr = self.pr.clone();
        let number = pr["number"].as_u64().unwrap();
        let start = offset.min(self.nodes.len());
        let end = (start + 100).min(self.nodes.len());
        pr["url"] = json!(format!("https://github.com/QwenLM/qwen-code/pull/{number}"));
        pr["title"] = json!("Qwen captured checks");
        pr["body"] = json!("");
        pr["isDraft"] = json!(false);
        pr["headRefName"] = json!("fixture");
        pr["baseRefName"] = json!("main");
        pr["author"] = json!({"login": "fixture"});
        pr["mergeable"] = json!("MERGEABLE");
        pr["isInMergeQueue"] = json!(false);
        pr["timelineItems"] = json!({"nodes": []});
        pr["reviewDecision"] = Value::Null;
        pr["reviews"] = json!({"nodes": [], "pageInfo": {"hasPreviousPage": false}});
        pr["reviewThreads"] = json!({"nodes": [], "pageInfo": {"hasNextPage": false}});
        pr["comments"] = json!({"totalCount": 0});
        // Required flags were not selected in the capture. All checks are
        // optional unless a test explicitly adds a synthetic required flag.
        pr["commits"]["nodes"][0]["commit"]["statusCheckRollup"]["contexts"] = json!({
            "totalCount": self.nodes.len(),
            "pageInfo": {"hasNextPage": end < self.nodes.len(), "endCursor": format!("page-{end}")},
            "nodes": self.nodes[start..end],
        });
        pr["commits"]["nodes"][0]["commit"]["oid"] = pr["headRefOid"].clone();
        let contexts = &mut pr["commits"]["nodes"][0]["commit"]["statusCheckRollup"]["contexts"];
        // Do not invent selections absent from the request. Before the fix the
        // old query therefore still returns its (truncated) nodes, without
        // getting a pageInfo the real server would never have supplied.
        let selection = query
            .split("contexts(")
            .nth(1)
            .unwrap()
            .split_once(')')
            .unwrap()
            .1;
        if !selection.contains("pageInfo{hasNextPageendCursor}") {
            contexts.as_object_mut().unwrap().remove("pageInfo");
        }
        match self.fault {
            Some(CheckFault::MissingCursor) => contexts["pageInfo"]["endCursor"] = Value::Null,
            Some(CheckFault::MissingPageInfo) => {
                contexts.as_object_mut().unwrap().remove("pageInfo");
            }
            Some(CheckFault::NullNodes) => contexts["nodes"] = Value::Null,
            Some(CheckFault::RepeatedCursor) if offset > 0 => {
                contexts["pageInfo"] = json!({"hasNextPage": true, "endCursor": "page-100"});
            }
            Some(CheckFault::CountChanged) if offset > 0 => contexts["totalCount"] = json!(999),
            Some(CheckFault::Endless) => {
                contexts["totalCount"] = json!(100_000);
                contexts["nodes"] = json!([self.nodes[0].clone()]);
                contexts["pageInfo"] =
                    json!({"hasNextPage": true, "endCursor": format!("page-{}", offset + 1)});
            }
            _ => {}
        }
        if offset > 0 {
            match self.fault {
                Some(CheckFault::HeadChanged) => {
                    pr["headRefOid"] = json!("different-head");
                    pr["commits"]["nodes"][0]["commit"]["oid"] = json!("different-head");
                }
                Some(CheckFault::CommitChanged) => {
                    pr["commits"]["nodes"][0]["commit"]["oid"] = json!("different-head");
                }
                _ => {}
            }
        }
        pr
    }

    fn rest_pr(&self) -> Value {
        let number = self.pr["number"].as_u64().unwrap();
        json!({
            "number": number, "title": "Qwen captured checks", "state": if self.pr["state"] == "OPEN" { "open" } else { "closed" },
            "html_url": format!("https://github.com/QwenLM/qwen-code/pull/{number}"),
            "draft": false, "head": {"ref": "fixture", "sha": self.rest_head.as_ref().map_or_else(|| self.pr["headRefOid"].clone(), |h| json!(h))},
            "base": {"ref": "main"}, "user": {"login": "fixture"},
            "mergeable": true, "mergeable_state": "blocked", "updated_at": self.pr["updatedAt"],
        })
    }

    fn respond(&self, target: &str, body: &Value) -> (u16, Value) {
        if target == "/graphql" {
            let query = body["query"].as_str().unwrap();
            if (query.contains("GetPrObservation") && self.mode != ReadMode::Folded)
                || (query.contains("GetMergeRequirements")
                    && matches!(self.mode, ReadMode::Rest | ReadMode::Degraded))
            {
                return (
                    200,
                    json!({"errors": [{"message": "fixture check probe unavailable"}]}),
                );
            }
            let compact: String = query.chars().filter(|c| !c.is_whitespace()).collect();
            if !compact.contains("contexts(") {
                return (
                    200,
                    json!({"data": {"repository": {"pullRequest": self.pr}}}),
                );
            }
            let offset = bound_check_cursor(&compact, &body["variables"]);
            if offset > 0 {
                match self.fault {
                    Some(CheckFault::ContinuationError) => {
                        return (
                            200,
                            json!({"errors": [{"message": "continuation unavailable"}]}),
                        )
                    }
                    Some(CheckFault::RateLimit) => {
                        return (
                            200,
                            json!({"errors": [{"type": "RATE_LIMITED", "message": "API rate limit exceeded"}]}),
                        )
                    }
                    _ => {}
                }
            }
            return (
                200,
                json!({"data": {"repository": {"pullRequest": self.graphql_pr(offset, &compact)}}}),
            );
        }
        if target.contains("/check-runs") {
            if self.mode == ReadMode::Degraded || self.rest_unreadable {
                return (404, json!({"message": "fixture checks unavailable"}));
            }
            let page = target
                .split('?')
                .nth(1)
                .unwrap_or("")
                .split('&')
                .find_map(|p| p.strip_prefix("page="))
                .unwrap_or("1")
                .parse::<usize>()
                .unwrap();
            match self.rest_fault {
                Some(CheckFault::ContinuationError) if page > 1 => {
                    return (404, json!({"message": "continuation unavailable"}))
                }
                Some(CheckFault::RateLimit) if page > 1 => {
                    return (403, json!({"message": "API rate limit exceeded"}))
                }
                Some(CheckFault::NullNodes) => return (200, json!({})),
                _ => {}
            }
            let runs: Vec<Value> = self
                .nodes
                .iter()
                .filter(|n| n["__typename"] == "CheckRun")
                .skip(if self.rest_fault == Some(CheckFault::Endless) {
                    0
                } else {
                    (page - 1) * 100
                })
                .take(100)
                .map(|n| {
                    json!({
                        "name": n["name"],
                        "status": n["status"].as_str().unwrap().to_ascii_lowercase(),
                        "conclusion": n["conclusion"].as_str().map(str::to_ascii_lowercase),
                        "started_at": n["startedAt"], "details_url": n["detailsUrl"],
                    })
                })
                .collect();
            let runs = if self.rest_fault == Some(CheckFault::Endless) {
                vec![runs[0].clone(); 100]
            } else {
                runs
            };
            return (200, json!({"check_runs": runs}));
        }
        if target.contains("/rules/branches/")
            || target.contains("/comments")
            || target.contains("/reviews")
        {
            return (200, json!([]));
        }
        if target.contains("/pulls/") {
            return (200, self.rest_pr());
        }
        // The monitor's quota probes need both enforced resources healthy.
        if target == "/user" || target.starts_with("/rate_limit") {
            return (200, json!({}));
        }
        panic!("unexpected Qwen fixture request: {target}");
    }
}

// A next page is served ONLY when a declared variable is actually bound to
// contexts(after:...). An unrelated variable or inline cursor token is not
// enough to make a broken paginator pass these tests.
fn bound_check_cursor(query: &str, vars: &Value) -> usize {
    let args = query
        .split("contexts(")
        .nth(1)
        .unwrap()
        .split(')')
        .next()
        .unwrap();
    let Some(after) = args.split(',').find_map(|a| a.strip_prefix("after:")) else {
        return 0;
    };
    let var = after.strip_prefix('$').expect("cursor must be a variable");
    assert!(
        query.contains(&format!("${var}:String")),
        "undeclared cursor: {query}"
    );
    match vars.get(var).and_then(Value::as_str) {
        None => 0,
        Some(value) => value
            .strip_prefix("page-")
            .expect("opaque returned cursor")
            .parse()
            .unwrap(),
    }
}

pub struct MockQwen {
    pub sc: Arc<GitHubSourceControl>,
    state: Arc<Mutex<State>>,
    requests: Arc<Mutex<Vec<String>>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for MockQwen {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl MockQwen {
    pub async fn start(number: u64) -> Self {
        let state = Arc::new(Mutex::new(State::captured(number)));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (s, r) = (state.clone(), requests.clone());
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (s, r) = (s.clone(), r.clone());
                tokio::spawn(async move {
                    serve(stream, s, r).await.unwrap();
                });
            }
        });
        Self {
            sc: Arc::new(GitHubSourceControl::new("fixture-token", Some(&base)).unwrap()),
            state,
            requests,
            server,
        }
    }

    pub fn edit(&self, f: impl FnOnce(&mut State)) {
        f(&mut self.state.lock().unwrap());
    }

    pub fn assert_check_queries(&self) {
        for request in self
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.contains("contexts("))
        {
            let body: Value =
                serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
            let query: String = body["query"]
                .as_str()
                .unwrap()
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            let selection = query.split("contexts(").nth(1).unwrap();
            assert!(
                selection.contains("pageInfo{hasNextPageendCursor}"),
                "missing page metadata: {query}"
            );
            assert!(
                selection.contains("totalCount"),
                "missing total count: {query}"
            );
            assert!(query.contains("headRefOid"), "missing PR identity: {query}");
            assert!(
                query.contains("commit{oid"),
                "missing rollup identity: {query}"
            );
            assert!(
                selection.split(')').next().unwrap().contains("after:$"),
                "cursor not bound: {query}"
            );
            bound_check_cursor(&query, &body["variables"]);
        }
    }

    pub fn calls(&self, needle: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.contains(needle))
            .count()
    }
}

async fn serve(
    mut stream: TcpStream,
    state: Arc<Mutex<State>>,
    requests: Arc<Mutex<Vec<String>>>,
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0; 4096];
    let (head_end, length) = loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..end]);
            let length = head
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            break (end + 4, length);
        }
    };
    while buf.len() < head_end + length {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let request = String::from_utf8_lossy(&buf);
    let target = request.split_whitespace().nth(1).unwrap();
    let body = if length == 0 {
        Value::Null
    } else {
        serde_json::from_slice(&buf[head_end..head_end + length]).unwrap()
    };
    requests.lock().unwrap().push(request.to_string());
    let (status, body) = state.lock().unwrap().respond(target, &body);
    let body = body.to_string();
    let resource = if target == "/graphql" {
        "graphql"
    } else {
        "core"
    };
    let response = format!(
        "HTTP/1.1 {status} Fixture\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\nx-ratelimit-resource: {resource}\r\nx-ratelimit-remaining: 4000\r\nx-ratelimit-limit: 5000\r\nx-ratelimit-reset: 9999999999\r\n\r\n{body}", body.len()
    );
    stream.write_all(response.as_bytes()).await
}
