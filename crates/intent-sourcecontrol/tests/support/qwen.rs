//! Public check payloads captured for intent#5881. Only check nodes, head SHA,
//! state and updatedAt are captured. Surrounding PR/review/rule fields and REST
//! projections are synthetic controls, not evidence from the affected daemon.
//! Shared with the service tests so they exercise the real GitHub adapter.

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

pub struct State {
    pub pr: Value,
    pub nodes: Vec<Value>,
    pub mode: ReadMode,
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
        }
    }

    fn graphql_pr(&self, offset: usize) -> Value {
        let mut pr = self.pr.clone();
        let number = pr["number"].as_u64().unwrap();
        let end = (offset + 100).min(self.nodes.len());
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
            "pageInfo": {"hasNextPage": end < self.nodes.len(), "endCursor": "MTAw"},
            "nodes": self.nodes[offset..end],
        });
        pr
    }

    fn rest_pr(&self) -> Value {
        let number = self.pr["number"].as_u64().unwrap();
        json!({
            "number": number, "title": "Qwen captured checks", "state": "open",
            "html_url": format!("https://github.com/QwenLM/qwen-code/pull/{number}"),
            "draft": false, "head": {"ref": "fixture", "sha": self.pr["headRefOid"]},
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
            // Accept a cursor supplied as a variable or inline in a follow-up
            // query. The captured second page starts at the opaque MTAw cursor.
            let next = body["variables"]
                .as_object()
                .is_some_and(|vars| vars.values().any(|v| v.as_str() == Some("MTAw")))
                || query.contains("MTAw");
            return (
                200,
                json!({"data": {"repository": {"pullRequest": self.graphql_pr(if next { 100 } else { 0 })}}}),
            );
        }
        if target.contains("/check-runs") {
            if self.mode == ReadMode::Degraded {
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
            let runs: Vec<Value> = self
                .nodes
                .iter()
                .filter(|n| n["__typename"] == "CheckRun")
                .skip((page - 1) * 100)
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
