//! Service regressions count real HTTP attempts through the production adapter.
use super::*;
use intent_sourcecontrol::traffic::{with_traffic, Operation, Traffic};
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Api {
    base: String,
    requests: Arc<Mutex<Vec<String>>>,
    pulls: Arc<Mutex<Vec<serde_json::Value>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Api {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn pull(number: u64, branch: &str) -> serde_json::Value {
    json!({"number":number,"html_url":format!("https://github.com/o/r/pull/{number}"),
        "title":format!("PR {number}"),"state":"open","draft":false,
        "head":{"ref":branch,"sha":"abc"},"base":{"ref":"main"},
        "created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"})
}

impl Api {
    async fn new(pulls: Vec<serde_json::Value>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let data = Arc::new(Mutex::new(pulls));
        let (seen, records) = (requests.clone(), data.clone());
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = Vec::new();
                loop {
                    let mut chunk = [0; 2048];
                    let n = socket.read(&mut chunk).await.unwrap();
                    if n == 0 { break; }
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") { break; }
                }
                let request = String::from_utf8_lossy(&buf);
                let path = request.split_whitespace().nth(1).unwrap_or("/");
                seen.lock().unwrap().push(path.to_string());
                let url = reqwest::Url::parse(&format!("http://fixture{path}")).unwrap();
                let query: std::collections::HashMap<_,_> = url.query_pairs().into_owned().collect();
                let page = query.get("page").and_then(|s|s.parse::<usize>().ok()).unwrap_or(1);
                let size = query.get("per_page").and_then(|s|s.parse::<usize>().ok()).unwrap_or(30);
                let body = {
                let items = records.lock().unwrap();
                if url.path().ends_with("/pulls") {
                    let filtered: Vec<_> = items.iter().filter(|p| query.get("head").is_none_or(|head| p["head"]["ref"] == *head)).cloned().collect();
                    json!(filtered.into_iter().skip((page-1)*size).take(size).collect::<Vec<_>>())
                } else {
                    let number = url.path().rsplit('/').next().and_then(|s|s.parse::<u64>().ok()).unwrap_or(0);
                    items.iter().find(|p|p["number"] == number).cloned().unwrap_or_else(||pull(number,"feature"))
                }.to_string()
                };
                let response = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        Self { base, requests, pulls:data, task }
    }

    fn sc(&self) -> Arc<dyn SourceControl> {
        Arc::new(intent_sourcecontrol::GitHubSourceControl::new("fixture-token",Some(&self.base)).unwrap())
    }
}

fn counts(traffic: &Traffic) -> (u64,u64,u64) {
    let snapshot = traffic.snapshot();
    let sum = |op|snapshot.counts.iter().filter(|((_,o),_)|*o == op).map(|(_,v)|v.rest_requests).sum();
    (sum(Operation::Discovery),sum(Operation::PrDetail),sum(Operation::Rules))
}

async fn consumer(svc: &Services, branch: &str, repo: &str) -> intent_core::Workspace {
    let mut ws = workspace(&WorkspaceId::new());
    ws.branch = branch.into();
    ws.base_ref = None;
    ws.repository_owner = Some("o".into());
    ws.repository_name = Some(repo.into());
    svc.store().insert_workspace(&ws).await.unwrap();
    ws
}

#[tokio::test]
async fn shared_discovery_one_vs_many_workspaces_and_roots_http_counts() {
    for consumers in [1, 20] {
        let api = Api::new(vec![]).await;
        let (_t,svc,_) = refresh_setup(StubForge::default(),"main",None,false).await;
        let sc = api.sc();
        let traffic = Traffic::default();
        with_traffic(traffic.clone(),async {
            for i in 0..consumers {
                let ws = consumer(&svc,&format!("branch-{i}"),"r").await;
                svc.refresh_workspace_pr_with_sc(ws.clone(),&sc).await.unwrap();
                let checkout = SweepRepo::init(&format!("root-{i}"),Some("https://github.com/o/r.git"));
                let root = sweep_root(&ws.id,&checkout.dir,Some(("o","r")));
                svc.store().upsert_workspace_git_root(&root).await.unwrap();
                svc.refresh_git_root_pr(root,&sc).await.unwrap();
            }
        }).await;
        assert_eq!(counts(&traffic),(1,0,0),"consumers={consumers}; HTTP={:?}",api.requests.lock().unwrap());
    }
}

#[tokio::test]
async fn shared_discovery_two_repositories_http_counts() {
    let api = Api::new(vec![]).await;
    let (_t,svc,_) = refresh_setup(StubForge::default(),"main",None,false).await;
    let traffic = Traffic::default();
    with_traffic(traffic.clone(),async {
        for i in 0..12 {
            let ws = consumer(&svc,&format!("branch-{i}"),if i%2 == 0 {"r"} else {"other"}).await;
            svc.refresh_workspace_pr_with_sc(ws,&api.sc()).await.unwrap();
        }
    }).await;
    assert_eq!(counts(&traffic),(2,0,0));
}

#[tokio::test]
async fn shared_discovery_concurrent_http_counts() {
    let api = Api::new(vec![]).await;
    let (_t,svc,_) = refresh_setup(StubForge::default(),"main",None,false).await;
    let (a,b) = (consumer(&svc,"a","r").await,consumer(&svc,"b","r").await);
    let sc = api.sc();
    let traffic = Traffic::default();
    with_traffic(traffic.clone(),async {
        let (a,b) = tokio::join!(svc.refresh_workspace_pr_with_sc(a,&sc),svc.refresh_workspace_pr_with_sc(b,&sc));
        a.unwrap(); b.unwrap();
    }).await;
    assert_eq!(counts(&traffic),(1,0,0));
}

#[tokio::test]
async fn shared_discovery_complete_pages_match_final_page() {
    let mut data:Vec<_> = (1..=200).map(|n|pull(n,"other")).collect();
    data.push(pull(201,"last"));
    let api = Api::new(data).await;
    let (_t,svc,_) = refresh_setup(StubForge::default(),"main",None,false).await;
    let ws = consumer(&svc,"last","r").await;
    let traffic = Traffic::default();
    with_traffic(traffic.clone(),svc.refresh_workspace_pr_with_sc(ws.clone(),&api.sc())).await.unwrap();
    assert_eq!(svc.store().get_workspace(&ws.id).await.unwrap().pr_number,Some(201));
    assert_eq!(counts(&traffic).0,3,"one complete 100-item page chain");
    assert!(api.requests.lock().unwrap().iter().all(|p|!p.contains("head=")));
}
