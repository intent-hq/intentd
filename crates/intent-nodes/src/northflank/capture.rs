//! Opt-in capture of the real Northflank API into
//! `fixtures/northflank.json`, the recording [`super::contract_tests`]
//! holds the fake to. It runs only from
//! `scripts/capture-northflank-fixtures.sh` (`NORTHFLANK_CAPTURE=1` plus a
//! token and a project); every other run skips it, and no CI job sets them.
//!
//! It makes one small service with its volume and one team tag, records
//! each REST exchange and command stream intentd depends on, and deletes
//! all three whatever happens, then checks nothing of the run is left.
//! Ids, names, timestamps and the token are replaced before the file is
//! written; review the diff before committing it.

use std::panic::AssertUnwindSafe;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{FutureExt as _, SinkExt as _, StreamExt as _};
use reqwest::Method;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

use super::client::scrub;
use super::fake_api::{
    FIXTURE_BACKEND, FIXTURE_BACKEND_TAG, FIXTURE_PROJECT, FIXTURE_SERVICE, FIXTURE_TEAM,
    FIXTURE_TIME,
};
use super::provider::TAGS_ENV;
use super::*;

const CAPTURE_ENV: &str = "NORTHFLANK_CAPTURE";
const TOKEN_ENV: &str = "NORTHFLANK_API_TOKEN";
const TOKEN_FILE_ENV: &str = "NORTHFLANK_API_TOKEN_FILE";
const PROJECT_ENV: &str = "NORTHFLANK_PROJECT_ID";
const OUT_ENV: &str = "NORTHFLANK_CAPTURE_OUT";
const IMAGE: &str = "ghcr.io/intent-hq/intent-node:latest";
/// The cheapest plans first: the image only idles.
const PLANS: [&str; 3] = ["nf-compute-20", "nf-compute-50", "nf-compute-200"];
/// Plans kept in the recording: the ones a machine size maps to.
const KEPT_PLANS: [&str; 4] = [
    "nf-compute-50",
    "nf-compute-200",
    "nf-compute-400",
    "nf-compute-800-16",
];
/// Keys whose values name the team's cluster or Northflank's internal ids.
const SCRUBBED_KEYS: [&str; 5] = [
    "uid",
    "namespace",
    "loadBalancers",
    "storageClassObjectId",
    "cursor",
];
/// One command, its output on both channels and a failing exit.
const SCRIPT: &str = "printf 'out\\n'; printf 'err\\n' >&2; exit 3";

struct Recorder {
    client: NorthflankClient,
    project: String,
    /// Real value → recorded placeholder, longest first.
    swaps: Vec<(String, String)>,
    exchanges: Vec<Value>,
    exec: Vec<Value>,
}

impl Recorder {
    fn swap(&mut self, real: &str, placeholder: &str) {
        if !real.is_empty() && real != placeholder {
            self.swaps.push((real.to_owned(), placeholder.to_owned()));
            self.swaps
                .sort_by_key(|(real, _)| std::cmp::Reverse(real.len()));
        }
    }

    fn api(&self, path: &str) -> String {
        format!("{}{path}", self.client.config.api_url.trim_end_matches('/'))
    }

    /// One REST call, recorded as `name` when given.
    async fn call(
        &mut self,
        name: Option<&str>,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut request = self
            .client
            .http
            .request(method.clone(), self.api(path))
            .bearer_auth(self.client.token.expose())
            // fixed-deadline: allow — a live Northflank request in a by-hand capture, never under the test multiplier
            .timeout(Duration::from_secs(60));
        if !query.is_empty() {
            request = request.query(query);
        }
        if let Some(body) = &body {
            request = request
                .header("Content-Type", "application/json")
                .body(body.to_string());
        }
        let (status, value) = match request.send().await {
            Ok(response) => {
                let status = response.status().as_u16();
                let bytes = response.bytes().await.unwrap_or_default();
                let value = serde_json::from_slice(&bytes).unwrap_or_else(
                    |_| json!({ "unparsed": String::from_utf8_lossy(&bytes).into_owned() }),
                );
                (status, value)
            }
            Err(e) => (0, json!({ "transport": self.client.scrub(&e.to_string()) })),
        };
        if let Some(name) = name {
            let target = if query.is_empty() {
                path.to_owned()
            } else {
                let q: Vec<String> = query.iter().map(|(k, v)| format!("{k}={v}")).collect();
                format!("{path}?{}", q.join("&"))
            };
            let mut request = json!({ "method": method.as_str(), "path": target });
            if let Some(body) = body {
                request["body"] = body;
            }
            self.exchanges.push(json!({
                "name": name,
                "request": request,
                "response": { "status": status, "body": value.clone() },
            }));
        }
        if (200..300).contains(&status) {
            eprintln!("{method} {path} -> {status}");
        } else {
            let message = super::client::error_message(&value).unwrap_or_default();
            eprintln!(
                "{method} {path} -> {status}: {}",
                self.client.scrub(&message)
            );
        }
        (status, value)
    }

    /// Read the service until `done`, recording only the read that saw it.
    async fn wait_service(
        &mut self,
        name: &str,
        service: &str,
        done: impl Fn(&Value) -> bool,
    ) -> Result<Value, String> {
        let path = format!("/projects/{}/services/{service}", self.project);
        for _ in 0..120 {
            let (status, value) = self.call(None, Method::GET, &path, &[], None).await;
            if status == 200 && done(&value["data"]) {
                let (_, value) = self.call(Some(name), Method::GET, &path, &[], None).await;
                return Ok(value);
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        Err(format!("{name}: the service never got there"))
    }

    /// One command over the exec WebSocket: every frame Northflank sends.
    /// `program` is what the command writes and exits with, outside any
    /// terminal: the fake renders its stream from it.
    async fn exec(
        &mut self,
        name: &str,
        service: &str,
        context: Value,
        program: Value,
    ) -> Result<(), String> {
        let url = format!(
            "{}/command-exec/projects/{}/services/{service}",
            self.client.config.exec_url.trim_end_matches('/'),
            self.project
        );
        let mut ws = self.client.open(&url).await.map_err(|e| e.to_string())?;
        let init = json!({
            "type": "init",
            "data": {
                "auth": { "type": "apiToken", "apiToken": self.client.token.expose() },
                "context": context,
            }
        });
        ws.send(Message::text(init.to_string()))
            .await
            .map_err(|e| e.to_string())?;
        let mut frames = Vec::new();
        let mut done = false;
        loop {
            let wait = if done { 10 } else { 120 };
            // fixed-deadline: allow — a live Northflank stream in a by-hand capture, never under the test multiplier
            let Ok(next) = tokio::time::timeout(Duration::from_secs(wait), ws.next()).await else {
                frames.push(json!({ "silent": wait }));
                break;
            };
            match next {
                Some(Ok(message @ (Message::Text(_) | Message::Binary(_)))) => {
                    let opcode = if message.is_binary() {
                        "binary"
                    } else {
                        "text"
                    };
                    let text = message
                        .into_text()
                        .map(|t| t.to_string())
                        .unwrap_or_default();
                    let frame: Value =
                        serde_json::from_str(&text).unwrap_or_else(|_| json!({ "unparsed": text }));
                    done |= frame["type"] == "completion";
                    frames.push(json!({ "opcode": opcode, "frame": frame }));
                }
                Some(Ok(Message::Close(close))) => {
                    frames.push(json!({ "close": close.map(|c| u16::from(c.code)) }));
                    break;
                }
                None => {
                    frames.push(json!({ "close": null }));
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    frames.push(json!({ "error": self.client.scrub(&e.to_string()) }));
                    break;
                }
            }
        }
        let _ = ws.close(None).await;
        eprintln!("exec {name}: {} frames", frames.len());
        self.exec.push(
            json!({ "name": name, "context": context, "program": program, "frames": frames }),
        );
        Ok(())
    }

    /// Every recorded value with ids, names, times and secrets replaced.
    fn sanitised(&self, value: &Value) -> Value {
        match value {
            Value::String(s) => Value::String(self.clean_text(s)),
            Value::Array(items) => Value::Array(items.iter().map(|v| self.sanitised(v)).collect()),
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(k, v)| {
                        let key = self.clean_text(k);
                        let lower = k.to_ascii_lowercase();
                        let secret = ["token", "secret", "password", "email", "createdby"]
                            .iter()
                            .any(|w| lower.contains(w))
                            || SCRUBBED_KEYS.contains(&k.as_str());
                        let value = match v {
                            Value::Null | Value::Object(_) => self.sanitised(v),
                            Value::Array(items) if secret => {
                                Value::Array(items.iter().map(|_| json!("<scrubbed>")).collect())
                            }
                            _ if secret => json!("<scrubbed>"),
                            _ => self.sanitised(v),
                        };
                        (key, value)
                    })
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    fn clean_text(&self, text: &str) -> String {
        if text == self.project {
            return FIXTURE_PROJECT.to_owned();
        }
        if is_timestamp(text) {
            return FIXTURE_TIME.to_owned();
        }
        let mut out = text.replace(
            &format!("/projects/{}/", self.project),
            &format!("/projects/{FIXTURE_PROJECT}/"),
        );
        if out.ends_with(&format!("/projects/{}", self.project)) {
            out = out.replace(
                &format!("/projects/{}", self.project),
                &format!("/projects/{FIXTURE_PROJECT}"),
            );
        }
        for (real, placeholder) in &self.swaps {
            out = out.replace(real, placeholder);
        }
        scrub(&out, &[self.client.token.expose()])
    }
}

fn is_timestamp(text: &str) -> bool {
    let b = text.as_bytes();
    (20..=30).contains(&b.len())
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[..4].iter().all(u8::is_ascii_digit)
}

/// Keep only the listed items of `data[key]` (or `data` itself) whose
/// `field` is in `keep`: a team's other services and tags stay private.
fn keep_only(body: &mut Value, key: Option<&str>, field: &str, keep: &[&str]) {
    let list = match key {
        Some(key) => body["data"].get_mut(key),
        None => body.get_mut("data"),
    };
    if let Some(Value::Array(items)) = list {
        items.retain(|item| item[field].as_str().is_some_and(|v| keep.contains(&v)));
    }
}

fn filter_last(recorder: &mut Recorder, key: Option<&str>, field: &str, keep: &[&str]) {
    if let Some(last) = recorder.exchanges.last_mut() {
        keep_only(&mut last["response"]["body"], key, field, keep);
    }
}

async fn record(recorder: &mut Recorder, run: &str) -> Result<(), String> {
    let p = recorder.project.clone();
    let backend = format!("capture-{run}");
    let tag = backend_tag(&backend);
    recorder.swap(&backend, FIXTURE_BACKEND);
    recorder.swap(&tag, FIXTURE_BACKEND_TAG);

    recorder
        .call(
            Some("get-project"),
            Method::GET,
            &format!("/projects/{p}"),
            &[],
            None,
        )
        .await;
    let (_, plans) = recorder
        .call(Some("list-plans"), Method::GET, "/plans", &[], None)
        .await;
    filter_last(recorder, Some("plans"), "id", &KEPT_PLANS);
    let plan = PLANS
        .iter()
        .find(|id| {
            plans["data"]["plans"]
                .as_array()
                .is_some_and(|all| all.iter().any(|p| p["id"] == **id))
        })
        .ok_or("none of the small plans exists")?;

    recorder
        .call(
            Some("create-tag"),
            Method::POST,
            "/tags",
            &[],
            Some(json!({ "name": tag })),
        )
        .await;
    recorder
        .call(
            Some("list-tags"),
            Method::GET,
            "/tags",
            &[("per_page", "100")],
            None,
        )
        .await;
    filter_last(recorder, Some("tags"), "name", &[&tag, NODE_TAG]);
    // The team has this run's tag and intent-node at least: a second page.
    recorder
        .call(
            Some("list-tags-paged"),
            Method::GET,
            "/tags",
            &[("per_page", "1")],
            None,
        )
        .await;
    filter_last(recorder, Some("tags"), "name", &[&tag, NODE_TAG]);

    let name = format!(
        "intent-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    recorder.swap(&name, FIXTURE_SERVICE);
    let team_tags = json!([NODE_TAG, tag]);
    let volume = |gib: u64| {
        json!({
            "name": name,
            "tags": team_tags,
            "mounts": [{ "containerMountPath": HOME }],
            "spec": { "accessMode": "ReadWriteOnce", "storageSize": gib * 1024 },
        })
    };
    let volumes = format!("/projects/{p}/volumes");
    recorder
        .call(
            Some("create-volume-under-minimum"),
            Method::POST,
            &volumes,
            &[],
            Some(volume(5)),
        )
        .await;
    let (status, created) = recorder
        .call(
            Some("create-volume"),
            Method::POST,
            &volumes,
            &[],
            Some(volume(u64::from(MIN_DISK_GIB))),
        )
        .await;
    if status != 200 {
        return Err(format!("create-volume: HTTP {status}"));
    }
    let volume_id = created["data"]["id"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    recorder.swap(&volume_id, FIXTURE_SERVICE);

    let tags_json = json!({ "intentBackendId": backend }).to_string();
    // What intentd sent before the tags moved to the runtime environment.
    let metadata = json!({ "annotations": { "intent.dev/tags": tags_json } });
    let body = |metadata: Option<&Value>| {
        let mut body = json!({
            "name": name,
            "description": "Intent cloud machine",
            "tags": team_tags,
            "billing": { "deploymentPlan": plan },
            "deployment": { "instances": 1, "external": { "imagePath": IMAGE } },
            "runtimeEnvironment": { TAGS_ENV: tags_json },
            "ports": [],
            "createOptions": { "volumesToAttach": [volume_id] },
        });
        if let Some(metadata) = metadata {
            body["deployment"]["metadata"] = metadata.clone();
        }
        body
    };
    let services = format!("/projects/{p}/services/deployment");
    let labels = json!({ "labels": { "intent-backend": tag } });
    let (status, _) = recorder
        .call(
            Some("create-service-with-labels"),
            Method::POST,
            &services,
            &[],
            Some(body(Some(&labels))),
        )
        .await;
    if status == 200 {
        return Err("Northflank took custom labels: record this with a new service name".into());
    }
    let (mut status, mut created) = recorder
        .call(
            Some("create-service-with-annotations"),
            Method::POST,
            &services,
            &[],
            Some(body(Some(&metadata))),
        )
        .await;
    if status != 200 {
        (status, created) = recorder
            .call(
                Some("create-service"),
                Method::POST,
                &services,
                &[],
                Some(body(None)),
            )
            .await;
    }
    if status != 200 {
        return Err(format!("create-service: HTTP {status}"));
    }
    let id = created["data"]["id"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    recorder.swap(&id, FIXTURE_SERVICE);
    // `appId` is `/<team>/<project>/<service>`.
    if let Some(team) = created["data"]["appId"]
        .as_str()
        .and_then(|a| a.split('/').nth(1))
    {
        let app = format!("/{team}/{p}/");
        recorder.swap(&app, &format!("/{FIXTURE_TEAM}/{FIXTURE_PROJECT}/"));
    }
    let service = format!("/projects/{p}/services/{id}");

    recorder
        .wait_service("get-service-running", &id, |s| {
            s.pointer("/status/deployment/status") == Some(&json!("COMPLETED"))
                && s["servicePaused"] != true
        })
        .await?;
    for _ in 0..60 {
        let (_, found) = recorder
            .call(
                None,
                Method::GET,
                &format!("{service}/containers"),
                &[],
                None,
            )
            .await;
        if found.to_string().contains("TASK_RUNNING") {
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    recorder
        .call(
            Some("list-containers-running"),
            Method::GET,
            &format!("{service}/containers"),
            &[],
            None,
        )
        .await;
    recorder
        .call(
            Some("list-services"),
            Method::GET,
            &format!("/projects/{p}/services"),
            &[("per_page", "100")],
            None,
        )
        .await;
    filter_last(recorder, Some("services"), "id", &[&id]);
    recorder
        .call(
            Some("list-volumes"),
            Method::GET,
            &volumes,
            &[("per_page", "100")],
            None,
        )
        .await;
    filter_last(recorder, None, "id", &[&volume_id]);

    let context = |tty: Option<bool>| {
        let mut c = json!({ "command": ["sh", "-c", SCRIPT], "shell": "none", "user": RUN_AS });
        if let Some(tty) = tty {
            c["tty"] = json!(tty);
        }
        c
    };
    let failing = json!({ "stdout": "out\n", "stderr": "err\n", "exitCode": 3 });
    recorder
        .exec("exec-tty-default", &id, context(None), failing.clone())
        .await?;
    recorder
        .exec("exec-tty-false", &id, context(Some(false)), failing)
        .await?;
    recorder
        .exec(
            "exec-tty-false-success",
            &id,
            json!({ "command": ["echo", "ok"], "shell": "none", "user": RUN_AS, "tty": false }),
            json!({ "stdout": "ok\n", "stderr": "", "exitCode": 0 }),
        )
        .await?;

    recorder
        .call(
            Some("delete-volume-attached"),
            Method::DELETE,
            &format!("{volumes}/{volume_id}"),
            &[],
            None,
        )
        .await;

    recorder
        .call(
            Some("patch-service-metadata"),
            Method::PATCH,
            &format!("/projects/{p}/services/deployment/{id}"),
            &[],
            Some(json!({ "deployment": { "metadata": metadata } })),
        )
        .await;
    recorder
        .wait_service("get-service-after-patch", &id, |s| {
            s.pointer("/status/deployment/status") == Some(&json!("COMPLETED"))
        })
        .await?;

    recorder
        .call(
            Some("pause"),
            Method::POST,
            &format!("{service}/pause"),
            &[],
            Some(json!({})),
        )
        .await;
    recorder
        .wait_service("get-service-paused", &id, |s| {
            s["servicePaused"] == true || s.pointer("/deployment/instances") == Some(&json!(0))
        })
        .await?;
    recorder
        .call(
            Some("list-containers-paused"),
            Method::GET,
            &format!("{service}/containers"),
            &[],
            None,
        )
        .await;
    recorder
        .call(
            Some("resume"),
            Method::POST,
            &format!("{service}/resume"),
            &[],
            Some(json!({ "instances": 1 })),
        )
        .await;
    recorder
        .wait_service("get-service-resumed", &id, |s| {
            s["servicePaused"] != true
                && s.pointer("/status/deployment/status") == Some(&json!("COMPLETED"))
        })
        .await?;

    recorder
        .call(
            Some("delete-service"),
            Method::DELETE,
            &service,
            &[("delete_child_objects", "true")],
            None,
        )
        .await;
    recorder
        .call(
            Some("delete-volume-after-service"),
            Method::DELETE,
            &format!("{volumes}/{volume_id}"),
            &[],
            None,
        )
        .await;
    Ok(())
}

/// Delete everything the run made until nothing is left, then the tag.
async fn clean_up(recorder: &mut Recorder, run: &str) -> Result<(), String> {
    let p = recorder.project.clone();
    let tag = backend_tag(&format!("capture-{run}"));
    for _ in 0..60 {
        let (_, services) = recorder
            .call(
                None,
                Method::GET,
                &format!("/projects/{p}/services"),
                &[("per_page", "100")],
                None,
            )
            .await;
        let (_, volumes) = recorder
            .call(
                None,
                Method::GET,
                &format!("/projects/{p}/volumes"),
                &[("per_page", "100")],
                None,
            )
            .await;
        let ours = |items: Option<&Vec<Value>>| -> Vec<String> {
            items
                .into_iter()
                .flatten()
                .filter(|i| {
                    i["tags"]
                        .as_array()
                        .is_some_and(|t| t.contains(&json!(tag)))
                })
                .filter_map(|i| i["id"].as_str().map(str::to_owned))
                .collect()
        };
        let services = ours(services["data"]["services"].as_array());
        let volumes = ours(volumes["data"].as_array());
        if services.is_empty() && volumes.is_empty() {
            let (_, tags) = recorder
                .call(None, Method::GET, "/tags", &[("per_page", "100")], None)
                .await;
            for found in tags["data"]["tags"].as_array().into_iter().flatten() {
                if found["name"] == tag.as_str() {
                    let path = format!("/tags/{}", found["id"].as_str().unwrap_or_default());
                    recorder.call(None, Method::DELETE, &path, &[], None).await;
                }
            }
            return Ok(());
        }
        for id in services {
            let path = format!("/projects/{p}/services/{id}");
            recorder
                .call(
                    None,
                    Method::DELETE,
                    &path,
                    &[("delete_child_objects", "true")],
                    None,
                )
                .await;
        }
        for id in volumes {
            let path = format!("/projects/{p}/volumes/{id}");
            recorder.call(None, Method::DELETE, &path, &[], None).await;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    Err("the run's service or volume is still there after 5 minutes".into())
}

#[tokio::test]
async fn capture_northflank_fixtures() {
    let token = std::env::var(TOKEN_ENV)
        .ok()
        .or_else(|| std::fs::read_to_string(std::env::var(TOKEN_FILE_ENV).ok()?).ok())
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty());
    let project = std::env::var(PROJECT_ENV).ok().filter(|p| !p.is_empty());
    let (true, Some(token), Some(project)) = (
        std::env::var(CAPTURE_ENV).is_ok_and(|v| v == "1"),
        token,
        project,
    ) else {
        eprintln!("skipped: run scripts/capture-northflank-fixtures.sh to capture");
        return;
    };
    let client = NorthflankClient::new(
        SecretString::new(token),
        NorthflankConfig {
            project_id: project.clone(),
            ..NorthflankConfig::default()
        },
    )
    .unwrap();
    let mut recorder = Recorder {
        client,
        project,
        swaps: Vec::new(),
        exchanges: Vec::new(),
        exec: Vec::new(),
    };
    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();

    let outcome = AssertUnwindSafe(record(&mut recorder, &run))
        .catch_unwind()
        .await;
    let cleaned = clean_up(&mut recorder, &run).await;
    match outcome {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(Err(e)) => panic!("capture failed: {e} (clean-up: {cleaned:?})"),
        Ok(Ok(())) => {}
    }
    cleaned.unwrap();

    let out = std::env::var(OUT_ENV).unwrap_or_else(|_| {
        format!(
            "{}/src/northflank/fixtures/northflank.json",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    let fixture = json!({
        "about": "Recorded from the real Northflank API by scripts/capture-northflank-fixtures.sh; ids, names, times and secrets replaced. The contract test holds the fake to it.",
        "image": IMAGE,
        "exchanges": recorder.sanitised(&Value::Array(recorder.exchanges.clone())),
        "exec": recorder.sanitised(&Value::Array(recorder.exec.clone())),
    });
    let mut text = serde_json::to_string_pretty(&fixture).unwrap();
    text.push('\n');
    assert!(
        !text.contains(recorder.client.token.expose()),
        "the token would be written"
    );
    std::fs::write(&out, text).unwrap();
    eprintln!("wrote {out}");
}
