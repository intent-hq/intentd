//! Opt-in capture of the real Agent37 API into `fixtures/agent37.json`,
//! the recording [`super::contract_tests`] holds the fake to. It runs only
//! from `scripts/capture-agent37-fixtures.sh` (`AGENT37_CAPTURE=1` plus a
//! key); every other run skips it, and no CI job sets them.
//!
//! It imports one template, makes one `default` instance in `us` and one
//! `vm` instance in `eu`, records each REST and exec exchange the adapter
//! depends on, and deletes all three whatever happens, then checks nothing
//! of the run is left. Every run tags its instances with the same backend
//! id and starts by deleting what an interrupted run left; so never run two
//! captures at once. Instance ids, times, digests and the key are replaced
//! before the file is written; review the diff before committing it.

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures_util::FutureExt as _;
use reqwest::Method;
use serde_json::{json, Value};

use super::client::scrub;
use super::fake_api::{fixture_instance_id, FIXTURE_DIGEST, FIXTURE_KEY};
use super::{DEFAULT_API_URL, DEFAULT_EXEC_URL};

pub(super) const CAPTURE_ENV: &str = "AGENT37_CAPTURE";
pub(super) const KEY_ENV: &str = "AGENT37_API_KEY";
pub(super) const KEY_FILE_ENV: &str = "AGENT37_API_KEY_FILE";
const OUT_ENV: &str = "AGENT37_CAPTURE_OUT";
const IMAGE_ENV: &str = "AGENT37_CAPTURE_IMAGE";

/// The backend id every capture tags its instances with.
pub(super) const CAPTURE_BACKEND: &str = "intent-capture";
/// The template every capture imports.
pub(super) const CAPTURE_TEMPLATE: &str = "intent-capture-template";
/// A published intent-node image, unless `AGENT37_CAPTURE_IMAGE` names another.
pub(super) const DEFAULT_IMAGE: &str = "ghcr.io/intent-hq/intent-node:0.11.235";
/// An image tag that does not exist.
pub(super) const MISSING_IMAGE: &str = "ghcr.io/intent-hq/intent-node:0.0.1";
/// A key Agent37 rejects, in the live key's format.
pub(super) const WRONG_KEY: &str = "wrong-agent37-key-0000000000000000";
/// One command, its output on both channels and a failing exit.
pub(super) const SCRIPT: &str = "id -un; printf 'out\\n'; printf 'err\\n' >&2; exit 3";
/// The time fields whose values change from run to run.
pub(super) const TIME_KEYS: [&str; 3] = ["created", "updated", "slept_at"];

/// The key from `AGENT37_API_KEY`, or the first line of the file
/// `AGENT37_API_KEY_FILE` names, trimmed.
pub(super) fn live_key() -> Option<String> {
    let key = std::env::var(KEY_ENV).ok().or_else(|| {
        let path = std::env::var(KEY_FILE_ENV).ok()?;
        let path = path
            .strip_prefix("~/")
            .and_then(|rest| std::env::var("HOME").ok().map(|h| format!("{h}/{rest}")))
            .unwrap_or(path);
        std::fs::read_to_string(path)
            .ok()?
            .lines()
            .next()
            .map(str::to_owned)
    })?;
    let key = key.trim().to_owned();
    (!key.is_empty()).then_some(key)
}

/// A plain HTTP client over both planes, with the live key.
pub(super) struct Live {
    pub http: reqwest::Client,
    pub key: String,
}

impl Live {
    pub fn new(key: String) -> Self {
        Self {
            http: reqwest::Client::builder()
                .tcp_keepalive(Duration::from_secs(15))
                .build()
                .expect("http client"),
            key,
        }
    }

    /// One call; `exec` sends to the sandbox host with `X-Agent37-Key`.
    pub async fn send(
        &self,
        exec: bool,
        key: &str,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> (u16, Value) {
        let base = if exec {
            DEFAULT_EXEC_URL
        } else {
            DEFAULT_API_URL
        };
        let mut request = self
            .http
            .request(method, format!("{base}{path}"))
            // fixed-deadline: allow — a live Agent37 call in a by-hand run, never under the test multiplier
            .timeout(Duration::from_secs(900));
        request = if exec {
            request.header("X-Agent37-Key", key)
        } else {
            request.bearer_auth(key)
        };
        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body.to_string());
        }
        match request.send().await {
            Ok(response) => {
                let status = response.status().as_u16();
                let bytes = response.bytes().await.unwrap_or_default();
                let value = serde_json::from_slice(&bytes).unwrap_or_else(
                    |_| json!({ "unparsed": scrub(&String::from_utf8_lossy(&bytes), &self.key) }),
                );
                (status, value)
            }
            Err(e) => (0, json!({ "transport": scrub(&e.to_string(), &self.key) })),
        }
    }

    /// Delete every instance tagged with `backend` and the template
    /// `template`, then check none is left.
    pub async fn clean(&self, backend: &str, template: Option<&str>) -> Vec<String> {
        let mut left = Vec::new();
        for _ in 0..3 {
            left.clear();
            let (_, listed) = self
                .send(false, &self.key, Method::GET, "/instances", None)
                .await;
            for instance in listed["data"].as_array().cloned().unwrap_or_default() {
                if instance.pointer("/metadata/intentBackendId") == Some(&json!(backend)) {
                    let id = instance["id"].as_str().unwrap_or_default().to_owned();
                    let (status, _) = self
                        .send(
                            false,
                            &self.key,
                            Method::DELETE,
                            &format!("/instances/{id}"),
                            None,
                        )
                        .await;
                    if status != 200 && status != 404 {
                        left.push(id);
                    }
                }
            }
            if let Some(template) = template {
                let (status, _) = self
                    .send(
                        false,
                        &self.key,
                        Method::DELETE,
                        &format!("/templates/{template}"),
                        None,
                    )
                    .await;
                if status != 200 && status != 404 {
                    left.push(format!("template {template}"));
                }
            }
            if left.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        left
    }
}

struct Recorder {
    live: Live,
    /// Real instance id → placeholder, in creation order.
    ids: Vec<(String, String)>,
    exchanges: Vec<Value>,
}

impl Recorder {
    /// One recorded call. `auth` is `"key"` or `"wrong"`.
    async fn call(
        &mut self,
        name: &str,
        exec: bool,
        auth: &str,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let key = if auth == "key" {
            self.live.key.clone()
        } else {
            WRONG_KEY.to_owned()
        };
        let (status, value) = self
            .live
            .send(exec, &key, method.clone(), path, body.as_ref())
            .await;
        if !exec && method == Method::POST && path == "/instances" && status == 201 {
            let id = value["id"].as_str().unwrap_or_default().to_owned();
            let placeholder = fixture_instance_id(self.ids.len() + 1);
            self.ids.push((id, placeholder));
        }
        let mut request = json!({
            "plane": if exec { "exec" } else { "rest" },
            "auth": auth,
            "method": method.as_str(),
            "path": path,
        });
        if let Some(body) = body {
            request["body"] = body;
        }
        self.exchanges.push(json!({
            "name": name,
            "request": request,
            "response": { "status": status, "body": value.clone() },
        }));
        eprintln!("{name}: {method} {path} -> {status}");
        (status, value)
    }

    fn sanitize(&self, mut value: Value) -> Value {
        fn walk(value: &mut Value, recorder: &Recorder) {
            match value {
                Value::String(text) => {
                    let mut out = scrub(text, &recorder.live.key);
                    for (real, placeholder) in &recorder.ids {
                        out = out.replace(real.as_str(), placeholder);
                    }
                    *text = out;
                }
                Value::Array(items) => items.iter_mut().for_each(|v| walk(v, recorder)),
                Value::Object(map) => {
                    for (key, v) in map.iter_mut() {
                        if TIME_KEYS.contains(&key.as_str()) && v.is_number() {
                            *v = json!(super::fake_api::FIXTURE_EPOCH);
                        } else if key == "image_digest" && v.is_string() {
                            *v = json!(FIXTURE_DIGEST);
                        } else {
                            walk(v, recorder);
                        }
                    }
                }
                _ => {}
            }
        }
        walk(&mut value, self);
        value
    }
}

fn instance_body(instance_type: &str, region: &str, disk: u32, agent: &str) -> Value {
    json!({
        "template": CAPTURE_TEMPLATE,
        "type": instance_type,
        "region": region,
        "resources": { "cpu": 2, "memory": 4, "disk": disk },
        "name": "intent-capture",
        "metadata": { "intentBackendId": CAPTURE_BACKEND, "agentId": agent },
        "auto_sleep": true,
        "idle_timeout_seconds": super::BACKSTOP_IDLE_SECS,
    })
}

async fn record(recorder: &mut Recorder, image: &str) {
    let r = recorder;
    let get = Method::GET;
    let post = Method::POST;
    r.call(
        "list-instances-wrong-key",
        false,
        "wrong",
        get.clone(),
        "/instances",
        None,
    )
    .await;
    r.call(
        "exec-wrong-key",
        true,
        "wrong",
        post.clone(),
        "/instances/zz00000000/exec",
        Some(json!({ "command": "true" })),
    )
    .await;
    let template = format!("/templates/{CAPTURE_TEMPLATE}");
    r.call(
        "get-template-missing",
        false,
        "key",
        get.clone(),
        &template,
        None,
    )
    .await;
    r.call(
        "import-template-unpullable",
        false,
        "key",
        post.clone(),
        "/templates",
        Some(json!({ "name": CAPTURE_TEMPLATE, "image_ref": MISSING_IMAGE })),
    )
    .await;
    let (status, _) = r
        .call(
            "import-template",
            false,
            "key",
            post.clone(),
            "/templates",
            Some(json!({ "name": CAPTURE_TEMPLATE, "image_ref": image })),
        )
        .await;
    assert_eq!(status, 201, "the template import failed");
    r.call(
        "import-template-conflict",
        false,
        "key",
        post.clone(),
        "/templates",
        Some(json!({ "name": CAPTURE_TEMPLATE, "image_ref": image })),
    )
    .await;
    r.call("get-template", false, "key", get.clone(), &template, None)
        .await;
    r.call(
        "update-template-same-image",
        false,
        "key",
        Method::PATCH,
        &template,
        Some(json!({ "image_ref": image })),
    )
    .await;
    r.call(
        "create-default-in-eu",
        false,
        "key",
        post.clone(),
        "/instances",
        Some(instance_body("default", "eu", 12, "capture")),
    )
    .await;
    r.call(
        "create-bad-shape",
        false,
        "key",
        post.clone(),
        "/instances",
        Some(json!({ "template": CAPTURE_TEMPLATE, "resources": { "cpu": 2, "memory": 6 } })),
    )
    .await;
    let (status, created) = r
        .call(
            "create-default",
            false,
            "key",
            post.clone(),
            "/instances",
            Some(instance_body("default", "us", 12, "capture")),
        )
        .await;
    assert_eq!(status, 201, "the default create failed");
    let id = created["id"].as_str().unwrap_or_default().to_owned();
    let path = format!("/instances/{id}");
    let exec = format!("/instances/{id}/exec");
    r.call("get-instance", false, "key", get.clone(), &path, None)
        .await;
    r.call(
        "list-instances",
        false,
        "key",
        get.clone(),
        "/instances",
        None,
    )
    .await;
    r.call(
        "exec-as-user",
        true,
        "key",
        post.clone(),
        &exec,
        Some(json!({ "command": SCRIPT, "name": "intent-capture-1" })),
    )
    .await;
    r.call(
        "exec-as-root",
        true,
        "key",
        post.clone(),
        &exec,
        Some(json!({ "command": "id -un", "user": "root", "name": "intent-capture-2" })),
    )
    .await;
    r.call(
        "exec-unknown-instance",
        true,
        "key",
        post.clone(),
        "/instances/zz00000000/exec",
        Some(json!({ "command": "true" })),
    )
    .await;
    r.call(
        "sleep",
        false,
        "key",
        post.clone(),
        &format!("{path}/sleep"),
        None,
    )
    .await;
    r.call(
        "get-instance-sleeping",
        false,
        "key",
        get.clone(),
        &path,
        None,
    )
    .await;
    r.call(
        "sleep-sleeping",
        false,
        "key",
        post.clone(),
        &format!("{path}/sleep"),
        None,
    )
    .await;
    r.call(
        "start-sleeping",
        false,
        "key",
        post.clone(),
        &format!("{path}/start"),
        None,
    )
    .await;
    r.call(
        "start-running",
        false,
        "key",
        post.clone(),
        &format!("{path}/start"),
        None,
    )
    .await;
    r.call(
        "sleep-again",
        false,
        "key",
        post.clone(),
        &format!("{path}/sleep"),
        None,
    )
    .await;
    r.call(
        "exec-wakes-a-sleeper",
        true,
        "key",
        post.clone(),
        &exec,
        Some(json!({ "command": "echo awake", "name": "intent-capture-3" })),
    )
    .await;
    r.call("get-instance-woken", false, "key", get.clone(), &path, None)
        .await;
    r.call("delete", false, "key", Method::DELETE, &path, None)
        .await;
    r.call("delete-again", false, "key", Method::DELETE, &path, None)
        .await;
    r.call("get-deleted", false, "key", get.clone(), &path, None)
        .await;
    r.call(
        "start-unknown",
        false,
        "key",
        post.clone(),
        "/instances/zz00000000/start",
        None,
    )
    .await;

    let (status, created) = r
        .call(
            "create-vm-in-eu",
            false,
            "key",
            post.clone(),
            "/instances",
            Some(instance_body("vm", "eu", 20, "capture-vm")),
        )
        .await;
    assert_eq!(status, 201, "the vm create failed");
    let vm = created["id"].as_str().unwrap_or_default().to_owned();
    let vm_path = format!("/instances/{vm}");
    r.call(
        "exec-vm",
        true,
        "key",
        post.clone(),
        &format!("{vm_path}/exec"),
        Some(json!({ "command": "id -un", "name": "intent-capture-4" })),
    )
    .await;
    r.call(
        "sleep-vm",
        false,
        "key",
        post.clone(),
        &format!("{vm_path}/sleep"),
        None,
    )
    .await;
    r.call(
        "start-vm",
        false,
        "key",
        post.clone(),
        &format!("{vm_path}/start"),
        None,
    )
    .await;
    r.call("delete-vm", false, "key", Method::DELETE, &vm_path, None)
        .await;
    r.call(
        "delete-template",
        false,
        "key",
        Method::DELETE,
        &template,
        None,
    )
    .await;
    r.call(
        "delete-template-again",
        false,
        "key",
        Method::DELETE,
        &template,
        None,
    )
    .await;
}

#[tokio::test]
async fn capture_agent37_fixtures() {
    if std::env::var(CAPTURE_ENV).as_deref() != Ok("1") {
        eprintln!("skipped: set {CAPTURE_ENV}=1 (scripts/capture-agent37-fixtures.sh)");
        return;
    }
    let key = live_key().unwrap_or_else(|| panic!("set {KEY_ENV} or {KEY_FILE_ENV}"));
    let image = std::env::var(IMAGE_ENV).unwrap_or_else(|_| DEFAULT_IMAGE.to_owned());
    let live = Live::new(key);
    let left = live.clean(CAPTURE_BACKEND, Some(CAPTURE_TEMPLATE)).await;
    assert!(left.is_empty(), "an earlier run left {left:?}");
    let mut recorder = Recorder {
        live,
        ids: Vec::new(),
        exchanges: Vec::new(),
    };
    let outcome = AssertUnwindSafe(record(&mut recorder, &image))
        .catch_unwind()
        .await;
    let left = recorder
        .live
        .clean(CAPTURE_BACKEND, Some(CAPTURE_TEMPLATE))
        .await;
    assert!(left.is_empty(), "the capture could not delete {left:?}");
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    let fixture = recorder.sanitize(json!({
        "recorded": "the real Agent37 API, by scripts/capture-agent37-fixtures.sh",
        "image": image,
        "key": FIXTURE_KEY,
        "exchanges": recorder.exchanges,
    }));
    let text = serde_json::to_string_pretty(&fixture).expect("encode");
    assert!(
        !text.contains(&recorder.live.key),
        "the key reached the fixture"
    );
    let out = std::env::var(OUT_ENV).unwrap_or_else(|_| {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/agent37/fixtures/agent37.json"
        )
        .to_owned()
    });
    std::fs::write(&out, text + "\n").expect("write the fixture");
    eprintln!("wrote {out}");
}
