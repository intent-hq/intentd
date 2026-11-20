//! Opt-in capture of the real Vercel Sandbox API into
//! `fixtures/vercel.json`, the recording [`super::contract_tests`] holds
//! the fake to. It runs only from `scripts/capture-vercel-fixtures.sh`
//! (`VERCEL_CAPTURE=1`, a token, a team and a project); every other run
//! skips it, and no CI job sets them.
//!
//! It makes one persistent sandbox from the published image, records each
//! REST, command and registry exchange the adapter depends on (create,
//! read, list by tag, command, extend, stop, resume, delete, and the
//! refusals), and deletes the sandbox and its snapshots whatever happens,
//! then checks nothing of the run is left. Every run tags its sandbox with
//! the same backend id and starts by deleting what an interrupted run
//! left; so never run two captures at once. The team, project, session
//! and snapshot ids, times and digests are replaced before the file is
//! written, and the token never reaches it; review the diff before
//! committing it.

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures_util::FutureExt as _;
use reqwest::Method;
use serde_json::{json, Value};

use super::client::scrub;
use super::fake_api::{
    fixture_session_id, fixture_snapshot_id, FIXTURE_DIGEST, FIXTURE_EPOCH_MS, FIXTURE_PROJECT,
    FIXTURE_TEAM, FIXTURE_TOKEN,
};
use super::{BACKEND_ID_TAG, DEFAULT_API_URL, DEFAULT_REGISTRY_URL};

pub(super) const CAPTURE_ENV: &str = "VERCEL_CAPTURE";
pub(super) const TOKEN_ENV: &str = "VERCEL_TOKEN";
pub(super) const TOKEN_FILE_ENV: &str = "VERCEL_TOKEN_FILE";
pub(super) const TEAM_ENV: &str = "VERCEL_TEAM_ID";
pub(super) const PROJECT_ENV: &str = "VERCEL_PROJECT_ID";
const OUT_ENV: &str = "VERCEL_CAPTURE_OUT";
const IMAGE_ENV: &str = "VERCEL_CAPTURE_IMAGE";

/// The backend id every capture tags its sandbox with.
pub(super) const CAPTURE_BACKEND: &str = "intent-capture";
/// The sandbox every capture makes.
pub(super) const CAPTURE_SANDBOX: &str = "intent-capture-sandbox";
/// A published intent-node image, unless `VERCEL_CAPTURE_IMAGE` names another.
pub(super) const DEFAULT_IMAGE: &str = "sutterhill/intent-images/intent-node:0.11.254";
/// A tag of the same repository that does not exist.
pub(super) const MISSING_TAG: &str = "0.0.1";
/// A token Vercel rejects.
pub(super) const WRONG_TOKEN: &str = "wrong-vercel-token-000000000000";
/// A team and a project that do not exist.
pub(super) const MISSING_TEAM: &str = "team_000000000000000000000000";
pub(super) const MISSING_PROJECT: &str = "prj_0000000000000000000000000000";
/// One command: who runs it, its working folder and environment, output on
/// both channels and a failing exit.
pub(super) const SCRIPT: &str =
    "id -un; pwd; printf '%s\\n' \"$CAPTURE_VAR\"; printf 'err\\n' >&2; echo kept > marker; exit 3";
/// The time fields whose values change from run to run (milliseconds).
pub(super) const TIME_KEYS: [&str; 12] = [
    "createdAt",
    "updatedAt",
    "requestedAt",
    "startedAt",
    "requestedStopAt",
    "stoppedAt",
    "statusUpdatedAt",
    "expiresAt",
    "snapshottedAt",
    "lastUsedAt",
    "abortedAt",
    "duration",
];
/// Fields whose values are measurements of the run, not its state.
pub(super) const MEASURED_KEYS: [&str; 7] = [
    "activeCpuDurationMs",
    "totalActiveCpuDurationMs",
    "totalDurationMs",
    "totalEgressBytes",
    "totalIngressBytes",
    "sizeBytes",
    "durationMs",
];

/// The token from `VERCEL_TOKEN`, or the first line of the file
/// `VERCEL_TOKEN_FILE` names, trimmed.
pub(super) fn live_token() -> Option<String> {
    let token = std::env::var(TOKEN_ENV).ok().or_else(|| {
        let path = std::env::var(TOKEN_FILE_ENV).ok()?;
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
    let token = token.trim().to_owned();
    (!token.is_empty()).then_some(token)
}

/// The team and project of a live run.
pub(super) fn live_scope() -> (String, String) {
    let team = std::env::var(TEAM_ENV).unwrap_or_else(|_| panic!("set {TEAM_ENV}"));
    let project = std::env::var(PROJECT_ENV).unwrap_or_else(|_| panic!("set {PROJECT_ENV}"));
    (team, project)
}

/// What one live call answered.
pub(super) struct Answer {
    pub status: u16,
    /// The JSON body; a newline-delimited stream is `{ "ndjson": [..] }`.
    pub body: Value,
}

/// A plain HTTP client with the live token.
pub(super) struct Live {
    pub http: reqwest::Client,
    pub token: String,
    pub team: String,
    pub project: String,
}

impl Live {
    pub fn new(token: String, team: String, project: String) -> Self {
        Self {
            http: reqwest::Client::builder()
                .tcp_keepalive(Duration::from_secs(15))
                .build()
                .expect("http client"),
            token,
            team,
            project,
        }
    }

    /// One REST call to `path` (with its query, `teamId` added unless
    /// given) with `token`.
    pub async fn send(
        &self,
        token: &str,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Answer {
        let sep = if path.contains('?') { '&' } else { '?' };
        let url = if path.contains("teamId=") {
            format!("{DEFAULT_API_URL}{path}")
        } else {
            format!("{DEFAULT_API_URL}{path}{sep}teamId={}", self.team)
        };
        let mut request = self
            .http
            .request(method, url)
            // fixed-deadline: allow — a live Vercel call in a by-hand run, never under the test multiplier
            .timeout(Duration::from_secs(300))
            .bearer_auth(token)
            // Vercel answers 415 to a POST without it, even with no body.
            .header("Content-Type", "application/json");
        if let Some(body) = body {
            request = request.body(body.to_string());
        }
        self.answer(request.send().await).await
    }

    /// One registry `HEAD` of `image`'s manifest, with the team and `token`
    /// as basic credentials.
    pub async fn manifest(&self, token: &str, image: &str) -> Answer {
        let (repository, tag) = image.rsplit_once(':').unwrap_or((image, "latest"));
        let request = self
            .http
            .head(format!(
                "{DEFAULT_REGISTRY_URL}/v2/{repository}/manifests/{tag}"
            ))
            // fixed-deadline: allow — a live registry call in a by-hand run
            .timeout(Duration::from_secs(60))
            .header("Accept", super::client::MANIFEST_ACCEPT)
            .basic_auth(&self.team, Some(token));
        self.answer(request.send().await).await
    }

    async fn answer(&self, sent: Result<reqwest::Response, reqwest::Error>) -> Answer {
        match sent {
            Ok(response) => {
                let status = response.status().as_u16();
                let ndjson = response
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .is_some_and(|v| v.starts_with("application/x-ndjson"));
                let bytes = response.bytes().await.unwrap_or_default();
                let text = String::from_utf8_lossy(&bytes);
                let body = if ndjson {
                    json!({ "ndjson": text
                        .lines()
                        .filter(|l| !l.trim().is_empty())
                        .map(|l| serde_json::from_str(l).unwrap_or_else(|_| json!({ "unparsed": scrub(l, &self.token) })))
                        .collect::<Vec<Value>>() })
                } else if bytes.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes)
                        .unwrap_or_else(|_| json!({ "unparsed": scrub(&text, &self.token) }))
                };
                Answer { status, body }
            }
            Err(e) => Answer {
                status: 0,
                body: json!({ "transport": scrub(&e.to_string(), &self.token) }),
            },
        }
    }

    /// Delete every sandbox tagged with `backend`, with its snapshots,
    /// then check none is left.
    pub async fn clean(&self, backend: &str) -> Vec<String> {
        let mut left = Vec::new();
        for _ in 0..3 {
            left.clear();
            let listed = self
                .send(
                    &self.token,
                    Method::GET,
                    &format!(
                        "/v2/sandboxes?project={}&tags={BACKEND_ID_TAG}:{backend}",
                        self.project
                    ),
                    None,
                )
                .await;
            if listed.status != 200 {
                left.push(format!("list answered {}", listed.status));
            }
            for sandbox in listed.body["sandboxes"]
                .as_array()
                .cloned()
                .unwrap_or_default()
            {
                let name = sandbox["name"].as_str().unwrap_or_default().to_owned();
                let deleted = self
                    .send(
                        &self.token,
                        Method::DELETE,
                        &format!(
                            "/v2/sandboxes/{name}?projectId={}&deleteOrphanSnapshots=true",
                            self.project
                        ),
                        None,
                    )
                    .await;
                if deleted.status != 200 && deleted.status != 404 {
                    left.push(name);
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
    /// Real id → placeholder: sessions and snapshots in the order seen,
    /// then the team and project.
    ids: Vec<(String, String)>,
    exchanges: Vec<Value>,
}

impl Recorder {
    fn learn(&mut self, value: &Value) {
        let mut found = Vec::new();
        for pointer in [
            "/session/id",
            "/sandbox/currentSessionId",
            "/snapshot/id",
            "/sandbox/currentSnapshotId",
            "/session/sourceSnapshotId",
        ] {
            if let Some(id) = value.pointer(pointer).and_then(Value::as_str) {
                found.push(id.to_owned());
            }
        }
        for id in found {
            if self.ids.iter().any(|(real, _)| *real == id) {
                continue;
            }
            let n = self.ids.len() + 1;
            let placeholder = if id.starts_with("snap_") {
                fixture_snapshot_id(n)
            } else {
                fixture_session_id(n)
            };
            self.ids.push((id, placeholder));
        }
    }

    /// One recorded REST call. `auth` is `"token"` or `"wrong"`.
    async fn call(
        &mut self,
        name: &str,
        auth: &str,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Answer {
        let token = if auth == "token" {
            self.live.token.clone()
        } else {
            WRONG_TOKEN.to_owned()
        };
        let answer = self
            .live
            .send(&token, method.clone(), path, body.as_ref())
            .await;
        self.learn(&answer.body);
        let mut request =
            json!({ "plane": "rest", "auth": auth, "method": method.as_str(), "path": path });
        if let Some(body) = body {
            request["body"] = body;
        }
        self.push(name, &request, &answer);
        answer
    }

    async fn registry(&mut self, name: &str, auth: &str, image: &str) -> Answer {
        let token = if auth == "token" {
            self.live.token.clone()
        } else {
            WRONG_TOKEN.to_owned()
        };
        let answer = self.live.manifest(&token, image).await;
        let (repository, tag) = image.rsplit_once(':').unwrap_or((image, "latest"));
        let path = format!("/v2/{repository}/manifests/{tag}");
        let request = json!({ "plane": "registry", "auth": auth, "method": "HEAD", "path": path });
        self.push(name, &request, &answer);
        answer
    }

    fn push(&mut self, name: &str, request: &Value, answer: &Answer) {
        let method = request["method"].as_str().unwrap_or_default().to_owned();
        let path = request["path"].as_str().unwrap_or_default().to_owned();
        self.exchanges.push(json!({
            "name": name,
            "request": request,
            "response": { "status": answer.status, "body": answer.body },
        }));
        eprintln!(
            "{name}: {method} {} -> {}",
            path.split('?').next().unwrap_or(&path),
            answer.status
        );
    }

    /// Replace ids, times, digests and the token. Request bodies keep
    /// their numbers: they are what was sent.
    fn sanitize(&self, mut value: Value) -> Value {
        if let Some(exchanges) = value.get_mut("exchanges").and_then(Value::as_array_mut) {
            for exchange in exchanges {
                if let Some(request) = exchange.get_mut("request") {
                    walk(request, self, false);
                }
                if let Some(response) = exchange.get_mut("response") {
                    walk(response, self, true);
                }
            }
        }
        walk(&mut value, self, false);
        value
    }
}

/// Sanitize `value` for the fixture; `numbers` replaces times and
/// measurements too.
fn walk(value: &mut Value, recorder: &Recorder, numbers: bool) {
    match value {
        Value::String(text) => {
            // Only the token goes: command output keeps its bytes.
            let mut out = text.replace(recorder.live.token.as_str(), "<redacted>");
            for (real, placeholder) in &recorder.ids {
                out = out.replace(real.as_str(), placeholder);
            }
            out = out
                .replace(recorder.live.team.as_str(), FIXTURE_TEAM)
                .replace(recorder.live.project.as_str(), FIXTURE_PROJECT);
            if let Some(at) = out.find("@sha256:") {
                out = format!("{}@{FIXTURE_DIGEST}", &out[..at]);
            }
            *text = out;
        }
        Value::Array(items) => items.iter_mut().for_each(|v| walk(v, recorder, numbers)),
        Value::Object(map) => {
            for (key, v) in map.iter_mut() {
                if numbers && TIME_KEYS.contains(&key.as_str()) && v.is_number() {
                    *v = json!(FIXTURE_EPOCH_MS);
                } else if numbers && MEASURED_KEYS.contains(&key.as_str()) && v.is_number() {
                    *v = json!(0);
                } else {
                    walk(v, recorder, numbers);
                }
            }
        }
        _ => {}
    }
}

/// The create body of the capture's sandbox.
pub(super) fn create_body(project: &str, image: &str, vcpus: u32) -> Value {
    json!({
        "projectId": project,
        "name": CAPTURE_SANDBOX,
        "image": image,
        "persistent": true,
        "resources": { "vcpus": vcpus },
        "timeout": 30 * 60 * 1000,
        "tags": {
            BACKEND_ID_TAG: CAPTURE_BACKEND,
            "workspaceId": "capture-workspace",
            "agentId": "capture-agent",
            "intentEntryId": "vercel-000000000000",
            "intentdVersion": "0.0.0",
        },
        "keepLastSnapshots": { "count": 1, "deleteEvicted": true },
        "snapshotExpiration": super::SNAPSHOT_EXPIRATION_MS,
    })
}

fn command(script: &str) -> Value {
    json!({
        "command": "/bin/sh",
        "args": ["-c", script],
        "cwd": "/home/user",
        "env": { "CAPTURE_VAR": "from-env" },
        "wait": true,
        "logs": true,
    })
}

async fn record(r: &mut Recorder, image: &str) {
    record_refusals(r, image).await;
    record_lifecycle(r, image).await;
}

/// The refusals before any sandbox: token, team, project, image, size.
async fn record_refusals(r: &mut Recorder, image: &str) {
    let (get, post) = (Method::GET, Method::POST);
    let project = r.live.project.clone();
    let list = format!("/v2/sandboxes?project={project}&limit=1");
    r.call("list-wrong-token", "wrong", get.clone(), &list, None)
        .await;
    r.call(
        "list-wrong-team",
        "token",
        get.clone(),
        &format!("{list}&teamId={MISSING_TEAM}"),
        None,
    )
    .await;
    r.call(
        "list-missing-project",
        "token",
        get.clone(),
        &format!("/v2/sandboxes?project={MISSING_PROJECT}&limit=1"),
        None,
    )
    .await;
    r.call("list", "token", get.clone(), &list, None).await;
    r.call(
        "list-page-too-large",
        "token",
        get.clone(),
        &format!("/v2/sandboxes?project={project}&limit=51"),
        None,
    )
    .await;
    let (repository, _) = image.rsplit_once(':').unwrap_or((image, ""));
    let missing = format!("{repository}:{MISSING_TAG}");
    r.registry("manifest", "token", image).await;
    r.registry("manifest-missing-tag", "token", &missing).await;
    r.registry("manifest-wrong-token", "wrong", image).await;
    r.call(
        "create-missing-image",
        "token",
        post.clone(),
        "/v3/sandboxes",
        Some(create_body(&project, &missing, 2)),
    )
    .await;
    r.call(
        "create-too-many-vcpus",
        "token",
        post.clone(),
        "/v3/sandboxes",
        Some(create_body(&project, image, 32)),
    )
    .await;
    r.call(
        "create-in-missing-project",
        "token",
        post.clone(),
        "/v3/sandboxes",
        Some(create_body(MISSING_PROJECT, image, 2)),
    )
    .await;
}

/// One sandbox's life: create, read, list, commands, extend, stop, resume,
/// delete, and the refusals on the way.
async fn record_lifecycle(r: &mut Recorder, image: &str) {
    let (get, post, delete) = (Method::GET, Method::POST, Method::DELETE);
    let project = r.live.project.clone();
    let created = r
        .call(
            "create",
            "token",
            post.clone(),
            "/v3/sandboxes",
            Some(create_body(&project, image, 2)),
        )
        .await;
    assert!(
        (200..300).contains(&created.status),
        "the create failed: {}",
        created.status
    );
    let session = created.body["session"]["id"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    r.call(
        "create-same-name",
        "token",
        post.clone(),
        "/v3/sandboxes",
        Some(create_body(&project, image, 2)),
    )
    .await;
    let path = format!("/v2/sandboxes/{CAPTURE_SANDBOX}?projectId={project}");
    r.call("get-sandbox", "token", get.clone(), &path, None)
        .await;
    r.call(
        "list-by-tag",
        "token",
        get.clone(),
        &format!("/v2/sandboxes?project={project}&tags={BACKEND_ID_TAG}:{CAPTURE_BACKEND}"),
        None,
    )
    .await;
    let cmd = format!("/v2/sandboxes/sessions/{session}/cmd");
    r.call(
        "command",
        "token",
        post.clone(),
        &cmd,
        Some(command(SCRIPT)),
    )
    .await;
    r.call(
        "command-missing-binary",
        "token",
        post.clone(),
        &cmd,
        Some(json!({ "command": "no-such-binary", "args": [], "wait": true, "logs": true })),
    )
    .await;
    r.call(
        "command-unknown-session",
        "token",
        post.clone(),
        "/v2/sandboxes/sessions/sbx_000000000000000000000000/cmd",
        Some(command("true")),
    )
    .await;
    let extend = format!("/v2/sandboxes/sessions/{session}/extend-timeout");
    r.call(
        "extend-timeout",
        "token",
        post.clone(),
        &extend,
        Some(json!({ "duration": 30 * 60 * 1000 })),
    )
    .await;
    r.call(
        "extend-timeout-past-the-cap",
        "token",
        post.clone(),
        &extend,
        Some(json!({ "duration": 30u64 * 60 * 60 * 1000 })),
    )
    .await;
    let stop = format!("/v2/sandboxes/sessions/{session}/stop");
    r.call("stop", "token", post.clone(), &stop, None).await;
    r.call("get-stopped", "token", get.clone(), &path, None)
        .await;
    r.call("stop-stopped", "token", post.clone(), &stop, None)
        .await;
    r.call(
        "command-on-stopped-session",
        "token",
        post.clone(),
        &cmd,
        Some(command("true")),
    )
    .await;
    r.call(
        "extend-stopped-session",
        "token",
        post.clone(),
        &extend,
        Some(json!({ "duration": 30 * 60 * 1000 })),
    )
    .await;
    let resumed = r
        .call(
            "resume",
            "token",
            get.clone(),
            &format!("{path}&resume=true"),
            None,
        )
        .await;
    let session2 = resumed.body["session"]["id"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    r.call(
        "resume-running",
        "token",
        get.clone(),
        &format!("{path}&resume=true"),
        None,
    )
    .await;
    r.call(
        "command-after-resume",
        "token",
        post.clone(),
        &format!("/v2/sandboxes/sessions/{session2}/cmd"),
        Some(command("cat /home/user/marker")),
    )
    .await;
    let del =
        format!("/v2/sandboxes/{CAPTURE_SANDBOX}?projectId={project}&deleteOrphanSnapshots=true");
    r.call("delete", "token", delete.clone(), &del, None).await;
    r.call("delete-again", "token", delete.clone(), &del, None)
        .await;
    r.call("get-deleted", "token", get.clone(), &path, None)
        .await;
    r.call(
        "resume-unknown",
        "token",
        get.clone(),
        &format!("/v2/sandboxes/intent-capture-missing?projectId={project}&resume=true"),
        None,
    )
    .await;
}

#[tokio::test]
async fn capture_vercel_fixtures() {
    if std::env::var(CAPTURE_ENV).as_deref() != Ok("1") {
        eprintln!("skipped: set {CAPTURE_ENV}=1 (scripts/capture-vercel-fixtures.sh)");
        return;
    }
    let token = live_token().unwrap_or_else(|| panic!("set {TOKEN_ENV} or {TOKEN_FILE_ENV}"));
    let (team, project) = live_scope();
    let image = std::env::var(IMAGE_ENV).unwrap_or_else(|_| DEFAULT_IMAGE.to_owned());
    let live = Live::new(token, team, project);
    let left = live.clean(CAPTURE_BACKEND).await;
    assert!(left.is_empty(), "an earlier run left {left:?}");
    let mut recorder = Recorder {
        live,
        ids: Vec::new(),
        exchanges: Vec::new(),
    };
    let outcome = AssertUnwindSafe(record(&mut recorder, &image))
        .catch_unwind()
        .await;
    let left = recorder.live.clean(CAPTURE_BACKEND).await;
    assert!(left.is_empty(), "the capture could not delete {left:?}");
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    let fixture = recorder.sanitize(json!({
        "recorded": "the real Vercel Sandbox API, by scripts/capture-vercel-fixtures.sh",
        "image": image,
        "token": FIXTURE_TOKEN,
        "team": FIXTURE_TEAM,
        "project": FIXTURE_PROJECT,
        "exchanges": recorder.exchanges,
    }));
    let text = serde_json::to_string_pretty(&fixture).expect("encode");
    assert!(
        !text.contains(&recorder.live.token),
        "the token reached the fixture"
    );
    let out = std::env::var(OUT_ENV).unwrap_or_else(|_| {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/vercel/fixtures/vercel.json"
        )
        .to_owned()
    });
    std::fs::write(&out, text + "\n").expect("write the fixture");
    eprintln!("wrote {out}");
}
