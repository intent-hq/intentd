//! The one model of the Vercel Sandbox API that every fake uses: the unit
//! fake server ([`super::fake_server`]), the e2e fake in intentd's tests,
//! and the contract test that holds it to the recording of the real API
//! (`fixtures/vercel.json`). It answers REST and registry calls and checks
//! command calls; the caller runs a command and builds its stream with
//! [`command_stream`].
//!
//! Behaviour it copies from the real API (recorded 2026-11-20): a wrong
//! token is `403` with `invalidToken`, a team the token cannot reach a
//! plain `403`, a missing project or image `404`; a create answers `200`
//! with the running sandbox and its first session; a stop snapshots the
//! disk and answers again for a stopped session; a resume (`GET` with
//! `resume=true`) starts a new session from the snapshot, without
//! processes; a stopped session's commands and extensions are `410`; an
//! extension past the plan's longest session is `400`
//! `sandbox_timeout_invalid`; a repeated delete is `404`; a command streams
//! newline-delimited JSON.

use serde_json::{json, Map, Value};

/// The token a fake accepts unless a test sets another.
pub const FIXTURE_TOKEN: &str = "fixture-vercel-token-0000000000";
/// The team and project the recording names.
pub const FIXTURE_TEAM: &str = "team_fixture000000000000000";
pub const FIXTURE_PROJECT: &str = "prj_fixture00000000000000000000";
/// The epoch millisecond every recorded time is written as.
pub const FIXTURE_EPOCH_MS: u64 = 1_794_900_000_000;
/// The image digest every fake sandbox reports.
pub const FIXTURE_DIGEST: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000000";
/// The default session timeout of a sandbox created without one.
const DEFAULT_TIMEOUT_MS: u64 = 5 * 60 * 1000;
/// The Pro plan's longest session.
pub const PRO_MAX_SESSION_MS: u64 = 24 * 60 * 60 * 1000;

/// The `n`-th id the fake hands out (from 1), as a session id; sessions
/// and snapshots share the count, so the capture writes the real ids as
/// these, in the order it saw them.
#[must_use]
pub fn fixture_session_id(n: usize) -> String {
    format!("sbx_fx{n:08}")
}

/// The `n`-th id the fake hands out, as a snapshot id.
#[must_use]
pub fn fixture_snapshot_id(n: usize) -> String {
    format!("snap_fx{n:08}")
}

/// One answer: an HTTP status and a JSON body (`Null` for none).
#[derive(Debug, Clone, PartialEq)]
pub struct Reply {
    pub status: u16,
    pub body: Value,
}

impl Reply {
    #[must_use]
    pub fn ok(body: Value) -> Self {
        Self { status: 200, body }
    }

    /// The REST error envelope.
    #[must_use]
    pub fn error(status: u16, code: &str, message: &str) -> Self {
        Self {
            status,
            body: json!({ "error": { "code": code, "message": message } }),
        }
    }

    /// A status with no body (the registry).
    #[must_use]
    pub fn empty(status: u16) -> Self {
        Self {
            status,
            body: Value::Null,
        }
    }
}

/// What happened to a sandbox, for a harness that runs its processes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Created(String),
    /// The session stopped (a stop, or its timeout ran out): processes
    /// end, the disk stays.
    Stopped(String),
    /// A new session from the snapshot: no processes, the same disk.
    Resumed(String),
    Deleted(String),
}

#[derive(Debug, Clone)]
pub struct Session {
    pub id: String,
    pub status: String,
    pub started_at: u64,
    /// Milliseconds from `started_at` to the session's end.
    pub timeout: u64,
    pub stopped_at: Option<u64>,
    pub source_snapshot: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Sandbox {
    pub name: String,
    pub project: String,
    /// The reference the create named.
    pub image: String,
    pub vcpus: u32,
    /// The timeout each new session starts with.
    pub timeout: u64,
    pub persistent: bool,
    pub tags: Map<String, Value>,
    pub keep_last_snapshots: Value,
    pub snapshot_expiration: Value,
    pub session: Session,
    pub snapshot: Option<String>,
    pub created_at: u64,
}

impl Sandbox {
    fn digest_image(&self) -> String {
        let repository = self.image.rsplit_once(':').map_or(&*self.image, |(r, _)| r);
        format!("{repository}@{FIXTURE_DIGEST}")
    }

    fn json(&self) -> Value {
        let mut value = json!({
            "name": self.name,
            "currentSessionId": self.session.id,
            "status": self.session.status,
            "statusUpdatedAt": self.created_at,
            "persistent": self.persistent,
            "workloadType": "sandbox",
            "region": "iad1",
            "failoverRegions": [],
            "vcpus": self.vcpus,
            "memory": self.vcpus * 2048,
            "runtime": "node22",
            "architecture": "amd64",
            "image": self.digest_image(),
            "timeout": self.timeout,
            "snapshotExpiration": self.snapshot_expiration,
            "keepLastSnapshots": self.keep_last_snapshots,
            "cwd": "/home/user",
            "tags": self.tags,
            "createdAt": self.created_at,
            "updatedAt": self.created_at,
        });
        if let Some(snapshot) = &self.snapshot {
            value["currentSnapshotId"] = json!(snapshot);
        }
        value
    }

    fn session_json(&self) -> Value {
        let session = &self.session;
        let mut value = json!({
            "id": session.id,
            "runtime": "node22",
            "architecture": "amd64",
            "requestedAt": session.started_at,
            "startedAt": session.started_at,
            "status": session.status,
            "vcpus": self.vcpus,
            "memory": self.vcpus * 2048,
            "timeout": session.timeout,
            "region": "iad1",
            "cwd": "/home/user",
            "workloadType": "sandbox",
            "createdAt": session.started_at,
            "updatedAt": session.started_at,
            "interactivePort": 26661,
            "sourceSandboxName": self.name,
            "projectId": self.project,
        });
        if let Some(at) = session.stopped_at {
            value["requestedStopAt"] = json!(at);
            value["stoppedAt"] = json!(at);
        }
        if let Some(snapshot) = &session.source_snapshot {
            value["sourceSnapshotId"] = json!(snapshot);
        }
        value
    }
}

/// A checked command: what to run, in which sandbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandStart {
    pub sandbox: String,
    pub session: String,
    pub command: String,
    pub args: Vec<String>,
    pub cwd: Option<String>,
    pub env: Vec<(String, String)>,
    /// Whether the caller asked for the output lines (`logs: true`).
    pub logs: bool,
}

/// The state of one fake Vercel team and project.
#[derive(Debug, Clone)]
pub struct VercelApi {
    pub token: String,
    pub team: String,
    pub project: String,
    pub sandboxes: Vec<Sandbox>,
    /// Image references (`<repository>:<tag>`) the registry has, besides
    /// every published Intent node tag.
    pub images: Vec<String>,
    /// The plan's most vCPUs per sandbox.
    pub max_vcpus: u32,
    /// The plan's longest session.
    pub max_session_ms: u64,
    /// Executables the image lacks: a command naming one is refused
    /// before it runs. The image has no `sudo`.
    pub missing: Vec<String>,
    pub events: Vec<Event>,
    ids: usize,
    commands: usize,
    clock: u64,
}

impl Default for VercelApi {
    fn default() -> Self {
        Self::new(FIXTURE_TOKEN, FIXTURE_TEAM, FIXTURE_PROJECT)
    }
}

fn bad_request(message: &str) -> Reply {
    Reply::error(400, "bad_request", message)
}

fn query<'a>(path: &'a str, name: &str) -> Option<&'a str> {
    path.split_once('?')?
        .1
        .split('&')
        .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
}

impl VercelApi {
    #[must_use]
    pub fn new(token: &str, team: &str, project: &str) -> Self {
        Self {
            token: token.to_owned(),
            team: team.to_owned(),
            project: project.to_owned(),
            sandboxes: Vec::new(),
            images: Vec::new(),
            max_vcpus: 8,
            max_session_ms: PRO_MAX_SESSION_MS,
            missing: vec!["sudo".into(), "no-such-binary".into()],
            events: Vec::new(),
            ids: 0,
            commands: 0,
            clock: 0,
        }
    }

    /// Now, in epoch milliseconds: the real clock, so a session's end
    /// compares with the client's, never going back.
    fn tick(&mut self) -> u64 {
        let now = u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
            .unwrap_or_default();
        self.clock = now.max(self.clock + 1);
        self.clock
    }

    fn next_id(&mut self) -> usize {
        self.ids += 1;
        self.ids
    }

    /// Whether the registry has `image`: a listed one, or a published
    /// Intent node tag (`<default repository>:<digits and dots>`, not
    /// `0.0.*`).
    #[must_use]
    pub fn has_image(&self, image: &str) -> bool {
        self.images.iter().any(|i| i == image)
            || image
                .strip_prefix(super::DEFAULT_IMAGE)
                .and_then(|rest| rest.strip_prefix(':'))
                .is_some_and(|tag| {
                    !tag.is_empty()
                        && tag.split('.').all(|part| {
                            !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit())
                        })
                        && !tag.starts_with("0.0.")
                })
    }

    #[must_use]
    pub fn sandbox(&self, name: &str) -> Option<&Sandbox> {
        self.sandboxes.iter().find(|s| s.name == name)
    }

    pub fn sandbox_mut(&mut self, name: &str) -> Option<&mut Sandbox> {
        self.sandboxes.iter_mut().find(|s| s.name == name)
    }

    fn by_session(&self, session: &str) -> Option<usize> {
        self.sandboxes.iter().position(|s| s.session.id == session)
    }

    /// The `403` of a wrong token, or of a team the token cannot reach.
    fn refuse(&self, bearer: Option<&str>, team: Option<&str>) -> Option<Reply> {
        if bearer != Some(self.token.as_str()) {
            return Some(Reply {
                status: 403,
                body: json!({ "error": {
                    "code": "forbidden", "message": "Not authorized", "invalidToken": true,
                } }),
            });
        }
        (team != Some(self.team.as_str())).then(|| Reply::error(403, "forbidden", "Not authorized"))
    }

    fn project_missing(project: &str) -> Reply {
        Reply::error(
            404,
            "not_found",
            &format!("Could not find project: {project}"),
        )
    }

    /// Answer a REST call to `https://api.vercel.com{path}` (with its
    /// query). `bearer` is the `Authorization: Bearer` value, if any.
    pub fn handle(
        &mut self,
        method: &str,
        path: &str,
        bearer: Option<&str>,
        body: Option<&Value>,
    ) -> Reply {
        if let Some(refused) = self.refuse(bearer, query(path, "teamId")) {
            return refused;
        }
        let route = path.split('?').next().unwrap_or_default();
        let parts: Vec<&str> = route.trim_matches('/').split('/').collect();
        let empty = Value::Object(Map::new());
        let body = body.unwrap_or(&empty);
        match (method, parts.as_slice()) {
            ("GET", ["v2", "sandboxes"]) => self.list(path),
            ("POST", ["v3", "sandboxes"]) => self.create(body),
            ("GET", ["v2", "sandboxes", name]) => self.get(name, path),
            ("DELETE", ["v2", "sandboxes", name]) => self.delete(name, path),
            ("POST", ["v2", "sandboxes", "sessions", id, "extend-timeout"]) => {
                self.extend(id, body)
            }
            ("POST", ["v2", "sandboxes", "sessions", id, "stop"]) => self.stop_session(id),
            _ => Reply::error(404, "not_found", "The requested path was not found."),
        }
    }

    fn list(&self, path: &str) -> Reply {
        let project = query(path, "project").unwrap_or_default();
        if project != self.project {
            return Self::project_missing(project);
        }
        let tag = query(path, "tags").and_then(|t| t.split_once(':'));
        let sandboxes: Vec<Value> = self
            .sandboxes
            .iter()
            .rev()
            .filter(|s| tag.is_none_or(|(k, v)| s.tags.get(k).and_then(Value::as_str) == Some(v)))
            .map(Sandbox::json)
            .collect();
        let limit = query(path, "limit")
            .and_then(|l| l.parse::<usize>().ok())
            .unwrap_or(50);
        if limit > 50 {
            return bad_request("Invalid request: `limit` should be <= 50.");
        }
        let page: Vec<Value> = sandboxes.into_iter().take(limit).collect();
        Reply::ok(json!({
            "sandboxes": page,
            "pagination": { "count": page.len(), "next": null },
        }))
    }

    fn create(&mut self, body: &Value) -> Reply {
        let project = body
            .get("projectId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if project != self.project {
            return Self::project_missing(project);
        }
        let vcpus = body
            .pointer("/resources/vcpus")
            .and_then(Value::as_u64)
            .map_or(2, |v| u32::try_from(v).unwrap_or(u32::MAX));
        if vcpus > self.max_vcpus {
            return bad_request(&format!(
                "`resources.vcpus` should be <= {}",
                self.max_vcpus
            ));
        }
        if vcpus != 1 && !vcpus.is_multiple_of(2) {
            return bad_request("`resources.vcpus` should be even");
        }
        let timeout = body
            .get("timeout")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_TIMEOUT_MS);
        if timeout > self.max_session_ms {
            return bad_request("`timeout` should be <= 1d");
        }
        let tags = body
            .get("tags")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        if tags.len() > super::MAX_TAGS {
            return bad_request("`tags` should have at most 5 entries");
        }
        let image = body
            .get("image")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if !self.has_image(&image) {
            return Reply::error(404, "not_found", "Image not found.");
        }
        let name = body
            .get("name")
            .and_then(Value::as_str)
            .map_or_else(|| format!("sandbox-{}", self.ids + 1), str::to_owned);
        if self.sandbox(&name).is_some() {
            return bad_request(&format!(
                "A sandbox with the name '{name}' already exists for this project. Use GET /sandboxes/:name to resume it or delete it first."
            ));
        }
        let snapshot_expiration = body
            .get("snapshotExpiration")
            .cloned()
            .unwrap_or(Value::Null);
        let mut keep_last = body
            .get("keepLastSnapshots")
            .cloned()
            .unwrap_or(Value::Null);
        if keep_last.is_object() && keep_last.get("expiration").is_none() {
            keep_last["expiration"] = snapshot_expiration.clone();
        }
        let now = self.tick();
        let id = self.next_id();
        let sandbox = Sandbox {
            name: name.clone(),
            project: project.to_owned(),
            image,
            vcpus,
            timeout,
            persistent: body
                .get("persistent")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            tags,
            keep_last_snapshots: keep_last,
            snapshot_expiration,
            session: Session {
                id: fixture_session_id(id),
                status: "running".into(),
                started_at: now,
                timeout,
                stopped_at: None,
                source_snapshot: None,
            },
            snapshot: None,
            created_at: now,
        };
        let reply = Reply::ok(json!({
            "sandbox": sandbox.json(),
            "session": sandbox.session_json(),
            "routes": [],
        }));
        self.sandboxes.push(sandbox);
        self.events.push(Event::Created(name));
        reply
    }

    fn not_named(name: &str) -> Reply {
        Reply::error(
            404,
            "not_found",
            &format!("Named sandbox '{name}' not found for this project."),
        )
    }

    fn get(&mut self, name: &str, path: &str) -> Reply {
        let project = query(path, "projectId").unwrap_or_default();
        if project != self.project {
            return Self::project_missing(project);
        }
        let resume = query(path, "resume") == Some("true");
        let Some(at) = self.sandboxes.iter().position(|s| s.name == name) else {
            return Self::not_named(name);
        };
        let mut resumed = false;
        if resume && self.sandboxes[at].session.status == "stopped" {
            let now = self.tick();
            let id = self.next_id();
            let sandbox = &mut self.sandboxes[at];
            sandbox.session = Session {
                id: fixture_session_id(id),
                status: "running".into(),
                started_at: now,
                timeout: sandbox.timeout,
                stopped_at: None,
                source_snapshot: sandbox.snapshot.clone(),
            };
            resumed = true;
            self.events.push(Event::Resumed(name.to_owned()));
        }
        let sandbox = &self.sandboxes[at];
        Reply::ok(json!({
            "sandbox": sandbox.json(),
            "session": sandbox.session_json(),
            "routes": [],
            "resumed": resumed,
        }))
    }

    fn delete(&mut self, name: &str, path: &str) -> Reply {
        let project = query(path, "projectId").unwrap_or_default();
        if project != self.project {
            return Self::project_missing(project);
        }
        let Some(at) = self.sandboxes.iter().position(|s| s.name == name) else {
            return Reply::error(
                404,
                "not_found",
                &format!("Sandbox '{name}' not found for this project."),
            );
        };
        let sandbox = self.sandboxes.remove(at);
        self.events.push(Event::Deleted(name.to_owned()));
        Reply::ok(json!({ "sandbox": sandbox.json() }))
    }

    fn session_gone() -> Reply {
        Reply::error(404, "not_found", "Vercel Sandbox not found.")
    }

    fn session_stopped() -> Reply {
        Reply::error(
            410,
            "sandbox_stopped",
            "Sandbox has stopped execution and is no longer available",
        )
    }

    fn extend(&mut self, session: &str, body: &Value) -> Reply {
        let Some(at) = self.by_session(session) else {
            return Self::session_gone();
        };
        let max = self.max_session_ms;
        let sandbox = &mut self.sandboxes[at];
        if sandbox.session.status != "running" {
            return Self::session_stopped();
        }
        let duration = body.get("duration").and_then(Value::as_u64).unwrap_or(0);
        if sandbox.session.timeout + duration > max {
            return Reply::error(
                400,
                "sandbox_timeout_invalid",
                "Failed to extend timeout: extension would exceed maximum execution timeout",
            );
        }
        sandbox.session.timeout += duration;
        Reply::ok(json!({ "session": sandbox.session_json() }))
    }

    fn stop_session(&mut self, session: &str) -> Reply {
        let Some(at) = self.by_session(session) else {
            return Self::session_gone();
        };
        if self.sandboxes[at].session.status == "running" {
            self.stop_at(at);
        }
        let sandbox = &self.sandboxes[at];
        let snapshot = sandbox.snapshot.clone().unwrap_or_default();
        Reply::ok(json!({
            "sandbox": sandbox.json(),
            "session": sandbox.session_json(),
            "snapshot": {
                "id": snapshot,
                "sourceSessionId": sandbox.session.id,
                "workloadType": "sandbox",
                "architecture": "amd64",
                "region": "iad1",
                "regions": ["iad1"],
                "status": "created",
                "sizeBytes": 0,
                "createdAt": sandbox.session.stopped_at,
                "updatedAt": sandbox.session.stopped_at,
                "creationMethod": "automatic",
            },
        }))
    }

    /// Stop sandbox `at`'s running session with a snapshot.
    fn stop_at(&mut self, at: usize) {
        let now = self.tick();
        let id = self.next_id();
        let sandbox = &mut self.sandboxes[at];
        sandbox.session.status = "stopped".into();
        sandbox.session.stopped_at = Some(now);
        if sandbox.persistent {
            sandbox.snapshot = Some(fixture_snapshot_id(id));
        }
        let name = sandbox.name.clone();
        self.events.push(Event::Stopped(name));
    }

    /// The session of sandbox `name` ran out of time: Vercel stops it
    /// with a snapshot, as a stop does.
    pub fn time_out(&mut self, name: &str) {
        if let Some(at) = self
            .sandboxes
            .iter()
            .position(|s| s.name == name && s.session.status == "running")
        {
            self.stop_at(at);
        }
    }

    /// Check a command to `POST /v2/sandboxes/sessions/{id}/cmd`. `Ok` is
    /// the command for the caller to run; `Err` the refusal to send.
    ///
    /// # Errors
    ///
    /// The refusal, as Vercel sends it.
    pub fn command(
        &mut self,
        session: &str,
        path: &str,
        bearer: Option<&str>,
        body: &Value,
    ) -> Result<CommandStart, Reply> {
        if let Some(refused) = self.refuse(bearer, query(path, "teamId")) {
            return Err(refused);
        }
        let Some(at) = self.by_session(session) else {
            return Err(Self::session_gone());
        };
        if self.sandboxes[at].session.status != "running" {
            return Err(Self::session_stopped());
        }
        let Some(command) = body.get("command").and_then(Value::as_str) else {
            return Err(bad_request("`command` is required"));
        };
        let sudo = body.get("sudo").and_then(Value::as_bool) == Some(true);
        let lacking = if sudo { Some("sudo") } else { None }
            .or_else(|| self.missing.iter().any(|m| m == command).then_some(command));
        if let Some(lacking) = lacking {
            return Err(Reply::error(
                400,
                "executable_not_found",
                &format!("[invalid_argument] executable file not found in $PATH: {lacking}"),
            ));
        }
        let strings = |value: Option<&Value>| -> Vec<String> {
            value
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default()
        };
        let env = body
            .get("env")
            .and_then(Value::as_object)
            .map(|map| {
                map.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
                    .collect()
            })
            .unwrap_or_default();
        Ok(CommandStart {
            sandbox: self.sandboxes[at].name.clone(),
            session: session.to_owned(),
            command: command.to_owned(),
            args: strings(body.get("args")),
            cwd: body.get("cwd").and_then(Value::as_str).map(str::to_owned),
            env,
            logs: body.get("logs").and_then(Value::as_bool) == Some(true),
        })
    }

    /// The next command id.
    pub fn command_id(&mut self) -> String {
        self.commands += 1;
        format!("cmd_fx{:08}", self.commands)
    }

    /// Answer a registry `HEAD /v2/<repository>/manifests/<tag>` with basic
    /// credentials `basic` (`team:token`).
    #[must_use]
    pub fn manifest(&self, path: &str, basic: Option<(&str, &str)>) -> Reply {
        if basic != Some((self.team.as_str(), self.token.as_str())) {
            return Reply::empty(403);
        }
        let Some((repository, tag)) = path
            .strip_prefix("/v2/")
            .and_then(|rest| rest.rsplit_once("/manifests/"))
        else {
            return Reply::empty(404);
        };
        Reply::empty(if self.has_image(&format!("{repository}:{tag}")) {
            200
        } else {
            404
        })
    }

    /// Take the events since the last call.
    pub fn drain_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }
}

/// The newline-delimited stream of a finished command: the command, its
/// output lines when `start.logs`, then the command with its exit code.
#[must_use]
pub fn command_stream(
    start: &CommandStart,
    id: &str,
    exit_code: i32,
    stdout: &str,
    stderr: &str,
) -> Vec<Value> {
    let command = |exit: Value| {
        let mut command = json!({
            "id": id,
            "name": start.command,
            "args": start.args,
            "cwd": start.cwd.clone().unwrap_or_else(|| "/home/user".into()),
            "sessionId": start.session,
            "startedAt": FIXTURE_EPOCH_MS,
            "exitCode": exit,
        });
        if !command["exitCode"].is_null() {
            command["durationMs"] = json!(0);
        }
        json!({ "command": command })
    };
    let mut lines = vec![command(Value::Null)];
    if start.logs {
        if !stderr.is_empty() {
            lines.push(json!({ "data": stderr, "stream": "stderr" }));
        }
        if !stdout.is_empty() {
            lines.push(json!({ "data": stdout, "stream": "stdout" }));
        }
    }
    lines.push(command(json!(exit_code)));
    lines
}
