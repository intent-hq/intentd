//! The one model of the Agent37 API that every fake uses: the unit fake
//! server ([`super::fake`]), the e2e fake in intentd's tests, and the
//! contract test that holds it to the recording of the real API
//! (`fixtures/agent37.json`). It answers REST calls and checks exec calls;
//! the caller runs an exec's command and builds its reply with
//! [`exec_reply`].
//!
//! Behaviour it copies from the real API (recorded 2026-11-17): a create
//! answers `201` once the instance runs; `default` runs only in `us`;
//! sleep answers once the instance sleeps and again for a sleeping one;
//! start wakes a sleeping or stopped instance and acks a running one; an
//! exec wakes a sleeping instance first; a repeated delete is `404`; exec
//! refusals on the sandbox host are flat `{"error": "<code>"}` bodies.

use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

/// The key a fake accepts unless a test sets another.
pub const FIXTURE_KEY: &str = "fixture-agent37-key-000000000000";
/// The epoch second the fake's clock starts at.
pub const FIXTURE_EPOCH: u64 = 1_794_900_000;
/// The image digest every fake template reports.
pub const FIXTURE_DIGEST: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// The `n`-th instance id the fake hands out (from 1); the capture writes
/// the real ids as these, in creation order.
#[must_use]
pub fn fixture_instance_id(n: usize) -> String {
    format!("fx{n:08}")
}

/// One answer: an HTTP status and a JSON body.
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

    /// A refusal before the request reached the instance (sandbox host).
    #[must_use]
    pub fn flat(status: u16, code: &str) -> Self {
        Self {
            status,
            body: json!({ "error": code }),
        }
    }
}

/// What happened to an instance, for a harness that runs its processes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Created(String),
    Slept(String),
    /// A wake; `fresh` when it booted from the disk, losing processes.
    Woke {
        id: String,
        fresh: bool,
    },
    Deleted(String),
}

#[derive(Debug, Clone)]
pub struct Instance {
    pub id: String,
    pub status: String,
    pub template: String,
    pub template_revision: u64,
    pub image_ref: String,
    pub instance_type: String,
    pub region: String,
    pub cpu: u32,
    pub memory: u32,
    pub disk: u32,
    pub name: Value,
    pub metadata: Value,
    pub auto_sleep: bool,
    pub idle_timeout_seconds: u64,
    pub created: u64,
    pub slept_at: Option<u64>,
    /// The next wake boots fresh instead of restoring memory.
    pub wake_fresh: bool,
}

impl Instance {
    fn json(&self) -> Value {
        let mut value = json!({
            "id": self.id,
            "status": self.status,
            "status_reason": null,
            "template": self.template,
            "template_revision": self.template_revision,
            "image_ref": self.image_ref,
            "image_digest": FIXTURE_DIGEST,
            "type": self.instance_type,
            "region": self.region,
            "mount_path": "/home/user",
            "resources": { "cpu": self.cpu, "memory": self.memory, "disk": self.disk },
            "url": format!("https://{}.agent37.app", self.id),
            "domain_urls": [],
            "public_ports": [],
            "name": self.name,
            "metadata": self.metadata,
            "auto_sleep": self.auto_sleep,
            "idle_timeout_seconds": self.idle_timeout_seconds,
            "network": "allow-all",
            "past_due": false,
            "created": self.created,
        });
        if self.status == "sleeping" || self.status == "waking" {
            value["slept_at"] = json!(self.slept_at);
        }
        value
    }

    fn ack(&self) -> Value {
        json!({ "id": self.id, "status": self.status })
    }
}

#[derive(Debug, Clone)]
pub struct Template {
    pub name: String,
    pub image_ref: String,
    pub revision: u64,
    pub created: u64,
    pub updated: u64,
}

impl Template {
    fn json(&self) -> Value {
        json!({
            "name": self.name,
            "scope": "workspace",
            "revision": self.revision,
            "image_ref": self.image_ref,
            "image_digest": FIXTURE_DIGEST,
            "agents": [],
            "description": null,
            "default_port": null,
            "entrypoint": null,
            "created": self.created,
            "updated": self.updated,
        })
    }
}

/// A checked exec: the command to run, as whom.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecStart {
    pub id: String,
    pub command: String,
    /// `None` runs as the image's default user.
    pub user: Option<String>,
    pub background: bool,
}

/// The state of one fake Agent37 workspace.
#[derive(Debug, Clone)]
pub struct Agent37Api {
    pub key: String,
    pub instances: Vec<Instance>,
    pub templates: BTreeMap<String, Template>,
    /// Image references a template import can pull.
    pub pullable: Vec<String>,
    /// While set, creates of this `(type, region)` answer `503 no_capacity`.
    pub no_capacity: Vec<(String, String)>,
    /// While false, creates answer `402 insufficient_balance`.
    pub funded: bool,
    /// The workspace's awake memory limit, in GB (32 on the free credit).
    pub memory_limit: u32,
    pub events: Vec<Event>,
    created: usize,
    clock: u64,
}

impl Default for Agent37Api {
    fn default() -> Self {
        Self::new(FIXTURE_KEY)
    }
}

/// The shapes Agent37 offers per type: `(cpu, memory, largest disk)`.
fn shapes(instance_type: &str) -> &'static [(u32, u32, u32)] {
    if instance_type == "default" {
        &[(2, 4, 12), (4, 8, 20), (4, 16, 40), (4, 32, 80)]
    } else {
        &[
            (2, 4, 100),
            (4, 8, 100),
            (4, 16, 100),
            (4, 32, 100),
            (8, 64, 100),
        ]
    }
}

fn valid_template_name(name: &str) -> bool {
    let b = name.as_bytes();
    (2..=63).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

fn invalid(message: &str) -> Reply {
    Reply::error(400, "invalid_request", message)
}

fn not_found(what: &str) -> Reply {
    Reply::error(404, "not_found", &format!("{what} not found."))
}

impl Agent37Api {
    #[must_use]
    pub fn new(key: &str) -> Self {
        Self {
            key: key.to_owned(),
            instances: Vec::new(),
            templates: BTreeMap::new(),
            pullable: Vec::new(),
            no_capacity: Vec::new(),
            funded: true,
            memory_limit: 32,
            events: Vec::new(),
            created: 0,
            clock: FIXTURE_EPOCH,
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// Whether a template import can pull `image`: any listed one, and
    /// every published Intent node tag (`<repo>:<digits and dots>`).
    fn can_pull(&self, image: &str) -> bool {
        self.pullable.iter().any(|p| p == image)
            || image
                .strip_prefix(super::IMAGE_REPOSITORY)
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
    pub fn instance(&self, id: &str) -> Option<&Instance> {
        self.instances.iter().find(|i| i.id == id)
    }

    pub fn instance_mut(&mut self, id: &str) -> Option<&mut Instance> {
        self.instances.iter_mut().find(|i| i.id == id)
    }

    fn awake_memory(&self) -> u32 {
        self.instances
            .iter()
            .filter(|i| matches!(i.status.as_str(), "running" | "waking" | "starting"))
            .map(|i| i.memory)
            .sum()
    }

    /// Answer a REST call to `https://api.agent37.com/v1{path}`. `bearer`
    /// is the `Authorization: Bearer` value, if any.
    pub fn handle(
        &mut self,
        method: &str,
        path: &str,
        bearer: Option<&str>,
        body: Option<&Value>,
    ) -> Reply {
        if bearer != Some(self.key.as_str()) {
            return Reply::error(
                401,
                "invalid_api_key",
                "Missing, malformed, or revoked API key.",
            );
        }
        let path = path.split('?').next().unwrap_or_default();
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let empty = Value::Object(Map::new());
        let body = body.unwrap_or(&empty);
        match (method, parts.as_slice()) {
            ("GET", ["instances"]) => {
                let data: Vec<Value> = self.instances.iter().rev().map(Instance::json).collect();
                Reply::ok(json!({ "data": data }))
            }
            ("POST", ["instances"]) => self.create(body),
            ("GET", ["instances", id]) => self
                .instance(id)
                .map_or_else(|| not_found("Instance"), |i| Reply::ok(i.json())),
            ("DELETE", ["instances", id]) => self.delete(id),
            ("POST", ["instances", id, "sleep"]) => self.sleep(id),
            ("POST", ["instances", id, "start"]) => self.start(id),
            ("GET", ["templates"]) => {
                let mut data = vec![json!({
                    "name": "agent37-base", "scope": "system", "revision": null, "agents": [],
                    "image_ref": "ghcr.io/agent37-platform/base:2026.10.24a",
                    "description": "A plain Linux sandbox.", "default_port": null,
                    "created": null, "updated": null,
                })];
                data.extend(self.templates.values().map(Template::json));
                Reply::ok(json!({ "data": data }))
            }
            ("POST", ["templates"]) => self.create_template(body),
            ("GET", ["templates", name]) => self
                .templates
                .get(*name)
                .map_or_else(|| not_found("Template"), |t| Reply::ok(t.json())),
            ("PATCH", ["templates", name]) => self.update_template(name, body),
            ("DELETE", ["templates", name]) => match self.templates.remove(*name) {
                Some(_) => Reply::ok(json!({ "name": name, "deleted": true })),
                None => not_found("Template"),
            },
            _ => Reply::error(404, "not_found", "Unknown path."),
        }
    }

    fn create(&mut self, body: &Value) -> Reply {
        let text = |key: &str, default: &str| {
            body.get(key)
                .and_then(Value::as_str)
                .unwrap_or(default)
                .to_owned()
        };
        let template_name = text("template", "agent37-base");
        let Some(template) = self.templates.get(&template_name).cloned() else {
            return invalid(&format!("Unknown template \"{template_name}\"."));
        };
        let instance_type = text("type", "default");
        if !["default", "vm", "performance"].contains(&instance_type.as_str()) {
            return invalid("type must be one of default, vm, performance.");
        }
        let region = text("region", "us");
        if !super::REGIONS.contains(&region.as_str()) {
            return invalid("region must be one of us, eu, ap-south.");
        }
        if instance_type == "default" && region != "us" {
            return invalid(&format!(
                "Region \"{region}\" runs VM and Performance instances only: send \"type\": \"vm\" or \"type\": \"performance\"."
            ));
        }
        let resources = body.get("resources").cloned().unwrap_or(json!({}));
        let want = |key: &str| resources.get(key).and_then(Value::as_u64);
        let memory = u32::try_from(want("memory").unwrap_or(4)).unwrap_or(0);
        let Some(&(cpu, memory, max_disk)) = shapes(&instance_type).iter().find(|s| s.1 == memory)
        else {
            return invalid("Unsupported resource shape.");
        };
        let disk = u32::try_from(want("disk").unwrap_or(4)).unwrap_or(0);
        if !(2..=max_disk).contains(&disk) {
            return invalid(&format!("disk must be 2-{max_disk} GB for this shape."));
        }
        let idle = body
            .get("idle_timeout_seconds")
            .and_then(Value::as_u64)
            .unwrap_or(900);
        if !(300..=86_400).contains(&idle) {
            return invalid("idle_timeout_seconds must be 300-86400.");
        }
        if self
            .no_capacity
            .iter()
            .any(|(t, r)| *t == instance_type && *r == region)
        {
            return Reply::error(
                503,
                "no_capacity",
                &format!("No host in region {region} has capacity for this shape right now."),
            );
        }
        if !self.funded {
            return Reply::error(
                402,
                "insufficient_balance",
                "Creating this instance requires at least one day of balance.",
            );
        }
        if self.awake_memory() + memory > self.memory_limit {
            return Reply::error(
                409,
                "instance_limit_reached",
                &format!("The workspace's memory limit is {} GB.", self.memory_limit),
            );
        }
        self.created += 1;
        let created = self.tick();
        let instance = Instance {
            id: fixture_instance_id(self.created),
            status: "running".into(),
            template: template.name.clone(),
            template_revision: template.revision,
            image_ref: template.image_ref.clone(),
            instance_type,
            region,
            cpu,
            memory,
            disk,
            name: body.get("name").cloned().unwrap_or(Value::Null),
            metadata: body.get("metadata").cloned().unwrap_or(Value::Null),
            auto_sleep: body
                .get("auto_sleep")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            idle_timeout_seconds: idle,
            created,
            slept_at: None,
            wake_fresh: false,
        };
        let reply = Reply {
            status: 201,
            body: instance.json(),
        };
        self.events.push(Event::Created(instance.id.clone()));
        self.instances.push(instance);
        reply
    }

    fn delete(&mut self, id: &str) -> Reply {
        let Some(at) = self.instances.iter().position(|i| i.id == id) else {
            return not_found("Instance");
        };
        self.instances.remove(at);
        self.events.push(Event::Deleted(id.to_owned()));
        Reply::ok(json!({ "id": id, "deleted": true }))
    }

    fn sleep(&mut self, id: &str) -> Reply {
        let now = self.tick();
        let Some(instance) = self.instance_mut(id) else {
            return not_found("Instance");
        };
        match instance.status.as_str() {
            "sleeping" => Reply::ok(instance.ack()),
            "running" => {
                instance.status = "sleeping".into();
                instance.slept_at = Some(now);
                let reply = Reply::ok(instance.ack());
                self.events.push(Event::Slept(id.to_owned()));
                reply
            }
            other => invalid(&format!(
                "Cannot sleep an instance that is {other}; it must be running."
            )),
        }
    }

    /// Wake a sleeping or stopped instance, recording how it woke.
    fn wake(&mut self, id: &str) {
        let Some(instance) = self.instance_mut(id) else {
            return;
        };
        let fresh = instance.wake_fresh || instance.status == "stopped";
        instance.wake_fresh = false;
        instance.status = "running".into();
        instance.slept_at = None;
        self.events.push(Event::Woke {
            id: id.to_owned(),
            fresh,
        });
    }

    fn start(&mut self, id: &str) -> Reply {
        let Some(instance) = self.instance(id) else {
            return not_found("Instance");
        };
        match instance.status.as_str() {
            "running" => Reply::ok(instance.ack()),
            "sleeping" | "stopped" => {
                self.wake(id);
                Reply::ok(self.instance(id).map(Instance::ack).unwrap_or_default())
            }
            other => invalid(&format!("Cannot start an instance that is {other}.")),
        }
    }

    fn create_template(&mut self, body: &Value) -> Reply {
        let name = body.get("name").and_then(Value::as_str).unwrap_or_default();
        if !valid_template_name(name) || name.starts_with("agent37-") {
            return invalid("name must match ^[a-z][a-z0-9-]{1,62}$ and not start with agent37-.");
        }
        let Some(image) = body.get("image_ref").and_then(Value::as_str) else {
            return invalid("image_ref is required.");
        };
        if self.templates.contains_key(name) {
            return Reply::error(
                409,
                "template_conflict",
                &format!("A template named \"{name}\" already exists."),
            );
        }
        if !self.can_pull(image) {
            return invalid(&format!("Could not pull \"{image}\"."));
        }
        let now = self.tick();
        let template = Template {
            name: name.to_owned(),
            image_ref: image.to_owned(),
            revision: 1,
            created: now,
            updated: now,
        };
        let reply = Reply {
            status: 201,
            body: template.json(),
        };
        self.templates.insert(name.to_owned(), template);
        reply
    }

    fn update_template(&mut self, name: &str, body: &Value) -> Reply {
        let image = body
            .get("image_ref")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if let Some(image) = &image {
            if !self.can_pull(image) {
                return invalid(&format!("Could not pull \"{image}\"."));
            }
        }
        let now = self.tick();
        let Some(template) = self.templates.get_mut(name) else {
            return not_found("Template");
        };
        if let Some(image) = image {
            if image != template.image_ref {
                template.image_ref = image;
                template.revision += 1;
            }
        }
        template.updated = now;
        Reply::ok(template.json())
    }

    /// Check an exec to `https://sandbox.agent37.app/v1/instances/{id}/exec`
    /// with `key` in `X-Agent37-Key`, waking a sleeping instance. `Ok` is
    /// the command for the caller to run; `Err` the refusal to send.
    ///
    /// # Errors
    ///
    /// The refusal, as the real sandbox host sends it.
    pub fn exec(&mut self, id: &str, key: Option<&str>, body: &Value) -> Result<ExecStart, Reply> {
        if key != Some(self.key.as_str()) {
            return Err(Reply {
                status: 401,
                body: json!({ "error": "invalid_api_key", "message": "Invalid API key." }),
            });
        }
        let Some(command) = body.get("command").and_then(Value::as_str) else {
            return Err(invalid("command is required."));
        };
        let user = match body.get("user") {
            None | Some(Value::Null) => None,
            Some(Value::String(user)) if user == "root" => Some(user.clone()),
            Some(_) => return Err(invalid("user must be \"root\" or omitted.")),
        };
        let Some(status) = self.instance(id).map(|i| i.status.clone()) else {
            return Err(Reply::flat(404, "not_found"));
        };
        match status.as_str() {
            "running" => {}
            "sleeping" => self.wake(id),
            other => {
                return Err(invalid(&format!(
                    "Cannot run a command on an instance that is {other}."
                )))
            }
        }
        Ok(ExecStart {
            id: id.to_owned(),
            command: command.to_owned(),
            user,
            background: body
                .get("background")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }

    /// Take the events since the last call.
    pub fn drain_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }
}

/// The reply to a finished exec.
#[must_use]
pub fn exec_reply(exit_code: i32, stdout: &str, stderr: &str) -> Reply {
    Reply::ok(json!({
        "exit_code": exit_code,
        "stdout": stdout,
        "stderr": stderr,
        "truncated": false,
    }))
}
