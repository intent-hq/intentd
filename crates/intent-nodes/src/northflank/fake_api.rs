//! The one model of the Northflank API that every fake in intentd shares:
//! the unit tests' scripted server builds its bodies and command frames
//! here, and the e2e suite's stateful fake answers its REST calls with
//! [`NorthflankApi`] and its command streams with [`exec_frames`].
//!
//! `contract_tests` replays the recording of the real API
//! (`fixtures/northflank.json`, made by
//! `scripts/capture-northflank-fixtures.sh`) against this model, so a
//! behaviour the fakes get wrong fails CI instead of a live run.
//!
//! Northflank finishes a pause and a delete in the background. The model
//! answers as the recording saw them just after the call: a paused
//! service still lists its container as running, and a deleted service's
//! volume can still be deleted. Everything else is the end state.

use std::collections::BTreeMap;

use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

/// Placeholders the recording uses for its real ids, names and times.
pub const FIXTURE_PROJECT: &str = "project-1";
pub const FIXTURE_TEAM: &str = "team-1";
pub const FIXTURE_SERVICE: &str = "intent-000000000001";
pub const FIXTURE_BACKEND: &str = "capture-000000000001";
pub const FIXTURE_BACKEND_TAG: &str = "intent-backend-000000000001";
pub const FIXTURE_TIME: &str = "2026-01-01T00:00:00.000Z";

/// The smallest volume Northflank's default `nvme` storage class makes.
pub const MIN_VOLUME_MB: u64 = 6144;

/// The name Northflank gives a deployment's container.
pub const CONTAINER_NAME: &str = "main-service";

/// An answer: HTTP status and JSON body.
#[derive(Debug, Clone, PartialEq)]
pub struct Reply {
    pub status: u16,
    pub body: Value,
}

impl Reply {
    #[must_use]
    pub fn data(data: Value) -> Self {
        Self {
            status: 200,
            body: json!({ "data": data }),
        }
    }

    /// Northflank's error envelope.
    #[must_use]
    pub fn error(status: u16, message: &str) -> Self {
        Self {
            status,
            body: json!({ "error": { "status": status, "message": message } }),
        }
    }
}

/// One page of `items`, `data` holding them under `key` or being the list.
/// `query` may set `per_page` and the `cursor` an earlier page returned.
#[must_use]
pub fn page(key: Option<&str>, items: Vec<Value>, query: &str) -> Reply {
    let param = |name: &str| {
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
    };
    let per_page = param("per_page")
        .and_then(|n| n.parse::<usize>().ok())
        .unwrap_or(50)
        .max(1);
    let start = param("cursor")
        .and_then(|c| c.strip_prefix("cursor-")?.parse::<usize>().ok())
        .unwrap_or(0)
        .min(items.len());
    let end = (start + per_page).min(items.len());
    let count = items.len();
    let shown: Vec<Value> = items[start..end].to_vec();
    let data = match key {
        Some(key) => json!({ key: shown }),
        None => Value::Array(shown),
    };
    let mut pagination = json!({ "hasNextPage": end < count, "count": count });
    if end < count {
        pagination["cursor"] = json!(format!("cursor-{end}"));
    }
    Reply {
        status: 200,
        body: json!({ "data": data, "pagination": pagination }),
    }
}

/// A compute plan.
#[derive(Debug, Clone)]
pub struct Plan {
    pub id: &'static str,
    pub cpu: f64,
    pub ram_mib: u32,
}

/// Northflank's plans that a machine size maps to, in its order.
pub const PLANS: [Plan; 4] = [
    Plan {
        id: "nf-compute-200",
        cpu: 2.0,
        ram_mib: 4096,
    },
    Plan {
        id: "nf-compute-400",
        cpu: 4.0,
        ram_mib: 8192,
    },
    Plan {
        id: "nf-compute-50",
        cpu: 0.5,
        ram_mib: 1024,
    },
    Plan {
        id: "nf-compute-800-16",
        cpu: 8.0,
        ram_mib: 16384,
    },
];

#[derive(Debug, Clone)]
pub struct Service {
    /// The create request, with any patch merged in.
    pub request: Value,
    pub paused: bool,
    pub created_at: String,
    pub volumes: Vec<String>,
}

impl Service {
    /// The team tags the service was created with.
    #[must_use]
    pub fn tags(&self) -> Vec<String> {
        strings(&self.request["tags"])
    }

    #[must_use]
    pub fn image(&self) -> &str {
        self.request
            .pointer("/deployment/external/imagePath")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct Volume {
    pub request: Value,
    pub created_at: String,
    /// Its service was deleted with its child objects: Northflank deletes
    /// it in the background, and an explicit delete still succeeds.
    pub deleting: bool,
}

impl Volume {
    #[must_use]
    pub fn tags(&self) -> Vec<String> {
        strings(&self.request["tags"])
    }
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect()
}

/// One Northflank project and its team's tags, as the REST API keeps and
/// returns them.
#[derive(Debug, Clone)]
pub struct NorthflankApi {
    pub project: String,
    pub region: String,
    /// Team tag names; a tag's id is its name.
    pub tags: Vec<String>,
    pub services: BTreeMap<String, Service>,
    pub volumes: BTreeMap<String, Volume>,
    /// `createdAt` of what is made from now on.
    pub now: String,
}

impl NorthflankApi {
    #[must_use]
    pub fn new(project: &str, region: &str) -> Self {
        Self {
            project: project.to_owned(),
            region: region.to_owned(),
            tags: Vec::new(),
            services: BTreeMap::new(),
            volumes: BTreeMap::new(),
            now: FIXTURE_TIME.to_owned(),
        }
    }

    /// Answer one REST call. `target` is the path under `/v1`, with its
    /// query; `body` the request's JSON, if any.
    pub fn handle(&mut self, method: &str, target: &str, body: Option<&Value>) -> Reply {
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let empty = Value::Null;
        let body = body.unwrap_or(&empty);
        match (method, parts.as_slice()) {
            ("GET", ["plans"]) => {
                let plans: Vec<Value> = PLANS.iter().map(plan_body).collect();
                Reply::data(json!({ "plans": plans }))
            }
            ("GET", ["tags"]) => {
                let tags = self.tags.iter().map(|name| self.tag_body(name)).collect();
                page(Some("tags"), tags, query)
            }
            ("POST", ["tags"]) => self.create_tag(body),
            ("DELETE", ["tags", id]) => {
                let before = self.tags.len();
                self.tags.retain(|name| name != id);
                if self.tags.len() == before {
                    Reply::error(404, "Tag not found")
                } else {
                    Reply::data(json!({}))
                }
            }
            (_, ["projects", project, ..]) if *project != self.project => {
                Reply::error(404, "Project not found")
            }
            ("GET", ["projects", _]) => Reply::data(json!({
                "id": self.project,
                "name": self.project,
                "description": "",
                "deployment": { "region": self.region },
                "createdAt": self.now,
            })),
            ("GET", ["projects", _, "volumes"]) => {
                let items = self
                    .volumes
                    .iter()
                    .filter(|(_, v)| !v.deleting)
                    .map(|(id, v)| self.volume_body(id, v))
                    .collect();
                page(None, items, query)
            }
            ("POST", ["projects", _, "volumes"]) => self.create_volume(body),
            ("DELETE", ["projects", _, "volumes", id]) => self.delete_volume(id),
            ("GET", ["projects", _, "services"]) => {
                let items = self
                    .services
                    .iter()
                    .map(|(id, s)| self.listed_service_body(id, s))
                    .collect();
                page(Some("services"), items, query)
            }
            ("POST", ["projects", _, "services", "deployment"]) => self.create_service(body),
            ("PATCH", ["projects", _, "services", "deployment", id]) => {
                self.patch_service(id, body)
            }
            ("GET", ["projects", _, "services", id]) => match self.services.get(*id) {
                Some(service) => Reply::data(self.service_body(id, service)),
                None => not_found_service(),
            },
            ("GET", ["projects", _, "services", id, "containers"]) => {
                match self.services.get(*id) {
                    Some(_) => {
                        let containers = vec![container_body(id)];
                        let mut reply = page(Some("containers"), containers, query);
                        reply.body["pagination"]["hasNextPage"] = json!(false);
                        reply
                    }
                    None => not_found_service(),
                }
            }
            ("POST", ["projects", _, "services", id, "pause"]) => self.set_paused(id, true),
            ("POST", ["projects", _, "services", id, "resume"]) => self.set_paused(id, false),
            ("DELETE", ["projects", _, "services", id]) => {
                let children = query
                    .split('&')
                    .any(|pair| pair == "delete_child_objects=true");
                self.delete_service(id, children)
            }
            _ => Reply::error(404, "Not found"),
        }
    }

    /// Add a service with its volume, as an earlier run left it.
    pub fn seed(&mut self, id: &str, request: Value, paused: bool, created_at: &str) {
        self.volumes.insert(
            id.to_owned(),
            Volume {
                request: json!({
                    "name": id,
                    "tags": request["tags"],
                    "spec": { "accessMode": "ReadWriteOnce", "storageSize": MIN_VOLUME_MB },
                }),
                created_at: created_at.to_owned(),
                deleting: false,
            },
        );
        self.services.insert(
            id.to_owned(),
            Service {
                request,
                paused,
                created_at: created_at.to_owned(),
                volumes: vec![id.to_owned()],
            },
        );
    }

    fn tag_body(&self, name: &str) -> Value {
        json!({
            "id": name,
            "name": name,
            "description": "",
            "useAsInfrastructureLabel": false,
            "color": "#57637A",
            "createdAt": self.now,
        })
    }

    fn create_tag(&mut self, body: &Value) -> Reply {
        let Some(name) = body["name"].as_str() else {
            return Reply::error(400, "name is required");
        };
        if self.tags.iter().any(|n| n == name) {
            return Reply::error(409, "A tag with this name already exists");
        }
        self.tags.push(name.to_owned());
        Reply::data(self.tag_body(name))
    }

    fn create_volume(&mut self, body: &Value) -> Reply {
        let Some(name) = body["name"].as_str() else {
            return Reply::error(400, "name is required");
        };
        let size = body
            .pointer("/spec/storageSize")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if size < MIN_VOLUME_MB {
            return Reply::error(
                409,
                &format!(
                    "Storage class nvme requires a minimum storage size of {MIN_VOLUME_MB}MB, requested size {size}MB"
                ),
            );
        }
        let mut spec = body["spec"].clone();
        spec["storageClassName"] = json!("nvme");
        let mounts: Vec<Value> = body["mounts"]
            .as_array()
            .into_iter()
            .flatten()
            .map(
                |m| json!({ "volumeMountPath": "", "containerMountPath": m["containerMountPath"] }),
            )
            .collect();
        let volume = Volume {
            request: body.clone(),
            created_at: self.now.clone(),
            deleting: false,
        };
        let reply = Reply::data(json!({
            "id": name,
            "status": "PENDING",
            "name": name,
            "mounts": mounts,
            "tags": volume.tags(),
            "spec": spec,
            "attachedObjects": [],
        }));
        self.volumes.insert(name.to_owned(), volume);
        reply
    }

    fn create_service(&mut self, body: &Value) -> Reply {
        let Some(name) = body["name"].as_str() else {
            return Reply::error(400, "name is required");
        };
        let labels = body
            .pointer("/deployment/metadata/labels")
            .and_then(Value::as_object)
            .is_some_and(|l| !l.is_empty());
        if labels {
            return Reply::error(
                400,
                "Custom labels or annotations can only be set on BYOC projects",
            );
        }
        let volumes = strings(
            body.pointer("/createOptions/volumesToAttach")
                .unwrap_or(&Value::Null),
        );
        let service = Service {
            request: body.clone(),
            paused: false,
            created_at: self.now.clone(),
            volumes,
        };
        let mut created = self.service_body(name, &service);
        // The create answer has no status or time yet.
        created["status"] = json!({});
        if let Some(created) = created.as_object_mut() {
            created.remove("createdAt");
            created.remove("projectId");
        }
        self.services.insert(name.to_owned(), service);
        Reply::data(created)
    }

    fn patch_service(&mut self, id: &str, body: &Value) -> Reply {
        let Some(service) = self.services.get_mut(id) else {
            return not_found_service();
        };
        merge(&mut service.request, body);
        let service = service.clone();
        let mut patched = self.service_body(id, &service);
        patched["status"] = json!({});
        if let Some(patched) = patched.as_object_mut() {
            patched.remove("projectId");
        }
        Reply::data(patched)
    }

    fn set_paused(&mut self, id: &str, paused: bool) -> Reply {
        match self.services.get_mut(id) {
            Some(service) => {
                service.paused = paused;
                Reply::data(json!({}))
            }
            None => not_found_service(),
        }
    }

    fn delete_service(&mut self, id: &str, children: bool) -> Reply {
        let Some(service) = self.services.remove(id) else {
            return not_found_service();
        };
        if children {
            for volume in &service.volumes {
                if let Some(volume) = self.volumes.get_mut(volume) {
                    volume.deleting = true;
                }
            }
        }
        Reply::data(json!({}))
    }

    fn delete_volume(&mut self, id: &str) -> Reply {
        if !self.volumes.contains_key(id) {
            return Reply::error(404, "Volume not found");
        }
        if self.attached_to(id).is_some() {
            return Reply::error(409, "Volume must be detached before it can be deleted");
        }
        self.volumes.remove(id);
        Reply::data(json!({}))
    }

    fn attached_to(&self, volume: &str) -> Option<&str> {
        self.services
            .iter()
            .find(|(_, s)| s.volumes.iter().any(|v| v == volume))
            .map(|(id, _)| id.as_str())
    }

    fn volume_body(&self, id: &str, volume: &Volume) -> Value {
        let attached: Vec<Value> = self
            .attached_to(id)
            .map(|owner| json!({ "id": owner, "type": "service" }))
            .into_iter()
            .collect();
        let mut spec = volume.request["spec"].clone();
        spec["storageClassName"] = json!("nvme");
        json!({
            "id": id,
            "name": volume.request["name"],
            "tags": volume.tags(),
            "projectId": self.project,
            "attachedObjects": attached,
            "spec": spec,
            "status": "BOUND",
            "createdAt": volume.created_at,
        })
    }

    /// `GET /projects/{p}/services/{id}`: the runtime environment comes
    /// back, every custom annotation and label is dropped, and a pause
    /// shows as no instances (there is no `servicePaused`).
    #[must_use]
    pub fn service_body(&self, id: &str, service: &Service) -> Value {
        let request = &service.request;
        json!({
            "id": id,
            "appId": format!("/{FIXTURE_TEAM}/{}/{id}", self.project),
            "serviceType": "deployment",
            "name": request["name"],
            "description": request["description"],
            "projectId": self.project,
            "tags": service.tags(),
            "billing": request["billing"],
            "deployment": {
                "type": "deployment",
                "instances": u8::from(!service.paused),
                "external": { "imagePath": service.image() },
                "metadata": { "labels": {}, "annotations": {} },
                "imageUrl": service.image(),
            },
            "runtimeEnvironment": request.get("runtimeEnvironment").cloned().unwrap_or_else(|| json!({})),
            "status": { "deployment": deployment_status(service.paused) },
            "createdAt": service.created_at,
        })
    }

    /// One item of `GET /projects/{p}/services`.
    fn listed_service_body(&self, id: &str, service: &Service) -> Value {
        json!({
            "id": id,
            "name": service.request["name"],
            "serviceType": "deployment",
            "description": service.request["description"],
            "tags": service.tags(),
            "status": { "deployment": deployment_status(service.paused) },
            "projectId": self.project,
        })
    }
}

fn deployment_status(paused: bool) -> Value {
    if paused {
        json!({ "status": "PENDING", "reason": "SCALING" })
    } else {
        json!({ "status": "COMPLETED", "reason": "DEPLOYING" })
    }
}

fn plan_body(plan: &Plan) -> Value {
    json!({
        "id": plan.id,
        "name": plan.id,
        "type": ["deployment"],
        "cpuResource": plan.cpu,
        "ramResource": plan.ram_mib,
    })
}

fn not_found_service() -> Reply {
    Reply::error(404, "Service not found")
}

/// A service's one container, running (still, just after a pause).
#[must_use]
pub fn container_body(service: &str) -> Value {
    json!({
        "name": format!("{service}-0"),
        "createdAt": 1_700_000_000,
        "status": "TASK_RUNNING",
        "updatedAt": 1_700_000_000,
    })
}

fn merge(into: &mut Value, patch: &Value) {
    match (into, patch) {
        (Value::Object(into), Value::Object(patch)) => {
            for (key, value) in patch {
                merge(into.entry(key.clone()).or_insert(Value::Null), value);
            }
        }
        (into, patch) => *into = patch.clone(),
    }
}

/// Whether a command's `init` context asks for a terminal: Northflank
/// gives one unless it sets `"tty": false`.
#[must_use]
pub fn wants_tty(context: &Value) -> bool {
    context.get("tty") != Some(&Value::Bool(false))
}

/// A frame as Northflank sends it: JSON in a binary WebSocket message.
#[must_use]
pub fn message(frame: &Value) -> Message {
    Message::binary(frame.to_string().into_bytes())
}

/// The frame of `text` a command wrote to stdout (or stderr), as
/// Northflank streams it: in a terminal stderr merges into `stdOut` and
/// every `\n` becomes `\r\n`.
#[must_use]
pub fn output_frame(tty: bool, stderr: bool, text: &str) -> Value {
    if tty {
        json!({ "type": "stdOut", "data": text.replace('\n', "\r\n") })
    } else if stderr {
        json!({ "type": "stdErr", "data": text })
    } else {
        json!({ "type": "stdOut", "data": text })
    }
}

/// The answer to an accepted `init`.
#[must_use]
pub fn init_frame(service: &str) -> Value {
    json!({
        "type": "init",
        "data": {
            "auth": "successful",
            "context": { "instanceName": format!("{service}-0"), "containerName": CONTAINER_NAME },
        },
    })
}

/// Every frame Northflank sends for one command after the client's
/// `init`: the auth answer, an empty `stdOut`, the output, then the exit.
#[must_use]
pub fn exec_frames(
    service: &str,
    context: &Value,
    stdout: &str,
    stderr: &str,
    exit_code: i32,
) -> Vec<Value> {
    let tty = wants_tty(context);
    let mut frames = vec![init_frame(service), output_frame(tty, false, "")];
    if tty {
        let merged = format!("{stdout}{stderr}");
        if !merged.is_empty() {
            frames.push(output_frame(true, false, &merged));
        }
    } else {
        if !stdout.is_empty() {
            frames.push(output_frame(false, false, stdout));
        }
        if !stderr.is_empty() {
            frames.push(output_frame(false, true, stderr));
        }
    }
    frames.push(completion_frame(exit_code));
    frames
}

/// The exit of a command.
#[must_use]
pub fn completion_frame(exit_code: i32) -> Value {
    if exit_code == 0 {
        json!({ "type": "completion", "data": { "exitCode": 0, "status": "Success" } })
    } else {
        json!({ "type": "completion", "data": {
            "exitCode": exit_code,
            "status": "Failure",
            "message": format!(
                "command terminated with non-zero exit code: command terminated with exit code {exit_code}"
            ),
        } })
    }
}
