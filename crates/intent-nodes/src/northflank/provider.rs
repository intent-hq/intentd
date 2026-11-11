//! [`NorthflankClient`] as a [`CloudProvider`]: a machine is a deployment
//! service with one volume, tags are two team tags plus the full tag map
//! in an annotation, and pause scales the service to zero.

use std::collections::{BTreeMap, HashSet};

use reqwest::Method;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::Instant;

use super::client::{validate_id, Call, MAX_TAGS_JSON};
use super::{
    backend_tag, NorthflankClient, CLOUD_PROVIDER_NORTHFLANK, DEFAULT_DISK_GIB, HOME, NODE_TAG,
    RUN_AS,
};
use crate::cloud::{
    CloudError, CloudErrorKind, CloudFuture, CloudInstance, CloudProvider, Connected,
    CreateInstance, InstanceState, ListInstances, ListedInstance, ProcessOutput, ProcessSpec,
};

/// The annotation that holds a machine's tags as compact JSON. A service
/// description is at most 200 characters and refuses `{` and `}`, too short
/// for the backend's tags, so the tags live on the workload instead.
pub(super) const TAGS_ANNOTATION: &str = "intent.dev/tags";
const BACKEND_ID_TAG: &str = "intentBackendId";
/// Prefix of a [`ListedInstance::id`] that names a volume without a service.
const VOLUME_PREFIX: &str = "volume:";
/// Polls with only exited containers before the wait gives up.
const CRASH_POLLS: u32 = 3;

#[derive(Deserialize)]
struct Named {
    id: String,
    #[serde(default)]
    name: String,
}

#[derive(Deserialize)]
struct ListedService {
    id: String,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListedVolume {
    id: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    attached_objects: Vec<Attached>,
    #[serde(default)]
    created_at: String,
}

#[derive(Deserialize)]
struct Attached {
    id: String,
}

#[derive(Deserialize)]
struct Containers {
    #[serde(default)]
    containers: Vec<Container>,
}

#[derive(Deserialize)]
struct Container {
    #[serde(default)]
    status: String,
}

/// The parts of `GET /projects/{p}/services/{id}` this adapter reads.
struct ServiceView {
    paused: bool,
    deployment_status: String,
    image: String,
    created_at: String,
    tags: Option<BTreeMap<String, String>>,
}

impl ServiceView {
    fn from(value: &Value) -> Self {
        let text = |ptr: &str| {
            value
                .pointer(ptr)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        let instances = value
            .pointer("/deployment/instances")
            .and_then(Value::as_u64);
        let flagged = value
            .get("servicePaused")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let tags = value
            .pointer(&format!(
                "/deployment/metadata/annotations/{}",
                TAGS_ANNOTATION.replace('~', "~0").replace('/', "~1")
            ))
            .and_then(Value::as_str)
            .and_then(|s| serde_json::from_str(s).ok());
        Self {
            paused: flagged || instances == Some(0),
            deployment_status: text("/status/deployment/status"),
            image: text("/deployment/external/imagePath"),
            created_at: text("/createdAt"),
            tags,
        }
    }
}

fn random_name() -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    format!("intent-{}", &hex[..12])
}

/// Idempotent: `chown -R` only when the home folder's owner differs, so a
/// resume of a large disk costs one `stat`.
const PREPARE_HOME: &str = r#"set -eu
u=$(id -u user)
g=$(id -g user)
if [ "$(stat -c %u:%g "$HOME")" != "$u:$g" ]; then
  chown -R "$u:$g" "$HOME"
fi
"#;

const PREPARE_HOME_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

fn invalid(message: impl Into<String>) -> CloudError {
    CloudError::new(CloudErrorKind::InvalidArgument, message)
}

impl NorthflankClient {
    async fn create_machine(&self, request: &CreateInstance) -> Result<CloudInstance, CloudError> {
        let project = self.project_id()?.to_string();
        let plan = request
            .plan
            .as_deref()
            .filter(|p| !p.is_empty())
            .ok_or_else(|| invalid("a Northflank machine needs a deployment plan"))?;
        let backend = request
            .tags
            .get(BACKEND_ID_TAG)
            .filter(|b| !b.is_empty())
            .ok_or_else(|| invalid("a Northflank machine needs the intentBackendId tag"))?;
        super::ImageRef::parse(&request.image)?;
        let disk = request.disk_gib.unwrap_or(DEFAULT_DISK_GIB);
        if disk == 0 {
            return Err(invalid("the disk must be at least 1 GiB"));
        }
        let tags_json = serde_json::to_string(&request.tags)
            .map_err(|e| invalid(format!("the tags cannot be stored: {e}")))?;
        if tags_json.len() > MAX_TAGS_JSON {
            return Err(invalid(format!(
                "the tags take {} bytes; a Northflank machine holds at most {MAX_TAGS_JSON}",
                tags_json.len()
            )));
        }
        let team_tags = [NODE_TAG.to_string(), backend_tag(backend)];
        self.ensure_tags(&team_tags).await?;

        let name = random_name();
        let volume: Named = self
            .post_data(
                Call::new("create-volume"),
                &format!("/projects/{project}/volumes"),
                &json!({
                    "name": name,
                    "tags": team_tags,
                    "mounts": [{ "containerMountPath": HOME }],
                    "spec": {
                        "accessMode": "ReadWriteOnce",
                        "storageSize": u64::from(disk) * 1024,
                    },
                }),
            )
            .await?;
        let service = self
            .post_data::<Named>(
                Call::new("create-service"),
                &format!("/projects/{project}/services/deployment"),
                &json!({
                    "name": name,
                    "description": "Intent cloud machine",
                    "tags": team_tags,
                    "billing": { "deploymentPlan": plan },
                    "deployment": {
                        "instances": 1,
                        "external": { "imagePath": request.image },
                        "metadata": { "annotations": { TAGS_ANNOTATION: tags_json } },
                    },
                    "ports": [],
                    "createOptions": { "volumesToAttach": [volume.id] },
                }),
            )
            .await;
        let service = match service {
            Ok(service) => service,
            Err(error) => {
                let _ = self.delete_volume(&volume.id).await;
                return Err(error);
            }
        };
        let _ = volume.name;
        let instance = CloudInstance {
            id: service.id,
            image: request.image.clone(),
            access_token: None,
            volume_id: Some(volume.id),
        };
        let ready = match self.wait_ready(&instance.id).await {
            Ok(_) => self.prepare_home(&instance).await,
            Err(error) => Err(error),
        };
        if let Err(error) = ready {
            let _ = self
                .delete_machine(&instance.id, instance.volume_id.as_deref())
                .await;
            return Err(error);
        }
        Ok(instance)
    }

    /// Add each team tag that does not exist yet: Northflank applies only
    /// existing tags.
    async fn ensure_tags(&self, wanted: &[String]) -> Result<(), CloudError> {
        let existing: Vec<Named> = self
            .list_all(Call::new("list-tags"), "/tags", Some("tags"))
            .await?;
        let names: HashSet<&str> = existing.iter().map(|t| t.name.as_str()).collect();
        for tag in wanted {
            if !names.contains(tag.as_str()) {
                let _: Value = self
                    .post_data(Call::new("create-tag"), "/tags", &json!({ "name": tag }))
                    .await?;
            }
        }
        Ok(())
    }

    async fn service(&self, id: &str) -> Result<Value, CloudError> {
        let path = format!("/projects/{}/services/{id}", self.project_id()?);
        self.get_data(Call::new("read-service"), &path, &[]).await
    }

    /// Wait until the deployment is `COMPLETED` with a running container.
    async fn wait_ready(&self, id: &str) -> Result<ServiceView, CloudError> {
        let deadline = Instant::now() + self.config.ready_timeout;
        let mut crashed = 0;
        loop {
            let view = ServiceView::from(&self.service(id).await?);
            match view.deployment_status.as_str() {
                "FAILED" => {
                    return Err(CloudError::new(
                        CloudErrorKind::Other,
                        format!(
                            "the Northflank deployment failed: the image {:?} could not be pulled or started",
                            view.image
                        ),
                    ))
                }
                "COMPLETED" if !view.paused => {
                    let path = format!("/projects/{}/services/{id}/containers", self.project_id()?);
                    let found: Containers = self
                        .get_data(Call::new("read-service"), &path, &[])
                        .await?;
                    if found.containers.iter().any(|c| c.status == "TASK_RUNNING") {
                        return Ok(view);
                    }
                    let exited = !found.containers.is_empty()
                        && found.containers.iter().all(|c| {
                            matches!(
                                c.status.as_str(),
                                "TASK_FAILED" | "TASK_KILLED" | "TASK_FINISHED"
                            )
                        });
                    crashed = if exited { crashed + 1 } else { 0 };
                    if crashed >= CRASH_POLLS {
                        return Err(CloudError::new(
                            CloudErrorKind::Other,
                            "the Northflank container keeps exiting before it runs",
                        ));
                    }
                }
                _ => {}
            }
            if Instant::now() + self.config.poll_interval > deadline {
                return Err(CloudError::new(
                    CloudErrorKind::Other,
                    format!(
                        "the Northflank service was not ready after {} s",
                        self.config.ready_timeout.as_secs()
                    ),
                ));
            }
            tokio::time::sleep(self.config.poll_interval).await;
        }
    }

    async fn connect_machine(&self, id: &str) -> Result<Connected, CloudError> {
        validate_id(id, "service id")?;
        let view = ServiceView::from(&self.service(id).await?);
        let resumed = view.paused;
        if resumed {
            let path = format!("/projects/{}/services/{id}/resume", self.project_id()?);
            self.request(
                Call::new("resume"),
                Method::POST,
                &path,
                &[],
                Some(&json!({ "instances": 1 })),
            )
            .await?;
        }
        let ready = self.wait_ready(id).await?;
        let instance = CloudInstance {
            id: id.to_string(),
            image: ready.image,
            access_token: None,
            volume_id: None,
        };
        self.prepare_home(&instance).await?;
        Ok(Connected { instance, resumed })
    }

    /// As root, hand the volume to [`RUN_AS`] unless it already owns it: a
    /// new volume mounts root-owned. Runs at create and on every connect.
    async fn prepare_home(&self, instance: &CloudInstance) -> Result<(), CloudError> {
        let mut setup = ProcessSpec::new("/bin/sh");
        setup.args = vec!["-c".into(), PREPARE_HOME.into()];
        setup.user = Some("root".into());
        setup.timeout = Some(PREPARE_HOME_TIMEOUT);
        let output = self.exec(instance, &setup).await?;
        if output.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(CloudError::new(
            CloudErrorKind::Other,
            format!(
                "the Northflank machine's home folder could not be handed to {RUN_AS:?} (exit {}): {}",
                output.exit.exit_code,
                stderr.trim()
            ),
        ))
    }

    async fn pause_machine(&self, id: &str) -> Result<(), CloudError> {
        validate_id(id, "service id")?;
        let path = format!("/projects/{}/services/{id}/pause", self.project_id()?);
        self.request(
            Call::new("pause"),
            Method::POST,
            &path,
            &[],
            Some(&json!({})),
        )
        .await
        .map(drop)
    }

    /// `true` when it deleted something.
    async fn delete_volume(&self, id: &str) -> Result<bool, CloudError> {
        let path = format!("/projects/{}/volumes/{id}", self.project_id()?);
        match self
            .request(Call::new("delete-volume"), Method::DELETE, &path, &[], None)
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if e.is_not_found() => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Delete the service with its child objects, then its volume (named
    /// like the service unless `volume` says otherwise) if it is still there.
    async fn delete_machine(&self, id: &str, volume: Option<&str>) -> Result<bool, CloudError> {
        let path = format!("/projects/{}/services/{id}", self.project_id()?);
        let service = match self
            .request(
                Call::new("delete-service"),
                Method::DELETE,
                &path,
                &[("delete_child_objects", "true".into())],
                None,
            )
            .await
        {
            Ok(_) => true,
            Err(e) if e.is_not_found() => false,
            Err(e) => return Err(e),
        };
        let volume = self.delete_volume(volume.unwrap_or(id)).await?;
        Ok(service || volume)
    }

    async fn delete_any(&self, id: &str) -> Result<bool, CloudError> {
        if let Some(volume) = id.strip_prefix(VOLUME_PREFIX) {
            validate_id(volume, "volume id")?;
            return self.delete_volume(volume).await;
        }
        validate_id(id, "service id")?;
        self.delete_machine(id, None).await
    }

    async fn list_machines(
        &self,
        filter: &ListInstances,
    ) -> Result<Vec<ListedInstance>, CloudError> {
        let backend = filter
            .tags
            .get(BACKEND_ID_TAG)
            .filter(|b| !b.is_empty())
            .ok_or_else(|| invalid("a Northflank list needs the intentBackendId tag"))?;
        let tag = backend_tag(backend);
        let project = self.project_id()?.to_string();
        let wants =
            |state: InstanceState| filter.states.is_empty() || filter.states.contains(&state);

        let services: Vec<ListedService> = self
            .list_all(
                Call::new("list-services"),
                &format!("/projects/{project}/services"),
                Some("services"),
            )
            .await?;
        let mut found = Vec::new();
        for listed in services.iter().filter(|s| s.tags.contains(&tag)) {
            let value = match self.service(&listed.id).await {
                Ok(value) => value,
                Err(e) if e.is_not_found() => continue,
                Err(e) => return Err(e),
            };
            let view = ServiceView::from(&value);
            let tags = view.tags.unwrap_or_default();
            if !filter.tags.iter().all(|(k, v)| tags.get(k) == Some(v)) {
                continue;
            }
            let state = if view.paused {
                InstanceState::Paused
            } else {
                InstanceState::Running
            };
            if wants(state) {
                found.push(ListedInstance {
                    id: listed.id.clone(),
                    state,
                    tags,
                    started_at: view.created_at,
                });
            }
        }

        // A volume whose service is gone is listed as a paused machine so
        // the reaper deletes it. Only the backend tag is known of it.
        let only_backend = filter.tags.len() == 1;
        if only_backend && wants(InstanceState::Paused) {
            let all: HashSet<&str> = services.iter().map(|s| s.id.as_str()).collect();
            let volumes: Vec<ListedVolume> = self
                .list_all(
                    Call::new("list-volumes"),
                    &format!("/projects/{project}/volumes"),
                    None,
                )
                .await?;
            for volume in volumes {
                let orphan = volume
                    .attached_objects
                    .iter()
                    .all(|a| !all.contains(a.id.as_str()));
                if volume.tags.contains(&tag) && orphan {
                    found.push(ListedInstance {
                        id: format!("{VOLUME_PREFIX}{}", volume.id),
                        state: InstanceState::Paused,
                        tags: BTreeMap::from([(BACKEND_ID_TAG.to_string(), backend.clone())]),
                        started_at: volume.created_at,
                    });
                }
            }
        }
        Ok(found)
    }
}

impl CloudProvider for NorthflankClient {
    fn kind(&self) -> &'static str {
        CLOUD_PROVIDER_NORTHFLANK
    }

    fn create<'a>(&'a self, request: &'a CreateInstance) -> CloudFuture<'a, CloudInstance> {
        Box::pin(self.create_machine(request))
    }

    fn connect<'a>(&'a self, id: &'a str, _timeout_secs: u32) -> CloudFuture<'a, Connected> {
        Box::pin(self.connect_machine(id))
    }

    fn run_command<'a>(
        &'a self,
        instance: &'a CloudInstance,
        spec: &'a ProcessSpec,
    ) -> CloudFuture<'a, ProcessOutput> {
        Box::pin(self.exec(instance, spec))
    }

    fn pause<'a>(&'a self, id: &'a str) -> CloudFuture<'a, ()> {
        Box::pin(self.pause_machine(id))
    }

    /// A Northflank service has no timeout: nothing to renew.
    fn keep_alive<'a>(&'a self, _id: &'a str, _timeout_secs: u32) -> CloudFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn delete<'a>(&'a self, id: &'a str) -> CloudFuture<'a, bool> {
        Box::pin(self.delete_any(id))
    }

    fn list<'a>(&'a self, filter: &'a ListInstances) -> CloudFuture<'a, Vec<ListedInstance>> {
        Box::pin(self.list_machines(filter))
    }

    fn check_key(&self) -> CloudFuture<'_, ()> {
        Box::pin(async move { self.project().await.map(drop) })
    }

    fn image_ready<'a>(&'a self, image: &'a str) -> CloudFuture<'a, bool> {
        Box::pin(self.image_exists(image))
    }

    fn has_lease(&self) -> bool {
        false
    }

    fn keeps_memory(&self) -> bool {
        false
    }
}
