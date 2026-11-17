//! [`Agent37Client`] as a [`CloudProvider`]: a machine is an instance
//! started from the template that holds the image, its tags are the
//! instance's metadata, and pause is an Agent37 sleep.

use std::collections::BTreeMap;

use reqwest::Method;
use serde_json::{json, Value};
use tokio::time::Instant;

use super::client::{validate_id, Call, InstanceBody, InstanceList, TemplateBody};
use super::{
    region_allows, template_name, Agent37Client, BACKEND_ID_TAG, BACKSTOP_IDLE_SECS,
    CLOUD_PROVIDER_AGENT37,
};
use crate::cloud::{
    CloudError, CloudErrorKind, CloudFuture, CloudInstance, CloudProvider, Connected,
    CreateInstance, InstanceState, ListInstances, ListedInstance, ProcessOutput, ProcessSpec,
};

/// The image's unprivileged account: Agent37 runs every exec as it unless
/// the call asks for root.
pub const RUN_AS: &str = "user";

fn invalid(message: impl Into<String>) -> CloudError {
    CloudError::new(CloudErrorKind::InvalidArgument, message)
}

fn random_name() -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    format!("intent-{}", &hex[..12])
}

/// The instance's metadata as tags: its string values.
fn tags_of(metadata: Option<&Value>) -> BTreeMap<String, String> {
    metadata
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
                .collect()
        })
        .unwrap_or_default()
}

fn state_of(status: &str) -> InstanceState {
    match status {
        "running" => InstanceState::Running,
        "sleeping" | "stopped" => InstanceState::Paused,
        _ => InstanceState::Other,
    }
}

/// Epoch seconds as RFC 3339; empty when unknown.
fn rfc3339(epoch: Option<u64>) -> String {
    epoch
        .and_then(|s| i64::try_from(s).ok())
        .and_then(|s| time::OffsetDateTime::from_unix_timestamp(s).ok())
        .and_then(|at| {
            at.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_default()
}

fn failed(instance: &InstanceBody) -> CloudError {
    let reason = instance
        .status_reason
        .as_ref()
        .and_then(|r| r.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("no reason given");
    CloudError::new(
        CloudErrorKind::Other,
        format!("the Agent37 instance failed: {reason}"),
    )
}

impl Agent37Client {
    /// Make sure the template that holds `image` exists with that image;
    /// return its name.
    ///
    /// # Errors
    ///
    /// [`CloudErrorKind::NotFound`] when Agent37 cannot pull the image.
    pub(super) async fn ensure_template(&self, image: &str) -> Result<String, CloudError> {
        let name = template_name(image);
        let path = format!("/templates/{name}");
        for _ in 0..3 {
            match self
                .get_json::<TemplateBody>(Call::new("read-template"), &path)
                .await
            {
                Ok(found) if found.image_ref.as_deref() == Some(image) => return Ok(found.name),
                Ok(_) => {
                    return self
                        .import(Method::PATCH, &path, &json!({ "image_ref": image }), image)
                        .await
                        .map(|()| name)
                }
                Err(e) if e.is_not_found() => {}
                Err(e) => return Err(e),
            }
            let body = json!({
                "name": name,
                "image_ref": image,
                "description": "Intent node, started by an Intent backend",
            });
            match self.import(Method::POST, "/templates", &body, image).await {
                Ok(()) => return Ok(name),
                // Another start imported it meanwhile: read it again.
                Err(e) if e.kind() == CloudErrorKind::Conflict => {}
                Err(e) => return Err(e),
            }
        }
        Err(CloudError::new(
            CloudErrorKind::Other,
            format!("the Agent37 template {name} kept changing while it was imported"),
        ))
    }

    async fn import(
        &self,
        method: Method,
        path: &str,
        body: &Value,
        image: &str,
    ) -> Result<(), CloudError> {
        let call = Call::slow(if method == Method::POST {
            "create-template"
        } else {
            "update-template"
        });
        match self
            .send_json::<TemplateBody>(call, method, path, body)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.status() == Some(400) => Err(CloudError::new(
                CloudErrorKind::NotFound,
                format!("Agent37 could not import the image {image}: {e}"),
            )
            .with_status(400)),
            Err(e) => Err(e),
        }
    }

    async fn create_machine(&self, request: &CreateInstance) -> Result<CloudInstance, CloudError> {
        let instance_type = request
            .machine_type
            .as_deref()
            .ok_or_else(|| invalid("an Agent37 machine needs an instance type"))?;
        let region = request.region.as_deref().unwrap_or("us");
        if !region_allows(instance_type, region) {
            return Err(invalid(format!(
                "Agent37 runs no {instance_type} instance in region {region}"
            )));
        }
        let (Some(cpu), Some(memory)) = (request.vcpu, request.memory_gib) else {
            return Err(invalid("an Agent37 machine needs a vCPU count and memory"));
        };
        if request
            .tags
            .get(BACKEND_ID_TAG)
            .is_none_or(String::is_empty)
        {
            return Err(invalid(format!(
                "an Agent37 machine needs the {BACKEND_ID_TAG} tag"
            )));
        }
        let template = self.ensure_template(&request.image).await?;
        let mut resources = json!({ "cpu": cpu, "memory": memory });
        if let Some(disk) = request.disk_gib {
            resources["disk"] = json!(disk);
        }
        let body = json!({
            "template": template,
            "type": instance_type,
            "region": region,
            "resources": resources,
            "name": random_name(),
            "metadata": request.tags,
            "auto_sleep": true,
            "idle_timeout_seconds": BACKSTOP_IDLE_SECS,
        });
        let created: InstanceBody = self
            .send_json(
                Call::slow("create-instance"),
                Method::POST,
                "/instances",
                &body,
            )
            .await?;
        let instance = CloudInstance {
            id: created.id.clone(),
            image: request.image.clone(),
            access_token: None,
            volume_id: None,
        };
        if created.status != "running" {
            if let Err(error) = self.wait_running(&created.id).await {
                let _ = self.delete_machine(&created.id).await;
                return Err(error);
            }
        }
        Ok(instance)
    }

    /// Wait until instance `id` runs.
    async fn wait_running(&self, id: &str) -> Result<InstanceBody, CloudError> {
        let deadline = Instant::now() + self.config.ready_timeout;
        loop {
            let instance = self.instance(id).await?;
            match instance.status.as_str() {
                "running" => return Ok(instance),
                "failed" => return Err(failed(&instance)),
                "deleting" | "deleted" => {
                    return Err(CloudError::new(
                        CloudErrorKind::NotFound,
                        format!("the Agent37 instance {id} is being deleted"),
                    ))
                }
                _ => {}
            }
            if Instant::now() + self.config.poll_interval > deadline {
                return Err(CloudError::new(
                    CloudErrorKind::Other,
                    format!(
                        "the Agent37 instance was not running after {} s",
                        self.config.ready_timeout.as_secs()
                    ),
                )
                .retryable(true));
            }
            tokio::time::sleep(self.config.poll_interval).await;
        }
    }

    async fn connect_machine(&self, id: &str) -> Result<Connected, CloudError> {
        let instance = self.instance(id).await?;
        let resumed = match instance.status.as_str() {
            "running" => false,
            "sleeping" | "stopped" => {
                let ack = self
                    .send_json::<super::client::Ack>(
                        Call::slow("start"),
                        Method::POST,
                        &format!("/instances/{id}/start"),
                        &json!({}),
                    )
                    .await?;
                if ack.status != "running" {
                    self.wait_running(id).await?;
                }
                true
            }
            "failed" => return Err(failed(&instance)),
            "deleting" | "deleted" => {
                return Err(CloudError::new(
                    CloudErrorKind::NotFound,
                    format!("the Agent37 instance {id} is being deleted"),
                ))
            }
            _ => {
                self.wait_running(id).await?;
                true
            }
        };
        Ok(Connected {
            instance: CloudInstance {
                id: id.to_owned(),
                image: instance.image_ref.unwrap_or_default(),
                access_token: None,
                volume_id: None,
            },
            resumed,
        })
    }

    async fn pause_machine(&self, id: &str) -> Result<(), CloudError> {
        validate_id(id)?;
        let slept = self
            .request(
                Call::slow("sleep"),
                Method::POST,
                &format!("/instances/{id}/sleep"),
                None,
            )
            .await;
        match slept {
            Ok(_) => Ok(()),
            // Not running: fine when it already sleeps.
            Err(e) if e.status() == Some(400) => match self.instance(id).await {
                Ok(i) if state_of(&i.status) == InstanceState::Paused => Ok(()),
                _ => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    /// One exec of `true`, which Agent37 counts as activity, so its own
    /// auto-sleep does not fire. A sleeping instance is not woken: it is
    /// reported as not found, so the backend looks it up and marks it paused.
    async fn renew(&self, id: &str) -> Result<(), CloudError> {
        let instance = self.instance(id).await?;
        if instance.status != "running" {
            return Err(CloudError::new(
                CloudErrorKind::NotFound,
                format!("the Agent37 instance {id} is {}", instance.status),
            ));
        }
        self.exec_line(id, "true", None, None).await.map(drop)
    }

    /// `true` when it deleted the instance.
    async fn delete_machine(&self, id: &str) -> Result<bool, CloudError> {
        validate_id(id)?;
        match self
            .request(
                Call::new("delete"),
                Method::DELETE,
                &format!("/instances/{id}"),
                None,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if e.is_not_found() => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn list_machines(
        &self,
        filter: &ListInstances,
    ) -> Result<Vec<ListedInstance>, CloudError> {
        let listed: InstanceList = self
            .get_json(Call::new("list-instances"), "/instances")
            .await?;
        let wants =
            |state: InstanceState| filter.states.is_empty() || filter.states.contains(&state);
        Ok(listed
            .data
            .into_iter()
            .filter_map(|instance| {
                let tags = tags_of(instance.metadata.as_ref());
                let state = state_of(&instance.status);
                (filter.tags.iter().all(|(k, v)| tags.get(k) == Some(v)) && wants(state)).then(
                    || ListedInstance {
                        id: instance.id,
                        state,
                        tags,
                        started_at: rfc3339(instance.created),
                    },
                )
            })
            .collect())
    }

    async fn image_ready_now(&self, image: &str) -> Result<bool, CloudError> {
        match self.ensure_template(image).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_not_found() => Ok(false),
            Err(e) => Err(e),
        }
    }
}

impl CloudProvider for Agent37Client {
    fn kind(&self) -> &'static str {
        CLOUD_PROVIDER_AGENT37
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
        Box::pin(self.exec_spec(&instance.id, spec))
    }

    fn pause<'a>(&'a self, id: &'a str) -> CloudFuture<'a, ()> {
        Box::pin(self.pause_machine(id))
    }

    fn keep_alive<'a>(&'a self, id: &'a str, _timeout_secs: u32) -> CloudFuture<'a, ()> {
        Box::pin(self.renew(id))
    }

    fn delete<'a>(&'a self, id: &'a str) -> CloudFuture<'a, bool> {
        Box::pin(self.delete_machine(id))
    }

    fn list<'a>(&'a self, filter: &'a ListInstances) -> CloudFuture<'a, Vec<ListedInstance>> {
        Box::pin(self.list_machines(filter))
    }

    fn check_key(&self) -> CloudFuture<'_, ()> {
        Box::pin(async move {
            self.get_json::<InstanceList>(Call::new("list-instances"), "/instances")
                .await
                .map(drop)
        })
    }

    fn image_ready<'a>(&'a self, image: &'a str) -> CloudFuture<'a, bool> {
        Box::pin(self.image_ready_now(image))
    }

    fn caps_run(&self) -> bool {
        false
    }

    fn keeps_memory(&self) -> bool {
        false
    }
}
