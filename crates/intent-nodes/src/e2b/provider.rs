//! [`E2bClient`] as a [`CloudProvider`]: a machine is a sandbox, an image a
//! template alias, tags are sandbox metadata and envd runs the commands.

use intent_core::machines::CLOUD_PROVIDER_E2B;

use super::{
    E2bClient, E2bError, ListSandboxes, ListedSandbox, ProcessOutput, ProcessSpec, Sandbox,
    SandboxState,
};
use crate::cloud::{
    CloudError, CloudErrorKind, CloudFuture, CloudInstance, CloudProvider, Connected,
    CreateInstance, InstanceState, ListInstances, ListedInstance,
};

impl From<E2bError> for CloudError {
    fn from(error: E2bError) -> Self {
        let retryable = error.is_retryable();
        let message = error.to_string();
        let (kind, status, retry_after) = match &error {
            E2bError::InvalidArgument(_) => (CloudErrorKind::InvalidArgument, None, None),
            E2bError::NotConfigured => (CloudErrorKind::NotConfigured, None, None),
            E2bError::RateLimited { retry_after, .. } => (
                CloudErrorKind::Quota(crate::cloud::QuotaLimit::Rate),
                None,
                *retry_after,
            ),
            E2bError::ConcurrencyLimit { .. } => (
                CloudErrorKind::Quota(crate::cloud::QuotaLimit::Concurrency),
                None,
                None,
            ),
            E2bError::PlanLimit { status, .. } => (
                CloudErrorKind::Quota(crate::cloud::QuotaLimit::Plan),
                Some(*status),
                None,
            ),
            E2bError::NotFound(_) => (CloudErrorKind::NotFound, None, None),
            E2bError::Conflict(_) => (CloudErrorKind::Conflict, None, None),
            E2bError::Unauthorized { status } => {
                (CloudErrorKind::Unauthorized, Some(*status), None)
            }
            E2bError::Api { status, .. } => (CloudErrorKind::Api, Some(*status), None),
            _ => (CloudErrorKind::Other, None, None),
        };
        let mut cloud = Self::new(kind, message)
            .retryable(retryable)
            .with_retry_after(retry_after);
        if let Some(status) = status {
            cloud = cloud.with_status(status);
        }
        cloud
    }
}

fn instance(sandbox: Sandbox) -> CloudInstance {
    CloudInstance {
        id: sandbox.sandbox_id,
        image: sandbox.template_id,
        access_token: sandbox.envd_access_token,
    }
}

/// The parts of a sandbox envd reads: its id and access token. The
/// other fields are not kept on a [`CloudInstance`].
fn sandbox(instance: &CloudInstance) -> Sandbox {
    Sandbox {
        sandbox_id: instance.id.clone(),
        template_id: instance.image.clone(),
        alias: None,
        envd_version: String::new(),
        envd_access_token: instance.access_token.clone(),
        traffic_access_token: None,
        domain: None,
    }
}

fn state(state: SandboxState) -> InstanceState {
    match state {
        SandboxState::Running => InstanceState::Running,
        SandboxState::Paused => InstanceState::Paused,
        SandboxState::Other => InstanceState::Other,
    }
}

fn listed(sandbox: ListedSandbox) -> ListedInstance {
    ListedInstance {
        id: sandbox.sandbox_id,
        state: state(sandbox.state),
        tags: sandbox.metadata,
        started_at: sandbox.started_at,
    }
}

impl CloudProvider for E2bClient {
    fn kind(&self) -> &'static str {
        CLOUD_PROVIDER_E2B
    }

    fn create<'a>(&'a self, request: &'a CreateInstance) -> CloudFuture<'a, CloudInstance> {
        Box::pin(async move {
            let created = E2bClient::create(
                self,
                &super::CreateSandbox {
                    template_id: request.image.clone(),
                    timeout_secs: request.timeout_secs,
                    auto_pause: request.pause_on_timeout,
                    metadata: request.tags.clone(),
                    idempotency_key: request.idempotency_key.clone(),
                },
            )
            .await?;
            Ok(instance(created))
        })
    }

    fn connect<'a>(&'a self, id: &'a str, timeout_secs: u32) -> CloudFuture<'a, Connected> {
        Box::pin(async move {
            let connected = E2bClient::connect(self, id, timeout_secs).await?;
            Ok(Connected {
                instance: instance(connected.sandbox),
                resumed: connected.resumed,
            })
        })
    }

    fn run_command<'a>(
        &'a self,
        instance: &'a CloudInstance,
        spec: &'a ProcessSpec,
    ) -> CloudFuture<'a, ProcessOutput> {
        Box::pin(async move { Ok(self.run_process(&sandbox(instance), spec).await?) })
    }

    fn pause<'a>(&'a self, id: &'a str) -> CloudFuture<'a, ()> {
        Box::pin(async move { Ok(E2bClient::pause(self, id).await?) })
    }

    fn keep_alive<'a>(&'a self, id: &'a str, timeout_secs: u32) -> CloudFuture<'a, ()> {
        Box::pin(async move { Ok(self.set_timeout(id, timeout_secs).await?) })
    }

    fn delete<'a>(&'a self, id: &'a str) -> CloudFuture<'a, bool> {
        Box::pin(async move { Ok(E2bClient::delete(self, id).await?) })
    }

    fn list<'a>(&'a self, filter: &'a ListInstances) -> CloudFuture<'a, Vec<ListedInstance>> {
        Box::pin(async move {
            let states = filter
                .states
                .iter()
                .map(|state| match state {
                    InstanceState::Running => SandboxState::Running,
                    InstanceState::Paused => SandboxState::Paused,
                    InstanceState::Other => SandboxState::Other,
                })
                .collect();
            let found = E2bClient::list(
                self,
                &ListSandboxes {
                    metadata: filter.tags.clone(),
                    states,
                },
            )
            .await?;
            Ok(found.into_iter().map(listed).collect())
        })
    }

    fn check_key(&self) -> CloudFuture<'_, ()> {
        Box::pin(async move { Ok(E2bClient::check_key(self).await?) })
    }

    fn image_ready<'a>(&'a self, image: &'a str) -> CloudFuture<'a, bool> {
        Box::pin(async move {
            let Some((name, tag)) = image.rsplit_once(':') else {
                return Err(E2bError::InvalidArgument(format!(
                    "bad template reference {image:?}: no tag"
                ))
                .into());
            };
            Ok(self.template_exists(name, tag).await?)
        })
    }
}
