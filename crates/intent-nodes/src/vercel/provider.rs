//! [`VercelClient`] as a [`CloudProvider`]: a machine is a persistent
//! sandbox named after it, its tags are the sandbox's tags, pause is a
//! stop (a disk snapshot), and a resume is a new session from it.

use std::fmt::Write as _;

use reqwest::Method;
use serde_json::json;
use tokio::time::Instant;

use super::client::{
    now_ms, validate_name, validate_session, Call, ExtendReply, SandboxList, SessionReply,
};
use super::{
    VercelClient, BACKEND_ID_TAG, CLOUD_PROVIDER_VERCEL, MAX_TAGS, SNAPSHOT_EXPIRATION_MS,
};
use crate::cloud::{
    CloudError, CloudErrorKind, CloudFuture, CloudInstance, CloudProvider, Connected,
    CreateInstance, InstanceState, ListInstances, ListedInstance, ProcessOutput, ProcessSpec,
};

/// The image's unprivileged account: Vercel runs every command as the
/// image's `USER`, and the image has no `sudo`.
pub const RUN_AS: &str = "user";

/// The session timeout of a new sandbox unless the request sets one.
const DEFAULT_TIMEOUT_SECS: u32 = 30 * 60;

/// Most sandboxes one list page asks for: Vercel's most.
const PAGE: u32 = 50;

fn invalid(message: impl Into<String>) -> CloudError {
    CloudError::new(CloudErrorKind::InvalidArgument, message)
}

fn random_name() -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    format!("intent-{}", &hex[..12])
}

fn state_of(status: &str) -> InstanceState {
    match status {
        "running" => InstanceState::Running,
        "stopped" => InstanceState::Paused,
        _ => InstanceState::Other,
    }
}

/// Epoch milliseconds as RFC 3339; empty when unknown.
fn rfc3339(epoch_ms: Option<u64>) -> String {
    epoch_ms
        .and_then(|ms| i64::try_from(ms / 1000).ok())
        .and_then(|s| time::OffsetDateTime::from_unix_timestamp(s).ok())
        .and_then(|at| {
            at.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_default()
}

/// `text` percent-encoded for a query value.
fn encode(text: &str) -> String {
    text.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b':') {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn gone(name: &str, status: &str) -> CloudError {
    CloudError::new(
        CloudErrorKind::NotFound,
        format!("the Vercel sandbox {name} is {status}"),
    )
}

impl VercelClient {
    async fn create_machine(&self, request: &CreateInstance) -> Result<CloudInstance, CloudError> {
        let vcpus = request
            .vcpu
            .ok_or_else(|| invalid("a Vercel machine needs a vCPU count"))?;
        if request
            .tags
            .get(BACKEND_ID_TAG)
            .is_none_or(String::is_empty)
        {
            return Err(invalid(format!(
                "a Vercel machine needs the {BACKEND_ID_TAG} tag"
            )));
        }
        if request.tags.len() > MAX_TAGS {
            return Err(invalid(format!(
                "Vercel keeps at most {MAX_TAGS} tags on a sandbox"
            )));
        }
        let name = random_name();
        let timeout_ms = u64::from(request.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS)) * 1000;
        let body = json!({
            "projectId": self.config.project_id,
            "name": name,
            "image": request.image,
            "persistent": true,
            "resources": { "vcpus": vcpus },
            "timeout": timeout_ms,
            "tags": request.tags,
            "keepLastSnapshots": { "count": 1, "deleteEvicted": true },
            "snapshotExpiration": SNAPSHOT_EXPIRATION_MS,
        });
        // A create is not sent twice: the name is unique, so a resend after
        // a lost answer would be refused while the first one runs.
        let created: SessionReply = self
            .send_json(
                Call::slow("create-sandbox").once(),
                Method::POST,
                "/v3/sandboxes",
                Some(&body),
            )
            .await?;
        if created.session.status != "running" {
            if let Err(error) = self.wait_running(&name, false).await {
                let _ = self.delete_machine(&name).await;
                return Err(error);
            }
        }
        Ok(CloudInstance {
            id: created.sandbox.name,
            image: request.image.clone(),
            access_token: None,
            volume_id: None,
        })
    }

    /// Wait until sandbox `name`'s session runs; with `resume`, a session
    /// that ended meanwhile (a stop that was under way) is resumed again.
    async fn wait_running(&self, name: &str, resume: bool) -> Result<SessionReply, CloudError> {
        let deadline = Instant::now() + self.config.ready_timeout;
        let mut stopped = false;
        loop {
            let current = self.sandbox(name, resume && stopped).await?;
            stopped = current.session.status == "stopped";
            match current.session.status.as_str() {
                "running" => return Ok(current),
                "failed" | "aborted" => {
                    return Err(CloudError::new(
                        CloudErrorKind::Other,
                        format!("the Vercel sandbox {name} {}", current.session.status),
                    ))
                }
                _ => {}
            }
            if Instant::now() + self.config.poll_interval > deadline {
                return Err(CloudError::new(
                    CloudErrorKind::Other,
                    format!(
                        "the Vercel sandbox was not running after {} s",
                        self.config.ready_timeout.as_secs()
                    ),
                )
                .retryable(true));
            }
            tokio::time::sleep(self.config.poll_interval).await;
        }
    }

    async fn connect_machine(
        &self,
        name: &str,
        timeout_secs: u32,
    ) -> Result<Connected, CloudError> {
        let mut current = self.sandbox(name, true).await?;
        let resumed = current.resumed == Some(true);
        if current.session.status != "running" {
            current = self.wait_running(name, true).await?;
        }
        // Keep the session alive past the dial, as a connect promises. At
        // the plan's cap it cannot be: the supervisor's renewal reports
        // that and plans the stop and resume.
        match self.renew(name, timeout_secs).await {
            Ok(()) => {}
            Err(e) if e.kind() == CloudErrorKind::SessionCap => {}
            Err(e) => return Err(e),
        }
        Ok(Connected {
            instance: CloudInstance {
                id: name.to_owned(),
                image: current.sandbox.image.unwrap_or_default(),
                access_token: None,
                volume_id: None,
            },
            resumed,
        })
    }

    async fn pause_machine(&self, name: &str) -> Result<(), CloudError> {
        let current = self.sandbox(name, false).await?;
        if current.session.status == "stopped" {
            return Ok(());
        }
        validate_session(&current.session.id)?;
        let path = format!("/v2/sandboxes/sessions/{}/stop", current.session.id);
        self.send_json::<SessionReply>(Call::slow("stop"), Method::POST, &path, None)
            .await
            .map(drop)
    }

    /// Keep the session running at least `timeout_secs` from now: when it
    /// ends sooner, extend it by what is missing, in whole minutes, so the
    /// plan's cap is reached as late as possible. A stopped sandbox is not
    /// resumed: it is reported as not found, so the backend looks it up
    /// and marks it paused. Past the plan's cap, the error is
    /// [`CloudErrorKind::SessionCap`].
    async fn renew(&self, name: &str, timeout_secs: u32) -> Result<(), CloudError> {
        let current = self.sandbox(name, false).await?;
        if current.session.status != "running" {
            return Err(gone(name, &current.session.status));
        }
        let wanted = u64::from(timeout_secs) * 1000;
        let ends = current
            .session
            .started_at
            .unwrap_or_else(now_ms)
            .saturating_add(current.session.timeout);
        let left = ends.saturating_sub(now_ms());
        if left >= wanted {
            return Ok(());
        }
        let duration = (wanted - left).div_ceil(60_000) * 60_000;
        validate_session(&current.session.id)?;
        let path = format!(
            "/v2/sandboxes/sessions/{}/extend-timeout",
            current.session.id
        );
        let extended: ExtendReply = self
            // Not sent twice: a resend after a lost answer extends twice.
            .send_json(
                Call::new("extend-timeout").once(),
                Method::POST,
                &path,
                Some(&json!({ "duration": duration })),
            )
            .await?;
        if extended.session.status == "running" {
            Ok(())
        } else {
            Err(gone(name, &extended.session.status))
        }
    }

    /// `true` when it deleted the sandbox.
    async fn delete_machine(&self, name: &str) -> Result<bool, CloudError> {
        validate_name(name)?;
        let path = format!(
            "/v2/sandboxes/{name}?projectId={}&deleteOrphanSnapshots=true",
            self.config.project_id
        );
        match self
            .request(Call::new("delete"), Method::DELETE, &path, None, None)
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
        // Vercel filters by one tag; the others are checked here.
        let mut query = format!(
            "/v2/sandboxes?project={}&limit={PAGE}",
            self.config.project_id
        );
        if let Some((key, value)) = filter
            .tags
            .get_key_value(BACKEND_ID_TAG)
            .or_else(|| filter.tags.iter().next())
        {
            let _ = write!(query, "&tags={}", encode(&format!("{key}:{value}")));
        }
        let wants =
            |state: InstanceState| filter.states.is_empty() || filter.states.contains(&state);
        let mut found = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let path = match &cursor {
                Some(next) => format!("{query}&cursor={}", encode(next)),
                None => query.clone(),
            };
            let page: SandboxList = self.get_json(Call::new("list-sandboxes"), &path).await?;
            for sandbox in page.sandboxes {
                let state = state_of(&sandbox.status);
                let matches = filter
                    .tags
                    .iter()
                    .all(|(k, v)| sandbox.tags.get(k) == Some(v));
                if matches && wants(state) {
                    found.push(ListedInstance {
                        id: sandbox.name,
                        state,
                        tags: sandbox.tags,
                        started_at: rfc3339(sandbox.created_at),
                    });
                }
            }
            match page.pagination.next {
                Some(next) if !next.is_empty() => cursor = Some(next),
                _ => return Ok(found),
            }
        }
    }

    async fn image_ready_now(&self, image: &str) -> Result<bool, CloudError> {
        // The registry path needs the team slug and project: an image in
        // the entry's own project (`<repository>:<tag>`) cannot be looked
        // up by id. Neither can a registry that does not answer this team
        // for the repository (another team's public image). Then the
        // create is the real check, and maps a missing image to
        // `template-missing`.
        if image
            .split(':')
            .next()
            .is_none_or(|repo| !repo.contains('/'))
        {
            return Ok(true);
        }
        Ok(self.manifest(image).await?.unwrap_or(true))
    }
}

impl CloudProvider for VercelClient {
    fn kind(&self) -> &'static str {
        CLOUD_PROVIDER_VERCEL
    }

    fn create<'a>(&'a self, request: &'a CreateInstance) -> CloudFuture<'a, CloudInstance> {
        Box::pin(self.create_machine(request))
    }

    fn connect<'a>(&'a self, id: &'a str, timeout_secs: u32) -> CloudFuture<'a, Connected> {
        Box::pin(self.connect_machine(id, timeout_secs))
    }

    fn run_command<'a>(
        &'a self,
        instance: &'a CloudInstance,
        spec: &'a ProcessSpec,
    ) -> CloudFuture<'a, ProcessOutput> {
        Box::pin(self.command(&instance.id, spec))
    }

    fn pause<'a>(&'a self, id: &'a str) -> CloudFuture<'a, ()> {
        Box::pin(self.pause_machine(id))
    }

    fn keep_alive<'a>(&'a self, id: &'a str, timeout_secs: u32) -> CloudFuture<'a, ()> {
        Box::pin(self.renew(id, timeout_secs))
    }

    fn delete<'a>(&'a self, id: &'a str) -> CloudFuture<'a, bool> {
        Box::pin(self.delete_machine(id))
    }

    fn list<'a>(&'a self, filter: &'a ListInstances) -> CloudFuture<'a, Vec<ListedInstance>> {
        Box::pin(self.list_machines(filter))
    }

    fn check_key(&self) -> CloudFuture<'_, ()> {
        Box::pin(async move {
            let path = format!("/v2/sandboxes?project={}&limit=1", self.config.project_id);
            self.get_json::<SandboxList>(Call::new("list-sandboxes"), &path)
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
