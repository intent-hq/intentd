//! Account-qualified child Git choice in the original paired file. This is not
//! public settings, a secret source, caller authority or native Git permission.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use intent_core::{Error as CoreError, FileSecretStore, Result as CoreResult};
use intent_sourcecontrol::{GitlabHost, GitlabInstance, SecretString};
use serde::{Deserialize, Serialize};

use super::{
    ChildChoice, GitlabCredentialGate, RepositoryOwner, RepositorySettledConnection,
    RepositoryWrite, WriteState,
};
use crate::repository_credential_writers::RepositoryWriterPreflight;
use crate::repository_credentials::{
    RepositoryCredentialError as Error, RepositoryMutationKind, RepositorySecretReader,
    RepositorySecretRequest, Result,
};
use crate::source_control_auth_ops::checkout::CheckoutAuthority;

const ACCOUNT: &str = "sourceControl.gitlab.childGitPolicy";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Grant {
    version: u8,
    instance_base_url: String,
    account_id: String,
    enabled: bool,
}
impl Grant {
    fn load(store: &FileSecretStore) -> Result<Option<Self>> {
        let Some(raw) = store.load(ACCOUNT).map_err(|_| Error::Indeterminate)? else {
            return Ok(None);
        };
        Self::parse(&raw).map(Some)
    }

    fn parse(raw: &str) -> Result<Self> {
        let grant: Self = serde_json::from_str(raw).map_err(|_| Error::Indeterminate)?;
        if grant.version != 1
            || grant.account_id.is_empty()
            || GitlabInstance::parse(&grant.instance_base_url)
                .ok()
                .as_ref()
                .map(GitlabInstance::as_str)
                != Some(grant.instance_base_url.as_str())
        {
            return Err(Error::Indeterminate);
        }
        Ok(grant)
    }
    fn matches(&self, instance: &str, account: &str) -> bool {
        self.instance_base_url == instance && self.account_id == account
    }
}

impl RepositoryWrite {
    /// The existing owner has begun and still holds its physical credential
    /// lease. Persist a lost choice before any replacement/revoke/settings effect;
    /// failed preflight never reaches this hook. Refresh waits for verified user.
    pub(super) fn prepare_child_choice(&self, state: &mut WriteState) -> Result<()> {
        if !matches!(state.child_choice, ChildChoice::Pending) {
            return Ok(());
        }
        if self.kind == RepositoryMutationKind::Refresh
            && state.user.is_none()
            && !state.disconnected
        {
            return Ok(());
        }
        let Some(store) = self.owner.settings.get().and_then(|s| s.store.as_ref()) else {
            return Ok(());
        };
        let raw = store.load(ACCOUNT).map_err(|_| Error::Indeterminate)?;
        // An invalid choice never grants access and must not prevent the
        // original credential writer from revoking/replacing its own source.
        // An unreadable paired map still fails above without writing anything.
        let grant = raw.as_deref().and_then(|raw| Grant::parse(raw).ok());
        let matching = grant.as_ref().is_some_and(|grant| {
            !state.disconnected
                && self
                    .descriptor
                    .as_ref()
                    .zip(state.user.as_ref())
                    .is_some_and(|(descriptor, user)| {
                        grant.matches(descriptor.instance().as_str(), &user.id.to_string())
                    })
        });
        if raw.is_some() && !matching {
            store.delete(ACCOUNT).map_err(|_| Error::Indeterminate)?;
        }
        state.child_choice = if raw.is_some() {
            ChildChoice::Known(matching && grant.as_ref().is_some_and(|grant| grant.enabled))
        } else {
            ChildChoice::Absent
        };
        Ok(())
    }
}

pub(crate) struct GitlabChildPolicyStatus {
    pub(crate) instance_base_url: String,
    pub(crate) account_id: String,
    pub(crate) connection_id: String,
    pub(crate) connection_generation: u64,
    pub(crate) revision: u64,
    pub(crate) enabled: bool,
}

/// Owned status plus its original final-delivery proof. No caller constructor.
pub(crate) struct GitlabChildPolicyOutput {
    status: GitlabChildPolicyStatus,
    receipt: GitlabChildPolicyReceipt,
}

impl GitlabChildPolicyOutput {
    pub(crate) fn into_parts(
        self,
    ) -> (
        super::GitlabChildPolicyStatus,
        super::GitlabChildPolicyReceipt,
    ) {
        (self.status, self.receipt)
    }
}

/// Token-free, one-use metadata output. The action must not await, do I/O,
/// spawn, or recursively take the retained R authority or P metadata locks.
pub(crate) struct GitlabChildPolicyReceipt {
    original: Arc<RepositorySettledConnection>,
    choice: (bool, u64),
    authority: Arc<dyn CheckoutAuthority>,
    consumed: AtomicBool,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "status output is composed by the transport consumer"
    )
)]
impl GitlabChildPolicyReceipt {
    /// Consume this receipt to bind one immutable encoded output frame. The
    /// returned guard may recheck its original facts on every transport phase.
    pub(crate) fn into_output(self) -> CoreResult<super::GitlabChildOutputGuard> {
        self.with_current(|| Ok(()))?;
        Ok(GitlabChildOutputGuard {
            original: self.original,
            authority: self.authority,
            proof: OutputProof::Policy(self.choice),
        })
    }

    pub(crate) fn with_current(
        &self,
        transfer: impl FnOnce() -> CoreResult<()> + Send,
    ) -> CoreResult<()> {
        let mut transfer = Some(transfer);
        let mut result = None;
        self.authority.dispatch(&mut || {
            self.original
                .with_child_policy_status(Some(self.choice), |_, _| {
                    if self.consumed.swap(true, Ordering::AcqRel) {
                        return Err(Error::Retired);
                    }
                    result = Some(transfer.take().ok_or(Error::Retired)?());
                    Ok(())
                })
                .map_err(local)
        })?;
        result.ok_or_else(|| local(Error::AuthorityUnavailable))?
    }
}

/// Caller-supplied comparison facts only; never an authority or a grant.
#[derive(Clone)]
pub(crate) struct GitlabChildPolicyExpected {
    pub(crate) instance_base_url: String,
    pub(crate) account_id: String,
    pub(crate) connection_id: String,
    pub(crate) connection_generation: u64,
    pub(crate) revision: u64,
}

pub(crate) struct GitlabChildCredential {
    token: SecretString,
    receipt: GitlabChildCredentialReceipt,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "credential output is composed by the UDS consumer"
    )
)]
impl GitlabChildCredential {
    pub(crate) fn into_parts(self) -> (SecretString, super::GitlabChildCredentialReceipt) {
        (self.token, self.receipt)
    }
}

/// Original acquired metadata, without the token. R uses this directly at the
/// final output boundary, not nested beneath the same authority a second time.
pub(crate) struct GitlabChildCredentialReceipt {
    original: Arc<RepositorySettledConnection>,
    selected: RepositorySecretRequest,
    child_revision: u64,
    project: String,
    authority: Arc<dyn CheckoutAuthority>,
    consumed: AtomicBool,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "final output receipt is composed by the UDS consumer"
    )
)]
impl GitlabChildCredentialReceipt {
    /// Bind one immutable credential frame; no token is retained in the guard.
    pub(crate) fn into_output(self) -> CoreResult<super::GitlabChildOutputGuard> {
        self.with_current(|| Ok(()))?;
        Ok(GitlabChildOutputGuard {
            original: self.original,
            authority: self.authority,
            proof: OutputProof::Credential {
                selected: Box::new(self.selected),
                child_revision: self.child_revision,
                project: self.project,
            },
        })
    }

    pub(crate) fn with_current(
        &self,
        transfer: impl FnOnce() -> CoreResult<()> + Send,
    ) -> CoreResult<()> {
        let mut transfer = Some(transfer);
        let mut result = None;
        self.authority.dispatch(&mut || {
            self.original
                .with_child_current(
                    Some(&self.selected),
                    &[&self.project],
                    true,
                    Some(self.child_revision),
                    |_, _| {
                        if self.consumed.swap(true, Ordering::AcqRel) {
                            return Err(Error::Retired);
                        }
                        result = Some(transfer.take().ok_or(Error::Retired)?());
                        Ok(())
                    },
                )
                .map_err(local)
        })?;
        result.ok_or_else(|| local(Error::AuthorityUnavailable))?
    }
}

enum OutputProof {
    Policy((bool, u64)),
    Credential {
        selected: Box<RepositorySecretRequest>,
        child_revision: u64,
        project: String,
    },
}

/// A nonsecret guard for ONE immutable frame owned by R. It is not cloneable
/// and cannot recapture an owner or rotate a credential. Every synchronous
/// nonblocking write/readiness/flush phase re-enters the original R/P fence;
/// no lock guard is retained while transport is Pending; no await/spawn is allowed.
pub(crate) struct GitlabChildOutputGuard {
    original: Arc<RepositorySettledConnection>,
    authority: Arc<dyn CheckoutAuthority>,
    proof: OutputProof,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "bound output phases are composed by the transport consumer"
    )
)]
impl GitlabChildOutputGuard {
    pub(crate) fn with_current(
        &self,
        action: impl FnOnce() -> CoreResult<()> + Send,
    ) -> CoreResult<()> {
        let mut action = Some(action);
        let mut result = None;
        let mut transfer = || {
            result = Some(action.take().ok_or(Error::Retired)?());
            Ok(())
        };
        self.authority.dispatch(&mut || {
            match &self.proof {
                OutputProof::Policy(choice) => self
                    .original
                    .with_child_policy_status(Some(*choice), |_, _| transfer()),
                OutputProof::Credential {
                    selected,
                    child_revision,
                    project,
                } => self.original.with_child_current(
                    Some(selected),
                    &[project],
                    true,
                    Some(*child_revision),
                    |_, _| transfer(),
                ),
            }
            .map_err(local)
        })?;
        result.ok_or_else(|| local(Error::AuthorityUnavailable))?
    }
}

pub(crate) struct GitlabChildPolicyCapture {
    original: Arc<RepositorySettledConnection>,
    owner: Arc<RepositoryOwner>,
    gate: GitlabCredentialGate,
    store: FileSecretStore,
    reader: Arc<dyn RepositorySecretReader>,
    #[cfg(test)]
    write_probe: Option<Arc<dyn Fn() + Send + Sync>>,
}

fn local(error: Error) -> CoreError {
    crate::pr_ops::map_sc_err(error.into())
}

impl crate::Services {
    /// Capture before any permission/lifetime await; no caller-supplied identity.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "child policy entry is composed by Services integration"
        )
    )]
    pub(crate) fn capture_gitlab_child_policy(
        &self,
    ) -> CoreResult<super::GitlabChildPolicyCapture> {
        let original = Arc::new(self.gitlab_repository_settled_connection().map_err(local)?);
        let owner = self
            .gitlab_credential_gate
            .repository
            .get()
            .cloned()
            .ok_or_else(|| local(Error::Unverified))?;
        let store = owner
            .settings
            .get()
            .and_then(|s| s.store.clone())
            .ok_or_else(|| local(Error::Unverified))?;
        Ok(GitlabChildPolicyCapture {
            original,
            owner,
            store,
            gate: self.gitlab_credential_gate.clone(),
            reader: self.gitlab_repository_secret_reader().map_err(local)?,
            #[cfg(test)]
            write_probe: None,
        })
    }
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "child policy entry is composed by Services integration"
    )
)]
impl GitlabChildPolicyCapture {
    fn current_status(&self) -> CoreResult<GitlabChildPolicyStatus> {
        self.original
            .with_child_policy_status(None, |enabled, revision| {
                let binding = &self.original.selected().binding;
                Ok(GitlabChildPolicyStatus {
                    instance_base_url: binding.account.instance_base_url.clone(),
                    account_id: binding.account.account_id.clone(),
                    connection_id: binding.scope.connection_id.clone(),
                    connection_generation: binding.scope.connection_generation,
                    revision,
                    enabled,
                })
            })
            .map_err(local)
    }

    fn status_output(
        &self,
        grant: Option<&Grant>,
        authority: &Arc<dyn CheckoutAuthority>,
    ) -> CoreResult<super::GitlabChildPolicyOutput> {
        let mut result = None;
        authority.dispatch(&mut || {
            if result.is_some() {
                return Err(local(Error::Retired));
            }
            let mut status = self.current_status()?;
            let choice = (status.enabled, status.revision);
            status.enabled &= grant.is_some_and(|grant| {
                grant.enabled && grant.matches(&status.instance_base_url, &status.account_id)
            });
            result = Some(GitlabChildPolicyOutput {
                status,
                receipt: GitlabChildPolicyReceipt {
                    original: self.original.clone(),
                    choice,
                    authority: authority.clone(),
                    consumed: AtomicBool::new(false),
                },
            });
            Ok(())
        })?;
        result.ok_or_else(|| local(Error::AuthorityUnavailable))
    }

    pub(crate) async fn status(
        &self,
        authority: Arc<dyn CheckoutAuthority>,
    ) -> CoreResult<super::GitlabChildPolicyOutput> {
        let guard = self.gate.lock().await;
        let store = self.store.clone();
        let lease = guard.lease();
        let grant = tokio::task::spawn_blocking(move || {
            let _lease = lease;
            Grant::load(&store).map_err(local)
        })
        .await
        .map_err(|_| local(Error::Indeterminate))??;
        self.status_output(grant.as_ref(), &authority)
    }

    pub(crate) async fn set(
        self,
        expected: super::GitlabChildPolicyExpected,
        enabled: bool,
        authority: Arc<dyn CheckoutAuthority>,
    ) -> CoreResult<super::GitlabChildPolicyOutput> {
        let guard = self.gate.lock().await;
        let mut mutation = None;
        authority.dispatch(&mut || {
            if mutation.is_some() {
                return Err(local(Error::Retired));
            }
            let current = self.current_status()?;
            if current.instance_base_url != expected.instance_base_url
                || current.account_id != expected.account_id
                || current.connection_id != expected.connection_id
                || current.connection_generation != expected.connection_generation
                || current.revision != expected.revision
            {
                return Err(local(Error::Retired));
            }
            mutation = self
                .owner
                .writers
                .reserve_child_policy(self.original.selected().binding.clone())
                .map_err(local)?
                .begin(|| Ok(RepositoryWriterPreflight::Change))
                .map_err(local)?;
            Ok(())
        })?;
        let mutation = mutation.ok_or_else(|| local(Error::AuthorityUnavailable))?;
        let grant = Grant {
            version: 1,
            instance_base_url: self.original.descriptor().instance().as_str().into(),
            account_id: self.original.selected().binding.account.account_id.clone(),
            enabled,
        };
        let encoded = serde_json::to_string(&grant).map_err(|_| local(Error::Indeterminate))?;
        let store = self.store.clone();
        let lease = guard.lease();
        #[cfg(test)]
        let write_probe = self.write_probe.clone();
        let job = tokio::task::spawn_blocking(move || {
            let _lease = lease;
            #[cfg(test)]
            if let Some(probe) = write_probe {
                probe();
            }
            // Strict read refuses corruption before FileSecretStore's ordinary
            // single-key persistence. Completion belongs to this actual worker.
            store.load(ACCOUNT)?;
            store.store(ACCOUNT, &encoded)?;
            mutation.complete(enabled).map_err(local)
        });
        job.await.map_err(|_| local(Error::Indeterminate))??;
        self.status_output(Some(&grant), &authority)
    }

    /// Resolve only the currently enabled original account's exact HTTPS root.
    /// R supplies the authentic child/process owner, never a synthetic workspace.
    pub(crate) async fn credential(
        self,
        protocol: &str,
        host: &str,
        path: &str,
        username: Option<&str>,
        authority: Arc<dyn CheckoutAuthority>,
    ) -> CoreResult<Option<super::GitlabChildCredential>> {
        if !protocol.eq_ignore_ascii_case("https")
            || username.is_some_and(|v| v != "oauth2")
            || host.contains(['/', '@', '?', '#', '\\'])
            || path.starts_with('/')
            || path.contains(['%', '?', '#', '\\'])
            || [host, path].iter().any(|v| v.chars().any(char::is_control))
        {
            return Ok(None);
        }
        let instance = self.original.descriptor().instance();
        let logical = GitlabHost::parse(instance.as_str()).map_err(crate::pr_ops::map_sc_err)?;
        let Ok(request_host) = GitlabHost::parse(host) else {
            return Ok(None);
        };
        if logical.host() != request_host.host() {
            return Ok(None);
        }
        let url = format!("https://{}/{path}", request_host.host());
        if !instance.contains_url(&url) {
            return Ok(None);
        }
        let project = url
            .strip_prefix(instance.as_str())
            .and_then(|v| v.strip_prefix('/'))
            .unwrap_or("");
        let project = project.strip_suffix(".git").unwrap_or(project);
        if project.split('/').count() < 2
            || project
                .split('/')
                .any(|v| v.is_empty() || v == "." || v == "..")
        {
            return Ok(None);
        }
        let (status, _) = self.status(authority.clone()).await?.into_parts();
        if !status.enabled {
            return Ok(None);
        }
        let mut expected = None;
        authority.dispatch(&mut || {
            if expected.is_some() {
                return Err(local(Error::Retired));
            }
            expected = Some(
                self.original
                    .with_child_current(
                        None,
                        &[project],
                        true,
                        Some(status.revision),
                        |selected, _| Ok(selected.clone()),
                    )
                    .map_err(local)?,
            );
            Ok(())
        })?;
        let expected = expected.ok_or_else(|| local(Error::AuthorityUnavailable))?;
        let snapshot = tokio::time::timeout(Duration::from_secs(5), self.reader.load(&expected))
            .await
            .map_err(|_| local(Error::TimedOut))?
            .map_err(local)?;
        if snapshot.request != expected {
            return Err(local(Error::SecretMismatch));
        }
        let mut token = Some(snapshot.token);
        let mut result = None;
        authority.dispatch(&mut || {
            self.original
                .with_child_current(
                    Some(&expected),
                    &[project],
                    true,
                    Some(status.revision),
                    |_, _| {
                        result = Some(token.take().ok_or(Error::Retired)?);
                        Ok(())
                    },
                )
                .map_err(local)
        })?;
        Ok(result.map(|token| GitlabChildCredential {
            token,
            receipt: GitlabChildCredentialReceipt {
                original: self.original,
                selected: expected,
                child_revision: status.revision,
                project: project.into(),
                authority,
                consumed: AtomicBool::new(false),
            },
        }))
    }
}

#[cfg(test)]
mod tests;
