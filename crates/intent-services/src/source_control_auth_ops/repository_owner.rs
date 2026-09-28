//! Original auth-writer ownership, attached explicitly to the repository directory.
//! The optional descriptor is an approved mapping, never reconstructed from settings.
//! This bridge neither persists secrets nor repairs an uncertain writer.

use std::sync::{Arc, Mutex, OnceLock};

mod adoption;
#[cfg(not(test))]
mod secret_reader;
#[cfg(test)]
pub(crate) mod secret_reader;
pub(crate) use adoption::{logical_instance, RepositorySettingsWrite};
pub(crate) use secret_reader::RepositoryReadEligibility;
pub(crate) use secret_reader::RepositorySettledConnection;
pub(crate) use secret_reader::{
    RepositoryAttachmentState, RepositoryChildPolicyState, RepositoryConnectionFacts,
    RepositoryDescriptorState,
};

use intent_sourcecontrol::gitlab_auth::{
    GitlabWriteObserver, GitlabWriteOutcome, PersistenceLease,
};
use intent_sourcecontrol::{GitlabDescriptor, GitlabHost, GitlabUser};

use crate::repository_credential_writers::{
    RepositoryCredentialWriters, RepositoryWriterMutation, RepositoryWriterPreflight,
    RepositoryWriterReservation,
};
use crate::repository_credentials::{
    RepositoryConnectionDirectory, RepositoryCredentialError, RepositoryCredentialSource,
    RepositoryMutationKind, SettledCredentialState, VerifiedRepositoryAccount,
};

/// Clones retain the original credential mutex. Attaching metadata adds no second
/// writer gate and cannot adopt a token merely because a host string matches.
#[derive(Clone)]
pub(crate) struct GitlabCredentialGate {
    mutex: Arc<tokio::sync::Mutex<()>>,
    repository: Arc<OnceLock<Arc<RepositoryOwner>>>,
}

pub(crate) struct GitlabCredentialGuard {
    mutex: Arc<tokio::sync::Mutex<()>>,
    lease: PersistenceLease,
}

impl GitlabCredentialGuard {
    pub(super) fn lease(&self) -> PersistenceLease {
        self.lease.clone()
    }
}

struct RepositoryOwner {
    directory: Arc<RepositoryConnectionDirectory>,
    writers: RepositoryCredentialWriters,
    descriptor: Mutex<Option<GitlabDescriptor>>,
    settings: OnceLock<adoption::SettingsAttachment>,
    evidence: secret_reader::SourceEvidence,
    #[cfg(test)]
    write_probe: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl GitlabCredentialGate {
    pub(super) fn new() -> Self {
        Self {
            mutex: Arc::new(tokio::sync::Mutex::new(())),
            repository: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) async fn lock(&self) -> GitlabCredentialGuard {
        GitlabCredentialGuard {
            mutex: self.mutex.clone(),
            lease: Arc::new(self.mutex.clone().lock_owned().await),
        }
    }

    /// The constructor owner supplies the SAME directory used by admission and
    /// its explicitly approved descriptor. This is not a settings/adoption API.
    pub(crate) fn attach_repository(
        &self,
        directory: Arc<RepositoryConnectionDirectory>,
        descriptor: Option<GitlabDescriptor>,
    ) -> Result<(), RepositoryCredentialError> {
        self.repository
            .set(Arc::new(RepositoryOwner {
                writers: RepositoryCredentialWriters::new(directory.clone()),
                directory,
                descriptor: Mutex::new(descriptor),
                settings: OnceLock::new(),
                evidence: secret_reader::SourceEvidence::new(),
                #[cfg(test)]
                write_probe: Mutex::new(None),
            }))
            .map_err(|_| RepositoryCredentialError::StaleMutation)
    }

    #[cfg(test)]
    pub(super) fn set_write_probe(&self, probe: Arc<dyn Fn() + Send + Sync>) {
        *self.repository.get().unwrap().write_probe.lock().unwrap() = Some(probe);
    }

    /// Reservation is synchronous and precedes preflight awaits. An unstarted
    /// reservation never changes availability; the actual owner calls begin.
    pub(super) fn reserve(
        &self,
        host: &GitlabHost,
        kind: RepositoryMutationKind,
    ) -> intent_sourcecontrol::Result<Option<Arc<RepositoryWrite>>> {
        let Some(owner) = self.repository.get() else {
            return Ok(None);
        };
        let descriptor = owner
            .descriptor
            .lock()
            .map_err(|_| map_owner_error(RepositoryCredentialError::Indeterminate))?
            .as_ref()
            .filter(|d| matches_host(d, host))
            .cloned();
        if owner.settings.get().is_some() && descriptor.is_none() {
            return Err(map_owner_error(RepositoryCredentialError::BoundaryMismatch));
        }
        // An unadopted legacy refresh cannot claim continuity with a connection.
        let kind = if kind == RepositoryMutationKind::Refresh {
            match owner.directory.binding() {
                Ok(_) => kind,
                Err(
                    RepositoryCredentialError::Unverified | RepositoryCredentialError::Disconnected,
                ) => RepositoryMutationKind::Replace,
                Err(e) => return Err(map_owner_error(e)),
            }
        } else {
            kind
        };
        let reservation = owner.writers.reserve(kind).map_err(map_owner_error)?;
        Ok(Some(Self::new_write(owner, descriptor, reservation, kind)))
    }

    fn new_write(
        owner: &Arc<RepositoryOwner>,
        descriptor: Option<GitlabDescriptor>,
        reservation: RepositoryWriterReservation,
        kind: RepositoryMutationKind,
    ) -> Arc<RepositoryWrite> {
        Arc::new(RepositoryWrite {
            descriptor,
            #[cfg(test)]
            write_probe: owner.write_probe.lock().unwrap().clone(),
            owner: owner.clone(),
            state: Mutex::new(WriteState {
                reservation: Some(reservation),
                mutation: None,
                user: None,
                disconnected: kind == RepositoryMutationKind::Disconnect,
                publication_confirmed: kind != RepositoryMutationKind::Replace,
                persisted: false,
                candidate: None,
            }),
        })
    }
}

fn matches_host(descriptor: &GitlabDescriptor, host: &GitlabHost) -> bool {
    let Ok(logical) = GitlabHost::parse(descriptor.instance().as_str()) else {
        return false;
    };
    if logical.host() != host.host() {
        return false;
    }
    let transport = if logical.base_url() == host.base_url() {
        Ok(GitlabDescriptor::new(descriptor.instance().clone()))
    } else {
        GitlabDescriptor::with_loopback_endpoint(descriptor.instance().clone(), host.base_url())
    };
    transport.is_ok_and(|actual| actual == *descriptor)
}

fn map_owner_error(error: RepositoryCredentialError) -> intent_sourcecontrol::Error {
    error.into()
}

/// No credential is stored here. Drop retains uncertainty through the existing
/// mutation guard. Only the actual persistence owner can report completion.
pub(crate) struct RepositoryWrite {
    owner: Arc<RepositoryOwner>,
    descriptor: Option<GitlabDescriptor>,
    #[cfg(test)]
    write_probe: Option<Arc<dyn Fn() + Send + Sync>>,
    state: Mutex<WriteState>,
}
struct WriteState {
    reservation: Option<RepositoryWriterReservation>,
    mutation: Option<RepositoryWriterMutation>,
    user: Option<GitlabUser>,
    disconnected: bool,
    publication_confirmed: bool,
    persisted: bool,
    candidate: Option<secret_reader::SourceFingerprint>,
}

impl RepositoryWrite {
    /// Refresh begins under the original gate BEFORE the exchange can rotate a
    /// token. Persistence calls the same fence again without starting twice.
    pub(super) fn begin(&self) -> intent_sourcecontrol::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| map_owner_error(RepositoryCredentialError::Indeterminate))?;
        if state.mutation.is_none() {
            let reservation = state
                .reservation
                .take()
                .ok_or(intent_sourcecontrol::Error::AdmissionRetired)?;
            state.mutation = reservation
                .begin(|| Ok(RepositoryWriterPreflight::Change))
                .map_err(map_owner_error)?;
            self.owner.evidence.invalidate().map_err(map_owner_error)?;
        }
        state
            .mutation
            .as_ref()
            .ok_or(intent_sourcecontrol::Error::AdmissionRetired)?
            .check_current()
            .map_err(map_owner_error)
    }

    /// Auth refusal cleanup uses the same owner that attempted refresh. There is
    /// no second reservation that could steal an unsettled mutation's completion.
    pub(super) fn deleting(&self) -> intent_sourcecontrol::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| map_owner_error(RepositoryCredentialError::Indeterminate))?;
        state.disconnected = true;
        state.publication_confirmed = true;
        Ok(())
    }

    /// Called only after the existing settings owner has published the matching
    /// legacy binding. Missing registry or a failed/pinned bind is not evidence.
    pub(super) fn confirm_publication(&self, confirmed: bool) {
        if let Ok(mut state) = self.state.lock() {
            state.publication_confirmed = confirmed;
            self.settle(&mut state);
        }
    }

    fn settle(&self, state: &mut WriteState) {
        if !state.persisted || !state.publication_confirmed {
            return;
        }
        let Some(mutation) = state.mutation.as_ref() else {
            return;
        };
        let outcome = if state.disconnected {
            SettledCredentialState::Disconnected
        } else if let Some(verified) =
            self.descriptor
                .as_ref()
                .zip(state.user.as_ref())
                .and_then(|(descriptor, user)| {
                    if self
                        .owner
                        .settings
                        .get()
                        .is_some_and(|s| s.source.is_none())
                    {
                        return None;
                    }
                    VerifiedRepositoryAccount::from_verified_user(
                        descriptor.clone(),
                        user.id,
                        RepositoryCredentialSource::GitlabSecretSlot,
                    )
                    .ok()
                })
        {
            SettledCredentialState::Verified(verified)
        } else {
            SettledCredentialState::Indeterminate
        };
        match mutation.completion().complete(outcome) {
            Ok(Some(binding)) => {
                if let Some((descriptor, fingerprint)) =
                    self.descriptor.as_ref().zip(state.candidate)
                {
                    if let Err(error) = self.owner.publish_source(&binding, descriptor, fingerprint)
                    {
                        tracing::debug!(%error, "repository source evidence remains unavailable");
                    }
                }
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%error, "repository credential settlement remains unavailable");
            }
        }
    }
}

impl GitlabWriteObserver for RepositoryWrite {
    fn before_write(&self) -> intent_sourcecontrol::Result<()> {
        self.begin()?;
        #[cfg(test)]
        if let Some(probe) = &self.write_probe {
            probe();
        }
        Ok(())
    }

    fn verified_user(&self, user: Option<GitlabUser>) {
        if let Ok(mut state) = self.state.lock() {
            state.user = user;
        }
    }

    fn persisted_credential(&self, access: &str, refresh: Option<&str>, expires_at: Option<u64>) {
        if let Ok(mut state) = self.state.lock() {
            let expiry = expires_at.map(|value| value.to_string());
            state.candidate = Some(self.owner.evidence.fingerprint(
                Some(access),
                refresh,
                expiry.as_deref(),
            ));
        }
    }

    fn settled(&self, outcome: GitlabWriteOutcome) {
        if let Ok(mut state) = self.state.lock() {
            match outcome {
                GitlabWriteOutcome::Persisted => {
                    state.persisted = true;
                    self.settle(&mut state);
                }
                GitlabWriteOutcome::Uncertain => {
                    if let Some(mutation) = &state.mutation {
                        let _ = mutation.completion().indeterminate();
                    }
                }
            }
        }
    }
}
