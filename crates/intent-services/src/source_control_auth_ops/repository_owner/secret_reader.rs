//! Fresh reads from the original paired source, compared with its settled owner.
//! A content fingerprint is equality evidence, never a credential or authority grant.

use std::sync::{Arc, Mutex};

use intent_core::FileSecretStore;
use intent_sourcecontrol::gitlab_auth::PersistenceLease;
use intent_sourcecontrol::gitlab_token::{
    EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT,
};
use intent_sourcecontrol::{GitlabDescriptor, SecretString};
use sha2::{Digest, Sha256};

use super::{GitlabCredentialGate, RepositoryOwner};
use crate::repository_credentials::authority::CredentialFuture;
use crate::repository_credentials::read::{
    RepositoryReadScope, RepositoryResponseAttribution, RepositoryResponseDisposition,
};
use crate::repository_credentials::{
    RepositoryConnectionBinding, RepositoryCredentialAdmission, RepositoryCredentialError as Error,
    RepositoryCredentialSource, RepositorySecretReader, RepositorySecretRequest,
    RepositorySecretSnapshot, Result,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct SourceFingerprint([u8; 32]);

struct AttestedSource {
    request: RepositorySecretRequest,
    descriptor: GitlabDescriptor,
    fingerprint: SourceFingerprint,
}

pub(super) struct SourceEvidence {
    nonce: uuid::Uuid,
    published: Mutex<Option<Arc<AttestedSource>>>,
    #[cfg(test)]
    read_probe: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl SourceEvidence {
    pub(super) fn new() -> Self {
        Self {
            nonce: uuid::Uuid::new_v4(),
            published: Mutex::new(None),
            #[cfg(test)]
            read_probe: Mutex::new(None),
        }
    }

    pub(super) fn fingerprint(
        &self,
        access: Option<&str>,
        refresh: Option<&str>,
        expiry: Option<&str>,
    ) -> SourceFingerprint {
        let mut digest = Sha256::new();
        digest.update(b"intent-repository-source-v1");
        digest.update(self.nonce.as_bytes());
        for value in [access, refresh, expiry] {
            match value {
                Some(value) => {
                    digest.update([1]);
                    digest.update((value.len() as u64).to_be_bytes());
                    digest.update(value.as_bytes());
                }
                None => digest.update([0]),
            }
        }
        SourceFingerprint(digest.finalize().into())
    }

    pub(super) fn invalidate(&self) -> Result<()> {
        *self.published.lock().map_err(|_| Error::Indeterminate)? = None;
        Ok(())
    }

    fn invalidate_matching(&self, original: &Arc<AttestedSource>) -> Result<()> {
        let mut current = self.published.lock().map_err(|_| Error::Indeterminate)?;
        if current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, original))
        {
            *current = None;
        }
        Ok(())
    }
}

/// Only a short-lived read/write operation owns these values.
/// The record in `SourceEvidence` retains their digest instead.
pub(super) struct SecretMaterial {
    access: Option<String>,
    refresh: Option<String>,
    expiry: Option<String>,
}

impl SecretMaterial {
    fn load(store: &FileSecretStore) -> Result<Self> {
        let values = store
            .load_many(&[
                SECRET_ACCOUNT,
                REFRESH_SECRET_ACCOUNT,
                EXPIRES_AT_SECRET_ACCOUNT,
            ])
            .map_err(|_| Error::Indeterminate)?;
        let mut values = values.into_iter();
        Ok(Self {
            access: values.next().flatten(),
            refresh: values.next().flatten(),
            expiry: values.next().flatten(),
        })
    }

    pub(super) fn access(&self) -> Result<&str> {
        self.access
            .as_deref()
            .filter(|token| !token.trim().is_empty())
            .ok_or(Error::Missing)
    }

    pub(super) fn fingerprint(&self, evidence: &SourceEvidence) -> SourceFingerprint {
        evidence.fingerprint(
            self.access.as_deref(),
            self.refresh.as_deref(),
            self.expiry.as_deref(),
        )
    }
}

pub(super) async fn load_material(
    store: FileSecretStore,
    lease: PersistenceLease,
) -> Result<SecretMaterial> {
    tokio::task::spawn_blocking(move || {
        let _lease = lease;
        SecretMaterial::load(&store)
    })
    .await
    .map_err(|_| Error::Indeterminate)?
}

impl RepositoryOwner {
    pub(super) fn publish_source(
        &self,
        binding: &RepositoryConnectionBinding,
        descriptor: &GitlabDescriptor,
        fingerprint: SourceFingerprint,
    ) -> Result<()> {
        let settings = self.settings.get().ok_or(Error::Unverified)?;
        if settings.store.is_none() || settings.source.is_none() {
            return Err(Error::Unverified);
        }
        self.check_descriptor(descriptor)?;
        let request = self.directory.selected_secret_request(binding)?;
        if request.source != RepositoryCredentialSource::GitlabSecretSlot {
            return Err(Error::Unverified);
        }
        *self
            .evidence
            .published
            .lock()
            .map_err(|_| Error::Indeterminate)? = Some(Arc::new(AttestedSource {
            request,
            descriptor: descriptor.clone(),
            fingerprint,
        }));
        Ok(())
    }

    fn check_descriptor(&self, expected: &GitlabDescriptor) -> Result<()> {
        let settings = self.settings.get().ok_or(Error::Unverified)?;
        let config = settings.config.lock().map_err(|_| Error::Indeterminate)?;
        let approved = super::adoption::approved_descriptor(&config, settings.fixture.as_ref());
        if approved.as_ref() != Some(expected)
            || self
                .descriptor
                .lock()
                .map_err(|_| Error::Indeterminate)?
                .as_ref()
                != Some(expected)
        {
            return Err(Error::BoundaryMismatch);
        }
        Ok(())
    }

    fn current_source(&self, expected: &RepositorySecretRequest) -> Result<Arc<AttestedSource>> {
        let actual = self.directory.selected_secret_request(&expected.binding)?;
        if actual != *expected {
            return Err(Error::SecretMismatch);
        }
        let original = self
            .evidence
            .published
            .lock()
            .map_err(|_| Error::Indeterminate)?
            .clone()
            .ok_or(Error::Unverified)?;
        if original.request != actual {
            return Err(Error::SecretMismatch);
        }
        self.check_descriptor(&original.descriptor)?;
        Ok(original)
    }
}

struct GitlabRepositorySecretReader {
    owner: Arc<RepositoryOwner>,
    gate: GitlabCredentialGate,
    store: FileSecretStore,
}

/// A past coherent observation of the original settled connection, not a
/// permission lease. Reobservation cannot switch its owner or connection.
pub(crate) struct RepositorySettledConnection {
    gate: GitlabCredentialGate,
    owner: Arc<RepositoryOwner>,
    descriptor: GitlabDescriptor,
    selected: RepositorySecretRequest,
}

impl RepositorySettledConnection {
    pub(crate) fn descriptor(&self) -> &GitlabDescriptor {
        &self.descriptor
    }

    pub(crate) fn selected(&self) -> &RepositorySecretRequest {
        &self.selected
    }

    /// A genuinely settled refresh may replace the proof and secret revision.
    /// Earlier observations and response stamps remain immutable.
    pub(crate) fn reobserve(&self) -> Result<Self> {
        Self::observe(self.gate.clone(), self.owner.clone(), Some(self))
    }

    fn observe(
        gate: GitlabCredentialGate,
        owner: Arc<RepositoryOwner>,
        original: Option<&Self>,
    ) -> Result<Self> {
        let (descriptor, selected) = {
            let settings = owner.settings.get().ok_or(Error::Unverified)?;
            let config = settings.config.lock().map_err(|_| Error::Indeterminate)?;
            let descriptor = owner.descriptor.lock().map_err(|_| Error::Indeterminate)?;
            owner.directory.with_settled_metadata(
                original.map(|value| &value.selected.binding),
                |actual, selected| {
                    let proof = owner
                        .evidence
                        .published
                        .lock()
                        .map_err(|_| Error::Indeterminate)?;
                    if settings.source.is_none()
                        || settings.store.is_none()
                        || selected.source != RepositoryCredentialSource::GitlabSecretSlot
                    {
                        return Err(Error::Unverified);
                    }
                    let proof = proof.as_ref().ok_or(Error::Unverified)?;
                    if proof.request != *selected {
                        return Err(Error::SecretMismatch);
                    }
                    if descriptor.as_ref() != Some(actual)
                        || proof.descriptor != *actual
                        || super::adoption::approved_descriptor(&config, settings.fixture.as_ref())
                            .as_ref()
                            != Some(actual)
                        || original.is_some_and(|value| {
                            value.descriptor != *actual || value.selected.source != selected.source
                        })
                    {
                        return Err(Error::BoundaryMismatch);
                    }
                    Ok((actual.clone(), selected.clone()))
                },
            )?
        };
        Ok(Self {
            gate,
            owner,
            descriptor,
            selected,
        })
    }
}

/// Original owner metadata, never an alternative permission or secret source.
/// The authority owner holds its original read-operation fence before either action.
pub(crate) struct RepositoryReadEligibility {
    owner: Arc<RepositoryOwner>,
    scope: RepositoryReadScope,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "qualified cache consumption remains separately owned"
    )
)]
impl RepositoryReadEligibility {
    pub(crate) fn check(&self) -> Result<()> {
        self.with_current(&mut || Ok(()))
    }

    /// Simultaneous eligibility of every captured scope, under the caller's
    /// fresh all-request authority fence. Only locks, never scopes, are deduped.
    /// The prebuilt transfer runs once with all metadata guards retained; it
    /// must not await, do I/O, or acquire caller/cache locks.
    pub(crate) fn with_all_current(
        originals: &[&Self],
        transfer: impl FnOnce() -> Result<()> + Send,
    ) -> Result<()> {
        let scopes = originals.iter().map(|item| &item.scope).collect::<Vec<_>>();
        let batch = RepositoryReadScope::prepare_all_current(&scopes)?;
        let mut owners = originals
            .iter()
            .map(|item| item.owner.clone())
            .collect::<Vec<_>>();
        owners.sort_unstable_by_key(Arc::as_ptr);
        owners.dedup_by(|left, right| Arc::ptr_eq(left, right));
        let indices = originals
            .iter()
            .map(|item| {
                owners
                    .binary_search_by_key(&Arc::as_ptr(&item.owner), Arc::as_ptr)
                    .expect("captured owner remains retained")
            })
            .collect::<Vec<_>>();
        let settings = owners
            .iter()
            .map(|owner| owner.settings.get().ok_or(Error::Unverified))
            .collect::<Result<Vec<_>>>()?;

        // All groups use this same rank order, including owners which share a
        // directory. No per-owner lock is reacquired inside the directory action.
        let configs = settings
            .iter()
            .map(|settings| settings.config.lock().map_err(|_| Error::Indeterminate))
            .collect::<Result<Vec<_>>>()?;
        let descriptors = owners
            .iter()
            .map(|owner| owner.descriptor.lock().map_err(|_| Error::Indeterminate))
            .collect::<Result<Vec<_>>>()?;
        batch.with_current(|selected| {
            let proofs = owners
                .iter()
                .map(|owner| {
                    owner
                        .evidence
                        .published
                        .lock()
                        .map_err(|_| Error::Indeterminate)
                })
                .collect::<Result<Vec<_>>>()?;
            for ((original, index), selected) in originals.iter().zip(indices).zip(selected) {
                original.check_source(
                    settings[index],
                    &configs[index],
                    descriptors[index].as_ref(),
                    proofs[index].as_deref(),
                    selected,
                )?;
            }
            transfer()
        })
    }

    /// Lock order: settings config -> descriptor -> directory -> source proof.
    /// Only a prebuilt, synchronous metadata/ownership action may run here.
    pub(crate) fn with_current(
        &self,
        action: &mut (dyn FnMut() -> Result<()> + Send),
    ) -> Result<()> {
        let settings = self.owner.settings.get().ok_or(Error::Unverified)?;
        let config = settings.config.lock().map_err(|_| Error::Indeterminate)?;
        let descriptor = self
            .owner
            .descriptor
            .lock()
            .map_err(|_| Error::Indeterminate)?;
        self.scope.with_current(&mut |selected| {
            let proof = self
                .owner
                .evidence
                .published
                .lock()
                .map_err(|_| Error::Indeterminate)?;
            self.check_source(
                settings,
                &config,
                descriptor.as_ref(),
                proof.as_deref(),
                selected,
            )?;
            action()
        })
    }

    pub(crate) fn with_response(
        &self,
        attribution: &RepositoryResponseAttribution,
        target: &intent_core::ReviewTarget,
        apply: &mut (dyn FnMut(RepositoryResponseDisposition) -> Result<()> + Send),
    ) -> Result<()> {
        let settings = self.owner.settings.get().ok_or(Error::Unverified)?;
        let config = settings.config.lock().map_err(|_| Error::Indeterminate)?;
        let descriptor = self
            .owner
            .descriptor
            .lock()
            .map_err(|_| Error::Indeterminate)?;
        self.scope
            .with_response(attribution, target, &mut |disposition, selected| {
                let proof = self
                    .owner
                    .evidence
                    .published
                    .lock()
                    .map_err(|_| Error::Indeterminate)?;
                if let Some(selected) = selected {
                    self.check_source(
                        settings,
                        &config,
                        descriptor.as_ref(),
                        proof.as_deref(),
                        selected,
                    )?;
                }
                // Rejection's accepted event survives its own directory disconnect.
                // NoDenial likewise says nothing about successful payload eligibility.
                apply(disposition)
            })
    }

    fn check_source(
        &self,
        settings: &super::adoption::SettingsAttachment,
        config: &intent_core::settings_file::GitlabSettings,
        descriptor: Option<&GitlabDescriptor>,
        proof: Option<&AttestedSource>,
        selected: &RepositorySecretRequest,
    ) -> Result<()> {
        if settings.source.is_none()
            || settings.store.is_none()
            || selected.source != RepositoryCredentialSource::GitlabSecretSlot
        {
            return Err(Error::Unverified);
        }
        let proof = proof.ok_or(Error::Unverified)?;
        if proof.request != *selected {
            return Err(Error::SecretMismatch);
        }
        let expected = self.scope.descriptor();
        if descriptor != Some(expected)
            || proof.descriptor != *expected
            || super::adoption::approved_descriptor(config, settings.fixture.as_ref()).as_ref()
                != Some(expected)
        {
            return Err(Error::BoundaryMismatch);
        }
        Ok(())
    }
}

impl crate::Services {
    /// Observe only this Services instance's original settled, paired owner.
    /// This reads no secret and supplies no caller, target or read authority.
    pub(crate) fn gitlab_repository_settled_connection(
        &self,
    ) -> Result<super::RepositorySettledConnection> {
        let gate = self.gitlab_credential_gate.clone();
        let owner = gate.repository.get().cloned().ok_or(Error::Unverified)?;
        RepositorySettledConnection::observe(gate, owner, None)
    }

    /// Capture only this Services instance's installed owner and actual read
    /// admission. Dispatch deadlines do not govern eligibility of retained data.
    pub(crate) fn gitlab_repository_read_eligibility(
        &self,
        admission: &RepositoryCredentialAdmission,
    ) -> Result<super::RepositoryReadEligibility> {
        let owner = self
            .gitlab_credential_gate
            .repository
            .get()
            .cloned()
            .ok_or(Error::Unverified)?;
        let scope = RepositoryReadScope::capture(owner.directory.clone(), admission)?;
        Ok(RepositoryReadEligibility { owner, scope })
    }

    /// Capture the original installed source, with no selectable replacement.
    pub(crate) fn gitlab_repository_secret_reader(
        &self,
    ) -> Result<Arc<dyn RepositorySecretReader>> {
        let gate = self.gitlab_credential_gate.clone();
        let owner = gate.repository.get().cloned().ok_or(Error::Unverified)?;
        let settings = owner.settings.get().ok_or(Error::Unverified)?;
        let store = settings.store.clone().ok_or(Error::Unverified)?;
        Ok(Arc::new(GitlabRepositorySecretReader {
            owner,
            gate,
            store,
        }))
    }
}

impl RepositorySecretReader for GitlabRepositorySecretReader {
    fn load<'a>(
        &'a self,
        expected: &'a RepositorySecretRequest,
    ) -> CredentialFuture<'a, RepositorySecretSnapshot> {
        Box::pin(async move {
            if expected.source != RepositoryCredentialSource::GitlabSecretSlot {
                return Err(Error::Unverified);
            }
            let guard = self.gate.lock().await;
            let original = self.owner.current_source(expected)?;
            let store = self.store.clone();
            let lease = guard.lease();
            #[cfg(test)]
            let probe = self.owner.evidence.read_probe.lock().unwrap().clone();
            let loaded = tokio::task::spawn_blocking(move || {
                let _lease = lease;
                #[cfg(test)]
                if let Some(probe) = probe {
                    probe();
                }
                SecretMaterial::load(&store)
            })
            .await
            .map_err(|_| Error::Indeterminate)?;
            let current = self.owner.current_source(expected)?;
            if !Arc::ptr_eq(&original, &current) {
                return Err(Error::SecretMismatch);
            }
            let material = loaded?;
            let token = match material.access() {
                Ok(token) => token,
                Err(error) => {
                    self.owner.evidence.invalidate_matching(&original)?;
                    return Err(error);
                }
            };
            if material.fingerprint(&self.owner.evidence) != original.fingerprint {
                self.owner.evidence.invalidate_matching(&original)?;
                return Err(Error::SecretMismatch);
            }
            let snapshot = RepositorySecretSnapshot {
                request: original.request.clone(),
                token: SecretString::from(token.trim()),
            };
            drop(guard);
            Ok(snapshot)
        })
    }
}

#[cfg(test)]
pub(crate) mod tests;

// The standalone credential test wrapper has no Services owner. Include these
// real-owner interaction cases only in the library's existing private owner.
#[cfg(test)]
#[path = "../../../tests/repository_credentials/read.rs"]
mod read_tests;
