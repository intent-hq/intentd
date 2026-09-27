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
use crate::repository_credentials::{
    RepositoryConnectionBinding, RepositoryCredentialError as Error, RepositoryCredentialSource,
    RepositorySecretReader, RepositorySecretRequest, RepositorySecretSnapshot, Result,
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

impl crate::Services {
    /// Capture the original installed source, with no selectable replacement.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "repository reader awaits its separately owned service consumer"
        )
    )]
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
