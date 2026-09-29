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

/// Configuration and pairing only; none of these states implies a credential.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryAttachmentState {
    Unattached,
    BoundaryMissing,
    Unpaired,
    Paired,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryDescriptorState {
    Unavailable,
    Unapproved,
    Approved,
}

/// Scoped to the snapshot's attested connection, independently of native use.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryChildPolicyState {
    Disabled,
    Enabled,
    Mutating,
    Indeterminate,
}

/// Past local metadata, never credential presence, permission or a context revision.
/// Retain the original allocations; no getter loads secrets or consults a new owner.
pub(crate) struct RepositoryConnectionFacts {
    gate: GitlabCredentialGate,
    owner: Option<Arc<RepositoryOwner>>,
    directory: Arc<crate::repository_credentials::RepositoryConnectionDirectory>,
    attachment: RepositoryAttachmentState,
    approval: RepositoryDescriptorState,
    descriptor: Option<GitlabDescriptor>,
    lifecycle: crate::repository_credentials::RepositoryConnectionState,
    mutation: Option<crate::repository_credentials::RepositoryMutationKind>,
    preflight_pending: bool,
    settled: Option<RepositorySettledConnection>,
    unavailable: Option<Error>,
    backoff_until: Option<std::time::Instant>,
    child_policy: Option<(RepositoryChildPolicyState, u64)>,
}

struct ConnectionFactsView<'a> {
    config: Option<&'a intent_core::settings_file::GitlabSettings>,
    descriptor: Option<&'a GitlabDescriptor>,
    proof: Option<&'a AttestedSource>,
    metadata: crate::repository_credentials::read::RepositoryConnectionMetadata<'a>,
}

impl RepositoryConnectionFacts {
    pub(crate) fn attachment(&self) -> super::RepositoryAttachmentState {
        self.attachment
    }
    pub(crate) fn approval(&self) -> super::RepositoryDescriptorState {
        self.approval
    }
    pub(crate) fn descriptor(&self) -> Option<&GitlabDescriptor> {
        self.descriptor.as_ref()
    }
    /// Raw directory lifecycle: Ready without `settled()` is still unattested.
    pub(crate) fn lifecycle(&self) -> crate::repository_credentials::RepositoryConnectionState {
        self.lifecycle
    }
    pub(crate) fn mutation(&self) -> Option<crate::repository_credentials::RepositoryMutationKind> {
        self.mutation
    }
    /// A reservation has not changed the prior availability or proved an auth phase.
    pub(crate) fn preflight_pending(&self) -> bool {
        self.preflight_pending
    }
    pub(crate) fn settled(&self) -> Option<&RepositorySettledConnection> {
        self.settled.as_ref()
    }
    pub(crate) fn unavailable_reason(&self) -> Option<Error> {
        self.unavailable
    }
    /// Captured monotonic value, even after expiry. Elapsed time is not a revision.
    pub(crate) fn backoff_until(&self) -> Option<std::time::Instant> {
        self.backoff_until
    }
    pub(crate) fn child_policy(&self) -> Option<(super::RepositoryChildPolicyState, u64)> {
        self.child_policy
    }

    /// Only a future authentic sealed-zero R/ACP branch may call this entry.
    /// Absence of optional facts does not certify absence of required reads.
    /// The caller retains its fresh output fence and a single prebuilt packet.
    pub(crate) fn with_optional_current(
        optional: Option<&Self>,
        transfer: impl FnOnce(bool) -> Result<()> + Send,
    ) -> Result<()> {
        RepositoryReadEligibility::with_output(&[], optional, transfer)
    }

    /// Requires the genuine original prompt owner/capture and its original
    /// pending-ID, one-permit consuming boundary through the prebuilt transfer.
    /// This entry supplies no prompt provenance or permission; a prompt must
    /// not fabricate MCP-sealed-zero evidence. The boolean only selects the
    /// original optional payload, and the consuming action cannot be retried.
    pub(crate) fn with_prompt_current(
        optional: Option<&Self>,
        transfer: impl FnOnce(bool) -> Result<()> + Send,
    ) -> Result<()> {
        RepositoryReadEligibility::with_output(&[], optional, transfer)
    }

    /// Requires the original authenticated Wire/context owner and the original
    /// prepared response slot. This supplies no native caller provenance or grant.
    /// The caller must refuse a payload containing facts when this returns false;
    /// its consuming action never refetches, rebuilds or retries the packet.
    pub(crate) fn with_native_context_current(
        optional: Option<&Self>,
        transfer: impl FnOnce(bool) -> Result<()> + Send,
    ) -> Result<()> {
        RepositoryReadEligibility::with_output(&[], optional, transfer)
    }

    fn retained_owner(&self) -> Option<&Arc<RepositoryOwner>> {
        if matches!(
            self.attachment,
            RepositoryAttachmentState::Unattached | RepositoryAttachmentState::BoundaryMissing
        ) {
            // Neither OnceLock installer participates in the ranked lock set.
            // A later installation can never repair this captured absence.
            return None;
        }
        let owner = self.owner.as_ref()?;
        if !self
            .gate
            .repository
            .get()
            .is_some_and(|installed| Arc::ptr_eq(installed, owner))
            || !Arc::ptr_eq(&owner.directory, &self.directory)
        {
            return None;
        }
        owner.settings.get()?;
        Some(owner)
    }

    fn same_facts(&self, current: &Self) -> bool {
        let settled = match (&self.settled, &current.settled) {
            (Some(original), Some(current)) => {
                original.descriptor == current.descriptor && original.selected == current.selected
            }
            (None, None) => true,
            _ => false,
        };
        self.attachment == current.attachment
            && self.approval == current.approval
            && self.descriptor == current.descriptor
            && self.lifecycle == current.lifecycle
            && self.mutation == current.mutation
            && self.preflight_pending == current.preflight_pending
            && settled
            && self.unavailable == current.unavailable
            && self.backoff_until == current.backoff_until
            && self.child_policy == current.child_policy
    }

    fn observe(
        gate: &GitlabCredentialGate,
        directory: &Arc<crate::repository_credentials::RepositoryConnectionDirectory>,
    ) -> Result<Self> {
        let owner = gate.repository.get().cloned();
        let settings = owner.as_ref().and_then(|owner| owner.settings.get());
        Self::observe_captured(gate, directory, owner.as_ref(), settings)
    }

    fn observe_captured(
        gate: &GitlabCredentialGate,
        directory: &Arc<crate::repository_credentials::RepositoryConnectionDirectory>,
        owner: Option<&Arc<RepositoryOwner>>,
        settings: Option<&super::adoption::SettingsAttachment>,
    ) -> Result<Self> {
        if owner
            .as_ref()
            .is_some_and(|owner| !Arc::ptr_eq(&owner.directory, directory))
        {
            return Err(Error::BoundaryMismatch);
        }
        let config = settings
            .map(|s| s.config.lock().map_err(|_| Error::Indeterminate))
            .transpose()?;
        let descriptor = owner
            .as_ref()
            .map(|o| o.descriptor.lock().map_err(|_| Error::Indeterminate))
            .transpose()?;
        directory.with_connection_metadata(|metadata| {
            let proof = owner
                .as_ref()
                .map(|o| {
                    o.evidence
                        .published
                        .lock()
                        .map_err(|_| Error::Indeterminate)
                })
                .transpose()?;
            // A one-time attachment may have appeared while waiting for these
            // metadata locks. Never combine a captured absence with its later state.
            if gate.repository.get().map(Arc::as_ptr) != owner.map(Arc::as_ptr)
                || owner.and_then(|o| o.settings.get()).map(std::ptr::from_ref)
                    != settings.map(std::ptr::from_ref)
            {
                return Err(Error::Unverified);
            }
            Ok(Self::from_held(
                gate,
                directory,
                owner,
                settings,
                ConnectionFactsView {
                    config: config.as_deref(),
                    descriptor: descriptor.as_deref().and_then(Option::as_ref),
                    proof: proof.as_deref().and_then(Option::as_deref),
                    metadata,
                },
            ))
        })
    }

    fn from_held(
        gate: &GitlabCredentialGate,
        directory: &Arc<crate::repository_credentials::RepositoryConnectionDirectory>,
        owner: Option<&Arc<RepositoryOwner>>,
        settings: Option<&super::adoption::SettingsAttachment>,
        view: ConnectionFactsView<'_>,
    ) -> Self {
        let attachment = match (owner, settings) {
            (None, _) => RepositoryAttachmentState::Unattached,
            (_, None) => RepositoryAttachmentState::BoundaryMissing,
            (_, Some(s)) if s.source.is_some() && s.store.is_some() => {
                RepositoryAttachmentState::Paired
            }
            _ => RepositoryAttachmentState::Unpaired,
        };
        let approved = settings.zip(view.config).and_then(|(s, config)| {
            super::adoption::approved_descriptor(config, s.fixture.as_ref())
        });
        let approved = approved.filter(|approved| view.descriptor == Some(approved));
        let approval = if settings.is_none() {
            RepositoryDescriptorState::Unavailable
        } else if approved.is_some() {
            RepositoryDescriptorState::Approved
        } else {
            RepositoryDescriptorState::Unapproved
        };
        let checked = view.metadata.ready.and_then(|(actual, selected)| {
            if attachment != RepositoryAttachmentState::Paired
                || selected.source != RepositoryCredentialSource::GitlabSecretSlot
            {
                return Err(Error::Unverified);
            }
            let proof = view.proof.ok_or(Error::Unverified)?;
            if proof.request != selected {
                return Err(Error::SecretMismatch);
            }
            if approved.as_ref() != Some(actual) || proof.descriptor != *actual {
                return Err(Error::BoundaryMismatch);
            }
            Ok(RepositorySettledConnection {
                gate: gate.clone(),
                owner: owner.ok_or(Error::Unverified)?.clone(),
                descriptor: actual.clone(),
                selected,
            })
        });
        let unavailable = checked.as_ref().err().copied();
        let settled = checked.ok();
        let backoff_until = settled.as_ref().and(view.metadata.backoff_until);
        let child_policy = settled.as_ref().map(|_| {
            let state = match view.metadata.child_pending {
                Some(true) => RepositoryChildPolicyState::Indeterminate,
                Some(false) => RepositoryChildPolicyState::Mutating,
                None if view.metadata.child_enabled => RepositoryChildPolicyState::Enabled,
                None => RepositoryChildPolicyState::Disabled,
            };
            (state, view.metadata.child_revision)
        });
        Self {
            gate: gate.clone(),
            owner: owner.cloned(),
            directory: directory.clone(),
            attachment,
            approval,
            descriptor: approved,
            lifecycle: view.metadata.lifecycle,
            mutation: view.metadata.mutation,
            preflight_pending: view.metadata.reserved,
            settled,
            unavailable,
            backoff_until,
            child_policy,
        }
    }
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

    /// Every required original remains mandatory. The boolean only chooses the
    /// original prebuilt optional payload; it is not an authority or a snapshot.
    pub(crate) fn with_all_current_and_facts(
        required: &[&Self],
        optional: Option<&RepositoryConnectionFacts>,
        transfer: impl FnOnce(bool) -> Result<()> + Send,
    ) -> Result<()> {
        if required.is_empty() {
            return Err(Error::Unverified);
        }
        Self::with_output(required, optional, transfer)
    }

    fn with_output(
        originals: &[&Self],
        optional: Option<&RepositoryConnectionFacts>,
        transfer: impl FnOnce(bool) -> Result<()> + Send,
    ) -> Result<()> {
        let optional = optional.filter(|facts| facts.retained_owner().is_some());
        let mut owners = originals
            .iter()
            .map(|original| original.owner.clone())
            .chain(optional.and_then(|facts| facts.owner.clone()))
            .collect::<Vec<_>>();
        owners.sort_unstable_by_key(Arc::as_ptr);
        owners.dedup_by(|a, b| Arc::ptr_eq(a, b));
        let index = |owner: &Arc<RepositoryOwner>| {
            owners
                .binary_search_by_key(&Arc::as_ptr(owner), Arc::as_ptr)
                .expect("original owner retained")
        };
        let indices = originals
            .iter()
            .map(|original| index(&original.owner))
            .collect::<Vec<_>>();
        let optional_index = optional.and_then(|facts| facts.owner.as_ref()).map(index);
        let required = owners
            .iter()
            .map(|owner| originals.iter().any(|item| Arc::ptr_eq(owner, &item.owner)))
            .collect::<Vec<_>>();
        let scopes = originals.iter().map(|item| &item.scope).collect::<Vec<_>>();
        let batch =
            RepositoryReadScope::prepare_output(&scopes, optional.map(|facts| &facts.directory));
        let settings = owners
            .iter()
            .map(|owner| owner.settings.get().ok_or(Error::Unverified))
            .collect::<Result<Vec<_>>>()?;
        // The original installed attachments are immutable. All unique configs
        // precede all descriptors, directories and proofs, even across owners.
        let configs = settings
            .iter()
            .zip(&required)
            .map(|(settings, required)| output_lock(&settings.config, *required))
            .collect::<Result<Vec<_>>>()?;
        let descriptors = owners
            .iter()
            .zip(&required)
            .map(|(owner, required)| output_lock(&owner.descriptor, *required))
            .collect::<Result<Vec<_>>>()?;
        batch.with_current(|selected, metadata| {
            let proofs = owners
                .iter()
                .zip(&required)
                .map(|(owner, required)| output_lock(&owner.evidence.published, *required))
                .collect::<Result<Vec<_>>>()?;
            for ((original, index), selected) in originals.iter().zip(indices).zip(selected) {
                original.check_source(
                    settings[index],
                    configs[index].as_deref().expect("required config locked"),
                    descriptors[index].as_deref().and_then(Option::as_ref),
                    proofs[index].as_deref().and_then(Option::as_deref),
                    selected,
                )?;
            }
            let include = optional
                .zip(optional_index)
                .is_some_and(|(original, index)| {
                    let (Some(config), Some(descriptor), Some(proof), Some(metadata)) = (
                        configs[index].as_deref(),
                        descriptors[index].as_deref(),
                        proofs[index].as_deref(),
                        metadata,
                    ) else {
                        return false;
                    };
                    let current = RepositoryConnectionFacts::from_held(
                        &original.gate,
                        &original.directory,
                        original.owner.as_ref(),
                        Some(settings[index]),
                        ConnectionFactsView {
                            config: Some(config),
                            descriptor: descriptor.as_ref(),
                            proof: proof.as_deref(),
                            metadata,
                        },
                    );
                    original.same_facts(&current)
                });
            transfer(include)
        })
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

/// Required locks retain their existing blocking/error semantics. Additional
/// optional locks never block and never recover poisoned data.
fn output_lock<T>(
    mutex: &Mutex<T>,
    required: bool,
) -> Result<Option<std::sync::MutexGuard<'_, T>>> {
    if required {
        mutex.lock().map(Some).map_err(|_| Error::Indeterminate)
    } else {
        Ok(mutex.try_lock().ok())
    }
}

impl crate::Services {
    /// Metadata only, including non-ready states. No read grant or source I/O.
    pub(crate) fn gitlab_repository_connection_facts(
        &self,
    ) -> Result<super::RepositoryConnectionFacts> {
        RepositoryConnectionFacts::observe(
            &self.gitlab_credential_gate,
            &self.repository_connection_directory(),
        )
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
