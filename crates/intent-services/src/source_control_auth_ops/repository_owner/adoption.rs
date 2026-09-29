//! Settings publication and stored-source adoption share the original auth owner.
//! These objects contain metadata and original writer leases, never credentials.

use intent_core::settings_file::GitlabSettings;
use intent_core::{Error, FileSecretStore, Result};
use intent_sourcecontrol::{GitlabInstance, StoredCredential};
use serde_json::Value;

use super::secret_reader::load_material;
use super::{
    matches_host, Arc, GitlabCredentialGate, GitlabCredentialGuard, GitlabDescriptor, GitlabHost,
    GitlabWriteObserver, Mutex, PersistenceLease, RepositoryCredentialSource,
    RepositoryMutationKind, RepositoryWrite, SettledCredentialState, VerifiedRepositoryAccount,
};
use crate::settings::AsyncSecretStore;
use crate::settings_registry::{SettingsRegistry, SettingsSnapshot};

pub(crate) struct SettingsAttachment {
    pub(super) config: Mutex<GitlabSettings>,
    pub(super) fixture: Option<GitlabDescriptor>,
    pub(super) source: Option<std::path::PathBuf>,
    pub(super) store: Option<FileSecretStore>,
    source_descriptor: Mutex<Option<GitlabDescriptor>>,
}

/// One existing settings batch, including its compensation. It cannot be cloned
/// into a new owner, retargeted, serialized, or used with a different attachment.
pub(crate) struct RepositorySettingsWrite {
    write: Arc<RepositoryWrite>,
    original: GitlabSettings,
    desired: GitlabSettings,
    original_source: Option<GitlabDescriptor>,
    lease: PersistenceLease,
    secret_changed: std::sync::atomic::AtomicBool,
}

fn unavailable() -> Error {
    Error::Internal("repository credential settings require their original prepared owner".into())
}

/// Pure canonical identity validation. A transport override is NOT identity.
pub(crate) fn logical_instance(config: &GitlabSettings) -> Result<GitlabInstance> {
    let bare = super::super::parse_gitlab_host(&config.host)
        .map_err(|_| Error::InvalidParams("invalid sourceControl.gitlab.host".into()))?;
    let instance =
        GitlabInstance::parse(config.instance_base_url.as_deref().unwrap_or(&config.host))
            .map_err(|_| {
                Error::InvalidParams("invalid sourceControl.gitlab.instanceBaseUrl".into())
            })?;
    let logical = GitlabHost::parse(instance.as_str()).map_err(|_| unavailable())?;
    if logical.host() != bare.host() {
        return Err(Error::InvalidParams(
            "GitLab instanceBaseUrl and host must name the same authority".into(),
        ));
    }
    Ok(instance)
}

pub(super) fn approved_descriptor(
    config: &GitlabSettings,
    fixture: Option<&GitlabDescriptor>,
) -> Option<GitlabDescriptor> {
    let instance = logical_instance(config).ok()?;
    let logical = GitlabHost::parse(instance.as_str()).ok()?;
    let endpoint = config
        .api_base_url
        .as_deref()
        .filter(|s| !s.trim().is_empty());
    let transport = endpoint.map_or_else(
        || Some(logical.clone()),
        |url| logical.clone().with_api_origin(url).ok(),
    )?;
    if transport.base_url() == logical.base_url() {
        return Some(GitlabDescriptor::new(instance));
    }
    fixture
        .filter(|d| d.instance() == &instance && matches_host(d, &transport))
        .cloned()
}

impl GitlabCredentialGate {
    pub(crate) fn has_settings_boundary(&self) -> bool {
        self.repository
            .get()
            .is_some_and(|owner| owner.settings.get().is_some())
    }

    /// Install before adoption. The directory and attachment are never replaced.
    /// Pairing is established by the settings wrapper's explicit file constructor;
    /// arbitrary injected stores, even with similar contents, cannot attest it.
    pub(crate) fn install_settings_boundary(
        &self,
        registry: &SettingsRegistry,
        secrets: &AsyncSecretStore,
        store: &FileSecretStore,
        fixture: Option<GitlabDescriptor>,
    ) -> Result<()> {
        let owner = self.repository.get().ok_or_else(unavailable)?;
        let config = registry.snapshot().effective.source_control.gitlab.clone();
        let descriptor = approved_descriptor(&config, fixture.as_ref());
        let source = secrets
            .is_paired_gitlab_store(store)
            .then(|| store.path().to_path_buf());
        owner
            .settings
            .set(SettingsAttachment {
                config: Mutex::new(config),
                fixture,
                store: source.as_ref().map(|_| store.clone()),
                source,
                source_descriptor: Mutex::new(descriptor.clone()),
            })
            .map_err(|_| unavailable())?;
        *owner.descriptor.lock().map_err(|_| unavailable())? = descriptor;
        registry.install_repository_boundary(self.clone())?;
        secrets.install_repository_boundary(self.clone())?;
        Ok(())
    }

    /// Called after ordinary validation under credential -> secret -> revision
    /// gates. Reservations do not retire anything until a real effect begins.
    pub(crate) fn prepare_settings(
        &self,
        registry: &SettingsRegistry,
        candidate: &SettingsSnapshot,
        guard: &GitlabCredentialGuard,
    ) -> Result<Arc<RepositorySettingsWrite>> {
        if !Arc::ptr_eq(&self.mutex, &guard.mutex) {
            return Err(unavailable());
        }
        let owner = self.repository.get().ok_or_else(unavailable)?;
        let original = registry.snapshot().effective.source_control.gitlab.clone();
        let desired = candidate.effective.source_control.gitlab.clone();
        let settings = owner.settings.get().ok_or_else(unavailable)?;
        if *settings.config.lock().map_err(|_| unavailable())? != original {
            return Err(unavailable());
        }
        let reservation = owner
            .writers
            .reserve(RepositoryMutationKind::Replace)
            .map_err(|_| unavailable())?;
        let descriptor = approved_descriptor(&desired, settings.fixture.as_ref());
        let write = Self::new_write(
            owner,
            descriptor,
            reservation,
            RepositoryMutationKind::Replace,
        );
        let original_source = settings
            .source_descriptor
            .lock()
            .map_err(|_| unavailable())?
            .clone();
        Ok(Arc::new(RepositorySettingsWrite {
            write,
            original,
            desired,
            original_source,
            lease: guard.lease(),
            secret_changed: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    pub(crate) fn before_settings_publication(
        &self,
        old: &SettingsSnapshot,
        new: &SettingsSnapshot,
        write: Option<&RepositorySettingsWrite>,
        auth: Option<&RepositoryWrite>,
    ) -> Result<()> {
        let old = &old.effective.source_control.gitlab;
        let new = &new.effective.source_control.gitlab;
        if old == new {
            return Ok(());
        }
        logical_instance(new)?;
        let owner = self.repository.get().ok_or_else(unavailable)?;
        if let Some(write) = write {
            if !Arc::ptr_eq(owner, &write.write.owner)
                || !((old == &write.original && new == &write.desired)
                    || (old == &write.desired && new == &write.original))
            {
                return Err(unavailable());
            }
            return write.write.begin().map_err(crate::pr_ops::map_sc_err);
        }
        if let Some(auth) = auth {
            if !Arc::ptr_eq(owner, &auth.owner) {
                return Err(unavailable());
            }
            // The successful auth owner may publish only its captured root.
            let settings = owner.settings.get().ok_or_else(unavailable)?;
            if auth.descriptor.as_ref()
                != approved_descriptor(new, settings.fixture.as_ref()).as_ref()
                || auth.descriptor.is_none()
            {
                return Err(unavailable());
            }
            return auth.begin().map_err(crate::pr_ops::map_sc_err);
        }
        Err(unavailable())
    }

    pub(crate) fn settings_published(&self, new: &SettingsSnapshot) {
        let Some(owner) = self.repository.get() else {
            return;
        };
        let Some(settings) = owner.settings.get() else {
            return;
        };
        let next = new.effective.source_control.gitlab.clone();
        if let (Ok(mut config), Ok(mut descriptor), Ok(mut source)) = (
            settings.config.lock(),
            owner.descriptor.lock(),
            settings.source_descriptor.lock(),
        ) {
            let new_descriptor = approved_descriptor(&next, settings.fixture.as_ref());
            if *descriptor != new_descriptor {
                *source = None;
            }
            *config = next;
            *descriptor = new_descriptor;
        }
    }

    pub(crate) fn check_settings_secret(
        &self,
        write: Option<&RepositorySettingsWrite>,
    ) -> Result<()> {
        let owner = self.repository.get().ok_or_else(unavailable)?;
        let write = write.ok_or_else(unavailable)?;
        if !Arc::ptr_eq(owner, &write.write.owner) {
            return Err(unavailable());
        }
        write
            .write
            .before_write()
            .map_err(crate::pr_ops::map_sc_err)?;
        write
            .secret_changed
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    fn stored_source_host(
        &self,
        registry: &SettingsRegistry,
        store: &FileSecretStore,
        allow_new: bool,
    ) -> Result<GitlabHost> {
        let owner = self.repository.get().ok_or_else(unavailable)?;
        let settings = owner.settings.get().ok_or_else(unavailable)?;
        if settings.source.as_deref() != Some(store.path()) {
            return Err(unavailable());
        }
        let current = registry.snapshot().effective.source_control.gitlab.clone();
        if *settings.config.lock().map_err(|_| unavailable())? != current {
            return Err(unavailable());
        }
        let descriptor =
            approved_descriptor(&current, settings.fixture.as_ref()).ok_or_else(unavailable)?;
        if !allow_new
            && settings
                .source_descriptor
                .lock()
                .map_err(|_| unavailable())?
                .as_ref()
                != Some(&descriptor)
        {
            return Err(unavailable());
        }
        let mut host =
            GitlabHost::parse(descriptor.instance().as_str()).map_err(crate::pr_ops::map_sc_err)?;
        let override_url = current
            .api_base_url
            .clone()
            .or_else(|| std::env::var(super::super::GITLAB_API_BASE_URI_ENV).ok());
        if let Some(endpoint) = override_url.filter(|v| !v.trim().is_empty()) {
            host = host
                .with_api_origin(&endpoint)
                .map_err(crate::pr_ops::map_sc_err)?;
        }
        if !matches_host(&descriptor, &host) {
            return Err(unavailable());
        }
        Ok(host)
    }
}

impl RepositorySettingsWrite {
    pub(crate) fn lease(&self) -> PersistenceLease {
        self.lease.clone()
    }
    pub(crate) fn began(&self) -> bool {
        self.write.state.lock().is_ok_and(|s| s.mutation.is_some())
    }
    pub(crate) fn indeterminate(&self) {
        if let Ok(state) = self.write.state.lock() {
            if let Some(mutation) = &state.mutation {
                let _ = mutation.completion().indeterminate();
            }
        }
    }
}

impl crate::Services {
    /// Conservative gate classification only; actual no-op/effect detection
    /// remains in the existing settings writer after placeholder/sibling checks.
    pub(crate) fn repository_settings_relevant(changes: &Value) -> bool {
        changes.as_array().is_some_and(|entries| {
            entries.iter().any(|entry| {
                entry
                    .get("path")
                    .and_then(Value::as_str)
                    .is_some_and(|path| path.starts_with("sourceControl.gitlab."))
            })
        })
    }

    pub(crate) fn prepare_gitlab_repository_settings(
        &self,
        changes: &Value,
        guard: Option<&GitlabCredentialGuard>,
    ) -> Result<Option<Arc<RepositorySettingsWrite>>> {
        if !Self::repository_settings_relevant(changes)
            || !self.gitlab_credential_gate.has_settings_boundary()
        {
            return Ok(None);
        }
        let guard = guard.ok_or_else(unavailable)?;
        let planned = self.settings_service().validate_update(changes)?;
        let registry = self.settings_registry.as_deref().ok_or_else(unavailable)?;
        let ordinary = planned
            .iter()
            .filter(|(def, _)| crate::settings_registry::KNOWN_PATHS.contains(&def.path))
            .map(|(def, value)| (def.path.into(), crate::settings::registry_value(def, value)))
            .collect::<Vec<_>>();
        let candidate = registry.preview(&ordinary)?;
        self.gitlab_credential_gate
            .prepare_settings(registry, &candidate, guard)
            .map(Some)
    }

    pub(crate) fn prepare_gitlab_repository_reset(
        &self,
        path: &str,
        guard: Option<&GitlabCredentialGuard>,
    ) -> Result<Option<Arc<RepositorySettingsWrite>>> {
        if !path.starts_with("sourceControl.gitlab.")
            || !self.gitlab_credential_gate.has_settings_boundary()
        {
            return Ok(None);
        }
        let guard = guard.ok_or_else(unavailable)?;
        let registry = self.settings_registry.as_deref().ok_or_else(unavailable)?;
        let def = crate::settings::find_definition(path).ok_or_else(unavailable)?;
        let changes = if def.sensitive {
            Vec::new()
        } else {
            vec![(path.into(), Value::Null)]
        };
        let candidate = registry.preview(&changes)?;
        self.gitlab_credential_gate
            .prepare_settings(registry, &candidate, guard)
            .map(Some)
    }

    /// Existing owners report the outcome of their complete original batch.
    /// An unknown or failed compensation never publishes usable metadata.
    pub(crate) async fn settle_gitlab_repository_settings(
        &self,
        write: Option<&RepositorySettingsWrite>,
        settled: bool,
        compensated: bool,
    ) {
        let Some(write) = write else {
            return;
        };
        if !settled {
            write.indeterminate();
            return;
        }
        if let Err(error) = self
            .finish_gitlab_repository_settings(write, compensated)
            .await
        {
            write.indeterminate();
            tracing::debug!(%error, "repository settings remain unverified after settlement");
        }
    }

    /// Boot initializer. Installation itself leaves the directory Unverified.
    /// No native provider or credential-reader is activated by this method.
    ///
    /// # Errors
    /// Returns an error when the original settings/store attachment or verified
    /// instance and account evidence is unavailable.
    pub async fn initialize_gitlab_repository_binding(&self) -> Result<()> {
        let guard = self.gitlab_credential_gate.lock().await;
        let registry = self.settings_registry.as_deref().ok_or_else(unavailable)?;
        self.gitlab_credential_gate.install_settings_boundary(
            registry,
            &self.secrets,
            &self.gitlab_secret_store,
            None,
        )?;
        self.reconcile_gitlab_repository_binding_locked(&guard)
            .await
    }

    /// Explicit cold test composition on the original paired settings and store.
    /// This is one-time installation, not a runtime rebind or production grant.
    ///
    /// # Errors
    /// Refuses mismatched descriptors, unavailable sources or repeated installation.
    #[doc(hidden)]
    #[cfg(any(test, feature = "repository-test-fixtures"))]
    pub async fn initialize_repository_test_fixture(
        &self,
        fixture: GitlabDescriptor,
    ) -> Result<()> {
        let guard = self.gitlab_credential_gate.lock().await;
        let registry = self.settings_registry.as_deref().ok_or_else(unavailable)?;
        self.gitlab_credential_gate.install_settings_boundary(
            registry,
            &self.secrets,
            &self.gitlab_secret_store,
            Some(fixture),
        )?;
        self.reconcile_gitlab_repository_binding_locked(&guard)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn reconcile_gitlab_repository_binding(&self) -> Result<()> {
        let guard = self.gitlab_credential_gate.lock().await;
        self.reconcile_gitlab_repository_binding_locked(&guard)
            .await
    }

    async fn reconcile_gitlab_repository_binding_locked(
        &self,
        guard: &GitlabCredentialGuard,
    ) -> Result<()> {
        let registry = self.settings_registry.as_deref().ok_or_else(unavailable)?;
        let host = self.gitlab_credential_gate.stored_source_host(
            registry,
            &self.gitlab_secret_store,
            false,
        )?;
        let credential =
            intent_sourcecontrol::gitlab_auth::stored_credential(self.gitlab_secret_store.clone())
                .await
                .map_err(crate::pr_ops::map_sc_err)?;
        if credential == StoredCredential::None {
            return Ok(());
        }
        if credential.needs_refresh() {
            let write = self
                .gitlab_credential_gate
                .reserve(&host, RepositoryMutationKind::Refresh)
                .map_err(crate::pr_ops::map_sc_err)?;
            let client_id = intent_sourcecontrol::gitlab_auth::resolve_client_id(
                &registry
                    .snapshot()
                    .effective
                    .source_control
                    .gitlab
                    .oauth_client_id,
                &host,
            );
            return match super::super::try_refresh(
                &host,
                client_id.as_deref(),
                self.gitlab_secret_store.clone(),
                guard,
                write.clone(),
            )
            .await
            {
                Ok(()) => {
                    if let Some(write) = write {
                        write.confirm_publication(true);
                    }
                    Ok(())
                }
                Err(super::super::RefreshFailure::Local(error)) => {
                    Err(crate::pr_ops::map_sc_err(error))
                }
                Err(_) => Err(unavailable()),
            };
        }
        let write = self
            .gitlab_credential_gate
            .reserve(&host, RepositoryMutationKind::Replace)
            .map_err(crate::pr_ops::map_sc_err)?
            .ok_or_else(unavailable)?;
        self.verify_stored_repository_account(&host, &write, None, guard.lease())
            .await
    }

    /// Run after the existing full batch (or settled compensation), outside the
    /// revision gate. Failure never rewrites/deletes credentials or invents Ready.
    pub(crate) async fn finish_gitlab_repository_settings(
        &self,
        write: &RepositorySettingsWrite,
        compensated: bool,
    ) -> Result<()> {
        if !write.began() {
            return Ok(());
        }
        let registry = self.settings_registry.as_deref().ok_or_else(unavailable)?;
        let current = registry.snapshot().effective.source_control.gitlab.clone();
        let settings = write.write.owner.settings.get().ok_or_else(unavailable)?;
        let restored_source = compensated
            && current == write.original
            && write.original_source.as_ref()
                == approved_descriptor(&current, settings.fixture.as_ref()).as_ref()
            && write.original_source.is_some();
        let allow_new = restored_source
            || (!compensated
                && current == write.desired
                && write
                    .secret_changed
                    .load(std::sync::atomic::Ordering::Acquire));
        let host = self.gitlab_credential_gate.stored_source_host(
            registry,
            &self.gitlab_secret_store,
            allow_new,
        )?;
        self.verify_stored_repository_account(&host, &write.write, Some(compensated), write.lease())
            .await
    }

    async fn verify_stored_repository_account(
        &self,
        host: &GitlabHost,
        write: &RepositoryWrite,
        compensated: Option<bool>,
        lease: PersistenceLease,
    ) -> Result<()> {
        let registry = self.settings_registry.as_deref().ok_or_else(unavailable)?;
        let snapshot = registry.snapshot();
        let settings = write.owner.settings.get().ok_or_else(unavailable)?;
        let store = settings.store.clone().ok_or_else(unavailable)?;
        let material = load_material(store.clone(), lease.clone())
            .await
            .map_err(|_| unavailable())?;
        let token = match material.access() {
            Ok(token) => token.trim(),
            Err(super::RepositoryCredentialError::Missing) => {
                if compensated.is_some() {
                    write.begin().map_err(crate::pr_ops::map_sc_err)?;
                    write.complete_settings(SettledCredentialState::Disconnected)?;
                }
                return Ok(());
            }
            Err(_) => return Err(unavailable()),
        };
        let fingerprint = material.fingerprint(&write.owner.evidence);
        let user = intent_sourcecontrol::gitlab_auth::validate_pat(host, token)
            .await
            .map_err(crate::pr_ops::map_sc_err)?;
        let after = load_material(store, lease)
            .await
            .map_err(|_| unavailable())?;
        if snapshot.effective.source_control.gitlab
            != registry.snapshot().effective.source_control.gitlab
            || after.fingerprint(&write.owner.evidence) != fingerprint
        {
            return Err(unavailable());
        }
        let settings = write.owner.settings.get().ok_or_else(unavailable)?;
        let descriptor = approved_descriptor(
            &snapshot.effective.source_control.gitlab,
            settings.fixture.as_ref(),
        )
        .ok_or_else(unavailable)?;
        let account = VerifiedRepositoryAccount::from_verified_user(
            descriptor.clone(),
            user.id,
            RepositoryCredentialSource::GitlabSecretSlot,
        )
        .map_err(|_| unavailable())?;
        write.begin().map_err(crate::pr_ops::map_sc_err)?;
        let binding = write
            .complete_settings(if compensated == Some(true) {
                SettledCredentialState::Compensated(account)
            } else {
                SettledCredentialState::Verified(account)
            })?
            .ok_or_else(unavailable)?;
        write
            .owner
            .publish_source(&binding, &descriptor, fingerprint)
            .map_err(|_| unavailable())?;
        *settings
            .source_descriptor
            .lock()
            .map_err(|_| unavailable())? = Some(descriptor);
        Ok(())
    }
}

impl RepositoryWrite {
    fn complete_settings(
        &self,
        outcome: SettledCredentialState,
    ) -> Result<Option<crate::repository_credentials::RepositoryConnectionBinding>> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        state
            .mutation
            .as_ref()
            .ok_or_else(unavailable)?
            .completion()
            .complete(outcome)
            .map_err(|_| unavailable())
    }
}

#[cfg(test)]
mod tests;
