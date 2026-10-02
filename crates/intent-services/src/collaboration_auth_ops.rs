//! Collaboration credentials live in private, primary/provider/instance-specific
//! files. Repository resolvers never receive these stores. OAuth/PAT and proof
//! HTTP engines are shared; their credential lifecycle and events are not.
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    now_iso, Error, FileSecretStore, IdentityProofErrorKind, PrincipalIdentity, Result, WorkspaceId,
};
use intent_sourcecontrol::device_flow::{GithubExchange, GithubGrant};
use intent_sourcecontrol::gitlab_auth::{self, GitlabExchange, GitlabGrant, PersistenceLease};
use intent_sourcecontrol::identity_proof::provider::{CreatedProof, ProofProvider};
use intent_sourcecontrol::{DeviceFlow, GitlabDeviceFlow, StoredCredential};
use intent_store::NewEvent;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::github_auth_ops::{self, FlowPhase, FlowSlot};
use crate::principal_ops::ForgeUser;
use crate::source_control_auth_ops::{self as repository, Provider, Target};
use crate::{publish_event, system_actor, Services};

const ACCOUNT: &str = "identity.account";
const IO_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) type AuthState = Arc<Mutex<HashMap<String, Arc<Credential>>>>;

pub(crate) struct Credential {
    store: FileSecretStore,
    secrets: Arc<crate::settings::AsyncSecretStore>,
    gate: Arc<Mutex<()>>,
    state: Mutex<State>,
    #[cfg(test)]
    sleep_pending: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    #[cfg(test)]
    commit_test_lease: std::sync::Mutex<Option<PersistenceLease>>,
}

enum CredentialRequest {
    Status {
        only_user: bool,
    },
    Select {
        external_id: String,
    },
    ProofCreate {
        nonce: String,
        label: String,
        expected: PrincipalIdentity,
    },
    ProofDelete {
        proof_id: String,
    },
}

struct CredentialOperation<'a> {
    entry: &'a Credential,
    target: &'a Target,
    lease: PersistenceLease,
    secrets: &'a crate::settings::AsyncSecretStore,
    generation: u64,
}

enum CredentialReply {
    Value(Value),
    Proof(ProofProvider, CreatedProof),
}

#[derive(Default)]
struct State {
    generation: u64,
    flow: Option<FlowSlot>,
    flow_id: String,
    unsupported: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Account {
    identity: PrincipalIdentity,
    user: Value,
    display_name: Option<String>,
    generation: String,
    method: String,
    scopes: Option<Vec<String>>,
    #[serde(default)]
    gitlab_binding: Option<GitlabBinding>,
}

/// The endpoint and public application that actually authorized this credential.
/// Repository settings are consulted only when starting a new sign-in.
#[derive(Clone, Serialize, Deserialize)]
struct GitlabBinding {
    base_url: String,
    client_id: Option<String>,
}

impl Account {
    fn new(
        user: ForgeUser,
        wire: Value,
        method: &str,
        scopes: Option<Vec<String>>,
    ) -> Result<Self> {
        Ok(Self {
            identity: user.identity.ok_or(Error::IdentityMismatch)?,
            user: wire,
            display_name: user.display_name,
            generation: uuid::Uuid::new_v4().to_string(),
            method: method.to_string(),
            scopes,
            gitlab_binding: None,
        })
    }

    fn device_client_id(&self) -> Result<&str> {
        self.gitlab_binding
            .as_ref()
            .and_then(|binding| binding.client_id.as_deref())
            .filter(|id| !id.trim().is_empty())
            .ok_or(Error::IdentityMismatch)
    }

    fn bound_target(&self, requested: &Target) -> Result<Target> {
        if self.identity.provider != requested.provider().as_wire()
            || self.identity.host != requested.host()
        {
            return Err(Error::IdentityMismatch);
        }
        match requested {
            Target::Github => Ok(Target::Github),
            Target::Gitlab { host } => {
                let binding = self
                    .gitlab_binding
                    .as_ref()
                    .ok_or(Error::IdentityMismatch)?;
                match self.method.as_str() {
                    "device" => {
                        self.device_client_id()?;
                    }
                    "pat" => {}
                    _ => return Err(Error::IdentityMismatch),
                }
                Ok(Target::Gitlab {
                    host: host
                        .clone()
                        .with_api_origin(&binding.base_url)
                        .map_err(|_| Error::IdentityMismatch)?,
                })
            }
        }
    }
}

impl Target {
    fn provider(&self) -> Provider {
        match self {
            Self::Github => Provider::Github,
            Self::Gitlab { .. } => Provider::Gitlab,
        }
    }
    fn host(&self) -> &str {
        match self {
            Self::Github => "github.com",
            Self::Gitlab { host } => host.host(),
        }
    }
    fn token_account(&self) -> &'static str {
        match self {
            Self::Github => github_auth_ops::SECRET_ACCOUNT,
            Self::Gitlab { .. } => intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
        }
    }
    fn not_connected(&self) -> Error {
        Error::IdentityProof(match self {
            Self::Github => IdentityProofErrorKind::NotConnected,
            Self::Gitlab { .. } => IdentityProofErrorKind::GitlabNotConnected,
        })
    }
    fn scopes(&self) -> &'static [&'static str] {
        match self {
            Self::Github => intent_sourcecontrol::device_flow::COLLABORATION_SCOPES,
            Self::Gitlab { .. } => gitlab_auth::DEVICE_GRANT_SCOPES,
        }
    }
}

async fn io<T: Send + 'static>(
    store: &FileSecretStore,
    lease: PersistenceLease,
    f: impl FnOnce(FileSecretStore) -> Result<T> + Send + 'static,
) -> Result<T> {
    let store = store.clone();
    let job = tokio::task::spawn_blocking(move || {
        let _lease = lease;
        f(store)
    });
    tokio::time::timeout(IO_TIMEOUT, job)
        .await
        .map_err(|_| Error::Internal("collaboration credential IO timed out".into()))?
        .map_err(|_| Error::Internal("collaboration credential IO failed".into()))?
}

fn account(store: &FileSecretStore) -> Result<Option<Account>> {
    store
        .load(ACCOUNT)?
        .map(|s| serde_json::from_str(&s).map_err(|_| Error::IdentityMismatch))
        .transpose()
}

async fn save_account(
    secrets: &crate::settings::AsyncSecretStore,
    account: &Account,
) -> Result<()> {
    let data = serde_json::to_string(account).map_err(|e| Error::Internal(e.to_string()))?;
    secrets.store(ACCOUNT, &data).await
}

async fn clear(secrets: &crate::settings::AsyncSecretStore, target: &Target) -> Result<bool> {
    let token_key = target.token_account();
    let present = secrets.load_fresh(token_key).await?.is_some();
    for key in [
        token_key,
        ACCOUNT,
        intent_sourcecontrol::gitlab_token::REFRESH_SECRET_ACCOUNT,
        intent_sourcecontrol::gitlab_token::EXPIRES_AT_SECRET_ACCOUNT,
    ] {
        secrets.delete(key).await?;
    }
    Ok(present)
}

fn flow_response(state: &State) -> Value {
    let mut result = github_auth_ops::connect_response(state.flow.as_ref().expect("resident flow"));
    result["purpose"] = json!("collaboration");
    result["flowId"] = json!(state.flow_id);
    result
}

impl Services {
    fn collaboration_target(provider: &str, host: Option<&str>) -> Result<Target> {
        let kind = Provider::parse(provider)?;
        repository::resolve_target(kind, host, "gitlab.com", None)
    }

    fn collaboration_connect_target(&self, provider: &str, host: Option<&str>) -> Result<Target> {
        let mut target = Self::collaboration_target(provider, host)?;
        if let Target::Gitlab { host } = &mut target {
            // A configured API override belongs only to its bound instance.
            // The environment seam supports the hosted-instance hermetic tests.
            let settings = self.effective_settings().source_control.gitlab;
            let origin = if self.gitlab_host_is_bound(host) {
                settings.api_base_url.filter(|s| !s.trim().is_empty())
            } else {
                None
            }
            .or_else(|| {
                (host.host() == "gitlab.com")
                    .then(|| std::env::var(repository::GITLAB_API_BASE_URI_ENV).ok())
                    .flatten()
            });
            if let Some(origin) = origin {
                *host = host
                    .clone()
                    .with_api_origin(&origin)
                    .map_err(crate::pr_ops::map_sc_err)?;
            }
        }
        Ok(target)
    }

    fn collaboration_client_id(&self, target: &Target) -> Option<String> {
        match target {
            Target::Github => Some(
                self.effective_settings()
                    .source_control
                    .github
                    .oauth_client_id,
            ),
            Target::Gitlab { host } if self.gitlab_host_is_bound(host) => {
                self.gitlab_client_id(host)
            }
            Target::Gitlab { host } => gitlab_auth::resolve_client_id("", host),
        }
    }

    async fn collaboration_credential(&self, target: &Target) -> Result<Arc<Credential>> {
        let primary = self.store.get_primary_principal().await?;
        let key = format!(
            "{}\0{}\0{}\0collaboration",
            primary.id,
            target.provider().as_wire(),
            target.host()
        );
        let digest = Sha256::digest(key.as_bytes()).iter().fold(
            String::with_capacity(64),
            |mut out, byte| {
                let _ = write!(out, "{byte:02x}");
                out
            },
        );
        let mut entries = self.collaboration_auth.lock().await;
        Ok(entries
            .entry(key)
            .or_insert_with(|| {
                let mut directory = self.gitlab_secret_store.path().as_os_str().to_os_string();
                directory.push(".collaboration");
                let store = FileSecretStore::with_path(
                    std::path::PathBuf::from(directory).join(format!("{digest}.json")),
                );
                Arc::new(Credential {
                    secrets: Arc::new(crate::settings::AsyncSecretStore::new(Arc::new(
                        store.clone(),
                    ))),
                    store,
                    gate: Arc::new(Mutex::new(())),
                    state: Mutex::new(State::default()),
                    #[cfg(test)]
                    sleep_pending: std::sync::Mutex::new(None),
                    #[cfg(test)]
                    commit_test_lease: std::sync::Mutex::new(None),
                })
            })
            .clone())
    }

    async fn collaboration_event(&self, target: &Target, status: &str, flow_id: Option<&str>) {
        let mut data = json!({"provider":target.provider().as_wire(),"host":target.host(),"purpose":"collaboration","status":status});
        if let Some(id) = flow_id {
            data["flowId"] = json!(id);
        }
        publish_event(
            self.event_bus.as_ref(),
            NewEvent {
                workspace_id: WorkspaceId::from_string(String::new()),
                timestamp: now_iso(),
                event_type: intent_core::events::IDENTITY_AUTH_CHANGED.to_string(),
                actor: system_actor(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data,
            },
        )
        .await;
    }

    async fn collaboration_verify(
        &self,
        target: &Target,
        token: &str,
        method: &str,
    ) -> Result<Account> {
        match target {
            Target::Github => {
                let base =
                    crate::invite_ops::resolve_api_base_uri(self.github_api_base_uri.as_deref());
                let (user, scopes) =
                    intent_sourcecontrol::github::GitHubSourceControl::new(token, base.as_deref())
                        .map_err(crate::pr_ops::map_sc_err)?
                        .get_user_with_scopes()
                        .await
                        .map_err(|e| map_auth_error(target, e))?;
                check_scopes(target, scopes.as_deref())?;
                Account::new(
                    ForgeUser::github(&user),
                    repository::github_user_to_wire(&user),
                    method,
                    scopes,
                )
            }
            Target::Gitlab { host } => {
                let (user, scopes) = gitlab_auth::validate_pat_with_scopes(host, token)
                    .await
                    .map_err(|e| map_auth_error(target, e))?;
                let mut account = Account::new(
                    ForgeUser::gitlab(host.host(), &user),
                    repository::gitlab_user_to_wire(&user),
                    method,
                    scopes,
                )?;
                account.gitlab_binding = Some(GitlabBinding {
                    base_url: host.base_url().into(),
                    client_id: None,
                });
                Ok(account)
            }
        }
    }

    async fn collaboration_refresh(
        &self,
        entry: &Credential,
        lease: PersistenceLease,
        target: &Target,
        client_id: &str,
        secrets: &crate::settings::AsyncSecretStore,
    ) -> Result<bool> {
        let Target::Gitlab { host } = target else {
            return Ok(false);
        };
        match gitlab_auth::refresh_grant(host, client_id, entry.store.clone()).await {
            Ok(grant) => {
                let scopes = grant.granted_scopes().map(<[String]>::to_vec);
                secrets.persist_gitlab_grant(grant, lease.clone()).await?;
                if let Some(mut saved) = io(&entry.store, lease.clone(), |s| account(&s)).await? {
                    saved.scopes = scopes;
                    save_account(secrets, &saved).await?;
                }
                Ok(true)
            }
            Err(intent_sourcecontrol::Error::Auth(_)) => {
                clear(secrets, target).await?;
                self.collaboration_event(target, "expired", None).await;
                Ok(false)
            }
            Err(e) => Err(crate::pr_ops::map_sc_err(e)),
        }
    }

    // Caller holds this credential's gate. Refresh cannot race revoke, reauth or
    // another refresh; no repository/env/CLI path is consulted here.
    async fn collaboration_probe(
        &self,
        entry: &Credential,
        lease: PersistenceLease,
        target: &Target,
        secrets: &crate::settings::AsyncSecretStore,
    ) -> Result<Option<(Account, String, Target)>> {
        let Some(mut saved) = io(&entry.store, lease.clone(), |s| account(&s)).await? else {
            return Ok(None);
        };
        // Restore and validate the original binding before reading/sending a
        // token. Missing metadata needs explicit reauthorization, never a guess
        // from mutable repository configuration or destructive expiry handling.
        let target = saved.bound_target(target)?;
        let credential = if matches!(target, Target::Gitlab { .. }) {
            gitlab_auth::stored_credential(entry.store.clone())
                .await
                .map_err(crate::pr_ops::map_sc_err)?
        } else {
            StoredCredential::Pat
        };
        let refreshed = if credential.needs_refresh() {
            if !self
                .collaboration_refresh(
                    entry,
                    lease.clone(),
                    &target,
                    saved.device_client_id()?,
                    secrets,
                )
                .await?
            {
                return Ok(None);
            }
            saved = io(&entry.store, lease.clone(), |s| account(&s))
                .await?
                .ok_or_else(|| target.not_connected())?;
            true
        } else {
            false
        };
        for attempt in 0..2 {
            let key = target.token_account();
            let Some(token) = io(&entry.store, lease.clone(), move |s| s.load(key)).await? else {
                return Ok(None);
            };
            match self
                .collaboration_verify(&target, &token, &saved.method)
                .await
            {
                Ok(observed) => {
                    if observed.identity != saved.identity {
                        return Err(Error::IdentityMismatch);
                    }
                    saved.user = observed.user;
                    saved.display_name = observed.display_name;
                    // A fresh provider report wins; otherwise retain the actual
                    // grant observed during authorization, never requested scopes.
                    saved.scopes = observed.scopes.or(saved.scopes);
                    check_scopes(&target, saved.scopes.as_deref())?;
                    save_account(secrets, &saved).await?;
                    return Ok(Some((saved, token, target)));
                }
                Err(Error::SourceControlUnauthorized { .. })
                    if attempt == 0
                        && !refreshed
                        && matches!(credential, StoredCredential::Device { .. }) =>
                {
                    if !self
                        .collaboration_refresh(
                            entry,
                            lease.clone(),
                            &target,
                            saved.device_client_id()?,
                            secrets,
                        )
                        .await?
                    {
                        return Ok(None);
                    }
                    saved = io(&entry.store, lease.clone(), |s| account(&s))
                        .await?
                        .ok_or_else(|| target.not_connected())?;
                }
                Err(Error::SourceControlUnauthorized { .. }) => {
                    clear(secrets, &target).await?;
                    self.collaboration_event(&target, "expired", None).await;
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    async fn collaboration_request(
        &self,
        provider: &str,
        host: Option<&str>,
        request: CredentialRequest,
    ) -> Result<CredentialReply> {
        let target = Self::collaboration_target(provider, host)?;
        let entry = self.collaboration_credential(&target).await?;
        let caller = intent_core::current_caller()
            .ok_or_else(|| Error::Internal("credential caller missing".into()))?;
        let credential = intent_core::caller::current_wire_credential();
        let expired = Arc::new(tokio::sync::Notify::new());
        let secrets = entry.secrets.settled_operation(expired.clone());
        let service = self.clone();
        let (response, receiver) = tokio::sync::oneshot::channel();
        let owner = async move {
            // Reserve a new explicit identity choice before waiting for this
            // credential. Only admitted roots may invalidate an older choice.
            let generation = if matches!(request, CredentialRequest::Select { .. }) {
                service
                    .identity_rekey_generation
                    .fetch_add(1, Ordering::SeqCst)
                    + 1
            } else {
                service.identity_rekey_generation.load(Ordering::SeqCst)
            };
            let lease: PersistenceLease = Arc::new(entry.gate.clone().lock_owned().await);
            let worker_lease = lease.clone();
            let worker_secrets = secrets.clone();
            let mut worker = intent_core::spawn_daemon(intent_core::with_caller(
                caller,
                intent_core::caller::with_wire_credential(credential, async move {
                    let operation = CredentialOperation {
                        entry: &entry,
                        target: &target,
                        lease: worker_lease,
                        secrets: &worker_secrets,
                        generation,
                    };
                    match request {
                        CredentialRequest::Status { only_user } => service
                            .collaboration_status_owned(operation, only_user)
                            .await
                            .map(CredentialReply::Value),
                        CredentialRequest::Select { external_id } => service
                            .collaboration_select_owned(operation, &external_id)
                            .await
                            .map(CredentialReply::Value),
                        CredentialRequest::ProofCreate {
                            nonce,
                            label,
                            expected,
                        } => service
                            .collaboration_proof_create_owned(operation, &nonce, &label, expected)
                            .await
                            .map(|(provider, proof)| CredentialReply::Proof(provider, proof)),
                        CredentialRequest::ProofDelete { proof_id } => service
                            .collaboration_proof_delete_owned(operation, &proof_id)
                            .await
                            .map(|()| CredentialReply::Value(Value::Null)),
                    }
                }),
            ));
            let mut response = Some(response);
            let joined = tokio::select! {
                biased;
                () = expired.notified() => {
                    let _ = response.take().unwrap().send(Err(Error::Internal(
                        "collaboration credential write timed out; operation continues, state may be unknown".into()
                    )));
                    worker.await
                }
                result = &mut worker => result,
            };
            let result = joined.unwrap_or_else(|error| {
                Err(Error::Internal(format!(
                    "collaboration credential worker failed: {error}; state may be unknown"
                )))
            });
            if let Some(response) = response {
                let _ = response.send(result);
            }
            secrets.finish_operation().await;
            drop(lease);
        };
        if self.settings_tasks.spawn_draining(owner).is_none() {
            return Err(Error::Internal("daemon is shutting down".into()));
        }
        receiver.await.map_err(|_| {
            Error::Internal("collaboration credential owner failed; state may be unknown".into())
        })?
    }

    pub(crate) async fn collaboration_status(
        &self,
        provider: &str,
        host: Option<&str>,
        only_user: bool,
    ) -> Result<Value> {
        match self
            .collaboration_request(provider, host, CredentialRequest::Status { only_user })
            .await?
        {
            CredentialReply::Value(value) => Ok(value),
            CredentialReply::Proof(..) => unreachable!("status reply"),
        }
    }

    async fn collaboration_status_owned(
        &self,
        operation: CredentialOperation<'_>,
        only_user: bool,
    ) -> Result<Value> {
        let CredentialOperation {
            entry,
            target,
            lease,
            secrets,
            generation: _,
        } = operation;
        let probed = self
            .collaboration_probe(entry, lease.clone(), target, secrets)
            .await?;
        if only_user {
            return Ok(json!({"user":probed.map(|(a,_,_)|a.user)}));
        }
        let state = entry.state.lock().await;
        let mut wire = repository::auth_status_to_wire(
            github_auth_ops::auth_status_to_wire(probed.is_some(), state.flow.as_ref()),
            target.provider(),
            target.host(),
            probed.as_ref().map(|(a, _, _)| a.method.as_str()),
            probed.as_ref().map(|(a, _, _)| a.user.clone()),
            !state.unsupported && self.collaboration_client_id(target).is_some(),
        );
        wire["purpose"] = json!("collaboration");
        wire["requestedScopes"] = json!(target.scopes());
        wire["grantedScopes"] = json!(probed.and_then(|(a, _, _)| a.scopes));
        Ok(wire)
    }

    pub(crate) async fn collaboration_cancel(
        &self,
        provider: &str,
        host: Option<&str>,
        flow_id: &str,
    ) -> Result<Value> {
        let target = Self::collaboration_target(provider, host)?;
        let entry = self.collaboration_credential(&target).await?;
        let mut state = entry.state.lock().await;
        let cancelled = state
            .flow
            .as_ref()
            .is_some_and(|f| f.phase == FlowPhase::Pending)
            && state.flow_id == flow_id;
        if cancelled {
            state.flow = None;
            state.generation += 1;
        }
        Ok(json!({"ok":true,"cancelled":cancelled}))
    }

    pub(crate) async fn collaboration_revoke(
        &self,
        provider: &str,
        host: Option<&str>,
    ) -> Result<Value> {
        let target = Self::collaboration_target(provider, host)?;
        let entry = self.collaboration_credential(&target).await?;
        let caller = intent_core::current_caller()
            .ok_or_else(|| Error::Internal("credential caller missing".into()))?;
        let credential = intent_core::caller::current_wire_credential();
        let expired = Arc::new(tokio::sync::Notify::new());
        let secrets = entry.secrets.settled_operation(expired.clone());
        let service = self.clone();
        let (response, receiver) = tokio::sync::oneshot::channel();
        let owner = async move {
            // Invalidate start/exchange before waiting for the existing IO.
            {
                let mut state = entry.state.lock().await;
                state.generation += 1;
                state.flow = None;
            }
            let lease: PersistenceLease = Arc::new(entry.gate.clone().lock_owned().await);
            let worker_secrets = secrets.clone();
            let worker_lease = lease.clone();
            let mut worker = intent_core::spawn_daemon(intent_core::with_caller(
                caller,
                intent_core::caller::with_wire_credential(credential, async move {
                    let key = target.token_account();
                    let present = io(&entry.store, worker_lease, move |store| store.load(key))
                        .await?
                        .is_some();
                    for key in [
                        key,
                        ACCOUNT,
                        intent_sourcecontrol::gitlab_token::REFRESH_SECRET_ACCOUNT,
                        intent_sourcecontrol::gitlab_token::EXPIRES_AT_SECRET_ACCOUNT,
                    ] {
                        worker_secrets.delete(key).await?;
                    }
                    if present {
                        service.collaboration_event(&target, "revoked", None).await;
                    }
                    Ok(json!({"ok":true}))
                }),
            ));
            let mut response = Some(response);
            let joined = tokio::select! {
                biased;
                () = expired.notified() => {
                    let _ = response.take().unwrap().send(Err(Error::Internal(
                        "collaboration credential write timed out; revoke continues, state may be unknown".into()
                    )));
                    worker.await
                }
                result = &mut worker => result,
            };
            let result = joined.unwrap_or_else(|error| {
                Err(Error::Internal(format!(
                    "collaboration revoke failed: {error}; state may be unknown"
                )))
            });
            if let Some(response) = response {
                let _ = response.send(result);
            }
            secrets.finish_operation().await;
            drop(lease);
        };
        if self.settings_tasks.spawn_draining(owner).is_none() {
            return Err(Error::Internal("daemon is shutting down".into()));
        }
        receiver.await.map_err(|_| {
            Error::Internal("collaboration revoke owner failed; state may be unknown".into())
        })?
    }

    async fn collaboration_connect_pat(
        &self,
        target: Target,
        entry: Arc<Credential>,
        token: String,
    ) -> Result<Value> {
        let caller = intent_core::current_caller()
            .ok_or_else(|| Error::Internal("credential caller missing".into()))?;
        let credential = intent_core::caller::current_wire_credential();
        let expired = Arc::new(tokio::sync::Notify::new());
        let secrets = entry.secrets.settled_operation(expired.clone());
        let service = self.clone();
        let (response, receiver) = tokio::sync::oneshot::channel();
        let owner = async move {
            let generation = {
                let mut state = entry.state.lock().await;
                state.generation += 1;
                state.flow = None;
                state.generation
            };
            let account = intent_core::with_caller(
                caller.clone(),
                intent_core::caller::with_wire_credential(
                    credential.clone(),
                    service.collaboration_verify(&target, &token, "pat"),
                ),
            )
            .await;
            let account = match account {
                Ok(account) => account,
                Err(error) => {
                    let _ = response.send(Err(error));
                    return;
                }
            };
            let lease: PersistenceLease = Arc::new(entry.gate.clone().lock_owned().await);
            let worker_secrets = secrets.clone();
            let mut worker = intent_core::spawn_daemon(intent_core::with_caller(
                caller,
                intent_core::caller::with_wire_credential(credential, async move {
                    let state = entry.state.lock().await;
                    if state.generation != generation {
                        return Err(Error::IdentityMismatch);
                    }
                    // Clear old proof receipts before the first token effect. Each
                    // sequential write retains its actual result on this owner's ledger.
                    worker_secrets.delete(ACCOUNT).await?;
                    worker_secrets.store(target.token_account(), &token).await?;
                    worker_secrets
                        .delete(intent_sourcecontrol::gitlab_token::REFRESH_SECRET_ACCOUNT)
                        .await?;
                    worker_secrets
                        .delete(intent_sourcecontrol::gitlab_token::EXPIRES_AT_SECRET_ACCOUNT)
                        .await?;
                    let data = serde_json::to_string(&account)
                        .map_err(|e| Error::Internal(e.to_string()))?;
                    worker_secrets.store(ACCOUNT, &data).await?;
                    service
                        .collaboration_event(&target, "authorized", None)
                        .await;
                    Ok(json!({"ok":true,"method":"pat","purpose":"collaboration"}))
                }),
            ));
            let mut response = Some(response);
            let joined = tokio::select! {
                biased;
                () = expired.notified() => {
                    let _ = response.take().unwrap().send(Err(Error::Internal(
                        "collaboration credential write timed out; PAT continues, state may be unknown".into()
                    )));
                    worker.await
                }
                result = &mut worker => result,
            };
            let result = joined.unwrap_or_else(|error| {
                Err(Error::Internal(format!(
                    "collaboration PAT failed: {error}; state may be unknown"
                )))
            });
            if let Some(response) = response {
                let _ = response.send(result);
            }
            secrets.finish_operation().await;
            drop(lease);
        };
        if self.settings_tasks.spawn_draining(owner).is_none() {
            return Err(Error::Internal("daemon is shutting down".into()));
        }
        receiver.await.map_err(|_| {
            Error::Internal("collaboration PAT owner failed; state may be unknown".into())
        })?
    }

    pub(crate) async fn collaboration_connect(
        &self,
        provider: &str,
        host: Option<&str>,
        method: Option<&str>,
        token: Option<String>,
    ) -> Result<Value> {
        let target = self.collaboration_connect_target(provider, host)?;
        let entry = self.collaboration_credential(&target).await?;
        let method = method.unwrap_or("device");
        if !matches!(method, "device" | "pat") {
            return Err(Error::InvalidParams("method must be device or pat".into()));
        }
        if method == "pat" && matches!(target, Target::Github) {
            return Err(Error::InvalidParams(
                "GitHub supports device authorization only".into(),
            ));
        }
        if method == "device" && token.is_some() {
            return Err(Error::InvalidParams("token requires method pat".into()));
        }
        let pat_token = if method == "pat" {
            Some(
                token
                    .filter(|t| !t.trim().is_empty())
                    .ok_or_else(|| Error::InvalidParams("token is required for pat".into()))?,
            )
        } else {
            None
        };
        if let Some(token) = pat_token {
            return self.collaboration_connect_pat(target, entry, token).await;
        }
        let generation = {
            let mut state = entry.state.lock().await;
            if self.settings_tasks.is_closed() || self.store_tasks.is_closed() {
                return Err(Error::Internal("daemon is shutting down".into()));
            }
            if method == "device" && state.flow.as_ref().is_some_and(FlowSlot::is_live) {
                return Ok(flow_response(&state));
            }
            state.generation += 1;
            state.flow = None;
            state.generation
        };
        let unsupported = || Error::DeviceGrantUnsupported {
            provider: provider.into(),
            host: target.host().into(),
        };
        let client_id = self
            .collaboration_client_id(&target)
            .ok_or_else(unsupported)?;
        let started = match &target {
            Target::Github => {
                let base =
                    github_auth_ops::resolve_login_base_uri(self.github_login_base_uri.as_deref());
                intent_sourcecontrol::device_flow::start_at_with_store(
                    &base,
                    &client_id,
                    target.scopes(),
                    entry.store.clone(),
                )
                .await
                .map(|(auth, flow)| {
                    #[cfg(test)]
                    assert_eq!(flow.persistence_path(), entry.store.path());
                    (
                        auth.user_code,
                        auth.verification_uri,
                        auth.expires_in,
                        auth.interval,
                        Flow::Github(flow),
                    )
                })
            }
            Target::Gitlab { host } => {
                gitlab_auth::start_device_grant_with_store(host, &client_id, entry.store.clone())
                    .await
                    .map(|(a, f)| {
                        (
                            a.user_code,
                            a.verification_uri_complete.unwrap_or(a.verification_uri),
                            a.expires_in,
                            a.interval,
                            Flow::Gitlab(f, client_id.clone()),
                        )
                    })
            }
        };
        let mut state = entry.state.lock().await;
        if self.settings_tasks.is_closed() {
            return Err(Error::Internal("daemon is shutting down".into()));
        }
        if state.generation != generation {
            return Err(Error::IdentityMismatch);
        }
        let (user_code, verification_uri, expires, interval, flow) = match started {
            Ok(r) => r,
            Err(intent_sourcecontrol::Error::DeviceGrantUnsupported(_)) => {
                state.unsupported = true;
                return Err(unsupported());
            }
            Err(e) => return Err(crate::pr_ops::map_sc_err(e)),
        };
        state.unsupported = false;
        let deadline = Instant::now() + Duration::from_secs(expires);
        // Register before publishing under the same state lock shutdown uses
        // to retire flows. A refused poll must never advertise usable codes.
        let service = self.clone();
        let poll_entry = entry.clone();
        self.store_tasks
            .spawn_draining(async move {
                service
                    .collaboration_poll(poll_entry, target, generation, deadline, flow)
                    .await;
            })
            .ok_or_else(|| Error::Internal("daemon is shutting down".into()))?;
        state.flow_id = uuid::Uuid::new_v4().to_string();
        state.flow = Some(FlowSlot {
            flow_id: github_auth_ops::next_flow_id(),
            user_code,
            verification_uri,
            interval,
            deadline,
            phase: FlowPhase::Pending,
        });
        Ok(flow_response(&state))
    }

    pub(crate) async fn shutdown_collaboration_flows(&self) {
        let entries: Vec<_> = self
            .collaboration_auth
            .lock()
            .await
            .values()
            .cloned()
            .collect();
        for entry in entries {
            let acquire = entry.state.lock();
            #[cfg(test)]
            let acquire = {
                let mut acquire = Box::pin(acquire);
                std::future::poll_fn(move |cx| {
                    let result = std::future::Future::poll(acquire.as_mut(), cx);
                    if result.is_pending() {
                        if let Some(tx) = self.secrets.writer_drain_pending.lock().unwrap().take() {
                            let _ = tx.send("collaboration-state");
                        }
                    }
                    result
                })
            };
            let mut state = acquire.await;
            state.generation += 1;
            state.flow = None;
        }
    }

    async fn collaboration_poll(
        &self,
        entry: Arc<Credential>,
        target: Target,
        generation: u64,
        deadline: Instant,
        mut flow: Flow,
    ) {
        let mut failures = 0;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                let sleep =
                    tokio::time::sleep(github_auth_ops::poll_sleep(flow.interval()).min(remaining));
                #[cfg(test)]
                let sleep = {
                    let mut sleep = Box::pin(sleep);
                    let entry = entry.clone();
                    std::future::poll_fn(move |cx| {
                        let result = std::future::Future::poll(sleep.as_mut(), cx);
                        if result.is_pending() {
                            if let Some(pending) = entry.sleep_pending.lock().unwrap().take() {
                                let _ = pending.send(());
                            }
                        }
                        result
                    })
                };
                // Cancel idle recurrence only; an exchange or write already
                // admitted below retains its existing completion path.
                tokio::select! {
                    biased;
                    () = self.settings_tasks.closed() => return,
                    () = sleep => {}
                }
            }
            if entry.state.lock().await.generation != generation {
                return;
            }
            let outcome = if Instant::now() >= deadline {
                Ok(Exchange::Terminal(FlowPhase::Expired))
            } else {
                flow.exchange().await
            };
            let outcome = match outcome {
                Ok(Exchange::Pending) => {
                    failures = 0;
                    continue;
                }
                Ok(other) => other,
                Err(_) => {
                    failures += 1;
                    if failures < github_auth_ops::MAX_CONSECUTIVE_POLL_ERRORS {
                        continue;
                    }
                    Exchange::Terminal(FlowPhase::Error)
                }
            };
            let lease: PersistenceLease = Arc::new(entry.gate.clone().lock_owned().await);
            let verified = match &outcome {
                Exchange::Grant(grant) => Some(self.verify_grant(&target, grant).await),
                _ => None,
            };
            let phase = match (outcome, verified) {
                (Exchange::Grant(grant), Some(Ok(account))) => {
                    self.collaboration_commit_grant(
                        entry, target, generation, lease, grant, account,
                    )
                    .await;
                    return;
                }
                (Exchange::Terminal(phase), _) => Some(phase),
                _ => Some(FlowPhase::Error),
            };
            let mut state = entry.state.lock().await;
            if state.generation != generation || state.flow.is_none() {
                return;
            }
            if let Some(phase) = phase {
                if let Some(slot) = &mut state.flow {
                    slot.phase = phase;
                }
            } else {
                state.flow = None;
            }
            self.collaboration_event(
                &target,
                phase.map_or("authorized", FlowPhase::as_wire),
                Some(&state.flow_id),
            )
            .await;
            return;
        }
    }

    async fn collaboration_commit_grant(
        &self,
        entry: Arc<Credential>,
        target: Target,
        generation: u64,
        lease: PersistenceLease,
        grant: Grant,
        account: Account,
    ) {
        // The tracked poll owns this supervisor and credential guard. The child
        // may fail, but its registered physical writes must settle before the
        // guard is released or shutdown can join this poll.
        let secrets = entry
            .secrets
            .settled_operation(Arc::new(tokio::sync::Notify::new()));
        let worker_secrets = secrets.clone();
        let worker_entry = entry.clone();
        let worker_target = target.clone();
        let worker_lease = lease.clone();
        let service = self.clone();
        let worker = intent_core::spawn_daemon(async move {
            let mut state = worker_entry.state.lock().await;
            if state.generation != generation || state.flow.is_none() {
                return;
            }
            let persisted: Result<()> = async {
                worker_secrets.delete(ACCOUNT).await?;
                #[cfg(test)]
                let worker_lease: PersistenceLease = {
                    let held = worker_entry.commit_test_lease.lock().unwrap().take();
                    Arc::new((worker_lease, held))
                };
                grant.commit(&worker_secrets, worker_lease).await?;
                let data = serde_json::to_string(&account)
                    .map_err(|error| Error::Internal(error.to_string()))?;
                worker_secrets.store(ACCOUNT, &data).await
            }
            .await;
            if persisted.is_ok() {
                state.flow = None;
            } else if let Some(flow) = &mut state.flow {
                flow.phase = FlowPhase::Error;
            }
            service
                .collaboration_event(
                    &worker_target,
                    if persisted.is_ok() {
                        "authorized"
                    } else {
                        "error"
                    },
                    Some(&state.flow_id),
                )
                .await;
        });
        let joined = worker.await;
        #[cfg(test)]
        if joined.is_err() {
            secrets.gitlab_poll_worker_failed.notify_one();
        }
        secrets.finish_operation().await;
        if let Err(error) = joined {
            tracing::warn!(%error, "collaboration grant worker failed; credential state may be unknown");
            let mut state = entry.state.lock().await;
            if state.generation == generation && state.flow.is_some() {
                if let Some(flow) = &mut state.flow {
                    flow.phase = FlowPhase::Error;
                }
                self.collaboration_event(&target, "error", Some(&state.flow_id))
                    .await;
            }
        }
        drop(lease);
    }

    async fn verify_grant(&self, target: &Target, grant: &Grant) -> Result<Account> {
        match (target, grant) {
            (Target::Github, Grant::Github(g)) => {
                let base =
                    crate::invite_ops::resolve_api_base_uri(self.github_api_base_uri.as_deref());
                let (user, scopes) = g
                    .verify(base.as_deref())
                    .await
                    .map_err(crate::pr_ops::map_sc_err)?;
                check_scopes(target, scopes.as_deref())?;
                Account::new(
                    ForgeUser::github(&user),
                    repository::github_user_to_wire(&user),
                    "device",
                    scopes,
                )
            }
            (Target::Gitlab { host }, Grant::Gitlab(g, client_id)) => {
                let (user, scopes) = g.verify(host).await.map_err(crate::pr_ops::map_sc_err)?;
                check_scopes(target, scopes.as_deref())?;
                let mut account = Account::new(
                    ForgeUser::gitlab(host.host(), &user),
                    repository::gitlab_user_to_wire(&user),
                    "device",
                    scopes,
                )?;
                account.gitlab_binding = Some(GitlabBinding {
                    base_url: host.base_url().into(),
                    client_id: Some(client_id.clone()),
                });
                Ok(account)
            }
            _ => Err(Error::IdentityMismatch),
        }
    }

    pub(crate) async fn collaboration_select(
        &self,
        provider: &str,
        host: Option<&str>,
        external_id: &str,
    ) -> Result<Value> {
        match self
            .collaboration_request(
                provider,
                host,
                CredentialRequest::Select {
                    external_id: external_id.into(),
                },
            )
            .await?
        {
            CredentialReply::Value(value) => Ok(value),
            CredentialReply::Proof(..) => unreachable!("selection reply"),
        }
    }

    async fn collaboration_select_owned(
        &self,
        operation: CredentialOperation<'_>,
        external_id: &str,
    ) -> Result<Value> {
        let CredentialOperation {
            entry,
            target,
            lease,
            secrets,
            generation,
        } = operation;
        let credential_generation = entry.state.lock().await.generation;
        let (account, _, _) = self
            .collaboration_probe(entry, lease.clone(), target, secrets)
            .await?
            .ok_or_else(|| target.not_connected())?;
        if account.identity.external_user_id != external_id {
            return Err(Error::IdentityMismatch);
        }
        let _transition = self.identity_transition.lock().await;
        let state = entry.state.lock().await;
        if self.identity_rekey_generation.load(Ordering::SeqCst) != generation
            || state.generation != credential_generation
        {
            return Err(Error::IdentityMismatch);
        }
        let primary = self.store.get_primary_principal().await?;
        self.select_primary_forge_identity_locked(
            primary,
            &ForgeUser {
                identity: Some(account.identity),
                login: account.user["login"].as_str().unwrap_or_default().into(),
                display_name: account.display_name,
                avatar_url: account.user["avatarUrl"].as_str().map(str::to_string),
            },
        )
        .await?;
        Ok(json!({"principal":self.principal_me_op().await?}))
    }

    pub(crate) async fn collaboration_proof_create(
        &self,
        provider: &str,
        host: Option<&str>,
        nonce: &str,
        label: &str,
        expected: PrincipalIdentity,
    ) -> Result<(ProofProvider, CreatedProof)> {
        match self
            .collaboration_request(
                provider,
                host,
                CredentialRequest::ProofCreate {
                    nonce: nonce.into(),
                    label: label.into(),
                    expected,
                },
            )
            .await?
        {
            CredentialReply::Proof(provider, proof) => Ok((provider, proof)),
            CredentialReply::Value(_) => unreachable!("proof reply"),
        }
    }

    async fn collaboration_proof_create_owned(
        &self,
        operation: CredentialOperation<'_>,
        nonce: &str,
        label: &str,
        expected: PrincipalIdentity,
    ) -> Result<(ProofProvider, CreatedProof)> {
        let CredentialOperation {
            entry,
            target,
            lease,
            secrets,
            generation,
        } = operation;
        let nonce = github_auth_ops::proof_line_param("nonce", nonce)?;
        let label = github_auth_ops::proof_line_param("hostLabel", label)?;
        if expected.provider != target.provider().as_wire() || expected.host != target.host() {
            return Err(Error::IdentityMismatch);
        }
        let credential_generation = entry.state.lock().await.generation;
        let (account, token, target) = self
            .collaboration_probe(entry, lease.clone(), target, secrets)
            .await?
            .ok_or_else(|| target.not_connected())?;
        if account.identity != expected {
            return Err(Error::IdentityMismatch);
        }
        let proof = self.proof_provider(&target);
        let created = proof
            .create(&token, &nonce, &label)
            .await
            .map_err(|e| github_auth_ops::map_identity_proof_err_for(target.provider(), e))?;
        let verified = self
            .collaboration_verify(&target, &token, &account.method)
            .await;
        match verified {
            Ok(a) if a.identity == expected => {}
            other => {
                let _ = proof.delete(&token, &created.proof_id).await;
                return Err(other.err().unwrap_or(Error::IdentityMismatch));
            }
        }
        let _transition = self.identity_transition.lock().await;
        let state = entry.state.lock().await;
        if self.identity_rekey_generation.load(Ordering::SeqCst) != generation
            || state.generation != credential_generation
            || created
                .owner
                .external_user_id
                .as_ref()
                .is_some_and(|id| id != &expected.external_user_id)
        {
            let _ = proof.delete(&token, &created.proof_id).await;
            return Err(Error::IdentityMismatch);
        }
        // Cleanup authorizes only proofs published by this precise credential
        // generation. Keeping this receipt in the isolated store survives restart.
        let key = format!("identity.proof.{}", created.proof_id);
        let receipt = account.generation.clone();
        if let Err(e) = secrets.store(&key, &receipt).await {
            let _ = proof.delete(&token, &created.proof_id).await;
            return Err(e);
        }
        Ok((proof, created))
    }

    pub(crate) async fn collaboration_proof_delete(
        &self,
        provider: &str,
        host: Option<&str>,
        proof_id: &str,
    ) -> Result<()> {
        self.collaboration_request(
            provider,
            host,
            CredentialRequest::ProofDelete {
                proof_id: proof_id.into(),
            },
        )
        .await?;
        Ok(())
    }

    async fn collaboration_proof_delete_owned(
        &self,
        operation: CredentialOperation<'_>,
        proof_id: &str,
    ) -> Result<()> {
        let CredentialOperation {
            entry,
            target,
            lease,
            secrets,
            generation: _,
        } = operation;
        let proof = self.proof_provider(target);
        if !proof.valid_proof_id(proof_id) {
            return Err(Error::InvalidParams("invalid proofId".into()));
        }
        let (account, token, target) = self
            .collaboration_probe(entry, lease.clone(), target, secrets)
            .await?
            .ok_or_else(|| target.not_connected())?;
        let proof = self.proof_provider(&target);
        let key = format!("identity.proof.{proof_id}");
        let receipt_key = key.clone();
        let receipt = io(&entry.store, lease.clone(), move |s| s.load(&receipt_key)).await?;
        if receipt.as_deref() != Some(&account.generation) {
            return Err(Error::IdentityMismatch);
        }
        proof
            .delete(&token, proof_id)
            .await
            .map_err(|e| github_auth_ops::map_identity_proof_err_for(target.provider(), e))?;
        // Retain the receipt so repeat cleanup stays idempotent for this account.
        Ok(())
    }
}

fn check_scopes(target: &Target, scopes: Option<&[String]>) -> Result<()> {
    if scopes.is_some_and(|s| {
        target
            .scopes()
            .iter()
            .any(|required| !s.iter().any(|actual| actual == required))
    }) {
        return Err(Error::IdentityProof(match target {
            Target::Github => IdentityProofErrorKind::ScopeMissing,
            Target::Gitlab { .. } => IdentityProofErrorKind::GitlabScopeMissing,
        }));
    }
    Ok(())
}

enum Flow {
    Github(DeviceFlow),
    Gitlab(GitlabDeviceFlow, String),
}
enum Grant {
    Github(GithubGrant),
    Gitlab(GitlabGrant, String),
}
enum Exchange {
    Grant(Grant),
    Pending,
    Terminal(FlowPhase),
}
impl Flow {
    fn interval(&self) -> u64 {
        match self {
            Self::Github(f) => f.interval_secs(),
            Self::Gitlab(f, _) => f.interval_secs(),
        }
    }
    async fn exchange(&mut self) -> intent_sourcecontrol::Result<Exchange> {
        Ok(match self {
            Self::Github(f) => match f.exchange_once().await? {
                GithubExchange::Authorized(g) => Exchange::Grant(Grant::Github(g)),
                GithubExchange::Pending => Exchange::Pending,
                GithubExchange::Expired => Exchange::Terminal(FlowPhase::Expired),
                GithubExchange::Denied => Exchange::Terminal(FlowPhase::Denied),
            },
            Self::Gitlab(f, client_id) => match f.exchange_once().await? {
                GitlabExchange::Authorized(g) => {
                    Exchange::Grant(Grant::Gitlab(g, client_id.clone()))
                }
                GitlabExchange::Pending => Exchange::Pending,
                GitlabExchange::Expired => Exchange::Terminal(FlowPhase::Expired),
                GitlabExchange::Denied => Exchange::Terminal(FlowPhase::Denied),
            },
        })
    }
}
impl Grant {
    async fn commit(
        self,
        secrets: &crate::settings::AsyncSecretStore,
        lease: PersistenceLease,
    ) -> Result<()> {
        match self {
            Self::Github(g) => secrets.persist_github_grant(g, Some(Box::new(lease))).await,
            Self::Gitlab(g, _) => secrets.persist_gitlab_grant(g, lease).await,
        }
    }
}

fn map_auth_error(target: &Target, error: intent_sourcecontrol::Error) -> Error {
    match error {
        intent_sourcecontrol::Error::Auth(_) => Error::SourceControlUnauthorized {
            provider: target.provider().as_wire().into(),
            host: target.host().into(),
        },
        other => crate::pr_ops::map_sc_err(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{pr::StubForge, TempDb};
    use intent_core::{with_caller, Caller, HostRole, PrincipalId, WorkspaceApi};
    use intent_sourcecontrol::GitlabHost;
    use intent_store::Store;

    async fn fixture() -> (TempDb, Services) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let secrets = FileSecretStore::with_path(tmp.path.with_extension("secrets"));
        let services = Services::new_with_file_secrets(store, secrets);
        (tmp, services)
    }

    async fn assert_probe_settlement(disconnect: bool) {
        use intent_sourcecontrol::gitlab_token::{
            EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT,
        };
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        for outcome in 0..5 {
            let (tmp, service) = fixture().await;
            let bus = crate::EventBus::new(service.store.clone());
            let service = service.with_event_bus(bus.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let host = GitlabHost::parse("gitlab.probe.test")
                .unwrap()
                .with_api_origin(&base)
                .unwrap();
            let target = Target::Gitlab { host: host.clone() };
            let server = intent_core::spawn_daemon(async move {
                let mut replies = vec![(
                    "POST",
                    "/oauth/token",
                    if disconnect { 400 } else { 200 },
                    if disconnect {
                        json!({"error":"invalid_grant"})
                    } else {
                        json!({"access_token":"rotated-token","refresh_token":"rotated-refresh","expires_in":7200,"scope":"api"})
                    },
                )];
                if !disconnect && outcome < 2 {
                    replies.extend([
                        (
                            "GET",
                            "/api/v4/user",
                            200,
                            json!({"id":42,"username":"observed-user","name":"Observed"}),
                        ),
                        (
                            "GET",
                            "/api/v4/personal_access_tokens/self",
                            200,
                            json!({"id":7,"scopes":["api"]}),
                        ),
                    ]);
                }
                for (method, path, status, body) in replies {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    assert!(line.starts_with(&format!("{method} {path} ")));
                    let mut length = 0;
                    loop {
                        line.clear();
                        reader.read_line(&mut line).await.unwrap();
                        if line == "\r\n" {
                            break;
                        }
                        if let Some((key, value)) = line.split_once(':') {
                            if key.eq_ignore_ascii_case("content-length") {
                                length = value.trim().parse().unwrap();
                            }
                        }
                    }
                    reader.read_exact(&mut vec![0; length]).await.unwrap();
                    let body = body.to_string();
                    reader.get_mut().write_all(format!("HTTP/1.1 {status} Test\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                }
            });
            let entry = service.collaboration_credential(&target).await.unwrap();
            let raw = entry.store.clone();
            let mut saved = Account::new(
                ForgeUser::on("gitlab", host.host(), "42", "old-user", None),
                json!({"id":"42","login":"old-user"}),
                "device",
                Some(vec!["api".into()]),
            )
            .unwrap();
            saved.gitlab_binding = Some(GitlabBinding {
                base_url: host.base_url().into(),
                client_id: Some("private-client".into()),
            });
            for (key, value) in [
                (SECRET_ACCOUNT, "old-token"),
                (REFRESH_SECRET_ACCOUNT, "old-refresh"),
                (EXPIRES_AT_SECRET_ACCOUNT, "1"),
            ] {
                raw.store(key, value).unwrap();
            }
            raw.store(ACCOUNT, &serde_json::to_string(&saved).unwrap())
                .unwrap();
            drop(entry);
            let entered = Arc::new(tokio::sync::Notify::new());
            let (release, held) = std::sync::mpsc::channel();
            let backend = Arc::new(HeldRevoke {
                store: raw.clone(),
                entered: entered.clone(),
                release: std::sync::Mutex::new(if disconnect { Some(held) } else { None }),
                outcome: match outcome {
                    2 => 1,
                    3 => 2,
                    _ => 0,
                },
            });
            let secrets = crate::settings::AsyncSecretStore::with_timings(
                backend,
                Duration::from_secs(5),
                if matches!(outcome, 0 | 4) {
                    Duration::from_secs(5)
                } else {
                    Duration::from_millis(10)
                },
                Duration::from_secs(60),
                Duration::from_secs(60),
            );
            // Refresh uses the actual grant closure; disconnect uses the backend delete.
            // Separate channels avoid moving the disconnect receiver into the refresh hook.
            let (refresh_release, refresh_held) = std::sync::mpsc::channel();
            if !disconnect {
                let signal = entered.clone();
                *secrets.before_gitlab_persistence.lock().unwrap() = Some(Box::new(move || {
                    signal.notify_one();
                    let _ = refresh_held.recv();
                    match outcome {
                        2 => Err(Error::Internal("controlled refresh write error".into())),
                        3 => panic!("controlled refresh backend panic"),
                        _ => Ok(()),
                    }
                }));
            }
            let (fail, failing) = tokio::sync::oneshot::channel();
            if outcome == 4 {
                *secrets.panic_mutation_caller.lock().unwrap() = Some(failing);
            }
            {
                let mut entries = service.collaboration_auth.lock().await;
                Arc::get_mut(entries.values_mut().next().unwrap())
                    .unwrap()
                    .secrets = Arc::new(secrets);
            }
            let entry = service.collaboration_credential(&target).await.unwrap();
            for key in [
                SECRET_ACCOUNT,
                REFRESH_SECRET_ACCOUNT,
                EXPIRES_AT_SECRET_ACCOUNT,
                ACCOUNT,
            ] {
                assert!(entry.secrets.load(key).await.unwrap().is_some());
            }
            let owner = service.clone();
            let caller = intent_core::spawn_daemon(async move {
                owner
                    .identity_get_user("gitlab".into(), Some("gitlab.probe.test".into()))
                    .await
            });
            tokio::time::timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            if outcome == 0 {
                caller.abort();
                assert!(caller.await.unwrap_err().is_cancelled());
            } else {
                if outcome == 4 {
                    fail.send(()).unwrap();
                }
                let error = tokio::time::timeout(Duration::from_secs(5), caller)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                assert!(
                    error.to_string().contains(if outcome == 4 {
                        "credential worker failed"
                    } else {
                        "timed out"
                    }),
                    "{error}"
                );
            }
            assert_eq!(entry.secrets.mutation_counts_for_test(), (1, 1));
            assert!(entry.gate.try_lock().is_err());
            assert_eq!(
                raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
                Some("old-token")
            );
            let (pending, pending_rx) = tokio::sync::oneshot::channel();
            *service.secrets.writer_drain_pending.lock().unwrap() = Some(pending);
            let owner = service.clone();
            let drain =
                intent_core::spawn_daemon(async move { owner.shutdown_store_writers().await });
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), pending_rx)
                    .await
                    .unwrap()
                    .unwrap(),
                "settings-tasks"
            );
            assert!(!drain.is_finished());
            if disconnect {
                release.send(()).unwrap();
            } else {
                refresh_release.send(()).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), drain)
                .await
                .unwrap()
                .unwrap();
            server.await.unwrap();
            assert_eq!(entry.secrets.mutation_counts_for_test(), (0, 1));
            assert!(entry.gate.try_lock().is_ok());
            for key in [
                SECRET_ACCOUNT,
                REFRESH_SECRET_ACCOUNT,
                EXPIRES_AT_SECRET_ACCOUNT,
                ACCOUNT,
            ] {
                assert_eq!(
                    entry.secrets.load(key).await.unwrap(),
                    raw.load(key).unwrap(),
                    "cache {key}, outcome {outcome}"
                );
            }
            assert_eq!(
                raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
                if matches!(outcome, 2 | 3) {
                    Some("old-token")
                } else if disconnect {
                    None
                } else {
                    Some("rotated-token")
                }
            );
            if disconnect {
                assert_eq!(raw.load(ACCOUNT).unwrap().is_none(), outcome < 2);
                assert_eq!(
                    raw.load(REFRESH_SECRET_ACCOUNT).unwrap().is_none(),
                    outcome < 2
                );
            } else {
                assert_eq!(
                    account(&raw).unwrap().unwrap().user["login"],
                    if outcome < 2 {
                        "observed-user"
                    } else {
                        "old-user"
                    }
                );
            }
            bus.shutdown().await.unwrap();
            service.store.close().await;
            let reopened = Store::open(&tmp.path).await.unwrap();
            let events = reopened
                .query_events(&intent_store::EventQuery::default())
                .await
                .unwrap();
            reopened.close().await;
            let auth: Vec<_> = events
                .iter()
                .filter(|e| e.event_type == intent_core::events::IDENTITY_AUTH_CHANGED)
                .collect();
            assert_eq!(auth.len(), usize::from(disconnect && outcome < 2));
            if let Some(event) = auth.first() {
                assert_eq!(event.data["status"], "expired");
            }
        }
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_refresh_retains_actual_result_through_shutdown() {
        assert_probe_settlement(false).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_probe_disconnect_retains_actual_result_through_shutdown() {
        assert_probe_settlement(true).await;
    }

    struct HeldCredentialTail {
        store: FileSecretStore,
        key: &'static str,
        entered: Arc<tokio::sync::Notify>,
        release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        outcome: u8,
    }

    impl crate::settings::SecretStore for HeldCredentialTail {
        fn load(&self, key: &str) -> Result<Option<String>> {
            self.store.load(key)
        }
        fn delete(&self, key: &str) -> Result<()> {
            self.store.delete(key)
        }
        fn store(&self, key: &str, value: &str) -> Result<()> {
            if key == self.key {
                let held = self.release.lock().unwrap().take();
                if let Some(held) = held {
                    self.entered.notify_one();
                    let _ = held.recv();
                    match self.outcome {
                        2 => return Err(Error::Internal("controlled receipt write error".into())),
                        3 => panic!("controlled receipt backend panic"),
                        _ => {}
                    }
                }
            }
            self.store.store(key, value)
        }
    }

    async fn assert_credential_tail_settlement(mode: u8) {
        use intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT;
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let select = mode == 0;
        let delete = mode == 2;
        for outcome in 0..if select || delete { 2 } else { 5 } {
            let (tmp, service) = fixture().await;
            let bus = crate::EventBus::new(service.store.clone());
            let service = service.with_event_bus(bus.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let host = GitlabHost::parse("gitlab.tail.test")
                .unwrap()
                .with_api_origin(&format!("http://{}", listener.local_addr().unwrap()))
                .unwrap();
            let target = Target::Gitlab { host: host.clone() };
            let entry = service.collaboration_credential(&target).await.unwrap();
            let raw = entry.store.clone();
            let mut saved = Account::new(
                ForgeUser::on("gitlab", host.host(), "42", "tail-user", None),
                json!({"id":"42","login":"tail-user"}),
                "pat",
                Some(vec!["api".into()]),
            )
            .unwrap();
            saved.gitlab_binding = Some(GitlabBinding {
                base_url: host.base_url().into(),
                client_id: None,
            });
            raw.store(SECRET_ACCOUNT, "private-pat").unwrap();
            raw.store(ACCOUNT, &serde_json::to_string(&saved).unwrap())
                .unwrap();
            if delete {
                raw.store("identity.proof.77", &saved.generation).unwrap();
            }
            drop(entry);
            let entered = Arc::new(tokio::sync::Notify::new());
            let (release, held) = std::sync::mpsc::channel();
            let secrets = crate::settings::AsyncSecretStore::with_timings(
                Arc::new(HeldCredentialTail {
                    store: raw.clone(),
                    key: if select || delete {
                        ACCOUNT
                    } else {
                        "identity.proof.77"
                    },
                    entered: entered.clone(),
                    release: std::sync::Mutex::new(Some(held)),
                    outcome,
                }),
                Duration::from_secs(5),
                if matches!(outcome, 0 | 4) {
                    Duration::from_secs(5)
                } else {
                    Duration::from_millis(10)
                },
                Duration::from_secs(60),
                Duration::from_secs(60),
            );
            let panic_slot = secrets.panic_mutation_caller.clone();
            let (fail, failing) = tokio::sync::oneshot::channel();
            let server = intent_core::spawn_daemon(async move {
                let user = json!({"id":42,"username":"tail-user","name":"Tail User"});
                let scopes = json!({"id":7,"scopes":["api"]});
                let snippet = json!({"id":77,"author":{"id":42,"username":"tail-user"},"files":[{"path":intent_sourcecontrol::identity_proof::PROOF_FILE_NAME}]});
                let mut replies = vec![
                    ("GET", "/api/v4/user", user.clone()),
                    ("GET", "/api/v4/personal_access_tokens/self", scopes.clone()),
                ];
                if delete {
                    replies.extend([
                        ("GET", "/api/v4/snippets/77", snippet.clone()),
                        ("DELETE", "/api/v4/snippets/77", Value::Null),
                    ]);
                } else if !select {
                    replies.extend([
                        ("POST", "/api/v4/snippets", snippet.clone()),
                        ("GET", "/api/v4/user", user),
                        ("GET", "/api/v4/personal_access_tokens/self", scopes),
                    ]);
                    if matches!(outcome, 2 | 3) {
                        replies.extend([
                            ("GET", "/api/v4/snippets/77", snippet),
                            ("DELETE", "/api/v4/snippets/77", Value::Null),
                        ]);
                    }
                }
                let mut failing = Some(failing);
                for (index, (method, path, body)) in replies.into_iter().enumerate() {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    assert!(line.starts_with(&format!("{method} {path} ")));
                    let mut length = 0;
                    let mut authorized = false;
                    loop {
                        line.clear();
                        reader.read_line(&mut line).await.unwrap();
                        if line == "\r\n" {
                            break;
                        }
                        if let Some((key, value)) = line.split_once(':') {
                            if key.eq_ignore_ascii_case("content-length") {
                                length = value.trim().parse().unwrap();
                            }
                            if key.eq_ignore_ascii_case("authorization") {
                                assert_eq!(value.trim(), "Bearer private-pat");
                                authorized = true;
                            }
                        }
                    }
                    assert!(authorized);
                    reader.read_exact(&mut vec![0; length]).await.unwrap();
                    // The probe's ACCOUNT write has already returned; the next
                    // write can only be this proof's receipt after verification.
                    if outcome == 4 && index == 4 {
                        *panic_slot.lock().unwrap() = failing.take();
                    }
                    let body = body.to_string();
                    reader.get_mut().write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                }
            });
            {
                let mut entries = service.collaboration_auth.lock().await;
                Arc::get_mut(entries.values_mut().next().unwrap())
                    .unwrap()
                    .secrets = Arc::new(secrets);
            }
            let entry = service.collaboration_credential(&target).await.unwrap();
            assert_eq!(
                entry.secrets.load("identity.proof.77").await.unwrap(),
                if delete {
                    Some(saved.generation.clone())
                } else {
                    None
                }
            );
            let expected = saved.identity.clone();
            let owner = service.clone();
            let caller = intent_core::spawn_daemon(async move {
                if select {
                    owner
                        .identity_select(
                            "gitlab".into(),
                            Some("gitlab.tail.test".into()),
                            "42".into(),
                        )
                        .await
                } else if delete {
                    owner
                        .source_control_identity_proof_delete(
                            "gitlab".into(),
                            Some("gitlab.tail.test".into()),
                            "77".into(),
                            Some("collaboration".into()),
                        )
                        .await
                } else {
                    owner
                        .source_control_identity_proof_create(
                            "gitlab".into(),
                            Some("gitlab.tail.test".into()),
                            "nonce".into(),
                            "test-host".into(),
                            Some("collaboration".into()),
                            Some(expected),
                        )
                        .await
                }
            });
            tokio::time::timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            if outcome == 0 {
                caller.abort();
                assert!(caller.await.unwrap_err().is_cancelled());
            } else {
                if outcome == 4 {
                    fail.send(()).unwrap();
                }
                let error = tokio::time::timeout(Duration::from_secs(5), caller)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                assert!(
                    error.to_string().contains(if outcome == 4 {
                        "credential worker failed"
                    } else {
                        "timed out"
                    }),
                    "{error}"
                );
            }
            assert!(entry.gate.try_lock().is_err());
            assert_eq!(entry.secrets.mutation_counts_for_test(), (1, 1));
            assert_eq!(
                raw.load("identity.proof.77").unwrap(),
                if delete {
                    Some(saved.generation.clone())
                } else {
                    None
                }
            );
            let (pending, pending_rx) = tokio::sync::oneshot::channel();
            *service.secrets.writer_drain_pending.lock().unwrap() = Some(pending);
            let owner = service.clone();
            let drain =
                intent_core::spawn_daemon(async move { owner.shutdown_store_writers().await });
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), pending_rx)
                    .await
                    .unwrap()
                    .unwrap(),
                "settings-tasks"
            );
            assert!(!drain.is_finished());
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), drain)
                .await
                .unwrap()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
            assert!(entry.gate.try_lock().is_ok());
            assert_eq!(entry.secrets.mutation_counts_for_test(), (0, 1));
            assert_eq!(
                raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
                Some("private-pat")
            );
            let receipt = raw.load("identity.proof.77").unwrap();
            assert_eq!(
                receipt.as_deref(),
                if delete || (!select && matches!(outcome, 0 | 1 | 4)) {
                    Some(saved.generation.as_str())
                } else {
                    None
                }
            );
            assert_eq!(
                entry.secrets.load("identity.proof.77").await.unwrap(),
                receipt
            );
            bus.shutdown().await.unwrap();
            service.store.close().await;
            let reopened = Store::open(&tmp.path).await.unwrap();
            let primary = reopened.get_primary_principal().await.unwrap();
            assert_eq!(
                primary
                    .identity
                    .as_ref()
                    .map(|identity| identity.external_user_id.as_str()),
                if select { Some("42") } else { None }
            );
            let events = reopened
                .query_events(&intent_store::EventQuery::default())
                .await
                .unwrap();
            reopened.close().await;
            assert_eq!(
                events
                    .iter()
                    .filter(
                        |event| event.event_type == intent_core::events::PRINCIPAL_IDENTITY_CHANGED
                    )
                    .count(),
                usize::from(select)
            );
        }
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_selection_retains_identity_tail_through_shutdown() {
        assert_credential_tail_settlement(0).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_proof_retains_receipt_and_cleanup_through_shutdown() {
        assert_credential_tail_settlement(1).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_proof_delete_survives_caller_abandonment() {
        assert_credential_tail_settlement(2).await;
    }

    struct HeldGrantResult {
        entered: Arc<tokio::sync::Notify>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl Drop for HeldGrantResult {
        fn drop(&mut self) {
            self.entered.notify_one();
            let _ = self.release.lock().unwrap().recv();
        }
    }

    async fn device_grant_fixture(
        github: bool,
    ) -> (
        TempDb,
        Services,
        crate::EventBus,
        Target,
        Arc<Credential>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let (tmp, service) = fixture().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = GitlabHost::parse("gitlab.grant.test")
            .unwrap()
            .with_api_origin(&format!("http://{}", listener.local_addr().unwrap()))
            .unwrap();
        let server = tokio::spawn(async move {
            let responses = if github {
                vec![
                    (
                        "POST",
                        "/login/device/code",
                        json!({"device_code":"private-code","user_code":"CODE","verification_uri":"https://github.com/login/device","expires_in":900,"interval":1}),
                    ),
                    (
                        "POST",
                        "/login/oauth/access_token",
                        json!({"access_token":"private-grant","token_type":"bearer","scope":"gist"}),
                    ),
                    (
                        "GET",
                        "/user",
                        json!({"id":42,"login":"grant-user","name":"Grant User","avatar_url":"https://example.com/avatar"}),
                    ),
                ]
            } else {
                vec![
                    (
                        "POST",
                        "/oauth/authorize_device",
                        json!({"device_code":"private-code","user_code":"CODE","verification_uri":"https://gitlab.grant.test/device","expires_in":900,"interval":1}),
                    ),
                    (
                        "POST",
                        "/oauth/token",
                        json!({"access_token":"private-grant","refresh_token":"private-refresh","expires_in":7200,"scope":"api"}),
                    ),
                    (
                        "GET",
                        "/api/v4/user",
                        json!({"id":42,"username":"grant-user","name":"Grant User"}),
                    ),
                    (
                        "GET",
                        "/api/v4/personal_access_tokens/self",
                        json!({"id":7,"scopes":["api"]}),
                    ),
                ]
            };
            for (method, path, body) in responses {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                assert!(line.starts_with(&format!("{method} {path} ")));
                let mut length = 0;
                loop {
                    line.clear();
                    reader.read_line(&mut line).await.unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((key, value)) = line.split_once(':') {
                        if key.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap();
                        }
                    }
                }
                reader.read_exact(&mut vec![0; length]).await.unwrap();
                let body = body.to_string();
                reader.get_mut().write_all(format!("HTTP/1.1 200 OK\r\nx-oauth-scopes: gist\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        });
        let registry =
            Arc::new(crate::SettingsRegistry::load(tmp.path.with_extension("toml")).unwrap());
        registry
            .apply(&[
                ("sourceControl.gitlab.host".into(), json!(host.host())),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(host.base_url()),
                ),
                (
                    "sourceControl.gitlab.oauthClientId".into(),
                    json!("private-client"),
                ),
            ])
            .unwrap();
        let bus = crate::EventBus::new(service.store.clone());
        let service = service
            .with_settings_registry(registry)
            .with_event_bus(bus.clone());
        let service = if github {
            service
                .with_github_login_base_uri(host.base_url())
                .with_github_api_base_uri(host.base_url())
        } else {
            service
        };
        let target = if github {
            Target::Github
        } else {
            Target::Gitlab { host }
        };
        let entry = service.collaboration_credential(&target).await.unwrap();
        assert!(entry.store.path().starts_with(
            tmp.path
                .with_extension("secrets")
                .with_extension("secrets.collaboration")
        ));
        (tmp, service, bus, target, entry, server)
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_device_retains_late_grant_result_and_account() {
        let (tmp, service, bus, target, entry, server) = device_grant_fixture(false).await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let (release, held) = std::sync::mpsc::channel();
        *entry.commit_test_lease.lock().unwrap() = Some(Arc::new(HeldGrantResult {
            entered: entered.clone(),
            release: std::sync::Mutex::new(held),
        }));
        service
            .identity_connect(
                target.provider().as_wire().into(),
                matches!(target, Target::Gitlab { .. }).then(|| target.host().into()),
                None,
                None,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        assert_eq!(
            entry
                .store
                .load(intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT)
                .unwrap()
                .as_deref(),
            Some("private-grant")
        );
        // timing-guard: cross the legacy ten-second engine wait while its
        // completed physical write is held before returning the actual result.
        tokio::time::sleep(Duration::from_secs(11)).await;
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), service.shutdown_store_writers())
            .await
            .unwrap();
        server.await.unwrap();
        let saved = account(&entry.store).unwrap();
        bus.shutdown().await.unwrap();
        service.store.close().await;
        let reopened = Store::open(&tmp.path).await.unwrap();
        let events = reopened
            .query_events(&intent_store::EventQuery::default())
            .await
            .unwrap();
        reopened.close().await;
        assert!(
            saved.is_some(),
            "late successful grant lost account continuation"
        );
        assert_eq!(saved.unwrap().user["login"], "grant-user");
        assert_eq!(
            events
                .iter()
                .filter(
                    |e| e.event_type == intent_core::events::IDENTITY_AUTH_CHANGED
                        && e.data["status"] == "authorized"
                )
                .count(),
            1
        );
        assert!(!events.iter().any(
            |e| e.event_type == intent_core::events::IDENTITY_AUTH_CHANGED
                && e.data["status"] == "error"
        ));
    }

    async fn assert_device_write_outcomes(github: bool, outcomes: std::ops::Range<u8>) {
        use intent_sourcecontrol::gitlab_token::{
            EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT,
        };
        for outcome in outcomes {
            let (tmp, service, bus, target, entry, server) = device_grant_fixture(github).await;
            let token_key = target.token_account();
            let raw = entry.store.clone();
            for (key, value) in [
                (token_key, "old-token"),
                (REFRESH_SECRET_ACCOUNT, "old-refresh"),
                (EXPIRES_AT_SECRET_ACCOUNT, "1"),
                (ACCOUNT, "old-account"),
            ] {
                raw.store(key, value).unwrap();
            }
            drop(entry);
            let backend = Arc::new(HeldPat {
                store: raw.clone(),
                entered: Arc::new(tokio::sync::Notify::new()),
                release: std::sync::Mutex::new(None),
                arm_worker_failure: std::sync::Mutex::new(None),
                outcome: 0,
            });
            let secrets = crate::settings::AsyncSecretStore::with_timings(
                backend.clone(),
                Duration::from_secs(5),
                Duration::from_secs(5),
                Duration::from_secs(60),
                Duration::from_secs(60),
            );
            let entered = Arc::new(tokio::sync::Notify::new());
            let signal = entered.clone();
            let (release, held) = std::sync::mpsc::channel();
            let partial_store = raw.clone();
            let before: Box<dyn FnOnce() -> Result<()> + Send> = Box::new(move || {
                signal.notify_one();
                let _ = held.recv();
                match outcome {
                    1 => Err(Error::Internal("controlled grant prewrite error".into())),
                    2 => panic!("controlled grant backend panic"),
                    4 => {
                        // Model the engine's non-atomic tuple: one real token
                        // write succeeded before the next operation failed.
                        partial_store.store(token_key, "partial-grant")?;
                        Err(Error::Internal("controlled partial grant write".into()))
                    }
                    _ => Ok(()),
                }
            });
            if github {
                *secrets.before_github_persistence.lock().unwrap() = Some(before);
            } else {
                *secrets.before_gitlab_persistence.lock().unwrap() = Some(before);
            }
            let (fail, failing) = tokio::sync::oneshot::channel();
            if matches!(outcome, 3 | 5) {
                let slot = secrets.panic_mutation_caller.clone();
                *backend.arm_worker_failure.lock().unwrap() = Some(Box::new(move || {
                    *slot.lock().unwrap() = Some(failing);
                }));
            }
            {
                let mut entries = service.collaboration_auth.lock().await;
                Arc::get_mut(entries.values_mut().next().unwrap())
                    .unwrap()
                    .secrets = Arc::new(secrets);
            }
            let entry = service.collaboration_credential(&target).await.unwrap();
            for key in [
                token_key,
                REFRESH_SECRET_ACCOUNT,
                EXPIRES_AT_SECRET_ACCOUNT,
                ACCOUNT,
            ] {
                assert!(entry.secrets.load(key).await.unwrap().is_some());
            }
            service
                .identity_connect(
                    target.provider().as_wire().into(),
                    matches!(target, Target::Gitlab { .. }).then(|| target.host().into()),
                    None,
                    None,
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            server.await.unwrap();
            if matches!(outcome, 3 | 5) {
                fail.send(()).unwrap();
                tokio::time::timeout(
                    Duration::from_secs(5),
                    entry.secrets.gitlab_poll_worker_failed.notified(),
                )
                .await
                .unwrap();
            }
            assert_eq!(entry.secrets.mutation_counts_for_test(), (1, 1));
            assert!(entry.gate.try_lock().is_err());
            assert_eq!(raw.load(token_key).unwrap().as_deref(), Some("old-token"));
            assert_eq!(raw.load(ACCOUNT).unwrap(), None);
            let newer = if outcome == 5 {
                let generation = entry.state.lock().await.generation;
                let owner = service.clone();
                let newer_target = target.clone();
                let revoke = intent_core::spawn_daemon(async move {
                    owner
                        .identity_revoke(
                            newer_target.provider().as_wire().into(),
                            matches!(newer_target, Target::Gitlab { .. })
                                .then(|| newer_target.host().into()),
                        )
                        .await
                });
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        if entry.state.lock().await.generation != generation {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert!(entry.state.lock().await.flow.is_none());
                assert!(!revoke.is_finished());
                assert!(entry.gate.try_lock().is_err());
                Some(revoke)
            } else {
                None
            };
            let (pending, pending_rx) = tokio::sync::oneshot::channel();
            *service.secrets.writer_drain_pending.lock().unwrap() = Some(pending);
            let owner = service.clone();
            let drain =
                intent_core::spawn_daemon(async move { owner.shutdown_store_writers().await });
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), pending_rx)
                    .await
                    .unwrap()
                    .unwrap(),
                if outcome == 5 {
                    "settings-tasks"
                } else if outcome == 3 {
                    "store-tasks"
                } else {
                    "collaboration-state"
                }
            );
            assert!(service.settings_tasks.is_closed());
            assert!(!drain.is_finished());
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), drain)
                .await
                .unwrap()
                .unwrap();
            if let Some(newer) = newer {
                newer.await.unwrap().unwrap();
            }
            assert!(entry.gate.try_lock().is_ok());
            assert_eq!(
                entry.secrets.mutation_counts_for_test(),
                (0, if outcome == 5 { 2 } else { 1 })
            );
            for key in [
                token_key,
                REFRESH_SECRET_ACCOUNT,
                EXPIRES_AT_SECRET_ACCOUNT,
                ACCOUNT,
            ] {
                assert_eq!(
                    entry.secrets.load(key).await.unwrap(),
                    raw.load(key).unwrap(),
                    "stale private cache for {key}"
                );
            }
            assert_eq!(
                raw.load(token_key).unwrap().as_deref(),
                match outcome {
                    0 | 3 => Some("private-grant"),
                    4 => Some("partial-grant"),
                    5 => None,
                    _ => Some("old-token"),
                }
            );
            assert_eq!(
                raw.load(REFRESH_SECRET_ACCOUNT).unwrap().as_deref(),
                match outcome {
                    0 | 3 if !github => Some("private-refresh"),
                    5 => None,
                    _ => Some("old-refresh"),
                }
            );
            assert_eq!(account(&raw).unwrap().is_some(), outcome == 0);
            bus.shutdown().await.unwrap();
            service.store.close().await;
            let reopened = Store::open(&tmp.path).await.unwrap();
            let events = reopened
                .query_events(&intent_store::EventQuery::default())
                .await
                .unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(
                        |e| e.event_type == intent_core::events::IDENTITY_AUTH_CHANGED
                            && e.data["status"] == "authorized"
                    )
                    .count(),
                usize::from(outcome == 0)
            );
            if outcome == 5 {
                let statuses: Vec<_> = events
                    .iter()
                    .filter(|e| e.event_type == intent_core::events::IDENTITY_AUTH_CHANGED)
                    .map(|e| e.data["status"].as_str().unwrap())
                    .collect();
                assert_eq!(
                    statuses,
                    vec!["revoked"],
                    "old grant published after newer owner"
                );
            }
            reopened.close().await;
        }
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_device_retains_prewrite_and_worker_failure_through_drain() {
        assert_device_write_outcomes(false, 0..4).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_device_partial_result_invalidates_private_siblings() {
        assert_device_write_outcomes(false, 4..5).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_device_worker_failure_cannot_publish_over_newer_revoke() {
        assert_device_write_outcomes(false, 5..6).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_github_device_retains_actual_results_through_drain() {
        assert_device_write_outcomes(true, 0..6).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_proof_create_refuses_after_early_close() {
        assert_collaboration_probe_root_refused("proof-create").await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_proof_delete_refuses_after_early_close() {
        assert_collaboration_probe_root_refused("proof-delete").await;
    }

    async fn assert_collaboration_probe_root_refused(operation: &str) {
        let (tmp, service) = fixture().await;
        let bus = crate::EventBus::new(service.store.clone());
        let service = service.with_event_bus(bus.clone());
        let host = "gitlab.closed-probe.test";
        let entry = service
            .collaboration_credential(&Target::Gitlab {
                host: GitlabHost::parse(host).unwrap(),
            })
            .await
            .unwrap();
        service.begin_settings_shutdown();
        let generation = service.identity_rekey_generation.load(Ordering::SeqCst);
        let result = match operation {
            "status" => {
                service
                    .identity_auth_status("gitlab".into(), Some(host.into()))
                    .await
            }
            "user" => {
                service
                    .identity_get_user("gitlab".into(), Some(host.into()))
                    .await
            }
            "select" => {
                service
                    .identity_select("gitlab".into(), Some(host.into()), "42".into())
                    .await
            }
            "proof-create" => {
                service
                    .source_control_identity_proof_create(
                        "gitlab".into(),
                        Some(host.into()),
                        "nonce".into(),
                        "host".into(),
                        Some("collaboration".into()),
                        ForgeUser::on("gitlab", host, "42", "person", None).identity,
                    )
                    .await
            }
            "proof-delete" => {
                service
                    .source_control_identity_proof_delete(
                        "gitlab".into(),
                        Some(host.into()),
                        "77".into(),
                        Some("collaboration".into()),
                    )
                    .await
            }
            _ => unreachable!(),
        };
        let after = service.identity_rekey_generation.load(Ordering::SeqCst);
        service.shutdown_store_writers().await;
        bus.shutdown().await.unwrap();
        service.store.close().await;
        let reopened = Store::open(&tmp.path).await.unwrap();
        let events = reopened
            .query_events(&intent_store::EventQuery::default())
            .await
            .unwrap();
        reopened.close().await;
        assert!(
            matches!(result,Err(Error::Internal(ref message)) if message.contains("shutting down")),
            "collaboration {operation} escaped closed root admission: {result:?}"
        );
        assert_eq!(
            generation, after,
            "refused selection changed identity generation"
        );
        assert_eq!(entry.secrets.mutation_counts_for_test().0, 0);
        assert!(entry.store.load(ACCOUNT).unwrap().is_none());
        assert!(!events
            .iter()
            .any(|e| e.event_type == intent_core::events::IDENTITY_AUTH_CHANGED));
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_status_root_refuses_after_early_close() {
        assert_collaboration_probe_root_refused("status").await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_user_root_refuses_after_early_close() {
        assert_collaboration_probe_root_refused("user").await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_select_root_refuses_after_early_close() {
        assert_collaboration_probe_root_refused("select").await;
    }

    async fn device_start_fixture() -> (
        TempDb,
        Services,
        GitlabHost,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let (tmp, service) = fixture().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = GitlabHost::parse("gitlab.device.test")
            .unwrap()
            .with_api_origin(&format!("http://{}", listener.local_addr().unwrap()))
            .unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let server = tokio::spawn({
            let entered = entered.clone();
            let release = release.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                assert!(line.starts_with("POST /oauth/authorize_device "));
                let mut length = 0;
                loop {
                    line.clear();
                    reader.read_line(&mut line).await.unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((key, value)) = line.split_once(':') {
                        if key.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap();
                        }
                    }
                }
                reader.read_exact(&mut vec![0; length]).await.unwrap();
                entered.notify_one();
                release.notified().await;
                let body=json!({"device_code":"private-code","user_code":"CODE","verification_uri":"https://gitlab.device.test/device","expires_in":900,"interval":3600}).to_string();
                reader.get_mut().write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
            }
        });
        let registry =
            Arc::new(crate::SettingsRegistry::load(tmp.path.with_extension("toml")).unwrap());
        registry
            .apply(&[
                ("sourceControl.gitlab.host".into(), json!(host.host())),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(host.base_url()),
                ),
                (
                    "sourceControl.gitlab.oauthClientId".into(),
                    json!("private-client"),
                ),
            ])
            .unwrap();
        (
            tmp,
            service.with_settings_registry(registry),
            host,
            entered,
            release,
            server,
        )
    }

    async fn assert_device_start_refused(close_during_http: bool, final_close: bool) {
        let (_tmp, service, host, entered, release, server) = device_start_fixture().await;
        let entry = service
            .collaboration_credential(&Target::Gitlab { host: host.clone() })
            .await
            .unwrap();
        if !close_during_http {
            if final_close {
                service.shutdown_store_writers().await;
            } else {
                service.begin_settings_shutdown();
            }
            release.notify_one();
        }
        let generation = entry.state.lock().await.generation;
        let worker = service.clone();
        let caller = intent_core::spawn_daemon(async move {
            worker
                .identity_connect("gitlab".into(), Some(host.host().into()), None, None)
                .await
        });
        if close_during_http {
            tokio::time::timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            service.begin_settings_shutdown();
            release.notify_one();
        }
        let result = tokio::time::timeout(Duration::from_secs(5), caller)
            .await
            .unwrap()
            .unwrap();
        server.abort();
        let _ = server.await;
        let has_flow = entry.state.lock().await.flow.is_some();
        let after = entry.state.lock().await.generation;
        service.shutdown_store_writers().await;
        service.store.close().await;
        assert!(
            matches!(result,Err(Error::Internal(ref message)) if message.contains("shutting down")),
            "collaboration device flow escaped closed admission: {result:?}"
        );
        assert!(!has_flow);
        if !close_during_http {
            assert_eq!(generation, after);
        }
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_device_refuses_after_early_close() {
        assert_device_start_refused(false, false).await;
    }
    #[intent_test_macros::daemon_test]
    async fn collaboration_device_refuses_after_writer_close() {
        assert_device_start_refused(false, true).await;
    }
    #[intent_test_macros::daemon_test]
    async fn collaboration_device_refuses_when_shutdown_wins_http_start() {
        assert_device_start_refused(true, false).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_device_shutdown_joins_an_actually_sleeping_poll() {
        let (_tmp, service, host, _entered, release, server) = device_start_fixture().await;
        let entry = service
            .collaboration_credential(&Target::Gitlab { host: host.clone() })
            .await
            .unwrap();
        let (pending, pending_rx) = tokio::sync::oneshot::channel();
        *entry.sleep_pending.lock().unwrap() = Some(pending);
        release.notify_one();
        let result = service
            .identity_connect("gitlab".into(), Some(host.host().into()), None, None)
            .await
            .unwrap();
        assert_eq!(result["ok"], true);
        server.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), pending_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(entry.state.lock().await.flow.is_some());
        tokio::time::timeout(Duration::from_secs(5), service.shutdown_store_writers())
            .await
            .unwrap();
        assert!(entry.state.lock().await.flow.is_none());
        assert!(service.store_tasks.is_closed());
        assert!(entry
            .store
            .load(intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT)
            .unwrap()
            .is_none());
        service.store.close().await;
    }

    async fn assert_pat_refused_after_close(final_close: bool) {
        let (tmp, service) = fixture().await;
        let (host, server) = crate::source_control_auth_ops::startup_tests::pat_host().await;
        let registry =
            Arc::new(crate::SettingsRegistry::load(tmp.path.with_extension("toml")).unwrap());
        registry
            .apply(&[
                ("sourceControl.gitlab.host".into(), json!(host.host())),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(host.base_url()),
                ),
            ])
            .unwrap();
        let service = service.with_settings_registry(registry);
        let target = Target::Gitlab { host: host.clone() };
        let entry = service.collaboration_credential(&target).await.unwrap();
        entry
            .store
            .store(target.token_account(), "saved-private-token")
            .unwrap();
        entry.store.store(ACCOUNT, "saved-private-account").unwrap();
        if final_close {
            service.shutdown_store_writers().await;
        } else {
            service.begin_settings_shutdown();
        }
        let generation = entry.state.lock().await.generation;
        let result = service
            .identity_connect(
                "gitlab".into(),
                Some(host.host().into()),
                Some("pat".into()),
                Some("valid-pat".into()),
            )
            .await;
        server.abort();
        let _ = server.await;
        let token = entry.store.load(target.token_account()).unwrap();
        let account = entry.store.load(ACCOUNT).unwrap();
        let after_generation = entry.state.lock().await.generation;
        service.shutdown_store_writers().await;
        service.store.close().await;
        assert!(
            matches!(result, Err(Error::Internal(ref message)) if message.contains("shutting down")),
            "collaboration PAT escaped closed admission: {result:?}"
        );
        assert_eq!(token.as_deref(), Some("saved-private-token"));
        assert_eq!(account.as_deref(), Some("saved-private-account"));
        assert_eq!(generation, after_generation);
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_pat_refuses_after_early_close() {
        assert_pat_refused_after_close(false).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_pat_refuses_after_writer_close() {
        assert_pat_refused_after_close(true).await;
    }

    async fn assert_revoke_refused_after_close(final_close: bool) {
        let (tmp, service) = fixture().await;
        let bus = crate::EventBus::new(service.store.clone());
        let service = service.with_event_bus(bus.clone());
        let entry = service
            .collaboration_credential(&Target::Github)
            .await
            .unwrap();
        entry
            .store
            .store(github_auth_ops::SECRET_ACCOUNT, "saved-private-token")
            .unwrap();
        entry.store.store(ACCOUNT, "saved-private-account").unwrap();
        entry.state.lock().await.flow = Some(FlowSlot {
            flow_id: github_auth_ops::next_flow_id(),
            user_code: "saved-code".into(),
            verification_uri: "https://github.com/login/device".into(),
            interval: 60,
            deadline: Instant::now() + Duration::from_secs(900),
            phase: FlowPhase::Pending,
        });
        if final_close {
            service.shutdown_store_writers().await;
        } else {
            service.begin_settings_shutdown();
        }
        let before = {
            let state = entry.state.lock().await;
            (
                state.generation,
                state
                    .flow
                    .as_ref()
                    .map(|slot| (slot.flow_id, slot.phase.as_wire())),
            )
        };
        let result = service.identity_revoke("github".into(), None).await;
        let token = entry.store.load(github_auth_ops::SECRET_ACCOUNT).unwrap();
        let account = entry.store.load(ACCOUNT).unwrap();
        let after = {
            let state = entry.state.lock().await;
            (
                state.generation,
                state
                    .flow
                    .as_ref()
                    .map(|slot| (slot.flow_id, slot.phase.as_wire())),
            )
        };
        assert_eq!(before, after);
        assert_eq!(entry.secrets.mutation_counts_for_test().0, 0);
        service.shutdown_store_writers().await;
        bus.shutdown().await.unwrap();
        service.store.close().await;
        let reopened = Store::open(&tmp.path).await.unwrap();
        assert!(!reopened
            .query_events(&intent_store::EventQuery::default())
            .await
            .unwrap()
            .iter()
            .any(|e| e.event_type == intent_core::events::IDENTITY_AUTH_CHANGED));
        reopened.close().await;
        assert!(
            matches!(result, Err(Error::Internal(ref message)) if message.contains("shutting down")),
            "collaboration revoke escaped closed admission: {result:?}"
        );
        assert_eq!(token.as_deref(), Some("saved-private-token"));
        assert_eq!(account.as_deref(), Some("saved-private-account"));
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_revoke_refuses_after_early_close() {
        assert_revoke_refused_after_close(false).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_revoke_refuses_after_writer_close() {
        assert_revoke_refused_after_close(true).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_direct_endpoints_refuse_members_before_admission() {
        let (tmp, service) = fixture().await;
        let (host, server) = crate::source_control_auth_ops::startup_tests::pat_host().await;
        let registry =
            Arc::new(crate::SettingsRegistry::load(tmp.path.with_extension("toml")).unwrap());
        registry
            .apply(&[
                ("sourceControl.gitlab.host".into(), json!(host.host())),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(host.base_url()),
                ),
            ])
            .unwrap();
        let bus = crate::EventBus::new(service.store.clone());
        let service = service
            .with_settings_registry(registry)
            .with_event_bus(bus.clone());
        for target in [Target::Github, Target::Gitlab { host: host.clone() }] {
            let entry = service.collaboration_credential(&target).await.unwrap();
            entry
                .store
                .store(target.token_account(), "saved-private-token")
                .unwrap();
            entry.store.store(ACCOUNT, "saved-private-account").unwrap();
            with_caller(
                Caller::Wire {
                    principal_id: PrincipalId::new(),
                    host_role: HostRole::Member,
                },
                async {
                    let result = service
                        .identity_revoke(
                            target.provider().as_wire().into(),
                            matches!(target, Target::Gitlab { .. }).then(|| target.host().into()),
                        )
                        .await;
                    assert!(matches!(result, Err(Error::Forbidden(_))), "{result:?}");
                    if matches!(target, Target::Gitlab { .. }) {
                        let result = service
                            .identity_connect(
                                "gitlab".into(),
                                Some(host.host().into()),
                                Some("pat".into()),
                                Some("valid-pat".into()),
                            )
                            .await;
                        assert!(matches!(result, Err(Error::Forbidden(_))), "{result:?}");
                    }
                },
            )
            .await;
            assert_eq!(entry.secrets.mutation_counts_for_test(), (0, 0));
            assert_eq!(entry.state.lock().await.generation, 0);
            assert_eq!(
                entry.store.load(target.token_account()).unwrap().as_deref(),
                Some("saved-private-token")
            );
            assert_eq!(
                entry.store.load(ACCOUNT).unwrap().as_deref(),
                Some("saved-private-account")
            );
        }
        server.abort();
        let _ = server.await;
        service.shutdown_store_writers().await;
        bus.shutdown().await.unwrap();
        service.store.close().await;
        let reopened = Store::open(&tmp.path).await.unwrap();
        assert!(!reopened
            .query_events(&intent_store::EventQuery::default())
            .await
            .unwrap()
            .iter()
            .any(|e| e.event_type == intent_core::events::IDENTITY_AUTH_CHANGED));
        reopened.close().await;
    }

    struct HeldPat {
        store: FileSecretStore,
        entered: Arc<tokio::sync::Notify>,
        release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        arm_worker_failure: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
        outcome: u8,
    }

    impl crate::settings::SecretStore for HeldPat {
        fn load(&self, key: &str) -> Result<Option<String>> {
            self.store.load(key)
        }
        fn store(&self, key: &str, value: &str) -> Result<()> {
            if key == intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT {
                let held = self.release.lock().unwrap().take();
                if let Some(held) = held {
                    self.entered.notify_one();
                    let _ = held.recv();
                    self.store.store(key, value)?;
                    match self.outcome {
                        1 => return Err(Error::Internal("controlled partial PAT write".into())),
                        2 => panic!("controlled partial PAT backend panic"),
                        _ => return Ok(()),
                    }
                }
            }
            self.store.store(key, value)
        }
        fn delete(&self, key: &str) -> Result<()> {
            self.store.delete(key)?;
            if key == ACCOUNT {
                if let Some(arm) = self.arm_worker_failure.lock().unwrap().take() {
                    arm();
                }
            }
            Ok(())
        }
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_pat_retains_partial_results_and_guard_through_shutdown() {
        use intent_sourcecontrol::gitlab_token::{
            EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT,
        };
        for outcome in 0..5 {
            let (tmp, service) = fixture().await;
            let (host, server) = crate::source_control_auth_ops::startup_tests::pat_host().await;
            let registry =
                Arc::new(crate::SettingsRegistry::load(tmp.path.with_extension("toml")).unwrap());
            registry
                .apply(&[
                    ("sourceControl.gitlab.host".into(), json!(host.host())),
                    (
                        "sourceControl.gitlab.apiBaseUrl".into(),
                        json!(host.base_url()),
                    ),
                ])
                .unwrap();
            let bus = crate::EventBus::new(service.store.clone());
            let service = service
                .with_settings_registry(registry)
                .with_event_bus(bus.clone());
            let target = Target::Gitlab { host: host.clone() };
            let entry = service.collaboration_credential(&target).await.unwrap();
            let raw = entry.store.clone();
            for (key, value) in [
                (SECRET_ACCOUNT, "old-token"),
                (REFRESH_SECRET_ACCOUNT, "old-refresh"),
                (EXPIRES_AT_SECRET_ACCOUNT, "9999999999"),
                (ACCOUNT, "old-account"),
            ] {
                raw.store(key, value).unwrap();
            }
            drop(entry);
            let entered = Arc::new(tokio::sync::Notify::new());
            let (release, held) = std::sync::mpsc::channel();
            let backend = Arc::new(HeldPat {
                store: raw.clone(),
                entered: entered.clone(),
                release: std::sync::Mutex::new(Some(held)),
                arm_worker_failure: std::sync::Mutex::new(None),
                outcome,
            });
            let secrets = crate::settings::AsyncSecretStore::with_timings(
                backend.clone(),
                Duration::from_secs(5),
                if matches!(outcome, 0 | 3) {
                    Duration::from_secs(5)
                } else {
                    Duration::from_millis(10)
                },
                Duration::from_secs(60),
                Duration::from_secs(60),
            );
            let (fail, failing) = tokio::sync::oneshot::channel();
            if outcome == 3 {
                let slot = secrets.panic_mutation_caller.clone();
                *backend.arm_worker_failure.lock().unwrap() = Some(Box::new(move || {
                    *slot.lock().unwrap() = Some(failing);
                }));
            }
            {
                let mut entries = service.collaboration_auth.lock().await;
                Arc::get_mut(entries.values_mut().next().unwrap())
                    .unwrap()
                    .secrets = Arc::new(secrets);
            }
            let entry = service.collaboration_credential(&target).await.unwrap();
            for key in [
                SECRET_ACCOUNT,
                REFRESH_SECRET_ACCOUNT,
                EXPIRES_AT_SECRET_ACCOUNT,
                ACCOUNT,
            ] {
                assert!(entry.secrets.load(key).await.unwrap().is_some());
            }
            let worker = service.clone();
            let caller = intent_core::spawn_daemon(async move {
                worker
                    .identity_connect(
                        "gitlab".into(),
                        Some(host.host().into()),
                        Some("pat".into()),
                        Some("new-pat".into()),
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            server.await.unwrap();
            if outcome == 0 {
                caller.abort();
                assert!(caller.await.unwrap_err().is_cancelled());
            } else if outcome == 3 {
                fail.send(()).unwrap();
                let error = tokio::time::timeout(Duration::from_secs(5), caller)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                assert!(
                    error.to_string().contains("PAT failed"),
                    "expected worker failure: {error}"
                );
            } else {
                let error = tokio::time::timeout(Duration::from_secs(5), caller)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                assert!(error.to_string().contains("timed out"));
            }
            assert!(entry.gate.try_lock().is_err());
            assert_eq!(entry.secrets.mutation_counts_for_test(), (1, 1));
            assert_eq!(
                raw.load(SECRET_ACCOUNT).unwrap().as_deref(),
                Some("old-token")
            );
            assert!(raw.load(ACCOUNT).unwrap().is_none());
            let (pending, pending_rx) = tokio::sync::oneshot::channel();
            *service.secrets.writer_drain_pending.lock().unwrap() = Some(pending);
            let worker = service.clone();
            let drain =
                intent_core::spawn_daemon(async move { worker.shutdown_store_writers().await });
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), pending_rx)
                    .await
                    .unwrap()
                    .unwrap(),
                "settings-tasks"
            );
            assert!(!drain.is_finished());
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), drain)
                .await
                .unwrap()
                .unwrap();
            assert!(entry.gate.try_lock().is_ok());
            assert_eq!(
                entry.secrets.load(SECRET_ACCOUNT).await.unwrap().as_deref(),
                Some("new-pat")
            );
            for key in [REFRESH_SECRET_ACCOUNT, EXPIRES_AT_SECRET_ACCOUNT] {
                assert_eq!(
                    entry.secrets.load(key).await.unwrap().is_none(),
                    matches!(outcome, 0 | 4)
                );
            }
            assert_eq!(
                entry.secrets.load(ACCOUNT).await.unwrap().is_some(),
                matches!(outcome, 0 | 4)
            );
            assert_eq!(entry.secrets.mutation_counts_for_test(), (0, 1));
            bus.shutdown().await.unwrap();
            service.store.close().await;
            let reopened = Store::open(&tmp.path).await.unwrap();
            let events = reopened
                .query_events(&intent_store::EventQuery::default())
                .await
                .unwrap();
            reopened.close().await;
            assert_eq!(
                events
                    .iter()
                    .filter(
                        |e| e.event_type == intent_core::events::IDENTITY_AUTH_CHANGED
                            && e.data["status"] == "authorized"
                    )
                    .count(),
                usize::from(matches!(outcome, 0 | 4))
            );
        }
    }

    struct HeldRevoke {
        store: FileSecretStore,
        entered: Arc<tokio::sync::Notify>,
        release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        outcome: u8,
    }

    impl crate::settings::SecretStore for HeldRevoke {
        fn load(&self, key: &str) -> Result<Option<String>> {
            self.store.load(key)
        }
        fn store(&self, key: &str, value: &str) -> Result<()> {
            self.store.store(key, value)
        }
        fn delete(&self, key: &str) -> Result<()> {
            let held = self.release.lock().unwrap().take();
            if let Some(held) = held {
                self.entered.notify_one();
                let _ = held.recv();
                match self.outcome {
                    1 => return Err(Error::InvalidParams("controlled revoke failure".into())),
                    2 => panic!("controlled revoke backend panic"),
                    _ => {}
                }
            }
            self.store.delete(key)
        }
    }

    async fn assert_revoke_retains_results_and_guard(target: Target) {
        use std::future::Future;
        for outcome in 0..5 {
            let (tmp, service) = fixture().await;
            let bus = crate::EventBus::new(service.store.clone());
            let service = service.with_event_bus(bus.clone());
            let entry = service.collaboration_credential(&target).await.unwrap();
            let raw = entry.store.clone();
            let token_key = target.token_account();
            let mut keys = vec![token_key, ACCOUNT];
            if matches!(target, Target::Gitlab { .. }) {
                keys.extend([
                    intent_sourcecontrol::gitlab_token::REFRESH_SECRET_ACCOUNT,
                    intent_sourcecontrol::gitlab_token::EXPIRES_AT_SECRET_ACCOUNT,
                ]);
            }
            for key in &keys {
                raw.store(key, "old-private-value").unwrap();
            }
            drop(entry);
            let entered = Arc::new(tokio::sync::Notify::new());
            let (release, held) = std::sync::mpsc::channel();
            let secrets = crate::settings::AsyncSecretStore::with_timings(
                Arc::new(HeldRevoke {
                    store: raw.clone(),
                    entered: entered.clone(),
                    release: std::sync::Mutex::new(Some(held)),
                    outcome,
                }),
                Duration::from_secs(5),
                if matches!(outcome, 0 | 3) {
                    Duration::from_secs(5)
                } else {
                    Duration::from_millis(10)
                },
                Duration::from_secs(60),
                Duration::from_secs(60),
            );
            {
                let mut entries = service.collaboration_auth.lock().await;
                Arc::get_mut(entries.values_mut().next().unwrap())
                    .unwrap()
                    .secrets = Arc::new(secrets);
            }
            let entry = service.collaboration_credential(&target).await.unwrap();
            let (fail, failing) = tokio::sync::oneshot::channel();
            if outcome == 3 {
                *entry.secrets.panic_mutation_caller.lock().unwrap() = Some(failing);
            }
            for key in &keys {
                assert_eq!(
                    entry.secrets.load(key).await.unwrap().as_deref(),
                    Some("old-private-value")
                );
            }
            let worker = service.clone();
            let provider = target.provider().as_wire().to_owned();
            let host = matches!(target, Target::Gitlab { .. }).then(|| target.host().to_owned());
            let caller =
                intent_core::spawn_daemon(
                    async move { worker.identity_revoke(provider, host).await },
                );
            tokio::time::timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            if outcome == 0 {
                caller.abort();
                assert!(caller.await.unwrap_err().is_cancelled());
            } else if outcome == 3 {
                fail.send(()).unwrap();
                let error = tokio::time::timeout(Duration::from_secs(5), caller)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                assert!(error.to_string().contains("revoke failed"));
            } else {
                let error = tokio::time::timeout(Duration::from_secs(5), caller)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                assert!(error.to_string().contains("timed out"));
            }
            assert!(entry.gate.try_lock().is_err());
            assert_eq!(
                raw.load(token_key).unwrap().as_deref(),
                Some("old-private-value")
            );
            assert_eq!(entry.secrets.mutation_counts_for_test(), (1, 1));
            let (pending, pending_rx) = tokio::sync::oneshot::channel();
            *service.secrets.writer_drain_pending.lock().unwrap() = Some(pending);
            let worker = service.clone();
            let mut drain = Box::pin(intent_core::spawn_daemon(async move {
                worker.shutdown_store_writers().await;
            }));
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), pending_rx)
                    .await
                    .unwrap()
                    .unwrap(),
                "settings-tasks"
            );
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    drain.as_mut().poll(cx).is_pending()
                ))
                .await
            );
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), drain)
                .await
                .unwrap()
                .unwrap();
            assert!(entry.gate.try_lock().is_ok());
            assert_eq!(entry.secrets.mutation_counts_for_test(), (0, 1));
            for key in &keys {
                let removed = matches!(outcome, 0 | 4) || (outcome == 3 && *key == token_key);
                assert_eq!(
                    raw.load(key).unwrap().is_none(),
                    removed,
                    "raw {key}, outcome {outcome}"
                );
                assert_eq!(
                    entry.secrets.load(key).await.unwrap().is_none(),
                    removed,
                    "cached {key}, outcome {outcome}"
                );
            }
            bus.shutdown().await.unwrap();
            service.store.close().await;
            let reopened = Store::open(&tmp.path).await.unwrap();
            let events = reopened
                .query_events(&intent_store::EventQuery::default())
                .await
                .unwrap();
            reopened.close().await;
            assert_eq!(
                events
                    .iter()
                    .filter(
                        |e| e.event_type == intent_core::events::IDENTITY_AUTH_CHANGED
                            && e.data["status"] == "revoked"
                    )
                    .count(),
                usize::from(matches!(outcome, 0 | 4))
            );
        }
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_revoke_retains_results_and_guard_through_shutdown() {
        assert_revoke_retains_results_and_guard(Target::Github).await;
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_gitlab_revoke_retains_siblings_and_receipts_through_shutdown() {
        assert_revoke_retains_results_and_guard(
            Services::collaboration_target("gitlab", Some("private-revoke.test")).unwrap(),
        )
        .await;
    }

    #[intent_test_macros::daemon_test]
    async fn stores_are_primary_provider_instance_and_purpose_scoped() {
        let (tmp, service) = fixture().await;
        let github = service
            .collaboration_credential(&Target::Github)
            .await
            .unwrap();
        let gl = Services::collaboration_target("gitlab", None).unwrap();
        let gitlab = service.collaboration_credential(&gl).await.unwrap();
        let other_target =
            Services::collaboration_target("gitlab", Some("Other.Example:9443")).unwrap();
        let other = service
            .collaboration_credential(&other_target)
            .await
            .unwrap();
        assert_ne!(github.store.path(), gitlab.store.path());
        assert_ne!(gitlab.store.path(), other.store.path());
        assert_eq!(other_target.host(), "other.example:9443");
        github
            .store
            .store(github_auth_ops::SECRET_ACCOUNT, "identity-only")
            .unwrap();
        // The safe execution read uses repository-purpose stores, even
        // when the host has a real collaboration-purpose credential file.
        let registry =
            Arc::new(crate::SettingsRegistry::load(tmp.path.with_extension("toml")).unwrap());
        registry
            .apply(&[("sourceControl.github.tokenSource".into(), json!("explicit"))])
            .unwrap();
        let execution = service
            .clone()
            .with_settings_registry(registry)
            .with_secret_store(Arc::new(crate::settings::InMemorySecretStore::default()));
        let context = execution.host_execution_context().await.unwrap();
        assert_eq!(context["repositoryConnections"][0]["configured"], false);
        assert!(!context.to_string().contains("identity-only"));
        assert!(service
            .gitlab_secret_store
            .load(github_auth_ops::SECRET_ACCOUNT)
            .unwrap()
            .is_none());
        assert!(gitlab
            .store
            .load(github_auth_ops::SECRET_ACCOUNT)
            .unwrap()
            .is_none());
        service
            .gitlab_secret_store
            .store(github_auth_ops::SECRET_ACCOUNT, "repository")
            .unwrap();
        let _ = with_caller(
            Caller::Daemon,
            service.identity_revoke("github".into(), None),
        )
        .await
        .unwrap();
        assert_eq!(
            service
                .gitlab_secret_store
                .load(github_auth_ops::SECRET_ACCOUNT)
                .unwrap()
                .as_deref(),
            Some("repository")
        );
        // Another primary using the same machine's secret file cannot resolve it.
        let second_store = Store::open(&tmp.path.with_extension("other.db"))
            .await
            .unwrap();
        let second =
            Services::new_with_file_secrets(second_store, service.gitlab_secret_store.clone());
        let second_entry = second
            .collaboration_credential(&Target::Github)
            .await
            .unwrap();
        assert_ne!(github.store.path(), second_entry.store.path());
        assert!(second_entry
            .store
            .load(github_auth_ops::SECRET_ACCOUNT)
            .unwrap()
            .is_none());
    }

    #[intent_test_macros::daemon_test]
    async fn explicit_selection_never_merges_principals_or_rotates_credentials() {
        let (_tmp, service) = fixture().await;
        let primary = service.store.get_primary_principal().await.unwrap();
        let credential = "e".repeat(64);
        service
            .store
            .insert_principal_credential(&primary.id, &credential)
            .await
            .unwrap();
        let chosen = ForgeUser::on("gitlab", "gitlab.example", "42", "person", None);
        let selected = service
            .select_primary_forge_identity_locked(primary.clone(), &chosen)
            .await
            .unwrap();
        assert_eq!(selected.id, primary.id);
        assert_eq!(selected.identity_key(), chosen.identity);
        let mut guest = selected.clone();
        guest.id = PrincipalId::new();
        guest.is_primary = false;
        guest.set_identity(PrincipalIdentity::github(42));
        service.store.upsert_principal(&guest).await.unwrap();
        assert!(matches!(
            service
                .select_primary_forge_identity_locked(
                    selected.clone(),
                    &ForgeUser::on("github", "github.com", "42", "person", None)
                )
                .await,
            Err(Error::IdentityInUse)
        ));
        assert_eq!(
            service
                .store
                .get_primary_principal()
                .await
                .unwrap()
                .identity_key(),
            selected.identity_key()
        );
        assert_eq!(
            service.store.get_host_role(&primary.id).await.unwrap(),
            HostRole::Owner
        );
        let preserved = service
            .store
            .lookup_principal_credential(&credential)
            .await
            .unwrap()
            .unwrap();
        assert!(preserved.is_active());
        assert_eq!(preserved.principal_id, primary.id);
    }

    #[intent_test_macros::daemon_test]
    async fn repository_reconnect_and_refresh_cannot_replace_selected_identity() {
        let (_tmp, service) = fixture().await;
        let service = service.with_source_control(Arc::new(StubForge::default()));
        let primary = service.store.get_primary_principal().await.unwrap();
        let chosen = ForgeUser::on("gitlab", "gitlab.example", "42", "person", None);
        let selected = service
            .select_primary_forge_identity_locked(primary, &chosen)
            .await
            .unwrap();
        let guard = service.connect_identity_guard();
        drop(
            guard(Arc::new(StubForge::default()))
                .await
                .expect("repo connect does not select GitHub"),
        );
        assert_eq!(
            service
                .store
                .get_primary_principal()
                .await
                .unwrap()
                .identity_key(),
            chosen.identity
        );
        let refreshed = service.refresh_primary_identity(selected).await.unwrap();
        assert_eq!(refreshed.identity_key(), chosen.identity);
    }

    #[test]
    fn granted_permissions_are_observed_not_inferred() {
        assert!(check_scopes(&Target::Github, None).is_ok());
        assert!(check_scopes(
            &Target::Github,
            Some(&["gist".into(), "repo".into(), "workflow".into()])
        )
        .is_ok());
        assert!(matches!(
            check_scopes(&Target::Github, Some(&["repo".into()])),
            Err(Error::IdentityProof(IdentityProofErrorKind::ScopeMissing))
        ));
        assert_eq!(Target::Github.scopes(), &["gist"]);
        let target = Target::Gitlab {
            host: gitlab_auth::GitlabHost::parse("gitlab.com").unwrap(),
        };
        assert!(matches!(
            check_scopes(&target, Some(&["read_user".into()])),
            Err(Error::IdentityProof(
                IdentityProofErrorKind::GitlabScopeMissing
            ))
        ));
    }

    #[tokio::test]
    async fn cancelled_io_retains_the_gate_until_blocking_persistence_finishes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileSecretStore::with_path(tmp.path().join("isolated.json"));
        let gate = Arc::new(Mutex::new(()));
        let lease: PersistenceLease = Arc::new(gate.clone().lock_owned().await);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            io(&store, lease, move |s| {
                let _ = started_tx.send(());
                release_rx.recv().unwrap();
                s.store(ACCOUNT, "private")
            })
            .await
        });
        started_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(
            gate.try_lock().is_err(),
            "cancelled RPC cannot release a live write's lease"
        );
        release_tx.send(()).unwrap();
        let _settled = tokio::time::timeout(Duration::from_secs(5), gate.lock())
            .await
            .unwrap();
        assert_eq!(
            FileSecretStore::with_path(tmp.path().join("isolated.json"))
                .load(ACCOUNT)
                .unwrap()
                .as_deref(),
            Some("private")
        );
    }

    #[intent_test_macros::daemon_test]
    async fn collaboration_account_and_proof_receipt_survive_service_restart() {
        let (_tmp, service) = fixture().await;
        let entry = service
            .collaboration_credential(&Target::Github)
            .await
            .unwrap();
        let account = Account::new(
            ForgeUser::on("github", "github.com", "42", "person", None),
            json!({"id":"42","login":"person"}),
            "device",
            Some(vec!["gist".into()]),
        )
        .unwrap();
        let lease: PersistenceLease = Arc::new(entry.gate.clone().lock_owned().await);
        save_account(&entry.secrets, &account).await.unwrap();
        drop(lease);
        entry
            .store
            .store("identity.proof.abcdef", &account.generation)
            .unwrap();
        let restarted = Services::new_with_file_secrets(
            service.store.clone(),
            service.gitlab_secret_store.clone(),
        );
        let reloaded = restarted
            .collaboration_credential(&Target::Github)
            .await
            .unwrap();
        let saved = super::account(&reloaded.store).unwrap().unwrap();
        assert_eq!(saved.identity, account.identity);
        assert_eq!(
            reloaded.store.load("identity.proof.abcdef").unwrap(),
            Some(saved.generation)
        );
        assert!(service.gitlab_secret_store.load(ACCOUNT).unwrap().is_none());
    }
}
