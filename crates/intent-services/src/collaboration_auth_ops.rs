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
    gate: Arc<Mutex<()>>,
    state: Mutex<State>,
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
    entry: &Credential,
    lease: PersistenceLease,
    account: &Account,
) -> Result<()> {
    let data = serde_json::to_string(account).map_err(|e| Error::Internal(e.to_string()))?;
    io(&entry.store, lease, move |store| {
        store.store(ACCOUNT, &data)
    })
    .await
}

async fn clear(entry: &Credential, lease: PersistenceLease, target: &Target) -> Result<bool> {
    let token_key = target.token_account();
    io(&entry.store, lease, move |store| {
        let present = store.load(token_key)?.is_some();
        for key in [
            token_key,
            ACCOUNT,
            intent_sourcecontrol::gitlab_token::REFRESH_SECRET_ACCOUNT,
            intent_sourcecontrol::gitlab_token::EXPIRES_AT_SECRET_ACCOUNT,
        ] {
            store.delete(key)?;
        }
        Ok(present)
    })
    .await
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
                Arc::new(Credential {
                    store: FileSecretStore::with_path(
                        std::path::PathBuf::from(directory).join(format!("{digest}.json")),
                    ),
                    gate: Arc::new(Mutex::new(())),
                    state: Mutex::new(State::default()),
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
    ) -> Result<bool> {
        let Target::Gitlab { host } = target else {
            return Ok(false);
        };
        match gitlab_auth::refresh_access_token_with_lease(
            host,
            client_id,
            entry.store.clone(),
            Some(lease.clone()),
        )
        .await
        {
            Ok(scopes) => {
                if let Some(mut saved) = io(&entry.store, lease.clone(), |s| account(&s)).await? {
                    saved.scopes = scopes;
                    save_account(entry, lease, &saved).await?;
                }
                Ok(true)
            }
            Err(intent_sourcecontrol::Error::Auth(_)) => {
                clear(entry, lease, target).await?;
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
                .collaboration_refresh(entry, lease.clone(), &target, saved.device_client_id()?)
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
                    save_account(entry, lease, &saved).await?;
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
                    clear(entry, lease, &target).await?;
                    self.collaboration_event(&target, "expired", None).await;
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    pub(crate) async fn collaboration_status(
        &self,
        provider: &str,
        host: Option<&str>,
        only_user: bool,
    ) -> Result<Value> {
        let target = Self::collaboration_target(provider, host)?;
        let entry = self.collaboration_credential(&target).await?;
        let lease: PersistenceLease = Arc::new(entry.gate.clone().lock_owned().await);
        let probed = self
            .collaboration_probe(&entry, lease.clone(), &target)
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
            !state.unsupported && self.collaboration_client_id(&target).is_some(),
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
        // Invalidate start/exchange immediately, then serialize deletion with IO.
        {
            let mut state = entry.state.lock().await;
            state.generation += 1;
            state.flow = None;
        }
        let lease: PersistenceLease = Arc::new(entry.gate.clone().lock_owned().await);
        if clear(&entry, lease.clone(), &target).await? {
            self.collaboration_event(&target, "revoked", None).await;
        }
        Ok(json!({"ok":true}))
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
        let generation = {
            let mut state = entry.state.lock().await;
            if method == "device" && state.flow.as_ref().is_some_and(FlowSlot::is_live) {
                return Ok(flow_response(&state));
            }
            state.generation += 1;
            state.flow = None;
            state.generation
        };
        if let Some(token) = pat_token {
            let account = self.collaboration_verify(&target, &token, "pat").await?;
            let lease: PersistenceLease = Arc::new(entry.gate.clone().lock_owned().await);
            let state = entry.state.lock().await;
            if state.generation != generation {
                return Err(Error::IdentityMismatch);
            }
            // Fail closed if either subsequent write fails: a new token must
            // never inherit the previous account's proof-cleanup receipts.
            io(&entry.store, lease.clone(), |s| s.delete(ACCOUNT)).await?;
            gitlab_auth::persist_gitlab_token_with_lease(
                entry.store.clone(),
                token.into(),
                lease.clone(),
            )
            .await
            .map_err(crate::pr_ops::map_sc_err)?;
            save_account(&entry, lease.clone(), &account).await?;
            self.collaboration_event(&target, "authorized", None).await;
            return Ok(json!({"ok":true,"method":"pat","purpose":"collaboration"}));
        }
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
                intent_sourcecontrol::device_flow::start_at(&base, &client_id, target.scopes())
                    .await
                    .map(|(auth, flow)| {
                        (
                            auth.user_code,
                            auth.verification_uri,
                            auth.expires_in,
                            auth.interval,
                            Flow::Github(flow.with_store(entry.store.clone())),
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
        let (user_code, verification_uri, expires, interval, flow) = match started {
            Ok(r) => r,
            Err(intent_sourcecontrol::Error::DeviceGrantUnsupported(_)) => {
                let mut state = entry.state.lock().await;
                if state.generation != generation {
                    return Err(Error::IdentityMismatch);
                }
                state.unsupported = true;
                return Err(unsupported());
            }
            Err(e) => return Err(crate::pr_ops::map_sc_err(e)),
        };
        let mut state = entry.state.lock().await;
        if state.generation != generation {
            return Err(Error::IdentityMismatch);
        }
        state.unsupported = false;
        let deadline = Instant::now() + Duration::from_secs(expires);
        state.flow_id = uuid::Uuid::new_v4().to_string();
        state.flow = Some(FlowSlot {
            flow_id: github_auth_ops::next_flow_id(),
            user_code,
            verification_uri,
            interval,
            deadline,
            phase: FlowPhase::Pending,
        });
        let result = flow_response(&state);
        drop(state);
        let service = self.clone();
        intent_core::spawn_daemon(async move {
            service
                .collaboration_poll(entry, target, generation, deadline, flow)
                .await;
        });
        Ok(result)
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
                tokio::time::sleep(github_auth_ops::poll_sleep(flow.interval()).min(remaining))
                    .await;
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
            let mut state = entry.state.lock().await;
            if state.generation != generation || state.flow.is_none() {
                return;
            }
            let phase = match (outcome, verified) {
                (Exchange::Grant(grant), Some(Ok(account))) => {
                    let prepared = io(&entry.store, lease.clone(), |s| s.delete(ACCOUNT)).await;
                    if prepared.is_ok()
                        && grant.commit(lease.clone()).await.is_ok()
                        && save_account(&entry, lease.clone(), &account).await.is_ok()
                    {
                        None
                    } else {
                        Some(FlowPhase::Error)
                    }
                }
                (Exchange::Terminal(phase), _) => Some(phase),
                _ => Some(FlowPhase::Error),
            };
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
        let target = Self::collaboration_target(provider, host)?;
        let entry = self.collaboration_credential(&target).await?;
        // Reserve the explicit choice before a network probe. A later explicit
        // setting/select supersedes this request, including a failed newer choice.
        let generation = self
            .identity_rekey_generation
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        let lease: PersistenceLease = Arc::new(entry.gate.clone().lock_owned().await);
        let credential_generation = entry.state.lock().await.generation;
        let (account, _, _) = self
            .collaboration_probe(&entry, lease.clone(), &target)
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
        let nonce = github_auth_ops::proof_line_param("nonce", nonce)?;
        let label = github_auth_ops::proof_line_param("hostLabel", label)?;
        let target = Self::collaboration_target(provider, host)?;
        if expected.provider != target.provider().as_wire() || expected.host != target.host() {
            return Err(Error::IdentityMismatch);
        }
        let generation = self.identity_rekey_generation.load(Ordering::SeqCst);
        let entry = self.collaboration_credential(&target).await?;
        let lease: PersistenceLease = Arc::new(entry.gate.clone().lock_owned().await);
        let credential_generation = entry.state.lock().await.generation;
        let (account, token, target) = self
            .collaboration_probe(&entry, lease.clone(), &target)
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
        if let Err(e) = io(&entry.store, lease.clone(), move |s| {
            s.store(&key, &receipt)
        })
        .await
        {
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
        let target = Self::collaboration_target(provider, host)?;
        let proof = self.proof_provider(&target);
        if !proof.valid_proof_id(proof_id) {
            return Err(Error::InvalidParams("invalid proofId".into()));
        }
        let entry = self.collaboration_credential(&target).await?;
        let lease: PersistenceLease = Arc::new(entry.gate.clone().lock_owned().await);
        let (account, token, target) = self
            .collaboration_probe(&entry, lease.clone(), &target)
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
    async fn commit(self, lease: PersistenceLease) -> intent_sourcecontrol::Result<()> {
        match self {
            Self::Github(g) => g.commit(Some(Box::new(lease))).await,
            Self::Gitlab(g, _) => g.commit_with_lease(Some(lease)).await,
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
    use intent_store::Store;

    async fn fixture() -> (TempDb, Services) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let secrets = FileSecretStore::with_path(tmp.path.with_extension("secrets"));
        let services = Services::new(store).with_gitlab_secret_store(secrets);
        (tmp, services)
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
        let second = Services::new(second_store)
            .with_gitlab_secret_store(service.gitlab_secret_store.clone());
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
        save_account(&entry, lease, &account).await.unwrap();
        entry
            .store
            .store("identity.proof.abcdef", &account.generation)
            .unwrap();
        let restarted = Services::new(service.store.clone())
            .with_gitlab_secret_store(service.gitlab_secret_store.clone());
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
