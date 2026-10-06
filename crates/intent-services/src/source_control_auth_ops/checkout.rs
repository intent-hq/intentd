//! Captured pre-workspace GitLab connection. R owns caller/host lifetime;
//! this adapter owns only the original settled credential and project responses.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{Error, Result};
use intent_sourcecontrol::gitlab::{
    GitlabAdmittedRequest, GitlabPreparedRequest, GitlabResponseObservation, GitlabResponseReceipt,
};
use intent_sourcecontrol::{
    ExposeSecret, GitLabSourceControl, GitlabInstance, GitlabRequestCredentials, SecretString,
};

use super::repository_owner::RepositorySettledConnection;
use crate::repository_credentials::{
    RepositoryCredentialError, RepositoryDispatchStamp, RepositorySecretReader,
    RepositorySecretRequest, RepositorySecretSnapshot,
};

/// Implemented only by the original pre-workspace caller/host owner. The action
/// transfers prepared data once; no await, disk/network IO or spawn under it.
pub(crate) trait CheckoutAuthority: Send + Sync {
    fn dispatch(&self, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()>;
}

pub(crate) struct GitlabCheckoutConnection {
    original: Arc<RepositorySettledConnection>,
    reader: Arc<dyn RepositorySecretReader>,
    authority: Arc<dyn CheckoutAuthority>,
    cursor_scope: String,
}

/// Credential-free observation captured before the caller's first await. It is
/// not authority and cannot switch to a later connection or secret revision.
pub(crate) struct GitlabCheckoutCapture {
    original: Arc<RepositorySettledConnection>,
    reader: Arc<dyn RepositorySecretReader>,
}

fn local(error: RepositoryCredentialError) -> Error {
    crate::pr_ops::map_sc_err(error.into())
}

impl crate::Services {
    pub(crate) fn capture_gitlab_checkout_connection(&self) -> Result<GitlabCheckoutCapture> {
        Ok(GitlabCheckoutCapture {
            original: Arc::new(self.gitlab_repository_settled_connection().map_err(local)?),
            reader: self.gitlab_repository_secret_reader().map_err(local)?,
        })
    }

    #[cfg(test)]
    pub(crate) fn gitlab_checkout_connection(
        &self,
        authority: Arc<dyn CheckoutAuthority>,
    ) -> Result<Arc<GitlabCheckoutConnection>> {
        self.capture_gitlab_checkout_connection()?.admit(authority)
    }
}

impl GitlabCheckoutCapture {
    /// Consume the original metadata beneath the caller's newly established
    /// fence. A pending capture never silently adopts even a same-binding refresh.
    pub(crate) fn admit(
        self,
        authority: Arc<dyn CheckoutAuthority>,
    ) -> Result<Arc<GitlabCheckoutConnection>> {
        let result = Arc::new(GitlabCheckoutConnection {
            original: self.original,
            reader: self.reader,
            authority,
            cursor_scope: uuid::Uuid::new_v4().to_string(),
        });
        result.with_project(
            None,
            Some(result.original.selected()),
            false,
            &mut || Ok(()),
        )?;
        Ok(result)
    }
}

impl GitlabCheckoutConnection {
    /// The new view carries the complete caller/lease/request fence. Retain the
    /// same captured owner, reader and cursor scope without nesting old locks.
    pub(crate) fn for_request(&self, authority: Arc<dyn CheckoutAuthority>) -> Arc<Self> {
        Arc::new(Self {
            original: self.original.clone(),
            reader: self.reader.clone(),
            authority,
            cursor_scope: self.cursor_scope.clone(),
        })
    }

    pub(crate) fn with_projects_current(
        &self,
        projects: &[String],
        action: &mut (dyn FnMut() -> Result<()> + Send),
    ) -> Result<()> {
        let projects: Vec<_> = projects.iter().map(String::as_str).collect();
        self.with_projects(&projects, None, false, action)
    }

    pub(crate) fn instance_base_url(&self) -> &str {
        self.original.descriptor().instance().as_str()
    }

    /// Credential-free original binding. Never a permission or current lookup.
    pub(crate) fn connection_key(&self) -> String {
        let binding = &self.original.selected().binding;
        format!(
            "{}:{}:{}:{}",
            binding.daemon_id,
            binding.account.account_id,
            binding.scope.connection_id,
            binding.scope.connection_generation
        )
    }

    pub(crate) fn provider(self: &Arc<Self>) -> intent_sourcecontrol::Result<GitLabSourceControl> {
        Ok(
            GitLabSourceControl::new(self.original.descriptor().clone(), self.clone())?
                .with_pagination_scope(self.cursor_scope.clone()),
        )
    }

    pub(crate) fn with_current(
        &self,
        action: &mut (dyn FnMut() -> Result<()> + Send),
    ) -> Result<()> {
        self.with_project(None, None, false, action)
    }

    pub(crate) fn with_project_current(
        &self,
        project: &str,
        action: &mut (dyn FnMut() -> Result<()> + Send),
    ) -> Result<()> {
        self.with_project(Some(project), None, false, action)
    }

    fn with_project(
        &self,
        project: Option<&str>,
        selected: Option<&RepositorySecretRequest>,
        dispatch: bool,
        action: &mut (dyn FnMut() -> Result<()> + Send),
    ) -> Result<()> {
        let projects: Vec<_> = project.into_iter().collect();
        self.with_projects(&projects, selected, dispatch, action)
    }

    fn with_projects(
        &self,
        projects: &[&str],
        selected: Option<&RepositorySecretRequest>,
        dispatch: bool,
        action: &mut (dyn FnMut() -> Result<()> + Send),
    ) -> Result<()> {
        let mut result = None;
        let mut once = Some(action);
        self.authority.dispatch(&mut || {
            self.original
                .with_checkout_current(selected, projects, dispatch, |_, _| {
                    let action = once.take().ok_or(RepositoryCredentialError::Retired)?;
                    result = Some(action());
                    Ok(())
                })
                .map_err(local)
        })?;
        result.ok_or_else(|| local(RepositoryCredentialError::AuthorityUnavailable))?
    }

    fn selected(
        &self,
        project: Option<&str>,
    ) -> intent_sourcecontrol::Result<RepositorySecretRequest> {
        let projects: Vec<_> = project.into_iter().collect();
        let mut selected = None;
        self.authority
            .dispatch(&mut || {
                if selected.is_some() {
                    return Err(local(RepositoryCredentialError::Retired));
                }
                selected = Some(self.original.with_checkout_current(
                    None,
                    &projects,
                    true,
                    |actual, _| Ok(actual.clone()),
                ));
                Ok(())
            })
            .map_err(|_| {
                intent_sourcecontrol::Error::AdmissionUnavailable(
                    intent_sourcecontrol::error::AdmissionUnavailable::AuthorityUnavailable,
                )
            })?;
        selected
            .ok_or(RepositoryCredentialError::AuthorityUnavailable)?
            .map_err(Into::into)
    }

    async fn load(
        &self,
        project: Option<&str>,
    ) -> intent_sourcecontrol::Result<RepositorySecretSnapshot> {
        let expected = self.selected(project)?;
        let snapshot = tokio::time::timeout(Duration::from_secs(10), self.reader.load(&expected))
            .await
            .map_err(|_| RepositoryCredentialError::TimedOut)??;
        if snapshot.request != expected {
            return Err(RepositoryCredentialError::SecretMismatch.into());
        }
        Ok(snapshot)
    }

    async fn admit(
        &self,
        prepared: GitlabPreparedRequest<'_>,
    ) -> intent_sourcecontrol::Result<GitlabAdmittedRequest> {
        let context = prepared.credential_request();
        if context.descriptor != self.original.descriptor() {
            return Err(RepositoryCredentialError::BoundaryMismatch.into());
        }
        let project = context.checkout_project()?;
        let snapshot = self.load(project.as_deref()).await?;
        let projects: Vec<_> = project.as_deref().into_iter().collect();
        let selected = snapshot.request;
        let mut pending = Some(prepared.authenticate(snapshot.token)?);
        let mut admitted = None;
        let mut local_error = None;
        self.authority
            .dispatch(&mut || {
                let result = self.original.with_checkout_current(
                    Some(&selected),
                    &projects,
                    true,
                    |_, stamp| {
                        let request = pending.take().ok_or(RepositoryCredentialError::Retired)?;
                        admitted = Some(request.admit(Some(Box::new(CheckoutReceipt {
                            original: self.original.clone(),
                            selected: selected.clone(),
                            stamp,
                            project: project.clone(),
                        }))));
                        Ok(())
                    },
                );
                if let Err(error) = result {
                    local_error = Some(error);
                }
                Ok(())
            })
            .map_err(|_| {
                intent_sourcecontrol::Error::AdmissionUnavailable(
                    intent_sourcecontrol::error::AdmissionUnavailable::AuthorityUnavailable,
                )
            })?;
        if let Some(error) = local_error {
            return Err(error.into());
        }
        admitted.ok_or_else(|| RepositoryCredentialError::AuthorityUnavailable.into())
    }

    /// Qualified cache retains the same project denial and connection source.
    pub(crate) fn cache(
        self: &Arc<Self>,
        cache_root: &std::path::Path,
        url: &str,
    ) -> Result<intent_git::repo_cache::qualified::QualifiedRepositoryCache> {
        let project = self.project_for_url(url)?;
        let authority = Arc::new(ProjectCacheAuthority {
            connection: self.clone(),
            project,
        });
        intent_git::repo_cache::qualified::QualifiedRepositoryCache::new(
            cache_root,
            intent_git::native_checkout::NativeCheckoutSource::https(url)?,
            &self.connection_key(),
            authority,
        )
    }

    pub(crate) async fn native_credential(
        self: &Arc<Self>,
        url: &str,
    ) -> Result<NativeCheckoutCredential> {
        let project = self.project_for_url(url)?;
        self.with_project_current(&project, &mut || Ok(()))?;
        let snapshot = self
            .load(Some(&project))
            .await
            .map_err(crate::pr_ops::map_sc_err)?;
        self.with_project(Some(&project), Some(&snapshot.request), false, &mut || {
            Ok(())
        })?;
        Ok(NativeCheckoutCredential {
            connection: self.clone(),
            selected: snapshot.request,
            token: snapshot.token,
            url: url.into(),
            project,
            stamp: None,
        })
    }

    pub(crate) fn project_for_url(&self, url: &str) -> Result<String> {
        let instance = self.original.descriptor().instance();
        if !instance.contains_url(url) {
            return Err(local(RepositoryCredentialError::BoundaryMismatch));
        }
        let project = url
            .strip_prefix(instance.as_str())
            .and_then(|s| s.strip_prefix('/'))
            .ok_or_else(|| local(RepositoryCredentialError::BoundaryMismatch))?
            .strip_suffix(".git")
            .unwrap_or_else(|| {
                url.strip_prefix(instance.as_str())
                    .unwrap()
                    .trim_start_matches('/')
            });
        if project.split('/').count() < 2
            || project
                .split('/')
                .any(|v| v.is_empty() || v == "." || v == "..")
            || project.contains('%')
        {
            return Err(local(RepositoryCredentialError::BoundaryMismatch));
        }
        Ok(project.into())
    }
}

struct CheckoutReceipt {
    original: Arc<RepositorySettledConnection>,
    selected: RepositorySecretRequest,
    stamp: RepositoryDispatchStamp,
    project: Option<String>,
}
impl GitlabResponseReceipt for CheckoutReceipt {
    fn observe_missing_checkout_resource(&self, response: GitlabResponseObservation) {
        self.original
            .observe_checkout_response(&self.stamp, response);
    }

    fn observe(&self, response: GitlabResponseObservation) {
        self.original
            .observe_checkout_response(&self.stamp, response);
        if matches!(response.status, 403 | 404) {
            if let Some(project) = &self.project {
                self.original
                    .reject_checkout_project(&self.selected, project);
            }
        }
    }
}

impl GitlabRequestCredentials for GitlabCheckoutConnection {
    fn admit_http_request<'s, 'r, 'f>(
        &'s self,
        prepared: GitlabPreparedRequest<'r>,
    ) -> Pin<
        Box<dyn Future<Output = intent_sourcecontrol::Result<GitlabAdmittedRequest>> + Send + 'f>,
    >
    where
        's: 'f,
        'r: 'f,
        Self: 'f,
    {
        Box::pin(self.admit(prepared))
    }
    fn token_for<'s, 'i, 'f>(
        &'s self,
        _: &'i GitlabInstance,
    ) -> Pin<Box<dyn Future<Output = intent_sourcecontrol::Result<SecretString>> + Send + 'f>>
    where
        's: 'f,
        'i: 'f,
        Self: 'f,
    {
        Box::pin(async { Err(RepositoryCredentialError::BoundaryMismatch.into()) })
    }
}

/// Operation-local memory only; no Debug/Clone/Serde/raw token accessor.
pub(crate) struct NativeCheckoutCredential {
    connection: Arc<GitlabCheckoutConnection>,
    selected: RepositorySecretRequest,
    token: SecretString,
    url: String,
    project: String,
    stamp: Option<RepositoryDispatchStamp>,
}
impl NativeCheckoutCredential {
    pub(crate) fn rejected(&self) {
        self.connection
            .original
            .reject_checkout_project(&self.selected, &self.project);
    }
}

struct ProjectCacheAuthority {
    connection: Arc<GitlabCheckoutConnection>,
    project: String,
}
impl intent_git::repo_cache::qualified::NativeCacheAuthority for ProjectCacheAuthority {
    fn with_current(&self, transfer: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        self.connection
            .with_project_current(&self.project, transfer)
    }
}

impl intent_git::native_checkout::NativeCheckoutCredentials for NativeCheckoutCredential {
    fn with_current(&self, transfer: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        self.connection
            .with_project(Some(&self.project), Some(&self.selected), false, transfer)
    }
    fn rejected(&self) {
        self.rejected();
    }
    fn with_basic_auth(
        &mut self,
        url: &str,
        prepare: &mut (dyn FnMut(&str, &str) -> Result<()> + Send),
    ) -> Result<()> {
        if url != self.url {
            return Err(local(RepositoryCredentialError::BoundaryMismatch));
        }
        let mut once = Some(prepare);
        let mut result = None;
        let mut stamp = None;
        self.connection.authority.dispatch(&mut || {
            self.connection
                .original
                .with_checkout_current(
                    Some(&self.selected),
                    &[self.project.as_str()],
                    true,
                    |_, acquired| {
                        let prepare = once.take().ok_or(RepositoryCredentialError::Retired)?;
                        result = Some(prepare("oauth2", self.token.expose_secret()));
                        stamp = Some(acquired);
                        Ok(())
                    },
                )
                .map_err(local)
        })?;
        self.stamp = stamp;
        result.ok_or_else(|| local(RepositoryCredentialError::AuthorityUnavailable))?
    }
    fn observe(&self, status: u16, backoff_until: Option<std::time::Instant>) {
        if let Some(stamp) = &self.stamp {
            self.connection.original.observe_checkout_response(
                stamp,
                GitlabResponseObservation {
                    status,
                    backoff_until,
                },
            );
            if matches!(status, 403 | 404) {
                self.rejected();
            }
        }
    }
}

#[cfg(test)]
mod tests;
