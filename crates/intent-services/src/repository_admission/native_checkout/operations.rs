//! Existing workspace fetch/push uses the same original native host connection.
//! Non-GitLab transports retain their existing credential/helper implementation.
use std::path::{Path, PathBuf};

use intent_core::{Workspace, WorkspaceId};
use intent_git::native_checkout::NativeCheckoutSource;

use super::{
    admit_lease, capture_connection, denied, project_from_url, unavailable, with_caller,
    with_wire_credential, AdmissionError, Arc, Caller, CheckoutFrame, Error,
    GitlabCheckoutConnection, Lease, OriginalConnection, Output, RepositoryLifecycleKey, Request,
    Result, Services, CAPTURE_LIMIT, CHECKOUT_REQUEST,
};

pub(crate) struct Entry {
    services: Services,
    request: Result<Option<Arc<Request>>>,
    original: Option<OriginalConnection>,
}

/// Capture the actual wire frame before Services clones itself or awaits Store.
pub(crate) fn capture(services: &Services, frame: &CheckoutFrame) -> Entry {
    let request = match CHECKOUT_REQUEST.try_with(Clone::clone) {
        Ok(_) => Request::current(services, frame).map(Some),
        Err(_) => Ok(None),
    };
    if let Ok(Some(request)) = &request {
        *request
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Output::Legacy;
    }
    Entry {
        services: services.clone(),
        request,
        original: capture_connection(services),
    }
}

pub(crate) struct Plan {
    request: Arc<Request>,
    lease: Arc<Lease>,
    provider: Arc<GitlabCheckoutConnection>,
    workspace_id: WorkspaceId,
    path: PathBuf,
    source: NativeCheckoutSource,
    project: String,
}

impl Entry {
    pub(crate) async fn prepare(
        self,
        workspace: &Workspace,
        path: &Path,
    ) -> Result<Option<Arc<Plan>>> {
        let request = self.request?;
        if request.is_none() && !matches!(intent_core::current_caller(), Some(Caller::Wire { .. }))
        {
            return Ok(None);
        }
        let Some(original) = self.original else {
            return Ok(None);
        };
        let instance = crate::source_control_auth_ops::repository_owner::logical_instance(
            &original.settings.effective.source_control.gitlab,
        )?;
        let original_path = path.to_owned();
        let read_path = original_path.clone();
        let (url, native_source) = tokio::task::spawn_blocking(move || {
            let url = intent_git::remote::origin_url(&read_path)?;
            let native_source = git2::Repository::open(&read_path)
                .ok()
                .and_then(|repo| repo.config().ok())
                .and_then(|config| config.get_string("intent.nativeCheckoutSource").ok());
            Ok::<_, Error>((url, native_source))
        })
        .await
        .map_err(denied)??;
        let Some(url) = url else {
            return Ok(None);
        };
        if native_source
            .as_deref()
            .is_some_and(|original| original != url)
        {
            return Err(unavailable());
        }
        let project = match project_from_url(instance.as_str(), &url) {
            Ok(project) => project,
            Err(_) if native_source.is_some() => return Err(unavailable()),
            Err(_) => return Ok(None),
        };
        // Agent/daemon Git tooling keeps the user-helper route. A native wire
        // caller can only use host credentials through its captured connection.
        let Some(request) = request else {
            return if matches!(intent_core::current_caller(), Some(Caller::Wire { .. })) {
                Err(unavailable())
            } else {
                Ok(None)
            };
        };
        request.public()?;
        let _legacy = tokio::time::timeout(CAPTURE_LIMIT, request.connection.caller.legacy_lease())
            .await
            .map_err(denied)?
            .map_err(denied)?;
        original
            .registry
            .with_original_snapshot(&original.settings, |current| {
                if current {
                    Ok(())
                } else {
                    Err(AdmissionError::Retired)
                }
            })
            .map_err(denied)?;
        let workspace_facts = self
            .services
            .store
            .repository_workspace_authority_snapshot(&workspace.id)
            .await?;
        if workspace_facts.workspace.value.is_none() {
            return Err(unavailable());
        }
        let coordinates = [
            RepositoryLifecycleKey::Workspace(workspace.id.clone()),
            RepositoryLifecycleKey::Worktree(original_path.clone()),
        ];
        let (lease, provider) = admit_lease(
            &request,
            original,
            Some(instance.as_str()),
            Some(workspace_facts),
            &coordinates,
        )
        .await?
        .map_err(|_| unavailable())?;
        *request.output.lock().map_err(denied)? = Output::Private {
            lease: lease.clone(),
            provider: provider.clone(),
            projects: vec![project.clone()],
        };
        let plan = Arc::new(Plan {
            request,
            lease,
            provider,
            workspace_id: workspace.id.clone(),
            path: original_path,
            source: NativeCheckoutSource::https(&url)?,
            project,
        });
        plan.current().await?;
        Ok(Some(plan))
    }
}

impl Plan {
    async fn current(&self) -> Result<()> {
        self.request.validate(&self.lease).await?;
        let services = &self.request.connection.services;
        services.require_member(&self.workspace_id).await?;
        let workspace = services.store.get_workspace(&self.workspace_id).await?;
        if crate::git_ops::worktree_path(&workspace).as_deref() != Some(self.path.as_path()) {
            return Err(unavailable());
        }
        self.provider
            .with_project_current(&self.project, &mut || Ok(()))
    }

    pub(crate) async fn execute(
        self: &Arc<Self>,
        force: Option<bool>,
    ) -> Result<Option<intent_git::push::PushOutcome>> {
        let plan = self.clone();
        let caller = plan.request.connection.caller.caller().clone();
        let wire = plan.request.connection.caller.wire_credential().cloned();
        let worker = self
            .request
            .connection
            .services
            .store_tasks
            .spawn_draining(with_caller(
                caller,
                with_wire_credential(wire, async move {
                    let _legacy = tokio::time::timeout(
                        CAPTURE_LIMIT,
                        plan.request.connection.caller.legacy_lease(),
                    )
                    .await
                    .map_err(denied)?
                    .map_err(denied)?;
                    let services = &plan.request.connection.services;
                    let (outcome, status) = services
                        .worktree_locks
                        .with_lock(&plan.path, || async {
                            plan.current().await?;
                            let branch = intent_git::status::current_branch_at(&plan.path)
                                .ok_or_else(unavailable)?;
                            let mut credential =
                                plan.provider.native_credential(plan.source.url()).await?;
                            plan.current().await?;
                            let worktree = plan.path.clone();
                            let source = NativeCheckoutSource::https(plan.source.url())?;
                            let outcome = tokio::task::spawn_blocking(move || {
                                if let Some(force) = force {
                                    intent_git::native_checkout::push_original(
                                        &worktree,
                                        &source,
                                        &branch,
                                        force,
                                        &mut credential,
                                    )
                                    .map(|selected| {
                                        Some(intent_git::push::PushOutcome {
                                            branch: selected.branch,
                                            pushed_sha: selected.commit_sha,
                                        })
                                    })
                                } else {
                                    intent_git::native_checkout::fetch_original(
                                        &worktree,
                                        &source,
                                        &branch,
                                        &mut credential,
                                    )?;
                                    Ok(None)
                                }
                            })
                            .await
                            .map_err(denied)??;
                            services.git_status_invalidator().invalidate(&plan.path);
                            let status = intent_git::status::status(&plan.path)
                                .unwrap_or_else(|_| intent_git::status::empty_status());
                            Ok::<_, Error>((
                                outcome,
                                serde_json::to_value(&status).unwrap_or(serde_json::Value::Null),
                            ))
                        })
                        .await?;
                    // Confirmed remote effects survive cancellation of the
                    // waiting RPC or ineligibility of its private response.
                    if let (Some(outcome), Some(force)) = (&outcome, force) {
                        crate::publish_event(
                            services.event_bus.as_ref(),
                            crate::git_push_event(
                                &plan.workspace_id,
                                &outcome.branch,
                                &outcome.pushed_sha,
                                force,
                            ),
                        )
                        .await;
                    }
                    crate::publish_event(
                        services.event_bus.as_ref(),
                        crate::changes_git_status_event(&plan.workspace_id, &status),
                    )
                    .await;
                    plan.current().await?;
                    Ok(outcome)
                }),
            ))
            .ok_or_else(unavailable)?;
        worker.await.map_err(denied)?
    }
}
