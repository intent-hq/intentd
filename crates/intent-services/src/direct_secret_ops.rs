//! Ownership of direct forge/MCP mutations through secret completion and
//! their settings, runtime, and event tails. Uses the early settings drain so
//! an admitted MCP operation cannot restart a server after final hub teardown.

use std::sync::Arc;

use intent_core::{Error, Result, WorkspaceId};
use serde_json::{json, Value};

use crate::Services;

pub(crate) enum Mutation {
    GithubRevoke,
    GitlabPat(
        intent_sourcecontrol::gitlab_auth::GitlabHost,
        intent_sourcecontrol::SecretString,
    ),
    GitlabRevoke(intent_sourcecontrol::gitlab_auth::GitlabHost),
    McpCreate(Value),
    McpUpdate(String, Value),
    McpDelete(String),
    McpToggle(String, bool, Option<WorkspaceId>),
    McpRestart(String),
}

impl Services {
    /// Authorization happens at the original entry point, before admission.
    /// The registered supervisor retains per-account ownership even if its
    /// response disappears or its algorithm worker panics.
    pub(crate) async fn owned_direct_secret_mutation(&self, mutation: Mutation) -> Result<Value> {
        let caller = intent_core::current_caller()
            .ok_or_else(|| Error::Internal("credential caller missing".into()))?;
        let credential = intent_core::caller::current_wire_credential();
        let expired = Arc::new(tokio::sync::Notify::new());
        let mut operation = self.clone();
        operation.secrets = Arc::new(self.secrets.settled_operation(expired.clone()));
        let (response, receiver) = tokio::sync::oneshot::channel();
        if self.settings_tasks.spawn_draining(async move {
            let mut response = Some(response);
            let github = matches!(mutation, Mutation::GithubRevoke);
            let gitlab = matches!(mutation, Mutation::GitlabPat(..) | Mutation::GitlabRevoke(..));
            // Preserve #5859's order: GitHub revoke holds only its credential
            // generation lock, never a batch-wide account lock. MCP uses its
            // own account gate, also shared with settings writes.
            let _github = if github {
                match operation.secrets.github_mutation().await {
                    Ok(mut guard) => {
                        *guard += 1;
                        Some(guard)
                    }
                    Err(error) => {
                        let _ = response.take().unwrap().send(Err(error));
                        return;
                    }
                }
            } else { None };
            let gitlab_guard = if gitlab {
                #[cfg(test)]
                let mut polled = if matches!(mutation, Mutation::GitlabRevoke(..)) {
                    operation.gitlab_auth.lock().await.revoke_gate_polled.take()
                } else { None };
                let acquire = operation.gitlab_credential_gate.clone().lock_owned();
                #[cfg(test)]
                let acquire = {
                    let mut acquire = Box::pin(acquire);
                    std::future::poll_fn(move |cx| {
                        let result = std::future::Future::poll(acquire.as_mut(), cx);
                        if let Some(tx) = polled.take() {
                            let _ = tx.send(result.is_pending());
                        }
                        result
                    })
                };
                Some(Arc::new(acquire.await))
            } else { None };
            let _accounts = if github || gitlab { None } else {
                Some(operation.settings_secret_gates.write(&["mcp.servers"]).await)
            };
            let worker = operation.clone();
            let gitlab_lease = gitlab_guard.as_ref().map(|guard| guard.clone() as intent_sourcecontrol::gitlab_auth::PersistenceLease);
            let mut worker = intent_core::spawn_daemon(intent_core::with_caller(caller,
                intent_core::caller::with_wire_credential(credential, async move {
                    worker.execute_direct_secret_mutation(mutation, gitlab_lease).await
                })
            ));
            let joined = tokio::select! {
                biased;
                () = expired.notified() => {
                    let _ = response.take().unwrap().send(Err(Error::Internal(
                        "secret-store write timed out; operation continues, state may be unknown".into()
                    )));
                    worker.await
                }
                result = &mut worker => result,
            };
            let result = joined.unwrap_or_else(|error| Err(Error::Internal(format!(
                "credential operation failed: {error}; state may be unknown"
            ))));
            if let Err(error) = &result {
                tracing::warn!(%error, "direct credential/config operation failed");
            }
            if let Some(response) = response {
                let _ = response.send(result);
            }
            operation.secrets.finish_operation().await;
        }).is_none() {
            return Err(Error::Internal("daemon is shutting down".into()));
        }
        receiver.await.map_err(|_| {
            Error::Internal("credential completion owner failed; state may be unknown".into())
        })?
    }

    async fn execute_direct_secret_mutation(
        &self,
        mutation: Mutation,
        gitlab_lease: Option<intent_sourcecontrol::gitlab_auth::PersistenceLease>,
    ) -> Result<Value> {
        match mutation {
            Mutation::GithubRevoke => self.github_revoke_owned().await,
            Mutation::GitlabPat(host, token) => {
                self.secrets
                    .persist_gitlab_pat(
                        self.gitlab_secret_store.clone(),
                        token,
                        gitlab_lease.expect("GitLab owner lease"),
                    )
                    .await
                    .map_err(|error| {
                        crate::pr_ops::map_sc_err(intent_sourcecontrol::Error::Api(format!(
                            "could not persist gitlab token: {error}"
                        )))
                    })?;
                {
                    let mut state = self.gitlab_auth.lock().await;
                    state.flow = None;
                    state.starting = None;
                }
                crate::source_control_auth_ops::bind_gitlab_host(
                    self.settings_registry.as_deref(),
                    host.host(),
                );
                crate::source_control_auth_ops::publish_auth_changed(
                    self.event_bus.as_ref(),
                    crate::source_control_auth_ops::Provider::Gitlab,
                    host.host(),
                    "authorized",
                )
                .await;
                Ok(crate::source_control_auth_ops::pat_connect_response())
            }
            Mutation::GitlabRevoke(host) => {
                self.gitlab_revoke_owned(host, gitlab_lease.expect("GitLab owner lease"))
                    .await
            }
            Mutation::McpCreate(config) => self.mcp_servers_service().create(config).await,
            Mutation::McpUpdate(id, config) => self.mcp_servers_service().update(&id, config).await,
            Mutation::McpDelete(id) => self.mcp_servers_service().delete(&id).await,
            Mutation::McpRestart(id) => self.mcp_servers_service().restart(&id).await,
            Mutation::McpToggle(id, enabled, None) => {
                self.mcp_servers_service().toggle(&id, enabled).await
            }
            Mutation::McpToggle(id, enabled, Some(workspace)) => {
                let out = self
                    .mcp_servers_service()
                    .toggle_workspace(workspace.as_str(), &id, enabled)
                    .await?;
                crate::publish_event(
                    self.event_bus.as_ref(),
                    crate::workspace_updated_event(
                        &workspace,
                        &json!({"mcpServerToggled":{"serverId":id,"workspaceDisabled":!enabled}}),
                    ),
                )
                .await;
                Ok(out)
            }
        }
    }

    async fn gitlab_revoke_owned(
        &self,
        host: intent_sourcecontrol::gitlab_auth::GitlabHost,
        lease: intent_sourcecontrol::gitlab_auth::PersistenceLease,
    ) -> Result<Value> {
        {
            let mut state = self.gitlab_auth.lock().await;
            if state
                .flow
                .as_ref()
                .is_some_and(|flow| flow.host == host.host())
            {
                state.flow = None;
            }
            if state
                .starting
                .as_ref()
                .is_some_and(|intent| intent.host == host.host())
            {
                state.starting = None;
            }
        }
        // Resolve binding and stored presence under the supervisor's primary
        // credential gate. Revoking another instance is still an exact no-op.
        if self.gitlab_host_is_bound(&host) {
            let stored = intent_sourcecontrol::gitlab_auth::stored_credential(
                self.gitlab_secret_store.clone(),
            )
            .await
            .map_err(crate::pr_ops::map_sc_err)?;
            if stored != intent_sourcecontrol::StoredCredential::None {
                self.secrets
                    .revoke_gitlab_token(self.gitlab_secret_store.clone(), lease)
                    .await
                    .map_err(|error| {
                        crate::pr_ops::map_sc_err(intent_sourcecontrol::Error::Api(format!(
                            "could not delete gitlab token: {error}"
                        )))
                    })?;
                crate::source_control_auth_ops::publish_auth_changed(
                    self.event_bus.as_ref(),
                    crate::source_control_auth_ops::Provider::Gitlab,
                    host.host(),
                    "revoked",
                )
                .await;
            }
        }
        Ok(json!({"ok":true}))
    }

    async fn github_revoke_owned(&self) -> Result<Value> {
        let secrets = &self.secrets;
        let logout_gh = crate::github_auth_ops::is_production_login_host(
            &crate::github_auth_ops::resolve_login_base_uri(self.github_login_base_uri.as_deref()),
        );
        let revoked_token = if logout_gh {
            secrets.load(crate::github_auth_ops::SECRET_ACCOUNT).await.unwrap_or_else(|error| {
                tracing::debug!(%error, "could not read stored github token before revoke; skipping gh CLI logout");
                None
            })
        } else {
            None
        };
        *self.github_auth_flow.lock().await = None;
        crate::github_auth_ops::delete_stored_token(secrets).await?;
        crate::source_control_auth_ops::publish_auth_changed(
            self.event_bus.as_ref(),
            crate::source_control_auth_ops::Provider::Github,
            crate::source_control_auth_ops::GITHUB_HOST,
            "revoked",
        )
        .await;
        if logout_gh {
            // CLI logout is still best-effort and has no Store/event tail.
            intent_core::spawn_daemon(intent_sourcecontrol::gh_sync::logout_gh_after_revoke(
                revoked_token,
            ));
        }
        Ok(json!({"ok":true}))
    }
}
