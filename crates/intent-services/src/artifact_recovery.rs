//! Prepared, unregistered recovery boundary. No construction/profile/read grant.
use crate::Services;
use intent_core::{note_artifact::response::Receipt, Caller, Error, Result, WorkspaceId};
use serde_json::Value;

/// Internal orchestration inputs, not registered RPC methods or a wire enum.
#[derive(Clone, Debug)]
pub enum PreparedArtifactRecovery {
    Status {
        job_id: String,
        header_digest: String,
    },
    Abort {
        job_ref: String,
    },
    Release {
        artifact_ref: String,
    },
}

impl Services {
    /// Capture the admitted caller once, revalidate workspace membership before
    /// lookup and before disclosure, then bound the actual response envelope.
    /// Production arena admission remains unavailable; this is not registered on
    /// `WorkspaceApi` or the router. A receipt never proves manifest readability.
    ///
    /// # Errors
    /// Rejects absent/revoked callers, missing arena authority, invalid scoped
    /// identities and oversized responses. Cancellation never implies rollback.
    pub async fn prepared_note_artifact_recovery(
        &self,
        workspace: WorkspaceId,
        operation: PreparedArtifactRecovery,
        rpc_id: Value,
    ) -> Result<Value> {
        let lookup_workspace = workspace.clone();
        self.prepared_artifact_disclose(workspace, rpc_id, |principal| async move {
            let workspace = lookup_workspace;
            Ok(match operation {
                PreparedArtifactRecovery::Status {
                    job_id,
                    header_digest,
                } => {
                    self.store
                        .recover_note_artifact_receipt(
                            &principal,
                            &workspace.0,
                            &job_id,
                            &header_digest,
                        )
                        .await?
                }
                PreparedArtifactRecovery::Abort { job_ref } => {
                    let ack = self
                        .store
                        .abort_note_artifact_journal(&principal, &workspace.0, &job_ref)
                        .await?;
                    self.store
                        .note_artifact_job_receipt(&principal, &workspace.0, &ack)
                        .await?
                }
                PreparedArtifactRecovery::Release { artifact_ref } => {
                    self.store
                        .release_note_artifact_lease(&principal, &workspace.0, &artifact_ref)
                        .await?;
                    Receipt::Released
                }
            })
        })
        .await
    }

    // The lookup is private and supplied only by the unregistered Store boundary
    // above. Tests substitute a controlled future, never an arena authority.
    async fn prepared_artifact_disclose<F, Fut>(
        &self,
        workspace: WorkspaceId,
        rpc_id: Value,
        lookup: F,
    ) -> Result<Value>
    where
        F: FnOnce(String) -> Fut,
        Fut: std::future::Future<Output = Result<Receipt>>,
    {
        let caller = intent_core::current_caller()
            .ok_or_else(|| Error::Forbidden("Caller required".into()))?;
        let principal = match &caller {
            Caller::Wire { principal_id, .. } => format!("principal:{}", principal_id.0),
            Caller::Agent { agent_id } => format!("agent:{}", agent_id.0),
            Caller::Daemon => "daemon".into(),
        };
        intent_core::with_caller(caller.clone(), self.require_member(&workspace)).await?;
        let outcome = lookup(principal).await;
        intent_core::with_caller(caller, self.require_member(&workspace)).await?;
        outcome?.rpc_result(&rpc_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn artifact_recovery_requires_captured_caller_before_arena_access() {
        let (_temp, services, workspace, _) = crate::tests::setup("").await;
        let operation = PreparedArtifactRecovery::Status {
            job_id: "job".into(),
            header_digest: "0".repeat(64),
        };
        let missing = services
            .prepared_note_artifact_recovery(workspace.clone(), operation.clone(), Value::Null)
            .await;
        assert!(matches!(missing, Err(Error::Forbidden(_))));
        let foreign = Caller::Wire {
            principal_id: intent_core::PrincipalId::new(),
            host_role: intent_core::HostRole::Guest,
        };
        let denied = intent_core::with_caller(
            foreign,
            services.prepared_note_artifact_recovery(
                workspace.clone(),
                operation.clone(),
                Value::Null,
            ),
        )
        .await;
        assert!(matches!(
            denied,
            Err(Error::NotFound(_) | Error::Forbidden(_))
        ));
        let admitted = intent_core::with_caller(
            Caller::Daemon,
            services.prepared_note_artifact_recovery(workspace, operation, Value::Null),
        )
        .await;
        assert!(matches!(admitted, Err(Error::InvalidParams(_))));
        assert!(intent_core::current_caller().is_none());
        services.store.close().await;
    }
}

#[cfg(test)]
mod disclosure_tests;
