//! One primary GitLab probe owns refresh/delete results and their event tail.
//! The supervisor retains only the current credential phase's gate; GET/user
//! and the caller's later identity transition run outside that gate.

use super::{
    probe_gitlab_owned, refresh_stored_credential_if_needed, stored_access_token, Arc, Error,
    GitlabCredentialGate, GitlabHost, IdentityProofErrorKind, ProbeOutcome, Result,
    StoredCredential,
};
use intent_sourcecontrol::gitlab_auth::PersistenceLease;

pub(super) type RetainedGate = Arc<std::sync::Mutex<Option<PersistenceLease>>>;

pub(crate) enum Reply {
    Probe(ProbeOutcome),
    Token(String),
}

pub(super) async fn acquire(
    gate: &GitlabCredentialGate,
    retained: &RetainedGate,
) -> PersistenceLease {
    let lease: PersistenceLease = Arc::new(gate.clone().lock_owned().await);
    *retained.lock().unwrap() = Some(lease.clone());
    lease
}

impl crate::Services {
    pub(crate) async fn owned_gitlab_probe(&self, host: GitlabHost, proof: bool) -> Result<Reply> {
        let caller = intent_core::current_caller()
            .ok_or_else(|| Error::Internal("credential caller missing".into()))?;
        let credential = intent_core::caller::current_wire_credential();
        let expired = Arc::new(tokio::sync::Notify::new());
        let mut operation = self.clone();
        operation.secrets = Arc::new(self.secrets.settled_operation(expired.clone()));
        let (response, receiver) = tokio::sync::oneshot::channel();
        let owner = async move {
            operation.primary_auth_admitted = true;
            let retained = Arc::new(std::sync::Mutex::new(None));
            let worker_gate = retained.clone();
            let worker = operation.clone();
            let mut worker = intent_core::spawn_daemon(intent_core::with_caller(
                caller,
                intent_core::caller::with_wire_credential(credential, async move {
                    if proof {
                        worker
                            .gitlab_proof_token_owned(&host, &worker_gate)
                            .await
                            .map(Reply::Token)
                    } else {
                        let client_id = worker.gitlab_client_id(&host);
                        probe_gitlab_owned(
                            &worker.secrets,
                            &worker_gate,
                            &host,
                            &|| worker.gitlab_host_is_bound(&host),
                            client_id.as_deref(),
                            worker.gitlab_secret_store.clone(),
                            &worker.gitlab_credential_gate,
                            worker.event_bus.as_ref(),
                        )
                        .await
                        .map(Reply::Probe)
                    }
                }),
            ));
            let mut response = Some(response);
            let joined = tokio::select! {
                biased;
                () = expired.notified() => {
                    let _ = response.take().unwrap().send(Err(Error::Internal(
                        "secret-store write timed out; probe continues, state may be unknown".into()
                    )));
                    worker.await
                }
                result = &mut worker => result,
            };
            let result = joined.unwrap_or_else(|error| {
                Err(Error::Internal(format!(
                    "GitLab probe failed: {error}; state may be unknown"
                )))
            });
            if let Some(response) = response {
                let _ = response.send(result);
            }
            operation.secrets.finish_operation().await;
            retained.lock().unwrap().take();
        };
        // Only a clone already executing inside a registered settings/identity
        // owner can derive work after root closure. The finite tail tracker is
        // drained after the early roots, including all of their children.
        let admitted = if self.primary_auth_admitted {
            self.store_tasks.spawn_draining(owner)
        } else {
            self.settings_tasks.spawn_draining(owner)
        };
        if admitted.is_none() {
            return Err(Error::Internal("daemon is shutting down".into()));
        }
        receiver.await.map_err(|_| {
            Error::Internal("GitLab probe completion owner failed; state may be unknown".into())
        })?
    }

    async fn gitlab_proof_token_owned(
        &self,
        host: &GitlabHost,
        retained: &RetainedGate,
    ) -> Result<String> {
        let lease = acquire(&self.gitlab_credential_gate, retained).await;
        if !self.gitlab_host_is_bound(host) {
            return Err(Error::IdentityProof(
                IdentityProofErrorKind::GitlabNotConnected,
            ));
        }
        let client_id = self.gitlab_client_id(host);
        let Some((credential, _)) = refresh_stored_credential_if_needed(
            &self.secrets,
            lease,
            host,
            client_id.as_deref(),
            &self.gitlab_secret_store,
            self.event_bus.as_ref(),
        )
        .await?
        else {
            return Err(Error::IdentityProof(
                IdentityProofErrorKind::GitlabNotConnected,
            ));
        };
        if credential == StoredCredential::None {
            return Err(Error::IdentityProof(
                IdentityProofErrorKind::GitlabNotConnected,
            ));
        }
        stored_access_token(&self.gitlab_secret_store)
            .await?
            .ok_or(Error::IdentityProof(
                IdentityProofErrorKind::GitlabNotConnected,
            ))
    }
}
