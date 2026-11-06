//! Machine side of the daemon-to-daemon executor link: the transport seams
//! that resolve `/node/*` bearer tokens, persist per-credential bindings and
//! publish credential revocations so live `/node/*` connections close.

use intent_core::ExecutorCredentialId;

use crate::Services;

/// Live feed of executor credentials just revoked; the transport closes the
/// `/node/*` connections still bound to them with `1008`.
pub(crate) type ExecutorRevocations = tokio::sync::broadcast::Sender<ExecutorCredentialId>;

impl Services {
    /// Tell the listeners that `credential_id` was revoked, so its live
    /// `/node/link` and `/node/git` connections close with `1008`. Call it
    /// after the store revoke committed. Having no listener is fine.
    pub fn notify_executor_revoked(&self, credential_id: &ExecutorCredentialId) {
        let _ = self.executor_revocations.send(credential_id.clone());
    }
}

#[cfg(test)]
mod tests {
    use intent_core::machines::ExecutorCredential;
    use intent_core::node_link::{BindDecision, LeaseBinding};
    use intent_core::nodes::NodeCounter;
    use intent_core::{ExecutorCredentialId, WorkspaceApi as _};

    use crate::Services;

    async fn services() -> (tempfile::TempDir, Services) {
        let dir = tempfile::tempdir().unwrap();
        let store = intent_store::Store::open(&dir.path().join("test.db"))
            .await
            .unwrap();
        (dir, Services::new(store))
    }

    fn credential(backend: &str) -> ExecutorCredential {
        ExecutorCredential {
            id: ExecutorCredentialId::new(),
            backend_id: backend.into(),
            backend_name: "Backend".into(),
            backend_fingerprint: None,
            created_at: intent_core::now_iso(),
            last_used_at: None,
            revoked_at: None,
        }
    }

    fn binding(head: &str, generation: u64) -> LeaseBinding {
        LeaseBinding {
            head_id: head.into(),
            node_id: "machine-1".into(),
            lease_id: "lease-1".into(),
            generation: NodeCounter(generation),
        }
    }

    #[tokio::test]
    async fn executor_tokens_resolve_only_while_active() {
        let (_dir, services) = services().await;
        let active = credential("backend-1");
        services
            .store
            .insert_executor_credential(&active, "hash-a")
            .await
            .unwrap();
        let resolved = services
            .resolve_executor_credential("hash-a".into())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resolved.id, active.id);
        assert!(services
            .resolve_executor_credential("hash-unknown".into())
            .await
            .unwrap()
            .is_none());
        services
            .store
            .revoke_executor_credential(&active.id)
            .await
            .unwrap();
        assert!(services
            .resolve_executor_credential("hash-a".into())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn executor_principal_hashes_never_cross() {
        let (_dir, services) = services().await;
        services
            .store
            .insert_executor_credential(&credential("backend-1"), "hash-x")
            .await
            .unwrap();
        assert_eq!(
            services
                .resolve_principal_credential("hash-x".into())
                .await
                .unwrap(),
            None,
            "an executor token never resolves as a principal credential"
        );
    }

    #[tokio::test]
    async fn bind_executor_persists_per_credential_and_refuses_other_backends() {
        let (_dir, services) = services().await;
        let first = credential("backend-1");
        services
            .store
            .insert_executor_credential(&first, "hash-1")
            .await
            .unwrap();
        let bind = |offered: LeaseBinding| {
            services.bind_executor(first.id.clone(), "machine-1".into(), offered)
        };
        assert_eq!(
            bind(binding("backend-1", 1)).await.unwrap(),
            BindDecision::Accept
        );
        assert_eq!(
            bind(binding("backend-2", 9)).await.unwrap(),
            BindDecision::OtherHead
        );
        assert!(matches!(
            bind(binding("backend-1", 1)).await.unwrap(),
            BindDecision::Stale { .. }
        ));
        services
            .store
            .revoke_executor_credential(&first.id)
            .await
            .unwrap();
        assert!(matches!(
            bind(binding("backend-1", 2)).await,
            Err(intent_core::Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn revocations_reach_every_subscriber() {
        let (_dir, services) = services().await;
        let mut a = services.subscribe_executor_revocations().unwrap();
        let mut b = services.subscribe_executor_revocations().unwrap();
        let id = ExecutorCredentialId::new();
        services.notify_executor_revoked(&id);
        assert_eq!(a.recv().await.unwrap(), id);
        assert_eq!(b.recv().await.unwrap(), id);
    }
}
