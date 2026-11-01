//! A bound executor link for tests: a real daemon WSS listener serving the
//! executor link in process, one credential, and the backend's (the test's)
//! dialed end.

use std::sync::{Arc, Mutex};

use intent_core::machines::ExecutorCredential;
use intent_core::node_link::{
    BindDecision, LeaseBinding, LinkHello, LinkWelcome, MachineCapabilities, NODE_LINK_PROTOCOLS,
};
use intent_core::nodes::{NodeArch, NodeCounter, NodeOs};
use intent_core::placement::AgentPlatform;
use intent_core::{BoxFuture, ExecutorCredentialId, HostRole, PrincipalId, WorkspaceApi};
use intent_transport::node_link::{
    dial_link, CertificatePin, DialedLink, ExecutorHost, ExecutorLinks, ExecutorToken,
    NodeConnection, NodeEndpoint, NodeLink,
};

const TOKEN: &str = "wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww";

struct Api {
    principal: PrincipalId,
    credential: ExecutorCredential,
    binding: Mutex<Option<LeaseBinding>>,
}

impl WorkspaceApi for Api {
    fn primary_principal_id(&self) -> BoxFuture<'_, intent_core::Result<PrincipalId>> {
        Box::pin(async move { Ok(self.principal.clone()) })
    }

    fn principal_host_role(
        &self,
        _id: PrincipalId,
    ) -> BoxFuture<'_, intent_core::Result<HostRole>> {
        Box::pin(async { Ok(HostRole::Member) })
    }

    fn resolve_executor_credential(
        &self,
        token_hash: String,
    ) -> BoxFuture<'_, intent_core::Result<Option<ExecutorCredential>>> {
        let known = intent_transport::auth::hash_token(TOKEN);
        Box::pin(async move { Ok((token_hash == known).then(|| self.credential.clone())) })
    }

    fn bind_executor(
        &self,
        _credential_id: ExecutorCredentialId,
        node_id: String,
        offered: LeaseBinding,
    ) -> BoxFuture<'_, intent_core::Result<BindDecision>> {
        Box::pin(async move {
            let mut binding = self.binding.lock().unwrap();
            let decision = LeaseBinding::decide(&node_id, binding.as_ref(), &offered);
            if decision == BindDecision::Accept {
                *binding = Some(offered);
            }
            Ok(decision)
        })
    }
}

struct OwnerToken;

impl intent_transport::TokenStore for OwnerToken {
    fn load_token(&self) -> Option<String> {
        Some("o".repeat(64))
    }
    fn store_token(&self, _token: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

struct Host(tokio::sync::mpsc::UnboundedSender<NodeLink>);

impl ExecutorHost for Host {
    fn ready(&self) -> bool {
        true
    }
    fn node_id(&self) -> String {
        "node-1".into()
    }
    fn welcome(&self, _credential: &ExecutorCredential, _hello: &LinkHello) -> LinkWelcome {
        LinkWelcome {
            protocol: 0,
            node_id: String::new(),
            running_agents: Vec::new(),
            capabilities: MachineCapabilities::new(
                AgentPlatform {
                    os: NodeOs::Linux,
                    arch: NodeArch::X86_64,
                },
                "test",
                1,
                "2026-10-30T00:00:00Z",
            ),
            hosts: Vec::new(),
            tc_address: None,
        }
    }
    fn accept(&self, _credential: ExecutorCredential, connection: NodeConnection) {
        if let NodeConnection::Link { link, .. } = connection {
            let _ = self.0.send(link);
        }
    }
}

/// Keeps the listener serving while the link is in use.
pub(super) struct Served(
    #[expect(dead_code, reason = "held only to keep the listener serving")]
    intent_transport::WsApiServer,
);

/// A bound link: the machine's end, the backend's end, and the listener.
pub(super) async fn bound_link(dir: &std::path::Path) -> (NodeLink, DialedLink, Served) {
    let api = Arc::new(Api {
        principal: PrincipalId::new(),
        credential: ExecutorCredential {
            id: ExecutorCredentialId::new(),
            backend_id: "head-1".into(),
            backend_name: "Head".into(),
            backend_fingerprint: None,
            created_at: "2026-10-31T00:00:00Z".into(),
            last_used_at: None,
            revoked_at: None,
        },
        binding: Mutex::new(None),
    });
    let bus = intent_services::EventBus::new(
        intent_store::Store::open(&dir.join("bus.db"))
            .await
            .unwrap(),
    );
    let certificate =
        intent_transport::generate_certificate(vec!["localhost".into(), "127.0.0.1".into()])
            .unwrap();
    let tokens = Arc::new(intent_transport::AsyncTokenStore::new(Arc::new(OwnerToken)));
    let mut server = intent_transport::WsApiServer::new(
        api,
        bus,
        &certificate,
        &tokens,
        intent_transport::WsOptions {
            base_port: 0,
            ..intent_transport::WsOptions::default()
        },
        None,
    )
    .unwrap();
    let (tx, mut accepted) = tokio::sync::mpsc::unbounded_channel();
    server.install_executor_links(ExecutorLinks::new(Arc::new(Host(tx))));
    let port = server.start().await.unwrap();
    let endpoint = NodeEndpoint {
        url: format!("wss://127.0.0.1:{port}"),
        pin: CertificatePin::parse(&certificate.fingerprint256).unwrap(),
        token: ExecutorToken::new(TOKEN).unwrap(),
        tailcat: None,
    };
    let hello = LinkHello {
        protocol: NODE_LINK_PROTOCOLS,
        intentd_version: "test".into(),
        binding: LeaseBinding {
            head_id: "head-1".into(),
            node_id: "node-1".into(),
            lease_id: "lease-1".into(),
            generation: NodeCounter(1),
        },
        acked_seq: NodeCounter(0),
    };
    let dialed = dial_link(&endpoint, hello).await.unwrap();
    let link = tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(10)),
        accepted.recv(),
    )
    .await
    .unwrap()
    .unwrap();
    (link, dialed, Served(server))
}
