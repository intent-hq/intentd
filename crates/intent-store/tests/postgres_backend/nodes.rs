//! Node, lease, final stop, node invite and executor store methods on both
//! backends. Every tenant creates the same fixed ids with tenant-specific
//! values. The decoy also owns decoy-only rows that the subject probes as
//! absent, and skips the writes under test (`writes` is false), so its rows
//! are still in the pre-write state when the subject writes.

use intent_core::{
    machines::{
        CloudMachine, CloudMachineSize, CloudSandboxState, CloudStartedBy, ExecutorCredential,
        ExposedMachine, RemoteMachineState,
    },
    node_link::{BindDecision, LeaseBinding, MachineCapabilities},
    nodes::{
        LeaseOwner, LeaseState, NodeArch, NodeCapacity, NodeCounter, NodeKind, NodeLease, NodeOs,
        NodeRecord, NodeState,
    },
    placement::AgentPlatform,
    remote_launch::{FinalStopReason, FinalStopSubmodule, RemoteFinalStopRecord},
    AgentId, Error, ExecutorCredentialId,
};
use intent_store::{
    ExecutorRevocation, NodeInvite, NodeInviteRefusal, NodeInviteRevocation, Store,
};

use crate::harness::parity_test;

const T0: &str = "2026-01-01T00:00:00.000Z";
const T1: &str = "2026-01-02T00:00:00.000Z";
const FUTURE: &str = "2999-01-01T00:00:00.000Z";
const PAST: &str = "2000-01-01T00:00:00.000Z";

/// A per-tenant lease incarnation, so a lease read without its tenant
/// filter finds an owner that does not match.
fn incarnation(tenant: &str) -> String {
    let n = match tenant {
        "sqlite" => 1,
        "decoy" => 2,
        _ => 3,
    };
    format!("00000000-0000-4000-8000-00000000000{n}")
}

fn node(tenant: &str, id: &str, head: &str) -> NodeRecord {
    NodeRecord {
        id: id.into(),
        head_id: head.into(),
        node_identity: format!("identity-{id}"),
        name: format!("{tenant} {id}"),
        kind: NodeKind::Static,
        os: NodeOs::Linux,
        arch: NodeArch::X86_64,
        state: NodeState::Offline,
        draining: false,
        capacity: NodeCapacity {
            max_agents: 4,
            reserved_agents: 0,
            memory_budget_bytes: 1 << 30,
            used_memory_bytes: 0,
            exclusive_reserved: false,
        },
        version: "1.0.0".into(),
        endpoint: Some("wss://node.example:443/node".into()),
        certificate_sha256: Some("a".repeat(64)),
        last_seen_at: None,
    }
}

fn lease(tenant: &str, record: &NodeRecord, lease_id: &str) -> NodeLease {
    NodeLease {
        owner: LeaseOwner {
            head_id: record.head_id.clone(),
            node_id: record.id.clone(),
            node_identity: record.node_identity.clone(),
            lease_id: lease_id.into(),
            incarnation: incarnation(tenant),
        },
        state: LeaseState::Acquiring,
        created_at: T0.into(),
        last_seen_at: None,
        ack_seq: NodeCounter(0),
        link_generation: NodeCounter(0),
        release_requested: false,
        fence: None,
    }
}

async fn enroll(store: &Store, tenant: &str, id: &str, head: &str) -> (NodeRecord, NodeLease) {
    let record = node(tenant, id, head);
    let initial = lease(tenant, &record, &format!("lease-{id}"));
    store.enroll_node(&record, &initial).await.unwrap();
    (record, initial)
}

parity_test!(nodes_enroll_list_get_and_find, |store, tenant| {
    if tenant == "decoy" {
        enroll(&store, tenant, "node-first", "head-decoy").await;
        enroll(&store, tenant, "node-decoy", "head-1").await;
        enroll(&store, tenant, "node-c2", "head-1").await;
    }
    let (a, lease_a) = enroll(&store, tenant, "node-a", "head-1").await;
    let (b, lease_b) = enroll(&store, tenant, "node-b", "head-1").await;
    let (c, _) = enroll(&store, tenant, "node-c", "head-2").await;

    assert!(store.enroll_node(&a, &lease_a).await.is_err());
    let mut bad = lease(tenant, &a, "lease-bad");
    bad.state = LeaseState::Ready;
    assert!(matches!(
        store.enroll_node(&a, &bad).await,
        Err(Error::InvalidParams(_))
    ));

    if tenant != "decoy" {
        assert_eq!(
            store.node_head_id().await.unwrap().as_deref(),
            Some("head-1")
        );
        assert_eq!(
            store.list_owned_nodes("head-1").await.unwrap(),
            vec![a.clone(), b.clone()]
        );
        assert_eq!(
            store.list_owned_node_leases("head-1").await.unwrap(),
            vec![lease_a.clone(), lease_b.clone()]
        );
        assert!(matches!(
            store.get_node("node-decoy", "head-1").await,
            Err(Error::NotFound(_))
        ));
        assert_eq!(store.find_execution_node("node-decoy").await.unwrap(), None);
        assert!(matches!(
            store.get_node("node-c2", "head-1").await,
            Err(Error::NotFound(_))
        ));
        let decoy = node("decoy", "node-decoy", "head-1");
        assert!(matches!(
            store
                .get_node_lease(&lease("decoy", &decoy, "lease-node-decoy").owner)
                .await,
            Err(Error::NotFound(_))
        ));
    }
    assert_eq!(
        store.list_owned_nodes("head-2").await.unwrap(),
        vec![c.clone()]
    );
    assert_eq!(store.get_node("node-a", "head-1").await.unwrap(), a);
    assert!(matches!(
        store.get_node("node-c", "head-1").await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(store.find_execution_node("node-c").await.unwrap(), Some(c));
    assert_eq!(store.get_node_lease(&lease_a.owner).await.unwrap(), lease_a);
    let mut stale = lease_a.owner.clone();
    stale.incarnation = "00000000-0000-4000-8000-000000000009".into();
    assert!(matches!(
        store.get_node_lease(&stale).await,
        Err(Error::NotFound(_))
    ));
});

parity_test!(node_lease_lifecycle_writes, |store, tenant| {
    // `return` ends only this backend's run.
    async {
        let writes = tenant != "decoy";
        let (record, initial) = enroll(&store, tenant, "node-w", "head-1").await;
        let mut local = node(tenant, "node-l", "head-1");
        local.kind = NodeKind::Local;
        local.endpoint = None;
        local.certificate_sha256 = None;
        let local_lease = lease(tenant, &local, "lease-l");
        store.enroll_node(&local, &local_lease).await.unwrap();
        if !writes {
            return;
        }
        let owner = &initial.owner;

        let ready = store
            .transition_node_lease(owner, LeaseState::Ready)
            .await
            .unwrap();
        assert_eq!(ready.state, LeaseState::Ready);
        assert!(matches!(
            store
                .transition_node_lease(owner, LeaseState::Released)
                .await,
            Err(Error::InvalidParams(_))
        ));
        assert_eq!(
            store.next_node_link_generation(owner).await.unwrap(),
            NodeCounter(1)
        );
        assert_eq!(
            store.next_node_link_generation(owner).await.unwrap(),
            NodeCounter(2)
        );
        assert_eq!(
            store
                .advance_node_lease_ack(owner, NodeCounter(5))
                .await
                .unwrap(),
            NodeCounter(5)
        );
        assert_eq!(
            store
                .advance_node_lease_ack(owner, NodeCounter(3))
                .await
                .unwrap(),
            NodeCounter(5)
        );

        let welcomed = store
            .record_node_welcome(owner, NodeOs::Macos, NodeArch::Aarch64, 8, "2.0.0")
            .await
            .unwrap();
        assert_eq!(welcomed.state, LeaseState::Ready);
        assert!(welcomed.last_seen_at.is_some());
        let host = store.get_node("node-w", "head-1").await.unwrap();
        assert_eq!(
            (
                host.os,
                host.arch,
                host.capacity.max_agents,
                host.version.as_str()
            ),
            (NodeOs::Macos, NodeArch::Aarch64, 8, "2.0.0")
        );
        assert_eq!(host.state, NodeState::Ready);
        assert_eq!(host.name, record.name);

        let released = store.request_node_lease_release(owner).await.unwrap();
        assert!(released.release_requested);
        assert!(store.get_node("node-w", "head-1").await.unwrap().draining);
        assert!(matches!(
            store.transition_node_lease(owner, LeaseState::Busy).await,
            Err(Error::InvalidParams(_))
        ));
        assert!(matches!(
            store.request_node_lease_release(&local_lease.owner).await,
            Err(Error::InvalidParams(_))
        ));

        let stored = store.get_node_lease(owner).await.unwrap();
        assert_eq!(stored.ack_seq, NodeCounter(5));
        assert_eq!(stored.link_generation, NodeCounter(2));
        assert!(stored.release_requested);
        assert_eq!(stored.owner.incarnation, incarnation(tenant));
    }
    .await;
});

parity_test!(machine_leases_renew_and_end, |store, tenant| {
    // `return` ends only this backend's run.
    async {
        let writes = tenant != "decoy";
        let machine = node(tenant, "m1", "head-1");
        let first = lease(tenant, &machine, "lease-1");
        store
            .renew_machine_lease(&machine, &first, NodeCounter(0))
            .await
            .unwrap();
        if !writes {
            let other = node(tenant, "m-decoy", "head-1");
            store
                .renew_machine_lease(&other, &lease(tenant, &other, "lease-2"), NodeCounter(0))
                .await
                .unwrap();
            return;
        }
        let mut renamed = machine.clone();
        renamed.name = format!("{tenant} renamed");
        let second = lease(tenant, &machine, "lease-2");
        store
            .renew_machine_lease(&renamed, &second, NodeCounter(3))
            .await
            .unwrap();
        assert_eq!(store.get_node("m1", "head-1").await.unwrap(), renamed);

        let leases = store.list_owned_node_leases("head-1").await.unwrap();
        assert_eq!(leases.len(), 2);
        assert_eq!(leases[0].owner.lease_id, "lease-1");
        assert_eq!(leases[0].state, LeaseState::Lost);
        assert!(leases[0].fence.is_some());
        assert_eq!(leases[1].owner.lease_id, "lease-2");
        assert_eq!(leases[1].state, LeaseState::Acquiring);
        assert_eq!(leases[1].link_generation, NodeCounter(3));
        let welcomed = store
            .record_node_welcome(&second.owner, NodeOs::Linux, NodeArch::X86_64, 2, "3.0.0")
            .await
            .unwrap();
        assert_eq!(welcomed.state, LeaseState::Ready);

        let foreign = node(tenant, "m1", "head-other");
        assert!(matches!(
            store
                .renew_machine_lease(
                    &foreign,
                    &lease(tenant, &foreign, "lease-3"),
                    NodeCounter(1)
                )
                .await,
            Err(Error::InvalidParams(_))
        ));
        assert_eq!(
            store.end_machine_leases("m1", "head-other").await.unwrap(),
            0
        );
        assert_eq!(
            store.end_machine_leases("m-decoy", "head-1").await.unwrap(),
            0
        );
        assert_eq!(store.end_machine_leases("m1", "head-1").await.unwrap(), 1);
        assert_eq!(store.end_machine_leases("m1", "head-1").await.unwrap(), 0);
        let ended = store.get_node_lease(&second.owner).await.unwrap();
        assert_eq!(ended.state, LeaseState::Lost);
        assert!(matches!(
            store.next_node_link_generation(&second.owner).await,
            Err(Error::InvalidParams(_))
        ));
        assert!(matches!(
            store
                .record_node_welcome(&second.owner, NodeOs::Linux, NodeArch::X86_64, 2, "3.0.1")
                .await,
            Err(Error::InvalidParams(_))
        ));
        assert!(matches!(
            store
                .advance_node_lease_ack(&second.owner, NodeCounter(9))
                .await,
            Err(Error::InvalidParams(_))
        ));
    }
    .await;
});

fn stop(tenant: &str, agent: &str, lease_id: &str, at: &str) -> RemoteFinalStopRecord {
    RemoteFinalStopRecord {
        agent_id: AgentId::from(agent.to_owned()),
        workspace_id: "ws-1".into(),
        machine_id: "m1".into(),
        lease_id: lease_id.into(),
        stop_id: format!("{tenant}-stop-{agent}"),
        reason: FinalStopReason::AgentDeleted,
        repo_path: format!("/repo/{tenant}"),
        requested_at: at.into(),
        submodules: vec![FinalStopSubmodule {
            path: "a".into(),
            module_key: format!("{tenant}-module"),
            git_dir: "/repo/.git/modules/a".into(),
        }],
    }
}

parity_test!(final_stops_record_list_and_get, |store, tenant| {
    if tenant == "decoy" {
        store
            .record_final_stop(&stop(tenant, "agent-decoy", "lease-a", T0))
            .await
            .unwrap();
        store
            .record_final_stop(&stop(tenant, "agent-decoy-2", "lease-none", T0))
            .await
            .unwrap();
    }
    let one = stop(tenant, "agent-1", "lease-a", T1);
    let two = stop(tenant, "agent-2", "lease-a", T0);
    let three = stop(tenant, "agent-3", "lease-b", T0);
    for s in [&one, &two, &three] {
        assert_eq!(&store.record_final_stop(s).await.unwrap(), s);
    }
    let mut resend = one.clone();
    resend.stop_id = "other".into();
    resend.reason = FinalStopReason::MachineRemoved;
    assert_eq!(store.record_final_stop(&resend).await.unwrap(), one);

    let agent = |id: &str| AgentId::from(id.to_owned());
    assert_eq!(
        store.get_final_stop(&agent("agent-1")).await.unwrap(),
        Some(one.clone())
    );
    if tenant != "decoy" {
        assert_eq!(
            store.get_final_stop(&agent("agent-decoy")).await.unwrap(),
            None
        );
        assert_eq!(
            store.list_final_stops("lease-a").await.unwrap(),
            vec![two.clone(), one.clone()]
        );
    }
    assert_eq!(
        store.list_final_stops("lease-b").await.unwrap(),
        vec![three]
    );
    if tenant != "decoy" {
        assert_eq!(store.list_final_stops("lease-none").await.unwrap(), vec![]);
    }
});

fn invite(tenant: &str, id: &str, expires_at: &str) -> NodeInvite {
    NodeInvite {
        id: id.into(),
        label: Some(format!("{tenant} {id}")),
        created_at: T0.into(),
        expires_at: expires_at.into(),
        used_at: None,
        credential_id: None,
        revoked_at: None,
    }
}

fn credential(tenant: &str, id: &str, backend: &str, created_at: &str) -> ExecutorCredential {
    ExecutorCredential {
        id: ExecutorCredentialId::from(id.to_owned()),
        backend_id: backend.into(),
        backend_name: format!("{tenant} {backend}"),
        backend_fingerprint: Some(format!("{backend}-fp")),
        created_at: created_at.into(),
        last_used_at: None,
        revoked_at: None,
    }
}

fn cred_id(id: &str) -> ExecutorCredentialId {
    ExecutorCredentialId::from(id.to_owned())
}

parity_test!(
    node_invites_insert_list_revoke_and_claim,
    |store, tenant| {
        // `return` ends only this backend's run.
        async {
            let writes = tenant != "decoy";
            let hash = |id: &str| format!("{tenant}-{id}");
            if !writes {
                let open = invite(tenant, "inv-decoy", FUTURE);
                store
                    .insert_node_invite(&open, &hash("inv-decoy"))
                    .await
                    .unwrap();
                let claimed = invite(tenant, "inv-decoy-claimed", FUTURE);
                store
                    .insert_node_invite(&claimed, &hash("inv-decoy-claimed"))
                    .await
                    .unwrap();
                let outcome = store
                    .claim_node_invite(
                        "inv-decoy-claimed",
                        &hash("inv-decoy-claimed"),
                        "backend-x",
                        &credential(tenant, "cred-inv", "backend-x", T0),
                        "token-decoy",
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(outcome, Ok(()));
            }
            let mut open = invite(tenant, "inv-open", FUTURE);
            open.created_at = T1.into();
            let revoke = invite(tenant, "inv-revoke", FUTURE);
            let claim = invite(tenant, "inv-claim", FUTURE);
            let old = invite(tenant, "inv-old", PAST);
            for i in [&open, &revoke, &claim, &old] {
                store.insert_node_invite(i, &hash(&i.id)).await.unwrap();
            }
            let mut closed = invite(tenant, "inv-bad", FUTURE);
            closed.used_at = Some(T0.into());
            assert!(matches!(
                store.insert_node_invite(&closed, "h").await,
                Err(Error::InvalidParams(_))
            ));
            if !writes {
                return;
            }
            assert_eq!(
                store.list_open_node_invites().await.unwrap(),
                vec![claim.clone(), revoke.clone(), open.clone()]
            );
            assert_eq!(store.claimed_node_invites().await.unwrap(), vec![]);

            assert_eq!(
                store.revoke_node_invite("inv-revoke").await.unwrap(),
                NodeInviteRevocation::Revoked
            );
            assert_eq!(
                store.revoke_node_invite("inv-revoke").await.unwrap(),
                NodeInviteRevocation::Closed
            );
            assert_eq!(
                store.revoke_node_invite("inv-decoy").await.unwrap(),
                NodeInviteRevocation::NotFound
            );

            let cred = credential(tenant, "cred-inv", "backend-1", T0);
            let claim_with = |id: &'static str, secret: String, late| {
                let store = &store;
                let cred = cred.clone();
                async move {
                    store
                        .claim_node_invite(id, &secret, "backend-1", &cred, "token-inv", late)
                        .await
                        .unwrap()
                }
            };
            assert_eq!(
                claim_with("inv-decoy", "decoy-inv-decoy".into(), None).await,
                Err(NodeInviteRefusal::Invalid)
            );
            assert_eq!(
                claim_with("inv-claim", "decoy-inv-claim".into(), None).await,
                Err(NodeInviteRefusal::Invalid)
            );
            assert_eq!(
                claim_with("inv-revoke", hash("inv-revoke"), None).await,
                Err(NodeInviteRefusal::Revoked)
            );
            assert_eq!(
                claim_with("inv-old", hash("inv-old"), None).await,
                Err(NodeInviteRefusal::Expired {
                    expires_at: PAST.into()
                })
            );
            assert_eq!(
                claim_with("inv-claim", hash("inv-claim"), Some("busy")).await,
                Err(NodeInviteRefusal::Late("busy"))
            );
            assert_eq!(
                claim_with("inv-claim", hash("inv-claim"), None).await,
                Ok(())
            );
            assert_eq!(
                claim_with("inv-claim", hash("inv-claim"), None).await,
                Err(NodeInviteRefusal::Used)
            );
            assert_eq!(
                store.claimed_node_invites().await.unwrap(),
                vec![("cred-inv".to_owned(), "inv-claim".to_owned())]
            );
            assert_eq!(
                store
                    .get_executor_credential(&cred_id("cred-inv"))
                    .await
                    .unwrap(),
                Some(cred)
            );
            assert_eq!(store.list_open_node_invites().await.unwrap(), vec![open]);
        }
        .await;
    }
);

parity_test!(
    executor_credentials_resolve_list_and_revoke,
    |store, tenant| {
        // `return` ends only this backend's run.
        async {
            let writes = tenant != "decoy";
            if !writes {
                let decoy = credential(tenant, "cred-decoy", "backend-d", T0);
                store
                    .insert_executor_credential(&decoy, "token-decoy")
                    .await
                    .unwrap();
            }
            let one = credential(tenant, "cred-1", "backend-a", T0);
            let two = credential(tenant, "cred-2", "backend-a", T1);
            store
                .insert_executor_credential(&one, "token-1")
                .await
                .unwrap();
            store
                .insert_executor_credential(&two, "token-2")
                .await
                .unwrap();
            assert!(store
                .insert_executor_credential(&credential(tenant, "cred-3", "b", T0), "token-1")
                .await
                .is_err());
            let mut empty = credential(tenant, "cred-4", "", T0);
            empty.backend_id.clear();
            assert!(matches!(
                store.insert_executor_credential(&empty, "token-4").await,
                Err(Error::InvalidParams(_))
            ));
            if !writes {
                return;
            }
            assert_eq!(
                store
                    .get_executor_credential(&cred_id("cred-1"))
                    .await
                    .unwrap(),
                Some(one.clone())
            );
            assert_eq!(
                store
                    .get_executor_credential(&cred_id("cred-decoy"))
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                store.list_active_executor_credentials().await.unwrap(),
                vec![one.clone(), two.clone()]
            );

            let resolved = store
                .resolve_active_executor_credential("token-1")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(resolved.backend_name, one.backend_name);
            assert!(resolved.last_used_at.is_some());
            assert_eq!(
                store
                    .resolve_active_executor_credential("token-decoy")
                    .await
                    .unwrap(),
                None
            );

            assert_eq!(
                store
                    .revoke_executor_credential(&cred_id("cred-2"))
                    .await
                    .unwrap(),
                ExecutorRevocation::Revoked
            );
            assert_eq!(
                store
                    .revoke_executor_credential(&cred_id("cred-2"))
                    .await
                    .unwrap(),
                ExecutorRevocation::AlreadyRevoked
            );
            assert_eq!(
                store
                    .revoke_executor_credential(&cred_id("cred-decoy"))
                    .await
                    .unwrap(),
                ExecutorRevocation::NotFound
            );
            assert_eq!(
                store
                    .resolve_active_executor_credential("token-2")
                    .await
                    .unwrap(),
                None
            );
            let listed = store.list_active_executor_credentials().await.unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].id, one.id);
        }
        .await;
    }
);

fn binding(head: &str, node_id: &str, generation: u64) -> LeaseBinding {
    LeaseBinding {
        head_id: head.into(),
        node_id: node_id.into(),
        lease_id: format!("lease-{head}-{generation}"),
        generation: NodeCounter(generation),
    }
}

parity_test!(executor_bindings_decide_and_persist, |store, tenant| {
    // `return` ends only this backend's run.
    async {
        let cred = cred_id("cred-b");
        store
            .insert_executor_credential(&credential(tenant, "cred-b", "head-b", T0), "token-b")
            .await
            .unwrap();
        if tenant == "decoy" {
            store
                .insert_executor_credential(
                    &credential(tenant, "cred-only", "head-b", T0),
                    "token-o",
                )
                .await
                .unwrap();
            assert_eq!(
                store
                    .bind_executor(&cred, "node-1", &binding("head-b", "node-1", 7))
                    .await
                    .unwrap(),
                BindDecision::Accept
            );
            assert_eq!(
                store
                    .bind_executor(
                        &cred_id("cred-only"),
                        "node-1",
                        &binding("head-b", "node-1", 5)
                    )
                    .await
                    .unwrap(),
                BindDecision::Accept
            );
            return;
        }
        assert_eq!(store.executor_binding(&cred).await.unwrap(), None);
        assert_eq!(
            store
                .bind_executor(&cred, "node-1", &binding("head-b", "node-1", 1))
                .await
                .unwrap(),
            BindDecision::Accept
        );
        assert_eq!(
            store
                .bind_executor(&cred, "node-1", &binding("head-b", "node-1", 1))
                .await
                .unwrap(),
            BindDecision::Stale {
                current: NodeCounter(1)
            }
        );
        assert_eq!(
            store
                .bind_executor(&cred, "node-1", &binding("head-x", "node-1", 9))
                .await
                .unwrap(),
            BindDecision::OtherHead
        );
        assert_eq!(
            store
                .bind_executor(&cred, "node-1", &binding("head-b", "node-2", 9))
                .await
                .unwrap(),
            BindDecision::WrongNode
        );
        assert_eq!(
            store
                .bind_executor(&cred, "node-1", &binding("head-b", "node-1", 2))
                .await
                .unwrap(),
            BindDecision::Accept
        );
        assert_eq!(
            store.executor_binding(&cred).await.unwrap(),
            Some(binding("head-b", "node-1", 2))
        );
        assert!(matches!(
            store
                .bind_executor(
                    &cred_id("cred-only"),
                    "node-1",
                    &binding("head-b", "node-1", 1)
                )
                .await,
            Err(Error::NotFound(_))
        ));
        assert_eq!(
            store.executor_binding(&cred_id("cred-only")).await.unwrap(),
            None
        );
        store.revoke_executor_credential(&cred).await.unwrap();
        assert!(matches!(
            store
                .bind_executor(&cred, "node-1", &binding("head-b", "node-1", 3))
                .await,
            Err(Error::NotFound(_))
        ));
        assert_eq!(
            store.executor_binding(&cred).await.unwrap(),
            Some(binding("head-b", "node-1", 2)),
            "a revoked credential keeps its binding"
        );
    }
    .await;
});

fn machine(tenant: &str, id: &str, added_at: &str) -> ExposedMachine {
    ExposedMachine {
        id: id.into(),
        name: format!("{tenant} {id}"),
        hosts: vec!["192.168.1.10".into()],
        port: 7443,
        tc_address: Some(format!("tc-{tenant}")),
        fingerprint: "ab".repeat(32),
        credential_id: cred_id(&format!("{tenant}-cred-{id}")),
        platform: None,
        capabilities: None,
        observed_at: None,
        state: RemoteMachineState::Disconnected,
        added_at: added_at.into(),
        last_connected_at: None,
        node_protocol: None,
        cloud: None,
    }
}

fn cloud(tenant: &str, sandbox: &str) -> CloudMachine {
    CloudMachine {
        provider: "e2b".into(),
        cloud_machine_id: "e2b".into(),
        sandbox_id: sandbox.into(),
        volume_id: None,
        state: CloudSandboxState::Running,
        template: format!("{tenant}-template"),
        size: CloudMachineSize::S4x8,
        started_by: CloudStartedBy {
            principal_id: format!("{tenant}-person"),
            display_name: None,
        },
        agent_id: "agent-1".into(),
        workspace_id: "ws-1".into(),
        started_at: T0.into(),
        running_seconds: 0,
        running_since: None,
        paused_at: None,
    }
}

parity_test!(
    exposed_machines_save_list_update_and_remove,
    |store, tenant| {
        // `return` ends only this backend's run.
        async {
            let writes = tenant != "decoy";
            if !writes {
                store
                    .save_exposed_machine(&machine(tenant, "mac-decoy", T0))
                    .await
                    .unwrap();
                let mut sandbox = machine(tenant, "sb-decoy", T0);
                sandbox.cloud = Some(cloud(tenant, "sb-decoy"));
                store.save_exposed_machine(&sandbox).await.unwrap();
            }
            let mac = machine(tenant, "mac", T1);
            let linux = machine(tenant, "linux", T0);
            let gone = machine(tenant, "gone", T1);
            let mut sandbox = machine(tenant, "sb-1", T0);
            sandbox.cloud = Some(cloud(tenant, "sb-1"));
            for m in [&mac, &linux, &gone, &sandbox] {
                assert_eq!(store.save_exposed_machine(m).await.unwrap(), None);
            }
            let mut connected = machine(tenant, "bad", T0);
            connected.state = RemoteMachineState::Connected;
            assert!(matches!(
                store.save_exposed_machine(&connected).await,
                Err(Error::InvalidParams(_))
            ));
            if !writes {
                return;
            }
            assert_eq!(
                store.get_exposed_machine("mac").await.unwrap(),
                Some(mac.clone())
            );
            assert_eq!(store.get_exposed_machine("mac-decoy").await.unwrap(), None);
            assert_eq!(
                store.list_exposed_machines().await.unwrap(),
                vec![linux.clone(), gone.clone(), mac.clone()]
            );
            assert_eq!(
                store.list_cloud_machines().await.unwrap(),
                vec![sandbox.clone()]
            );

            let mut replaced = mac.clone();
            replaced.credential_id = cred_id(&format!("{tenant}-cred-new"));
            assert_eq!(
                store.save_exposed_machine(&replaced).await.unwrap(),
                Some(mac.credential_id.clone())
            );
            assert_eq!(store.save_exposed_machine(&replaced).await.unwrap(), None);

            assert!(store
                .set_exposed_machine_state("mac", RemoteMachineState::Revoked)
                .await
                .unwrap());
            assert!(!store
                .set_exposed_machine_state("mac-decoy", RemoteMachineState::Revoked)
                .await
                .unwrap());
            assert!(matches!(
                store
                    .set_exposed_machine_state("mac", RemoteMachineState::Connected)
                    .await,
                Err(Error::InvalidParams(_))
            ));
            let hosts = vec![format!("10.0.0.{}", tenant.len())];
            assert!(store
                .record_exposed_machine_connection("mac", &hosts, None, T1)
                .await
                .unwrap());
            assert!(!store
                .record_exposed_machine_connection("mac-decoy", &hosts, None, T1)
                .await
                .unwrap());
            assert!(matches!(
                store
                    .record_exposed_machine_connection("mac", &[], None, T1)
                    .await,
                Err(Error::InvalidParams(_))
            ));
            assert!(store
                .record_exposed_machine_protocol("mac", 19)
                .await
                .unwrap());
            assert!(!store
                .record_exposed_machine_protocol("mac-decoy", 19)
                .await
                .unwrap());
            let platform = AgentPlatform {
                os: NodeOs::Macos,
                arch: NodeArch::Aarch64,
            };
            let report = MachineCapabilities::new(platform, format!("{tenant}-1.0"), 4, T1);
            assert!(store
                .record_exposed_machine_capabilities("mac", &report)
                .await
                .unwrap());
            assert!(!store
                .record_exposed_machine_capabilities("mac-decoy", &report)
                .await
                .unwrap());
            let saved = store.get_exposed_machine("mac").await.unwrap().unwrap();
            assert_eq!(saved.state, RemoteMachineState::Revoked);
            assert_eq!(saved.hosts, hosts);
            assert_eq!(saved.tc_address, None);
            assert_eq!(saved.last_connected_at.as_deref(), Some(T1));
            assert_eq!(saved.node_protocol, Some(19));
            assert_eq!(saved.platform, Some(platform));
            assert_eq!(saved.observed_at.as_deref(), Some(T1));
            assert_eq!(saved.capabilities, Some(report));
            assert_eq!(saved.credential_id, replaced.credential_id);

            let mut moved = cloud(tenant, "sb-1");
            moved.state = CloudSandboxState::Paused;
            assert!(store.update_cloud_machine("sb-1", &moved).await.unwrap());
            assert!(!store.update_cloud_machine("mac", &moved).await.unwrap());
            assert!(!store
                .update_cloud_machine("sb-decoy", &moved)
                .await
                .unwrap());
            assert!(store
                .rename_cloud_machine("sb-1", &format!("{tenant} box"))
                .await
                .unwrap());
            assert!(!store.rename_cloud_machine("mac", "x").await.unwrap());
            assert!(!store.rename_cloud_machine("sb-decoy", "x").await.unwrap());
            assert!(matches!(
                store.rename_cloud_machine("sb-1", "").await,
                Err(Error::InvalidParams(_))
            ));
            let box_ = store.get_exposed_machine("sb-1").await.unwrap().unwrap();
            assert_eq!(box_.name, format!("{tenant} box"));
            assert_eq!(box_.cloud, Some(moved));

            assert_eq!(
                store.remove_exposed_machine("gone").await.unwrap(),
                Some(gone.credential_id.clone())
            );
            assert_eq!(store.remove_exposed_machine("gone").await.unwrap(), None);
            assert_eq!(
                store.remove_exposed_machine("mac-decoy").await.unwrap(),
                None
            );
            let names: Vec<String> = store
                .list_exposed_machines()
                .await
                .unwrap()
                .into_iter()
                .map(|m| m.id)
                .collect();
            assert_eq!(names, ["linux", "mac"]);
        }
        .await;
    }
);

parity_test!(
    cloud_machine_providers_save_list_and_delete,
    |store, tenant| {
        // `return` ends only this backend's run.
        async {
            let writes = tenant != "decoy";
            let config = |name: &str| format!("{{\"tenant\":\"{tenant}\",\"name\":\"{name}\"}}");
            if !writes {
                store
                    .save_cloud_machine_provider("decoy-provider", &config("decoy"))
                    .await
                    .unwrap();
            }
            store
                .save_cloud_machine_provider("e2b", &config("e2b"))
                .await
                .unwrap();
            store
                .save_cloud_machine_provider("northflank", &config("nf"))
                .await
                .unwrap();
            if !writes {
                return;
            }
            assert_eq!(
                store.get_cloud_machine_provider("e2b").await.unwrap(),
                Some(config("e2b"))
            );
            assert_eq!(
                store
                    .get_cloud_machine_provider("decoy-provider")
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                store.list_cloud_machine_providers().await.unwrap(),
                vec![
                    ("e2b".to_owned(), config("e2b")),
                    ("northflank".to_owned(), config("nf"))
                ]
            );
            store
                .save_cloud_machine_provider("e2b", &config("e2b-2"))
                .await
                .unwrap();
            assert!(store
                .delete_cloud_machine_provider("northflank")
                .await
                .unwrap());
            assert!(!store
                .delete_cloud_machine_provider("northflank")
                .await
                .unwrap());
            assert!(!store
                .delete_cloud_machine_provider("decoy-provider")
                .await
                .unwrap());
            assert_eq!(
                store.list_cloud_machine_providers().await.unwrap(),
                vec![("e2b".to_owned(), config("e2b-2"))]
            );
        }
        .await;
    }
);
