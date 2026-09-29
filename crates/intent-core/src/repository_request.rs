//! Private, transport-neutral ownership of an original repository read request.
//!
//! These interfaces carry an existing service owner's scope. They neither
//! authenticate a caller nor grant repository access. They are deliberately
//! independent of transport frames, Store, provider credentials and Serde.

use std::sync::Arc;

use crate::{BoxFuture, Result};

/// Selected by the original transport admission, never by request parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryWireEntry {
    /// The actual authenticated connection's bearer binding.
    Bearer,
    /// A connection admitted by the local control transport.
    AdmittedLocal,
}

/// The router's typed service outcome, before JSON envelope construction.
///
/// Neither variant establishes whether the payload is private. In particular,
/// service errors can contain private data, and a successful value can itself
/// contain an `error` field. The original scope owns that classification and
/// retains actual provider error/quota evidence independently of delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryReadReplyKind {
    Result,
    ServiceError,
}

/// One original authenticated connection, independent of client and RPC ids.
pub trait RepositoryReadConnection: Send + Sync {
    /// Capture synchronously before the request's first await or spawn.
    /// Each call returns a distinct original request, including equal RPC ids.
    fn capture(&self) -> Arc<dyn RepositoryReadRequestScope>;

    /// Attach declared native read coverage synchronously before queueing.
    /// The query supplies invalidation keys, never authorization. Older owners
    /// retain their ordinary capture behavior.
    fn capture_context(
        &self,
        _query: &RepositoryContextQuery,
    ) -> Arc<dyn RepositoryReadRequestScope> {
        self.capture()
    }

    /// Retire only this connection's original request cohort. Idempotent.
    /// Concrete owners must join admitted leaves without holding cohort maps.
    fn retire(&self);

    /// One socket-private retirement feed. Older service implementations have none.
    fn take_retirements(&self) -> Option<Box<dyn RepositoryReadRetirements>> {
        None
    }
}

/// An original request's scope, kept until its final response handling finishes.
pub trait RepositoryReadRequestScope: Send + Sync {
    /// Restore the concrete owner's private context and run `body` exactly once.
    /// This must not turn an absent or retired capture into a later owner.
    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()>;

    /// Complete/cancel the original request even if clones of this scope escape.
    fn retire(&self);

    /// Validate and admit one already prepared response using original evidence.
    ///
    /// An unused ordinary request may transfer directly. A qualified request
    /// must freshly revalidate within a new child of the SAME original request
    /// and consume its authority fence through `transfer`. The synchronous
    /// action may only move a prebuilt packet into a pre-reserved slot: no
    /// serialization, reservation, cache lock, I/O, await or spawn inside.
    ///
    /// Service errors require explicit outcome policy; they are not public
    /// transport failures. Preserve actual provider errors/quota independently
    /// of private-payload eligibility. An error returned here must be safe to
    /// map to the existing local error policy without disclosing the withheld
    /// payload. Never retry an action that already transferred its packet.
    fn deliver<'a>(
        &'a self,
        kind: RepositoryReadReplyKind,
        transfer: &'a mut (dyn FnMut() -> Result<()> + Send),
    ) -> BoxFuture<'a, Result<()>>;
}

#[cfg(test)]
mod tests;

/// Original-socket control receiver. Dropping it permanently closes its feed.
pub trait RepositoryReadRetirements: Send {
    fn next(&mut self) -> BoxFuture<'_, Option<RepositoryContextRetired>>;
}

/// A requested lookup, never a path or caller-provided authority.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryContextQuery {
    pub workspace_id: crate::WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_root_id: Option<crate::WorkspaceGitRootId>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryContextBoundQuery {
    pub workspace_id: crate::WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_root_id: Option<crate::WorkspaceGitRootId>,
    pub repository_lifetime_id: String,
}

/// Explicit read coverage; neither variant permits a write or a provider call.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RepositoryContextCoverage {
    WorkspaceInventory {
        workspace_id: crate::WorkspaceId,
    },
    RegisteredRoot {
        workspace_id: crate::WorkspaceId,
        git_root_id: crate::WorkspaceGitRootId,
    },
}

/// These fields are rejection/correlation data, not transferable permission.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryContextCapture {
    pub lifetime_id: String,
    pub scope: crate::ExecutionScope,
    pub coverage: RepositoryContextCoverage,
    pub retirement_sequence: String,
    pub expires_after_ms: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryContextReleased {
    pub released: bool,
}

/// Contains no root, account, credential or private observation payload.
/// Sequence is emitted from a checked u64 as its canonical decimal string.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryContextRetired {
    pub lifetime_ids: Vec<String>,
    pub sequence: String,
    pub all_retired: bool,
    pub terminal: bool,
}

#[cfg(test)]
mod native_contract_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn repository_native_queries_and_coverage_have_one_strict_wire_shape() {
        let workspace = crate::WorkspaceId::new();
        let root = crate::WorkspaceGitRootId::new();
        let input = json!({"workspaceId":workspace});
        let q: RepositoryContextQuery = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(q).unwrap(), input);
        for key in [
            "principalId",
            "authorityScopeId",
            "path",
            "repositoryLifetimeId",
        ] {
            let mut bad = input.clone();
            bad[key] = json!("forged");
            assert!(serde_json::from_value::<RepositoryContextQuery>(bad).is_err());
        }
        let input =
            json!({"workspaceId":workspace,"gitRootId":root,"repositoryLifetimeId":"original"});
        let q: RepositoryContextBoundQuery = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(q).unwrap(), input);
        let coverage = RepositoryContextCoverage::RegisteredRoot {
            workspace_id: workspace.clone(),
            git_root_id: root.clone(),
        };
        assert_eq!(
            serde_json::to_value(coverage).unwrap(),
            json!({"kind":"registeredRoot","workspaceId":workspace,"gitRootId":root})
        );
        assert_eq!(
            serde_json::to_value(RepositoryContextCoverage::WorkspaceInventory {
                workspace_id: workspace.clone()
            })
            .unwrap(),
            json!({"kind":"workspaceInventory","workspaceId":workspace})
        );
        assert!(serde_json::from_value::<RepositoryContextCoverage>(
            json!({"kind":"workspaceInventory","workspaceId":workspace,"gitRootId":root})
        )
        .is_err());
    }
    #[test]
    fn repository_native_counters_remain_decimal_without_private_retirement_payload() {
        let capture = RepositoryContextCapture {
            lifetime_id: "lifetime".into(),
            scope: crate::ExecutionScope {
                daemon_id: "boot".into(),
                authority_scope_id: "scope".into(),
                authority_generation: u64::MAX,
            },
            coverage: RepositoryContextCoverage::WorkspaceInventory {
                workspace_id: crate::WorkspaceId::new(),
            },
            retirement_sequence: u64::MAX.to_string(),
            expires_after_ms: 300_000,
        };
        let value = serde_json::to_value(capture).unwrap();
        assert_eq!(value["scope"]["authorityGeneration"], u64::MAX.to_string());
        assert_eq!(value["retirementSequence"], u64::MAX.to_string());
        let notice = RepositoryContextRetired {
            lifetime_ids: vec![],
            sequence: u64::MAX.to_string(),
            all_retired: true,
            terminal: true,
        };
        assert_eq!(
            serde_json::to_value(notice).unwrap(),
            json!({"lifetimeIds":[],"sequence":u64::MAX.to_string(),"allRetired":true,"terminal":true})
        );
    }
}
