//! Git and forge gate for plain-folder workspaces (PROTOCOL §5.1
//! "Plain-folder workspaces"): an explicit git, staging, accept-changes or
//! pull-request mutation scoped to a plain-folder workspace fails with
//! `-32602 { code: "plain-folder-unsupported", operation }` and the message
//! `<operation> is not available in plain-folder workspaces`, and changes
//! nothing — so it can never run git in the user's folder or emit a
//! `git:*`, `changes:*` or `pr:*` event for it.
//!
//! The gate runs at the RPC front door before dispatch, next to the retired
//! gate. A call is scoped by its `workspaceId` param; only methods in
//! [`PLAIN_FOLDER_GATED_METHODS`] pay the one store lookup, which reads the
//! persisted `Workspace.plainFolder` flag. Lookup failures fail open: the
//! handler reports a missing workspace with its usual error.

use intent_core::{Error, WorkspaceApi, WorkspaceId};
use serde_json::{Map, Value};

/// Workspace-scoped git and forge mutations refused on a plain folder.
pub(crate) const PLAIN_FOLDER_GATED_METHODS: &[&str] = &[
    "accept-changes.addRemote",
    "accept-changes.execute",
    "accept-changes.mergePR",
    "accept-changes.prepare",
    "accept-changes.reconcile",
    "accept-changes.release",
    "file-tracking.stage",
    "file-tracking.unstage",
    "git.agentCommit",
    "git.checkoutBranch",
    "git.createBranch",
    "git.discard",
    "git.fetch",
    "git.push",
    "git.removeLockFile",
    "git.renameBranch",
    "git.stage",
    "git.stageHunk",
    "git.unstage",
    "git.unstageHunk",
    "pr.refresh",
];

/// Refuse `method` when it is a gated git or forge mutation scoped to a
/// plain-folder workspace.
pub(crate) async fn check(
    api: &dyn WorkspaceApi,
    method: &str,
    params: &Map<String, Value>,
) -> Result<(), Error> {
    let method = crate::catalog::canonical_method(method);
    let Some(operation) = PLAIN_FOLDER_GATED_METHODS
        .iter()
        .copied()
        .find(|gated| *gated == method)
    else {
        return Ok(());
    };
    let Some(workspace_id) = params
        .get("workspaceId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(WorkspaceId::from)
    else {
        return Ok(());
    };
    if api.is_plain_folder_workspace(workspace_id).await {
        Err(Error::PlainFolderUnsupported { operation })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::BoxFuture;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Every gated method is a real router method and a workspace mutation
    /// (it is also gated for retired workspaces).
    #[test]
    fn gated_methods_are_router_mutations() {
        for method in PLAIN_FOLDER_GATED_METHODS {
            assert!(
                crate::catalog::ROUTER_METHODS.contains(method),
                "{method} is not a router method"
            );
            assert!(
                crate::retired_gate::RETIRED_GATED_METHODS.contains(method),
                "{method} is not a gated mutation"
            );
        }
    }

    #[derive(Default)]
    struct Api {
        lookups: AtomicUsize,
    }

    impl WorkspaceApi for Api {
        fn is_plain_folder_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, bool> {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { id.as_str() == "ws-plain" })
        }
    }

    fn params(workspace_id: &str) -> Map<String, Value> {
        json!({ "workspaceId": workspace_id, "paths": ["a.txt"] })
            .as_object()
            .unwrap()
            .clone()
    }

    #[tokio::test]
    async fn plain_folder_mutations_are_refused_with_the_documented_error() {
        let api = Api::default();
        for method in PLAIN_FOLDER_GATED_METHODS {
            let err = check(&api, method, &params("ws-plain")).await.unwrap_err();
            assert!(
                matches!(err, Error::PlainFolderUnsupported { operation } if operation == *method),
                "{method}: {err:?}"
            );
            assert_eq!(err.code(), -32602, "{method}");
            assert_eq!(
                err.to_string(),
                format!("{method} is not available in plain-folder workspaces")
            );
        }
    }

    #[tokio::test]
    async fn git_workspaces_reads_and_unscoped_calls_pass() {
        let api = Api::default();
        for method in PLAIN_FOLDER_GATED_METHODS {
            check(&api, method, &params("ws-git")).await.unwrap();
            // A missing workspace reads as not plain: the handler reports it.
            check(&api, method, &params("ws-missing")).await.unwrap();
            check(&api, method, &Map::new()).await.unwrap();
        }
        let lookups = api.lookups.load(Ordering::SeqCst);
        // Reads and other methods never pay the lookup.
        for method in [
            "git.status",
            "accept-changes.getStatus",
            "note.add",
            "file.write",
        ] {
            check(&api, method, &params("ws-plain")).await.unwrap();
        }
        assert_eq!(api.lookups.load(Ordering::SeqCst), lookups);
    }
}
