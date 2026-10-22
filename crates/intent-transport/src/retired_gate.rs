//! Read-only gate for retired workspaces (PROTOCOL §5.1 "Retired
//! workspaces"): **every** mutating router call scoped to a retired
//! workspace fails with `-32602 { code: "workspace-retired", workspaceId }`
//! and writes nothing — database-only writes (notes, tasks, comments,
//! primitives, context, settings, `workspace.update`) as well as calls that
//! need the deleted folder.
//!
//! The gate runs at the RPC front door before dispatch. A call is scoped to
//! a workspace by its `workspaceId` param, or by its `agentId` param (the
//! agent's workspace). Only methods in [`RETIRED_GATED_METHODS`] pay the one
//! store lookup; reads, daemon-global methods and the documented exceptions
//! ([`RETIRED_EXEMPT_METHODS`]) never do. Every router method must be
//! classified in exactly one list (enforced by the golden test below), so a
//! new method cannot silently bypass the gate.

use intent_core::{AgentId, Error, WorkspaceApi, WorkspaceId};
use serde_json::{Map, Value};

/// Workspace- or agent-scoped mutations refused on a retired workspace.
pub(crate) const RETIRED_GATED_METHODS: &[&str] = &[
    "accept-changes.addRemote",
    "accept-changes.execute",
    "accept-changes.mergePR",
    "accept-changes.prepare",
    "accept-changes.reconcile",
    "accept-changes.release",
    "agent.appendMessage",
    "agent.cancelDelete",
    "agent.cancelSubscriptions",
    "agent.create",
    "agent.delegate",
    "agent.delete",
    "agent.dismissQuestions",
    "agent.editAndRegenerate",
    "agent.editQueuedMessage",
    "agent.queueMessage",
    "agent.removeQueuedMessage",
    "agent.rename",
    "agent.replaceMessages",
    "agent.reportToParent",
    "agent.resolveInterrupted",
    "agent.resolveProposal",
    "agent.respondPermission",
    "agent.restore",
    "agent.retire",
    "agent.retry",
    "agent.sendQueuedMessageNow",
    "agent.sendQueuedMessagesNow",
    "agent.sendToTask",
    "agent.setModel",
    "agent.stop",
    "agent.update",
    "agent.wakeOrCreate",
    "comment.add",
    "comment.delete",
    "comment.resolveThread",
    "comment.respond",
    "file-tracking.stage",
    "file-tracking.unstage",
    "file.attachmentUpload.abort",
    "file.attachmentUpload.begin",
    "file.attachmentUpload.chunk",
    "file.attachmentUpload.commit",
    "file.delete",
    "file.mkdir",
    "file.placeAttachment",
    "file.rename",
    "file.write",
    "git.agentCommit",
    "git.checkoutBranch",
    "git.createBranch",
    "git.discard",
    "git.fetch",
    "git.pull",
    "git.push",
    "git.removeLockFile",
    "git.renameBranch",
    "git.stage",
    "git.stageHunk",
    "git.unstage",
    "git.unstageHunk",
    "hook.cancel",
    "hook.runNow",
    "note.add",
    "note.create",
    "note.delete",
    "note.edit",
    "note.editLines",
    "note.lineAttribution.computeNow",
    "note.restoreVersion",
    "note.saveAsset",
    "note.setContent",
    "note.update",
    "note.updateMetadata",
    "pr.refresh",
    "prMonitor.cancel",
    "prMonitor.flush",
    "primitive.addAgentAction",
    "primitive.addCli",
    "primitive.addPatch",
    "primitive.addReference",
    "repoConfig.ensureDir",
    "repoConfig.save",
    "rules.update",
    "sandbox.cow.discard",
    "sandbox.cow.merge",
    "script.archive",
    "script.create",
    "script.remove",
    "script.restart",
    "script.restore",
    "script.run",
    "script.start",
    "script.stop",
    "scriptMonitor.cancel",
    "scriptMonitor.cancelRun",
    "settings.reset",
    "settings.update",
    "specialist.create",
    "specialist.delete",
    "specialist.edit",
    "task.assignAgent",
    "task.convertBlocks",
    "task.createPrerequisite",
    "task.linkAgent",
    "task.markAsTask",
    "task.removeAgentFromAllTasks",
    "task.setRelations",
    "task.unlinkAgent",
    "task.update",
    "task.updateNoteStatus",
    "task.updateStatus",
    "terminal.create",
    "terminal.kill",
    "terminal.resize",
    "terminal.write",
    "workspace.archive",
    "workspace.cleanup",
    "workspace.duplicate",
    "workspace.export.abort",
    "workspace.export.finalize",
    "workspace.export.start",
    "workspace.generateSetupScript",
    "workspace.initializeRepository",
    "workspace.invite.revoke",
    "workspace.members.add",
    "workspace.members.leave",
    "workspace.members.remove",
    "workspace.repositorySelection.reconcile",
    "workspace.repositorySelection.release",
    "workspace.repositorySelection.reset",
    "workspace.repositorySelection.save",
    "workspace.saveSetupScript",
    "workspace.setAutoCommit",
    "workspace.setBrowserClient",
    "workspace.update",
    "workspace.updateContext",
    "workspace.updateUiContext",
];

/// Mutations allowed on a retired workspace: delete / cancelDelete, the
/// revive triggers (`workspace.unarchive`, `workspace.restore`, a user
/// message via `agent.sendMessage`), the idempotent `workspace.retire`, and
/// per-viewer bookkeeping that changes no workspace content.
#[cfg_attr(not(test), expect(dead_code))]
pub(crate) const RETIRED_EXEMPT_METHODS: &[&str] = &[
    "agent.markSeen",
    "agent.sendMessage",
    "workspace.cancelDelete",
    "workspace.delete",
    "workspace.dismissAttention",
    "workspace.markSeen",
    "workspace.restore",
    "workspace.retire",
    "workspace.unarchive",
];

/// Reads and daemon-global methods (no workspace content to protect): never
/// gated. Listed so the classification golden test covers every method.
#[cfg_attr(not(test), expect(dead_code))]
pub(crate) const RETIRED_UNGATED_METHODS: &[&str] = &[
    "accept-changes.getStatus",
    "agent.completeOnce",
    "agent.diagnostics",
    "agent.enhancePrompt",
    "agent.get",
    "agent.getConversation",
    "agent.getCreationPreferences",
    "agent.getMessageBlock",
    "agent.getModels",
    "agent.getQueue",
    "agent.getSession",
    "agent.getSessionStats",
    "agent.getSubscriptions",
    "agent.list",
    "agent.listActive",
    "agent.listInterrupted",
    "agent.listUserMessages",
    "agent.memoryUsage",
    "agent.pendingPermissions",
    "agent.subscribe",
    "agent.summary",
    "agent.unsubscribe",
    "agent.workers.list",
    "agent.workers.stop",
    "client.list",
    "comment.getThread",
    "comment.list",
    "crossWorkspace.listNotes",
    "crossWorkspace.listSiblings",
    "crossWorkspace.readNote",
    "debug.sampleStacks",
    "event.agentActivity",
    "event.query",
    "event.workspaceSummary",
    "file-tracking.getAgentLocks",
    "file-tracking.getChanges",
    "file-tracking.loadCommits",
    "file.exists",
    "file.getAttachmentInfo",
    "file.list",
    "file.read",
    "file.readChunk",
    "file.stat",
    "file.tree",
    "git.branchDiff",
    "git.branchStatus",
    "git.changes",
    "git.checkMergeConflicts",
    "git.clone",
    "git.commitDetails",
    "git.commits",
    "git.diffs",
    "git.getBranches",
    "git.getConfig",
    "git.getRemoteUrl",
    "git.numstat",
    "git.showFile",
    "git.status",
    "gitRoot.list",
    "github.authStatus",
    "github.branches.list",
    "github.branches.listCached",
    "github.cancelAuth",
    "github.connect",
    "github.getReviewThreads",
    "github.getUser",
    "github.identityProof.create",
    "github.identityProof.delete",
    "github.issues.get",
    "github.issues.list",
    "github.issues.search",
    "github.listReviewComments",
    "github.pulls.checks",
    "github.pulls.create",
    "github.pulls.files",
    "github.pulls.get",
    "github.pulls.list",
    "github.pulls.merge",
    "github.pulls.reviews",
    "github.pulls.search",
    "github.pulls.updateBranch",
    "github.relatedRepos.list",
    "github.replyReviewComment",
    "github.repoConfig.get",
    "github.repos.get",
    "github.repos.list",
    "github.repos.search",
    "github.resolveThread",
    "github.revoke",
    "github.unresolveThread",
    "github.users.search",
    "hook.list",
    "host.executionContext",
    "host.invite.list",
    "host.invite.revoke",
    "host.invite.searchAccounts",
    "host.members.list",
    "host.members.remove",
    "identity.authStatus",
    "identity.cancelAuth",
    "identity.connect",
    "identity.getUser",
    "identity.revoke",
    "identity.select",
    "linear.authStatus",
    "linear.createIssue",
    "linear.getIssue",
    "linear.listIssues",
    "linear.listLabels",
    "linear.listProjects",
    "linear.listTeams",
    "linear.listWorkflowStates",
    "linear.searchIssues",
    "linear.updateIssue",
    "linear.viewer",
    "mcp.oauth.delete",
    "mcp.oauth.get",
    "mcp.oauth.list",
    "mcp.oauth.set",
    "mcp.servers.create",
    "mcp.servers.delete",
    "mcp.servers.getStatus",
    "mcp.servers.list",
    "mcp.servers.restart",
    "mcp.servers.toggle",
    "mcp.servers.update",
    "mcp.testConnection",
    "metrics.getAgentStats",
    "models.list",
    "note.get",
    "note.getVersion",
    "note.lineAttribution.load",
    "note.list",
    "note.listTasks",
    "note.listVersions",
    "note.readAsset",
    "prMonitor.list",
    "presence.snapshot",
    "principal.list",
    "principal.me",
    "principal.revokeSelf",
    "providers.catalog",
    "providers.profiles.create",
    "providers.profiles.delete",
    "providers.profiles.list",
    "providers.profiles.update",
    "repo.list",
    "repo.remove",
    "repo.warmCache",
    "repoConfig.get",
    "repoConfig.has",
    "rules.get",
    "rules.list",
    "script.get",
    "script.list",
    "script.output",
    "script.status",
    "scriptMonitor.get",
    "scriptMonitor.list",
    "search.cancel",
    "search.codebase",
    "search.events",
    "search.fileNames",
    "search.inFiles",
    "search.messages",
    "search.notes",
    "sentry.assignIssue",
    "sentry.authStatus",
    "sentry.getIssue",
    "sentry.ignoreIssue",
    "sentry.listIssues",
    "sentry.listProjects",
    "sentry.resolveIssue",
    "sentry.searchIssues",
    "settings.get",
    "settings.list",
    "skill.list",
    "sourceControl.authStatus",
    "sourceControl.cancelAuth",
    "sourceControl.checkout.branches",
    "sourceControl.checkout.capture",
    "sourceControl.checkout.project",
    "sourceControl.checkout.projects",
    "sourceControl.checkout.release",
    "sourceControl.checkout.repoConfig",
    "sourceControl.checkout.warm",
    "sourceControl.connect",
    "sourceControl.getUser",
    "sourceControl.gitlab.childGit.get",
    "sourceControl.gitlab.childGit.set",
    "sourceControl.identityProof.create",
    "sourceControl.identityProof.delete",
    "sourceControl.read.capture",
    "sourceControl.read.captureScoped",
    "sourceControl.read.detail",
    "sourceControl.read.detailScoped",
    "sourceControl.read.discussions",
    "sourceControl.read.list",
    "sourceControl.read.release",
    "sourceControl.read.releaseScoped",
    "sourceControl.read.resolveKnownRepository",
    "sourceControl.revoke",
    "specialist.get",
    "specialist.list",
    "stats.getRateHistory",
    "stats.getUsage",
    "system.capabilities",
    "task.get",
    "task.getMyTask",
    "task.list",
    "task.listAgentLinks",
    "terminal.getBuffer",
    "terminal.list",
    "terminal.readOutput",
    "unsloth.status",
    "unsloth.stop",
    "voice.getWorkspaceVocabulary",
    "voice.transcribe",
    "workspace.create",
    "workspace.detectProjectType",
    "workspace.diskUsage",
    "workspace.export.read",
    "workspace.findByPullRequest",
    "workspace.findRepositories",
    "workspace.get",
    "workspace.getAutoCommit",
    "workspace.getBrowserClient",
    "workspace.getContext",
    "workspace.getSetupScript",
    "workspace.getTokenUsage",
    "workspace.getUiContext",
    "workspace.import.abort",
    "workspace.import.begin",
    "workspace.import.chunk",
    "workspace.import.commit",
    "workspace.invite.list",
    "workspace.list",
    "workspace.localChanges",
    "workspace.members.list",
    "workspace.repositoryContext",
    "workspace.repositoryContext.capture",
    "workspace.repositoryContext.release",
    "workspace.repositorySelection.capture",
    "workspace.transfer.plan",
];

/// Refuse `method` when it is a gated mutation scoped to a retired
/// workspace. Lookup failures fail open: the handler's own lookup reports
/// missing workspaces and agents with its usual error.
pub(crate) async fn check(
    api: &dyn WorkspaceApi,
    method: &str,
    params: &Map<String, Value>,
) -> Result<(), Error> {
    let method = crate::catalog::canonical_method(method);
    if !RETIRED_GATED_METHODS.contains(&method) {
        return Ok(());
    }
    let workspace_id = params
        .get("workspaceId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(WorkspaceId::from);
    let agent_id = params
        .get("agentId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(|s| AgentId::from(s.to_string()));
    if workspace_id.is_none() && agent_id.is_none() {
        return Ok(());
    }
    match api.retired_workspace_for(workspace_id, agent_id).await {
        Ok(Some(retired)) => Err(Error::WorkspaceRetired {
            workspace_id: retired.as_str().to_string(),
        }),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Every router method is classified in exactly one list.
    #[test]
    fn every_router_method_is_classified_once() {
        let mut seen = HashSet::new();
        for m in RETIRED_GATED_METHODS
            .iter()
            .chain(RETIRED_EXEMPT_METHODS)
            .chain(RETIRED_UNGATED_METHODS)
        {
            assert!(seen.insert(*m), "{m} is classified more than once");
        }
        let router: HashSet<&str> = crate::catalog::ROUTER_METHODS.iter().copied().collect();
        let unclassified: Vec<_> = router.difference(&seen).collect();
        assert!(
            unclassified.is_empty(),
            "classify these router methods in retired_gate.rs (gated write, exempt, or ungated read/global): {unclassified:?}"
        );
        let stale: Vec<_> = seen.difference(&router).collect();
        assert!(stale.is_empty(), "not router methods: {stale:?}");
    }

    struct RetiredApi;

    impl WorkspaceApi for RetiredApi {
        fn retired_workspace_for(
            &self,
            workspace_id: Option<WorkspaceId>,
            agent_id: Option<AgentId>,
        ) -> intent_core::BoxFuture<'_, intent_core::Result<Option<WorkspaceId>>> {
            Box::pin(async move {
                if workspace_id
                    .as_ref()
                    .is_some_and(|w| w.as_str() == "ws-retired")
                    || agent_id.as_ref().is_some_and(|a| a.0 == "agent-retired")
                {
                    return Ok(Some(WorkspaceId::from("ws-retired")));
                }
                Ok(None)
            })
        }
    }

    fn params(v: &Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[tokio::test]
    async fn gated_writes_are_refused_on_retired_workspaces_only() {
        let retired = params(&serde_json::json!({ "workspaceId": "ws-retired" }));
        let live = params(&serde_json::json!({ "workspaceId": "ws-live" }));
        for method in [
            "note.create",
            "task.update",
            "workspace.update",
            "file.write",
            "git.agentCommit",
            "script.start",
            "script.run",
            "terminal.create",
        ] {
            let err = check(&RetiredApi, method, &retired).await.unwrap_err();
            assert!(
                matches!(&err, Error::WorkspaceRetired { workspace_id } if workspace_id == "ws-retired"),
                "{method}: {err:?}"
            );
            check(&RetiredApi, method, &live).await.expect(method);
        }
        // Agent-scoped writes resolve the agent's workspace.
        let by_agent = params(&serde_json::json!({ "agentId": "agent-retired" }));
        assert!(check(&RetiredApi, "agent.rename", &by_agent).await.is_err());
        // Reads and the documented exceptions pass.
        for method in [
            "note.get",
            "workspace.get",
            "agent.getConversation",
            "workspace.delete",
            "workspace.unarchive",
            "workspace.markSeen",
            "agent.markSeen",
            "agent.sendMessage",
        ] {
            check(&RetiredApi, method, &retired).await.expect(method);
            check(&RetiredApi, method, &by_agent).await.expect(method);
        }
    }
}
