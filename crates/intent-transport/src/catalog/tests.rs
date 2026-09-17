//! Golden freeze tests for the v2.0 protocol catalog.
//!
//! These tests enforce that any surface drift (added/removed/renamed methods)
//! fails CI with a clear "update the catalog + docs/protocol/ + bump protocol
//! version" message.
//!
//! Router and fast-path methods are mechanically extracted from source at test
//! runtime to detect drift. Notifications and reverse RPCs use count-based
//! checks only (extracting reverse `.request("...")` call sites would be
//! fragile); renames are a lower-risk edge case caught during code review.

use super::{
    canonical_method, collaborator_may_call, COLLABORATOR_METHODS, FASTPATH_METHODS,
    METHOD_ALIASES, NOTIFICATIONS, REVERSE_METHODS, ROUTER_METHODS,
};
use std::collections::{BTreeSet, HashSet};
use std::fmt::Write as _;

/// Extract router methods from the actual source code at test runtime.
///
/// Assumptions: router.rs uses single-line match arms of the form `"method.name" => ...`
/// or `"alias1" | "alias2" => ...`; no escaped quotes in method names; assumes inline
/// comments (if any) appear after the match arm and are tolerated. Namespaces may be
/// hyphenated (`accept-changes.*`, `file-tracking.*`).
fn extract_router_methods() -> HashSet<String> {
    // Read source at runtime to detect drift even after the test is compiled
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let router_path = std::path::Path::new(manifest_dir).join("src/router.rs");
    let source =
        std::fs::read_to_string(&router_path).expect("Failed to read router.rs at test time");

    let mut methods = HashSet::new();

    // Match patterns like: "method.name" => or "git.diffs" | "git.diff" =>
    for line in source.lines() {
        // Look for quoted method names followed by => (with possible | for aliases)
        if let Some(start) = line.find('"') {
            if line[start..].contains("=>") {
                // Extract all quoted strings before =>
                let before_arrow = &line[..line.find("=>").unwrap_or(line.len())];
                for part in before_arrow.split('"') {
                    let trimmed = part.trim();
                    // Check if it looks like a method name (has a dot and alphanumeric)
                    if trimmed.contains('.')
                        && trimmed
                            .chars()
                            .all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '-')
                    {
                        methods.insert(trimmed.to_string());
                    }
                }
            }
        }
    }

    methods
}

/// Extract fast-path methods from source files at test runtime.
///
/// Assumption: each fast-path module (events.rs, client.rs, etc.) contains methods
/// exclusively from one namespace prefix (e.g., events.rs contains only events.* methods).
fn extract_fastpath_methods() -> HashSet<String> {
    let mut methods = HashSet::new();
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let base_path = std::path::Path::new(manifest_dir).join("src");

    // Extract from each fast-path module
    // Look for match arms and direct comparisons (like client.hello)
    for (filename, prefix) in [
        ("events.rs", "events."),
        ("client.rs", "client."),
        ("drafts.rs", "drafts."),
        ("browser.rs", "browser."),
        ("forward.rs", "forward."),
        ("host.rs", "host."),
        ("control.rs", "system."),
        ("pairing.rs", "pairing."),
        ("server.rs", "server."),
        ("provider_setup.rs", "providers.setup."),
        ("invite.rs", "invite."),
        ("invite.rs", "workspace.invite."),
        ("presence.rs", "presence."),
        ("presence.rs", "note.presence."),
    ] {
        let source = std::fs::read_to_string(base_path.join(filename))
            .unwrap_or_else(|_| panic!("Failed to read {filename} at test time"));
        for line in source.lines() {
            let trimmed = line.trim();
            // Skip comments
            if trimmed.starts_with("//") {
                continue;
            }
            // Only process lines that are match arms (contain =>) or comparisons (!= or ==)
            let is_match_arm =
                trimmed.contains("=>") && !trimmed.contains("//") && trimmed.contains('"');
            let is_comparison = (trimmed.contains("!=") || trimmed.contains("=="))
                && trimmed.contains('"')
                && trimmed.contains(prefix);

            if is_match_arm || is_comparison {
                // Extract quoted strings
                let mut start_idx = 0;
                while let Some(start) = line[start_idx..].find('"') {
                    let abs_start = start_idx + start;
                    if let Some(end) = line[abs_start + 1..].find('"') {
                        let method = &line[abs_start + 1..abs_start + 1 + end];
                        if method.starts_with(prefix)
                            && method
                                .chars()
                                .all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '-')
                        {
                            methods.insert(method.to_string());
                        }
                        start_idx = abs_start + 1 + end + 1;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    methods
}

/// Golden count: total client-callable methods (router + fastpath + aliases).
///
/// This constant must match the sum below. If it doesn't, you've added or
/// removed methods without updating the catalog. The catalog freeze is
/// intentional: any surface change requires a protocol version bump and a
/// docs/protocol/ update.
///
/// REV-2 browser tab registry (intent-hq/intent#461): +4 fast-path methods
/// (`browser.listTabs` / `browser.upsertTab` / `browser.removeTab` /
/// `browser.syncTabs`), protocol 9.10. REV-2 routing: +2 fast-path methods
/// (`browser.navigateTab` / `browser.closeTab`), protocol 9.11.
///
/// 355 → 366: `extract_router_methods` rejected `-` in method names, so the 11
/// already-shipped `accept-changes.*` / `file-tracking.*` router arms were never
/// frozen here. No protocol bump — the wire surface did not change.
///
/// Multiplayer w4 (invite links + identity-only join): +4 router methods
/// (`workspace.invite.list` / `workspace.invite.revoke` /
/// `workspace.members.leave` / `principal.revokeSelf`) and +2 fast-path
/// methods (`workspace.invite.create`, `invite.redeem`).
///
/// Multiplayer w5 (presence): +2 fast-path methods (`presence.update`,
/// `note.presence.update`) and +1 router method (`presence.snapshot`); the
/// `note.presence.subscribe` / `note.presence.unsubscribe` channel pair is
/// counted with the other subscription channels, not here.
///
/// Also within 10.2: +1 router method (`github.users.search`, the
/// collaborator picker's login-prefix user search).
///
/// Returning guest (multiplayer w4): +2 fast-path methods on the `/invite`
/// endpoint (`invite.inspect`, `invite.accept`).
///
/// Gist identity proof (guest half): +2 router methods
/// (`github.identityProof.create`, `github.identityProof.delete`).
///
/// Gist identity proof (host half): +2 fast-path methods on the `/invite`
/// endpoint (`invite.challenge`, `invite.prove`).
///
/// Gist identity proof replaces the host-side device flow: −1 fast-path
/// method (`invite.redeem`); the guest joins through `invite.challenge` /
/// `invite.prove` (or `invite.accept` with a credential) instead.
const EXPECTED_TOTAL_METHODS: usize = 384;

/// Golden count: router methods (canonical + canonical forms of aliases).
/// This includes both git.diffs and git.commits (the canonical forms) even
/// though git.diff→git.diffs and git.log→git.commits are listed as aliases.
const EXPECTED_ROUTER_METHODS: usize = 326;

/// Golden count: fast-path methods (intercepted before router).
const EXPECTED_FASTPATH_METHODS: usize = 56;

/// Golden count: method aliases.
const EXPECTED_ALIASES: usize = 2;

/// Golden count: server→client notifications.
const EXPECTED_NOTIFICATIONS: usize = 1;

/// Golden count: client-served reverse RPCs.
const EXPECTED_REVERSE_METHODS: usize = 5;

#[test]
fn router_methods_match_actual_source() {
    let actual = extract_router_methods();
    let catalog: HashSet<String> = ROUTER_METHODS
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    // Combine catalog + aliases for comparison
    let mut expected = catalog.clone();
    for (alias, _) in METHOD_ALIASES {
        expected.insert(alias.to_string());
    }

    // Find methods in router.rs but not in catalog
    let missing: Vec<_> = actual
        .difference(&expected)
        .map(std::string::String::as_str)
        .collect();

    // Find methods in catalog but not in router.rs
    let extra: Vec<_> = catalog
        .difference(&actual)
        .map(std::string::String::as_str)
        .collect();

    if !missing.is_empty() || !extra.is_empty() {
        let mut msg = String::from(
            "Router method drift detected!\n\
             Update ROUTER_METHODS in catalog.rs, update docs/protocol/05-method-catalog.md, and bump the protocol version.\n"
        );

        if !missing.is_empty() {
            let _ = writeln!(
                msg,
                "\nMethods in router.rs but NOT in catalog ({}):",
                missing.len()
            );
            for m in missing.iter().take(10) {
                let _ = writeln!(msg, "  - {m}");
            }
            if missing.len() > 10 {
                let _ = writeln!(msg, "  ... and {} more", missing.len() - 10);
            }
        }

        if !extra.is_empty() {
            let _ = writeln!(
                msg,
                "\nMethods in catalog but NOT in router.rs ({}):",
                extra.len()
            );
            for m in extra.iter().take(10) {
                let _ = writeln!(msg, "  - {m}");
            }
            if extra.len() > 10 {
                let _ = writeln!(msg, "  ... and {} more", extra.len() - 10);
            }
        }

        panic!("{}", msg);
    }
}

#[test]
fn fastpath_methods_match_actual_source() {
    let actual = extract_fastpath_methods();
    let catalog: HashSet<String> = FASTPATH_METHODS
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    let missing: Vec<_> = actual
        .difference(&catalog)
        .map(std::string::String::as_str)
        .collect();

    let extra: Vec<_> = catalog
        .difference(&actual)
        .map(std::string::String::as_str)
        .collect();

    if !missing.is_empty() || !extra.is_empty() {
        let mut msg = String::from(
            "Fast-path method drift detected!\n\
             Update FASTPATH_METHODS in catalog.rs, update docs/protocol/05-method-catalog.md, and bump the protocol version.\n"
        );

        if !missing.is_empty() {
            let _ = writeln!(
                msg,
                "\nMethods in source but NOT in catalog ({}):",
                missing.len()
            );
            for m in &missing {
                let _ = writeln!(msg, "  - {m}");
            }
        }

        if !extra.is_empty() {
            let _ = writeln!(
                msg,
                "\nMethods in catalog but NOT in source ({}):",
                extra.len()
            );
            for m in &extra {
                let _ = writeln!(msg, "  - {m}");
            }
        }

        panic!("{}", msg);
    }
}

#[test]
fn catalog_counts_frozen() {
    let router_count = ROUTER_METHODS.len();
    let fastpath_count = FASTPATH_METHODS.len();
    let alias_count = METHOD_ALIASES.len();
    let total = router_count + fastpath_count + alias_count;

    assert_eq!(
        router_count, EXPECTED_ROUTER_METHODS,
        "Router method count drift detected: expected {EXPECTED_ROUTER_METHODS}, got {router_count}. \
         If you added/removed router methods, update ROUTER_METHODS in catalog.rs, \
         bump EXPECTED_ROUTER_METHODS, update docs/protocol/05-method-catalog.md, and bump the protocol version."
    );

    assert_eq!(
        fastpath_count, EXPECTED_FASTPATH_METHODS,
        "Fast-path method count drift detected: expected {EXPECTED_FASTPATH_METHODS}, got {fastpath_count}. \
         If you added/removed fast-path methods, update FASTPATH_METHODS in catalog.rs, \
         bump EXPECTED_FASTPATH_METHODS, update docs/protocol/05-method-catalog.md, and bump the protocol version."
    );

    assert_eq!(
        alias_count, EXPECTED_ALIASES,
        "Alias count drift detected: expected {EXPECTED_ALIASES}, got {alias_count}. \
         If you added/removed aliases, update METHOD_ALIASES in catalog.rs, \
         bump EXPECTED_ALIASES, update docs/protocol/05-method-catalog.md, and bump the protocol version."
    );

    assert_eq!(
        total, EXPECTED_TOTAL_METHODS,
        "Total method count drift detected: expected {EXPECTED_TOTAL_METHODS}, got {total} ({router_count} router + {fastpath_count} fastpath + {alias_count} aliases). \
         Update the catalog, docs/protocol/05-method-catalog.md, and bump the protocol version."
    );

    let notification_count = NOTIFICATIONS.len();
    assert_eq!(
        notification_count, EXPECTED_NOTIFICATIONS,
        "Notification count drift detected: expected {EXPECTED_NOTIFICATIONS}, got {notification_count}. \
         Update NOTIFICATIONS in catalog.rs, docs/protocol/05-method-catalog.md, and bump the protocol version."
    );

    let reverse_count = REVERSE_METHODS.len();
    assert_eq!(
        reverse_count, EXPECTED_REVERSE_METHODS,
        "Reverse RPC count drift detected: expected {EXPECTED_REVERSE_METHODS}, got {reverse_count}. \
         Update REVERSE_METHODS in catalog.rs, docs/protocol/05-method-catalog.md, and bump the protocol version."
    );
}

#[test]
fn router_methods_are_sorted() {
    let mut sorted = ROUTER_METHODS.to_vec();
    sorted.sort_unstable();
    assert_eq!(
        ROUTER_METHODS,
        &sorted[..],
        "ROUTER_METHODS must be sorted alphabetically for readability"
    );
}

#[test]
fn fastpath_methods_are_sorted() {
    let mut sorted = FASTPATH_METHODS.to_vec();
    sorted.sort_unstable();
    assert_eq!(
        FASTPATH_METHODS,
        &sorted[..],
        "FASTPATH_METHODS must be sorted alphabetically for readability"
    );
}

#[test]
fn no_duplicate_router_methods() {
    let mut seen = std::collections::HashSet::new();
    for method in ROUTER_METHODS {
        assert!(seen.insert(method), "Duplicate router method: {method}");
    }
}

#[test]
fn no_duplicate_fastpath_methods() {
    let mut seen = std::collections::HashSet::new();
    for method in FASTPATH_METHODS {
        assert!(seen.insert(method), "Duplicate fast-path method: {method}");
    }
}

#[test]
fn no_overlap_between_router_and_fastpath() {
    let router_set: std::collections::HashSet<_> = ROUTER_METHODS.iter().collect();
    for method in FASTPATH_METHODS {
        assert!(
            !router_set.contains(method),
            "Method {method} appears in both ROUTER_METHODS and FASTPATH_METHODS"
        );
    }
}

#[test]
fn aliases_point_to_router_methods() {
    let router_set: std::collections::HashSet<_> = ROUTER_METHODS.iter().collect();
    for (alias, canonical) in METHOD_ALIASES {
        assert!(
            router_set.contains(canonical),
            "Alias {alias} points to {canonical}, but {canonical} is not in ROUTER_METHODS"
        );
    }
}

/// Multiplayer w2 — the frozen set of user-origin chat entry points (Product
/// Brief "Chat attribution"). Each is a `(method, how the human principal
/// stamp reaches the persisted row / queue entry)` tuple; the service-side
/// matrix (`intent_services::agent_ops::tests::principal_stamp_overwrites_client_value_on_every_user_origin_entry_point`)
/// proves the stamp per row, and this golden freezes the set so a new entry
/// point cannot be added without classifying it.
const USER_ORIGIN_MESSAGE_ENTRY_POINTS: &[(&str, &str)] = &[
    (
        "agent.appendMessage",
        "role `user` rows are stamped in `WorkspaceApi::agent_append_message`; other roles strip",
    ),
    (
        "agent.create",
        "stores `initialMessage` on the session only — no transcript row; the kickoff arrives through agent.sendMessage",
    ),
    (
        "agent.editAndRegenerate",
        "the edited message is a fresh user row stamped with the editor in `WorkspaceApi::agent_edit_and_regenerate`",
    ),
    (
        "agent.editQueuedMessage",
        "a wire edit of a human-authored entry (stamped, or user-origin) re-stamps the editor; agent/daemon edits keep the author",
    ),
    (
        "agent.queueMessage",
        "stamped on the queue entry in `WorkspaceApi::agent_queue_message`; the drain re-persists it",
    ),
    (
        "agent.retry",
        "re-delivers the requeued entry with the stamp captured at enqueue / wake delivery — the retrier is never the author (`guest_wake_stamp_survives_terminal_failure_requeue_over_wss`)",
    ),
    (
        "agent.sendMessage",
        "user-origin sends are stamped in `WorkspaceApi::agent_send_message`; direct persist, busy enqueue and auto-queue share it",
    ),
    (
        "agent.sendQueuedMessageNow",
        "drains the entry with the stamp captured at enqueue (the drainer is not the author)",
    ),
    (
        "agent.sendToTask",
        "stamped in `WorkspaceApi::agent_send_to_task` before the assignee delivery",
    ),
    (
        "agent.wakeOrCreate",
        "stamped on the input in `WorkspaceApi::agent_wake_or_create`; the wake row, parked queue entry and worker options carry it",
    ),
    (
        "workspace.create",
        "the `initialAgent.prompt` kickoff is the creator's first user row, stamped in `WorkspaceApi::create_workspace` before the runtime / store-only delivery",
    ),
];

/// The complement of [`USER_ORIGIN_MESSAGE_ENTRY_POINTS`]: every other method
/// in the full catalog (router + fast path), each affirmed NOT to write a
/// human-authored chat row or queue entry. Together the two ledgers must
/// partition the catalog exactly ([`chat_write_classification`]), so adding
/// ANY method — with or without a `messageMetadata` param — fails CI until it
/// is placed in one list or the other.
///
/// Classification notes for the entries a reader might question:
/// - `agent.replaceMessages` swaps the transcript wholesale (compaction /
///   restore); it re-persists rows with the metadata they already carry and
///   is not a human authoring event, so the Product Brief's entry-point set
///   excludes it. Provenance is intentionally preserved verbatim there —
///   including any `fromPrincipalId` on restored user rows — because it is an
///   administrator-only historical restore/compaction operation: wave-3
///   enforcement removes it from `COLLABORATOR_METHODS`, so collaborators
///   cannot reach it and no non-owner caller can author a row through it.
/// - `agent.dismissQuestions` delivers a daemon-authored system notice
///   (`type: questions_dismissed`, `source: system` — agent/automatic to
///   the author projection), never a human row; `agent.respondPermission`
///   and `agent.stop` drive turn control without appending a chat row
///   (question answers travel as `agent.sendMessage` content).
/// - `note.*`, `comment.*`, `task.*`, `github.*` write notes, comments and
///   forge objects — never the agent transcript; `accept-changes.*` and
///   `file-tracking.*` drive the worktree / PR flow the same way.
const NON_USER_ORIGIN_METHODS: &[&str] = &[
    "accept-changes.addRemote",
    "accept-changes.execute",
    "accept-changes.getStatus",
    "accept-changes.mergePR",
    "accept-changes.prepare",
    "agent.cancelDelete",
    "agent.cancelSubscriptions",
    "agent.completeOnce",
    "agent.delegate",
    "agent.delete",
    "agent.diagnostics",
    "agent.dismissQuestions",
    "agent.enhancePrompt",
    "agent.get",
    "agent.getConversation",
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
    "agent.markSeen",
    "agent.pendingPermissions",
    "agent.removeQueuedMessage",
    "agent.rename",
    "agent.replaceMessages",
    "agent.reportToParent",
    "agent.resolveInterrupted",
    "agent.resolveProposal",
    "agent.respondPermission",
    "agent.restore",
    "agent.setModel",
    "agent.stop",
    "agent.subscribe",
    "agent.summary",
    "agent.unsubscribe",
    "agent.update",
    "browser.closeTab",
    "browser.exec",
    "browser.listTabs",
    "browser.navigateTab",
    "browser.removeTab",
    "browser.syncTabs",
    "browser.upsertTab",
    "client.hello",
    "client.list",
    "comment.add",
    "comment.delete",
    "comment.getThread",
    "comment.list",
    "comment.resolveThread",
    "comment.respond",
    "crossWorkspace.listNotes",
    "crossWorkspace.listSiblings",
    "crossWorkspace.readNote",
    "debug.sampleStacks",
    "drafts.clear",
    "drafts.get",
    "drafts.set",
    "event.agentActivity",
    "event.query",
    "event.workspaceSummary",
    "events.subscribe",
    "events.unsubscribe",
    "file-tracking.getAgentLocks",
    "file-tracking.getChanges",
    "file-tracking.getLineStats",
    "file-tracking.loadCommits",
    "file-tracking.stage",
    "file-tracking.unstage",
    "file.attachmentUpload.abort",
    "file.attachmentUpload.begin",
    "file.attachmentUpload.chunk",
    "file.attachmentUpload.commit",
    "file.delete",
    "file.exists",
    "file.getAttachmentInfo",
    "file.list",
    "file.mkdir",
    "file.placeAttachment",
    "file.read",
    "file.readChunk",
    "file.rename",
    "file.stat",
    "file.tree",
    "file.write",
    "forward.close",
    "forward.create",
    "forward.list",
    "git.agentCommit",
    "git.branchDiff",
    "git.branchStatus",
    "git.changes",
    "git.checkMergeConflicts",
    "git.checkoutBranch",
    "git.clone",
    "git.commit",
    "git.commitDetails",
    "git.commits",
    "git.createBranch",
    "git.diffs",
    "git.discard",
    "git.fetch",
    "git.getBranches",
    "git.getConfig",
    "git.getRemoteUrl",
    "git.numstat",
    "git.pull",
    "git.push",
    "git.removeLockFile",
    "git.renameBranch",
    "git.showFile",
    "git.stage",
    "git.stageHunk",
    "git.status",
    "git.unstage",
    "git.unstageHunk",
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
    "github.pulls.create",
    "github.pulls.get",
    "github.pulls.list",
    "github.pulls.merge",
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
    "hook.cancel",
    "hook.list",
    "hook.runNow",
    "host.checkAuggie",
    "host.checkGh",
    "host.checkGit",
    "host.checkNode",
    "host.createDirectory",
    "host.directoryStatus",
    "host.env",
    "host.exec",
    "host.execStream",
    "host.execStream.cancel",
    "host.execStream.write",
    "host.findApp",
    "host.findBinary",
    "host.listDirectory",
    "host.listInstalledEditors",
    "host.openInEditor",
    "host.providerAuthStatus",
    "host.providerDiscovery",
    "host.providerTestPrompt",
    "host.status",
    "host.toolAvailability",
    "invite.accept",
    "invite.challenge",
    "invite.inspect",
    "invite.prove",
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
    "metrics.clearAgentStats",
    "metrics.getAgentStats",
    "metrics.getAllWorkspaceStats",
    "metrics.getWorkspaceStats",
    "models.list",
    "note.add",
    "note.create",
    "note.delete",
    "note.edit",
    "note.editLines",
    "note.get",
    "note.getVersion",
    "note.lineAttribution.computeNow",
    "note.lineAttribution.load",
    "note.list",
    "note.listTasks",
    "note.listVersions",
    "note.presence.update",
    "note.readAsset",
    "note.restoreVersion",
    "note.saveAsset",
    "note.setContent",
    "note.update",
    "note.updateMetadata",
    "pairing.getInfo",
    "pr.refresh",
    "pr.status",
    "prMonitor.cancel",
    "prMonitor.flush",
    "prMonitor.list",
    "presence.snapshot",
    "presence.update",
    "primitive.addAgentAction",
    "primitive.addCli",
    "primitive.addPatch",
    "primitive.addReference",
    "principal.me",
    "principal.revokeSelf",
    "providers.catalog",
    "providers.setup.cancel",
    "providers.setup.login",
    "providers.setup.start",
    "providers.setup.status",
    "repo.list",
    "repo.remove",
    "repo.warmCache",
    "repoConfig.ensureDir",
    "repoConfig.get",
    "repoConfig.has",
    "repoConfig.save",
    "rules.get",
    "rules.list",
    "rules.update",
    "sandbox.cow.discard",
    "sandbox.cow.merge",
    "script.create",
    "script.list",
    "script.output",
    "script.remove",
    "script.restart",
    "script.run",
    "script.start",
    "script.status",
    "script.stop",
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
    "server.pairingInfo",
    "server.rotateToken",
    "settings.get",
    "settings.list",
    "settings.reset",
    "settings.update",
    "skill.list",
    "specialist.create",
    "specialist.delete",
    "specialist.edit",
    "specialist.get",
    "specialist.list",
    "stats.getRateHistory",
    "stats.getUsage",
    "system.capabilities",
    "system.gitCredential",
    "system.importLegacy",
    "system.requestUpdate",
    "system.shutdown",
    "system.status",
    "task.assignAgent",
    "task.convertBlocks",
    "task.createPrerequisite",
    "task.get",
    "task.getMyTask",
    "task.linkAgent",
    "task.list",
    "task.listAgentLinks",
    "task.markAsTask",
    "task.removeAgentFromAllTasks",
    "task.setRelations",
    "task.unlinkAgent",
    "task.update",
    "task.updateNoteStatus",
    "task.updateStatus",
    "terminal.create",
    "terminal.getBuffer",
    "terminal.kill",
    "terminal.list",
    "terminal.readOutput",
    "terminal.resize",
    "terminal.write",
    "unsloth.status",
    "unsloth.stop",
    "voice.getWorkspaceVocabulary",
    "voice.transcribe",
    "workspace.archive",
    "workspace.cancelDelete",
    "workspace.cleanup",
    "workspace.delete",
    "workspace.detectProjectType",
    "workspace.diskUsage",
    "workspace.dismissAttention",
    "workspace.duplicate",
    "workspace.export.abort",
    "workspace.export.finalize",
    "workspace.export.read",
    "workspace.export.start",
    "workspace.findRepositories",
    "workspace.generateSetupScript",
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
    "workspace.initializeRepository",
    "workspace.invite.create",
    "workspace.invite.list",
    "workspace.invite.revoke",
    "workspace.list",
    "workspace.localChanges",
    "workspace.markSeen",
    "workspace.members.leave",
    "workspace.members.list",
    "workspace.members.remove",
    "workspace.restore",
    "workspace.saveSetupScript",
    "workspace.setAutoCommit",
    "workspace.setBrowserClient",
    "workspace.transfer.plan",
    "workspace.unarchive",
    "workspace.update",
    "workspace.updateContext",
    "workspace.updateUiContext",
];

/// Partition check shared by the golden and its negative control: every
/// catalog method must be in exactly one ledger, and every ledger entry must
/// name a catalog method. Returns `(unclassified, doubly_classified, stale)`.
fn chat_write_classification(catalog: &[&str]) -> (Vec<String>, Vec<String>, Vec<String>) {
    let catalog: HashSet<&str> = catalog.iter().copied().collect();
    let user_write: HashSet<&str> = USER_ORIGIN_MESSAGE_ENTRY_POINTS
        .iter()
        .map(|(m, _)| *m)
        .collect();
    let non_write: HashSet<&str> = NON_USER_ORIGIN_METHODS.iter().copied().collect();
    let mut unclassified: Vec<String> = catalog
        .iter()
        .filter(|m| !user_write.contains(*m) && !non_write.contains(*m))
        .map(ToString::to_string)
        .collect();
    let mut doubly: Vec<String> = user_write
        .intersection(&non_write)
        .map(ToString::to_string)
        .collect();
    let mut stale: Vec<String> = user_write
        .union(&non_write)
        .filter(|m| !catalog.contains(*m))
        .map(ToString::to_string)
        .collect();
    unclassified.sort();
    doubly.sort();
    stale.sort();
    (unclassified, doubly, stale)
}

/// Router arms whose body reads a `messageMetadata` param, attributed to the
/// arm's method name(s). Same single-line-arm assumption as
/// [`extract_router_methods`].
fn router_arms_reading_message_metadata() -> HashSet<String> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let router_path = std::path::Path::new(manifest_dir).join("src/router.rs");
    let source =
        std::fs::read_to_string(&router_path).expect("Failed to read router.rs at test time");
    let mut current: Vec<String> = Vec::new();
    let mut methods = HashSet::new();
    for line in source.lines() {
        let trimmed = line.trim_start();
        // A non-indented line is a top-level item boundary (the dispatch
        // function ended; free helpers below mention the param in errors).
        if !line.is_empty() && !line.starts_with(' ') {
            current.clear();
        }
        if trimmed.starts_with('"') && line.contains("=>") {
            let before_arrow = &line[..line.find("=>").unwrap_or(line.len())];
            current = before_arrow
                .split('"')
                .map(str::trim)
                .filter(|part| {
                    part.contains('.')
                        && part
                            .chars()
                            .all(|c| c.is_alphanumeric() || c == '.' || c == '_')
                })
                .map(str::to_string)
                .collect();
        }
        if line.contains("\"messageMetadata\"") && !trimmed.starts_with("//") {
            methods.extend(current.iter().cloned());
        }
    }
    methods
}

#[test]
fn user_origin_message_entry_points_frozen() {
    let router_set: HashSet<&str> = ROUTER_METHODS.iter().copied().collect();
    let mut seen = HashSet::new();
    let mut previous: Option<&str> = None;
    for (method, note) in USER_ORIGIN_MESSAGE_ENTRY_POINTS {
        assert!(
            router_set.contains(method),
            "{method} is classified as a user-origin message entry point but is not in ROUTER_METHODS"
        );
        assert!(
            !note.trim().is_empty(),
            "{method}: the classification note must explain how the stamp reaches the row"
        );
        assert!(seen.insert(*method), "{method} is classified twice");
        if let Some(prev) = previous {
            assert!(
                prev < *method,
                "USER_ORIGIN_MESSAGE_ENTRY_POINTS must be sorted: {prev} before {method}"
            );
        }
        previous = Some(method);
    }
    assert_eq!(
        USER_ORIGIN_MESSAGE_ENTRY_POINTS.len(),
        11,
        "the Product Brief's ten user-origin entry points plus `workspace.create`'s \
         initialAgent kickoff; a change here needs the service matrix and \
         docs/protocol/ updated alongside"
    );

    // Exhaustive partition of the FULL catalog (router + fast path): a new
    // method — content-only or metadata-carrying — cannot land unclassified.
    let full_catalog: Vec<&str> = ROUTER_METHODS
        .iter()
        .chain(FASTPATH_METHODS.iter())
        .copied()
        .collect();
    let (unclassified, doubly, stale) = chat_write_classification(&full_catalog);
    assert!(
        unclassified.is_empty(),
        "catalog methods with no chat-write classification — add each to \
         USER_ORIGIN_MESSAGE_ENTRY_POINTS (and stamp the human principal) or to \
         NON_USER_ORIGIN_METHODS (affirming it writes no human chat row): {unclassified:?}"
    );
    assert!(
        doubly.is_empty(),
        "methods classified as both user-write and non-write: {doubly:?}"
    );
    assert!(
        stale.is_empty(),
        "classified methods that are no longer in the catalog: {stale:?}"
    );
    assert_eq!(
        USER_ORIGIN_MESSAGE_ENTRY_POINTS.len() + NON_USER_ORIGIN_METHODS.len(),
        full_catalog.len(),
        "the two ledgers must partition the catalog exactly"
    );
    let mut previous: Option<&str> = None;
    for method in NON_USER_ORIGIN_METHODS {
        if let Some(prev) = previous {
            assert!(
                prev < *method,
                "NON_USER_ORIGIN_METHODS must be sorted and duplicate-free: {prev} before {method}"
            );
        }
        previous = Some(method);
    }

    // Belt and braces: every router arm that reads `messageMetadata` must be
    // on the user-write side (or classified non-write on purpose).
    let reading = router_arms_reading_message_metadata();
    assert!(
        !reading.is_empty(),
        "no router arm reads \"messageMetadata\" — the source scan is broken"
    );
    let mut unclassified: Vec<_> = reading
        .iter()
        .filter(|m| !seen.contains(m.as_str()))
        .cloned()
        .collect();
    unclassified.sort();
    assert!(
        unclassified.is_empty(),
        "router arms read `messageMetadata` without a USER_ORIGIN_MESSAGE_ENTRY_POINTS \
         classification (stamp the human principal or classify why not): {unclassified:?}"
    );
}

/// Negative control for [`user_origin_message_entry_points_frozen`]: a
/// content-only method (no `messageMetadata` param, so invisible to the
/// source scan) added to the catalog is reported as unclassified until a
/// ledger names it.
#[test]
fn content_only_new_method_is_reported_unclassified() {
    let mut probed: Vec<&str> = ROUTER_METHODS
        .iter()
        .chain(FASTPATH_METHODS.iter())
        .copied()
        .collect();
    probed.push("agent.newHumanWrite");
    let (unclassified, doubly, stale) = chat_write_classification(&probed);
    assert_eq!(unclassified, vec!["agent.newHumanWrite".to_string()]);
    assert!(doubly.is_empty() && stale.is_empty());

    // And removing a classified method from the catalog surfaces as stale.
    let shrunk: Vec<&str> = probed
        .iter()
        .copied()
        .filter(|m| *m != "agent.newHumanWrite" && *m != "agent.appendMessage")
        .collect();
    let (_, _, stale) = chat_write_classification(&shrunk);
    assert_eq!(stale, vec!["agent.appendMessage".to_string()]);
}

// ---------------------------------------------------------------------------
// Collaborator allowlist goldens (multiplayer w3)
// ---------------------------------------------------------------------------

/// Subscription-channel fast paths (`subscriptions.rs`): the `*.subscribe` /
/// `*.unsubscribe` arms the connection task intercepts before the router.
/// They are not part of `ROUTER_METHODS` / `FASTPATH_METHODS` (that extractor
/// walks one namespace per file), so the allowlist universe adds them here,
/// extracted from source like the other two so a new channel cannot slip in
/// unclassified. `agent.subscribe` / `agent.unsubscribe` also have router
/// arms and are already cataloged there.
fn extract_subscription_channel_methods() -> HashSet<String> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = std::path::Path::new(manifest_dir).join("src/subscriptions.rs");
    let source =
        std::fs::read_to_string(&path).expect("Failed to read subscriptions.rs at test time");
    let mut methods = HashSet::new();
    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("//") {
            continue;
        }
        let mut rest = trimmed;
        while let Some(start) = rest.find('"') {
            let after = &rest[start + 1..];
            let Some(end) = after.find('"') else { break };
            let candidate = &after[..end];
            if (candidate.ends_with(".subscribe") || candidate.ends_with(".unsubscribe"))
                && candidate
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '.' || c == '_')
            {
                methods.insert(candidate.to_string());
            }
            rest = &after[end + 1..];
        }
    }
    assert!(
        methods.contains("chat.subscribe") && methods.contains("workspace.unsubscribe"),
        "subscription-channel extraction broke: {methods:?}"
    );
    methods
}

/// Every method a client can put on the wire: router arms, fast paths, and
/// the subscription channels.
fn client_callable_universe() -> BTreeSet<String> {
    ROUTER_METHODS
        .iter()
        .chain(FASTPATH_METHODS.iter())
        .map(std::string::ToString::to_string)
        .chain(extract_subscription_channel_methods())
        .collect()
}

/// Golden: every client-callable method a non-administrator may **not** call.
/// `client_callable_universe() \ COLLABORATOR_METHODS` must equal this list
/// exactly, so adding a method to the wire surface fails CI until it is
/// classified — either allowed with a vetting note in `COLLABORATOR_METHODS`
/// or named here. The failure message prints the recomputed golden.
///
/// Owner-only families: `host.*` but the two display probes, `browser.*`,
/// `forward.*`, `terminal.*`, `script.*`, `github.*`, `linear.*`, `sentry.*`,
/// `voice.*`, `settings.*`, `repo.*` / `repoConfig.*`, `mcp.*`, `server.*`,
/// `pairing.*`, `providers.setup.*`, `system.*` (but `system.capabilities`
/// and `system.status`),
/// `rules.*`, `sandbox.*`, `unsloth.*`, `debug.*`, workspace lifecycle /
/// export / import / setup / browser-client pinning, `git.clone`,
/// `git.agentCommit` (agent-only), agent deletion / proposals / one-shot
/// completions, `agent.replaceMessages` (persists client-supplied user rows
/// verbatim, so a non-owner could forge `fromPrincipalId`), hook run/cancel,
/// PR-monitor cancel/flush, daemon-wide metrics, and the `accept-changes.*` /
/// `file-tracking.*` publishing flow (stages, commits, pushes and merges as
/// the primary user; refused as a family pending a per-method decision).
const COLLABORATOR_REFUSED_METHODS: &[&str] = &[
    "accept-changes.addRemote",
    "accept-changes.execute",
    "accept-changes.getStatus",
    "accept-changes.mergePR",
    "accept-changes.prepare",
    "agent.cancelDelete",
    "agent.completeOnce",
    "agent.delete",
    "agent.diagnostics",
    "agent.enhancePrompt",
    "agent.replaceMessages",
    "agent.reportToParent",
    "agent.resolveProposal",
    "browser.closeTab",
    "browser.exec",
    "browser.listTabs",
    "browser.navigateTab",
    "browser.removeTab",
    "browser.syncTabs",
    "browser.upsertTab",
    "client.list",
    "debug.sampleStacks",
    "file-tracking.getAgentLocks",
    "file-tracking.getChanges",
    "file-tracking.getLineStats",
    "file-tracking.loadCommits",
    "file-tracking.stage",
    "file-tracking.unstage",
    "forward.close",
    "forward.create",
    "forward.list",
    "git.agentCommit",
    "git.clone",
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
    "github.pulls.create",
    "github.pulls.get",
    "github.pulls.list",
    "github.pulls.merge",
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
    "hook.cancel",
    "hook.runNow",
    "host.checkAuggie",
    "host.checkGh",
    "host.checkGit",
    "host.checkNode",
    "host.createDirectory",
    "host.directoryStatus",
    "host.env",
    "host.exec",
    "host.execStream",
    "host.execStream.cancel",
    "host.execStream.write",
    "host.findApp",
    "host.findBinary",
    "host.listDirectory",
    "host.listInstalledEditors",
    "host.openInEditor",
    "host.providerAuthStatus",
    "host.providerDiscovery",
    "host.providerTestPrompt",
    "invite.accept",
    "invite.challenge",
    "invite.inspect",
    "invite.prove",
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
    "metrics.clearAgentStats",
    "metrics.getAllWorkspaceStats",
    "pairing.getInfo",
    "prMonitor.cancel",
    "prMonitor.flush",
    "providers.setup.cancel",
    "providers.setup.login",
    "providers.setup.start",
    "providers.setup.status",
    "repo.list",
    "repo.remove",
    "repo.warmCache",
    "repoConfig.ensureDir",
    "repoConfig.get",
    "repoConfig.has",
    "repoConfig.save",
    "rules.get",
    "rules.list",
    "rules.update",
    "sandbox.cow.discard",
    "sandbox.cow.merge",
    "script.create",
    "script.list",
    "script.output",
    "script.remove",
    "script.restart",
    "script.run",
    "script.start",
    "script.status",
    "script.stop",
    "sentry.assignIssue",
    "sentry.authStatus",
    "sentry.getIssue",
    "sentry.ignoreIssue",
    "sentry.listIssues",
    "sentry.listProjects",
    "sentry.resolveIssue",
    "sentry.searchIssues",
    "server.pairingInfo",
    "server.rotateToken",
    "settings.get",
    "settings.list",
    "settings.reset",
    "settings.update",
    "specialist.create",
    "specialist.delete",
    "specialist.edit",
    "system.gitCredential",
    "system.importLegacy",
    "system.requestUpdate",
    "system.shutdown",
    "terminal.create",
    "terminal.getBuffer",
    "terminal.kill",
    "terminal.list",
    "terminal.readOutput",
    "terminal.resize",
    "terminal.write",
    "unsloth.status",
    "unsloth.stop",
    "voice.getWorkspaceVocabulary",
    "voice.transcribe",
    "workspace.archive",
    "workspace.cancelDelete",
    "workspace.cleanup",
    "workspace.create",
    "workspace.delete",
    "workspace.detectProjectType",
    "workspace.diskUsage",
    "workspace.duplicate",
    "workspace.export.abort",
    "workspace.export.finalize",
    "workspace.export.read",
    "workspace.export.start",
    "workspace.findRepositories",
    "workspace.generateSetupScript",
    "workspace.getBrowserClient",
    "workspace.getSetupScript",
    "workspace.import.abort",
    "workspace.import.begin",
    "workspace.import.chunk",
    "workspace.import.commit",
    "workspace.initializeRepository",
    "workspace.invite.create",
    "workspace.invite.list",
    "workspace.invite.revoke",
    "workspace.members.remove",
    "workspace.restore",
    "workspace.saveSetupScript",
    "workspace.setAutoCommit",
    "workspace.setBrowserClient",
    "workspace.transfer.plan",
    "workspace.unarchive",
];

#[test]
fn collaborator_methods_are_sorted_unique_and_vetted() {
    let mut sorted: Vec<&str> = COLLABORATOR_METHODS.iter().map(|(m, _)| *m).collect();
    let listed = sorted.clone();
    sorted.sort_unstable();
    assert_eq!(
        listed, sorted,
        "COLLABORATOR_METHODS must be sorted alphabetically by method"
    );
    let mut seen = HashSet::new();
    for (method, note) in COLLABORATOR_METHODS {
        assert!(
            seen.insert(method),
            "Duplicate collaborator method: {method}"
        );
        assert!(
            !note.trim().is_empty(),
            "Collaborator method {method} has no vetting note — every allowed entry must say what it reads or mutates, why a guest needs it, and why it cannot reach the host or another workspace"
        );
    }
}

#[test]
fn collaborator_methods_are_canonical_and_cataloged() {
    let universe = client_callable_universe();
    let aliases: HashSet<&str> = METHOD_ALIASES.iter().map(|(alias, _)| *alias).collect();
    for (method, _) in COLLABORATOR_METHODS {
        assert!(
            !aliases.contains(method),
            "COLLABORATOR_METHODS lists alias {method}; list the canonical method ({}) instead — aliases are canonicalised before the lookup",
            canonical_method(method)
        );
        assert!(
            universe.contains(*method),
            "COLLABORATOR_METHODS lists {method}, which is not in ROUTER_METHODS, FASTPATH_METHODS, or the subscription channels"
        );
    }
}

#[test]
fn collaborator_refused_remainder_is_classified() {
    let allowed: HashSet<&str> = COLLABORATOR_METHODS.iter().map(|(m, _)| *m).collect();
    let refused: Vec<String> = client_callable_universe()
        .into_iter()
        .filter(|m| !allowed.contains(m.as_str()))
        .collect();
    let golden: Vec<String> = COLLABORATOR_REFUSED_METHODS
        .iter()
        .map(std::string::ToString::to_string)
        .collect();
    if refused != golden {
        let mut msg = String::from(
            "Collaborator allowlist drift: a client-callable method is neither in COLLABORATOR_METHODS nor in COLLABORATOR_REFUSED_METHODS (or a refused entry is stale).\n\
             Classify it: allow it in catalog.rs COLLABORATOR_METHODS with a vetting note, or name it in the refused golden below and mirror the change in docs/protocol/.\n",
        );
        let refused_set: BTreeSet<&str> = refused.iter().map(String::as_str).collect();
        let golden_set: BTreeSet<&str> = golden.iter().map(String::as_str).collect();
        let unclassified: Vec<_> = refused_set.difference(&golden_set).collect();
        let stale: Vec<_> = golden_set.difference(&refused_set).collect();
        if !unclassified.is_empty() {
            let _ = writeln!(
                msg,
                "\nUnclassified (refused by default, not in the golden):"
            );
            for m in unclassified {
                let _ = writeln!(msg, "  - {m}");
            }
        }
        if !stale.is_empty() {
            let _ = writeln!(
                msg,
                "\nStale golden entries (no longer client-callable or now allowed):"
            );
            for m in stale {
                let _ = writeln!(msg, "  - {m}");
            }
        }
        let _ = writeln!(msg, "\nRecomputed COLLABORATOR_REFUSED_METHODS golden:");
        for m in &refused {
            let _ = writeln!(msg, "    \"{m}\",");
        }
        panic!("{msg}");
    }
}

#[test]
fn collaborator_refused_golden_is_sorted_unique_and_disjoint() {
    let mut sorted = COLLABORATOR_REFUSED_METHODS.to_vec();
    sorted.sort_unstable();
    assert_eq!(
        COLLABORATOR_REFUSED_METHODS,
        &sorted[..],
        "COLLABORATOR_REFUSED_METHODS must be sorted alphabetically"
    );
    let allowed: HashSet<&str> = COLLABORATOR_METHODS.iter().map(|(m, _)| *m).collect();
    let mut seen = HashSet::new();
    for method in COLLABORATOR_REFUSED_METHODS {
        assert!(seen.insert(method), "Duplicate refused method: {method}");
        assert!(
            !allowed.contains(method),
            "{method} is both allowed and refused"
        );
    }
}

#[test]
fn collaborator_lookup_canonicalises_aliases_and_denies_by_default() {
    for (alias, canonical) in METHOD_ALIASES {
        assert_eq!(canonical_method(alias), *canonical);
        assert_eq!(
            collaborator_may_call(alias),
            collaborator_may_call(canonical),
            "alias {alias} must classify exactly like {canonical}"
        );
    }
    assert_eq!(canonical_method("note.get"), "note.get");
    assert!(
        collaborator_may_call("git.diff"),
        "git.diff is treated as git.diffs"
    );
    assert!(
        collaborator_may_call("git.log"),
        "git.log is treated as git.commits"
    );
    for allowed in [
        "client.hello",
        "events.subscribe",
        "chat.subscribe",
        "workspace.subscribe",
        "system.capabilities",
        "system.status",
        "host.status",
        "principal.me",
        "pr.status",
        "pr.refresh",
        "prMonitor.list",
        "presence.snapshot",
        "presence.update",
        "note.presence.subscribe",
        "agent.create",
        "agent.stop",
        "agent.sendMessage",
        "agent.setModel",
        "hook.list",
        "git.commit",
        "git.push",
    ] {
        assert!(collaborator_may_call(allowed), "{allowed} must be allowed");
    }
    for refused in [
        "host.exec",
        "host.openInEditor",
        "browser.exec",
        "browser.listTabs",
        "forward.create",
        "github.authStatus",
        "github.pulls.create",
        "github.pulls.get",
        "mcp.servers.list",
        "prMonitor.cancel",
        "settings.get",
        "system.shutdown",
        "system.requestUpdate",
        "repo.list",
        "voice.transcribe",
        "workspace.create",
        "git.clone",
        "agent.delete",
        "agent.replaceMessages",
        "terminal.list",
        "script.list",
        "debug.sampleStacks",
    ] {
        assert!(!collaborator_may_call(refused), "{refused} must be refused");
    }
    // Default-deny: anything not cataloged is refused too.
    assert!(!collaborator_may_call("totally.unknown"));
    assert!(!collaborator_may_call(""));
}

#[test]
fn reverse_methods_are_never_on_the_collaborator_allowlist() {
    for method in REVERSE_METHODS {
        assert!(
            !collaborator_may_call(method),
            "reverse RPC {method} must not be callable by a non-administrator"
        );
    }
}

// ---------------------------------------------------------------------------
// Owner-only method surface, unbound (multiplayer w4b: fail-closed caller)
// ---------------------------------------------------------------------------

/// Every owner-only **router** method — `ROUTER_METHODS \ COLLABORATOR_METHODS`,
/// the service-layer half of [`COLLABORATOR_REFUSED_METHODS`] — dispatched
/// through the real router into a real `Services` with **no caller bound**
/// must answer `-32003 Forbidden` from the capability gate — never a
/// successful early return ahead of a gate (the `agent.respondPermission`
/// no-manager path was one, fixed and pinned by
/// `unbound_respond_permission_is_forbidden_without_a_manager`) — unless the
/// method is pinned by name in one of two goldens: its gate is conditional
/// on something the minimal call omits — an optional scope argument or a
/// live export session ([`CONDITIONALLY_GATED_AT_SERVICE_LAYER`], whose
/// armed modes `armed_conditional_gates_are_forbidden_unbound` asserts
/// separately) — or it has no service-layer gate at all
/// ([`UNGATED_AT_SERVICE_LAYER`]).
///
/// The table below is params-only: it supplies the minimal *valid* arguments
/// so each arm gets past `-32602` parsing and reaches the service method
/// (the fixture workspace exists, so a lookup ordered before the gate still
/// ends at the gate). It is derived from the catalog, so a new owner-only
/// router method fails here until it is given a row — and then fails again
/// until its service method is gated or it is named in
/// [`UNGATED_AT_SERVICE_LAYER`] on purpose.
///
/// The remaining refused methods — the connection-task fast paths (`host.*`,
/// `browser.*`, `forward.*`, `system.*`, `pairing.*`, `server.*`,
/// `providers.setup.*`, `invite.inspect` / `invite.accept` /
/// `invite.challenge` / `invite.prove`)
/// and the subscription channels — have
/// no `WorkspaceApi` method to gate; they are protected only by the
/// transport allowlist in `conn::process_frame` (`-32003`) and stay out of
/// this table by construction. That partition is asserted, not assumed.
mod unbound_owner_only_methods {
    use super::{COLLABORATOR_METHODS, ROUTER_METHODS};
    use crate::router::handle_message;
    use intent_core::{chief_workspace, AgentId, Error, WorkspaceApi as _, WorkspaceId};
    use intent_services::Services;
    use intent_store::Store;
    use serde_json::{json, Value};
    use std::collections::BTreeMap;
    use std::fmt::Write as _;

    /// How a [`CONDITIONALLY_GATED_AT_SERVICE_LAYER`] row's gate is armed.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Arming {
        /// `require_member` runs only when the optional `workspaceId` is
        /// given.
        WorkspaceId,
        /// `require_agent_member` runs only when the optional `agentId` is
        /// given.
        AgentId,
        /// The gated calls (`update_workspace` for `finalStatusMessage`,
        /// `archive_workspace` for `archiveSource`) run only once the
        /// `exportId` resolves to a Ready session **and** the option is
        /// given; an unknown id returns at the lookup, and a Ready session
        /// finalized with neither option runs no gated call at all — it
        /// only retires the session. That live unarmed mode is pinned `ok`
        /// by the armed test, not left implicit.
        ReadyExport,
    }

    /// Golden: owner-only router methods whose service-layer gate is
    /// **conditional** — on an optional scope argument, or on the export
    /// session the id names — so the sweep's minimal call never reaches it.
    /// Each row is `(method, unarmed outcome, what arms the gate)`. The
    /// unarmed outcome is pinned here so the sweep sees it as classified,
    /// not as ungated; the unarmed mode is owner-only by the transport
    /// allowlist in `conn::process_frame` alone. Every armed mode is
    /// asserted by [`armed_conditional_gates_are_forbidden_unbound`], which
    /// arms the gate and requires `-32003`. A row leaves this list when its
    /// gate stops being conditional (then it is simply `-32003` in the
    /// sweep); making a gate unconditional is a capability-rule change and
    /// not something this table decides.
    const CONDITIONALLY_GATED_AT_SERVICE_LAYER: &[(&str, &str, Arming)] = &[
        ("agent.completeOnce", "ok", Arming::WorkspaceId),
        ("agent.diagnostics", "ok", Arming::AgentId),
        ("agent.enhancePrompt", "ok", Arming::WorkspaceId),
        // Unscoped, the call proceeds to the worktree lookup (Internal here).
        ("git.agentCommit", "-32603", Arming::AgentId),
        ("rules.list", "ok", Arming::WorkspaceId),
        // Unknown `exportId`: NotFound at the registry lookup (`-32602`,
        // `not-found`), ahead of both gated mutations. The other unarmed
        // mode — a Ready session with neither option, which retires the
        // session ungated — is asserted `ok` by the armed test.
        ("workspace.export.finalize", "-32602", Arming::ReadyExport),
    ];

    /// Golden: owner-only router methods whose service method has **no
    /// service-layer gate at all**, with the outcome an unbound caller
    /// observes today (`ok` or the error code). Each is owner-only by the
    /// transport allowlist in `conn::process_frame` alone, and its unbound
    /// behaviour is whatever the body does. Shrinking this list is the goal;
    /// growing it needs a reason on the row. The failure message prints the
    /// recomputed list.
    const UNGATED_AT_SERVICE_LAYER: &[(&str, &str)] = &[
        // No gate: daemon-wide reverse-client listing.
        ("client.list", "ok"),
        // No gate: process-wide stack sampler.
        ("debug.sampleStacks", "ok"),
        // No gate: daemon-wide metrics read.
        ("metrics.getAllWorkspaceStats", "ok"),
        // No gate: known-repo registry read.
        ("repo.list", "ok"),
        // No gate: `_workspace_id` is unused; reads the global rule row.
        ("rules.get", "ok"),
        // No gate: no-manager early return `{ running: false }`.
        ("unsloth.status", "ok"),
        // No gate: no-manager early return `{ stopped: false }`.
        ("unsloth.stop", "ok"),
        // No gate: proceeds to provider selection (Internal without an engine).
        ("voice.transcribe", "-32603"),
        // No gate: export sessions are keyed by the `exportId` handed out by
        // the gated `workspace.export.start`, and neither method mutates the
        // workspace; an unknown id is a no-op / -32602. (`finalize` does
        // mutate and is classified above.)
        ("workspace.export.abort", "ok"),
        ("workspace.export.read", "-32602"),
        // No gate: host filesystem scan under `directory`.
        ("workspace.findRepositories", "ok"),
        // No gate: `git init` at `path` on the host.
        ("workspace.initializeRepository", "ok"),
    ];

    struct Fixture {
        services: Services,
        ws: WorkspaceId,
        /// Swept on drop; every path the table hands out lives under it.
        dir: tempfile::TempDir,
    }

    async fn fixture() -> Fixture {
        let dir = tempfile::Builder::new()
            .prefix("intentd-unbound-table-")
            .tempdir()
            .expect("tempdir");
        let store = Store::open(&dir.path().join("svc.db"))
            .await
            .expect("open store");
        let ws = WorkspaceId::new();
        store
            .insert_workspace(&intent_core::Workspace {
                id: ws.clone(),
                title: "WS".to_string(),
                branch: "main".to_string(),
                ..chief_workspace()
            })
            .await
            .expect("insert workspace");
        let services = Services::new(store)
            .with_workspaces_root(dir.path().join("workspaces"))
            .with_assets_root(dir.path().join("assets"));
        Fixture { services, ws, dir }
    }

    /// Minimal valid params per owner-only router method. Values are inert
    /// when a gate is missing: unknown ids, closed ports, empty edits, and
    /// every path stays inside the fixture tempdir.
    fn minimal_params(f: &Fixture) -> Vec<(&'static str, Value)> {
        let ws = f.ws.as_str();
        let dir = f.dir.path().to_string_lossy().into_owned();
        let gh = json!({ "owner": "o", "repo": "r" });
        let gh_n = json!({ "owner": "o", "repo": "r", "number": 1 });
        let sid = json!({ "serverId": "srv" });
        let script = json!({ "workspaceId": ws, "scriptId": "s1" });
        let term = json!({ "terminalId": "t1" });
        vec![
            (
                "accept-changes.addRemote",
                json!({ "workspaceId": ws, "remoteUrl": "u" }),
            ),
            (
                "accept-changes.execute",
                json!({ "workspaceId": ws, "action": "a" }),
            ),
            ("accept-changes.getStatus", json!({ "workspaceId": ws })),
            (
                "accept-changes.mergePR",
                json!({ "workspaceId": ws, "prNumber": 1 }),
            ),
            (
                "accept-changes.prepare",
                json!({ "workspaceId": ws, "action": "a" }),
            ),
            ("agent.cancelDelete", json!({ "agentId": "a1" })),
            (
                "agent.completeOnce",
                json!({ "prompt": "p", "timeoutMs": 1 }),
            ),
            ("agent.delete", json!({ "agentId": "a1" })),
            ("agent.diagnostics", json!({ "workspaceId": ws })),
            (
                "agent.enhancePrompt",
                json!({ "prompt": "p", "timeoutMs": 1 }),
            ),
            (
                "agent.replaceMessages",
                json!({ "agentId": "a1", "messages": [] }),
            ),
            (
                "agent.reportToParent",
                json!({ "workspaceId": ws, "report": "r" }),
            ),
            (
                "agent.resolveProposal",
                json!({ "workspaceId": ws, "agentId": "a1", "proposalId": "p1", "outcome": "reject" }),
            ),
            ("client.list", json!({})),
            ("debug.sampleStacks", json!({ "durationMs": 1 })),
            ("file-tracking.getAgentLocks", json!({ "workspaceId": ws })),
            ("file-tracking.getChanges", json!({ "workspaceId": ws })),
            ("file-tracking.getLineStats", json!({ "workspaceId": ws })),
            ("file-tracking.loadCommits", json!({ "workspaceId": ws })),
            (
                "file-tracking.stage",
                json!({ "workspaceId": ws, "paths": [] }),
            ),
            (
                "file-tracking.unstage",
                json!({ "workspaceId": ws, "paths": [] }),
            ),
            (
                "git.agentCommit",
                json!({ "workspaceId": ws, "message": "m" }),
            ),
            (
                "git.clone",
                json!({ "url": "file:///nonexistent", "parentDir": dir }),
            ),
            ("github.authStatus", json!({})),
            ("github.branches.list", gh.clone()),
            ("github.branches.listCached", gh.clone()),
            ("github.cancelAuth", json!({})),
            ("github.connect", json!({})),
            ("github.getReviewThreads", gh_n.clone()),
            ("github.getUser", json!({})),
            (
                "github.identityProof.create",
                json!({ "nonce": "n", "hostLabel": "h" }),
            ),
            ("github.identityProof.delete", json!({ "gistId": "g" })),
            ("github.issues.get", gh_n.clone()),
            ("github.issues.list", gh.clone()),
            ("github.issues.search", gh.clone()),
            ("github.listReviewComments", gh_n.clone()),
            (
                "github.pulls.create",
                json!({ "owner": "o", "repo": "r", "title": "t", "body": "b", "head": "h", "base": "b" }),
            ),
            ("github.pulls.get", gh_n.clone()),
            ("github.pulls.list", gh.clone()),
            ("github.pulls.merge", gh_n.clone()),
            ("github.pulls.search", gh.clone()),
            ("github.pulls.updateBranch", gh_n.clone()),
            ("github.relatedRepos.list", gh.clone()),
            (
                "github.replyReviewComment",
                json!({ "owner": "o", "repo": "r", "number": 1, "commentId": 1, "body": "b" }),
            ),
            ("github.repoConfig.get", gh.clone()),
            ("github.repos.get", gh.clone()),
            ("github.repos.list", json!({})),
            ("github.repos.search", json!({ "query": "q" })),
            ("github.resolveThread", json!({ "threadId": "t" })),
            ("github.revoke", json!({})),
            ("github.unresolveThread", json!({ "threadId": "t" })),
            ("github.users.search", json!({ "query": "q" })),
            ("hook.cancel", json!({ "workspaceId": ws, "hookId": "h1" })),
            ("hook.runNow", json!({ "workspaceId": ws, "hookId": "h1" })),
            ("linear.authStatus", json!({})),
            (
                "linear.createIssue",
                json!({ "title": "t", "teamId": "team" }),
            ),
            ("linear.getIssue", json!({ "id": "i" })),
            ("linear.listIssues", json!({})),
            ("linear.listLabels", json!({})),
            ("linear.listProjects", json!({})),
            ("linear.listTeams", json!({})),
            ("linear.listWorkflowStates", json!({})),
            ("linear.searchIssues", json!({ "query": "q" })),
            ("linear.updateIssue", json!({ "issueId": "i" })),
            ("linear.viewer", json!({})),
            ("mcp.oauth.delete", sid.clone()),
            ("mcp.oauth.get", sid.clone()),
            ("mcp.oauth.list", json!({})),
            (
                "mcp.oauth.set",
                json!({ "serverId": "srv", "tokenBag": {} }),
            ),
            ("mcp.servers.create", json!({ "config": {} })),
            ("mcp.servers.delete", sid.clone()),
            ("mcp.servers.getStatus", sid.clone()),
            ("mcp.servers.list", json!({})),
            ("mcp.servers.restart", sid.clone()),
            (
                "mcp.servers.toggle",
                json!({ "serverId": "srv", "enabled": false }),
            ),
            (
                "mcp.servers.update",
                json!({ "serverId": "srv", "config": {} }),
            ),
            (
                "mcp.testConnection",
                json!({ "url": "http://127.0.0.1:9/" }),
            ),
            ("metrics.clearAgentStats", json!({ "agentId": "a1" })),
            ("metrics.getAllWorkspaceStats", json!({})),
            (
                "prMonitor.cancel",
                json!({ "workspaceId": ws, "monitorId": "m1" }),
            ),
            (
                "prMonitor.flush",
                json!({ "workspaceId": ws, "monitorId": "m1" }),
            ),
            ("repo.list", json!({})),
            ("repo.remove", json!({ "path": dir })),
            (
                "repo.warmCache",
                json!({ "githubUrl": "https://github.com/o/r" }),
            ),
            ("repoConfig.ensureDir", json!({ "workspaceId": ws })),
            ("repoConfig.get", json!({ "workspaceId": ws })),
            ("repoConfig.has", json!({ "workspaceId": ws })),
            (
                "repoConfig.save",
                json!({ "workspaceId": ws, "config": {} }),
            ),
            (
                "rules.get",
                json!({ "workspaceId": ws, "ruleType": "agents" }),
            ),
            ("rules.list", json!({})),
            (
                "rules.update",
                json!({ "workspaceId": ws, "ruleType": "agents", "content": "" }),
            ),
            (
                "sandbox.cow.discard",
                json!({ "workspaceId": ws, "agentId": "a1" }),
            ),
            (
                "sandbox.cow.merge",
                json!({ "workspaceId": ws, "agentId": "a1" }),
            ),
            (
                "script.create",
                json!({ "workspaceId": ws, "name": "n", "command": "true", "mode": "command" }),
            ),
            ("script.list", json!({ "workspaceId": ws })),
            ("script.output", script.clone()),
            ("script.remove", script.clone()),
            ("script.restart", script.clone()),
            ("script.run", script.clone()),
            ("script.start", script.clone()),
            ("script.status", script.clone()),
            ("script.stop", script.clone()),
            ("sentry.assignIssue", json!({ "id": "i" })),
            ("sentry.authStatus", json!({})),
            ("sentry.getIssue", json!({ "id": "i" })),
            ("sentry.ignoreIssue", json!({ "id": "i" })),
            ("sentry.listIssues", json!({})),
            ("sentry.listProjects", json!({})),
            ("sentry.resolveIssue", json!({ "id": "i" })),
            ("sentry.searchIssues", json!({ "query": "q" })),
            ("settings.get", json!({ "path": "model.defaultProvider" })),
            ("settings.list", json!({})),
            ("settings.reset", json!({ "path": "model.defaultProvider" })),
            ("settings.update", json!({ "changes": {} })),
            ("specialist.create", json!({ "id": "s", "spec": {} })),
            ("specialist.delete", json!({ "id": "s", "scope": "global" })),
            (
                "specialist.edit",
                json!({ "id": "s", "spec": {}, "scope": "global" }),
            ),
            ("terminal.create", json!({ "workspaceId": ws })),
            ("terminal.getBuffer", term.clone()),
            ("terminal.kill", term.clone()),
            ("terminal.list", json!({ "workspaceId": ws })),
            (
                "terminal.readOutput",
                json!({ "workspaceId": ws, "terminalId": "t1" }),
            ),
            ("terminal.resize", term.clone()),
            ("terminal.write", json!({ "terminalId": "t1", "data": "" })),
            ("unsloth.status", json!({})),
            ("unsloth.stop", json!({})),
            ("voice.getWorkspaceVocabulary", json!({ "workspaceId": ws })),
            ("voice.transcribe", json!({ "audio": "AAAA" })),
            ("workspace.archive", json!({ "workspaceId": ws })),
            ("workspace.cancelDelete", json!({ "workspaceId": ws })),
            ("workspace.cleanup", json!({ "workspaceId": ws })),
            (
                "workspace.create",
                json!({ "title": "t", "repositoryPath": format!("{dir}/missing-repo") }),
            ),
            ("workspace.delete", json!({ "workspaceId": ws })),
            ("workspace.detectProjectType", json!({ "workspaceId": ws })),
            ("workspace.diskUsage", json!({ "workspaceId": ws })),
            ("workspace.duplicate", json!({ "workspaceId": ws })),
            ("workspace.export.abort", json!({ "exportId": "e1" })),
            ("workspace.export.finalize", json!({ "exportId": "e1" })),
            (
                "workspace.export.read",
                json!({ "exportId": "e1", "seq": 0 }),
            ),
            ("workspace.export.start", json!({ "workspaceId": ws })),
            ("workspace.findRepositories", json!({ "directory": dir })),
            (
                "workspace.generateSetupScript",
                json!({ "workspaceId": ws }),
            ),
            ("workspace.getBrowserClient", json!({ "workspaceId": ws })),
            ("workspace.getSetupScript", json!({ "workspaceId": ws })),
            ("workspace.import.abort", json!({ "importId": "i1" })),
            (
                "workspace.import.begin",
                json!({ "manifest": {}, "archiveSizeBytes": 1, "archiveSha256": "00" }),
            ),
            (
                "workspace.import.chunk",
                json!({ "importId": "i1", "seq": 0, "data": "" }),
            ),
            ("workspace.import.commit", json!({ "importId": "i1" })),
            (
                "workspace.initializeRepository",
                json!({ "path": format!("{dir}/missing-repo") }),
            ),
            ("workspace.invite.list", json!({ "workspaceId": ws })),
            (
                "workspace.invite.revoke",
                json!({ "workspaceId": ws, "inviteId": "inv" }),
            ),
            (
                "workspace.members.remove",
                json!({ "workspaceId": ws, "principalId": "p" }),
            ),
            ("workspace.restore", json!({ "workspaceId": ws })),
            (
                "workspace.saveSetupScript",
                json!({ "workspaceId": ws, "script": "" }),
            ),
            (
                "workspace.setAutoCommit",
                json!({ "workspaceId": ws, "enabled": false }),
            ),
            (
                "workspace.setBrowserClient",
                json!({ "workspaceId": ws, "clientId": null }),
            ),
            ("workspace.transfer.plan", json!({ "workspaceId": ws })),
            ("workspace.unarchive", json!({ "workspaceId": ws })),
        ]
    }

    /// The derived universe: owner-only router methods, sorted.
    fn owner_only_router_methods() -> Vec<&'static str> {
        let allowed: std::collections::HashSet<&str> =
            COLLABORATOR_METHODS.iter().map(|(m, _)| *m).collect();
        ROUTER_METHODS
            .iter()
            .copied()
            .filter(|m| !allowed.contains(m))
            .collect()
    }

    /// One unbound dispatch through the real router; the outcome label is
    /// `ok` or the JSON-RPC error code.
    async fn dispatch_unbound(services: &Services, method: &str, params: &Value) -> String {
        assert_eq!(
            intent_core::current_caller(),
            None,
            "{method}: caller leaked"
        );
        let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let raw = handle_message(services, &frame.to_string())
            .await
            .expect("request gets a response");
        let response: Value = serde_json::from_str(&raw).expect("response is JSON");
        match response.get("error").and_then(|e| e["code"].as_i64()) {
            Some(code) => code.to_string(),
            None => "ok".to_string(),
        }
    }

    #[tokio::test]
    async fn every_owner_only_router_method_is_forbidden_unbound() {
        let f = fixture().await;
        let table: BTreeMap<&str, Value> = minimal_params(&f).into_iter().collect();
        let universe = owner_only_router_methods();

        // The table and the derived universe must match exactly, so a new
        // owner-only router method cannot escape the sweep.
        let missing: Vec<&str> = universe
            .iter()
            .copied()
            .filter(|m| !table.contains_key(m))
            .collect();
        let stale: Vec<&str> = table
            .keys()
            .copied()
            .filter(|m| !universe.contains(m))
            .collect();
        assert!(
            missing.is_empty() && stale.is_empty(),
            "owner-only router table drift — add a minimal-params row for each new \
             method (missing: {missing:?}); drop rows for methods no longer owner-only \
             or no longer routed (stale: {stale:?})"
        );

        let mut ungated: Vec<(String, String)> = Vec::new();
        for method in &universe {
            let outcome = dispatch_unbound(&f.services, method, &table[method]).await;
            if outcome != "-32003" {
                ungated.push(((*method).to_string(), outcome));
            }
        }
        // The two goldens are disjoint and, merged by method, are exactly
        // what the sweep may observe: a conditionally gated row's *unarmed*
        // outcome and a genuinely ungated method's outcome.
        let mut golden: BTreeMap<String, String> = BTreeMap::new();
        for (m, o, _) in CONDITIONALLY_GATED_AT_SERVICE_LAYER {
            assert!(
                golden.insert((*m).to_string(), (*o).to_string()).is_none(),
                "{m}: duplicated in CONDITIONALLY_GATED_AT_SERVICE_LAYER"
            );
        }
        for (m, o) in UNGATED_AT_SERVICE_LAYER {
            assert!(
                golden.insert((*m).to_string(), (*o).to_string()).is_none(),
                "{m}: named in both CONDITIONALLY_GATED_AT_SERVICE_LAYER and \
                 UNGATED_AT_SERVICE_LAYER"
            );
        }
        let golden: Vec<(String, String)> = golden.into_iter().collect();
        if ungated != golden {
            let mut msg = String::from(
                "Unbound owner-only router methods not answering -32003 Forbidden drifted \
                 from CONDITIONALLY_GATED_AT_SERVICE_LAYER ∪ UNGATED_AT_SERVICE_LAYER.\n\
                 A `-32602` here usually means the table row is not valid enough to reach \
                 the service method; an `ok` or other code means the service method \
                 returns before its capability gate — move the gate first (see \
                 `agent_respond_permission`), or name it in \
                 CONDITIONALLY_GATED_AT_SERVICE_LAYER (gate conditional on an optional \
                 argument or a live session; add its armed cell) or in \
                 UNGATED_AT_SERVICE_LAYER (no gate) with a reason.\n\
                 Recomputed (merged) golden:\n",
            );
            for (m, o) in &ungated {
                let _ = writeln!(msg, "    (\"{m}\", \"{o}\"),");
            }
            panic!("{msg}");
        }
    }

    /// One dispatch through the real router with the daemon caller bound;
    /// the decoded response envelope.
    async fn dispatch_as_daemon(services: &Services, method: &str, params: &Value) -> Value {
        intent_core::with_caller(intent_core::Caller::Daemon, async {
            let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
            let raw = handle_message(services, &frame.to_string())
                .await
                .expect("request gets a response");
            serde_json::from_str::<Value>(&raw).expect("response is JSON")
        })
        .await
    }

    /// Drive the gated `workspace.export.start` as the daemon on the fixture
    /// workspace (repo-less, so the build is a manifest and no bundle) and
    /// wait for the session to seal: `workspace.export.read` answers
    /// `-32602` while building and `ok` once Ready. Returns the `exportId`.
    async fn ready_export(f: &Fixture) -> String {
        let start = dispatch_as_daemon(
            &f.services,
            "workspace.export.start",
            &json!({ "workspaceId": f.ws.as_str() }),
        )
        .await;
        let export_id = start["result"]["exportId"]
            .as_str()
            .unwrap_or_else(|| panic!("daemon-bound workspace.export.start: {start}"))
            .to_string();
        let read = json!({ "exportId": export_id, "seq": 0 });
        for _ in 0..400 {
            let response = dispatch_as_daemon(&f.services, "workspace.export.read", &read).await;
            match response.get("error") {
                None => return export_id,
                Some(e) if e["data"]["code"] == json!("not-found") => {
                    panic!("export {export_id} failed to build: {response}")
                }
                Some(_) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
            }
        }
        panic!("export {export_id} did not become Ready");
    }

    /// Every armed mode of every [`CONDITIONALLY_GATED_AT_SERVICE_LAYER`]
    /// row, through the real router into the same unbound `Services`, must
    /// be `-32003`:
    ///
    /// - Scope arguments: the sweep's minimal params plus `workspaceId` /
    ///   `agentId`. The gate runs before any lookup, so an unknown `agentId`
    ///   is still `Forbidden` unbound, never `NotFound`.
    /// - `workspace.export.finalize`: a Ready session (built by the daemon)
    ///   with `finalStatusMessage` reaches the gated `update_workspace`, and
    ///   with `archiveSource: true` the gated `archive_workspace`. Both
    ///   mutations run before the session is retired, so the one session
    ///   serves both cells and stays Ready after each refusal. The same
    ///   session then pins the row's live unarmed mode: finalize with
    ///   neither option runs no gated call, so unbound it is `ok` and
    ///   retires the session (the next read is `-32602`). Gating that
    ///   mode is a capability-rule change, out of this table's remit.
    ///
    /// `git.agentCommit` is the one row whose router arm cannot arm the gate:
    /// it passes `agent_id = None` unconditionally (the wire shape has no
    /// `agentId`; the MCP bridge is the only caller that supplies one). Both
    /// halves are asserted — the wire call with an `agentId` keeps the
    /// unscoped outcome, and the service method called directly with one is
    /// `Forbidden` — so a router arm that starts forwarding `agentId` fails
    /// here and moves the cell onto the router path.
    #[tokio::test]
    async fn armed_conditional_gates_are_forbidden_unbound() {
        let f = fixture().await;
        let table: BTreeMap<&str, Value> = minimal_params(&f).into_iter().collect();
        for (method, unarmed, arming) in CONDITIONALLY_GATED_AT_SERVICE_LAYER {
            let mut params = table[method].clone();
            let (scope, scope_value) = match arming {
                Arming::WorkspaceId => ("workspaceId", f.ws.as_str().to_string()),
                Arming::AgentId => ("agentId", "a1".to_string()),
                Arming::ReadyExport => {
                    let export_id = ready_export(&f).await;
                    for armed in [
                        json!({ "exportId": export_id, "finalStatusMessage": "done" }),
                        json!({ "exportId": export_id, "archiveSource": true }),
                    ] {
                        let outcome = dispatch_unbound(&f.services, method, &armed).await;
                        assert_eq!(
                            outcome, "-32003",
                            "{method} on a Ready export with {armed} must reach its \
                             capability gate unbound"
                        );
                    }
                    let still_ready = dispatch_as_daemon(
                        &f.services,
                        "workspace.export.read",
                        &json!({ "exportId": export_id, "seq": 0 }),
                    )
                    .await;
                    assert!(
                        still_ready.get("error").is_none(),
                        "a refused finalize must leave the export intact: {still_ready}"
                    );
                    let bare = json!({ "exportId": export_id });
                    let outcome = dispatch_unbound(&f.services, method, &bare).await;
                    assert_eq!(
                        outcome, "ok",
                        "{method} on a Ready export with neither option runs no gated \
                         call today; if this is now -32003 the gate stopped being \
                         conditional — move the row into the sweep"
                    );
                    let retired = dispatch_as_daemon(
                        &f.services,
                        "workspace.export.read",
                        &json!({ "exportId": export_id, "seq": 0 }),
                    )
                    .await;
                    assert_eq!(
                        retired["error"]["data"]["code"],
                        json!("not-found"),
                        "the bare finalize must have retired the session: {retired}"
                    );
                    continue;
                }
            };
            assert!(
                params.get(scope).is_none(),
                "{method}: minimal params already carry `{scope}`, so the sweep is not \
                 exercising the unscoped call"
            );
            params[scope] = json!(scope_value);
            let outcome = dispatch_unbound(&f.services, method, &params).await;
            if *method == "git.agentCommit" {
                assert_eq!(
                    outcome, *unarmed,
                    "git.agentCommit: the router arm now forwards `agentId` — assert \
                     -32003 through the router here and drop the direct call"
                );
                continue;
            }
            assert_eq!(
                outcome, "-32003",
                "{method} with `{scope}` supplied must reach its capability gate unbound"
            );
        }

        assert_eq!(intent_core::current_caller(), None, "caller leaked");
        let direct = f
            .services
            .git_agent_commit(
                f.ws.clone(),
                "m".to_string(),
                Some(AgentId::from("a1")),
                None,
                None,
                false,
                None,
            )
            .await;
        assert!(
            matches!(direct, Err(Error::Forbidden(_))),
            "git_agent_commit with an agentId, unbound: {direct:?}"
        );
    }

    /// Positive control for the table: the same real router + `Services`
    /// answers a bound daemon caller on a gated method, so `-32003` above is
    /// the missing binding and not the fixture.
    #[tokio::test]
    async fn bound_daemon_control_passes_a_gated_method() {
        let f = fixture().await;
        let params = json!({ "workspaceId": f.ws.as_str() });
        let unbound = dispatch_unbound(&f.services, "workspace.invite.list", &params).await;
        assert_eq!(unbound, "-32003");
        let bound = intent_core::with_caller(intent_core::Caller::Daemon, async {
            let frame = json!({
                "jsonrpc": "2.0", "id": 1,
                "method": "workspace.invite.list", "params": params,
            });
            let raw = handle_message(&f.services, &frame.to_string())
                .await
                .expect("response");
            serde_json::from_str::<Value>(&raw).expect("json")
        })
        .await;
        assert!(
            bound.get("error").is_none(),
            "daemon-bound workspace.invite.list: {bound}"
        );
    }

    /// Regression (PR #1877 review): `agent.respondPermission` returned
    /// `Ok({ resolved: false })` from its no-manager early return *before*
    /// the caller gate, so an unbound context saw a success. The gate now
    /// runs first; with no runtime manager (this fixture) the unbound call is
    /// `-32003`, while a bound daemon caller keeps the unresolved answer.
    #[tokio::test]
    async fn unbound_respond_permission_is_forbidden_without_a_manager() {
        let f = fixture().await;
        let params = json!({ "requestId": "req-1", "outcome": { "outcome": "cancelled" } });
        let unbound = dispatch_unbound(&f.services, "agent.respondPermission", &params).await;
        assert_eq!(unbound, "-32003");
        let bound = intent_core::with_caller(intent_core::Caller::Daemon, async {
            let frame = json!({
                "jsonrpc": "2.0", "id": 1,
                "method": "agent.respondPermission", "params": params,
            });
            let raw = handle_message(&f.services, &frame.to_string())
                .await
                .expect("response");
            serde_json::from_str::<Value>(&raw).expect("json")
        })
        .await;
        assert_eq!(
            bound["result"],
            json!({ "resolved": false }),
            "daemon-bound agent.respondPermission: {bound}"
        );
    }
}
