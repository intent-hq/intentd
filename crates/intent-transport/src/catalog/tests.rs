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
        ("host.rs", "host."),
        ("control.rs", "system."),
        ("startup.rs", "system."),
        ("pairing.rs", "pairing."),
        ("server.rs", "server."),
        ("provider_setup.rs", "providers.setup."),
        ("invite.rs", "invite."),
        ("invite.rs", "workspace.invite."),
        ("invite.rs", "host.invite."),
        ("presence.rs", "presence."),
        ("presence.rs", "note.presence."),
        ("agent_workers.rs", "agent.workers."),
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
/// Also within 10.3: +1 router method (`github.users.search`, the
/// collaborator picker's login-prefix user search).
///
/// Agent memory attribution (§5.5): +1 router method (`agent.memoryUsage`).
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
///
/// Direct member add: +2 router methods (`principal.list`, the owner-only
/// roster of credentialed guests; `workspace.members.add`, the owner-only
/// direct attach of one of them).
///
/// Provider-generic auth (protocol 10.5, §5.27): +5 router methods
/// (`sourceControl.authStatus` / `connect` / `cancelAuth` / `revoke` /
/// `getUser`); the `github.*` auth quintet stays as byte-identical aliases.
///
/// Provider-generic identity proof (protocol 10.8): +2 router methods
/// (`sourceControl.identityProof.create` / `delete`, the GitHub gist or
/// GitLab snippet proof by `provider`); the `github.identityProof.*` pair
/// stays as byte-identical aliases.
/// Direct user retirement (protocol 10.10): +1 router method (`agent.retire`).
/// Explicit PR detail reads (protocol 13.2): +3 router methods.
/// Explicit queued batch sending (protocol 13.1): +1 router method.
/// Reversible script history (protocol 10.11): +2 router methods
/// (`script.archive`, `script.restore`).
/// Durable script monitors (protocol 12.1): +3 router methods.
/// Protocol 13.0 removes the deprecated git.commit router method.
// GitLab pre-workspace checkout (13.5): +6 router methods.
// Provider adapter preparation (13.6): +1 fast-path method.
// Desktop control adds four router methods; reverse methods are counted separately.
// GitLab checkout repository configuration adds one router method.
// Cancellable note deletion (13.10): +3 router methods.
// Legacy workspace import removal (15.0): -1 fast-path method.
// Human workspace discussions: +5 router methods; PR workspace lookup: +1.
// Named provider profiles (§5.54): +4 router methods.
// Provider profile order (§5.54 `providers.profiles.reorder`): +1 router method.
// Default configuration overrides (§5.54 `providers.profiles.updateDefault`): +1 router method.
// Generic event recovery adds the events.fence fast path.
// Correlated discussion controls: +2 router methods.
// Persisted guest roles: +1 owner-only role assignment method.
// Startup status (14.1): +1 fast-path method.
// Scoped repository pages and child Git policy (15.1): +8 router methods.
// Provider worker observations (§5.5b, `agentWorkers: 1`): +2 router, +1 fast path.
// Note paging removal: -3 router methods (cancellable note deletion).
const EXPECTED_TOTAL_METHODS: usize = 462;

/// Golden count: router methods (canonical + canonical forms of aliases).
/// Protocol 12.0 removes five router methods, three fast paths and two aliases.
/// Protocol 12.1 adds the three script-monitor controls.
/// The subsequent git.commit removal removes one more router method.
/// Protocol 13.10 adds three cancellable note deletion controls.
/// Five discussion methods and one PR workspace lookup method are additive.
/// Named provider profiles (§5.54) add the four `providers.profiles.*` methods,
/// then `providers.profiles.reorder` and `providers.profiles.updateDefault`.
/// Correlated discussion controls add `discussion.stopAgent` and `discussion.retryAgent`.
/// Persisted guest roles add `workspace.members.setRole`.
/// Scoped repository pages and child Git policy add eight router methods.
/// Provider worker observations (§5.5b) add `agent.workers.list` / `.stop`.
/// Removing note paging drops the three cancellable note deletion controls.
const EXPECTED_ROUTER_METHODS: usize = 404;

/// Golden count: fast-path methods (intercepted before router).
/// `agent.workers.subscribe` (§5.5b) holds per-connection subscription state.
const EXPECTED_FASTPATH_METHODS: usize = 58;

/// Golden count: method aliases.
const EXPECTED_ALIASES: usize = 0;

/// Golden count: server→client notifications.
const EXPECTED_NOTIFICATIONS: usize = 7;

/// Golden count: client-served reverse RPCs.
const EXPECTED_REVERSE_METHODS: usize = 6;

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
        "re-delivers the requeued entry with the stamp captured at enqueue / wake delivery — the retrier is never the author (`wake_stamp_survives_terminal_failure_requeue_over_wss`)",
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
        "agent.sendQueuedMessagesNow",
        "drains the selected entries with their captured stamps; the drainer is not the author (WSS explicit batch coverage)",
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
/// - `discussion.*` uses its own human-only store, never the agent transcript.
/// - `note.*`, `comment.*`, `task.*`, `github.*` write notes, comments and
///   forge objects — never the agent transcript; `accept-changes.*` and
///   `file-tracking.*` drive the worktree / PR flow the same way.
const NON_USER_ORIGIN_METHODS: &[&str] = &[
    "accept-changes.addRemote",
    "accept-changes.execute",
    "accept-changes.getStatus",
    "accept-changes.mergePR",
    "accept-changes.prepare",
    "accept-changes.reconcile",
    "accept-changes.release",
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
    "agent.markSeen",
    "agent.memoryUsage",
    "agent.pendingPermissions",
    "agent.removeQueuedMessage",
    "agent.rename",
    "agent.replaceMessages",
    "agent.reportToParent",
    "agent.resolveInterrupted",
    "agent.resolveProposal",
    "agent.respondPermission",
    "agent.restore",
    "agent.retire",
    "agent.setModel",
    "agent.stop",
    "agent.subscribe",
    "agent.summary",
    "agent.unsubscribe",
    "agent.update",
    "agent.workers.list",
    "agent.workers.stop",
    "agent.workers.subscribe",
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
    "desktop.getState",
    "desktop.respondPermission",
    "desktop.revoke",
    "desktop.setPermission",
    "discussion.conversations",
    "discussion.list",
    "discussion.react",
    "discussion.recipients",
    "discussion.retryAgent",
    "discussion.send",
    "discussion.stopAgent",
    "drafts.clear",
    "drafts.get",
    "drafts.set",
    "event.agentActivity",
    "event.query",
    "event.workspaceSummary",
    "events.fence",
    "events.subscribe",
    "events.unsubscribe",
    "file-tracking.getAgentLocks",
    "file-tracking.getChanges",
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
    "git.agentCommit",
    "git.branchDiff",
    "git.branchStatus",
    "git.changes",
    "git.checkMergeConflicts",
    "git.checkoutBranch",
    "git.clone",
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
    "host.executionContext",
    "host.findApp",
    "host.findBinary",
    "host.invite.create",
    "host.invite.list",
    "host.invite.revoke",
    "host.invite.searchAccounts",
    "host.listDirectory",
    "host.listInstalledEditors",
    "host.members.list",
    "host.members.remove",
    "host.openInEditor",
    "host.prepareProviderAdapters",
    "host.providerAuthStatus",
    "host.providerDiscovery",
    "host.providerTestPrompt",
    "host.status",
    "host.toolAvailability",
    "identity.authStatus",
    "identity.cancelAuth",
    "identity.connect",
    "identity.getUser",
    "identity.revoke",
    "identity.select",
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
    "metrics.getAgentStats",
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
    "pairing.getSelfInfo",
    "pr.refresh",
    "prMonitor.cancel",
    "prMonitor.flush",
    "prMonitor.list",
    "presence.snapshot",
    "presence.update",
    "primitive.addAgentAction",
    "primitive.addCli",
    "primitive.addPatch",
    "primitive.addReference",
    "principal.list",
    "principal.me",
    "principal.revokeSelf",
    "providers.catalog",
    "providers.profiles.create",
    "providers.profiles.delete",
    "providers.profiles.list",
    "providers.profiles.reorder",
    "providers.profiles.update",
    "providers.profiles.updateDefault",
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
    "script.archive",
    "script.create",
    "script.get",
    "script.list",
    "script.output",
    "script.remove",
    "script.restart",
    "script.restore",
    "script.run",
    "script.start",
    "script.status",
    "script.stop",
    "scriptMonitor.cancel",
    "scriptMonitor.cancelRun",
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
    "server.pairingInfo",
    "server.rotateToken",
    "settings.get",
    "settings.list",
    "settings.reset",
    "settings.update",
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
    "specialist.create",
    "specialist.delete",
    "specialist.edit",
    "specialist.get",
    "specialist.list",
    "stats.getRateHistory",
    "stats.getUsage",
    "system.capabilities",
    "system.gitCredential",
    "system.requestUpdate",
    "system.shutdown",
    "system.startupStatus",
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
    "workspace.findByPullRequest",
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
    "workspace.members.add",
    "workspace.members.leave",
    "workspace.members.list",
    "workspace.members.remove",
    "workspace.members.setRole",
    "workspace.repositoryContext",
    "workspace.repositoryContext.capture",
    "workspace.repositoryContext.release",
    "workspace.repositorySelection.capture",
    "workspace.repositorySelection.reconcile",
    "workspace.repositorySelection.release",
    "workspace.repositorySelection.reset",
    "workspace.repositorySelection.save",
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
        12,
        "the user-origin entry points including explicit queue batching and \
         workspace.create's initialAgent kickoff; a change here needs \
         attribution coverage and docs/protocol/ updated alongside"
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
/// `forward.*`, `terminal.*`, script definition writes, `github.*`, `linear.*`, `sentry.*`,
/// `voice.*`, `settings.*`, `repo.*` / `repoConfig.*`, `mcp.*`, `server.*`,
/// `pairing.*`, `providers.setup.*`, `system.*` (but `system.capabilities`
/// and `system.status`),
/// `rules.list` / `rules.update`, `sandbox.*`, `unsloth.*`, `debug.*`, workspace lifecycle /
/// export / import / setup / browser-client pinning, `git.clone`,
/// `git.agentCommit` (agent-only), agent delegation (`agent.delegate`,
/// `agent.wakeOrCreate`), agent deletion / proposals /
/// one-shot completions, `agent.replaceMessages` (persists client-supplied user rows
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
    "accept-changes.reconcile",
    "accept-changes.release",
    "agent.cancelDelete",
    "agent.completeOnce",
    "agent.delegate",
    "agent.delete",
    "agent.diagnostics",
    "agent.enhancePrompt",
    "agent.memoryUsage",
    "agent.replaceMessages",
    "agent.reportToParent",
    "agent.resolveProposal",
    "agent.wakeOrCreate",
    "browser.closeTab",
    "browser.exec",
    "browser.listTabs",
    "browser.navigateTab",
    "browser.removeTab",
    "browser.syncTabs",
    "browser.upsertTab",
    "debug.sampleStacks",
    "desktop.getState",
    "desktop.respondPermission",
    "desktop.setPermission",
    "file-tracking.getAgentLocks",
    "file-tracking.getChanges",
    "file-tracking.loadCommits",
    "file-tracking.stage",
    "file-tracking.unstage",
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
    "host.executionContext",
    "host.findApp",
    "host.findBinary",
    "host.invite.create",
    "host.invite.list",
    "host.invite.revoke",
    "host.invite.searchAccounts",
    "host.listDirectory",
    "host.listInstalledEditors",
    "host.members.list",
    "host.members.remove",
    "host.openInEditor",
    "host.prepareProviderAdapters",
    "host.providerAuthStatus",
    "host.providerDiscovery",
    "host.providerTestPrompt",
    "identity.authStatus",
    "identity.cancelAuth",
    "identity.connect",
    "identity.getUser",
    "identity.revoke",
    "identity.select",
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
    "pairing.getInfo",
    "prMonitor.cancel",
    "prMonitor.flush",
    "principal.list",
    "providers.profiles.create",
    "providers.profiles.delete",
    "providers.profiles.list",
    "providers.profiles.reorder",
    "providers.profiles.update",
    "providers.profiles.updateDefault",
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
    "rules.list",
    "rules.update",
    "sandbox.cow.discard",
    "sandbox.cow.merge",
    "script.archive",
    "script.create",
    "script.remove",
    "script.restore",
    "scriptMonitor.cancel",
    "scriptMonitor.cancelRun",
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
    "sourceControl.authStatus",
    "sourceControl.cancelAuth",
    "sourceControl.connect",
    "sourceControl.getUser",
    "sourceControl.gitlab.childGit.get",
    "sourceControl.gitlab.childGit.set",
    "sourceControl.identityProof.create",
    "sourceControl.identityProof.delete",
    "sourceControl.read.resolveKnownRepository",
    "sourceControl.revoke",
    "specialist.create",
    "specialist.delete",
    "specialist.edit",
    "system.gitCredential",
    "system.requestUpdate",
    "system.shutdown",
    "system.startupStatus",
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
    "workspace.members.add",
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
fn retired_rpc_names_are_absent_from_catalog_and_authorization() {
    for method in [
        "git.diff",
        "git.log",
        "pr.status",
        "file-tracking.getLineStats",
        "metrics.getWorkspaceStats",
        "metrics.getAllWorkspaceStats",
        "metrics.clearAgentStats",
        "forward.create",
        "forward.list",
        "forward.close",
    ] {
        assert!(!ROUTER_METHODS.contains(&method), "{method}");
        assert!(!FASTPATH_METHODS.contains(&method), "{method}");
        assert_eq!(canonical_method(method), method);
        assert!(!collaborator_may_call(method), "{method}");
        assert!(!super::member_may_call(method), "{method}");
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
    assert!(!collaborator_may_call("git.diff"));
    assert!(!collaborator_may_call("git.log"));
    assert!(collaborator_may_call("git.diffs"));
    assert!(collaborator_may_call("git.commits"));
    for allowed in [
        "client.hello",
        "events.fence",
        "events.subscribe",
        "chat.subscribe",
        "workspace.subscribe",
        "system.capabilities",
        "system.status",
        "host.status",
        "principal.me",
        "pr.refresh",
        "prMonitor.list",
        "presence.snapshot",
        "presence.update",
        "note.presence.subscribe",
        "agent.rename",
        "agent.update",
        "agent.stop",
        "agent.sendMessage",
        "agent.setModel",
        "hook.list",
        "git.push",
    ] {
        assert!(collaborator_may_call(allowed), "{allowed} must be allowed");
    }
    for refused in [
        "host.exec",
        "host.openInEditor",
        "browser.exec",
        "browser.listTabs",
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
        "agent.delegate",
        "agent.wakeOrCreate",
        "agent.delete",
        "agent.cancelDelete",
        "agent.replaceMessages",
        "terminal.list",
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

/// Freeze a separate member remainder; the guest golden remains unchanged.
#[test]
fn member_methods_and_administrator_remainder_are_classified() {
    const REFUSED: &[&str] = &[
        "agent.memoryUsage",
        "agent.replaceMessages",
        "agent.reportToParent",
        "debug.sampleStacks",
        "github.authStatus",
        "github.cancelAuth",
        "github.connect",
        "github.getUser",
        "github.identityProof.create",
        "github.identityProof.delete",
        "github.revoke",
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
        "host.invite.create",
        "host.invite.list",
        "host.invite.revoke",
        "host.invite.searchAccounts",
        "host.listDirectory",
        "host.listInstalledEditors",
        "host.members.list",
        "host.members.remove",
        "host.openInEditor",
        "host.prepareProviderAdapters",
        "host.providerTestPrompt",
        "identity.authStatus",
        "identity.cancelAuth",
        "identity.connect",
        "identity.getUser",
        "identity.revoke",
        "identity.select",
        "invite.accept",
        "invite.challenge",
        "invite.inspect",
        "invite.prove",
        "linear.authStatus",
        "mcp.oauth.delete",
        "mcp.oauth.get",
        "mcp.oauth.list",
        "mcp.oauth.set",
        "mcp.servers.create",
        "mcp.servers.delete",
        "mcp.servers.getStatus",
        "mcp.servers.list",
        "mcp.servers.restart",
        "mcp.servers.update",
        "mcp.testConnection",
        "pairing.getInfo",
        "providers.profiles.create",
        "providers.profiles.delete",
        "providers.profiles.list",
        "providers.profiles.reorder",
        "providers.profiles.update",
        "providers.profiles.updateDefault",
        "providers.setup.cancel",
        "providers.setup.login",
        "providers.setup.start",
        "providers.setup.status",
        "repo.remove",
        "rules.update",
        "sentry.authStatus",
        "server.pairingInfo",
        "server.rotateToken",
        "settings.get",
        "settings.list",
        "settings.reset",
        "settings.update",
        "sourceControl.authStatus",
        "sourceControl.cancelAuth",
        "sourceControl.connect",
        "sourceControl.getUser",
        "sourceControl.gitlab.childGit.get",
        "sourceControl.gitlab.childGit.set",
        "sourceControl.identityProof.create",
        "sourceControl.identityProof.delete",
        "sourceControl.read.resolveKnownRepository",
        "sourceControl.revoke",
        "specialist.create",
        "specialist.delete",
        "specialist.edit",
        "system.gitCredential",
        "system.requestUpdate",
        "system.shutdown",
        "system.startupStatus",
        "unsloth.status",
        "unsloth.stop",
    ];
    let universe: BTreeSet<_> = ROUTER_METHODS
        .iter()
        .chain(FASTPATH_METHODS)
        .copied()
        .collect();
    let allowed: BTreeSet<_> = super::MEMBER_METHODS.iter().copied().collect();
    assert_eq!(allowed.len(), super::MEMBER_METHODS.len());
    assert!(allowed
        .iter()
        .all(|m| universe.contains(m) && !collaborator_may_call(m)));
    assert_eq!(
        allowed.iter().copied().collect::<Vec<_>>(),
        super::MEMBER_METHODS
    );
    let refused: Vec<_> = universe
        .into_iter()
        .filter(|m| !super::member_may_call(m))
        .collect();
    assert_eq!(refused, REFUSED);
    for (alias, canonical) in METHOD_ALIASES {
        assert_eq!(
            super::member_may_call(alias),
            super::member_may_call(canonical)
        );
    }
    assert!(!super::member_may_call("unknown.method"));
    for method in REVERSE_METHODS {
        assert_eq!(super::member_may_call(method), *method == "browser.exec");
    }
}

#[test]
fn removed_git_commit_is_not_advertised_or_authorized() {
    assert!(!super::ROUTER_METHODS.contains(&"git.commit"));
    assert!(!super::collaborator_may_call("git.commit"));
    assert!(!super::collaborator_may_call("git.agentCommit"));
    assert!(super::member_may_call("git.agentCommit"));
}
