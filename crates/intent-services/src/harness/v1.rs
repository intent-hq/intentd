//! Harness **v1**: the original post-#2457 text set, byte-pinned by
//! `crate::v1_goldens` and `agent_manager::v1_turn_envelope_goldens`. Every
//! string here was moved verbatim from `rules.rs` / `agent_manager.rs` /
//! `lib.rs` / `agent_ops.rs` (H5 byte-neutral refactor); any edit MUST fail
//! the goldens and force a deliberate new-version decision instead.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use intent_core::events::{AGENT_DELETED, AGENT_FAILED, AGENT_IDLE};
use intent_core::TaskStatus;

use super::{ChildSettlementParams, Doctrine, Harness, HarnessEntry, TurnEnvelopeParams};
use crate::agent_ops::ready_delta::{UnblockedReason, UnblockedTask};
use crate::pr_monitor::PrMonitorSnapshot;

/// The v1 harness singleton.
pub(crate) struct V1;

/// v1's bundled doctrine: the `resources/agent-instructions/v1/` instruction
/// set and the `resources/specialists/v1/` embedded specialist bundle.
static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V1,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V1,
};

/// The v1 registry row. `version` is the stamped `"1.0"` every pre-1.1
/// session carries (and the migration-0096 backfill value); the feature
/// defaults are the `[agentFeatures]` defaults this doctrine was written
/// against (all on), used to gate legacy NULL-snapshot sessions the way a
/// live read would have when v1 was current.
pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "1.0",
    harness: &V1,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: FEATURE_LABELS,
};

/// Human-readable labels for every `agentFeatures` toggle v1 knows about.
/// Shared with later harness versions, whose feature surface is unchanged.
pub(crate) const FEATURE_LABELS: &[(&str, &str)] = &[
    ("backgroundHooks", "Background hooks (ws.hook.*)"),
    ("hostExec", "Host command execution (ws.host.exec)"),
    ("scripts", "Saved scripts (ws.script.*)"),
    ("terminalAccess", "Terminal read access (ws.terminal.*)"),
    ("browserAutomation", "Browser automation (ws.browser.*)"),
    ("richChatBlocks", "Rich chat block guidance"),
    (
        "structuredQuestions",
        "Structured questions (ws.app.question.ask)",
    ),
    (
        "attentionRequests",
        "Attention requests (reportBlocker / requestDiscussion)",
    ),
    ("stateSnapshot", "Per-turn state snapshot line"),
    ("prMonitor", "Centralized PR monitoring (ws.pr.monitor)"),
    ("taskGraph", "Task-graph workflow teaching"),
];

/// The `\n\n---\n\n` separator every assembled-prompt layer is joined with.
const LAYER_SEPARATOR: &str = "\n\n---\n\n";

/// Stable prefix of the rendered unblocked section, used by the delivery
/// paths as an idempotency guard (a requeued entry whose content already
/// carries a section is never re-annotated — same contract as the
/// dequeue-wait note).
pub(crate) const UNBLOCKED_SECTION_PREFIX: &str = "Tasks now unblocked by";

/// Stable prefix of [`Harness::stale_redrive_note`], used to keep the
/// annotation idempotent when a stale entry is requeued and redriven again.
pub(crate) const STALE_REDRIVE_NOTE_PREFIX: &str =
    "[SYSTEM NOTE] This message was queued before you completed";

/// Stable prefix of [`Harness::dequeue_wait_note`], used to keep the
/// annotation idempotent when an already-annotated entry is requeued and
/// drained again. Distinct from [`STALE_REDRIVE_NOTE_PREFIX`] ("…queued
/// before you completed"), so the two checks never shadow each other.
pub(crate) const DEQUEUE_WAIT_NOTE_PREFIX: &str = "[SYSTEM NOTE] This message was queued at";

/// Cap (in chars) on the `[hook logs]` section appended to dispatch/evict
/// wakes.
pub(crate) const HOOK_WAKE_LOGS_CAP: usize = 2048;

/// `{name} ({id})` display label for a settling child, falling back to the
/// bare id when no name resolved.
fn child_label(params: &ChildSettlementParams<'_>) -> String {
    params.agent_name.map_or_else(
        || params.child_id.to_string(),
        |name| format!("{name} ({})", params.child_id),
    )
}

/// The settlement verb for a child completion event type.
fn settlement_kind(event_type: &str) -> &str {
    match event_type {
        AGENT_IDLE => "completed",
        AGENT_FAILED => "failed",
        AGENT_DELETED => "was deleted",
        other => other,
    }
}

/// The monorepo#1016 stall tail shared by the completion wake and the group
/// child line.
fn stall_suffix_text(task_title: &str, task_status: &str) -> String {
    format!(
        " No completion report and assigned task \"{task_title}\" is still {task_status} — the \
         agent may have stalled rather than finished (monorepo#1016). Consider \
         ws.agent.wakeOrCreate to resume it."
    )
}

/// The kind-flavored attention verb (`requests a discussion` / `reports a
/// blocker`) shared by the parent and watcher attention wakes.
fn attention_verb(kind: &str) -> &str {
    if kind == "blocker" {
        "reports a blocker"
    } else {
        "requests a discussion"
    }
}

fn describe_opt<T: std::fmt::Display>(v: Option<T>) -> String {
    v.map_or_else(|| "unknown".to_string(), |v| v.to_string())
}

fn signed(delta: i64) -> String {
    if delta > 0 {
        format!("+{delta}")
    } else {
        delta.to_string()
    }
}

fn plural(n: i64) -> &'static str {
    if n == 1 {
        ""
    } else {
        "s"
    }
}

/// Per-check state transitions between two snapshots: added, removed, and
/// state-changed checks, plus a required-flag flip when both sides report
/// trustworthy `requiredKnown` flags.
///
/// Normal success transitions are suppressed: a check going `pending` →
/// `passed`, or appearing already green, is expected progress rather than a
/// reportable change — the suite-completion summary in
/// [`V1::pr_diff_lines`] covers the "everything finished" moment. A
/// `failed` → `passed` recovery IS reported, since it resolves a previously
/// reported failure.
fn diff_checks(old: &PrMonitorSnapshot, new: &PrMonitorSnapshot) -> Vec<String> {
    let (o, n) = (&old.requirements.checks, &new.requirements.checks);
    let required_known = o.required_known && n.required_known;
    let by_name = |items: &[crate::pr_ops::MergeRequirementCheck]| {
        items
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect::<HashMap<_, _>>()
    };
    let before = by_name(&o.items);
    let mut changes = Vec::new();
    for check in &n.items {
        match before.get(&check.name) {
            None => {
                if check.status != "passed" {
                    changes.push(format!("check started: {} ({})", check.name, check.status));
                }
            }
            Some(prev) => {
                if prev.status != check.status
                    && !(check.status == "passed" && prev.status == "pending")
                {
                    changes.push(format!(
                        "check {}: {} → {}",
                        check.name, prev.status, check.status
                    ));
                }
                if required_known && prev.required != check.required {
                    changes.push(format!(
                        "check {} is {} required to merge",
                        check.name,
                        if check.required { "now" } else { "no longer" }
                    ));
                }
            }
        }
    }
    let after: HashSet<&str> = n.items.iter().map(|c| c.name.as_str()).collect();
    for check in &o.items {
        if !after.contains(check.name.as_str()) {
            changes.push(format!("check removed: {}", check.name));
        }
    }
    changes
}

/// Fallback phrasing for the workspace-naming nudge when the provider's MCP
/// tool naming convention is unknown (or its workspace-MCP wiring hasn't
/// landed yet).
pub(crate) const GENERIC_NAMING_TOOL_REFERENCE: &str =
    "the `set_workspace_title` tool from the workspace MCP server";
pub(crate) const GENERIC_AGENT_NAMING_TOOL_REFERENCE: &str =
    "the `workspace_api` tool from the workspace MCP server";

impl Harness for V1 {
    fn join_prompt_layers(&self, parts: &[String]) -> String {
        parts.join(LAYER_SEPARATOR)
    }

    fn user_rules_wrapper(&self, content: &str, source: &str) -> String {
        format!(
            "## User Rules & Guidelines\n\nThe following rules and guidelines have been configured for this project. Please follow these conventions and best practices:\n\n```\n{content}\n```\n\nThese rules are loaded from: {source}"
        )
    }

    fn rtk_instruction_line(&self, subcommands: &[String]) -> String {
        format!(
            "Prefix these commands with rtk for compressed, LLM-friendly output: {}",
            subcommands.join(", ")
        )
    }

    fn sandboxed_implementor_hint(&self, sandbox_path: &str, sandbox_branch: &str) -> String {
        // Base commit isn't on AgentSession; it is tracked in the sandbox
        // record, so the hint names that instead of a SHA.
        let base_sha_note = "base commit tracked in sandbox metadata";
        format!(
            "## Workspace Isolation\n\n\
             You are working in an **isolated CoW (copy-on-write) sandbox** at `{sandbox_path}` \
             on branch `{sandbox_branch}` ({base_sha_note}). Your dependency caches (node_modules, \
             target/, .venv, etc.) are warm — you inherited them from the canonical workspace.\n\n\
             **Critical constraints:**\n\
             - Do NOT switch branches or checkout other refs in your sandbox.\n\
             - On completion, the system automatically merges your branch back to the canonical workspace.\n\
             - If your changes conflict with canonical, you will be **woken with the conflicting paths** \
             and a ref to reconcile against. When that happens, resolve the conflicts **in your sandbox only** \
             (rebase or merge onto the fetched canonical ref), then end your turn again. The system will \
             retry the merge. Do NOT attempt to touch other checkouts or the canonical workspace directly.\n\
             - You have up to 2 conflict-resolution attempts before the merge is deferred to manual intervention."
        )
    }

    fn coordinator_cow_hint(&self) -> String {
        "## Agent Delegation & Isolation\n\n\
         Delegated agents in this workspace run in **isolated CoW sandboxes** when you \
         use `isolation: \"cow\"` (or when the workspace's `cowIsolation` setting defaults it). \
         Each sandboxed agent works in its own copy-on-write clone of the workspace directory, \
         so parallel delegation is safe even when tasks touch overlapping files — agents cannot \
         stomp each other's work.\n\n\
         **Merge-back is automatic:** when a sandboxed agent completes, the system merges its \
         commits back into the canonical workspace **before** waking you. Clean merges propagate \
         completion normally. Conflicts suppress completion propagation and wake the agent (not you) \
         with conflict paths and resolution instructions; the agent fixes its sandbox and retries \
         the merge (up to 2 attempts).\n\n\
         **You only handle `blocked` outcomes:** if the canonical workspace has uncommitted changes \
         overlapping with the agent's work, or if conflict retries are exhausted, completion propagates \
         with `merge_pending` status. Use `sandbox.cow.merge` or `sandbox.cow.discard` RPCs, or ask the user \
         to commit/stash their WIP, then manually merge."
            .to_string()
    }

    fn specialist_role_section(&self, behavior_prompt: &str) -> String {
        format!(
            "# Your Specialist Role\n\n<specialist_role>\n{behavior_prompt}\n</specialist_role>\n\n\
             The instructions in <specialist_role> define your primary function. \
             Prioritize them above general guidance."
        )
    }

    fn commit_policy_clause(&self) -> String {
        "## Commit Policy\n\n\
         Commit through `ws.git.commit` — never run `git commit` yourself \
         unless the user explicitly asks for a git workflow that \
         `ws.git.commit` cannot express (e.g. multiple scoped commits on a \
         branch). You may commit when it makes sense for the work; the system \
         may also automatically commit any remaining changes when your turn \
         ends."
            .to_string()
    }

    fn role_reminder_footer(&self, name: &str, reminder: Option<&str>) -> String {
        let reminder = reminder.unwrap_or("Follow the instructions in <specialist_role> above.");
        format!("## Role Reminder\n\nYou are a {name}. {reminder}")
    }

    fn ask_questions_block(&self) -> String {
        "## Asking the User Questions\n\n\
         When requirements are ambiguous or a decision needs user input, ask \
         structured clarifying questions with `ws.app.question.ask` via the \
         `workspace_api` tool instead of burying questions in prose. Call it once \
         per question with 2-4 options; do not add an \"Other\" option — a \
         free-form answer is always offered automatically. Ask all your \
         questions, then end the turn: questions are presented when your turn \
         ends, and the answers arrive in the next user message."
            .to_string()
    }

    fn suggested_next_steps_block(&self, effective_auto_commit: bool) -> String {
        let example_second_line = if effective_auto_commit {
            "Check the changes in the diff view."
        } else {
            "Review changes before committing."
        };
        let auto_commit_clause = if effective_auto_commit {
            " Auto-commit is enabled; do not include prompts about committing or reviewing changes before committing."
        } else {
            ""
        };
        format!(
            "## Suggested Next Steps\n\n\
             At the end of your response, offer the user clear next actions as a \
             `<!-- suggested-prompts ... -->` HTML comment block:\n\n\
             ```\n\
             <!-- suggested-prompts\n\
             Run the tests to verify the implementation.\n\
             {example_second_line}\n\
             -->\n\
             ```\n\n\
             Write 2–4 prompts, each a short directive sentence phrased as \
             something the user might say next.{auto_commit_clause}"
        )
    }

    fn first_turn_prepend_block(&self, prompt: &str) -> String {
        format!("<system>\n{prompt}\n</system>")
    }

    fn snapshot_line(&self, json: &str) -> String {
        format!("current ws.agent.snapshot() => {json}")
    }

    fn naming_tool_reference(&self, provider_id: &str) -> &'static str {
        // Providers affix the MCP server name differently: auggie exposes
        // `<tool>_<server>` (trailing suffix), opencode exposes
        // `<server>_<tool>` (leading prefix; confirmed against captured
        // opencode 1.18.3 traffic). Every other provider gets the generic
        // fallback phrasing.
        match provider_id {
            "auggie" => "the `set_workspace_title_workspace-mcp` tool",
            "opencode" => "the `workspace-mcp_set_workspace_title` tool",
            _ => GENERIC_NAMING_TOOL_REFERENCE,
        }
    }

    fn agent_naming_tool_reference(&self, provider_id: &str) -> &'static str {
        match provider_id {
            "auggie" => "the `workspace_api_workspace-mcp` tool",
            "opencode" => "the `workspace-mcp_workspace_api` tool",
            _ => GENERIC_AGENT_NAMING_TOOL_REFERENCE,
        }
    }

    fn naming_nudge(
        &self,
        agent_tool_reference: Option<&str>,
        workspace_tool_reference: Option<&str>,
    ) -> String {
        let mut instructions = Vec::new();
        if let Some(tool_reference) = agent_tool_reference {
            instructions.push(format!(
                "This agent still has a generated name. Early in your first turn, call \
                 `ws.workspace.setAgentName` through {tool_reference} with a short 1–5 word \
                 task-specific name. Do this independently of workspace title naming and in \
                 parallel with information-gathering."
            ));
        }
        if let Some(tool_reference) = workspace_tool_reference {
            instructions.push(format!(
                "This workspace needs a title. As your first action, call {tool_reference} with a short 3\u{2013}5 word sentence-case title describing the task. This can be called in parallel with information-gathering."
            ));
        }
        format!("<system>\n{}\n</system>", instructions.join("\n"))
    }

    fn role_reminder_prefix(&self, name: &str, reminder: &str) -> String {
        format!("[Role Reminder: You are a {name}. {reminder}]")
    }

    fn compose_turn_prompt(&self, params: &TurnEnvelopeParams<'_>) -> String {
        // Inside-out layering, `\n\n` joins: body ← role reminder ← naming
        // nudge ← Context block ← snapshot line ← FirstTurnPrepend.
        let prompt_text = match params.role_reminder {
            Some(r) => format!("{r}\n\n{}", params.body),
            None => params.body.to_string(),
        };
        let prompt_text = match params.naming_nudge {
            Some(sys) => format!("{sys}\n\n{prompt_text}"),
            None => prompt_text,
        };
        let prompt_text = match params.stdin_context {
            Some(ctx) => format!("Context:\n{ctx}\n\n---\n\n{prompt_text}"),
            None => prompt_text,
        };
        let prompt_text = match params.snapshot_line {
            Some(line) => format!("{line}\n\n{prompt_text}"),
            None => prompt_text,
        };
        match params.first_turn_prepend {
            Some(sys) => format!("{sys}\n\n{prompt_text}"),
            None => prompt_text,
        }
    }

    fn stale_redrive_note(&self, report_timestamp: &str) -> String {
        format!(
            "[SYSTEM NOTE] This message was queued before you completed; your completion report \
             was already delivered to your parent at {report_timestamp}. Only call reportToParent \
             again if this message materially changes the outcome — do not re-send the same report."
        )
    }

    fn dequeue_wait_note(&self, queued_at: &str, waited: &str) -> String {
        format!(
            "[SYSTEM NOTE] This message was queued at {queued_at} and waited {waited} before delivery."
        )
    }

    fn wait_duration(&self, secs: i64) -> String {
        let secs = secs.max(0);
        let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
        if h > 0 {
            format!("{h}h {m}m")
        } else if m > 0 {
            format!("{m}m {s}s")
        } else {
            format!("{s}s")
        }
    }

    fn idle_timeout_warning(&self, window: &str) -> String {
        format!(
            "[SYSTEM WARNING] Your turn exceeded the inactivity timeout ({window}s of silence) \
             and was interrupted. If you were waiting on something external, schedule a \
             `ws.hook.schedule` background hook to watch the condition and end your turn instead \
             of blocking — the hook's wake message resumes you. Assess where you left off and \
             continue the work."
        )
    }

    fn truncation_redrive_nudge(&self) -> String {
        "[SYSTEM NOTE] Automatic redrive (monorepo#2863): your last turn appears truncated — \
         it ended after a sustained period of silence without completing your assigned task. \
         Assess where you left off and continue the work."
            .to_string()
    }

    fn empty_wake_redrive_nudge(&self) -> String {
        "[SYSTEM NOTE] Automatic recovery (monorepo#3262): your previous turn ended \
         unexpectedly mid-response, and the follow-up wake produced no output. Assess where \
         you left off and continue the work; if the work is already complete, say so \
         explicitly."
            .to_string()
    }

    fn empty_wake_attention_reason(&self) -> String {
        "Turn ended unexpectedly: the last turn stopped mid-response and the automatic \
         recovery wake produced no output. The agent is idle and needs a fresh message to \
         continue."
            .to_string()
    }

    fn note_images_notice(&self, n: usize) -> String {
        format!("[System: {n} image(s) from the referenced note(s) are attached to this message.]")
    }

    fn attachment_reference_notice(
        &self,
        name: &str,
        mime: Option<&str>,
        size: Option<u64>,
        id: &str,
    ) -> String {
        let mime_note = mime.map(|m| format!(", type {m}")).unwrap_or_default();
        let size_note = size.map(|s| format!(", {s} bytes")).unwrap_or_default();
        format!(
            "[Attachment: \"{name}\"{mime_note}{size_note} — attachmentId: {id}. The \
             file is NOT inlined in this message. Call \
             ws.file.getAttachment(\"{id}\") to copy it into your working directory, \
             then read it from the returned path.]"
        )
    }

    fn completion_wake(&self, params: &ChildSettlementParams<'_>, watch_retired: bool) -> String {
        let kind = settlement_kind(params.event_type);
        let label = child_label(params);
        let mut msg = format!("[WORKSPACE EVENTS] Child agent {label} {kind}.");
        let mut report_rendered = false;
        if let Some(report) = params.completion_report {
            let _ = write!(msg, " Report: {report}");
            report_rendered = true;
        } else if let Some(summary) = params.last_response_summary {
            let _ = write!(msg, " Summary: {summary}");
        }
        if let Some(err) = params.error {
            let _ = write!(msg, " Error: {err}");
        }
        // monorepo#1898: the stall suspicion and the report can come from
        // different session reads, so the tail is derived from what was
        // actually rendered — a wake carrying a `Report:` clause never gets
        // the contradictory "No completion report … may have stalled" suffix.
        if let Some((title, status)) = params.stall {
            if !report_rendered {
                msg.push_str(&stall_suffix_text(title, status));
            }
        }
        if watch_retired {
            // A deleted agent fails closed in `agent.watch` (rejected as
            // unknown) and has no next completion, so the deleted-kind wake
            // must not carry the re-arm pointer — say the agent cannot be
            // re-watched instead.
            if params.event_type == AGENT_DELETED {
                msg.push_str(
                    " NOTE: this wake consumed your one-shot watch on this agent — the watch is \
                     now retired. The agent was deleted, so it cannot be re-watched.",
                );
            } else {
                let _ = write!(msg, " NOTE: this wake consumed your one-shot watch on this agent — the watch is now \
                     retired. Call ws.agent.watch(\"{}\") again to be woken at its next completion.",
                    params.child_id);
            }
        }
        msg
    }

    fn group_child_line(&self, params: &ChildSettlementParams<'_>) -> String {
        let kind = settlement_kind(params.event_type);
        let label = child_label(params);
        let mut line = format!("- {label} {kind}.");
        let mut report_rendered = false;
        if let Some(report) = params.completion_report {
            let _ = write!(line, " Report: {report}");
            report_rendered = true;
        } else if let Some(summary) = params.last_response_summary {
            let _ = write!(line, " Summary: {summary}");
        }
        if let Some(err) = params.error {
            let _ = write!(line, " Error: {err}");
        }
        // Pending attention request (agent:attention-requested): the child's
        // immediate parent wake already fired at raise time (the alert); the
        // aggregated line carries the kind-flavored attention text as the
        // record.
        if let Some((kind, reason)) = params.attention {
            let verb = if kind == "blocker" {
                "Reported a blocker"
            } else {
                "Requested a discussion"
            };
            let _ = write!(line, " {verb}: {reason}");
        }
        // monorepo#1898: same consistency guard as `completion_wake` — never
        // append the "No completion report" tail to a line that already
        // rendered a `Report:` clause.
        if let Some((title, status)) = params.stall {
            if !report_rendered {
                line.push_str(&stall_suffix_text(title, status));
            }
        }
        line
    }

    fn group_settlement_wake(&self, total: usize, partial: bool, child_lines: &[String]) -> String {
        let status = if partial { "partial" } else { "completed" };
        let mut msg = format!(
            "[WORKSPACE EVENTS] All {total} delegated child agent(s) settled (completionStatus: {status})."
        );
        for line in child_lines {
            msg.push('\n');
            msg.push_str(line);
        }
        msg
    }

    fn report_to_parent_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        report: &str,
        watch_consumed: bool,
    ) -> String {
        let mut wake_text = format!(
            "[WORKSPACE EVENTS] Child agent {agent_name} ({agent_id}) reported. Report: {report}"
        );
        if watch_consumed {
            let _ = write!(
                wake_text,
                " NOTE: this report consumed your one-shot watch on this agent — it will NOT \
                 fire again on completion (failure/deletion still deliver). Call \
                 ws.agent.watch(\"{agent_id}\") again to be woken at its next completion."
            );
        }
        wake_text
    }

    fn attention_parent_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        kind: &str,
        reason: &str,
    ) -> String {
        format!(
            "[WORKSPACE EVENTS] Child agent {agent_name} ({agent_id}) {}: {reason}",
            attention_verb(kind)
        )
    }

    fn attention_watcher_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        kind: &str,
        reason: &str,
        grouped_watch: bool,
    ) -> String {
        let completion_promise = if grouped_watch {
            "you will be woken when its delegation group settles"
        } else {
            "you will still be woken at its completion"
        };
        format!(
            "[WORKSPACE EVENTS] Watched agent {agent_name} ({agent_id}) {}: {reason} (Your watch \
             on this agent remains armed; {completion_promise}.)",
            attention_verb(kind)
        )
    }

    fn event_subscription_wake(&self, event_count: usize, event_types: &[&str]) -> String {
        format!(
            "[WORKSPACE EVENTS] {} event(s) matched your subscription: {}.",
            event_count,
            event_types.join(", ")
        )
    }

    fn unblocked_section(&self, delta: &[UnblockedTask], multiple_triggers: bool) -> String {
        let noun = if multiple_triggers {
            "these completions"
        } else {
            "this completion"
        };
        let items = delta
            .iter()
            .map(|t| {
                let reason = match t.reason {
                    UnblockedReason::DepsSatisfied => "deps satisfied",
                    UnblockedReason::ConflictCleared => "conflict cleared",
                };
                let attention = match t.attention {
                    Some(TaskStatus::Waiting) => "; currently waiting — needs attention",
                    Some(TaskStatus::DiscussionNeeded) => {
                        "; currently discussion_needed — needs attention"
                    }
                    Some(TaskStatus::Blocked) => "; currently blocked — needs attention",
                    Some(TaskStatus::ReviewRequired) => {
                        "; currently review_required — needs attention"
                    }
                    _ => "",
                };
                format!(
                    "[{}](intent://local/task/{}) ({}{})",
                    t.title, t.note_id, reason, attention
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("{UNBLOCKED_SECTION_PREFIX} {noun}: {items}.")
    }

    fn hook_wake_logs_section(&self, message: &str, logs: Option<&str>) -> String {
        let Some(logs) = logs.filter(|l| !l.is_empty()) else {
            return message.to_string();
        };
        if logs.chars().count() <= HOOK_WAKE_LOGS_CAP {
            return format!("{message}\n\n[hook logs]\n{logs}");
        }
        let start = logs
            .char_indices()
            .rev()
            .nth(HOOK_WAKE_LOGS_CAP - 1)
            .map_or(0, |(i, _)| i);
        format!(
            "{message}\n\n[hook logs]\n[earlier log lines truncated]\n{}",
            &logs[start..]
        )
    }

    fn hook_state_dropped_warning(&self, state_bytes: usize, cap_bytes: usize) -> String {
        format!(
            "[hook state dropped: {state_bytes} bytes exceeds the {cap_bytes}-byte cap; \
             previous state kept]"
        )
    }

    fn hook_exec_failures_warning(&self, lines: &[&str], total: usize) -> String {
        let omitted = total.saturating_sub(lines.len());
        let more = if omitted > 0 {
            format!("; …and {omitted} more not shown")
        } else {
            String::new()
        };
        format!(
            "last run: {} host exec call{} failed (the run itself completed): {}{}",
            total,
            plural(i64::try_from(total).unwrap_or(i64::MAX)),
            lines.join("; "),
            more
        )
    }

    fn hook_wake_framing(
        &self,
        hook_name: &str,
        message: &str,
        state_note: Option<&str>,
    ) -> String {
        let mut content = format!("[Background hook \"{hook_name}\"] {message}");
        if let Some(note) = state_note {
            content.push_str("\n\n");
            content.push_str(note);
        }
        content
    }

    fn hook_dispatch_active_note(&self, expires_at: Option<&str>) -> String {
        format!(
            "[This hook remains active until {} — cancel via ws.hook.cancel \
             when no longer needed.]",
            expires_at.unwrap_or("its TTL elapses")
        )
    }

    fn hook_dispatch_retired_note(&self, hook_id: &str) -> String {
        format!(
            "[This hook is now retired and will not run again — recover its \
             script via ws.hook.get(\"{hook_id}\") and reschedule via \
             ws.hook.schedule if still needed.]"
        )
    }

    fn hook_evicted_state_note(&self, hook_id: &str) -> String {
        format!(
            "[This hook will not run again. Recover its script via \
             ws.hook.get(\"{hook_id}\") and schedule a new hook via \
             ws.hook.schedule if the condition is still worth watching.]"
        )
    }

    fn hook_evicted_failed_run_notice(&self, hook_name: &str, error: &str) -> String {
        format!("Your background hook \"{hook_name}\" was evicted after a failed run: {error}")
    }

    fn hook_evicted_internal_error_notice(&self, hook_name: &str, error: &str) -> String {
        format!("Your background hook \"{hook_name}\" was evicted after an internal error: {error}")
    }

    fn hook_expired_notice(
        &self,
        hook_name: &str,
        hook_id: &str,
        perpetual: bool,
        run_count: i64,
        dispatch_count: i64,
    ) -> String {
        let tally = if perpetual {
            format!(
                "{} run{}, {} dispatch{}",
                run_count,
                plural(run_count),
                dispatch_count,
                if dispatch_count == 1 { "" } else { "es" }
            )
        } else {
            format!(
                "{} run{} completed without a dispatch",
                run_count,
                plural(run_count)
            )
        };
        format!(
            "Your background hook \"{hook_name}\" expired after reaching its TTL ({tally}). \
             Schedule a new hook via ws.hook.schedule if the condition is still worth \
             watching — the original script is retrievable via ws.hook.get(\"{hook_id}\")."
        )
    }

    fn hook_cancelled_from_app_notice(&self) -> String {
        "This hook was cancelled from the app.".to_string()
    }

    fn hook_cancelled_workspace_archived_notice(&self) -> String {
        "This hook was cancelled because its workspace was archived.".to_string()
    }

    fn pr_monitor_label(&self, owner: &str, name: &str, number: i64) -> String {
        format!("{owner}/{name}#{number}")
    }

    fn pr_checklist(&self, s: &PrMonitorSnapshot) -> String {
        let r = &s.requirements;
        let mut lines = vec![format!("state: {}", r.state)];
        let approvals = match r.approvals.needed {
            Some(needed) => format!(
                "approvals: {} ({}/{} required)",
                r.approvals.decision, r.approvals.have, needed
            ),
            None => format!(
                "approvals: {} ({} approving)",
                r.approvals.decision, r.approvals.have
            ),
        };
        lines.push(approvals);
        if r.approvals.changes_requested > 0 {
            lines.push(format!(
                "changes requested by {} reviewer{}",
                r.approvals.changes_requested,
                plural(r.approvals.changes_requested)
            ));
        }
        let mut checks = format!(
            "checks: {} passed, {} failed, {} pending (of {})",
            r.checks.passed, r.checks.failed, r.checks.pending, r.checks.total
        );
        if !r.checks.failing_required.is_empty() {
            let _ = write!(
                checks,
                "; failing required: {}",
                r.checks.failing_required.join(", ")
            );
        }
        if !r.checks.pending_required.is_empty() {
            let _ = write!(
                checks,
                "; pending required: {}",
                r.checks.pending_required.join(", ")
            );
        }
        if !r.checks.required_known {
            checks.push_str(" (required-check flags unavailable)");
        }
        lines.push(checks);
        let threads = match r.threads.resolution_required {
            Some(true) => format!(
                "unresolved threads: {} (resolution required to merge)",
                r.threads.unresolved
            ),
            _ => format!("unresolved threads: {}", r.threads.unresolved),
        };
        lines.push(threads);
        if r.has_conflicts {
            lines.push("merge conflicts present".to_string());
        }
        if r.is_behind {
            lines.push("branch is behind its base".to_string());
        }
        if r.is_in_merge_queue == Some(true) {
            lines.push("in merge queue".to_string());
        }
        if let Some(reason) = &r.merge_blocked_reason {
            lines.push(format!("blocked: {reason}"));
        }
        if !r.rules_known {
            lines.push(
                "(branch rules unreadable — approval/thread requirements unknown)".to_string(),
            );
        }
        lines
            .into_iter()
            .map(|l| format!("- {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn pr_diff_lines(&self, old: &PrMonitorSnapshot, new: &PrMonitorSnapshot) -> Vec<String> {
        let mut changes = Vec::new();
        let (o, n) = (&old.requirements, &new.requirements);

        if o.state != n.state {
            changes.push(format!("state: {} → {}", o.state, n.state));
        }
        if o.is_draft != n.is_draft {
            changes.push(if n.is_draft {
                "marked as draft".to_string()
            } else {
                "marked ready for review".to_string()
            });
        }
        if old.head_sha != new.head_sha {
            let sha = new.head_sha.as_deref().unwrap_or("unknown");
            let short: String = sha.chars().take(8).collect();
            changes.push(format!("new commits pushed (head is now {short})"));
        }

        // Review decision + approval counts.
        if o.approvals.decision != n.approvals.decision {
            changes.push(format!(
                "review decision: {} → {}",
                o.approvals.decision, n.approvals.decision
            ));
        }
        if o.approvals.have != n.approvals.have {
            let verb = if n.approvals.have > o.approvals.have {
                "new approval"
            } else {
                "approval withdrawn"
            };
            changes.push(format!(
                "{verb} ({} → {} approving)",
                o.approvals.have, n.approvals.have
            ));
        }
        if o.approvals.changes_requested != n.approvals.changes_requested {
            changes.push(format!(
                "changes-requested reviews: {} → {}",
                o.approvals.changes_requested, n.approvals.changes_requested
            ));
        }
        if o.approvals.needed != n.approvals.needed {
            changes.push(format!(
                "required approvals: {} → {}",
                describe_opt(o.approvals.needed),
                describe_opt(n.approvals.needed)
            ));
        }

        // Comments + threads.
        if old.conversation_count != new.conversation_count {
            let delta = new.conversation_count - old.conversation_count;
            changes.push(format!(
                "{} conversation comment{} ({} total)",
                signed(delta),
                plural(delta.abs()),
                new.conversation_count
            ));
        }
        if old.review_comment_count != new.review_comment_count {
            let delta = new.review_comment_count - old.review_comment_count;
            changes.push(format!(
                "{} review comment{} ({} total)",
                signed(delta),
                plural(delta.abs()),
                new.review_comment_count
            ));
        }
        if o.threads.unresolved != n.threads.unresolved {
            let verb = if n.threads.unresolved < o.threads.unresolved {
                "thread(s) resolved"
            } else {
                "thread(s) unresolved/opened"
            };
            changes.push(format!(
                "{verb}: {} → {} unresolved",
                o.threads.unresolved, n.threads.unresolved
            ));
        }

        changes.extend(diff_checks(old, new));

        // Suite completion: the last pending check finishing is reported as
        // ONE aggregate line (individual success lines are suppressed above).
        if o.checks.pending > 0 && n.checks.pending == 0 && n.checks.total > 0 {
            changes.push(if n.checks.failed == 0 {
                format!("all checks passed ({})", n.checks.total)
            } else {
                format!(
                    "all checks completed: {} passed, {} failed",
                    n.checks.passed, n.checks.failed
                )
            });
        }

        // Mergeability + residual signals.
        if o.has_conflicts != n.has_conflicts {
            changes.push(if n.has_conflicts {
                "merge conflicts appeared".to_string()
            } else {
                "merge conflicts resolved".to_string()
            });
        }
        if o.is_behind != n.is_behind {
            changes.push(if n.is_behind {
                "branch is now behind its base".to_string()
            } else {
                "branch is no longer behind its base".to_string()
            });
        }
        if o.is_in_merge_queue != n.is_in_merge_queue {
            changes.push(if n.is_in_merge_queue == Some(true) {
                "entered the merge queue".to_string()
            } else {
                "left the merge queue".to_string()
            });
        }
        // Keyed on the event identity (`at`), not the queued flag: an
        // enter→eject pair that nets out on `isInMergeQueue` still yields
        // this reportable line.
        let ejection_at = |r: &crate::pr_ops::MergeRequirements| {
            r.merge_queue_ejection.as_ref().map(|e| e.at.clone())
        };
        if ejection_at(o) != ejection_at(n) {
            if let Some(e) = &n.merge_queue_ejection {
                changes.push(match &e.reason {
                    Some(reason) => format!(
                        "removed from the merge queue ({})",
                        reason.replace('_', " ")
                    ),
                    None => "removed from the merge queue".to_string(),
                });
            }
        }
        if o.mergeable != n.mergeable {
            changes.push(format!(
                "mergeable: {} → {}",
                describe_opt(o.mergeable),
                describe_opt(n.mergeable)
            ));
        }
        if o.merge_state_status != n.merge_state_status {
            changes.push(format!(
                "merge state: {} → {}",
                o.merge_state_status.as_deref().unwrap_or("unknown"),
                n.merge_state_status.as_deref().unwrap_or("unknown")
            ));
        }
        if o.merge_blocked_reason != n.merge_blocked_reason {
            changes.push(match &n.merge_blocked_reason {
                Some(reason) => format!("merge blocked: {reason}"),
                None => "merge is no longer blocked".to_string(),
            });
        }
        changes
    }

    fn pr_change_wake(
        &self,
        label: &str,
        changes: &[String],
        snapshot: &PrMonitorSnapshot,
    ) -> String {
        let bullets = changes
            .iter()
            .map(|c| format!("- {c}"))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "[PR monitor {label}] {} change{} detected on \"{}\" ({}):\n{bullets}\n\nWhere the PR \
             stands now:\n{}",
            changes.len(),
            plural(i64::try_from(changes.len()).expect("value fits in i64")),
            snapshot.title,
            snapshot.url,
            self.pr_checklist(snapshot)
        )
    }

    fn pr_terminal_wake(
        &self,
        label: &str,
        changes: &[String],
        snapshot: &PrMonitorSnapshot,
    ) -> String {
        let outcome = if snapshot.requirements.state == "merged" {
            "was MERGED"
        } else {
            "was CLOSED without merging"
        };
        let mut body = format!(
            "[PR monitor {label}] \"{}\" {outcome} ({}).\n\nMonitoring has STOPPED — this monitor \
             is retired and will not report again.",
            snapshot.title, snapshot.url
        );
        if !changes.is_empty() {
            let bullets = changes
                .iter()
                .map(|c| format!("- {c}"))
                .collect::<Vec<_>>()
                .join("\n");
            let _ = write!(body, "\n\nChanges since the last report:\n{bullets}");
        }
        body
    }

    fn pr_monitor_cancelled_from_app_notice(&self, label: &str) -> String {
        format!(
            "[PR monitor {label}] This monitor was cancelled from the app — it will not \
             report again."
        )
    }

    fn pr_monitor_cancelled_workspace_archived_notice(&self, label: &str) -> String {
        format!(
            "[PR monitor {label}] This monitor was cancelled because its workspace was \
             archived — it will not report again."
        )
    }

    fn delegation_first_message(&self, body: Option<&str>, title: &str, note_id: &str) -> String {
        // Build the preamble from adjacent string literals (via `concat!`)
        // so no source-level indentation leaks into the emitted bytes. Every
        // `\n` is explicit; the resulting string is byte-for-byte the
        // reference `DelegateTaskTool` preamble
        // (`agent-interaction-tools.ts`).
        let preamble = format!(
            concat!(
                "**Your Task Note:** \"{title}\" (ID: {note_id})\n",
                "This note is your workspace for this task. Update it with your progress, findings, and deliverables.\n",
                "\n",
                "**SCOPE: Complete THIS task only.** When done, mark it complete and end your session. Do not pick up other tasks.",
            ),
            title = title,
            note_id = note_id,
        );
        match body {
            Some(body) if !body.is_empty() => {
                format!("{body}\n\n---\n{preamble}")
            }
            _ => preamble,
        }
    }

    fn questions_dismissed_notice(&self, count: usize) -> String {
        let noun = match count {
            0 => "questions".to_string(),
            1 => "1 question".to_string(),
            n => format!("{n} questions"),
        };
        format!(
            "User dismissed your {noun} without answering. This is an informative \
             notice only — do not re-ask and do not proceed with any work; end \
             your turn and wait for the user's next message."
        )
    }
}
