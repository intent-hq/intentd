//! Workspace MCP tool registry: after the WSAPI-8 cutover this exposes a
//! single tool, `workspace_api`, whose dispatch lives in [`super::dispatch`].
//! Names carry NO `_workspace-mcp` suffix: ACP providers (auggie) already
//! suffix every tool with its MCP server name, so agents see
//! `workspace_api_workspace-mcp`; baking the suffix in here would double it.

use std::borrow::Cow;

use intent_core::settings_file::AgentFeaturesSettings;
use serde_json::{json, Map, Value};

/// One input parameter of a tool, used to synthesize an MCP `inputSchema`.
pub struct Param {
    /// JSON property name.
    pub name: &'static str,
    /// JSON Schema type (`string`, `integer`, `boolean`, `array`).
    pub ty: &'static str,
    /// Whether the parameter is required.
    pub required: bool,
}

const fn p(name: &'static str, ty: &'static str, required: bool) -> Param {
    Param { name, ty, required }
}

/// A tool definition: name, human description, and its parameter list.
pub struct ToolDef {
    /// Registry tool name (`workspace_api`); agents see it with the
    /// provider-appended server suffix (`workspace_api_workspace-mcp`).
    pub name: &'static str,
    /// Short human description.
    pub description: &'static str,
    /// Declared parameters.
    pub params: &'static [Param],
}

impl ToolDef {
    /// Synthesize the MCP `inputSchema` (`type: object` + properties + required).
    pub fn schema(&self) -> Value {
        let mut props = Map::new();
        let mut required = Vec::new();
        for param in self.params {
            let mut prop = Map::new();
            prop.insert("type".to_string(), Value::String(param.ty.to_string()));
            if param.ty == "array" {
                prop.insert("items".to_string(), json!({ "type": "string" }));
            }
            props.insert(param.name.to_string(), Value::Object(prop));
            if param.required {
                required.push(Value::String(param.name.to_string()));
            }
        }
        json!({
            "type": "object",
            "properties": Value::Object(props),
            "required": Value::Array(required),
        })
    }
}

/// The full tool registry (pre-denylist). Returns the chief variant when
/// `is_chief` is true (full `ws.app.*` surface), base variant otherwise.
pub fn all_tools(is_chief: bool) -> &'static [ToolDef] {
    if is_chief {
        ALL_TOOLS_CHIEF
    } else {
        ALL_TOOLS
    }
}

/// `workspace_api` tool description — the `ws.*` API reference agents read
/// before writing JS. Mirrors the TS reference `TOOL_DESCRIPTION` in
/// `workspace-js-api-tool.ts`, restricted to the surface actually bound in
/// `super::bindings::*`. Exclusions vs the reference:
///
/// * The `ws.app.*` namespace (chief-workspace app APIs) is only advertised
///   to chief-workspace agents via [`WORKSPACE_API_DESCRIPTION_CHIEF`] —
///   with ONE exception: `ws.app.question.ask` is un-gated (any agent may
///   ask the user structured questions), so both descriptions advertise it.
/// * `ws.workspace.context`, `ws.workspace.timeline`,
///   `ws.workspace.referenceDocs`, `ws.workspace.emitNotification` — deferred
///   per the WSAPI-5 report; the bindings surface a clear
///   "not yet available in this daemon port" error rather than pretending to
///   support them, so they are omitted from the advertised API.
///
/// The `#[test] description_only_names_bound_methods` below verifies every
/// `ws.<ns>.<method>(` mention here maps to a real dispatch arm in the
/// matching `bindings/<ns>.rs`, preventing silent drift when the description
/// or the bindings change.
pub const WORKSPACE_API_DESCRIPTION: &str = r###"Execute JavaScript against the workspace API. Your code runs as an async function — use `return` to send results back.

Rules:
  - Your code is wrapped in `(async () => { ... })()` before execution.
  - Use `await` for async calls. Use `return` to send the final value back.
  - Use `Promise.all([...])` for independent reads/writes in one tool call.
  - Use `Promise.allSettled([...])` when you need partial results even if some calls fail.
  - Errors from invalid method names or bad arguments are returned directly.
  - Do not add code comments — the code is executed and discarded, never read by humans.

Parameters:
  code (required): JavaScript code to execute.
  summary (required): Short description of what this call does, shown in the UI.

API:
  ws.workspace.info() → { id, path }  // Current workspace ID + absolute path.
  ws.workspace.details() → { id, title, hasTitle, status, statusMessage, statusImageAssetId, branch, repositoryName, tags }  // Workspace metadata; `status` is the lifecycle enum and `statusMessage` is the user-facing work summary.
  ws.workspace.setTitle(title) → { ok, title, branch, skipped? }  // Set a short 1-5 word workspace title. May rename the branch if it is still auto-generated; returns `skipped` if the workspace already has a custom title.
  ws.workspace.setStatusMessage(message) → { ok, statusMessage }  // Set or clear the 1-2 sentence user-facing workspace status message; does not change lifecycle `status` or task statuses. Pass an empty string or null to clear.
  ws.workspace.setStatusImage({ data, mimeType, originalName? } | null) → { ok, statusImageAssetId, url? }  // Set or clear the workspace status screenshot shown on the workspace card. `data` is base64 image bytes (a `data:` URL prefix is accepted), `mimeType` must be image/*. Pass null to clear. Unavailable in the chief-of-staff workspace.
  ws.workspace.setAgentName(name) → { ok, name }  // Rename the current agent session. Call this early in your first response and use a short 1-5 word task-focused name.
  ws.workspace.archive() → { ok, status, archivedAt }  // Archive the current workspace. ONLY call this on explicit user request (same convention as user-requested commits). Refuses if other agents are running or queued (no override); unavailable in the chief-of-staff workspace.
  ws.workspace.unarchive() → { ok, status }  // Unarchive the current workspace. ONLY call this on explicit user request. Unavailable in the chief-of-staff workspace.

  ws.app.question.ask({ question, header, options, explanation?, multiSelect? }) → { ok, attachmentId, message }  // Ask the user ONE structured clarifying question. Call once per question (aim for at most ~4 questions per turn); `header` is a short topic label; `options` is 2-4 choices [{ label, description? }] — at least 2 required; do NOT add an "Other" option, a free-form answer is always offered automatically; `multiSelect: true` lets the user pick several. Questions are presented when your turn ends; the answers arrive as plain-text Q:/A: pairs in the next user message ("(skipped)" for skipped questions). Ask all your questions, then finish the turn.

  ws.note.read(id) → { id, title, content, tags, ... }  // Read a note. Use id=`spec` for the workspace spec. Content has line numbers like `   1 | text`.
  ws.note.create(title, content, tags?) → { id, title, tags, link, markdownLink }  // Create a new note and return canonical `intent://local/{workspaceId}/note/{noteId}` links. Share `markdownLink` with users so they can open the note. DO NOT use this for the spec: the spec already exists as note ID `spec`; edit or add to it instead.
  ws.note.list(tag?) → [{ id, title, tags, ... }]  // List notes. Optional tag filter narrows results.
  ws.note.listTasks(id) → [{ text, status, taskNoteId, linkedTaskNoteId, lineNumber, ... }]  // Faster than `read()` when you only need checkbox/task IDs. Use `taskNoteId` for delegation; `linkedTaskNoteId` is a backward-compatible alias.
  ws.note.readAsset(asset) → { assetId, mimeType, data, sizeKb }  // `asset` can be an asset ID or `workspace-asset://...` URL. Image assets (PNG, JPEG, GIF, WebP) are returned as native image content blocks (the model sees the image directly); non-image assets return the JSON object.
  ws.note.setContent(id, content, confirmReplacement?) → { ... }  // ⚠️ FULL REPLACEMENT: replaces the entire note. Prefer `add()` / `edit()` / `editLines()` unless you intentionally want to overwrite everything.
    If the new content is much shorter, call again with `confirmReplacement=true`. ```task blocks auto-convert into linked task notes.```
  ws.note.add(id, { content, heading?, position? }) → { ... }  // Safest way to add information without losing existing content. Prefer this when asked to "add", "put", "document", or "include" something.
    `position` can be `"end"` (default), `"start"`, or `"after:## Heading"` such as `"after:## Phase 1"`.
  ws.note.edit(id, { old, new }) → { ... }  // Surgical text replacement. `old` must match EXACTLY, including whitespace and line breaks; only the first occurrence is replaced.
  ws.note.editLines(id, { start, end, content }) → { ... }  // Line-based replace/delete/insert. `start` and `end` are 1-based and INCLUSIVE. To delete lines, pass `content: ""`. To insert after a line, set `start` and `end` to the same line and include both the original line and new lines in `content`.
  ws.note.updateMetadata(id, { title?, tags? }) → { ... }  // Safest way to change only title/tags; content is untouched. The spec note title is always `Spec` and cannot be changed.
  ws.note.delete(id) → { ok, noteId, deleted }  // Permanently removes a note.

  ws.comment.add(noteId, { searchContext, commentTarget, comment, type?, author?, authorType? }) → { ... }  // Anchor a comment by text search. Use enough `searchContext` to be unique; `commentTarget` must be a substring inside it. `authorType` is `"user"` or `"agent"` (default `"agent"`).
    Search is case- and whitespace-sensitive. You can use the same text for both fields to comment on an entire phrase, and anchor errors explain how to fix mismatches.
  ws.comment.list(noteId, { since?, authorType?, status?, includeComments? }) → [threads]  // Thread summaries grouped by latest activity. Great for agents finding open threads where the user commented last.
    Example filter combo: `{ since: "<timestamp>", authorType: "user", status: "open", includeComments: true }`.
  ws.comment.getThread(noteId, { threadId?, commentId? }) → thread  // Fetch one full thread with replies in order.
  ws.comment.respond(noteId, { threadId?, commentId?, comment, type?, author?, authorType?, suggestionOriginal?, suggestionProposed? }) → { ... }  // Recommended way to reply: it reuses the parent anchor automatically, so you do not need to search for text again.
    `type` can be `"comment"`, `"suggestion"`, `"question"`, or `"change-request"`. For suggestions, pass both `suggestionOriginal` and `suggestionProposed`.
  ws.comment.delete(noteId, commentId) → { ... }  // Deletes a single comment by ID.

  ws.task.updateStatus(noteId, taskText, status) → { ok, noteId, status, note }  // Atomically change one checkbox status by task text. Prefer this over `note.setContent()` when marking tasks done/in progress to avoid conflicts.
    `status`: `"done"`, `"todo"`, or `"in-progress"`. `taskText` must match the checkbox text exactly.
  ws.task.updateNoteStatus(noteId, status) → { ok, noteId, status }  // Task-note metadata status. Values include `"not_started"`, `"waiting"`, `"discussion_needed"`, `"blocked"`, `"in_progress"`, `"review_required"`, `"complete"`, `"cancelled"`.
  ws.task.update(noteId, line, { text?, status?, expected? }) → { ok, lineNumber, ... }  // Atomically edit only one checkbox line, preserving the rest of the note. Prefer this over `note.setContent()` for task edits.
    `line` is the 1-based task line number from `note.read()`. `status`: `"done"`, `"todo"`, or `"in-progress"`. `expected` enables conflict detection if another agent may have changed the task.
  ws.task.getMyTask(taskNoteId) → task  // Reads a task note with metadata, dependencies, and acceptance criteria.
  ws.task.markAsTask(noteId, status, { acceptanceCriteria?, effort? }) → { ... }  // Convert a note into a task note. `acceptanceCriteria` may be an array or JSON string; `effort` maps to estimated effort.
  ws.task.convertBlocks(noteId) → { convertedCount, createdNoteIds }  // Convert ```task blocks into linked task notes. Note updates already auto-convert them; use this for manual re-conversion.
  ws.task.createPrerequisite(dependentNoteId, title, { content?, status? }) → { ... }  // Adds a prerequisite task dependency.
  ws.task.assignAgent(noteId, agentId) → { ok, noteId, agentId }  // Assign an existing agent to a task note. `agentId` must be `agent-{uuid}`; to create and assign in one step, use `ws.agent.create(..., { taskNoteId: noteId })`.

  ws.primitive.addReference(noteId, semanticId, description, snapshot?) → { ok, primitiveId, noteId }  // Code reference primitive; `semanticId` examples: `src/file.ts#symbol:Foo` or `src/file.ts#L10-20`.
  ws.primitive.addCli(noteId, command, description, workingDirectory?) → { ok, primitiveId, noteId }  // CLI primitive; optional cwd is relative to workspace root.
  ws.primitive.addPatch(noteId, filePath, diff, description) → { ok, primitiveId, noteId }  // Stores a patch block that can be applied in a note.
  ws.primitive.addAgentAction(noteId, agentId, goal, description) → { ok, primitiveId, noteId }  // Adds a triggerable agent action block.

  ws.agent.create(name, message, opts?) → { ok, id?, text?, ... }  // Create and start an agent immediately. You are auto-subscribed to its completion events and will be woken when it finishes.
    Specialists include `"implementor"` for implementation work and `"verifier"` for review/verification. `createLinkedNote=true` with `noteContent` creates a linked note; agents are background by default unless `isBackground=false`.
    You can override specialist defaults with `model` or `behaviorPrompt`.
    Pass `mergeOnTurnEnd: false` to keep a sandboxed agent's sandbox unmerged when its turn ends — inspect it and merge later (fan out several sandboxed agents, then pick); default true auto-merges on completion.
  ws.agent.delegate({ taskNoteId?, noteId?, taskText?, agentInstructions?, specialist?, model?, behaviorPrompt?, waitMode?, skipAutoCommit?, mergeOnTurnEnd? }) → { ok, text?, ... }  // Delegate an existing task to a new agent. Prefer `taskNoteId` from `intent://local/task/{id}`; otherwise pass `noteId` + exact `taskText` from a checkbox.
    Delegation starts immediately and auto-subscribes you to completion events. `waitMode`: `"immediate"` wakes after each agent, `"after_all"` wakes after the whole group. Example: `taskNoteId: "abc-123"`.
    Pass `mergeOnTurnEnd: false` to keep a sandboxed agent's sandbox unmerged when its turn ends — inspect it and merge later (fan out several sandboxed agents, then pick); default true auto-merges on completion.
  ws.agent.send(agentId, message, priority?) → { ok, agentId, ... }  // Send a message to another agent. `priority="interrupt"` stops the target mid-response and delivers the message immediately.
  ws.agent.sendToTask(taskNoteId, message, priority?) → { ok, taskNoteId, ... }  // Follow up with the agent assigned to a task note; more convenient than `send()` when you only know the task note ID. `priority="interrupt"` also stops mid-response.
  ws.agent.subscribe(eventTypes, { excludeSelf?, batchWindow? }) → { subscriptionId, ... }  // Compatibility alias for `ws.event.subscribe()`. `eventTypes` must be an array.
  ws.agent.unsubscribe(subscriptionId) → { ok, subscriptionId }  // Compatibility alias for `ws.event.unsubscribe()`.
  ws.agent.watch(agentId) → { ok, subscriptionId, agentId }  // Watch another agent: you are woken once, at its next completion (it goes idle with an empty pending message queue, fails, or is deleted), and the watch is then retired. Blocker/discussion attention wakes are delivered along the way without ending the watch. Watch again if you care about future turns. A watch adopted into an `after_all` delegation group ends at group settlement and cannot be unwatched while grouped (use `agent.cancelSubscriptions` with the groupId).
  ws.agent.unwatch(subscriptionIdOrAgentId) → { ok, removed }  // Stop watching an agent (accepts the watch's subscriptionId or the watched agentId).
  ws.agent.list(includeCompleted?) → [agents]  // Lists agents in this workspace; completed agents are omitted unless requested. Sandboxed agents carry `metadata.sandboxId`/`sandboxPath`/`sandboxBranch`.
  ws.agent.status(agentId) → agent  // Detailed agent status including task linkage, activity timestamps, and the pending message queue (`queue` + `queueLength`; entries in the getQueue shape with `content` truncated to 200 chars). Sandboxed agents additionally surface `sandboxStatus` and `mergeOnTurnEnd` (sandbox merge state).
  ws.agent.getQueue(agentId) → { ok, agentId, queueLength, queue }  // The agent's full pending message queue in drain order (position 0 = next delivery; interrupt-priority entries first, then normal FIFO; entries under edit are flagged `editing: true` at the end). Each entry: `{ id, content, queuedAt, position, turnId?, interruptPriority?, editing?, fromAgentId?, fromAgentName? }` — attribution absent for user-sent entries.
  ws.agent.removeQueuedMessage(agentId, messageId) → { ok, agentId, messageId }  // Retract YOUR OWN pending message from an agent's queue before delivery. Only messages you sent can be removed; entries from other senders (or the user) are rejected.
  ws.agent.diagnostics({ agentId?, taskNoteId?, includeCompleted?, staleRespondingAfterMs? }?) → { diagnostics, text }  // Sanitized snapshot of agent statuses, subscriptions, queues, delegation groups, delivery stats, recent delivery events, and stuck-risk signals.
  ws.agent.wakeOrCreate(taskNoteId, contextMessage, model?) → { ... }  // Ensure a task has a working agent: checks assigned agents, resumes a running/restorable one if possible, otherwise creates a new agent for the task.
  ws.agent.readConversation(agentId, { lastN?, startTurn?, endTurn?, includeToolCalls? }) → messages  // Read another agent’s conversation history.
  ws.agent.summary(agentId) → summary  // Quick summary of what another agent did.
  ws.agent.reportToParent(report) → { ok, ... }  // Send a concise report on completed or progressing work to the parent agent — if you are blocked or need input, use `ws.agent.reportBlocker`/`ws.agent.requestDiscussion` instead. Only works for delegated agents; user-created agents will get an error.
  ws.agent.requestDiscussion(reason) → { ok, kind, reason, savedAt }  // Raise a pending attention request when you need user/coordinator input to proceed — call it BEFORE ending your turn. `reason` is required. Available to every agent; if you have a linked task it moves to `discussion_needed`.
  ws.agent.reportBlocker(reason) → { ok, kind, reason, savedAt }  // Report an infrastructure/environment problem you cannot resolve (broken sandbox, failing environment, missing credentials) — call it BEFORE ending your turn. `reason` is required. Available to every agent; if you have a linked task it moves to `blocked`.

  ws.git.status() → { modified, staged, untracked, deleted, ... }  // Working tree summary with file lists grouped by status.
  ws.git.stage(paths) → { ok, paths }  // `paths` may be a CSV string or array. Staging all files (`.`, `*`, `--all`) is intentionally blocked; stage only specific files you changed.
  ws.git.commit(message) → { ok, hash?, files? }  // DEPRECATED. Prefer `ws.git.agentCommit()`. This commits already-staged files and still obeys workspace auto-commit policy.
  ws.git.agentCommit(message, { files?, userRequested? }) → { ok, hash, files, fileCount }  // Preferred commit helper. Auto-stages only your changes and is mainly for explicit user-requested checkpoint commits.
    If workspace auto-commit is disabled, set `userRequested=true` to confirm the user asked for the commit.
  ws.git.checkMergeConflicts(targetBranch?) → { hasConflicts, conflictedFiles, targetBranch, currentBranch, ... }  // Checks whether merging into target would conflict.

  ws.event.recentFiles(limit?) → [files]  // Recently modified files. Default limit is 10.
  ws.event.agentActivity(agentId?, minutesAgo?) → [events]  // With `agentId`, narrows to that agent; otherwise returns recent activity window.
  ws.event.workspaceSummary(minutesAgo?) → summary  // Aggregated workspace activity summary.
  ws.event.directoryChanges(dir, limit?) → [changes]  // Recent file changes under one directory prefix.
  ws.event.query({ eventType?, actorType?, actorId?, path?, minutesAgo?, limit? }) → [events]  // Advanced event query filters.
  ws.event.subscribe(eventTypes, { excludeSelf?, batchWindow? }) → { subscriptionId, eventTypes }  // Subscribe to batched workspace events. `eventTypes` must be an array: `["file:*", "task:*"]`. Use explicit categories or event types such as `file:*`, `task:*`, `git:*`, `note:*`, `terminal:*`, `test:*`, `build:*`, `workspace:*`, `spec:*`, `goal:*`, `comment:*`.
    Prefer explicit categories over bare `*`; `excludeSelf` defaults to true and `batchWindow` defaults to 500ms. `agent:*` events are not subscribable — use `ws.agent.watch(agentId)` to be woken when another agent completes, fails, or raises a blocker/discussion.
  ws.event.unsubscribe(subscriptionId) → { ok, subscriptionId }  // Removes one event subscription.

  ws.script.list() → [scripts]  // Lists saved scripts with runtime status when available.
  ws.script.create(name, command, mode, { cwd?, env?, category?, autoStart?, scriptId? }) → { id }  // Create or update a saved script. `mode="service"` is for long-running auto-restart processes; `mode="command"` runs once to completion.
  ws.script.remove(scriptId) → { ok, scriptId }  // Stops and removes a saved script definition.
  ws.script.start(scriptId) → { ok, scriptId }  // Starts an existing script.
  ws.script.stop(scriptId) → { ok, scriptId }  // Stops a running script.
  ws.script.restart(scriptId) → { ok, scriptId }  // Stops then restarts a script.
  ws.script.output(scriptId, maxLines?) → string  // Returns recent output buffer text.
  ws.script.status(scriptId) → status  // Runtime state, pid, exit code, detected URL, timings.
  ws.script.run(scriptId, { maxLines?, timeoutSeconds? }) → { exitCode?, output, timedOut?, warning? }  // Run a command-mode script and wait for it to finish. Use this for builds/tests/linting, not long-running services.
    `timeoutSeconds` defaults to 30. If the timeout is hit, it returns partial output with `timedOut=true`. For service-mode scripts it returns a warning telling you to use `ws.script.start()` instead.

  ws.host.exec({ command, args?, cwd?, env?, timeoutMs? }) → { stdout, stderr, exitCode, timedOut? }  // One-shot process exec on the daemon host. `command` + `args` are argv (no shell interpolation); `cwd` is resolved against and contained within the workspace root; `timeoutMs` (max 600000) kills the whole process group on expiry (`timedOut: true`). For long-running or streaming processes use `ws.script.*` / terminals instead.

  ws.hook.schedule({ name, code, delayMs, ttlMs? }) → { hook, dispatched }  // Register a background hook: a small JS script the daemon runs every `delayMs` ms (min 10000) until it returns `{ dispatch: true, message }` (you are woken with the message and the hook ends), throws/times out (evicted, you are woken with the error), is cancelled, or expires. `name` ≤ 19 chars. The first run happens immediately as validation: a failure rejects the call, a dispatch wakes you right away (`dispatched: true`) without persisting a schedule.
    The script runs with this same `ws.*` API available and a 60s budget per run. Return `{ dispatch: false }` or nothing to keep watching. Use hooks to watch for conditions (CI results, file changes) instead of blocking or polling in your own turn — idle turns time out after ~30 minutes of silence, so hooks are how to wait for slow external conditions.
    Carry state between runs: a returned `state` field (any JSON value, ~16 KiB cap) persists and is injected into the next run as the `hookState` global (`null` on the first run); omit `state` to keep the previous value, return `state: null` to clear it.
    Every hook has a TTL counted from creation: `ttlMs` defaults to and is capped at 3600000 (60 minutes; values are clamped into [10000, 3600000]), persisted as `expiresAt` on the hook. When the TTL elapses the hook expires (terminal state `expired`; a run already in flight completes normally, and its dispatch still wins) and you are woken so you can schedule a new hook if the condition is still worth watching. Set `ttlMs` to your estimated time-to-fire plus reasonable margin rather than defaulting to the cap, so expiry doubles as an "overdue — reassess" wake.
  ws.hook.list() → [hooks]  // Hooks in this workspace with `hookId`, `name`, `state` (scheduled|running|dispatched|evicted|cancelled|expired), `nextRunAt`, `expiresAt` (TTL deadline, ≤ 60 min from creation), `runCount`, `lastError?`, `lastState?` (the carry-over state JSON from the most recent run).
  ws.hook.cancel(hookId) → { ok, hook }  // Stop one of your active hooks.
  ws.hook.runNow(hookId) → { ok, hookId }  // Trigger an immediate run of an active hook; its inter-run timer resets after the run.

  ws.browser.exec(actions, tabId?) → result | results[]  // Chrome DevTools browser automation. Each action is an object with an `action` field; common actions include `listTabs`, `focusTab`, `getAccessibilityTree`, `screenshot`, `evaluate`, `navigate`, `openTab`, `snapshot`, and capture/trace actions.
    Single-action calls return one result; multiple actions return an array. Use `ws.browser.docs("overview"|"capture"|"examples")` for the full action reference, `waitFor` options, and longer examples.
  ws.browser.docs(topic) → string  // Browser API docs. Topics include `overview`, `capture`, and `examples`.

  ws.terminal.list() → [terminals]  // Active workspace terminal sessions.
  ws.terminal.readOutput(terminalId, maxLines?) → string  // Read a terminal output buffer. Use `ws.terminal.list()` first to discover terminal IDs.

  ws.crossWorkspace.listSiblings() → [workspaces]  // Other workspaces sharing the same repository (repo-scoped).
  ws.crossWorkspace.readNote(targetWorkspaceId, noteId) → note  // Read a note from another sibling workspace in the same repository. Use `listSiblings()` first to discover valid workspace IDs; use noteId=`spec` for its spec.
  ws.crossWorkspace.listNotes(targetWorkspaceId) → [notes]  // List notes in another sibling workspace. Use this before `readNote()` if you do not know which note IDs exist there.

  ws.file.read(path) → string  // Read an actual project file relative to workspace root. Do not use this for notes/spec content; use `ws.note.read()` for workspace notes. Paths outside the workspace are rejected.
  ws.file.write(path, content) → { ok, path, size }  // Writes/creates a file inside the workspace and records attribution.
  ws.file.list(path?) → [{ name, type }]  // Lists files/directories. Default path is `.`.
  ws.file.delete(path) → { ok, path, deleted }  // Deletes a file. Directories must use other tooling.
  ws.file.mkdir(path) → { ok, path, created?|existed? }  // Creates a directory inside the workspace.
  ws.file.rename(oldPath, newPath) → { ok, oldPath, newPath }  // Renames/moves a file or directory inside the workspace.

  ws.pr.merge({ mergeMethod?, commitTitle?, commitMessage? }?) → { merged, sha, mergeMethod, message, prNumber }  // Requires an active PR. `mergeMethod`: `"merge"`, `"squash"`, or `"rebase"`.
  ws.pr.status() → { prNumber, title, url, state, mergeable, mergeableState, hasConflicts, isDraft, isMerged, isClosed, summary }  // Requires an active PR.
  ws.pr.snapshot(prNumber) → { prNumber, title, url, state, isDraft, isMerged, isClosed, headSha, updatedAt, mergeable, mergeableState, mergeBlockedReason, checks: { total, passed, failed, pending, failedNames }, reviews: { decision, approvals, changesRequested }, comments: { conversationCount, reviewCommentCount, unresolvedThreadCount, totalCount } }  // Compact, diff-friendly snapshot of PR `prNumber` in the workspace repo. `prNumber` is required — no active-PR fallback.
    Use this to monitor a PR: schedule a hook that diffs the snapshot against the previous one in hookState and dispatches on meaningful change (new comments incl. thread replies, failed checks, mergeBlockedReason, review decision) — or diff isMerged alone if merging is all the user cares about.
  ws.pr.updateBranch() → { ... }  // Updates the PR branch from its base branch when supported.
  ws.pr.listReviewComments({ path?, status? }?) → reviewComments  // Inline code review comments (attached to specific lines in a diff). `status`: `"unresolved"`, `"resolved"`, or `"all"`.
  ws.pr.replyToReviewComment(commentId, body) → { ... }  // Reply to an inline review comment by numeric ID.
  ws.pr.resolveThread(threadId, action?) → { ... }  // `action`: `"resolve"` or `"unresolve"`.
  ws.pr.listComments({ count? }?) → comments  // Lists conversation-level PR comments (not inline code comments).
  ws.pr.postComment(body) → { ... }  // Posts a conversation-level PR comment.

Examples (the final one shows the N+1 pattern: list items first, then batch-read their details in a single Promise.all):
  return await ws.workspace.info()

  const [spec, tasks, agents] = await Promise.all([
    ws.note.read("spec"),
    ws.note.listTasks("spec"),
    ws.agent.list(),
  ])
  return { specTitle: spec.title, taskCount: tasks.length, agentCount: agents.length }

  const note = await ws.note.read("spec")
  if (!note.content.includes("## Phase 2")) {
    await ws.note.add("spec", { heading: "## Phase 2", content: "Draft plan", position: "end" })
  }
  return await ws.workspace.details()

  const tasks = await ws.note.listTasks("spec")
  const taskNoteIds = tasks.filter(t => t.taskNoteId).map(t => t.taskNoteId)
  const taskNotes = await Promise.all(taskNoteIds.map(id => ws.task.getMyTask(id)))
  return taskNotes.map(t => ({ id: t.noteId, title: t.title, status: t.status }))
"###;

/// Chief-workspace variant of [`WORKSPACE_API_DESCRIPTION`]: includes the
/// full `ws.app.*` surface (agents, settings, specialists, ui, workspaces).
/// Reference wording from `workspace-js-api-tool.ts` lines 58–78.
pub const WORKSPACE_API_DESCRIPTION_CHIEF: &str = r###"Execute JavaScript against the workspace API. Your code runs as an async function — use `return` to send results back.

Rules:
  - Your code is wrapped in `(async () => { ... })()` before execution.
  - Use `await` for async calls. Use `return` to send the final value back.
  - Use `Promise.all([...])` for independent reads/writes in one tool call.
  - Use `Promise.allSettled([...])` when you need partial results even if some calls fail.
  - Errors from invalid method names or bad arguments are returned directly.
  - Do not add code comments — the code is executed and discarded, never read by humans.

Parameters:
  code (required): JavaScript code to execute.
  summary (required): Short description of what this call does, shown in the UI.

API:
  ws.workspace.info() → { id, path }  // Current workspace ID + absolute path.
  ws.workspace.details() → { id, title, hasTitle, status, statusMessage, statusImageAssetId, branch, repositoryName, tags }  // Workspace metadata; `status` is the lifecycle enum and `statusMessage` is the user-facing work summary.
  ws.workspace.setTitle(title) → { ok, title, branch, skipped? }  // Set a short 1-5 word workspace title. May rename the branch if it is still auto-generated; returns `skipped` if the workspace already has a custom title.
  ws.workspace.setStatusMessage(message) → { ok, statusMessage }  // Set or clear the 1-2 sentence user-facing workspace status message; does not change lifecycle `status` or task statuses. Pass an empty string or null to clear.
  ws.workspace.setStatusImage({ data, mimeType, originalName? } | null) → { ok, statusImageAssetId, url? }  // Set or clear the workspace status screenshot shown on the workspace card. `data` is base64 image bytes (a `data:` URL prefix is accepted), `mimeType` must be image/*. Pass null to clear. Unavailable in the chief-of-staff workspace.
  ws.workspace.setAgentName(name) → { ok, name }  // Rename the current agent session. Call this early in your first response and use a short 1-5 word task-focused name.
  ws.workspace.archive() → { ok, status, archivedAt }  // Archive the current workspace. ONLY call this on explicit user request (same convention as user-requested commits). Refuses if other agents are running or queued (no override); unavailable in the chief-of-staff workspace.
  ws.workspace.unarchive() → { ok, status }  // Unarchive the current workspace. ONLY call this on explicit user request. Unavailable in the chief-of-staff workspace.

  ws.app.agents.list({ workspaceId?, includeCompleted?, limit?, cursor? }?) → { threads, total, returned, nextCursor? }  // Chief workspace only. Lists readable agent threads across app workspaces; metadata only, no transcript content. Defaults to 50 threads, max 200.
  ws.app.agents.readConversation(workspaceId, agentId, { lastN?, startTurn?, endTurn?, includeToolCalls? }?) → { workspaceId, workspaceTitle, agentId, agentName, totalMessages, returnedMessages, startTurn, endTurn, includeToolCalls, taskNoteId?, messages }  // Chief workspace only. Reads a bounded cross-workspace agent conversation. Defaults to last 20 messages, max 100, and excludes tool-call blocks unless `includeToolCalls=true`.
    Safe usage: list first, then read only the relevant thread slices with `lastN` or `startTurn`/`endTurn`; keep `includeToolCalls` false unless the user explicitly needs raw tool-call details.
  ws.app.agents.waitFor({ agentIds, waitMode? }) → { ok, waitMode, results }  // Chief workspace only. Register to be woken when existing agents (in any workspace) complete — the subscription side of `agent.delegate` without creating agents. `waitMode`: `"immediate"` (default) wakes you as each agent completes; `"after_all"` delivers one aggregated wake once all of them settle. Each result is { agentId, agentName, workspaceId, subscriptionId, groupId }.
  ws.app.proposal.show(proposal) → ProposalCard  // Chief workspace only. Render an app-level proposal card in chat.
  ws.app.question.ask({ question, header, options, explanation?, multiSelect? }) → { ok, attachmentId, message }  // Ask the user ONE structured clarifying question. Call once per question (aim for at most ~4 questions per turn); `header` is a short topic label; `options` is 2-4 choices [{ label, description? }] — at least 2 required; do NOT add an "Other" option, a free-form answer is always offered automatically; `multiSelect: true` lets the user pick several. Questions are presented when your turn ends; the answers arrive as plain-text Q:/A: pairs in the next user message ("(skipped)" for skipped questions). Ask all your questions, then finish the turn.
  ws.app.settings.list({ includeValues?, category? }?) → settings[]  // List schema-backed persisted user settings, optionally with current values.
  ws.app.settings.get(path) → setting  // Read a persisted user setting by schema path; sensitive values are redacted.
  ws.app.settings.propose(changes[] | { changes }) → ProposalCard  // Preview settings changes with a diff; never auto-applies.
  ws.app.specialists.list() → specialists[]  // List app-level specialists with id, name, description, model, prompt, and source metadata.
  ws.app.specialists.get(id) → specialist  // Get one app-level specialist by ID; throws a clear not-found error when missing.
  ws.app.specialists.propose({ action: "create"|"edit"|"delete", id?, name?, description?, model?, prompt?, scope? }) → ProposalCard  // Render a specialist-edit proposal with editable name/description/model/prompt fields.
  ws.app.ui.navigate(route, { highlightId?, durationMs? }?) → { ok, route, workspaceId, highlightId?, durationMs? }  // Navigate the app UI via the renderer router. If highlightId is omitted, the URL hash is used when present.
  ws.app.ui.highlight(id, { durationMs? }?) → { ok, id, workspaceId, durationMs? }  // Pulse a registered highlight target using the UI highlight system.
  ws.app.ui.targets() → [{ id, label, route, tab, category, description, dynamic?, idPattern?, hashAliases?, scrollSelector?, highlightSelector? }]  // Discover typed app UI targets and highlight ID patterns.
  ws.app.workspaces.archive(id) → ProposalCard  // Chief workspace only. Proposes archive of a single workspace via ws.app.proposal.show; the user confirms before applying.
  ws.app.workspaces.bulkArchive(ids) → ProposalCard  // Chief workspace only. Proposes bulk archive via ws.app.proposal.show.
  ws.app.workspaces.bulkDelete(ids) → ProposalCard  // Chief workspace only. Proposes bulk delete via ws.app.proposal.show.
  ws.app.workspaces.create(params) → ProposalCard  // Chief workspace only. Proposes workspace creation via ws.app.proposal.show; does not create directly. Key params: `title?`, `repositoryPath?` (local clone path), `githubUrl?` (PR/issue URL), `branch?`/`baseRef?`, `initialPrompt?`, `specialist?`.
    `branch`/`baseRef` is the EXISTING base ref to branch FROM (e.g. a PR head branch or a branch the user named) — NOT a name for the new working branch. Omit it and the daemon defaults it to the repository's default branch; a non-existent ref fails at apply with a `cannot resolve base ref '<ref>'` error.
  ws.app.workspaces.delete(id) → ProposalCard  // Chief workspace only. Proposes delete of a single workspace via ws.app.proposal.show; the user confirms before applying.
  ws.app.workspaces.get(id) → workspace  // Chief workspace only. Get one workspace metadata summary.
  ws.app.workspaces.list({ filter?, sort? }) → workspaces[]  // Chief workspace only. Cross-workspace metadata list with query/status/repository/tags filtering.
  ws.app.workspaces.open(id, { openInNewWindow? }?) → { ok, queued }  // Chief workspace only. Opens a workspace through workspace-operations-saga. Pass `{ openInNewWindow: true }` to open in a new window.

  ws.note.read(id) → { id, title, content, tags, ... }  // Read a note. Use id=`spec` for the workspace spec. Content has line numbers like `   1 | text`.
  ws.note.create(title, content, tags?) → { id, title, tags, link, markdownLink }  // Create a new note and return canonical `intent://local/{workspaceId}/note/{noteId}` links. Share `markdownLink` with users so they can open the note. DO NOT use this for the spec: the spec already exists as note ID `spec`; edit or add to it instead.
  ws.note.list(tag?) → [{ id, title, tags, ... }]  // List notes. Optional tag filter narrows results.
  ws.note.listTasks(id) → [{ text, status, taskNoteId, linkedTaskNoteId, lineNumber, ... }]  // Faster than `read()` when you only need checkbox/task IDs. Use `taskNoteId` for delegation; `linkedTaskNoteId` is a backward-compatible alias.
  ws.note.readAsset(asset) → { assetId, mimeType, data, sizeKb }  // `asset` can be an asset ID or `workspace-asset://...` URL. Image assets (PNG, JPEG, GIF, WebP) are returned as native image content blocks (the model sees the image directly); non-image assets return the JSON object.
  ws.note.setContent(id, content, confirmReplacement?) → { ... }  // ⚠️ FULL REPLACEMENT: replaces the entire note. Prefer `add()` / `edit()` / `editLines()` unless you intentionally want to overwrite everything.
    If the new content is much shorter, call again with `confirmReplacement=true`. ```task blocks auto-convert into linked task notes.```
  ws.note.add(id, { content, heading?, position? }) → { ... }  // Safest way to add information without losing existing content. Prefer this when asked to "add", "put", "document", or "include" something.
    `position` can be `"end"` (default), `"start"`, or `"after:## Heading"` such as `"after:## Phase 1"`.
  ws.note.edit(id, { old, new }) → { ... }  // Surgical text replacement. `old` must match EXACTLY, including whitespace and line breaks; only the first occurrence is replaced.
  ws.note.editLines(id, { start, end, content }) → { ... }  // Line-based replace/delete/insert. `start` and `end` are 1-based and INCLUSIVE. To delete lines, pass `content: ""`. To insert after a line, set `start` and `end` to the same line and include both the original line and new lines in `content`.
  ws.note.updateMetadata(id, { title?, tags? }) → { ... }  // Safest way to change only title/tags; content is untouched. The spec note title is always `Spec` and cannot be changed.
  ws.note.delete(id) → { ok, noteId, deleted }  // Permanently removes a note.

  ws.comment.add(noteId, { searchContext, commentTarget, comment, type?, author?, authorType? }) → { ... }  // Anchor a comment by text search. Use enough `searchContext` to be unique; `commentTarget` must be a substring inside it. `authorType` is `"user"` or `"agent"` (default `"agent"`).
    Search is case- and whitespace-sensitive. You can use the same text for both fields to comment on an entire phrase, and anchor errors explain how to fix mismatches.
  ws.comment.list(noteId, { since?, authorType?, status?, includeComments? }) → [threads]  // Thread summaries grouped by latest activity. Great for agents finding open threads where the user commented last.
    Example filter combo: `{ since: "<timestamp>", authorType: "user", status: "open", includeComments: true }`.
  ws.comment.getThread(noteId, { threadId?, commentId? }) → thread  // Fetch one full thread with replies in order.
  ws.comment.respond(noteId, { threadId?, commentId?, comment, type?, author?, authorType?, suggestionOriginal?, suggestionProposed? }) → { ... }  // Recommended way to reply: it reuses the parent anchor automatically, so you do not need to search for text again.
    `type` can be `"comment"`, `"suggestion"`, `"question"`, or `"change-request"`. For suggestions, pass both `suggestionOriginal` and `suggestionProposed`.
  ws.comment.delete(noteId, commentId) → { ... }  // Deletes a single comment by ID.

  ws.task.updateStatus(noteId, taskText, status) → { ok, noteId, status, note }  // Atomically change one checkbox status by task text. Prefer this over `note.setContent()` when marking tasks done/in progress to avoid conflicts.
    `status`: `"done"`, `"todo"`, or `"in-progress"`. `taskText` must match the checkbox text exactly.
  ws.task.updateNoteStatus(noteId, status) → { ok, noteId, status }  // Task-note metadata status. Values include `"not_started"`, `"waiting"`, `"discussion_needed"`, `"blocked"`, `"in_progress"`, `"review_required"`, `"complete"`, `"cancelled"`.
  ws.task.update(noteId, line, { text?, status?, expected? }) → { ok, lineNumber, ... }  // Atomically edit only one checkbox line, preserving the rest of the note. Prefer this over `note.setContent()` for task edits.
    `line` is the 1-based task line number from `note.read()`. `status`: `"done"`, `"todo"`, or `"in-progress"`. `expected` enables conflict detection if another agent may have changed the task.
  ws.task.getMyTask(taskNoteId) → task  // Reads a task note with metadata, dependencies, and acceptance criteria.
  ws.task.markAsTask(noteId, status, { acceptanceCriteria?, effort? }) → { ... }  // Convert a note into a task note. `acceptanceCriteria` may be an array or JSON string; `effort` maps to estimated effort.
  ws.task.convertBlocks(noteId) → { convertedCount, createdNoteIds }  // Convert ```task blocks into linked task notes. Note updates already auto-convert them; use this for manual re-conversion.
  ws.task.createPrerequisite(dependentNoteId, title, { content?, status? }) → { ... }  // Adds a prerequisite task dependency.
  ws.task.assignAgent(noteId, agentId) → { ok, noteId, agentId }  // Assign an existing agent to a task note. `agentId` must be `agent-{uuid}`; to create and assign in one step, use `ws.agent.create(..., { taskNoteId: noteId })`.

  ws.primitive.addReference(noteId, semanticId, description, snapshot?) → { ok, primitiveId, noteId }  // Code reference primitive; `semanticId` examples: `src/file.ts#symbol:Foo` or `src/file.ts#L10-20`.
  ws.primitive.addCli(noteId, command, description, workingDirectory?) → { ok, primitiveId, noteId }  // CLI primitive; optional cwd is relative to workspace root.
  ws.primitive.addPatch(noteId, filePath, diff, description) → { ok, primitiveId, noteId }  // Stores a patch block that can be applied in a note.
  ws.primitive.addAgentAction(noteId, agentId, goal, description) → { ok, primitiveId, noteId }  // Adds a triggerable agent action block.

  ws.agent.create(name, message, opts?) → { ok, id?, text?, ... }  // Create and start an agent immediately. You are auto-subscribed to its completion events and will be woken when it finishes.
    Specialists include `"implementor"` for implementation work and `"verifier"` for review/verification. `createLinkedNote=true` with `noteContent` creates a linked note; agents are background by default unless `isBackground=false`.
    You can override specialist defaults with `model` or `behaviorPrompt`.
    Pass `mergeOnTurnEnd: false` to keep a sandboxed agent's sandbox unmerged when its turn ends — inspect it and merge later (fan out several sandboxed agents, then pick); default true auto-merges on completion.
  ws.agent.delegate({ taskNoteId?, noteId?, taskText?, agentInstructions?, specialist?, model?, behaviorPrompt?, waitMode?, skipAutoCommit?, mergeOnTurnEnd? }) → { ok, text?, ... }  // Delegate an existing task to a new agent. Prefer `taskNoteId` from `intent://local/task/{id}`; otherwise pass `noteId` + exact `taskText` from a checkbox.
    Delegation starts immediately and auto-subscribes you to completion events. `waitMode`: `"immediate"` wakes after each agent, `"after_all"` wakes after the whole group. Example: `taskNoteId: "abc-123"`.
    Pass `mergeOnTurnEnd: false` to keep a sandboxed agent's sandbox unmerged when its turn ends — inspect it and merge later (fan out several sandboxed agents, then pick); default true auto-merges on completion.
  ws.agent.send(agentId, message, priority?) → { ok, agentId, ... }  // Send a message to another agent. `priority="interrupt"` stops the target mid-response and delivers the message immediately.
  ws.agent.sendToTask(taskNoteId, message, priority?) → { ok, taskNoteId, ... }  // Follow up with the agent assigned to a task note; more convenient than `send()` when you only know the task note ID. `priority="interrupt"` also stops mid-response.
  ws.agent.subscribe(eventTypes, { excludeSelf?, batchWindow? }) → { subscriptionId, ... }  // Compatibility alias for `ws.event.subscribe()`. `eventTypes` must be an array.
  ws.agent.unsubscribe(subscriptionId) → { ok, subscriptionId }  // Compatibility alias for `ws.event.unsubscribe()`.
  ws.agent.watch(agentId) → { ok, subscriptionId, agentId }  // Watch another agent: you are woken once, at its next completion (it goes idle with an empty pending message queue, fails, or is deleted), and the watch is then retired. Blocker/discussion attention wakes are delivered along the way without ending the watch. Watch again if you care about future turns. A watch adopted into an `after_all` delegation group ends at group settlement and cannot be unwatched while grouped (use `agent.cancelSubscriptions` with the groupId).
  ws.agent.unwatch(subscriptionIdOrAgentId) → { ok, removed }  // Stop watching an agent (accepts the watch's subscriptionId or the watched agentId).
  ws.agent.list(includeCompleted?) → [agents]  // Lists agents in this workspace; completed agents are omitted unless requested. Sandboxed agents carry `metadata.sandboxId`/`sandboxPath`/`sandboxBranch`.
  ws.agent.status(agentId) → agent  // Detailed agent status including task linkage and activity timestamps. Sandboxed agents additionally surface `sandboxStatus` and `mergeOnTurnEnd` (sandbox merge state).
  ws.agent.diagnostics({ agentId?, taskNoteId?, includeCompleted?, staleRespondingAfterMs? }?) → { diagnostics, text }  // Sanitized snapshot of agent statuses, subscriptions, queues, delegation groups, delivery stats, recent delivery events, and stuck-risk signals.
  ws.agent.wakeOrCreate(taskNoteId, contextMessage, model?) → { ... }  // Ensure a task has a working agent: checks assigned agents, resumes a running/restorable one if possible, otherwise creates a new agent for the task.
  ws.agent.readConversation(agentId, { lastN?, startTurn?, endTurn?, includeToolCalls? }) → messages  // Read another agent's conversation history.
  ws.agent.summary(agentId) → summary  // Quick summary of what another agent did.
  ws.agent.reportToParent(report) → { ok, ... }  // Send a concise report on completed or progressing work to the parent agent — if you are blocked or need input, use `ws.agent.reportBlocker`/`ws.agent.requestDiscussion` instead. Only works for delegated agents; user-created agents will get an error.
  ws.agent.requestDiscussion(reason) → { ok, kind, reason, savedAt }  // Raise a pending attention request when you need user/coordinator input to proceed — call it BEFORE ending your turn. `reason` is required. Available to every agent; if you have a linked task it moves to `discussion_needed`.
  ws.agent.reportBlocker(reason) → { ok, kind, reason, savedAt }  // Report an infrastructure/environment problem you cannot resolve (broken sandbox, failing environment, missing credentials) — call it BEFORE ending your turn. `reason` is required. Available to every agent; if you have a linked task it moves to `blocked`.

  ws.git.status() → { modified, staged, untracked, deleted, ... }  // Working tree summary with file lists grouped by status.
  ws.git.stage(paths) → { ok, paths }  // `paths` may be a CSV string or array. Staging all files (`.`, `*`, `--all`) is intentionally blocked; stage only specific files you changed.
  ws.git.commit(message) → { ok, hash?, files? }  // DEPRECATED. Prefer `ws.git.agentCommit()`. This commits already-staged files and still obeys workspace auto-commit policy.
  ws.git.agentCommit(message, { files?, userRequested? }) → { ok, hash, files, fileCount }  // Preferred commit helper. Auto-stages only your changes and is mainly for explicit user-requested checkpoint commits.
    If workspace auto-commit is disabled, set `userRequested=true` to confirm the user asked for the commit.
  ws.git.checkMergeConflicts(targetBranch?) → { hasConflicts, conflictedFiles, targetBranch, currentBranch, ... }  // Checks whether merging into target would conflict.

  ws.event.recentFiles(limit?) → [files]  // Recently modified files. Default limit is 10.
  ws.event.agentActivity(agentId?, minutesAgo?) → [events]  // With `agentId`, narrows to that agent; otherwise returns recent activity window.
  ws.event.workspaceSummary(minutesAgo?) → summary  // Aggregated workspace activity summary.
  ws.event.directoryChanges(dir, limit?) → [changes]  // Recent file changes under one directory prefix.
  ws.event.query({ eventType?, actorType?, actorId?, path?, minutesAgo?, limit? }) → [events]  // Advanced event query filters.
  ws.event.subscribe(eventTypes, { excludeSelf?, batchWindow? }) → { subscriptionId, eventTypes }  // Subscribe to batched workspace events. `eventTypes` must be an array: `["file:*", "task:*"]`. Use explicit categories or event types such as `file:*`, `task:*`, `git:*`, `note:*`, `terminal:*`, `test:*`, `build:*`, `workspace:*`, `spec:*`, `goal:*`, `comment:*`.
    Prefer explicit categories over bare `*`; `excludeSelf` defaults to true and `batchWindow` defaults to 500ms. `agent:*` events are not subscribable — use `ws.agent.watch(agentId)` to be woken when another agent completes, fails, or raises a blocker/discussion.
  ws.event.unsubscribe(subscriptionId) → { ok, subscriptionId }  // Removes one event subscription.

  ws.script.list() → [scripts]  // Lists saved scripts with runtime status when available.
  ws.script.create(name, command, mode, { cwd?, env?, category?, autoStart?, scriptId? }) → { id }  // Create or update a saved script. `mode="service"` is for long-running auto-restart processes; `mode="command"` runs once to completion.
  ws.script.remove(scriptId) → { ok, scriptId }  // Stops and removes a saved script definition.
  ws.script.start(scriptId) → { ok, scriptId }  // Starts an existing script.
  ws.script.stop(scriptId) → { ok, scriptId }  // Stops a running script.
  ws.script.restart(scriptId) → { ok, scriptId }  // Stops then restarts a script.
  ws.script.output(scriptId, maxLines?) → string  // Returns recent output buffer text.
  ws.script.status(scriptId) → status  // Runtime state, pid, exit code, detected URL, timings.
  ws.script.run(scriptId, { maxLines?, timeoutSeconds? }) → { exitCode?, output, timedOut?, warning? }  // Run a command-mode script and wait for it to finish. Use this for builds/tests/linting, not long-running services.
    `timeoutSeconds` defaults to 30. If the timeout is hit, it returns partial output with `timedOut=true`. For service-mode scripts it returns a warning telling you to use `ws.script.start()` instead.

  ws.hook.schedule({ name, code, delayMs, ttlMs? }) → { hook, dispatched }  // Register a background hook: a small JS script the daemon runs every `delayMs` ms (min 10000) until it returns `{ dispatch: true, message }` (you are woken with the message and the hook ends), throws/times out (evicted, you are woken with the error), is cancelled, or expires. `name` ≤ 19 chars. The first run happens immediately as validation: a failure rejects the call, a dispatch wakes you right away (`dispatched: true`) without persisting a schedule.
    The script runs with this same `ws.*` API available and a 60s budget per run. Return `{ dispatch: false }` or nothing to keep watching. Use hooks to watch for conditions (CI results, file changes) instead of blocking or polling in your own turn — idle turns time out after ~30 minutes of silence, so hooks are how to wait for slow external conditions.
    Carry state between runs: a returned `state` field (any JSON value, ~16 KiB cap) persists and is injected into the next run as the `hookState` global (`null` on the first run); omit `state` to keep the previous value, return `state: null` to clear it.
    Every hook has a TTL counted from creation: `ttlMs` defaults to and is capped at 3600000 (60 minutes; values are clamped into [10000, 3600000]), persisted as `expiresAt` on the hook. When the TTL elapses the hook expires (terminal state `expired`; a run already in flight completes normally, and its dispatch still wins) and you are woken so you can schedule a new hook if the condition is still worth watching. Set `ttlMs` to your estimated time-to-fire plus reasonable margin rather than defaulting to the cap, so expiry doubles as an "overdue — reassess" wake.
  ws.hook.list() → [hooks]  // Hooks in this workspace with `hookId`, `name`, `state` (scheduled|running|dispatched|evicted|cancelled|expired), `nextRunAt`, `expiresAt` (TTL deadline, ≤ 60 min from creation), `runCount`, `lastError?`, `lastState?` (the carry-over state JSON from the most recent run).
  ws.hook.cancel(hookId) → { ok, hook }  // Stop one of your active hooks.
  ws.hook.runNow(hookId) → { ok, hookId }  // Trigger an immediate run of an active hook; its inter-run timer resets after the run.

  ws.browser.exec(actions, tabId?) → result | results[]  // Chrome DevTools browser automation. Each action is an object with an `action` field; common actions include `listTabs`, `focusTab`, `getAccessibilityTree`, `screenshot`, `evaluate`, `navigate`, `openTab`, `snapshot`, and capture/trace actions.
    Single-action calls return one result; multiple actions return an array. Use `ws.browser.docs("overview"|"capture"|"examples")` for the full action reference, `waitFor` options, and longer examples.
  ws.browser.docs(topic) → string  // Browser API docs. Topics include `overview`, `capture`, and `examples`.

  ws.terminal.list() → [terminals]  // Active workspace terminal sessions.
  ws.terminal.readOutput(terminalId, maxLines?) → string  // Read a terminal output buffer. Use `ws.terminal.list()` first to discover terminal IDs.

  ws.crossWorkspace.listSiblings() → [workspaces]  // Other workspaces sharing the same repository (repo-scoped).
  ws.crossWorkspace.readNote(targetWorkspaceId, noteId) → note  // Read a note from another sibling workspace in the same repository. Use `listSiblings()` first to discover valid workspace IDs; use noteId=`spec` for its spec.
  ws.crossWorkspace.listNotes(targetWorkspaceId) → [notes]  // List notes in another sibling workspace. Use this before `readNote()` if you do not know which note IDs exist there.

  ws.file.read(path) → string  // Read an actual project file relative to workspace root. Do not use this for notes/spec content; use `ws.note.read()` for workspace notes. Paths outside the workspace are rejected.
  ws.file.write(path, content) → { ok, path, size }  // Writes/creates a file inside the workspace and records attribution.
  ws.file.list(path?) → [{ name, type }]  // Lists files/directories. Default path is `.`.
  ws.file.delete(path) → { ok, path, deleted }  // Deletes a file. Directories must use other tooling.
  ws.file.mkdir(path) → { ok, path, created?|existed? }  // Creates a directory inside the workspace.
  ws.file.rename(oldPath, newPath) → { ok, oldPath, newPath }  // Renames/moves a file or directory inside the workspace.

  ws.pr.merge({ mergeMethod?, commitTitle?, commitMessage? }?) → { merged, sha, mergeMethod, message, prNumber }  // Requires an active PR. `mergeMethod`: `"merge"`, `"squash"`, or `"rebase"`.
  ws.pr.status() → { prNumber, title, url, state, mergeable, mergeableState, hasConflicts, isDraft, isMerged, isClosed, summary }  // Requires an active PR.
  ws.pr.snapshot(prNumber) → { prNumber, title, url, state, isDraft, isMerged, isClosed, headSha, updatedAt, mergeable, mergeableState, mergeBlockedReason, checks: { total, passed, failed, pending, failedNames }, reviews: { decision, approvals, changesRequested }, comments: { conversationCount, reviewCommentCount, unresolvedThreadCount, totalCount } }  // Compact, diff-friendly snapshot of PR `prNumber` in the workspace repo. `prNumber` is required — no active-PR fallback.
    Use this to monitor a PR: schedule a hook that diffs the snapshot against the previous one in hookState and dispatches on meaningful change (new comments incl. thread replies, failed checks, mergeBlockedReason, review decision) — or diff isMerged alone if merging is all the user cares about.
  ws.pr.updateBranch() → { ... }  // Updates the PR branch from its base branch when supported.
  ws.pr.listReviewComments({ path?, status? }?) → reviewComments  // Inline code review comments (attached to specific lines in a diff). `status`: `"unresolved"`, `"resolved"`, or `"all"`.
  ws.pr.replyToReviewComment(commentId, body) → { ... }  // Reply to an inline review comment by numeric ID.
  ws.pr.resolveThread(threadId, action?) → { ... }  // `action`: `"resolve"` or `"unresolve"`.
  ws.pr.listComments({ count? }?) → comments  // Lists conversation-level PR comments (not inline code comments).
  ws.pr.postComment(body) → { ... }  // Posts a conversation-level PR comment.

Examples (the final one shows the N+1 pattern: list items first, then batch-read their details in a single Promise.all):
  return await ws.workspace.info()

  const [spec, tasks, agents] = await Promise.all([
    ws.note.read("spec"),
    ws.note.listTasks("spec"),
    ws.agent.list(),
  ])
  return { specTitle: spec.title, taskCount: tasks.length, agentCount: agents.length }

  const note = await ws.note.read("spec")
  if (!note.content.includes("## Phase 2")) {
    await ws.note.add("spec", { heading: "## Phase 2", content: "Draft plan", position: "end" })
  }
  return await ws.workspace.details()

  const tasks = await ws.note.listTasks("spec")
  const taskNoteIds = tasks.filter(t => t.taskNoteId).map(t => t.taskNoteId)
  const taskNotes = await Promise.all(taskNoteIds.map(id => ws.task.getMyTask(id)))
  return taskNotes.map(t => ({ id: t.noteId, title: t.title, status: t.status }))
"###;

/// The full tool registry — after WSAPI-8, the daemon exposes a single MCP
/// tool. Agents access the workspace surface exclusively through
/// `workspace_api` (agent-supplied JS against the `ws.*` bindings); every
/// discrete tool that used to live here was removed in the cutover.
static ALL_TOOLS: &[ToolDef] = &[ToolDef {
    name: "workspace_api",
    description: WORKSPACE_API_DESCRIPTION,
    params: &[p("code", "string", true), p("summary", "string", true)],
}];

/// The chief-workspace tool registry variant: same structure as [`ALL_TOOLS`],
/// but with the chief description that advertises the full `ws.app.*` surface.
static ALL_TOOLS_CHIEF: &[ToolDef] = &[ToolDef {
    name: "workspace_api",
    description: WORKSPACE_API_DESCRIPTION_CHIEF,
    params: &[p("code", "string", true), p("summary", "string", true)],
}];

/// The `ws.` path prefixes gated by each disabled `[agentFeatures]` toggle.
/// Namespace-level prefixes end with `.`; method-level prefixes (the
/// `attentionRequests` pair) name one full method each. Shared by the
/// description assembler below, the prelude assembler in [`super::bindings`],
/// and the dispatch deny in [`super::bindings`] (via [`denied_feature`]), so
/// the three layers cannot drift.
fn gated_prefixes(features: &AgentFeaturesSettings) -> Vec<(&'static str, &'static str)> {
    let mut out = Vec::new();
    if !features.background_hooks {
        out.push(("ws.hook.", "agentFeatures.backgroundHooks"));
    }
    if !features.host_exec {
        out.push(("ws.host.", "agentFeatures.hostExec"));
    }
    if !features.scripts {
        out.push(("ws.script.", "agentFeatures.scripts"));
    }
    if !features.terminal_access {
        out.push(("ws.terminal.", "agentFeatures.terminalAccess"));
    }
    if !features.browser_automation {
        out.push(("ws.browser.", "agentFeatures.browserAutomation"));
    }
    if !features.structured_questions {
        out.push(("ws.app.question.", "agentFeatures.structuredQuestions"));
    }
    if !features.attention_requests {
        out.push((
            "ws.agent.requestDiscussion",
            "agentFeatures.attentionRequests",
        ));
        out.push(("ws.agent.reportBlocker", "agentFeatures.attentionRequests"));
    }
    out
}

/// The `ws.agent.reportToParent` doc line's cross-reference to the two
/// attention-request methods, scrubbed from the assembled description when
/// `agentFeatures.attentionRequests` is off (a unit test guards that this
/// clause still matches both description variants verbatim).
const REPORT_TO_PARENT_ATTENTION_XREF: &str = " — if you are blocked or need input, use `ws.agent.reportBlocker`/`ws.agent.requestDiscussion` instead";

/// Sandbox doc clauses scrubbed from the assembled description when the
/// workspace is NOT CoW-capable (no CoW filesystem support and not a microVM
/// workspace): the `mergeOnTurnEnd` continuation line under
/// `ws.agent.create`/`ws.agent.delegate` (whole line, scrubbed twice per
/// variant), the `mergeOnTurnEnd?` arg in the delegate signature, and the
/// sandbox-field cross-references on the `ws.agent.list`/`ws.agent.status`
/// doc lines. Description-only — dispatch accepts and ignores
/// `mergeOnTurnEnd` regardless (advisory; no sandbox exists to honor it).
/// Unit tests guard that each clause matches both description variants
/// verbatim so the `replacen` scrubs cannot silently become no-ops.
const SANDBOX_MERGE_ON_TURN_END_DOC_LINE: &str = "    Pass `mergeOnTurnEnd: false` to keep a sandboxed agent's sandbox unmerged when its turn ends — inspect it and merge later (fan out several sandboxed agents, then pick); default true auto-merges on completion.\n";
const SANDBOX_DELEGATE_MERGE_ARG: &str = ", mergeOnTurnEnd?";
const SANDBOX_LIST_FIELDS_XREF: &str =
    " Sandboxed agents carry `metadata.sandboxId`/`sandboxPath`/`sandboxBranch`.";
const SANDBOX_STATUS_FIELDS_XREF: &str =
    " Sandboxed agents additionally surface `sandboxStatus` and `mergeOnTurnEnd` (sandbox merge state).";

/// The `[agentFeatures]` settings path whose toggle is off and gates `method`
/// (the `host({ method })` frame name, e.g. `hook.list`), or `None` when the
/// method is not feature-gated or its toggle is on. The dispatch-deny layer
/// in [`super::bindings::try_dispatch`] uses this as defense in depth behind
/// the description/prelude pruning.
pub(super) fn denied_feature(
    features: &AgentFeaturesSettings,
    method: &str,
) -> Option<&'static str> {
    gated_prefixes(features)
        .into_iter()
        .find_map(|(prefix, feature)| {
            // Frame methods carry no `ws.` prefix.
            let ns = prefix.strip_prefix("ws.").unwrap_or(prefix);
            // Namespace entries end with `.` and gate everything under them;
            // method-level entries (the `attentionRequests` pair) name one
            // full method and must match exactly, so a future
            // `agent.requestDiscussionHistory` would not be over-denied.
            let hit = if ns.ends_with('.') {
                method.starts_with(ns)
            } else {
                method == ns
            };
            hit.then_some(feature)
        })
}

/// Assemble the `workspace_api` description for one bridge from the static
/// variants, pruning the doc lines of every feature disabled in
/// `[agentFeatures]` (a method line and its indented continuation lines drop
/// together; doubled blank lines left by a removed namespace paragraph
/// collapse to one), then scrubbing the sandbox doc clauses when the
/// workspace is not CoW-capable. With every toggle on and `cow_capable` —
/// the default — this returns the static const unchanged, so the
/// all-defaults description is byte-identical to today's by construction.
pub fn workspace_api_description(
    is_chief: bool,
    features: &AgentFeaturesSettings,
    cow_capable: bool,
) -> Cow<'static, str> {
    let base = if is_chief {
        WORKSPACE_API_DESCRIPTION_CHIEF
    } else {
        WORKSPACE_API_DESCRIPTION
    };
    let gated = gated_prefixes(features);
    if gated.is_empty() && cow_capable {
        return Cow::Borrowed(base);
    }
    // Method doc lines sit at exactly two spaces of indentation
    // (`  ws.<ns>.<method>(...`); their wrapped continuation lines are
    // indented deeper. Anything else (Rules/Parameters/Examples prose) never
    // matches a gated `ws.` prefix at indent 2.
    let mut kept: Vec<&str> = Vec::new();
    let mut skipping = false;
    for line in base.lines() {
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();
        if indent == 2 && trimmed.starts_with("ws.") {
            skipping = gated.iter().any(|(prefix, _)| trimmed.starts_with(prefix));
        } else if indent < 4 || trimmed.is_empty() {
            skipping = false;
        }
        if !skipping {
            kept.push(line);
        }
    }
    let mut out = String::with_capacity(base.len());
    let mut prev_blank = false;
    for line in kept {
        if line.is_empty() && prev_blank {
            continue;
        }
        prev_blank = line.is_empty();
        out.push_str(line);
        out.push('\n');
    }
    if !base.ends_with('\n') {
        out.pop();
    }
    // Method-level scrub for `attentionRequests`: the surviving
    // `ws.agent.reportToParent` doc line cross-references the two pruned
    // methods, so drop that clause too.
    if !features.attention_requests {
        out = out.replacen(REPORT_TO_PARENT_ATTENTION_XREF, "", 1);
    }
    // Clause-level scrub for non-CoW-capable workspaces: the sandbox doc
    // clauses on `agent.create`/`agent.delegate`/`agent.list`/`agent.status`
    // advertise behavior that needs a sandbox to exist. Dispatch is NOT
    // gated — `mergeOnTurnEnd` stays accepted-and-ignored (advisory).
    if !cow_capable {
        out = out.replacen(SANDBOX_MERGE_ON_TURN_END_DOC_LINE, "", 2);
        out = out.replacen(SANDBOX_DELEGATE_MERGE_ARG, "", 1);
        out = out.replacen(SANDBOX_LIST_FIELDS_XREF, "", 1);
        out = out.replacen(SANDBOX_STATUS_FIELDS_XREF, "", 1);
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::{
        denied_feature, workspace_api_description, AgentFeaturesSettings, Cow,
        REPORT_TO_PARENT_ATTENTION_XREF, WORKSPACE_API_DESCRIPTION,
        WORKSPACE_API_DESCRIPTION_CHIEF,
    };
    use std::collections::HashSet;

    // Source of every `ws.<ns>.<method>` binding actually dispatched by
    // `super::super::bindings::<ns>::dispatch`, included verbatim so the two
    // drift tests below re-evaluate whenever a dispatch arm is added or
    // removed. `description_only_names_bound_methods` guards the forward
    // direction (documented → bound); `every_bound_method_is_documented`
    // guards the reverse (bound → documented). Together they fail on any
    // drift in either direction, minus the `deferred()` exemptions.
    const BINDINGS_WORKSPACE: &str = include_str!("bindings/workspace.rs");
    const BINDINGS_NOTE: &str = include_str!("bindings/note.rs");
    const BINDINGS_TASK: &str = include_str!("bindings/task.rs");
    const BINDINGS_COMMENT: &str = include_str!("bindings/comment.rs");
    const BINDINGS_PRIMITIVE: &str = include_str!("bindings/primitive.rs");
    const BINDINGS_CROSS_WORKSPACE: &str = include_str!("bindings/cross_workspace.rs");
    const BINDINGS_PR: &str = include_str!("bindings/pr.rs");
    const BINDINGS_BROWSER: &str = include_str!("bindings/browser.rs");
    const BINDINGS_AGENT: &str = include_str!("bindings/agent.rs");
    const BINDINGS_EVENT: &str = include_str!("bindings/event.rs");
    const BINDINGS_GIT: &str = include_str!("bindings/git.rs");
    const BINDINGS_HOST: &str = include_str!("bindings/host.rs");
    const BINDINGS_HOOK: &str = include_str!("bindings/hook.rs");
    const BINDINGS_SCRIPT: &str = include_str!("bindings/script.rs");
    const BINDINGS_TERMINAL: &str = include_str!("bindings/terminal.rs");
    const BINDINGS_FILE: &str = include_str!("bindings/file.rs");
    const BINDINGS_APP_PROPOSAL: &str = include_str!("bindings/app/proposal.rs");
    const BINDINGS_APP_QUESTION: &str = include_str!("bindings/app/question.rs");
    const BINDINGS_APP_AGENTS: &str = include_str!("bindings/app/agents.rs");
    const BINDINGS_APP_SETTINGS: &str = include_str!("bindings/app/settings.rs");
    const BINDINGS_APP_SPECIALISTS: &str = include_str!("bindings/app/specialists.rs");
    const BINDINGS_APP_UI: &str = include_str!("bindings/app/ui.rs");
    const BINDINGS_APP_WORKSPACES: &str = include_str!("bindings/app/workspaces.rs");

    // The full set of `(namespace, bindings source)` pairs. Iterated by the
    // reverse-direction drift test below so a NEW dispatch arm added without
    // a description update fails the suite.
    fn all_namespaces() -> &'static [(&'static str, &'static str)] {
        &[
            ("workspace", BINDINGS_WORKSPACE),
            ("note", BINDINGS_NOTE),
            ("task", BINDINGS_TASK),
            ("comment", BINDINGS_COMMENT),
            ("primitive", BINDINGS_PRIMITIVE),
            ("crossWorkspace", BINDINGS_CROSS_WORKSPACE),
            ("pr", BINDINGS_PR),
            ("browser", BINDINGS_BROWSER),
            ("agent", BINDINGS_AGENT),
            ("event", BINDINGS_EVENT),
            ("git", BINDINGS_GIT),
            ("host", BINDINGS_HOST),
            ("hook", BINDINGS_HOOK),
            ("script", BINDINGS_SCRIPT),
            ("terminal", BINDINGS_TERMINAL),
            ("file", BINDINGS_FILE),
        ]
    }

    // For nested namespaces like `ws.app.proposal`, we need special handling
    // since the normal extractor finds "ws.app.proposal.show" but the bindings
    // only look at the final segment. Map dotted names to their binding source.
    fn nested_namespace_bindings(full_ns: &str) -> Option<&'static str> {
        match full_ns {
            "app.proposal" => Some(BINDINGS_APP_PROPOSAL),
            "app.question" => Some(BINDINGS_APP_QUESTION),
            "app.agents" => Some(BINDINGS_APP_AGENTS),
            "app.settings" => Some(BINDINGS_APP_SETTINGS),
            "app.specialists" => Some(BINDINGS_APP_SPECIALISTS),
            "app.ui" => Some(BINDINGS_APP_UI),
            "app.workspaces" => Some(BINDINGS_APP_WORKSPACES),
            _ => None,
        }
    }

    fn bindings_for(namespace: &str) -> &'static str {
        all_namespaces()
            .iter()
            .find(|(ns, _)| *ns == namespace)
            .map(|(_, src)| *src)
            .unwrap_or("")
    }

    // Methods whose dispatch arm exists solely to surface a
    // "not yet available in this daemon port" error (WSAPI-5 report).
    // These are bound but MUST NOT appear in the tool description — both
    // drift tests below use this set as an exemption in opposite directions.
    fn deferred() -> HashSet<(&'static str, &'static str)> {
        [
            ("workspace", "context"),
            ("workspace", "timeline"),
            ("workspace", "referenceDocs"),
            ("workspace", "emitNotification"),
        ]
        .into_iter()
        .collect()
    }

    // Extract every `ws.<ns>.<method>` triple mentioned in `text`. Matches
    // identifier chars only, so parenthesized args and neighbouring
    // punctuation are ignored. For nested namespaces (e.g. ws.app.proposal.show),
    // captures "app.proposal" as the namespace and "show" as the method.
    fn extract_ws_methods(text: &str) -> HashSet<(String, String)> {
        let mut out = HashSet::new();
        for (idx, _) in text.match_indices("ws.") {
            let rest = &text[idx + 3..];
            let ns_end = rest
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '.')
                .unwrap_or(rest.len());
            if ns_end == 0 {
                continue;
            }
            let full_ns = &rest[..ns_end];
            // Handle nested namespaces: find the last dot to get the method
            if let Some(last_dot) = full_ns.rfind('.') {
                let namespace = &full_ns[..last_dot];
                let method = &full_ns[last_dot + 1..];
                if !method.is_empty() {
                    out.insert((namespace.to_string(), method.to_string()));
                }
            }
        }
        out
    }

    // Parse the top-level `match method { ... }` block of a bindings file's
    // `dispatch` function and return the set of quoted method names bound in
    // it. Stops at the wildcard `other =>` (or `_ =>`) arm so nested match
    // blocks in helper functions further down the file (e.g. `browser::docs`
    // topic matching, `script::create` mode matching) are not counted.
    fn bound_methods(src: &str) -> HashSet<String> {
        let mut out = HashSet::new();
        let Some(match_idx) = src.find("match method {") else {
            return out;
        };
        let body = &src[match_idx + "match method {".len()..];
        for raw_line in body.lines() {
            let line = raw_line.trim_start();
            if line.starts_with("other =>") || line.starts_with("_ =>") {
                break;
            }
            let Some(rest) = line.strip_prefix('"') else {
                continue;
            };
            let Some(end) = rest.find('"') else {
                continue;
            };
            let name = &rest[..end];
            let after = rest[end + 1..].trim_start();
            if after.starts_with("=>") {
                out.insert(name.to_string());
            }
        }
        out
    }

    // The un-gated `ws.app.question.ask` doc line appears verbatim in BOTH
    // the base and chief descriptions; if one copy is edited without the
    // other, chief and non-chief agents would receive divergent guidance.
    #[test]
    fn question_ask_description_line_is_identical_in_both_variants() {
        let line_in = |desc: &str| -> String {
            desc.lines()
                .find(|l| l.trim_start().starts_with("ws.app.question.ask("))
                .expect("description advertises ws.app.question.ask")
                .to_string()
        };
        assert_eq!(
            line_in(WORKSPACE_API_DESCRIPTION),
            line_in(WORKSPACE_API_DESCRIPTION_CHIEF),
            "the ws.app.question.ask doc line drifted between the base and chief descriptions"
        );
    }

    // Every method reference in the tool description must correspond to a
    // real top-level dispatch arm in the matching bindings module — and must
    // NOT be one of the deferred "not yet available" arms. Guards against
    // hallucinated method names in the description.
    #[test]
    fn description_only_names_bound_methods() {
        let deferred = deferred();

        // Test base (non-chief) description: must NOT advertise ws.app.*
        // except ws.app.question, the one un-gated app namespace (available
        // to every workspace agent).
        for (ns, method) in extract_ws_methods(WORKSPACE_API_DESCRIPTION) {
            assert!(
                !ns.starts_with("app.") || ns == "app.question",
                "base description must not advertise ws.{ns}.{method} — only \
                 ws.app.question.* is un-gated"
            );
        }

        for (ns, method) in extract_ws_methods(WORKSPACE_API_DESCRIPTION) {
            assert!(
                !deferred.contains(&(ns.as_str(), method.as_str())),
                "base description advertises ws.{ns}.{method} but its binding is a \
                 `not yet available in this daemon port` stub",
            );
            // Check nested namespaces first, then fall back to flat namespace
            let src = nested_namespace_bindings(&ns).or_else(|| {
                let flat_src = bindings_for(&ns);
                if flat_src.is_empty() {
                    None
                } else {
                    Some(flat_src)
                }
            });
            assert!(
                src.is_some(),
                "base description mentions ws.{ns}.{method} but no bindings module `{ns}` exists",
            );
            let bound = bound_methods(src.unwrap());
            assert!(
                bound.contains(&method),
                "base description mentions ws.{ns}.{method} but bindings/{ns}.rs has no matching top-level dispatch arm",
            );
        }

        // Test chief description: must advertise the full ws.app.* surface
        for (ns, method) in extract_ws_methods(WORKSPACE_API_DESCRIPTION_CHIEF) {
            assert!(
                !deferred.contains(&(ns.as_str(), method.as_str())),
                "chief description advertises ws.{ns}.{method} but its binding is a \
                 `not yet available in this daemon port` stub",
            );
            let src = nested_namespace_bindings(&ns).or_else(|| {
                let flat_src = bindings_for(&ns);
                if flat_src.is_empty() {
                    None
                } else {
                    Some(flat_src)
                }
            });
            assert!(
                src.is_some(),
                "chief description mentions ws.{ns}.{method} but no bindings module `{ns}` exists",
            );
            let bound = bound_methods(src.unwrap());
            assert!(
                bound.contains(&method),
                "chief description mentions ws.{ns}.{method} but bindings/{ns}.rs has no matching top-level dispatch arm",
            );
        }
    }

    // Reverse direction: every top-level dispatch arm in every bindings
    // module (except the deferred stubs) must be advertised in the tool
    // description. Guards against a new binding shipping without a doc
    // entry, which would silently reduce agent-visible surface.
    #[test]
    fn every_bound_method_is_documented() {
        let deferred = deferred();
        let documented = extract_ws_methods(WORKSPACE_API_DESCRIPTION);
        for (ns, src) in all_namespaces() {
            for method in bound_methods(src) {
                if deferred.contains(&(*ns, method.as_str())) {
                    continue;
                }
                assert!(
                    documented.contains(&((*ns).to_string(), method.clone())),
                    "bindings/{ns}.rs binds ws.{ns}.{method} but the tool description does not advertise it",
                );
            }
        }
    }

    // ---- [agentFeatures] segment-assembly tests ----------------------------

    // The gated `ws.` doc prefixes paired with the mutator that flips their
    // `[agentFeatures]` toggle off. Namespace-level toggles gate one
    // `ws.<ns>.` prefix; method-level toggles (attentionRequests) gate one
    // full method name per prefix.
    type FeatureCase = (&'static [&'static str], fn(&mut AgentFeaturesSettings));

    // Each toggle mapped to the `ws.` doc prefixes it prunes and a mutator
    // that flips it off. Iterated by the assembly tests below so a new toggle
    // cannot ship without joining the sweep.
    fn feature_cases() -> Vec<FeatureCase> {
        vec![
            (&["ws.hook."], |f| f.background_hooks = false),
            (&["ws.host."], |f| f.host_exec = false),
            (&["ws.script."], |f| f.scripts = false),
            (&["ws.terminal."], |f| f.terminal_access = false),
            (&["ws.browser."], |f| f.browser_automation = false),
            (&["ws.app.question."], |f| f.structured_questions = false),
            (
                &["ws.agent.requestDiscussion", "ws.agent.reportBlocker"],
                |f| f.attention_requests = false,
            ),
        ]
    }

    // Hard requirement: with every toggle on (the default), the assembled
    // description IS the static const — byte-identical, both variants.
    #[test]
    fn all_defaults_description_is_byte_identical() {
        let features = AgentFeaturesSettings::default();
        let base = workspace_api_description(false, &features, true);
        assert!(
            matches!(base, Cow::Borrowed(_)),
            "all-on must not reassemble"
        );
        assert_eq!(&*base, WORKSPACE_API_DESCRIPTION);
        let chief = workspace_api_description(true, &features, true);
        assert!(
            matches!(chief, Cow::Borrowed(_)),
            "all-on must not reassemble"
        );
        assert_eq!(&*chief, WORKSPACE_API_DESCRIPTION_CHIEF);
    }

    // Disabling one feature removes exactly its own doc lines: no method
    // matching a gated prefix stays documented (a passing textual
    // cross-reference in another namespace's doc line — e.g. `ws.script.*`
    // inside the `ws.host.exec` entry — may remain), every other documented
    // method survives, and the pruned text never leaves doubled blank lines
    // or continuation orphans behind.
    #[test]
    fn disabling_one_feature_prunes_only_its_lines() {
        for is_chief in [false, true] {
            let full = workspace_api_description(is_chief, &AgentFeaturesSettings::default(), true);
            let full_methods = extract_ws_methods(&full);
            for (prefixes, disable) in feature_cases() {
                let mut features = AgentFeaturesSettings::default();
                disable(&mut features);
                let pruned = workspace_api_description(is_chief, &features, true);
                for prefix in prefixes {
                    assert!(
                        !pruned
                            .lines()
                            .any(|l| l.strip_prefix("  ").is_some_and(|t| t.starts_with(prefix))),
                        "chief={is_chief}: a `{prefix}` doc line survived disabling its toggle"
                    );
                }
                let pruned_methods = extract_ws_methods(&pruned);
                for (ns, method) in &full_methods {
                    let full_name = format!("ws.{ns}.{method}");
                    if prefixes.iter().any(|p| full_name.starts_with(p)) {
                        assert!(
                            !pruned_methods.contains(&(ns.clone(), method.clone())),
                            "chief={is_chief}: {full_name} still documented after \
                             disabling `{prefixes:?}`"
                        );
                    } else {
                        assert!(
                            pruned_methods.contains(&(ns.clone(), method.clone())),
                            "chief={is_chief}: disabling `{prefixes:?}` also dropped {full_name}"
                        );
                    }
                }
                assert!(
                    !pruned.contains("\n\n\n"),
                    "chief={is_chief}: pruning `{prefixes:?}` left doubled blank lines"
                );
            }
        }
    }

    // All toggles off at once: every gated prefix is gone, the un-gated
    // surface (notes, tasks, git, files, crossWorkspace, ...) is intact.
    #[test]
    fn disabling_all_features_keeps_ungated_surface() {
        let features = AgentFeaturesSettings {
            background_hooks: false,
            host_exec: false,
            scripts: false,
            terminal_access: false,
            browser_automation: false,
            rich_chat_blocks: false,
            structured_questions: false,
            attention_requests: false,
        };
        for is_chief in [false, true] {
            let pruned = workspace_api_description(is_chief, &features, true);
            for (prefixes, _) in feature_cases() {
                for prefix in prefixes {
                    assert!(
                        !pruned.contains(prefix),
                        "chief={is_chief}: `{prefix}` survived"
                    );
                }
            }
            for kept in [
                "ws.note.read(",
                "ws.task.updateStatus(",
                "ws.git.status(",
                "ws.file.read(",
                "ws.crossWorkspace.listSiblings(",
                "ws.agent.create(",
                "ws.agent.reportToParent(",
                "ws.event.subscribe(",
                "ws.pr.status(",
            ] {
                assert!(
                    pruned.contains(kept),
                    "chief={is_chief}: `{kept}` was wrongly pruned"
                );
            }
        }
    }

    // `richChatBlocks` is prompt-only: flipping it must not touch the tool
    // description at all.
    #[test]
    fn rich_chat_blocks_does_not_affect_description() {
        let features = AgentFeaturesSettings {
            rich_chat_blocks: false,
            ..AgentFeaturesSettings::default()
        };
        assert_eq!(
            &*workspace_api_description(false, &features, true),
            WORKSPACE_API_DESCRIPTION
        );
        assert_eq!(
            &*workspace_api_description(true, &features, true),
            WORKSPACE_API_DESCRIPTION_CHIEF
        );
    }

    // `structuredQuestions` is method-level: other `ws.app.*` docs in the
    // chief variant must survive it.
    #[test]
    fn structured_questions_off_keeps_other_app_docs_in_chief() {
        let features = AgentFeaturesSettings {
            structured_questions: false,
            ..AgentFeaturesSettings::default()
        };
        let pruned = workspace_api_description(true, &features, true);
        assert!(!pruned.contains("ws.app.question."));
        for kept in [
            "ws.app.agents.list(",
            "ws.app.settings.list(",
            "ws.app.ui.navigate(",
        ] {
            assert!(pruned.contains(kept), "`{kept}` was wrongly pruned");
        }
    }

    // Guard: the reportToParent cross-reference clause scrubbed by the
    // `attentionRequests` gate still matches both description variants
    // verbatim, so the `replacen` scrub cannot silently become a no-op.
    #[test]
    fn attention_xref_clause_is_present_in_both_variants() {
        assert!(WORKSPACE_API_DESCRIPTION.contains(REPORT_TO_PARENT_ATTENTION_XREF));
        assert!(WORKSPACE_API_DESCRIPTION_CHIEF.contains(REPORT_TO_PARENT_ATTENTION_XREF));
    }

    // Guard: each sandbox doc clause scrubbed by the CoW-capability gate
    // matches both description variants verbatim (the mergeOnTurnEnd doc
    // line appears twice per variant — under create and delegate), so the
    // `replacen` scrubs cannot silently become no-ops.
    #[test]
    fn sandbox_doc_clauses_are_present_in_both_variants() {
        for desc in [WORKSPACE_API_DESCRIPTION, WORKSPACE_API_DESCRIPTION_CHIEF] {
            assert_eq!(
                desc.matches(super::SANDBOX_MERGE_ON_TURN_END_DOC_LINE)
                    .count(),
                2,
                "mergeOnTurnEnd doc line must appear under both create and delegate"
            );
            assert!(desc.contains(super::SANDBOX_DELEGATE_MERGE_ARG));
            assert!(desc.contains(super::SANDBOX_LIST_FIELDS_XREF));
            assert!(desc.contains(super::SANDBOX_STATUS_FIELDS_XREF));
        }
    }

    // CoW-capable (the default): the sandbox clauses are present and the
    // all-defaults description is byte-identical to the static const.
    // Not capable: every sandbox clause is scrubbed while the method lines
    // themselves survive.
    #[test]
    fn cow_capability_gates_sandbox_doc_clauses() {
        let features = AgentFeaturesSettings::default();
        for is_chief in [false, true] {
            let capable = workspace_api_description(is_chief, &features, true);
            assert!(capable.contains("mergeOnTurnEnd"));

            let pruned = workspace_api_description(is_chief, &features, false);
            assert!(
                !pruned.contains("mergeOnTurnEnd"),
                "chief={is_chief}: mergeOnTurnEnd must be scrubbed when not CoW-capable"
            );
            assert!(
                !pruned.contains("sandboxStatus") && !pruned.contains("sandboxId"),
                "chief={is_chief}: sandbox field docs must be scrubbed when not CoW-capable"
            );
            for kept in [
                "ws.agent.create(",
                "ws.agent.delegate(",
                "ws.agent.list(",
                "ws.agent.status(",
            ] {
                assert!(
                    pruned.contains(kept),
                    "chief={is_chief}: `{kept}` was wrongly pruned"
                );
            }
        }
    }

    // `attentionRequests` is method-level: other `ws.agent.*` docs — most
    // importantly `reportToParent`, minus its cross-reference to the pruned
    // pair — must survive it, and no textual mention of the pruned methods
    // may remain anywhere in the description.
    #[test]
    fn attention_requests_off_keeps_other_agent_docs() {
        let features = AgentFeaturesSettings {
            attention_requests: false,
            ..AgentFeaturesSettings::default()
        };
        for is_chief in [false, true] {
            let pruned = workspace_api_description(is_chief, &features, true);
            assert!(!pruned.contains("ws.agent.requestDiscussion"));
            assert!(!pruned.contains("ws.agent.reportBlocker"));
            for kept in [
                "ws.agent.reportToParent(",
                "ws.agent.create(",
                "ws.agent.delegate(",
                "ws.agent.watch(",
            ] {
                assert!(
                    pruned.contains(kept),
                    "chief={is_chief}: `{kept}` was wrongly pruned"
                );
            }
        }
    }

    // The dispatch-deny mapping: gated frame methods name their feature,
    // un-gated methods and enabled toggles pass through.
    #[test]
    fn denied_feature_maps_gated_methods_only() {
        let all_off = AgentFeaturesSettings {
            background_hooks: false,
            host_exec: false,
            scripts: false,
            terminal_access: false,
            browser_automation: false,
            rich_chat_blocks: false,
            structured_questions: false,
            attention_requests: false,
        };
        assert_eq!(
            denied_feature(&all_off, "hook.schedule"),
            Some("agentFeatures.backgroundHooks")
        );
        assert_eq!(
            denied_feature(&all_off, "host.exec"),
            Some("agentFeatures.hostExec")
        );
        assert_eq!(
            denied_feature(&all_off, "script.run"),
            Some("agentFeatures.scripts")
        );
        assert_eq!(
            denied_feature(&all_off, "terminal.list"),
            Some("agentFeatures.terminalAccess")
        );
        assert_eq!(
            denied_feature(&all_off, "browser.exec"),
            Some("agentFeatures.browserAutomation")
        );
        assert_eq!(
            denied_feature(&all_off, "app.question.ask"),
            Some("agentFeatures.structuredQuestions")
        );
        assert_eq!(
            denied_feature(&all_off, "agent.requestDiscussion"),
            Some("agentFeatures.attentionRequests")
        );
        assert_eq!(
            denied_feature(&all_off, "agent.reportBlocker"),
            Some("agentFeatures.attentionRequests")
        );
        // Un-gated namespaces pass even with everything off.
        assert_eq!(denied_feature(&all_off, "note.read"), None);
        assert_eq!(
            denied_feature(&all_off, "crossWorkspace.listSiblings"),
            None
        );
        assert_eq!(denied_feature(&all_off, "app.settings.list"), None);
        // Sibling `ws.agent.*` methods pass even with attentionRequests off.
        assert_eq!(denied_feature(&all_off, "agent.reportToParent"), None);
        assert_eq!(denied_feature(&all_off, "agent.list"), None);
        // Method-level entries match exactly: a longer method sharing the
        // gated method as a prefix is not over-denied.
        assert_eq!(
            denied_feature(&all_off, "agent.requestDiscussionHistory"),
            None
        );
        // Enabled toggles never deny.
        assert_eq!(
            denied_feature(&AgentFeaturesSettings::default(), "hook.schedule"),
            None
        );
        assert_eq!(
            denied_feature(&AgentFeaturesSettings::default(), "host.exec"),
            None
        );
        assert_eq!(
            denied_feature(&AgentFeaturesSettings::default(), "agent.requestDiscussion"),
            None
        );
    }
}
