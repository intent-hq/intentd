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

/// One delegation model option a specialist declares (PROTOCOL §5.11
/// `modelOptions`): the internal compound model id plus the author's hint.
pub struct SpecialistModelOption {
    /// Internal compound model id (e.g. `opencode:kimi-k3`), passed verbatim
    /// as the `model` param of `ws.agent.delegate` / `ws.agent.create`.
    pub model: String,
    /// Free-text hint for choosing this option; empty when the author gave none.
    pub hint: String,
    /// Reasoning-effort level this option implies, passed as `reasoningEffort`
    /// when the option is chosen; empty when the author declared none.
    pub reasoning_effort: String,
}

/// One specialist's resolved `modelOptions` list, injected into the
/// `workspace_api` tool description so delegating agents can pick a model.
pub struct SpecialistModelOptions {
    /// Specialist id (the `specialist` param of delegate/create).
    pub specialist: String,
    /// Compound id a no-`model` delegate would pin, as resolved by the same
    /// resolver the `resolvedModel` preview uses; `None` when resolution
    /// yields the provider CLI default.
    pub default_model: Option<String>,
    /// Ordered options as authored in the winning tier's frontmatter.
    pub options: Vec<SpecialistModelOption>,
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
/// A compact `Namespaces` index sits directly after the Rules/Parameters
/// block — well before any plausible client-side truncation point — so MCP
/// clients that truncate long tool descriptions still surface the full
/// `ws.*` capability map. Index lines share the `  ws.<ns>.` indent-2 shape
/// of method doc lines, so [`workspace_api_description`] prunes a gated
/// namespace's index line together with its doc lines.
///
/// The `#[test] description_only_names_bound_methods` below verifies every
/// `ws.<ns>.<method>(` mention here maps to a real dispatch arm in the
/// matching `bindings/<ns>.rs`, preventing silent drift when the description
/// or the bindings change; `namespace_index_matches_documented_surface`
/// keeps the index in lockstep with the API sections.
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

Namespaces (index — full signatures in API below):
  ws.help(namespace?) — runtime docs: ws.help() returns this index, ws.help("pr") the full pr docs
  ws.workspace.* — workspace info, title, status message
  ws.app.question.* — ask the user structured questions
  ws.note.* — notes; the spec is note id "spec"
  ws.comment.* — comment threads on notes
  ws.task.* — task notes + checkbox statuses
  ws.primitive.* — rich note blocks (reference/cli/patch/agent-action)
  ws.agent.* — create/delegate/message/watch agents
  ws.git.* — attributed commits + secondary git root registry
  ws.event.* — activity queries + event subscriptions
  ws.script.* — saved build/test/service scripts
  ws.host.* — host.exec = one-shot host command exec
  ws.hook.* — background watchers; can call full ws.* incl. pr.snapshot and host.exec
  ws.browser.* — Chrome DevTools browser automation
  ws.terminal.* — read workspace terminal output
  ws.crossWorkspace.* — read sibling-workspace notes
  ws.file.* — read/write workspace project files
  ws.pr.* — pr.monitor = daemon-run PR watch (preferred); pr.snapshot = one-shot state; other PR ops use `gh`

API:
  ws.help(namespace?) → string  // Offline API docs, robust to clients that truncate this description: `ws.help()` returns the Namespaces index; `ws.help("pr")` returns the full doc lines for one namespace. Namespaces disabled in settings are omitted and error when requested.

  ws.workspace.info() → { id, path }  // Current workspace ID + absolute path.
  ws.workspace.details() → { id, title, hasTitle, status, statusMessage, statusImageAssetId, branch, repositoryName, tags }  // Workspace metadata; `status` is the lifecycle enum and `statusMessage` is the user-facing work summary.
  ws.workspace.setTitle(title) → { ok, title, branch, skipped? }  // Set a short 1-5 word workspace title. May rename the branch if it is still auto-generated; returns `skipped` if the workspace already has a custom title.
  ws.workspace.setStatusMessage(message) → { ok, statusMessage }  // Set or clear the 1-2 sentence user-facing workspace status message; does not change lifecycle `status` or task statuses. Pass an empty string or null to clear.
  ws.workspace.setStatusImage({ data, mimeType, originalName? } | null) → { ok, statusImageAssetId, url? }  // Set or clear the workspace status screenshot shown on the workspace card. `data` is base64 image bytes (a `data:` URL prefix is accepted), `mimeType` must be image/*. Pass null to clear. Unavailable in the chief-of-staff workspace.
  ws.workspace.setAgentName(name) → { ok, name }  // Rename the current agent session. Call this early in your first response and use a short 1-5 word task-focused name.
  ws.workspace.archive() → { ok, status, archivedAt }  // Archive the current workspace. ONLY call this on explicit user request (same convention as user-requested commits). Refuses if other agents are running or queued (no override); unavailable in the chief-of-staff workspace.
  ws.workspace.unarchive() → { ok, status }  // Unarchive the current workspace. ONLY call this on explicit user request. Unavailable in the chief-of-staff workspace.

  ws.app.question.ask({ header, question, options, explanation?, multiSelect? }) → { ok, attachmentId, message }  // Ask the user ONE structured clarifying question. REQUIRED: `header` (short topic label), `question` (the prompt text), and `options` — an array of at least 2 OBJECTS [{ label, description? }] (NOT bare strings); do NOT add an "Other" option, a free-form answer is always offered automatically. Example: ws.app.question.ask({ header: "Auth method", question: "Which auth should the endpoint use?", options: [{ label: "OAuth", description: "OAuth 2.0 flow" }, { label: "API key", description: "Static key in header" }] }). Call once per question (aim for at most ~4 questions per turn); `multiSelect: true` lets the user pick several. Questions are presented when your turn ends; the answers arrive as plain-text Q:/A: pairs in the next user message ("(skipped)" for skipped questions). Ask all your questions, then finish the turn.

  ws.note.read(id) → { id, title, content, tags, ... }  // Read a note. Use id=`spec` for the workspace spec. Content has line numbers like `   1 | text`.
  ws.note.create(title, content, tags?) → { id, title, tags, link, markdownLink, convertedCount, createdTaskNoteIds, createdTasks, warnings }  // Create a new note and return canonical `intent://local/{workspaceId}/note/{noteId}` links. Share `markdownLink` with users so they can open the note. `@@@task` blocks in the content auto-convert into linked task notes, and the result carries the conversion's `createdTasks` + `warnings` like the content-write ops. DO NOT use this for the spec: the spec already exists as note ID `spec`; edit or add to it instead.
  ws.note.list(tag?) → [{ id, title, tags, ... }]  // List notes. Optional tag filter narrows results.
  ws.note.listTasks(id) → [{ text, status, taskNoteId, linkedTaskNoteId, lineNumber, ... }]  // Faster than `read()` when you only need checkbox/task IDs. Use `taskNoteId` for delegation; `linkedTaskNoteId` is a backward-compatible alias.
  ws.note.readAsset(asset) → { assetId, mimeType, data, sizeKb }  // `asset` can be an asset ID or `workspace-asset://...` URL. Image assets (PNG, JPEG, GIF, WebP) are returned as native image content blocks (the model sees the image directly); non-image assets return the JSON object.
  ws.note.setContent(id, content, confirmReplacement?) → { ... }  // ⚠️ FULL REPLACEMENT: replaces the entire note. Prefer `add()` / `edit()` / `editLines()` unless you intentionally want to overwrite everything.
    If the new content is much shorter, call again with `confirmReplacement=true`. `@@@task` blocks auto-convert into linked task notes; the fence line takes optional `key=` / `dependsOn=` / `conflictsWith=` / `effort=` attributes (see `ws.task.convertBlocks`), and every content-write result (`add` / `edit` / `editLines` / `setContent`) carries the conversion's `createdTasks` + `warnings`.
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
  ws.task.markAsTask(noteId, status, { acceptanceCriteria?, effort?, dependsOn?, conflictsWith? }) → { ... }  // Convert a note into a task note. `acceptanceCriteria` may be an array or JSON string; `effort` maps to estimated effort; `dependsOn`/`conflictsWith` seed task relations (validated like `setRelations`).
  ws.task.setRelations(noteId, { dependsOn?, conflictsWith? }) → { ok, noteId, dependsOn, conflictsWith }  // Replace a task's relation lists (arrays of task note ids). Omitted field → kept; `[]` → cleared. `dependsOn` writes that close a dependency cycle or reference a tree ancestor/descendant of the task are rejected with the offending path/relationship named.
  ws.task.convertBlocks(noteId) → { convertedCount, createdNoteIds, createdTasks, warnings }  // Convert `@@@task` blocks into linked task notes. Note updates already auto-convert them; use this for manual re-conversion. The fence line takes optional attributes — `@@@task key=api dependsOn=a,b conflictsWith=c effort=2h` — bare tokens, comma-separated lists, whitespace-tolerant; `dependsOn`/`conflictsWith` values resolve against sibling block `key=`s, then exact sibling titles, then existing task-note ids, and `effort` seeds the estimated effort.
    Conversion never fails on bad attributes: blocks always convert, and unresolvable/ambiguous references or rejected edges (cycle, tree ancestor/descendant) are skipped with a warning naming the block and reference. `createdTasks` is `[{ key?, title, noteId }]` in block order; check `warnings` after converting.
  ws.task.createPrerequisite(dependentNoteId, title, { content?, status? }) → { ... }  // Adds a prerequisite task dependency.
  ws.task.assignAgent(noteId, agentId) → { ok, noteId, agentId }  // Assign an existing agent to a task note. `agentId` must be `agent-{uuid}`; to create and assign in one step, use `ws.agent.create(..., { taskNoteId: noteId })`.

  ws.primitive.addReference(noteId, semanticId, description, snapshot?) → { ok, primitiveId, noteId }  // Code reference primitive; `semanticId` examples: `src/file.ts#symbol:Foo` or `src/file.ts#L10-20`.
  ws.primitive.addCli(noteId, command, description, workingDirectory?) → { ok, primitiveId, noteId }  // CLI primitive; optional cwd is relative to workspace root.
  ws.primitive.addPatch(noteId, filePath, diff, description) → { ok, primitiveId, noteId }  // Stores a patch block that can be applied in a note.
  ws.primitive.addAgentAction(noteId, agentId, goal, description) → { ok, primitiveId, noteId }  // Adds a triggerable agent action block.

  ws.agent.create(name, message, opts?) → { ok, id?, text?, ... }  // Create and start an agent immediately. You are auto-subscribed to its completion events and will be woken when it finishes.
    Specialists include `"implementor"` for implementation work and `"verifier"` for review/verification. `createLinkedNote=true` with `noteContent` creates a linked note; agents are background by default unless `isBackground=false`.
    You can override specialist defaults with `model`, `reasoningEffort`, or `behaviorPrompt`. A `reasoningEffort` the resolved model does not support is rejected with the list of valid values.
    Pass `mergeOnTurnEnd: false` to keep a sandboxed agent's sandbox unmerged when its turn ends — inspect it and merge later (fan out several sandboxed agents, then pick); default true auto-merges on completion.
    Pass `vmResources: { vcpus?, memMib? }` to size a microVM-sandboxed agent's VM (vcpus 1-16, memMib >= 128); missing fields fall back to the `sandbox.microvm` settings. Accepted and ignored outside microVM workspaces.
  ws.agent.delegate({ taskNoteId?, noteId?, taskText?, agentInstructions?, specialist?, model?, reasoningEffort?, behaviorPrompt?, waitMode?, skipAutoCommit?, mergeOnTurnEnd?, vmResources?, tasks? }) → { ok, text?, ... }  // Delegate an existing task to a new agent. Prefer `taskNoteId` from `intent://local/task/{id}`; otherwise pass `noteId` + exact `taskText` from a checkbox.
    Delegation starts immediately and auto-subscribes you to completion events. `waitMode`: `"immediate"` wakes after each agent, `"after_all"` wakes after the whole group. Example: `taskNoteId: "abc-123"`. Completion wakes may carry an advisory `Tasks now unblocked by this completion: …` (or `by these completions:` when coalesced) section naming tasks that just became startable (computed fresh at delivery time); nothing auto-starts — delegate the ones you want started.
    `reasoningEffort` sets the child's reasoning level (e.g. `"low"` / `"medium"` / `"high"`); omit it to inherit the chosen model option's effort, else the specialist's own default. A level the resolved model does not support is rejected with the list of valid values.
    Pass `mergeOnTurnEnd: false` to keep a sandboxed agent's sandbox unmerged when its turn ends — inspect it and merge later (fan out several sandboxed agents, then pick); default true auto-merges on completion.
    Pass `vmResources: { vcpus?, memMib? }` to size a microVM-sandboxed agent's VM (vcpus 1-16, memMib >= 128); missing fields fall back to the `sandbox.microvm` settings. Accepted and ignored outside microVM workspaces.
    Batch form: each `tasks` entry is a bare taskNoteId or `{ taskNoteId, specialist?, model?, reasoningEffort? }` (per-task overrides of the call's top-level defaults). Every listed task is classified and only the eligible subset starts — tasks with unmet `dependsOn` are `held:blocked-on-deps`, tasks whose `conflictsWith` overlaps the running/starting set are `held:conflict` (delegate a held task individually to force it past the hold), and already-running/complete/cancelled tasks are `skipped` (re-calling with the same list is idempotent). Startable tasks are admitted in effort-weighted critical-path priority order (task `estimatedEffort` strings are parsed; unparseable/missing default to 30 min), so a conflict is resolved in favor of the task heading the longest remaining dependent chain, not the one listed first. `agentInstructions` and `force` are rejected alongside `tasks` (each started task's first message resolves from its own task note; occupied tasks classify as `skipped`). The result enumerates every task with disposition + reason and an `unlockPlan` naming what becomes startable at settlement; when any requested chain carries an explicit estimate the plan also carries `criticalPathMinutes` (~N min of serial work remaining on the critical path; spans the requested tasks and their downstream dependents only — incomplete upstream deps outside the request are not counted, and the number reflects only estimated chains, so it can understate when an unestimated chain is longer). Rows for tasks the graph does not cover — no `dependsOn`/`conflictsWith` of their own and not referenced by any other requested task's relations — classify exactly as before (the flag never changes a disposition) but carry `relationsUnknown: true`, and the summary counts the started ones.
  ws.agent.send(agentId, message, priority?) → { ok, agentId, ... }  // Send a message to another agent. Delivers with interrupt priority by DEFAULT: the target is stopped mid-response and the message is delivered immediately. Pass `priority="queue"` to opt out and queue the message if the target is busy.
  ws.agent.sendToTask(taskNoteId, message, priority?) → { ok, taskNoteId, ... }  // Follow up with the agent assigned to a task note; more convenient than `send()` when you only know the task note ID. Same interrupt-by-default delivery as `send()`; `priority="queue"` opts out.
  ws.agent.subscribe(eventTypes, { excludeSelf?, batchWindow? }) → { subscriptionId, ... }  // Compatibility alias for `ws.event.subscribe()`. `eventTypes` must be an array.
  ws.agent.unsubscribe(subscriptionId) → { ok, subscriptionId }  // Compatibility alias for `ws.event.unsubscribe()`.
  ws.agent.watch(agentId) → { ok, subscriptionId, agentId }  // Watch another agent: you are woken once, at its next completion (it goes idle with an empty pending message queue, fails, or is deleted), and the watch is then retired. Blocker/discussion attention wakes are delivered along the way without ending the watch. Watch again if you care about future turns. A watch adopted into an `after_all` delegation group ends at group settlement and cannot be unwatched while grouped (use `agent.cancelSubscriptions` with the groupId).
  ws.agent.unwatch(subscriptionIdOrAgentId) → { ok, removed }  // Stop watching an agent (accepts the watch's subscriptionId or the watched agentId).
  ws.agent.list(includeCompleted?) → [agents]  // Lists agents in this workspace; completed agents are omitted unless requested. Sandboxed agents carry `metadata.sandboxId`/`sandboxPath`/`sandboxBranch`.
  ws.agent.status(agentId) → agent  // Detailed agent status including task linkage, activity timestamps, and the pending message queue (`queue` + `queueLength`; entries in the getQueue shape with `content` truncated to 200 chars). Sandboxed agents additionally surface `sandboxStatus` and `mergeOnTurnEnd` (sandbox merge state).
  ws.agent.mergeSandbox(agentId) → { ok, agentId, status, reason? }  // Start a sandbox merge-back into the workspace repo — the follow-up to `mergeOnTurnEnd: false` (inspect the sandbox, then merge when ready). ASYNC: returns immediately with `status: "started"` (merge runs in the background; large repos take minutes) or `"in_progress"` (a merge already owns the sandbox). Poll `ws.agent.status(agentId).sandboxStatus` for the outcome — `merged`, `conflict` (terminal; work preserved on an `sb/…` branch in the workspace repo), `merge_pending` (blocked/failed; retried automatically), or unchanged on a dirty bounce (uncommitted sandbox changes with workspace auto-commit off). Errors if the agent has no sandbox.
  ws.agent.getQueue(agentId) → { ok, agentId, queueLength, queue }  // The agent's full pending message queue in drain order (position 0 = next delivery; interrupt-priority entries first, then normal FIFO; entries under edit are flagged `editing: true` at the end). Each entry: `{ id, content, queuedAt, position, turnId?, interruptPriority?, editing?, fromAgentId?, fromAgentName? }` — attribution absent for user-sent entries.
  ws.agent.removeQueuedMessage(agentId, messageId) → { ok, agentId, messageId }  // Retract YOUR OWN pending message from an agent's queue before delivery. Only messages you sent can be removed; entries from other senders (or the user) are rejected.
  ws.agent.diagnostics({ agentId?, taskNoteId?, includeCompleted?, staleRespondingAfterMs? }?) → { diagnostics, text }  // Sanitized snapshot of agent statuses, subscriptions, queues, delegation groups, delivery stats, recent delivery events, and stuck-risk signals.
  ws.agent.snapshot() → { time, hooks?, agentWatches?, queuedMessages?, eventSubscriptions?, runningSubAgents?, numQuestionsAsked?, pendingAttention? }  // YOUR OWN compact state digest (the cheap counterpart to `diagnostics`): active hooks, sub-agent watches, queued messages, event subscriptions, unsettled child agents, pending structured questions, and any unresolved blocker/discussion you raised. Zero/absent fields are omitted; `time` is current UTC.
  ws.agent.wakeOrCreate(taskNoteId, contextMessage, model?, messageMetadata?, reasoningEffort?) → { ... }  // Ensure a task has a working agent: checks assigned agents, resumes a running/restorable one if possible, otherwise creates a new agent for the task. `reasoningEffort` applies only when a new agent is created.
  ws.agent.readConversation(agentId, { lastN?, startTurn?, endTurn?, includeToolCalls? }) → messages  // Read another agent’s conversation history.
  ws.agent.summary(agentId) → summary  // Quick summary of what another agent did.
  ws.agent.reportToParent(report) → { ok, ... }  // Send a concise report on completed or progressing work to the parent agent — if you are blocked or need input, use `ws.agent.reportBlocker`/`ws.agent.requestDiscussion` instead. Only works for delegated agents; user-created agents will get an error.
  ws.agent.requestDiscussion(reason) → { ok, kind, reason, savedAt }  // Raise a pending attention request when you need user/coordinator input to proceed — call it BEFORE ending your turn. `reason` is required. Available to every agent; if you have a linked task it moves to `discussion_needed`.
  ws.agent.reportBlocker(reason) → { ok, kind, reason, savedAt }  // Report an infrastructure/environment problem you cannot resolve (broken sandbox, failing environment, missing credentials) — call it BEFORE ending your turn. `reason` is required. Available to every agent; if you have a linked task it moves to `blocked`.

  ws.git.commit(message, { files?, userRequested? }) → { ok, hash, files, fileCount }  // The commit helper. Auto-stages only your changes and is mainly for explicit user-requested checkpoint commits.
    If workspace auto-commit is disabled, set `userRequested=true` to confirm the user asked for the commit.
    For status/stage/diff/merge-check and every other git read or write, run the plain `git` CLI instead.
  ws.git.registerRoot(path) → { id, workspaceId, path, source, repoOwner?, repoName?, branch?, ... }  // Register a secondary git repository (submodule checkout, sibling clone) for the workspace's git root tracking. `path` must be an existing git repo root (has a `.git` entry); a relative path resolves against the workspace worktree, and the result is canonicalized and may live anywhere on the host. The workspace's own primary root is rejected (tracked implicitly). Idempotent by canonical path — re-registering merges attribution and upgrades an auto-detected row to `source: "agent"`.
  ws.git.unregisterRoot(path) → { ok, gitRootId, path }  // Remove a registered secondary git root by path (relative paths resolve against the workspace worktree). Errors when no root is registered for the path.
  ws.git.listRoots() → [{ id, workspaceId, path, source, repoOwner?, repoName?, branch?, ... }]  // List the workspace's registered secondary git roots; `branch` is read live per call.

  ws.event.agentActivity(agentId?, minutesAgo?) → [events]  // With `agentId`, narrows to that agent; otherwise returns recent activity window.
  ws.event.workspaceSummary(minutesAgo?) → summary  // Aggregated workspace activity summary.
  ws.event.query({ eventType?, actorType?, actorId?, path?, minutesAgo?, limit? }) → [events]  // Advanced event query filters. `eventType` accepts the same glob syntax as subscribe: a category wildcard like `note:*`, an exact type like `note:updated`, or bare `*` for no type filter.
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

  ws.hook.schedule({ name, code, delayMs, ttlMs?, perpetual? }) → { hook, dispatched }  // Register a background hook: a small JS script the daemon runs every `delayMs` ms (min 10000) until it returns `{ dispatch: true, message }` (you are woken with the message and the hook ends), throws/times out (evicted, you are woken with the error), is cancelled, or expires. `name` ≤ 50 chars — a short human-readable description of what the hook watches (shown to the user). The first run happens immediately as validation: a failure rejects the call, a dispatch wakes you right away (`dispatched: true`) without persisting a schedule.
    The script runs with this same `ws.*` API available — the full surface, including `ws.pr.snapshot` and `ws.host.exec` — and a 60s budget per run, so make hooks self-checking: the hook performs the check itself and dispatches only on a meaningful change (diffed against `hookState`), not a bare timer that wakes you to do the check. Return `{ dispatch: false }` or nothing to keep watching. Use hooks to watch for conditions (CI results, PR activity, file changes) instead of blocking or polling in your own turn — idle turns time out after ~30 minutes of silence, so hooks are how to wait for slow external conditions. For PR monitoring prefer `ws.pr.monitor` — a hook has a TTL and expires while a PR sits blocked, the monitor does not.
    Carry state between runs: a returned `state` field (any JSON value, ~16 KiB cap) persists and is injected into the next run as the `hookState` global (`null` on the first run); omit `state` to keep the previous value, return `state: null` to clear it.
    Every hook has a TTL counted from creation: `ttlMs` defaults to and is capped at 86400000 (24 hours; values are clamped into [10000, 86400000]), persisted as `expiresAt` on the hook. When the TTL elapses the hook expires (terminal state `expired`; a run already in flight completes normally, and its dispatch still wins) and you are woken so you can schedule a new hook if the condition is still worth watching. Set `ttlMs` to your estimated time-to-fire plus reasonable margin rather than defaulting to the cap, so expiry doubles as an "overdue — reassess" wake.
    `perpetual: true` makes a dispatch NON-terminal: you are woken exactly as usual, then the hook returns to `scheduled` with a fresh `nextRunAt` and keeps running on its cadence until its TTL elapses (or you cancel it, or a failing run evicts it) — so one hook can report a stream of changes instead of firing once. Each perpetual fire's wake states both facts (it fired, and it stays active until `expiresAt`) and points at `ws.hook.cancel`; the expiry notice reports runs AND dispatches. A dispatching validation run on a perpetual hook wakes you AND persists the active schedule. Omitted (or `false`) is the default one-shot hook: the first dispatch retires it.
  ws.hook.list() → [hooks]  // Hooks in this workspace (every agent's, not just yours) with `hookId`, `agentId` (the owning agent), `name`, `state` (scheduled|running|dispatched|evicted|cancelled|expired), `nextRunAt`, `expiresAt` (TTL deadline, ≤ 24 h from creation), `runCount`, `perpetual`, `dispatchCount` (fires so far — only perpetual hooks ever exceed 1), `lastError?`, `lastState?` (the carry-over state JSON from the most recent run).
  ws.hook.cancel(hookId) → { ok, hook }  // Stop one of YOUR OWN active hooks. Hooks are agent-owned: cancelling a hook whose `agentId` is another agent is rejected with an error naming the owner — check `agentId` from `ws.hook.list()` before cancelling, and ask the owning agent instead.
  ws.hook.runNow(hookId) → { ok, hookId }  // Trigger an immediate run of an active hook; its inter-run timer resets after the run.

  ws.browser.exec(actions, tabId?) → result | results[]  // Chrome DevTools browser automation. Each action is an object with an `action` field; common actions include `listTabs`, `focusTab`, `getAccessibilityTree`, `screenshot`, `evaluate`, `navigate`, `openTab`, `closeTab` (requires an explicit `tabId`; no default-tabId fallback), `snapshot`, and capture/trace actions.
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
  ws.file.getAttachment(attachmentId, destDir?) → { path, fileName, mimeType?, size, uploadedAt }  // Copies a user-uploaded attachment (referenced by an attachment notice in a message) into your working directory (default `.intent/attachments/`, git-ignored) and returns the relative `path` to read it from. Skips the copy when an identical file is already present. If the attachment's file was deleted by the user, the error says so — continue without the file instead of retrying.

  ws.pr.monitor(prNumber, { repo? }) → { ok, monitor, requirements }  // PREFERRED way to watch a PR: registers a daemon-run monitor on `prNumber` (workspace repo unless `repo: "owner/name"` overrides it) and returns the merge-requirements checklist right now — `requirements` carries `state`, `isDraft`, `hasConflicts`, `isBehind`, `mergeable`, `checks` (with `failingRequired` / `pendingRequired` named, `requiredKnown` false when the host did not report which checks are required), `approvals` (`decision`, `have`, `needed?`, `changesRequested`), `threads` (`unresolved`, `resolutionRequired?`), `mergeStateStatus?`, `mergeBlockedReason?` and `rulesKnown`.
    The daemon polls the PR for you and wakes you with ONE consolidated message after the PR has been quiet for the debounce window, so a stream of comments/checks does not wake you repeatedly. Merge or close stops the monitor with an immediate final wake; the monitor otherwise has NO TTL and survives daemon restarts — this is why it beats a self-authored polling hook for PR watching. Re-registering the same PR is idempotent: it refreshes the baseline instead of adding a second monitor.
  ws.pr.unmonitor(prNumber, { repo? }) → { ok, monitor }  // Stop monitoring a PR you registered. Errors when you have no active monitor on it; you can only cancel your own monitors, and your own cancel never wakes you.
  ws.pr.monitors() → [monitors]  // YOUR active and completed monitors: `monitorId`, `repo`, `prNumber`, `title`, `url`, `state` (active|completed), `lastSnapshot` (the last-refresh checklist summary), `pendingChanges` / `hasPendingChanges` (the net changes since the last report, awaiting the debounce emit), `lastChangeAt?`, `lastPolledAt?`, `lastError?`.
  ws.pr.snapshot(prNumber, { repo? }?) → { repo, prNumber, title, url, state, isDraft, isMerged, isClosed, headSha, updatedAt, mergeable, mergeableState, mergeBlockedReason, checks: { total, passed, failed, pending, failedNames }, reviews: { decision, approvals, changesRequested }, comments: { conversationCount, reviewCommentCount, unresolvedThreadCount, totalCount }, requirements: { state, isDraft, hasConflicts, isBehind, mergeable?, checks: { total, passed, failed, pending, items, failingRequired, pendingRequired, requiredKnown }, approvals: { decision, have, needed?, changesRequested }, threads: { unresolved, resolutionRequired? }, mergeStateStatus?, mergeBlockedReason?, rulesKnown } }  // Compact, diff-friendly ONE-SHOT read of PR `prNumber`, scoped to the workspace repo unless `repo: "owner/name"` overrides it (e.g. a submodule's repo); the result echoes the resolved `repo` so a wrong-repo read is detectable. `prNumber` is required — no active-PR fallback.
    `requirements` is the full merge-requirements checklist — what is still needed to merge — with `failingRequired` / `pendingRequired` naming the required checks, `requiredKnown` false when the host did not report which checks are required, and `rulesKnown` false when the base branch's rules were unreadable (`approvals.needed` / `threads.resolutionRequired` then omitted). The top-level `checks` / `reviews` / `comments` blocks are the compact projection of the same read.
    This is the SAME enriched object `ws.pr.monitor` returns and monitor wakes / `ws.pr.monitors` rows carry — one canonical shape across all three surfaces — except that a snapshot registers nothing and triggers no monitoring. For PR monitoring prefer `ws.pr.monitor` — it runs the polling, debouncing and merge detection in the daemon, so you do not have to author a hook that diffs snapshots and expires while the PR sits blocked. Use `ws.pr.snapshot` when you just want the current state once.
    These are the only `ws.pr.*` methods. For every other PR operation — create, view, comment, review threads, branch update, merge — use the `gh` CLI instead.

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

Namespaces (index — full signatures in API below):
  ws.help(namespace?) — runtime docs: ws.help() returns this index, ws.help("pr") the full pr docs
  ws.workspace.* — workspace info, title, status message
  ws.app.* — chief app surface: agents, proposal, settings, specialists, ui, workspaces
  ws.app.question.* — ask the user structured questions
  ws.note.* — notes; the spec is note id "spec"
  ws.comment.* — comment threads on notes
  ws.task.* — task notes + checkbox statuses
  ws.primitive.* — rich note blocks (reference/cli/patch/agent-action)
  ws.agent.* — create/delegate/message/watch agents
  ws.git.* — attributed commits + secondary git root registry
  ws.event.* — activity queries + event subscriptions
  ws.script.* — saved build/test/service scripts
  ws.hook.* — background watchers; can call full ws.* incl. pr.snapshot
  ws.browser.* — Chrome DevTools browser automation
  ws.terminal.* — read workspace terminal output
  ws.crossWorkspace.* — read sibling-workspace notes
  ws.file.* — read/write workspace project files
  ws.pr.* — pr.monitor = daemon-run PR watch (preferred); pr.snapshot = one-shot state; other PR ops use `gh`

API:
  ws.help(namespace?) → string  // Offline API docs, robust to clients that truncate this description: `ws.help()` returns the Namespaces index; `ws.help("pr")` returns the full doc lines for one namespace. Namespaces disabled in settings are omitted and error when requested.

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
  ws.app.question.ask({ header, question, options, explanation?, multiSelect? }) → { ok, attachmentId, message }  // Ask the user ONE structured clarifying question. REQUIRED: `header` (short topic label), `question` (the prompt text), and `options` — an array of at least 2 OBJECTS [{ label, description? }] (NOT bare strings); do NOT add an "Other" option, a free-form answer is always offered automatically. Example: ws.app.question.ask({ header: "Auth method", question: "Which auth should the endpoint use?", options: [{ label: "OAuth", description: "OAuth 2.0 flow" }, { label: "API key", description: "Static key in header" }] }). Call once per question (aim for at most ~4 questions per turn); `multiSelect: true` lets the user pick several. Questions are presented when your turn ends; the answers arrive as plain-text Q:/A: pairs in the next user message ("(skipped)" for skipped questions). Ask all your questions, then finish the turn.
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
  ws.note.create(title, content, tags?) → { id, title, tags, link, markdownLink, convertedCount, createdTaskNoteIds, createdTasks, warnings }  // Create a new note and return canonical `intent://local/{workspaceId}/note/{noteId}` links. Share `markdownLink` with users so they can open the note. `@@@task` blocks in the content auto-convert into linked task notes, and the result carries the conversion's `createdTasks` + `warnings` like the content-write ops. DO NOT use this for the spec: the spec already exists as note ID `spec`; edit or add to it instead.
  ws.note.list(tag?) → [{ id, title, tags, ... }]  // List notes. Optional tag filter narrows results.
  ws.note.listTasks(id) → [{ text, status, taskNoteId, linkedTaskNoteId, lineNumber, ... }]  // Faster than `read()` when you only need checkbox/task IDs. Use `taskNoteId` for delegation; `linkedTaskNoteId` is a backward-compatible alias.
  ws.note.readAsset(asset) → { assetId, mimeType, data, sizeKb }  // `asset` can be an asset ID or `workspace-asset://...` URL. Image assets (PNG, JPEG, GIF, WebP) are returned as native image content blocks (the model sees the image directly); non-image assets return the JSON object.
  ws.note.setContent(id, content, confirmReplacement?) → { ... }  // ⚠️ FULL REPLACEMENT: replaces the entire note. Prefer `add()` / `edit()` / `editLines()` unless you intentionally want to overwrite everything.
    If the new content is much shorter, call again with `confirmReplacement=true`. `@@@task` blocks auto-convert into linked task notes; the fence line takes optional `key=` / `dependsOn=` / `conflictsWith=` / `effort=` attributes (see `ws.task.convertBlocks`), and every content-write result (`add` / `edit` / `editLines` / `setContent`) carries the conversion's `createdTasks` + `warnings`.
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
  ws.task.markAsTask(noteId, status, { acceptanceCriteria?, effort?, dependsOn?, conflictsWith? }) → { ... }  // Convert a note into a task note. `acceptanceCriteria` may be an array or JSON string; `effort` maps to estimated effort; `dependsOn`/`conflictsWith` seed task relations (validated like `setRelations`).
  ws.task.setRelations(noteId, { dependsOn?, conflictsWith? }) → { ok, noteId, dependsOn, conflictsWith }  // Replace a task's relation lists (arrays of task note ids). Omitted field → kept; `[]` → cleared. `dependsOn` writes that close a dependency cycle or reference a tree ancestor/descendant of the task are rejected with the offending path/relationship named.
  ws.task.convertBlocks(noteId) → { convertedCount, createdNoteIds, createdTasks, warnings }  // Convert `@@@task` blocks into linked task notes. Note updates already auto-convert them; use this for manual re-conversion. The fence line takes optional attributes — `@@@task key=api dependsOn=a,b conflictsWith=c effort=2h` — bare tokens, comma-separated lists, whitespace-tolerant; `dependsOn`/`conflictsWith` values resolve against sibling block `key=`s, then exact sibling titles, then existing task-note ids, and `effort` seeds the estimated effort.
    Conversion never fails on bad attributes: blocks always convert, and unresolvable/ambiguous references or rejected edges (cycle, tree ancestor/descendant) are skipped with a warning naming the block and reference. `createdTasks` is `[{ key?, title, noteId }]` in block order; check `warnings` after converting.
  ws.task.createPrerequisite(dependentNoteId, title, { content?, status? }) → { ... }  // Adds a prerequisite task dependency.
  ws.task.assignAgent(noteId, agentId) → { ok, noteId, agentId }  // Assign an existing agent to a task note. `agentId` must be `agent-{uuid}`; to create and assign in one step, use `ws.agent.create(..., { taskNoteId: noteId })`.

  ws.primitive.addReference(noteId, semanticId, description, snapshot?) → { ok, primitiveId, noteId }  // Code reference primitive; `semanticId` examples: `src/file.ts#symbol:Foo` or `src/file.ts#L10-20`.
  ws.primitive.addCli(noteId, command, description, workingDirectory?) → { ok, primitiveId, noteId }  // CLI primitive; optional cwd is relative to workspace root.
  ws.primitive.addPatch(noteId, filePath, diff, description) → { ok, primitiveId, noteId }  // Stores a patch block that can be applied in a note.
  ws.primitive.addAgentAction(noteId, agentId, goal, description) → { ok, primitiveId, noteId }  // Adds a triggerable agent action block.

  ws.agent.create(name, message, opts?) → { ok, id?, text?, ... }  // Create and start an agent immediately. You are auto-subscribed to its completion events and will be woken when it finishes.
    Specialists include `"implementor"` for implementation work and `"verifier"` for review/verification. `createLinkedNote=true` with `noteContent` creates a linked note; agents are background by default unless `isBackground=false`.
    You can override specialist defaults with `model`, `reasoningEffort`, or `behaviorPrompt`. A `reasoningEffort` the resolved model does not support is rejected with the list of valid values.
    Pass `mergeOnTurnEnd: false` to keep a sandboxed agent's sandbox unmerged when its turn ends — inspect it and merge later (fan out several sandboxed agents, then pick); default true auto-merges on completion.
    Pass `vmResources: { vcpus?, memMib? }` to size a microVM-sandboxed agent's VM (vcpus 1-16, memMib >= 128); missing fields fall back to the `sandbox.microvm` settings. Accepted and ignored outside microVM workspaces.
  ws.agent.delegate({ taskNoteId?, noteId?, taskText?, agentInstructions?, specialist?, model?, reasoningEffort?, behaviorPrompt?, waitMode?, skipAutoCommit?, mergeOnTurnEnd?, vmResources?, tasks? }) → { ok, text?, ... }  // Delegate an existing task to a new agent. Prefer `taskNoteId` from `intent://local/task/{id}`; otherwise pass `noteId` + exact `taskText` from a checkbox.
    Delegation starts immediately and auto-subscribes you to completion events. `waitMode`: `"immediate"` wakes after each agent, `"after_all"` wakes after the whole group. Example: `taskNoteId: "abc-123"`. Completion wakes may carry an advisory `Tasks now unblocked by this completion: …` (or `by these completions:` when coalesced) section naming tasks that just became startable (computed fresh at delivery time); nothing auto-starts — delegate the ones you want started.
    `reasoningEffort` sets the child's reasoning level (e.g. `"low"` / `"medium"` / `"high"`); omit it to inherit the chosen model option's effort, else the specialist's own default. A level the resolved model does not support is rejected with the list of valid values.
    Pass `mergeOnTurnEnd: false` to keep a sandboxed agent's sandbox unmerged when its turn ends — inspect it and merge later (fan out several sandboxed agents, then pick); default true auto-merges on completion.
    Pass `vmResources: { vcpus?, memMib? }` to size a microVM-sandboxed agent's VM (vcpus 1-16, memMib >= 128); missing fields fall back to the `sandbox.microvm` settings. Accepted and ignored outside microVM workspaces.
    Batch form: each `tasks` entry is a bare taskNoteId or `{ taskNoteId, specialist?, model?, reasoningEffort? }` (per-task overrides of the call's top-level defaults). Every listed task is classified and only the eligible subset starts — tasks with unmet `dependsOn` are `held:blocked-on-deps`, tasks whose `conflictsWith` overlaps the running/starting set are `held:conflict` (delegate a held task individually to force it past the hold), and already-running/complete/cancelled tasks are `skipped` (re-calling with the same list is idempotent). Startable tasks are admitted in effort-weighted critical-path priority order (task `estimatedEffort` strings are parsed; unparseable/missing default to 30 min), so a conflict is resolved in favor of the task heading the longest remaining dependent chain, not the one listed first. `agentInstructions` and `force` are rejected alongside `tasks` (each started task's first message resolves from its own task note; occupied tasks classify as `skipped`). The result enumerates every task with disposition + reason and an `unlockPlan` naming what becomes startable at settlement; when any requested chain carries an explicit estimate the plan also carries `criticalPathMinutes` (~N min of serial work remaining on the critical path; spans the requested tasks and their downstream dependents only — incomplete upstream deps outside the request are not counted, and the number reflects only estimated chains, so it can understate when an unestimated chain is longer). Rows for tasks the graph does not cover — no `dependsOn`/`conflictsWith` of their own and not referenced by any other requested task's relations — classify exactly as before (the flag never changes a disposition) but carry `relationsUnknown: true`, and the summary counts the started ones.
  ws.agent.send(agentId, message, priority?) → { ok, agentId, ... }  // Send a message to another agent. Delivers with interrupt priority by DEFAULT: the target is stopped mid-response and the message is delivered immediately. Pass `priority="queue"` to opt out and queue the message if the target is busy.
  ws.agent.sendToTask(taskNoteId, message, priority?) → { ok, taskNoteId, ... }  // Follow up with the agent assigned to a task note; more convenient than `send()` when you only know the task note ID. Same interrupt-by-default delivery as `send()`; `priority="queue"` opts out.
  ws.agent.subscribe(eventTypes, { excludeSelf?, batchWindow? }) → { subscriptionId, ... }  // Compatibility alias for `ws.event.subscribe()`. `eventTypes` must be an array.
  ws.agent.unsubscribe(subscriptionId) → { ok, subscriptionId }  // Compatibility alias for `ws.event.unsubscribe()`.
  ws.agent.watch(agentId) → { ok, subscriptionId, agentId }  // Watch another agent: you are woken once, at its next completion (it goes idle with an empty pending message queue, fails, or is deleted), and the watch is then retired. Blocker/discussion attention wakes are delivered along the way without ending the watch. Watch again if you care about future turns. A watch adopted into an `after_all` delegation group ends at group settlement and cannot be unwatched while grouped (use `agent.cancelSubscriptions` with the groupId).
  ws.agent.unwatch(subscriptionIdOrAgentId) → { ok, removed }  // Stop watching an agent (accepts the watch's subscriptionId or the watched agentId).
  ws.agent.list(includeCompleted?) → [agents]  // Lists agents in this workspace; completed agents are omitted unless requested. Sandboxed agents carry `metadata.sandboxId`/`sandboxPath`/`sandboxBranch`.
  ws.agent.status(agentId) → agent  // Detailed agent status including task linkage and activity timestamps. Sandboxed agents additionally surface `sandboxStatus` and `mergeOnTurnEnd` (sandbox merge state).
  ws.agent.mergeSandbox(agentId) → { ok, agentId, status, reason? }  // Start a sandbox merge-back into the workspace repo — the follow-up to `mergeOnTurnEnd: false` (inspect the sandbox, then merge when ready). ASYNC: returns immediately with `status: "started"` (merge runs in the background; large repos take minutes) or `"in_progress"` (a merge already owns the sandbox). Poll `ws.agent.status(agentId).sandboxStatus` for the outcome — `merged`, `conflict` (terminal; work preserved on an `sb/…` branch in the workspace repo), `merge_pending` (blocked/failed; retried automatically), or unchanged on a dirty bounce (uncommitted sandbox changes with workspace auto-commit off). Errors if the agent has no sandbox.
  ws.agent.diagnostics({ agentId?, taskNoteId?, includeCompleted?, staleRespondingAfterMs? }?) → { diagnostics, text }  // Sanitized snapshot of agent statuses, subscriptions, queues, delegation groups, delivery stats, recent delivery events, and stuck-risk signals.
  ws.agent.snapshot() → { time, hooks?, agentWatches?, queuedMessages?, eventSubscriptions?, runningSubAgents?, numQuestionsAsked?, pendingAttention? }  // YOUR OWN compact state digest (the cheap counterpart to `diagnostics`): active hooks, sub-agent watches, queued messages, event subscriptions, unsettled child agents, pending structured questions, and any unresolved blocker/discussion you raised. Zero/absent fields are omitted; `time` is current UTC.
  ws.agent.wakeOrCreate(taskNoteId, contextMessage, model?, messageMetadata?, reasoningEffort?) → { ... }  // Ensure a task has a working agent: checks assigned agents, resumes a running/restorable one if possible, otherwise creates a new agent for the task. `reasoningEffort` applies only when a new agent is created.
  ws.agent.readConversation(agentId, { lastN?, startTurn?, endTurn?, includeToolCalls? }) → messages  // Read another agent's conversation history.
  ws.agent.summary(agentId) → summary  // Quick summary of what another agent did.
  ws.agent.reportToParent(report) → { ok, ... }  // Send a concise report on completed or progressing work to the parent agent — if you are blocked or need input, use `ws.agent.reportBlocker`/`ws.agent.requestDiscussion` instead. Only works for delegated agents; user-created agents will get an error.
  ws.agent.requestDiscussion(reason) → { ok, kind, reason, savedAt }  // Raise a pending attention request when you need user/coordinator input to proceed — call it BEFORE ending your turn. `reason` is required. Available to every agent; if you have a linked task it moves to `discussion_needed`.
  ws.agent.reportBlocker(reason) → { ok, kind, reason, savedAt }  // Report an infrastructure/environment problem you cannot resolve (broken sandbox, failing environment, missing credentials) — call it BEFORE ending your turn. `reason` is required. Available to every agent; if you have a linked task it moves to `blocked`.

  ws.git.commit(message, { files?, userRequested? }) → { ok, hash, files, fileCount }  // The commit helper. Auto-stages only your changes and is mainly for explicit user-requested checkpoint commits.
    If workspace auto-commit is disabled, set `userRequested=true` to confirm the user asked for the commit.
    For status/stage/diff/merge-check and every other git read or write, run the plain `git` CLI instead.
  ws.git.registerRoot(path) → { id, workspaceId, path, source, repoOwner?, repoName?, branch?, ... }  // Register a secondary git repository (submodule checkout, sibling clone) for the workspace's git root tracking. `path` must be an existing git repo root (has a `.git` entry); a relative path resolves against the workspace worktree, and the result is canonicalized and may live anywhere on the host. The workspace's own primary root is rejected (tracked implicitly). Idempotent by canonical path — re-registering merges attribution and upgrades an auto-detected row to `source: "agent"`.
  ws.git.unregisterRoot(path) → { ok, gitRootId, path }  // Remove a registered secondary git root by path (relative paths resolve against the workspace worktree). Errors when no root is registered for the path.
  ws.git.listRoots() → [{ id, workspaceId, path, source, repoOwner?, repoName?, branch?, ... }]  // List the workspace's registered secondary git roots; `branch` is read live per call.

  ws.event.agentActivity(agentId?, minutesAgo?) → [events]  // With `agentId`, narrows to that agent; otherwise returns recent activity window.
  ws.event.workspaceSummary(minutesAgo?) → summary  // Aggregated workspace activity summary.
  ws.event.query({ eventType?, actorType?, actorId?, path?, minutesAgo?, limit? }) → [events]  // Advanced event query filters. `eventType` accepts the same glob syntax as subscribe: a category wildcard like `note:*`, an exact type like `note:updated`, or bare `*` for no type filter.
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

  ws.hook.schedule({ name, code, delayMs, ttlMs?, perpetual? }) → { hook, dispatched }  // Register a background hook: a small JS script the daemon runs every `delayMs` ms (min 10000) until it returns `{ dispatch: true, message }` (you are woken with the message and the hook ends), throws/times out (evicted, you are woken with the error), is cancelled, or expires. `name` ≤ 50 chars — a short human-readable description of what the hook watches (shown to the user). The first run happens immediately as validation: a failure rejects the call, a dispatch wakes you right away (`dispatched: true`) without persisting a schedule.
    The script runs with this same `ws.*` API available — the full surface, including `ws.pr.snapshot` — and a 60s budget per run, so make hooks self-checking: the hook performs the check itself and dispatches only on a meaningful change (diffed against `hookState`), not a bare timer that wakes you to do the check. Return `{ dispatch: false }` or nothing to keep watching. Use hooks to watch for conditions (CI results, PR activity, file changes) instead of blocking or polling in your own turn — idle turns time out after ~30 minutes of silence, so hooks are how to wait for slow external conditions. For PR monitoring prefer `ws.pr.monitor` — a hook has a TTL and expires while a PR sits blocked, the monitor does not.
    Carry state between runs: a returned `state` field (any JSON value, ~16 KiB cap) persists and is injected into the next run as the `hookState` global (`null` on the first run); omit `state` to keep the previous value, return `state: null` to clear it.
    Every hook has a TTL counted from creation: `ttlMs` defaults to and is capped at 86400000 (24 hours; values are clamped into [10000, 86400000]), persisted as `expiresAt` on the hook. When the TTL elapses the hook expires (terminal state `expired`; a run already in flight completes normally, and its dispatch still wins) and you are woken so you can schedule a new hook if the condition is still worth watching. Set `ttlMs` to your estimated time-to-fire plus reasonable margin rather than defaulting to the cap, so expiry doubles as an "overdue — reassess" wake.
    `perpetual: true` makes a dispatch NON-terminal: you are woken exactly as usual, then the hook returns to `scheduled` with a fresh `nextRunAt` and keeps running on its cadence until its TTL elapses (or you cancel it, or a failing run evicts it) — so one hook can report a stream of changes instead of firing once. Each perpetual fire's wake states both facts (it fired, and it stays active until `expiresAt`) and points at `ws.hook.cancel`; the expiry notice reports runs AND dispatches. A dispatching validation run on a perpetual hook wakes you AND persists the active schedule. Omitted (or `false`) is the default one-shot hook: the first dispatch retires it.
  ws.hook.list() → [hooks]  // Hooks in this workspace (every agent's, not just yours) with `hookId`, `agentId` (the owning agent), `name`, `state` (scheduled|running|dispatched|evicted|cancelled|expired), `nextRunAt`, `expiresAt` (TTL deadline, ≤ 24 h from creation), `runCount`, `perpetual`, `dispatchCount` (fires so far — only perpetual hooks ever exceed 1), `lastError?`, `lastState?` (the carry-over state JSON from the most recent run).
  ws.hook.cancel(hookId) → { ok, hook }  // Stop one of YOUR OWN active hooks. Hooks are agent-owned: cancelling a hook whose `agentId` is another agent is rejected with an error naming the owner — check `agentId` from `ws.hook.list()` before cancelling, and ask the owning agent instead.
  ws.hook.runNow(hookId) → { ok, hookId }  // Trigger an immediate run of an active hook; its inter-run timer resets after the run.

  ws.browser.exec(actions, tabId?) → result | results[]  // Chrome DevTools browser automation. Each action is an object with an `action` field; common actions include `listTabs`, `focusTab`, `getAccessibilityTree`, `screenshot`, `evaluate`, `navigate`, `openTab`, `closeTab` (requires an explicit `tabId`; no default-tabId fallback), `snapshot`, and capture/trace actions.
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
  ws.file.getAttachment(attachmentId, destDir?) → { path, fileName, mimeType?, size, uploadedAt }  // Copies a user-uploaded attachment (referenced by an attachment notice in a message) into your working directory (default `.intent/attachments/`, git-ignored) and returns the relative `path` to read it from. Skips the copy when an identical file is already present. If the attachment's file was deleted by the user, the error says so — continue without the file instead of retrying.

  ws.pr.monitor(prNumber, { repo? }) → { ok, monitor, requirements }  // PREFERRED way to watch a PR: registers a daemon-run monitor on `prNumber` (workspace repo unless `repo: "owner/name"` overrides it) and returns the merge-requirements checklist right now — `requirements` carries `state`, `isDraft`, `hasConflicts`, `isBehind`, `mergeable`, `checks` (with `failingRequired` / `pendingRequired` named, `requiredKnown` false when the host did not report which checks are required), `approvals` (`decision`, `have`, `needed?`, `changesRequested`), `threads` (`unresolved`, `resolutionRequired?`), `mergeStateStatus?`, `mergeBlockedReason?` and `rulesKnown`.
    The daemon polls the PR for you and wakes you with ONE consolidated message after the PR has been quiet for the debounce window, so a stream of comments/checks does not wake you repeatedly. Merge or close stops the monitor with an immediate final wake; the monitor otherwise has NO TTL and survives daemon restarts — this is why it beats a self-authored polling hook for PR watching. Re-registering the same PR is idempotent: it refreshes the baseline instead of adding a second monitor.
  ws.pr.unmonitor(prNumber, { repo? }) → { ok, monitor }  // Stop monitoring a PR you registered. Errors when you have no active monitor on it; you can only cancel your own monitors, and your own cancel never wakes you.
  ws.pr.monitors() → [monitors]  // YOUR active and completed monitors: `monitorId`, `repo`, `prNumber`, `title`, `url`, `state` (active|completed), `lastSnapshot` (the last-refresh checklist summary), `pendingChanges` / `hasPendingChanges` (the net changes since the last report, awaiting the debounce emit), `lastChangeAt?`, `lastPolledAt?`, `lastError?`.
  ws.pr.snapshot(prNumber, { repo? }?) → { repo, prNumber, title, url, state, isDraft, isMerged, isClosed, headSha, updatedAt, mergeable, mergeableState, mergeBlockedReason, checks: { total, passed, failed, pending, failedNames }, reviews: { decision, approvals, changesRequested }, comments: { conversationCount, reviewCommentCount, unresolvedThreadCount, totalCount }, requirements: { state, isDraft, hasConflicts, isBehind, mergeable?, checks: { total, passed, failed, pending, items, failingRequired, pendingRequired, requiredKnown }, approvals: { decision, have, needed?, changesRequested }, threads: { unresolved, resolutionRequired? }, mergeStateStatus?, mergeBlockedReason?, rulesKnown } }  // Compact, diff-friendly ONE-SHOT read of PR `prNumber`, scoped to the workspace repo unless `repo: "owner/name"` overrides it (e.g. a submodule's repo); the result echoes the resolved `repo` so a wrong-repo read is detectable. `prNumber` is required — no active-PR fallback.
    `requirements` is the full merge-requirements checklist — what is still needed to merge — with `failingRequired` / `pendingRequired` naming the required checks, `requiredKnown` false when the host did not report which checks are required, and `rulesKnown` false when the base branch's rules were unreadable (`approvals.needed` / `threads.resolutionRequired` then omitted). The top-level `checks` / `reviews` / `comments` blocks are the compact projection of the same read.
    This is the SAME enriched object `ws.pr.monitor` returns and monitor wakes / `ws.pr.monitors` rows carry — one canonical shape across all three surfaces — except that a snapshot registers nothing and triggers no monitoring. For PR monitoring prefer `ws.pr.monitor` — it runs the polling, debouncing and merge detection in the daemon, so you do not have to author a hook that diffs snapshots and expires while the PR sits blocked. Use `ws.pr.snapshot` when you just want the current state once.
    These are the only `ws.pr.*` methods. For every other PR operation — create, view, comment, review threads, branch update, merge — use the `gh` CLI instead.

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
    if !features.pr_monitor {
        // Method-level, not the whole `ws.pr.` namespace: `ws.pr.snapshot`
        // survives the toggle. `monitors` is listed separately so the
        // dispatch deny (exact match) covers it too.
        out.push(("ws.pr.monitors", "agentFeatures.prMonitor"));
        out.push(("ws.pr.monitor", "agentFeatures.prMonitor"));
        out.push(("ws.pr.unmonitor", "agentFeatures.prMonitor"));
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
const SANDBOX_VM_RESOURCES_DOC_LINE: &str = "    Pass `vmResources: { vcpus?, memMib? }` to size a microVM-sandboxed agent's VM (vcpus 1-16, memMib >= 128); missing fields fall back to the `sandbox.microvm` settings. Accepted and ignored outside microVM workspaces.\n";
const SANDBOX_DELEGATE_VM_RESOURCES_ARG: &str = ", vmResources?";
const SANDBOX_LIST_FIELDS_XREF: &str =
    " Sandboxed agents carry `metadata.sandboxId`/`sandboxPath`/`sandboxBranch`.";
const SANDBOX_STATUS_FIELDS_XREF: &str =
    " Sandboxed agents additionally surface `sandboxStatus` and `mergeOnTurnEnd` (sandbox merge state).";
const SANDBOX_MERGE_SANDBOX_DOC_LINE: &str = "  ws.agent.mergeSandbox(agentId) → { ok, agentId, status, reason? }  // Start a sandbox merge-back into the workspace repo — the follow-up to `mergeOnTurnEnd: false` (inspect the sandbox, then merge when ready). ASYNC: returns immediately with `status: \"started\"` (merge runs in the background; large repos take minutes) or `\"in_progress\"` (a merge already owns the sandbox). Poll `ws.agent.status(agentId).sandboxStatus` for the outcome — `merged`, `conflict` (terminal; work preserved on an `sb/…` branch in the workspace repo), `merge_pending` (blocked/failed; retried automatically), or unchanged on a dirty bounce (uncommitted sandbox changes with workspace auto-commit off). Errors if the agent has no sandbox.\n";

/// The base variant's cross-references to `ws.host.exec` inside the
/// `ws.hook.*` docs (the Namespaces index hint and the `ws.hook.schedule`
/// continuation line), scrubbed from the assembled description when
/// `agentFeatures.hostExec` is off so the surviving hook docs don't advertise
/// a pruned method. The chief variant never names `ws.host.exec`, so the
/// scrub is a no-op there (unit tests guard both needles).
const HOOK_HOST_EXEC_INDEX_XREF: &str = " and host.exec";
const HOOK_HOST_EXEC_DOC_XREF: &str = " and `ws.host.exec`";

/// The cross-references to `ws.pr.monitor` that live OUTSIDE its own doc
/// lines — the `ws.pr.*` Namespaces index entry, the `ws.hook.schedule`
/// steer, and the `ws.pr.snapshot` steer (a whole continuation line, which
/// method-line pruning cannot reach) plus its "only method" phrasing. All are
/// scrubbed when `agentFeatures.prMonitor` is off so the surviving docs never
/// advertise a pruned method (unit tests guard every needle verbatim).
const PR_MONITOR_INDEX_XREF: &str = "pr.monitor = daemon-run PR watch (preferred); ";
const PR_MONITOR_INDEX_SNAPSHOT_LABEL: &str = "pr.snapshot = one-shot state";
const PR_MONITOR_INDEX_SNAPSHOT_LABEL_OFF: &str = "pr.snapshot = compact PR watch state";
const PR_MONITOR_HOOK_XREF: &str = " For PR monitoring prefer `ws.pr.monitor` — a hook has a TTL and expires while a PR sits blocked, the monitor does not.";
const PR_MONITOR_SNAPSHOT_XREF_LINE: &str = "    This is the SAME enriched object `ws.pr.monitor` returns and monitor wakes / `ws.pr.monitors` rows carry — one canonical shape across all three surfaces — except that a snapshot registers nothing and triggers no monitoring. For PR monitoring prefer `ws.pr.monitor` — it runs the polling, debouncing and merge detection in the daemon, so you do not have to author a hook that diffs snapshots and expires while the PR sits blocked. Use `ws.pr.snapshot` when you just want the current state once.\n";
const PR_MONITOR_ONLY_METHODS: &str = "These are the only `ws.pr.*` methods.";
const PR_MONITOR_ONLY_METHODS_OFF: &str = "This is the only `ws.pr.*` method.";

/// Task-graph teaching scrubbed from the assembled description when
/// `agentFeatures.taskGraph` is off (intent-hq/monorepo#2445). Docs only —
/// `delegate({ tasks })` is never dispatch-denied, so this never joins
/// [`gated_prefixes`]. Four scrubs, each guarded verbatim by unit tests:
/// the `tasks?` param of the `ws.agent.delegate` signature, the
/// unblocked-wake advisory sentence on its first continuation line, its
/// whole "Batch form:" continuation line, the `@@@task` fence-attribute
/// clause of `ws.note.setContent`, and the fence-attribute grammar of
/// `ws.task.convertBlocks` (rewritten to name only the result shape).
const TASK_GRAPH_DELEGATE_PARAMS: &str = ", tasks? })";
const TASK_GRAPH_DELEGATE_PARAMS_OFF: &str = " })";
const TASK_GRAPH_UNBLOCKED_WAKE_XREF: &str = " Completion wakes may carry an advisory `Tasks now unblocked by this completion: …` (or `by these completions:` when coalesced) section naming tasks that just became startable (computed fresh at delivery time); nothing auto-starts — delegate the ones you want started.";
const TASK_GRAPH_BATCH_FORM_LINE: &str = "    Batch form: each `tasks` entry is a bare taskNoteId or `{ taskNoteId, specialist?, model?, reasoningEffort? }` (per-task overrides of the call's top-level defaults). Every listed task is classified and only the eligible subset starts — tasks with unmet `dependsOn` are `held:blocked-on-deps`, tasks whose `conflictsWith` overlaps the running/starting set are `held:conflict` (delegate a held task individually to force it past the hold), and already-running/complete/cancelled tasks are `skipped` (re-calling with the same list is idempotent). Startable tasks are admitted in effort-weighted critical-path priority order (task `estimatedEffort` strings are parsed; unparseable/missing default to 30 min), so a conflict is resolved in favor of the task heading the longest remaining dependent chain, not the one listed first. `agentInstructions` and `force` are rejected alongside `tasks` (each started task's first message resolves from its own task note; occupied tasks classify as `skipped`). The result enumerates every task with disposition + reason and an `unlockPlan` naming what becomes startable at settlement; when any requested chain carries an explicit estimate the plan also carries `criticalPathMinutes` (~N min of serial work remaining on the critical path; spans the requested tasks and their downstream dependents only — incomplete upstream deps outside the request are not counted, and the number reflects only estimated chains, so it can understate when an unestimated chain is longer). Rows for tasks the graph does not cover — no `dependsOn`/`conflictsWith` of their own and not referenced by any other requested task's relations — classify exactly as before (the flag never changes a disposition) but carry `relationsUnknown: true`, and the summary counts the started ones.\n";
const TASK_GRAPH_SETCONTENT_XREF: &str = "; the fence line takes optional `key=` / `dependsOn=` / `conflictsWith=` / `effort=` attributes (see `ws.task.convertBlocks`), and every content-write result (`add` / `edit` / `editLines` / `setContent`) carries the conversion's `createdTasks` + `warnings`";
const TASK_GRAPH_CONVERT_BLOCKS_GRAMMAR: &str = " The fence line takes optional attributes — `@@@task key=api dependsOn=a,b conflictsWith=c effort=2h` — bare tokens, comma-separated lists, whitespace-tolerant; `dependsOn`/`conflictsWith` values resolve against sibling block `key=`s, then exact sibling titles, then existing task-note ids, and `effort` seeds the estimated effort.\n    Conversion never fails on bad attributes: blocks always convert, and unresolvable/ambiguous references or rejected edges (cycle, tree ancestor/descendant) are skipped with a warning naming the block and reference. `createdTasks` is `[{ key?, title, noteId }]` in block order; check `warnings` after converting.";
const TASK_GRAPH_CONVERT_BLOCKS_GRAMMAR_OFF: &str =
    " `createdTasks` is `[{ key?, title, noteId }]` in block order; `warnings` names skipped relation references.";

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
/// workspace is not CoW-capable. With every toggle on — including the opt-in
/// `taskGraph` — and `cow_capable`, this returns the static const unchanged,
/// so the every-gate-open description is byte-identical to today's by
/// construction.
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
    if gated.is_empty() && cow_capable && features.task_graph {
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
        out = out.replacen(SANDBOX_MERGE_SANDBOX_DOC_LINE, "", 1);
        out = out.replacen(SANDBOX_VM_RESOURCES_DOC_LINE, "", 2);
        out = out.replacen(SANDBOX_DELEGATE_VM_RESOURCES_ARG, "", 1);
    }
    // Cross-reference scrub for `hostExec`: the surviving `ws.hook.*` index
    // hint and `ws.hook.schedule` doc line name `ws.host.exec` as callable
    // from hook code, so drop those mentions when the namespace is pruned.
    if !features.host_exec {
        out =
            out.replacen(HOOK_HOST_EXEC_INDEX_XREF, "", 1)
                .replacen(HOOK_HOST_EXEC_DOC_XREF, "", 1);
    }
    // Cross-reference scrub for `prMonitor`: the three monitor doc lines are
    // pruned above, but the surviving `ws.pr.*` index entry, hook steer and
    // `ws.pr.snapshot` continuation lines still point at them.
    if !features.pr_monitor {
        out = out
            .replacen(PR_MONITOR_INDEX_XREF, "", 1)
            .replacen(
                PR_MONITOR_INDEX_SNAPSHOT_LABEL,
                PR_MONITOR_INDEX_SNAPSHOT_LABEL_OFF,
                1,
            )
            .replacen(PR_MONITOR_HOOK_XREF, "", 1)
            .replacen(PR_MONITOR_SNAPSHOT_XREF_LINE, "", 1)
            .replacen(PR_MONITOR_ONLY_METHODS, PR_MONITOR_ONLY_METHODS_OFF, 1);
    }
    // Teaching scrub for `taskGraph` (intent-hq/monorepo#2445): docs only —
    // the APIs stay dispatchable — so the batch-delegate params, batch-form
    // line, unblocked-wake advisory, and fence-attribute grammar disappear
    // from the description without joining `gated_prefixes`.
    if !features.task_graph {
        out = out
            .replacen(
                TASK_GRAPH_DELEGATE_PARAMS,
                TASK_GRAPH_DELEGATE_PARAMS_OFF,
                1,
            )
            .replacen(TASK_GRAPH_UNBLOCKED_WAKE_XREF, "", 1)
            .replacen(TASK_GRAPH_BATCH_FORM_LINE, "", 1)
            .replacen(TASK_GRAPH_SETCONTENT_XREF, "", 1)
            .replacen(
                TASK_GRAPH_CONVERT_BLOCKS_GRAMMAR,
                TASK_GRAPH_CONVERT_BLOCKS_GRAMMAR_OFF,
                1,
            );
    }
    Cow::Owned(out)
}

/// `ws.help()` — the runtime docs index. Returns the `Namespaces` block of
/// the assembled description (header + entries), so chief-ness and
/// `[agentFeatures]` gating apply exactly as they do to the advertised tool
/// description, plus a hint on how to fetch one namespace's full docs.
pub(super) fn help_index(is_chief: bool, features: &AgentFeaturesSettings) -> String {
    // `ws.help` renders the capable (full) docs: the CoW-capability gate is
    // an advisory description-only scrub and the host dispatch path does not
    // carry the per-bridge capability; dispatch accepts and ignores
    // `mergeOnTurnEnd` either way.
    let desc = workspace_api_description(is_chief, features, true);
    let block: Vec<&str> = desc
        .lines()
        .skip_while(|l| !l.starts_with("Namespaces"))
        .take_while(|l| !l.trim().is_empty())
        .collect();
    let mut out = block.join("\n");
    out.push_str("\n\nCall ws.help(\"<namespace>\") for one namespace's full signatures.");
    out
}

/// `ws.help("<namespace>")` — one namespace's full doc lines, cut verbatim
/// from the assembled description (every indent-2 `ws.<ns>.` method line plus
/// its indented continuation lines). Accepts forgiving spellings: `"pr"`,
/// `"ws.pr"`, `"pr.*"`, and nested names like `"app.question"`; `"help"`
/// resolves to the `ws.help(...)` entry itself. Errors name the disabling
/// `[agentFeatures]` toggle for gated-off namespaces and list the available
/// namespaces for unknown ones — except `app.question` on a sub-agent bridge
/// (whose effective features force `structuredQuestions` off), where the
/// honest reason is the top-level-only rule, not a settings toggle.
pub(super) fn help_namespace(
    is_chief: bool,
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    namespace: &str,
) -> Result<String, String> {
    let ns = namespace
        .trim()
        .trim_start_matches("ws.")
        .trim_end_matches('*')
        .trim_end_matches('.');
    let desc = workspace_api_description(is_chief, features, true);
    if !ns.is_empty() {
        // Direct calls like `ws.help(...)` are documented without a trailing
        // dot, so match both the `ws.<ns>.` and `ws.<ns>(` spellings.
        let dot_prefix = format!("ws.{ns}.");
        let call_prefix = format!("ws.{ns}(");
        let mut lines: Vec<&str> = Vec::new();
        let mut in_segment = false;
        for line in desc.lines().skip_while(|l| !l.starts_with("API:")) {
            let trimmed = line.trim_start();
            let indent = line.len() - trimmed.len();
            if indent == 2 && trimmed.starts_with("ws.") {
                in_segment = trimmed.starts_with(&dot_prefix) || trimmed.starts_with(&call_prefix);
            } else if indent < 4 || trimmed.is_empty() {
                in_segment = false;
            }
            if in_segment {
                lines.push(line);
            }
        }
        if !lines.is_empty() {
            return Ok(lines.join("\n"));
        }
        let gate = format!("ws.{ns}.");
        if let Some((prefix, feature)) = gated_prefixes(features)
            .into_iter()
            .find(|(prefix, _)| gate.starts_with(prefix) || prefix.starts_with(&gate))
        {
            // Sub-agent question gate FIRST, matching the dispatch layer: a
            // sub-agent bridge's effective features force `structuredQuestions`
            // off, which would otherwise misattribute the pruning to settings.
            if is_sub_agent && prefix == "ws.app.question." {
                return Err(format!(
                    "namespace `{ns}` — {}",
                    super::dispatch::SUB_AGENT_QUESTION_DENIED
                ));
            }
            return Err(format!(
                "namespace `{ns}` is disabled in settings ({feature} = false)"
            ));
        }
    }
    let available: Vec<String> = desc
        .lines()
        .skip_while(|l| !l.starts_with("Namespaces"))
        .skip(1)
        .take_while(|l| !l.trim().is_empty())
        .filter_map(|l| {
            let rest = l.trim_start().strip_prefix("ws.")?;
            if let Some((name, _)) = rest.split_once(".*") {
                Some(name.to_string())
            } else {
                // The index's `ws.help(namespace?)` entry has no `.*`.
                rest.starts_with("help(").then(|| "help".to_string())
            }
        })
        .collect();
    Err(format!(
        "unknown namespace `{namespace}` — available: {}",
        available.join(", ")
    ))
}

/// Live microVM spawn facts injected into the `workspace_api` description of
/// microVM-workspace bridges (alongside the `vmResources`/`mergeOnTurnEnd`
/// clauses): the guest image spawns will use and the settings-resolved
/// default VM size. Built at bridge creation from already-loaded settings and
/// the statically-known image resolution — never from a manifest fetch.
#[derive(Debug, Clone, PartialEq)]
pub struct MicrovmSpawnHints {
    /// Human-readable label of the resolved guest image: the built-in pin's
    /// version when no override applies, else the override's manifest URL
    /// (the id/version live inside the manifest, which must not be fetched
    /// at description-build time).
    pub image_label: String,
    /// Settings-resolved default vCPU count (`sandbox.microvm.vcpus`).
    pub vcpus: u8,
    /// Settings-resolved default guest memory MiB (`sandbox.microvm.memMib`).
    pub mem_mib: u32,
}

/// [`workspace_api_description`] plus the per-specialist delegation model
/// options (PROTOCOL §5.11 `modelOptions`) and the microVM spawn hints, both
/// injected as continuation lines of the `ws.agent.delegate` doc entry so
/// delegating agents see which models a specialist's author suggests and
/// what a spawned agent gets (guest image, default VM size). `model_options`
/// lists only specialists that carry options and `microvm_hints` is `Some`
/// only for microVM-capable workspaces; with neither — the all-defaults case
/// — the assembled text is returned unchanged, so the default description
/// stays byte-identical by construction.
pub fn workspace_api_description_with_model_options(
    is_chief: bool,
    features: &AgentFeaturesSettings,
    cow_capable: bool,
    model_options: &[SpecialistModelOptions],
    microvm_hints: Option<&MicrovmSpawnHints>,
) -> Cow<'static, str> {
    let base = workspace_api_description(is_chief, features, cow_capable);
    let mut block = String::new();
    if let Some(hints) = microvm_hints {
        block.push_str(&microvm_hints_block(hints));
    }
    if !model_options.is_empty() {
        block.push_str(&model_options_block(model_options));
    }
    if block.is_empty() {
        return base;
    }
    // Anchor on the `ws.agent.delegate` doc line (indent 2) and append the
    // block after its indented continuation lines, so the injected text
    // reads as part of the delegate/create docs. No anchor (the line can
    // never be feature-pruned today, but stay safe) → unchanged.
    let mut out = String::with_capacity(base.len() + 256);
    let mut in_delegate = false;
    let mut inserted = false;
    for line in base.lines() {
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();
        let is_continuation = indent >= 4 && !trimmed.is_empty();
        if in_delegate && !is_continuation && !inserted {
            out.push_str(&block);
            inserted = true;
        }
        if indent == 2 && trimmed.starts_with("ws.") {
            in_delegate = trimmed.starts_with("ws.agent.delegate(");
        }
        out.push_str(line);
        out.push('\n');
    }
    if in_delegate && !inserted {
        out.push_str(&block);
        inserted = true;
    }
    if !inserted {
        return base;
    }
    if !base.ends_with('\n') {
        out.pop();
    }
    Cow::Owned(out)
}

/// Render the microVM spawn-hints continuation line (indent ≥4 so the
/// `[agentFeatures]` pruning treats it as part of the `ws.agent.delegate`
/// entry). Values are flattened onto one line so a pathological image URL
/// cannot break the description's line structure.
fn microvm_hints_block(hints: &MicrovmSpawnHints) -> String {
    let flat = |s: &str| s.replace(['\n', '\r'], " ");
    format!(
        "    microVM spawns here get guest image {} and a default VM size of {} vCPUs / {} MiB unless `vmResources` overrides.\n",
        flat(&hints.image_label),
        hints.vcpus,
        hints.mem_mib,
    )
}

/// Render the injected continuation block: one header line plus one line per
/// specialist naming its resolved default (`` default `<compound id>` ``, or
/// `default: provider default` when resolution yields the provider CLI
/// default) followed by its options as `` `<compound id>` (<hint>) `` entries
/// (the hint parenthetical is omitted when empty, and an option's declared
/// reasoning effort is appended to it as `effort: <level>`). All lines are
/// indented ≥4 so the `[agentFeatures]` pruning treats them as continuation
/// lines of the `ws.agent.delegate` entry. Author-supplied text is flattened
/// onto one line so a multi-line hint cannot break the description's line
/// structure.
fn model_options_block(model_options: &[SpecialistModelOptions]) -> String {
    let flat = |s: &str| s.replace(['\n', '\r'], " ");
    let mut block = String::from(
        "    Specialist model options (pass the compound id as `model` to \
         `ws.agent.delegate`/`ws.agent.create`; omit `model` to use the \
         specialist's default; on `ws.agent.delegate` an option's `effort` is \
         applied automatically unless you pass an explicit `reasoningEffort` — \
         `ws.agent.create` applies only the `reasoningEffort` you pass):\n",
    );
    for spec in model_options {
        block.push_str("      ");
        block.push_str(&flat(&spec.specialist));
        block.push_str(": ");
        let mut entries: Vec<String> = vec![match spec.default_model.as_deref() {
            Some(m) => format!("default `{}`", flat(m)),
            None => "default: provider default".to_string(),
        }];
        entries.extend(spec.options.iter().map(|o| {
            let mut paren: Vec<String> = Vec::new();
            if !o.hint.is_empty() {
                paren.push(flat(&o.hint));
            }
            if !o.reasoning_effort.is_empty() {
                paren.push(format!("effort: {}", flat(&o.reasoning_effort)));
            }
            if paren.is_empty() {
                format!("`{}`", flat(&o.model))
            } else {
                format!("`{}` ({})", flat(&o.model), paren.join("; "))
            }
        }));
        block.push_str(&entries.join(", "));
        block.push('\n');
    }
    block
}

#[cfg(test)]
mod tests {
    use super::{
        denied_feature, help_index, help_namespace, workspace_api_description,
        workspace_api_description_with_model_options, AgentFeaturesSettings, Cow,
        MicrovmSpawnHints, SpecialistModelOption, SpecialistModelOptions, HOOK_HOST_EXEC_DOC_XREF,
        HOOK_HOST_EXEC_INDEX_XREF, PR_MONITOR_HOOK_XREF, PR_MONITOR_INDEX_SNAPSHOT_LABEL,
        PR_MONITOR_INDEX_XREF, PR_MONITOR_ONLY_METHODS, PR_MONITOR_SNAPSHOT_XREF_LINE,
        REPORT_TO_PARENT_ATTENTION_XREF, TASK_GRAPH_BATCH_FORM_LINE,
        TASK_GRAPH_CONVERT_BLOCKS_GRAMMAR, TASK_GRAPH_DELEGATE_PARAMS, TASK_GRAPH_SETCONTENT_XREF,
        TASK_GRAPH_UNBLOCKED_WAKE_XREF, WORKSPACE_API_DESCRIPTION, WORKSPACE_API_DESCRIPTION_CHIEF,
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

    // Parse the `Namespaces` index block near the top of a description into
    // the set of namespaces it names (each entry is `  ws.<ns>.* — hint`).
    // The `ws.help(` entry is a callable, not a namespace — it has its own
    // parity tests below and is skipped here.
    fn index_namespaces(desc: &str) -> HashSet<String> {
        desc.lines()
            .skip_while(|l| !l.starts_with("Namespaces"))
            .skip(1)
            .take_while(|l| !l.trim().is_empty())
            .filter(|l| !l.trim_start().starts_with("ws.help("))
            .map(|l| {
                l.trim_start()
                    .strip_prefix("ws.")
                    .and_then(|rest| rest.split_once(".*"))
                    .map(|(ns, _)| ns.to_string())
                    .expect("index entries are `  ws.<ns>.* — hint` lines")
            })
            .collect()
    }

    // The `Namespaces` index — inserted so MCP clients that truncate long
    // tool descriptions still surface the full ws.* capability map — must
    // stay in lockstep with the API sections: every documented method's
    // namespace is covered by an index entry (directly or via its top-level
    // segment, e.g. `app` covering `app.settings`), and every index entry
    // names a namespace with at least one documented method.
    #[test]
    fn namespace_index_matches_documented_surface() {
        for (variant, desc) in [
            ("base", WORKSPACE_API_DESCRIPTION),
            ("chief", WORKSPACE_API_DESCRIPTION_CHIEF),
        ] {
            let index = index_namespaces(desc);
            assert!(
                !index.is_empty(),
                "{variant} description has no Namespaces index block"
            );
            let documented: HashSet<String> = extract_ws_methods(desc)
                .into_iter()
                .map(|(ns, _)| ns)
                .collect();
            for ns in &documented {
                let top = ns.split('.').next().unwrap();
                assert!(
                    index.contains(ns) || index.contains(top),
                    "{variant}: namespace `{ns}` is documented in the API section \
                     but missing from the Namespaces index"
                );
            }
            for ns in &index {
                let prefix = format!("{ns}.");
                assert!(
                    documented.iter().any(|d| d == ns || d.starts_with(&prefix)),
                    "{variant}: Namespaces index entry `{ns}` has no documented methods"
                );
            }
        }
    }

    // Regression guard for upstream Claude Code bug
    // https://github.com/anthropics/claude-code/issues/53933 ("Tool
    // descriptions silently truncated without user notification or retrieval
    // mechanism"): Claude Code cuts tool descriptions at ~2k chars with no
    // indication anything was lost, so the `Namespaces` capability index MUST
    // fit entirely within the first ~2k chars or truncating clients silently
    // lose the ws.* capability map (the whole reason the index sits above the
    // API sections). This pins the prefix — start of the description through
    // the END of the index block — to a conservative 2000-char budget, for
    // both variants and every `[agentFeatures]` gating combination that
    // matters. All-defaults is the longest prefix (gating only removes index
    // lines), but the single-feature and all-off combinations are swept too
    // so a scrub regression cannot reorder or grow the prefix unnoticed.
    // Future doc additions above or inside the index that push it past the
    // cutoff fail here instead of silently breaking truncating clients.
    #[test]
    fn namespace_index_fits_within_truncation_budget() {
        const BUDGET: usize = 2000;
        let mut feature_sets: Vec<(String, AgentFeaturesSettings)> = vec![
            ("all-defaults".into(), AgentFeaturesSettings::default()),
            ("all-gates-open".into(), all_gates_open()),
        ];
        for (i, (prefixes, disable)) in feature_cases().into_iter().enumerate() {
            let mut features = AgentFeaturesSettings::default();
            disable(&mut features);
            feature_sets.push((format!("case-{i}-{prefixes:?}"), features));
        }
        feature_sets.push((
            "all-off".into(),
            AgentFeaturesSettings {
                background_hooks: false,
                host_exec: false,
                scripts: false,
                terminal_access: false,
                browser_automation: false,
                rich_chat_blocks: false,
                structured_questions: false,
                attention_requests: false,
                state_snapshot: false,
                pr_monitor: false,
                task_graph: false,
            },
        ));
        for is_chief in [false, true] {
            for (label, features) in &feature_sets {
                let desc = workspace_api_description(is_chief, features, true);
                let start = desc
                    .find("Namespaces")
                    .unwrap_or_else(|| panic!("chief={is_chief} {label}: no Namespaces index"));
                // The index block ends at the first blank line after its header.
                let end = desc[start..]
                    .find("\n\n")
                    .map(|i| start + i)
                    .unwrap_or(desc.len());
                assert!(
                    end <= BUDGET,
                    "chief={is_chief} {label}: description prefix through the end of the \
                     Namespaces index is {end} chars, over the {BUDGET}-char truncation \
                     budget (Claude Code cuts descriptions at ~2k chars — see \
                     https://github.com/anthropics/claude-code/issues/53933); move or \
                     shrink text above/inside the index"
                );
            }
        }
    }

    // ---- ws.help() runtime docs tests --------------------------------------

    // The `ws.help` index entry and API doc line appear verbatim in BOTH
    // description variants — same drift guard as `ws.app.question.ask`.
    #[test]
    fn help_doc_lines_are_identical_in_both_variants() {
        for needle in ["ws.help(namespace?) —", "ws.help(namespace?) →"] {
            let line_in = |desc: &str| -> String {
                desc.lines()
                    .find(|l| l.trim_start().starts_with(needle))
                    .unwrap_or_else(|| panic!("description advertises `{needle}`"))
                    .to_string()
            };
            assert_eq!(
                line_in(WORKSPACE_API_DESCRIPTION),
                line_in(WORKSPACE_API_DESCRIPTION_CHIEF),
                "the ws.help doc line drifted between the base and chief descriptions"
            );
        }
    }

    // help_index returns the description's own Namespaces block: every index
    // namespace appears, and gated-off namespaces are omitted.
    #[test]
    fn help_index_mirrors_namespace_index() {
        for (is_chief, desc) in [
            (false, WORKSPACE_API_DESCRIPTION),
            (true, WORKSPACE_API_DESCRIPTION_CHIEF),
        ] {
            let index = help_index(is_chief, &AgentFeaturesSettings::default());
            assert!(index.starts_with("Namespaces"));
            for ns in index_namespaces(desc) {
                assert!(
                    index.contains(&format!("ws.{ns}.*")),
                    "chief={is_chief}: help_index is missing `ws.{ns}.*`"
                );
            }
        }
        let features = AgentFeaturesSettings {
            host_exec: false,
            ..AgentFeaturesSettings::default()
        };
        let gated = help_index(false, &features);
        assert!(!gated.contains("ws.host."));
        assert!(gated.contains("ws.note.*"));
    }

    // help_namespace returns every namespace's doc segment verbatim from the
    // assembled description, for every index namespace in both variants
    // (every gate open, so the assembly is the static const).
    #[test]
    fn help_namespace_returns_verbatim_doc_segments() {
        let features = all_gates_open();
        for (is_chief, desc) in [
            (false, WORKSPACE_API_DESCRIPTION),
            (true, WORKSPACE_API_DESCRIPTION_CHIEF),
        ] {
            for ns in index_namespaces(desc) {
                let docs = help_namespace(is_chief, &features, false, &ns)
                    .unwrap_or_else(|e| panic!("chief={is_chief}: help({ns}) errored: {e}"));
                assert!(
                    docs.lines()
                        .next()
                        .is_some_and(|l| l.trim_start().starts_with(&format!("ws.{ns}."))),
                    "chief={is_chief}: help({ns}) does not start with a ws.{ns}. line"
                );
                assert!(
                    desc.contains(&docs),
                    "chief={is_chief}: help({ns}) is not a verbatim description segment"
                );
            }
        }
        // Forgiving spellings resolve to the same segment.
        let plain = help_namespace(false, &features, false, "pr").unwrap();
        for alias in ["ws.pr", "pr.*", " pr. "] {
            assert_eq!(
                help_namespace(false, &features, false, alias).unwrap(),
                plain
            );
        }
        // `help` resolves to the ws.help entry itself (documented as a call,
        // not a dotted namespace).
        let help_docs = help_namespace(false, &features, false, "help").unwrap();
        assert!(
            help_docs.trim_start().starts_with("ws.help(namespace?)"),
            "help(help) should return the ws.help entry, got: {help_docs}"
        );
    }

    // Gated-off namespaces error naming the disabling toggle; unknown
    // namespaces error listing what is available.
    #[test]
    fn help_namespace_errors_are_actionable() {
        let features = AgentFeaturesSettings {
            host_exec: false,
            ..AgentFeaturesSettings::default()
        };
        let err = help_namespace(false, &features, false, "host").unwrap_err();
        assert!(
            err.contains("agentFeatures.hostExec"),
            "gated error must name the toggle: {err}"
        );
        let err =
            help_namespace(false, &AgentFeaturesSettings::default(), false, "nope").unwrap_err();
        assert!(err.contains("unknown namespace `nope`"), "{err}");
        assert!(
            err.contains("note"),
            "unknown error lists namespaces: {err}"
        );
        assert!(
            err.contains("help"),
            "unknown error lists `help` as available: {err}"
        );
        // Chief-only namespaces are unknown to base workspaces.
        assert!(help_namespace(
            false,
            &AgentFeaturesSettings::default(),
            false,
            "app.workspaces"
        )
        .is_err());
        assert!(help_namespace(
            true,
            &AgentFeaturesSettings::default(),
            false,
            "app.workspaces"
        )
        .is_ok());
    }

    // A sub-agent bridge's help("app.question") names the top-level-only
    // rule, never the settings toggle its effective features force off; when
    // the toggle is GENUINELY off, the settings error stands for every caller.
    #[test]
    fn help_namespace_sub_agent_question_error_names_top_level_rule() {
        let forced_off = AgentFeaturesSettings {
            structured_questions: false,
            ..AgentFeaturesSettings::default()
        };
        let err = help_namespace(false, &forced_off, true, "app.question").unwrap_err();
        assert!(
            err.contains("only available to top-level agents")
                && err.contains("ws.agent.requestDiscussion"),
            "{err}"
        );
        assert!(!err.contains("disabled in settings"), "{err}");
        // Genuine settings-off on a top-level bridge keeps the toggle error.
        let err = help_namespace(false, &forced_off, false, "app.question").unwrap_err();
        assert!(err.contains("agentFeatures.structuredQuestions"), "{err}");
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
            (
                &["ws.pr.monitors", "ws.pr.monitor", "ws.pr.unmonitor"],
                |f| f.pr_monitor = false,
            ),
        ]
    }

    // Every gate open: the defaults plus the opt-in `taskGraph`.
    fn all_gates_open() -> AgentFeaturesSettings {
        AgentFeaturesSettings {
            task_graph: true,
            ..AgentFeaturesSettings::default()
        }
    }

    // Hard requirement: with every gate open (the defaults plus the opt-in
    // `taskGraph`), the assembled description IS the static const —
    // byte-identical, both variants.
    #[test]
    fn all_gates_open_description_is_byte_identical() {
        let features = all_gates_open();
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

    // Guard: every task-graph teaching needle the scrub rewrites still
    // matches both description variants verbatim, so the `replacen` scrubs
    // cannot silently become no-ops.
    #[test]
    fn task_graph_needles_match_both_variants() {
        for desc in [WORKSPACE_API_DESCRIPTION, WORKSPACE_API_DESCRIPTION_CHIEF] {
            for needle in [
                TASK_GRAPH_DELEGATE_PARAMS,
                TASK_GRAPH_UNBLOCKED_WAKE_XREF,
                TASK_GRAPH_BATCH_FORM_LINE,
                TASK_GRAPH_SETCONTENT_XREF,
                TASK_GRAPH_CONVERT_BLOCKS_GRAMMAR,
            ] {
                assert!(
                    desc.contains(needle),
                    "task-graph needle missing: {needle:?}"
                );
            }
        }
    }

    // `taskGraph` on: the delegate docs are advisory (intent-hq/monorepo#2457)
    // — the batch form stays documented factually with per-task option
    // entries, no `greedy` param, and no "re-call delegate" doctrine.
    #[test]
    fn task_graph_on_delegate_docs_are_advisory() {
        for desc in [WORKSPACE_API_DESCRIPTION, WORKSPACE_API_DESCRIPTION_CHIEF] {
            assert!(!desc.contains("re-call delegate"));
            assert!(!desc.contains("greedy"));
            assert!(desc.contains("nothing auto-starts — delegate the ones you want started."));
            assert!(desc.contains(
                "bare taskNoteId or `{ taskNoteId, specialist?, model?, reasoningEffort? }`"
            ));
            assert!(desc.contains("delegate a held task individually to force it past the hold"));
            // Relation-less annotation (monorepo#2457 part 3): the batch form
            // documents `relationsUnknown` on uncovered rows.
            assert!(desc.contains(
                "classify exactly as before (the flag never changes a disposition) but carry `relationsUnknown: true`, and the summary counts the started ones"
            ));
        }
    }

    // `taskGraph` off (the default) scrubs the teaching text — batch-delegate
    // params/line, unblocked-wake advisory, fence-attribute grammar — while
    // every method line survives (docs-only gate: nothing joins
    // `gated_prefixes`, so no method is pruned or denied).
    #[test]
    fn task_graph_off_scrubs_teaching_but_keeps_methods() {
        let features = AgentFeaturesSettings::default();
        assert!(!features.task_graph, "taskGraph must be opt-in");
        for is_chief in [false, true] {
            let pruned = workspace_api_description(is_chief, &features, true);
            for gone in [
                "skipAutoCommit?, tasks?",
                "Batch form:",
                "unlockPlan",
                "criticalPathMinutes",
                "relationsUnknown",
                "Tasks now unblocked",
                "dependsOn=",
                "conflictsWith=",
                "key=api",
            ] {
                assert!(
                    !pruned.contains(gone),
                    "chief={is_chief}: `{gone}` survived taskGraph off"
                );
            }
            // The delegate signature keeps its non-batch params, and every
            // task/note method — the setRelations/markAsTask relation params
            // included (non-goal: older relation APIs stay documented) —
            // survives untouched.
            for kept in [
                "ws.agent.delegate({ taskNoteId?, noteId?, taskText?, agentInstructions?, specialist?, model?, reasoningEffort?, behaviorPrompt?, waitMode?, skipAutoCommit?, mergeOnTurnEnd?, vmResources? })",
                "ws.task.convertBlocks(noteId)",
                "ws.task.setRelations(noteId, { dependsOn?, conflictsWith? })",
                "ws.task.markAsTask(noteId, status, { acceptanceCriteria?, effort?, dependsOn?, conflictsWith? })",
                "ws.note.setContent(id, content, confirmReplacement?)",
                "`@@@task` blocks auto-convert into linked task notes",
            ] {
                assert!(
                    pruned.contains(kept),
                    "chief={is_chief}: `{kept}` was wrongly scrubbed"
                );
            }
            // No dangling separators at the scrub seams.
            assert!(
                !pruned.contains("\n\n\n"),
                "chief={is_chief}: blank-line gap"
            );
        }
    }

    // `taskGraph` is docs-only: it must never join the dispatch-deny table.
    #[test]
    fn task_graph_off_never_denies_dispatch() {
        let features = AgentFeaturesSettings::default();
        for method in ["agent.delegate", "task.convertBlocks", "task.setRelations"] {
            assert_eq!(
                denied_feature(&features, method),
                None,
                "{method} must stay dispatchable with taskGraph off"
            );
        }
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
            state_snapshot: false,
            pr_monitor: false,
            task_graph: false,
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
                "ws.git.commit(",
                "ws.file.read(",
                "ws.crossWorkspace.listSiblings(",
                "ws.agent.create(",
                "ws.agent.reportToParent(",
                "ws.event.subscribe(",
                "ws.pr.snapshot(",
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
            ..all_gates_open()
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
            assert!(desc.contains(super::SANDBOX_MERGE_SANDBOX_DOC_LINE));
            assert_eq!(
                desc.matches(super::SANDBOX_VM_RESOURCES_DOC_LINE).count(),
                2,
                "vmResources doc line must appear under both create and delegate"
            );
            assert!(desc.contains(super::SANDBOX_DELEGATE_VM_RESOURCES_ARG));
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
            assert!(
                !pruned.contains("ws.agent.mergeSandbox"),
                "chief={is_chief}: mergeSandbox doc line must be scrubbed when not CoW-capable"
            );
            assert!(
                !pruned.contains("vmResources"),
                "chief={is_chief}: vmResources clauses must be scrubbed when not CoW-capable"
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

    // Guard: the hook-docs cross-references to `ws.host.exec` scrubbed by the
    // `hostExec` gate still match the base variant verbatim (and stay out of
    // the chief variant, which has no `ws.host.*` surface), so the `replacen`
    // scrubs cannot silently become no-ops.
    #[test]
    fn hook_host_exec_xrefs_match_base_variant_only() {
        assert!(WORKSPACE_API_DESCRIPTION.contains(HOOK_HOST_EXEC_INDEX_XREF));
        assert!(WORKSPACE_API_DESCRIPTION.contains(HOOK_HOST_EXEC_DOC_XREF));
        assert!(!WORKSPACE_API_DESCRIPTION_CHIEF.contains("host.exec"));
    }

    // `hostExec` off: no textual mention of `host.exec` survives anywhere in
    // the assembled description, while the hook docs — including their
    // `ws.pr.snapshot` self-checking guidance — stay intact.
    #[test]
    fn host_exec_off_scrubs_hook_cross_references() {
        let features = AgentFeaturesSettings {
            host_exec: false,
            ..AgentFeaturesSettings::default()
        };
        for is_chief in [false, true] {
            let pruned = workspace_api_description(is_chief, &features, true);
            assert!(
                !pruned.contains("host.exec"),
                "chief={is_chief}: a `host.exec` mention survived disabling hostExec"
            );
            for kept in [
                "ws.hook.* — background watchers; can call full ws.* incl. pr.snapshot",
                "ws.hook.schedule(",
                "including `ws.pr.snapshot`",
                "ws.pr.snapshot(prNumber, { repo? }?)",
            ] {
                assert!(
                    pruned.contains(kept),
                    "chief={is_chief}: `{kept}` was wrongly pruned"
                );
            }
        }
    }

    // Guard: every `prMonitor` cross-reference the scrub rewrites still
    // matches both description variants verbatim, so a doc edit cannot
    // silently turn a `replacen` into a no-op.
    #[test]
    fn pr_monitor_xrefs_match_both_variants() {
        for base in [WORKSPACE_API_DESCRIPTION, WORKSPACE_API_DESCRIPTION_CHIEF] {
            for needle in [
                PR_MONITOR_INDEX_XREF,
                PR_MONITOR_INDEX_SNAPSHOT_LABEL,
                PR_MONITOR_HOOK_XREF,
                PR_MONITOR_SNAPSHOT_XREF_LINE,
                PR_MONITOR_ONLY_METHODS,
            ] {
                assert!(base.contains(needle), "missing verbatim needle: {needle}");
            }
        }
    }

    // `prMonitor` is method-level: `ws.pr.snapshot` and its docs survive, no
    // textual mention of the pruned monitor methods remains anywhere, and the
    // surviving index entry / `ws.hook.schedule` steer stop advertising them.
    #[test]
    fn pr_monitor_off_scrubs_cross_references_but_keeps_snapshot() {
        let features = AgentFeaturesSettings {
            pr_monitor: false,
            ..AgentFeaturesSettings::default()
        };
        for is_chief in [false, true] {
            let pruned = workspace_api_description(is_chief, &features, true);
            assert!(
                !pruned.contains("pr.monitor"),
                "chief={is_chief}: a `pr.monitor` mention survived disabling prMonitor"
            );
            for kept in [
                "ws.pr.snapshot(prNumber, { repo? }?)",
                "pr.snapshot = compact PR watch state",
                "This is the only `ws.pr.*` method.",
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
            state_snapshot: false,
            pr_monitor: false,
            task_graph: false,
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
        // `ws.agent.snapshot()` is NEVER gated: `stateSnapshot` governs only
        // the turn-prompt injection, so even a session created with the
        // toggle already off keeps the tool callable.
        assert_eq!(denied_feature(&all_off, "agent.snapshot"), None);
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

    // ---- specialist modelOptions injection ---------------------------------

    fn sample_options() -> Vec<SpecialistModelOptions> {
        vec![
            SpecialistModelOptions {
                specialist: "implementor".to_string(),
                default_model: Some("auggie:claude-opus-5".to_string()),
                options: vec![
                    SpecialistModelOption {
                        model: "opencode:kimi-k3".to_string(),
                        hint: "cheap".to_string(),
                        reasoning_effort: String::new(),
                    },
                    SpecialistModelOption {
                        model: "auggie:opus".to_string(),
                        hint: String::new(),
                        reasoning_effort: String::new(),
                    },
                ],
            },
            // No resolved default → the provider CLI default label.
            SpecialistModelOptions {
                specialist: "verifier".to_string(),
                default_model: None,
                options: vec![SpecialistModelOption {
                    model: "grok:grok-5".to_string(),
                    hint: "fast reviews".to_string(),
                    reasoning_effort: String::new(),
                }],
            },
        ]
    }

    // Hard requirement: with no specialist carrying options (the default),
    // the injected description IS the plain assembly — byte-identical, and
    // still `Cow::Borrowed` in the every-gate-open case.
    #[test]
    fn no_model_options_keeps_description_byte_identical() {
        let features = all_gates_open();
        for is_chief in [false, true] {
            let got =
                workspace_api_description_with_model_options(is_chief, &features, true, &[], None);
            assert!(
                matches!(got, Cow::Borrowed(_)),
                "no options must not reassemble"
            );
            assert_eq!(
                &*got,
                &*workspace_api_description(is_chief, &features, true),
                "chief={is_chief}: empty options changed the description"
            );
        }
    }

    // Options are injected as continuation lines directly under the
    // `ws.agent.delegate` doc entry: the resolved default first, then compound
    // id + hint per specialist, the hint parenthetical omitted when empty, and
    // the next method line (`ws.agent.send`) still follows.
    #[test]
    fn model_options_injected_into_delegate_docs() {
        let features = AgentFeaturesSettings::default();
        for is_chief in [false, true] {
            let got = workspace_api_description_with_model_options(
                is_chief,
                &features,
                true,
                &sample_options(),
                None,
            );
            assert!(
                got.contains("Specialist model options"),
                "chief={is_chief}: header missing"
            );
            assert!(
                got.contains(
                    "implementor: default `auggie:claude-opus-5`, \
                     `opencode:kimi-k3` (cheap), `auggie:opus`"
                ),
                "chief={is_chief}: implementor options line missing/miswritten:\n{got}"
            );
            // An unresolved default renders the provider-CLI-default label
            // rather than a fabricated id.
            assert!(
                got.contains("verifier: default: provider default, `grok:grok-5` (fast reviews)"),
                "chief={is_chief}: verifier options line missing/miswritten:\n{got}"
            );
            // The block sits between the delegate entry and the next method
            // line, i.e. inside the delegate docs.
            let delegate_idx = got.find("ws.agent.delegate(").expect("delegate line");
            let block_idx = got.find("Specialist model options").expect("block");
            let send_idx = got[delegate_idx..]
                .find("ws.agent.send(")
                .map(|i| i + delegate_idx)
                .expect("send line after delegate");
            assert!(
                delegate_idx < block_idx && block_idx < send_idx,
                "chief={is_chief}: block not inside the delegate docs \
                 (delegate={delegate_idx}, block={block_idx}, send={send_idx})"
            );
            // Injected lines are continuation-indented (≥4 spaces) so the
            // feature-gating pruner treats them as part of the entry.
            for line in got.lines().filter(|l| {
                l.contains("Specialist model options")
                    || l.trim_start().starts_with("implementor:")
                    || l.trim_start().starts_with("verifier:")
            }) {
                assert!(
                    line.starts_with("    "),
                    "injected line not continuation-indented: {line:?}"
                );
            }
        }
    }

    // A declared per-option `reasoningEffort` is rendered inside the option's
    // parenthetical as `effort: <level>` — appended after a hint when both are
    // present, and standing alone when the author gave no hint.
    #[test]
    fn model_options_block_renders_per_option_effort() {
        let options = vec![SpecialistModelOptions {
            specialist: "implementor".to_string(),
            default_model: None,
            options: vec![
                SpecialistModelOption {
                    model: "fable-5".to_string(),
                    hint: "hard tasks".to_string(),
                    reasoning_effort: "high".to_string(),
                },
                SpecialistModelOption {
                    model: "sonnet5".to_string(),
                    hint: String::new(),
                    reasoning_effort: "low".to_string(),
                },
            ],
        }];
        let got = workspace_api_description_with_model_options(
            false,
            &AgentFeaturesSettings::default(),
            true,
            &options,
            None,
        );
        assert!(
            got.contains(
                "implementor: default: provider default, `fable-5` (hard tasks; effort: high), \
                 `sonnet5` (effort: low)"
            ),
            "per-option effort not rendered:\n{got}"
        );
    }

    // The injection composes with feature pruning: a bridge with a disabled
    // toggle still gets the options block, and the pruned namespace stays
    // gone.
    #[test]
    fn model_options_compose_with_feature_pruning() {
        let features = AgentFeaturesSettings {
            background_hooks: false,
            ..AgentFeaturesSettings::default()
        };
        let got = workspace_api_description_with_model_options(
            false,
            &features,
            true,
            &sample_options(),
            None,
        );
        assert!(!got.contains("ws.hook."), "pruned namespace resurfaced");
        assert!(
            got.contains("implementor: default `auggie:claude-opus-5`, `opencode:kimi-k3` (cheap)"),
            "options block missing on a pruned description"
        );
    }

    // Multi-line author text cannot break the description's line structure:
    // newlines in hints (and ids) are flattened to spaces.
    #[test]
    fn model_options_flatten_multiline_hints() {
        let options = vec![SpecialistModelOptions {
            specialist: "implementor".to_string(),
            default_model: Some("auggie:claude-opus-5".to_string()),
            options: vec![SpecialistModelOption {
                model: "opencode:kimi-k3".to_string(),
                hint: "line one\nline two".to_string(),
                reasoning_effort: String::new(),
            }],
        }];
        let got = workspace_api_description_with_model_options(
            false,
            &AgentFeaturesSettings::default(),
            true,
            &options,
            None,
        );
        assert!(
            got.contains("`opencode:kimi-k3` (line one line two)"),
            "multi-line hint not flattened:\n{got}"
        );
    }

    // ---- microVM spawn hints injection --------------------------------------

    fn sample_hints() -> MicrovmSpawnHints {
        MicrovmSpawnHints {
            image_label: "`intentd-guest-base` v0.1.0 (built-in pin)".to_string(),
            vcpus: 4,
            mem_mib: 4096,
        }
    }

    // `None` hints (non-microVM bridges — the default) keep the description
    // byte-identical to the plain assembly.
    #[test]
    fn no_microvm_hints_keeps_description_byte_identical() {
        let features = all_gates_open();
        for is_chief in [false, true] {
            let got =
                workspace_api_description_with_model_options(is_chief, &features, true, &[], None);
            assert!(
                matches!(got, Cow::Borrowed(_)),
                "no hints must not reassemble"
            );
        }
    }

    // `Some` hints inject a continuation line under the delegate docs naming
    // the live guest image and the settings-resolved default VM size.
    #[test]
    fn microvm_hints_injected_into_delegate_docs() {
        let features = AgentFeaturesSettings::default();
        for is_chief in [false, true] {
            let hints = sample_hints();
            let got = workspace_api_description_with_model_options(
                is_chief,
                &features,
                true,
                &[],
                Some(&hints),
            );
            assert!(
                got.contains(
                    "microVM spawns here get guest image `intentd-guest-base` v0.1.0 \
                     (built-in pin) and a default VM size of 4 vCPUs / 4096 MiB unless \
                     `vmResources` overrides."
                ),
                "chief={is_chief}: hint line missing/miswritten:\n{got}"
            );
            // Inside the delegate docs, before the next method line.
            let delegate_idx = got.find("ws.agent.delegate(").expect("delegate line");
            let hint_idx = got.find("microVM spawns here").expect("hint line");
            let send_idx = got[delegate_idx..]
                .find("ws.agent.send(")
                .map(|i| i + delegate_idx)
                .expect("send line after delegate");
            assert!(
                delegate_idx < hint_idx && hint_idx < send_idx,
                "chief={is_chief}: hint not inside the delegate docs"
            );
            // Continuation-indented so feature pruning keeps it attached.
            let line = got
                .lines()
                .find(|l| l.contains("microVM spawns here"))
                .unwrap();
            assert!(
                line.starts_with("    "),
                "hint line not continuation-indented: {line:?}"
            );
        }
    }

    // Hints and model options compose: both blocks land under the delegate
    // docs, hints first.
    #[test]
    fn microvm_hints_compose_with_model_options() {
        let hints = sample_hints();
        let got = workspace_api_description_with_model_options(
            false,
            &AgentFeaturesSettings::default(),
            true,
            &sample_options(),
            Some(&hints),
        );
        let hint_idx = got.find("microVM spawns here").expect("hint line");
        let options_idx = got.find("Specialist model options").expect("options block");
        assert!(
            hint_idx < options_idx,
            "hints must precede the model options block"
        );
    }

    // A pathological image label cannot break the description's line
    // structure: newlines are flattened to spaces.
    #[test]
    fn microvm_hints_flatten_multiline_image_label() {
        let hints = MicrovmSpawnHints {
            image_label: "`https://example.com/a`\n(repo-config)".to_string(),
            vcpus: 2,
            mem_mib: 2048,
        };
        let got = workspace_api_description_with_model_options(
            false,
            &AgentFeaturesSettings::default(),
            true,
            &[],
            Some(&hints),
        );
        assert!(
            got.contains("guest image `https://example.com/a` (repo-config) and"),
            "multi-line image label not flattened:\n{got}"
        );
    }
}
