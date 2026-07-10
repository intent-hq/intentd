//! Workspace MCP tool registry: each entry maps a bare tool name (matching the
//! §18.4 denylist naming) to the `WorkspaceApi` method dispatched in
//! [`super::dispatch`]. Tool names mirror the TS workspace MCP server. Names
//! carry NO `_workspace-mcp` suffix: ACP providers (auggie) already suffix
//! every tool with its MCP server name, so agents see `<name>_workspace-mcp`;
//! baking the suffix in here would double it.

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
    /// Registry tool name (e.g. `add_to_note`); agents see it with the
    /// provider-appended server suffix (`add_to_note_workspace-mcp`).
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

/// The full tool registry (pre-denylist). The server filters this per agent.
pub fn all_tools() -> &'static [ToolDef] {
    ALL_TOOLS
}

static ALL_TOOLS: &[ToolDef] = &[
    // ---- Read tools (never restricted) ----
    ToolDef {
        name: "list_notes",
        description: "List all notes in the workspace.",
        params: &[],
    },
    ToolDef {
        name: "get_note",
        description: "Read a note by id. Use noteId=`spec` for the workspace spec. Content \
                      has line numbers like `   1 | text`.",
        params: &[p("noteId", "string", true)],
    },
    ToolDef {
        name: "list_note_tasks",
        description: "List checkbox tasks parsed from a note. Faster than `get_note` when \
                      you only need checkbox/task IDs. Use `taskNoteId` for delegation; \
                      `linkedTaskNoteId` is a backward-compatible alias.",
        params: &[p("noteId", "string", true)],
    },
    ToolDef {
        name: "get_my_task",
        description: "Read a task note's title, content, and task metadata (status, \
                      assigned agents, acceptance criteria, effort), along with its \
                      parent id and subtasks. Use this to load your own task note by id \
                      (from intent://local/task/{id} links) at the start of a task.",
        params: &[p("taskNoteId", "string", true)],
    },
    ToolDef {
        name: "get_workspace_details",
        description: "Read workspace metadata (id, title, hasTitle, status, statusMessage, \
                      branch, repositoryName, tags).",
        params: &[],
    },
    // ---- Note write tools ----
    ToolDef {
        name: "create_note",
        description: "Create a new note. DO NOT use this for the spec: the spec already \
                      exists as note ID `spec`; edit or add to it instead.",
        params: &[
            p("title", "string", true),
            p("content", "string", false),
            p("tags", "array", false),
        ],
    },
    ToolDef {
        name: "add_to_note",
        description: "Append, prepend, or insert content into a note — the safest way to \
                      add information without losing existing content. Prefer this when \
                      asked to \"add\", \"put\", \"document\", or \"include\" something. \
                      `position` can be \"end\" (default), \"start\", or \
                      \"after:## Heading\" such as \"after:## Phase 1\".",
        params: &[
            p("noteId", "string", true),
            p("content", "string", true),
            p("heading", "string", false),
            p("position", "string", false),
        ],
    },
    ToolDef {
        name: "set_note_content",
        description: "FULL REPLACEMENT: replaces the entire content of a note. Prefer \
                      `add_to_note` / `edit_note` / `edit_note_lines` unless you \
                      intentionally want to overwrite everything. If the new content is \
                      much shorter, call again with `confirmReplacement=true`. @@@task \
                      blocks auto-convert into linked task notes.",
        params: &[
            p("noteId", "string", true),
            p("content", "string", true),
            p("confirmReplacement", "boolean", false),
        ],
    },
    ToolDef {
        name: "edit_note",
        description: "Surgical text replacement in a note. `old` must match EXACTLY, \
                      including whitespace and line breaks; only the first occurrence is \
                      replaced.",
        params: &[
            p("noteId", "string", true),
            p("old", "string", true),
            p("new", "string", true),
        ],
    },
    ToolDef {
        name: "edit_note_lines",
        description: "Line-based replace/delete/insert. `start` and `end` are 1-based and \
                      INCLUSIVE. To delete lines, pass `content: \"\"`. To insert after a \
                      line, set `start` and `end` to the same line and include both the \
                      original line and the new lines in `content`.",
        params: &[
            p("noteId", "string", true),
            p("start", "integer", true),
            p("end", "integer", true),
            p("content", "string", true),
        ],
    },
    ToolDef {
        name: "update_note_metadata",
        description: "Update only a note's title and/or tags; content is untouched. The \
                      spec note title is always `Spec` and cannot be changed.",
        params: &[
            p("noteId", "string", true),
            p("title", "string", false),
            p("tags", "array", false),
        ],
    },
    // ---- Workspace metadata write tools ----
    ToolDef {
        name: "set_workspace_title",
        description: "Set the workspace title (1-5 words describing the task). Skips when \
                      the workspace already has a custom title (title different from its id).",
        params: &[p("title", "string", true)],
    },
    ToolDef {
        name: "set_workspace_status_message",
        description: "Set or clear the workspace status message (1-2 sentence user-facing \
                      work summary). Pass an empty string to clear.",
        params: &[p("statusMessage", "string", false)],
    },
    ToolDef {
        name: "delete_note",
        description: "Permanently delete a note.",
        params: &[p("noteId", "string", true)],
    },
    // ---- Task write tools ----
    ToolDef {
        name: "update_task_status",
        description: "Atomically flip one checkbox status by exact task text — prefer this \
                      over `set_note_content` when marking tasks done/in progress to avoid \
                      conflicts. `status`: \"done\", \"todo\", or \"in-progress\"; \
                      `taskText` must match the checkbox text exactly.",
        params: &[
            p("noteId", "string", true),
            p("taskText", "string", true),
            p("status", "string", true),
        ],
    },
    ToolDef {
        name: "update_note_task_status",
        description: "Set a task note's metadata status. Values include \"not_started\", \
                      \"waiting\", \"discussion_needed\", \"in_progress\", \
                      \"review_required\", \"complete\", \"cancelled\".",
        params: &[p("noteId", "string", true), p("status", "string", true)],
    },
    ToolDef {
        name: "update_task",
        description: "Atomically edit a single checkbox line, preserving the rest of the \
                      note — prefer this over `set_note_content` for task edits. `line` is \
                      the 1-based task line number from `get_note`; `status`: \"done\", \
                      \"todo\", or \"in-progress\"; `expected` enables conflict detection \
                      if another agent may have changed the task.",
        params: &[
            p("noteId", "string", true),
            p("line", "integer", true),
            p("text", "string", false),
            p("status", "string", false),
            p("expected", "string", false),
        ],
    },
    ToolDef {
        name: "mark_as_task",
        description: "Convert a note into a task note (attach or replace task metadata). \
                      `acceptanceCriteria` lists testable conditions; `effort` maps to \
                      estimated effort.",
        params: &[
            p("noteId", "string", true),
            p("status", "string", true),
            p("acceptanceCriteria", "array", false),
            p("effort", "string", false),
        ],
    },
    ToolDef {
        name: "convert_task_blocks",
        description: "Convert @@@task blocks into linked child task notes. Note updates \
                      already auto-convert them; use this for manual re-conversion.",
        params: &[p("noteId", "string", true)],
    },
    ToolDef {
        name: "create_prerequisite",
        description: "Create a prerequisite child task note (adds a task dependency).",
        params: &[
            p("noteId", "string", true),
            p("title", "string", true),
            p("content", "string", false),
            p("status", "string", false),
        ],
    },
    ToolDef {
        name: "assign_agent",
        description: "Append an existing agent to a task note's assignee list. `agentId` \
                      must be `agent-{uuid}`; to create and assign in one step, use \
                      `create_agent` with `taskNoteId`.",
        params: &[p("noteId", "string", true), p("agentId", "string", true)],
    },
    // ---- Comment write tools ----
    ToolDef {
        name: "add_note_comment",
        description: "Add a text-anchored comment to a note. Use enough `searchContext` to \
                      be unique; `commentTarget` must be a substring inside it. Search is \
                      case- and whitespace-sensitive; use the same text for both fields to \
                      comment on an entire phrase.",
        params: &[
            p("noteId", "string", true),
            p("searchContext", "string", true),
            p("commentTarget", "string", true),
            p("comment", "string", true),
            p("type", "string", false),
            p("author", "string", false),
        ],
    },
    ToolDef {
        name: "respond_to_comment_thread",
        description: "Reply to an existing comment thread — the parent anchor is reused \
                      automatically, so you do not need to search for text again. `type` \
                      can be \"comment\", \"suggestion\", \"question\", or \
                      \"change-request\"; for suggestions, pass both `suggestionOriginal` \
                      and `suggestionProposed`.",
        params: &[
            p("noteId", "string", true),
            p("threadId", "string", false),
            p("commentId", "string", false),
            p("comment", "string", true),
            p("type", "string", false),
            p("author", "string", false),
            p("suggestionOriginal", "string", false),
            p("suggestionProposed", "string", false),
        ],
    },
    // ---- Agent creation tools ----
    ToolDef {
        name: "create_agent",
        description: "Create a new agent to work on a task. The new agent runs independently \
                      and starts working immediately. You are automatically subscribed to its \
                      completion events: end your turn and you will be woken up when the agent \
                      completes. This allows you to create multiple agents in parallel and be \
                      notified as each finishes. Use `specialist` to auto-configure model and \
                      behavior (\"implementor\" for implementation tasks, \"verifier\" for \
                      verification/review); explicit `model`/`behaviorPrompt` override it. \
                      Agents are background by default (set `isBackground=false` for \
                      foreground). `idempotencyKey` dedupes retried calls: a retry with \
                      the same key returns the originally created agent instead of \
                      spawning a duplicate. `createLinkedNote`/`noteContent`/`parentNoteId` \
                      are accepted for wire parity but linked-note creation is not yet \
                      supported by the daemon.",
        params: &[
            p("name", "string", true),
            p("initialMessage", "string", true),
            p("taskNoteId", "string", false),
            p("specialist", "string", false),
            p("model", "string", false),
            p("behaviorPrompt", "string", false),
            p("isBackground", "boolean", false),
            p("idempotencyKey", "string", false),
            p("createLinkedNote", "boolean", false),
            p("noteContent", "string", false),
            p("parentNoteId", "string", false),
        ],
    },
    ToolDef {
        name: "delegate_task",
        description: "Delegate an existing task to a new agent. Specify the task by \
                      `taskNoteId` (preferred; extract from intent://local/task/{id} links) or \
                      by `noteId` + `taskText` matching a checkbox in a parent note. The agent \
                      starts working immediately and you are automatically subscribed to its \
                      completion events: end your turn and you will be woken up when the agent \
                      completes. `waitMode`: \"immediate\" (default) wakes you when EACH \
                      delegated agent completes; \"after_all\" wakes you once when ALL agents \
                      delegated with after_all in the same turn complete — use it when \
                      delegating multiple related tasks to review all results at once. Use \
                      `specialist` (\"implementor\"/\"verifier\") to auto-configure model and \
                      behavior; explicit `model`/`behaviorPrompt` override it.",
        params: &[
            p("taskNoteId", "string", false),
            p("noteId", "string", false),
            p("taskText", "string", false),
            p("agentInstructions", "string", false),
            p("specialist", "string", false),
            p("model", "string", false),
            p("behaviorPrompt", "string", false),
            p("waitMode", "string", false),
            p("skipAutoCommit", "boolean", false),
        ],
    },
    ToolDef {
        name: "report_to_parent",
        description: "Send a concise completion/update report to your parent agent. Only \
                      works for delegated agents; user-created agents will get an error.",
        params: &[p("report", "string", true)],
    },
    ToolDef {
        name: "send_message_to_agent",
        description: "Send a message to another agent. `priority: \"interrupt\"` stops the \
                      target mid-response and delivers the message immediately.",
        params: &[
            p("agentId", "string", true),
            p("message", "string", true),
            p("priority", "string", false),
        ],
    },
    ToolDef {
        name: "send_message_to_task_agent",
        description: "Follow up with the agent assigned to a task note — more convenient \
                      than `send_message_to_agent` when you only know the task note ID. \
                      `priority: \"interrupt\"` also stops mid-response.",
        params: &[
            p("taskNoteId", "string", true),
            p("message", "string", true),
            p("priority", "string", false),
        ],
    },
    ToolDef {
        name: "wake_or_create_task_agent",
        description: "Ensure a task has a working agent: checks assigned agents, resumes a \
                      running/restorable one if possible, otherwise creates a new agent \
                      for the task.",
        params: &[
            p("taskNoteId", "string", true),
            p("contextMessage", "string", true),
            p("model", "string", false),
        ],
    },
    // ---- Agent read tools (never restricted) ----
    ToolDef {
        name: "list_agents",
        description:
            "List agents in this workspace; completed agents are omitted unless requested.",
        params: &[p("includeCompleted", "boolean", false)],
    },
    ToolDef {
        name: "get_agent_status",
        description:
            "Detailed status for one agent including task linkage and activity timestamps.",
        params: &[p("agentId", "string", true)],
    },
    ToolDef {
        name: "read_agent_conversation",
        description: "Read another agent's conversation transcript.",
        params: &[
            p("agentId", "string", true),
            p("lastN", "integer", false),
            p("pageToken", "string", false),
        ],
    },
    ToolDef {
        name: "get_agent_summary",
        description: "Quick summary of what another agent did.",
        params: &[p("agentId", "string", true)],
    },
    ToolDef {
        name: "get_agent_diagnostics",
        description: "Sanitized snapshot of agent statuses, subscriptions, queues, \
                      delegation groups, delivery stats, recent delivery events, and \
                      stuck-risk signals.",
        params: &[
            p("agentId", "string", false),
            p("taskNoteId", "string", false),
            p("staleRespondingAfterMs", "integer", false),
        ],
    },
    // ---- Event subscription tools ----
    ToolDef {
        name: "subscribe_to_events",
        description: "Subscribe to batched workspace events (service-style; not the WSS \
                      streaming surface). `eventTypes` must be an array such as \
                      [\"agent:*\", \"file:*\"]; prefer explicit categories (`agent:*`, \
                      `file:*`, `task:*`, `git:*`, `note:*`, `terminal:*`, `test:*`, \
                      `build:*`, `workspace:*`, `spec:*`, `goal:*`, `comment:*`) over a \
                      bare `*`. `excludeSelf` defaults to true and `batchWindow` defaults \
                      to 500ms.",
        params: &[
            p("eventTypes", "array", true),
            p("excludeSelf", "boolean", false),
            p("batchWindow", "integer", false),
        ],
    },
    ToolDef {
        name: "unsubscribe_from_events",
        description: "Cancel one event subscription by id.",
        params: &[p("subscriptionId", "string", true)],
    },
    // ---- Git write tools ----
    ToolDef {
        name: "git_commit",
        description: "Stage and commit only the calling agent's changes, recording an \
                      Agent-Id attribution trailer from the agent's context. If workspace \
                      auto-commit is disabled, set `userRequested=true` to confirm the \
                      user asked for the commit.",
        params: &[
            p("message", "string", true),
            p("files", "array", false),
            p("userRequested", "boolean", false),
        ],
    },
];
