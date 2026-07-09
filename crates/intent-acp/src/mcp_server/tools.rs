//! Workspace MCP tool registry: each entry maps a `*_workspace-mcp` tool name
//! (matching the §18.4 denylist naming) to the `WorkspaceApi` method dispatched
//! in [`super::dispatch`]. Tool names mirror the TS workspace MCP server.

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
    /// Tool name as agents see it (e.g. `add_to_note_workspace-mcp`).
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
        name: "list_notes_workspace-mcp",
        description: "List all notes in the workspace.",
        params: &[],
    },
    ToolDef {
        name: "get_note_workspace-mcp",
        description: "Read a note by id.",
        params: &[p("noteId", "string", true)],
    },
    ToolDef {
        name: "list_note_tasks_workspace-mcp",
        description: "List checkbox tasks parsed from a note.",
        params: &[p("noteId", "string", true)],
    },
    ToolDef {
        name: "get_workspace_details_workspace-mcp",
        description: "Read workspace metadata (id, title, hasTitle, status, statusMessage,                       branch, repositoryName, tags).",
        params: &[],
    },
    // ---- Note write tools ----
    ToolDef {
        name: "create_note_workspace-mcp",
        description: "Create a new note.",
        params: &[
            p("title", "string", true),
            p("content", "string", false),
            p("tags", "array", false),
        ],
    },
    ToolDef {
        name: "add_to_note_workspace-mcp",
        description: "Append, prepend, or insert content into a note.",
        params: &[
            p("noteId", "string", true),
            p("content", "string", true),
            p("heading", "string", false),
            p("position", "string", false),
        ],
    },
    ToolDef {
        name: "set_note_content_workspace-mcp",
        description: "Replace the entire content of a note.",
        params: &[
            p("noteId", "string", true),
            p("content", "string", true),
            p("confirmReplacement", "boolean", false),
        ],
    },
    ToolDef {
        name: "edit_note_workspace-mcp",
        description: "Replace the first exact-match occurrence in a note.",
        params: &[
            p("noteId", "string", true),
            p("old", "string", true),
            p("new", "string", true),
        ],
    },
    ToolDef {
        name: "edit_note_lines_workspace-mcp",
        description: "Replace/delete/insert by 1-based inclusive line range.",
        params: &[
            p("noteId", "string", true),
            p("start", "integer", true),
            p("end", "integer", true),
            p("content", "string", true),
        ],
    },
    ToolDef {
        name: "update_note_metadata_workspace-mcp",
        description: "Update a note's title and/or tags.",
        params: &[
            p("noteId", "string", true),
            p("title", "string", false),
            p("tags", "array", false),
        ],
    },
    // ---- Workspace metadata write tools ----
    ToolDef {
        name: "set_workspace_title_workspace-mcp",
        description: "Set the workspace title (1-5 words describing the task). Skips when                       the workspace already has a custom title (title different from its id).",
        params: &[p("title", "string", true)],
    },
    ToolDef {
        name: "set_workspace_status_message_workspace-mcp",
        description: "Set or clear the workspace status message (1-2 sentence user-facing                       work summary). Pass an empty string to clear.",
        params: &[p("statusMessage", "string", false)],
    },
    ToolDef {
        name: "delete_note_workspace-mcp",
        description: "Delete a note.",
        params: &[p("noteId", "string", true)],
    },
    // ---- Task write tools ----
    ToolDef {
        name: "update_task_status_workspace-mcp",
        description: "Flip a checkbox by exact task text.",
        params: &[
            p("noteId", "string", true),
            p("taskText", "string", true),
            p("status", "string", true),
        ],
    },
    ToolDef {
        name: "update_note_task_status_workspace-mcp",
        description: "Set a task note's metadata status.",
        params: &[p("noteId", "string", true), p("status", "string", true)],
    },
    ToolDef {
        name: "update_task_workspace-mcp",
        description: "Atomically edit a single task line.",
        params: &[
            p("noteId", "string", true),
            p("line", "integer", true),
            p("text", "string", false),
            p("status", "string", false),
            p("expected", "string", false),
        ],
    },
    ToolDef {
        name: "mark_as_task_workspace-mcp",
        description: "Attach or replace task metadata on a note.",
        params: &[
            p("noteId", "string", true),
            p("status", "string", true),
            p("acceptanceCriteria", "array", false),
            p("effort", "string", false),
        ],
    },
    ToolDef {
        name: "convert_task_blocks_workspace-mcp",
        description: "Convert @@@task blocks into linked child task notes.",
        params: &[p("noteId", "string", true)],
    },
    ToolDef {
        name: "create_prerequisite_workspace-mcp",
        description: "Create a prerequisite child task note.",
        params: &[
            p("noteId", "string", true),
            p("title", "string", true),
            p("content", "string", false),
            p("status", "string", false),
        ],
    },
    ToolDef {
        name: "assign_agent_workspace-mcp",
        description: "Append an agent to a task's assignee list.",
        params: &[p("noteId", "string", true), p("agentId", "string", true)],
    },
    // ---- Comment write tools ----
    ToolDef {
        name: "add_note_comment_workspace-mcp",
        description: "Add a text-anchored comment to a note.",
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
        name: "respond_to_comment_thread_workspace-mcp",
        description: "Reply to an existing comment thread.",
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
        name: "create_agent_workspace-mcp",
        description: "Create a new agent to work on a task; it starts working immediately. \
                      `createLinkedNote`/`noteContent`/`parentNoteId` are accepted for wire \
                      parity but linked-note creation is not yet supported by the daemon.",
        params: &[
            p("name", "string", true),
            p("initialMessage", "string", true),
            p("taskNoteId", "string", false),
            p("specialist", "string", false),
            p("model", "string", false),
            p("behaviorPrompt", "string", false),
            p("isBackground", "boolean", false),
            p("createLinkedNote", "boolean", false),
            p("noteContent", "string", false),
            p("parentNoteId", "string", false),
        ],
    },
    ToolDef {
        name: "delegate_task_workspace-mcp",
        description: "Delegate a task to a new agent.",
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
        name: "report_to_parent_workspace-mcp",
        description: "Send a completion report to your parent agent (delegated agents only).",
        params: &[p("report", "string", true)],
    },
    ToolDef {
        name: "send_message_to_agent_workspace-mcp",
        description: "Send a message to another agent; `priority: \"interrupt\"` stops the target mid-turn.",
        params: &[
            p("agentId", "string", true),
            p("message", "string", true),
            p("priority", "string", false),
        ],
    },
    ToolDef {
        name: "send_message_to_task_agent_workspace-mcp",
        description: "Follow up with the agent assigned to a task note by ID.",
        params: &[
            p("taskNoteId", "string", true),
            p("message", "string", true),
            p("priority", "string", false),
        ],
    },
    ToolDef {
        name: "wake_or_create_task_agent_workspace-mcp",
        description: "Ensure a task has a working agent: resume the assigned one or create a new agent for the task.",
        params: &[
            p("taskNoteId", "string", true),
            p("contextMessage", "string", true),
            p("model", "string", false),
        ],
    },
    // ---- Agent read tools (never restricted) ----
    ToolDef {
        name: "list_agents_workspace-mcp",
        description: "List agents in this workspace; completed agents are omitted unless requested.",
        params: &[p("includeCompleted", "boolean", false)],
    },
    ToolDef {
        name: "get_agent_status_workspace-mcp",
        description: "Detailed status for one agent including task linkage and activity timestamps.",
        params: &[p("agentId", "string", true)],
    },
    ToolDef {
        name: "read_agent_conversation_workspace-mcp",
        description: "Read another agent's conversation transcript.",
        params: &[
            p("agentId", "string", true),
            p("lastN", "integer", false),
            p("pageToken", "string", false),
        ],
    },
    ToolDef {
        name: "get_agent_summary_workspace-mcp",
        description: "Quick summary of what another agent did.",
        params: &[p("agentId", "string", true)],
    },
    ToolDef {
        name: "get_agent_diagnostics_workspace-mcp",
        description: "Sanitized snapshot of agent statuses, subscriptions, delegation groups, and stuck-risk signals.",
        params: &[
            p("agentId", "string", false),
            p("taskNoteId", "string", false),
            p("staleRespondingAfterMs", "integer", false),
        ],
    },
    // ---- Event subscription tools ----
    ToolDef {
        name: "subscribe_to_events_workspace-mcp",
        description: "Subscribe to batched workspace events (service-style; not the WSS streaming surface).",
        params: &[
            p("eventTypes", "array", true),
            p("excludeSelf", "boolean", false),
            p("batchWindow", "integer", false),
        ],
    },
    ToolDef {
        name: "unsubscribe_from_events_workspace-mcp",
        description: "Cancel one event subscription by id.",
        params: &[p("subscriptionId", "string", true)],
    },
    // ---- Git write tools ----
    ToolDef {
        name: "git_commit_workspace-mcp",
        description: "Stage and commit the agent's changes, recording an Agent-Id attribution \
                      trailer from the calling agent's context.",
        params: &[
            p("message", "string", true),
            p("files", "array", false),
            p("userRequested", "boolean", false),
        ],
    },
];
