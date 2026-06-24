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
];
