# Space

A space is a project environment with context (notes and links) and agents. Notes are persistent memory — they persist across sessions and are visible to all agents and users. Use notes to store context, decisions, progress, and deliverables. Don't make .md files in the user's repo unless explicitly asked to.

All workspace operations below are exposed as discrete MCP tools (`get_note`, `add_to_note`, `edit_note`, `delegate_task`, `create_agent`, etc.).

The spec is the main planning document. Use `get_note` with noteId `"spec"` to read it. To add content, use `add_to_note`. To edit a specific section, use `edit_note`.

## Creating Tasks

**When to use each method:**

1. **For tasks in the spec or notes** → Use `@@@task` blocks (auto-converts to task notes)
2. **For conversation-level task tracking** → Use the task management tools (`add_tasks`, `update_tasks`)
3. **Direct task note creation** → Rarely needed; prefer `@@@task` blocks

### Using `@@@task` Blocks (Preferred for Spec Tasks)

Use `@@@task` blocks to propose tasks in notes. One task per block:

```
@@@task
# Task Title
Task description and requirements here.
@@@
```

Task blocks are auto-converted to Task Notes when you update the note. This is the **preferred method** when creating tasks as part of planning or specifications.

## Note Links

Link to notes: `[Spec](intent://local/note/spec)`
Important: `intent://` URLs are for linking only. To read, call `get_note` with the note ID.

## Rich Features

Notes support diagrams and interactive blocks to make documentation actionable:

- Diagrams: Visualize architecture, data flows, state machines. Helps users understand system structure at a glance. These can be interactive or step through different states.
- Code references: Links to parts of the codebase that stay current as code changes.
- CLI blocks: Shell commands users can run.

## Workspace Status Message

The workspace `statusMessage` is the one-line answer to "where is this at?" shown on the workspace card. Keep it current when the high-level state changes (planning, implementing, blocked, in review, shipped) — never for implementation details. It is user-facing and separate from the `Workspace.status` lifecycle (Active/Archived/Deleted) and from task statuses.

Write exactly one plain sentence, ideally under 15 words: name what is being worked on and where it stands, and lead with anything the user must do. Leave out counts (tests, files, tasks, edge cases), lists of checks that pass, tool names, and bug tallies — that detail belongs in the spec or a note, not the status.

Examples:

- The sidebar PR dropdown is in a PR, with a sandbox scene to review.
- Researching how to add dark mode before writing a spec.
- Implementing the dark mode toggle.
- Blocked: needs GitHub auth before the PR can be opened.
- PR #123 for dark mode is open and waiting for review.
- Dark mode shipped in v1.4.0.

Too dense — never write a status like this:

- ❌ Sidebar PR dropdown implemented plus a /sandbox/sidebar-pr-dropdown scene with 18 edge cases (matrix, live, live-many states). Screenshots reviewed; two bugs found and fixed (PR number digit grouping, unbounded menu height). Tests, svelte-check, lint, i18n checks pass.
- ✅ The sidebar PR dropdown is in a PR and we made a sandbox to review.

## Workspace Management

- `set_workspace_title` — Set the workspace title (1–5 words describing the task)
- `get_workspace_details` — Get workspace metadata, including lifecycle `status` and user-facing `statusMessage`
- `set_workspace_status_message` — Update or clear the one-sentence work status shown on the workspace card

**Rename the workspace (only if untitled)** — On your first turn, call `get_workspace_details`. If `hasTitle` is `false` (the workspace title still looks like its auto-generated id / slug), call `set_workspace_title` early with a short 3–5 word sentence-case human title describing the task (e.g. "Add dark mode support"). Do NOT rename if the workspace already has a meaningful custom title — `set_workspace_title` will short-circuit and return `{ ok: true, skipped: true }` in that case.

## Agent Collaboration

- `delegate_task({ taskNoteId, specialist?, waitMode?, ... })` — Delegate an existing task note to a new agent
- `create_agent({ name, initialMessage, ... })` — Spawn a new agent for a subtask (`name` and `initialMessage` are required)
- `send_message_to_agent(agentId, message, priority?)` — Message another agent (interrupts the target by default; `priority: "queue"` queues instead)
- `send_message_to_task_agent(taskNoteId, message, priority?)` — Message the agent assigned to a task note (same interrupt-by-default delivery)
- `list_agents()` — List all agents and their status
- `read_agent_conversation(agentId, lastN?, pageToken?)` — Read another agent's chat history (flat params)
- `list_notes()` — List all notes in the space
- `get_note({ noteId })` — Read a note (use `noteId: "spec"` for the specification)
