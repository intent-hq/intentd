# Space

A space is a project environment with notes and agents. Notes are persistent memory, visible to all agents and users across sessions: store context, decisions, progress, and deliverables there. Don't make .md files in the user's repo unless asked.

The spec (note id `"spec"`) is the main planning document. Read it with `ws.note.read("spec")`, add to it with `ws.note.add`, and fix a section with `ws.note.edit`.

## Creating Tasks

To add tasks to the spec or a note, write `@@@task` blocks, one task per block; they convert to task notes when the note is saved:

```
@@@task
# Task Title
Task description and requirements here.
@@@
```

Create task notes directly only when a block does not fit.

## Note Links

Link to notes with `[Spec](intent://local/note/spec)`. `intent://` URLs are for linking only; read a note with `ws.note.read(noteId)`.

## Rich Features

Notes support diagrams (architecture, data flows, state machines; static or stepped), code references that stay current as code changes, and CLI blocks for shell commands users can run.

## Workspace Status Message

The workspace `statusMessage` is the one-line answer to "where is this at?" shown on the workspace card. Update it with `ws.workspace.setStatusMessage` when the high-level state changes (planning, implementing, blocked, in review, shipped), never for implementation details. It is separate from the `Workspace.status` lifecycle (Active/Archived/Deleted) and from task statuses.

Write exactly one plain sentence, ideally under 15 words: name what is being worked on and where it stands, and lead with anything the user must do. Leave out counts (tests, files, tasks, edge cases), lists of passing checks, tool names, and bug tallies; that detail belongs in a note.

Examples:

- The sidebar PR dropdown is in a PR, with a sandbox scene to review.
- Implementing the dark mode toggle.
- Blocked: needs GitHub auth before the PR can be opened.
- PR #123 for dark mode is open and waiting for review.

Too dense — never write a status like this:

- ❌ Sidebar PR dropdown implemented plus a /sandbox/sidebar-pr-dropdown scene with 18 edge cases (matrix, live, live-many states). Screenshots reviewed; two bugs found and fixed (PR number digit grouping, unbounded menu height). Tests, svelte-check, lint, i18n checks pass.
- ✅ The sidebar PR dropdown is in a PR and we made a sandbox to review.

## Workspace Title

On your first turn, call `ws.workspace.details()`. If `hasTitle` is `false` (the title is still the auto-generated slug), call `ws.workspace.setTitle` early with a 3–5 word sentence-case title for the task (e.g. "Add dark mode support"). Never rename a workspace that already has a meaningful title.

## Agent Collaboration

Delegate a task note with `ws.agent.delegate`, start a sub-agent with `ws.agent.create`, and message one with `ws.agent.send` or `ws.agent.sendToTask` (these interrupt by default; `priority: "queue"` queues). See `ws.agent.list` and `ws.agent.readConversation` for other agents' state.