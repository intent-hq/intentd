# Notes System Guide

Notes are markdown documents for specs, plans, and documentation. The spec (ID: "spec") is the main planning document.

All note and comment calls below run through the `workspace_api` tool — invoke `workspace_api` and pass JavaScript that calls the `ws.*` API (e.g. `return await ws.note.read("spec")`).

## Key APIs

### Reading Notes
- `ws.note.read(id)` — Read a note (use "spec" for the specification)
- `ws.note.list()` — List all notes
- `ws.note.listTasks(id)` — List just the tasks in a note (returns task IDs, status, and text without the full note content — use this instead of `ws.note.read` when you only need task IDs for delegation)

### Editing Notes (Choose the Right API!)

**SAFE APIs (preserves existing content):**
- `ws.note.add(id, { content, heading?, position? })` — **SAFEST**: Add content (default: end)
  - position: "end" (default), "start", or "after:## Heading"
- `ws.note.edit(id, { old, new })` — Surgical str_replace style editing
- `ws.note.editLines(id, { start, end, content })` — Line-based editing
- `ws.note.updateMetadata(id, { title?, tags? })` — Update only title/tags
- `ws.task.update(noteId, line, { status })` — Update a single task status

**DANGEROUS APIs (full replacement):**
- `ws.note.setContent(id, content, confirmReplacement?)` — ⚠️ **REPLACES ENTIRE NOTE** - requires confirmation!

### Creating/Deleting Notes
- `ws.note.create(title, content, tags?)` — Create a new note
- `ws.note.delete(id)` — Delete a note

### When to Use Each Editing API
| User Request | Use This API |
|--------------|---------------|
| "Add this to the spec" | `ws.note.add` |
| "Put this information in the note" | `ws.note.add` |
| "Insert after Phase 1" | `ws.note.add(id, { content, position: "after:## Phase 1" })` |
| "Fix this typo" | `ws.note.edit` |
| "Update lines 5-10" | `ws.note.editLines` |
| "Change the title" | `ws.note.updateMetadata` |
| "Mark task as done" | `ws.task.update` |
| "Rewrite the entire spec" | `ws.note.setContent` (call again with `confirmReplacement=true` if much shorter) |

## Comments

Use `ws.comment.add(noteId, { searchContext, commentTarget, comment })` to comment on specific text:
- `searchContext`: A unique phrase from the document
- `commentTarget`: The specific text within that context to anchor to

Use `ws.comment.list(noteId, { ... })` to see threads, `ws.comment.respond(noteId, { ... })` to reply.

## Tips

- Always read before updating to avoid overwriting changes
- Prefer `ws.note.add` and `ws.note.edit` over `ws.note.setContent`
- Use `intent://local/note/{noteId}` links to reference notes
- Use tags to organize notes
