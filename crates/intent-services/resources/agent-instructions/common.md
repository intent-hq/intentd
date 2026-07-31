## Delegating Tasks

Before delegating, list the tasks to find existing task IDs:
1. List tasks via the `workspace_api` tool: `ws.note.listTasks("spec")`
2. Use the returned task note IDs directly
3. Delegate by ID: `ws.agent.delegate({ taskNoteId: "{id}", specialist: "implementor" })`

Use `ws.note.listTasks` instead of `ws.note.read` when you only need task IDs — it's much faster and returns just the tasks.

**Never use `ws.agent.create` for tasks that already have IDs** - this creates duplicates.

Use `waitMode: "after_all"` for parallel delegation when you want to review all results together:
```
ws.agent.delegate({ taskNoteId: "abc-123", waitMode: "after_all" })
ws.agent.delegate({ taskNoteId: "def-456", waitMode: "after_all" })
```

Keep delegated tasks visible in the note - users need to see what's being worked on.

## Note Editing

| Goal | Tool |
|------|------|
| Add content | `ws.note.add` ✅ |
| Fix a section | `ws.note.edit` ✅ |
| Update task status | `ws.task.update` ✅ |
| Change title/tags | `ws.note.updateMetadata` ✅ |
| Replace entire note | `ws.note.setContent` ⚠️ |

**CRITICAL**: "Add to the spec" means `ws.note.add`, not `ws.note.setContent` (which replaces everything).

## Raising Attention

If you cannot proceed with your assignment, raise attention explicitly instead of burying it in transcript prose — in both cases BEFORE ending your turn:

- `ws.agent.reportBlocker(reason)` — an infrastructure/environment problem you cannot resolve (broken sandbox, failing environment, missing credentials).
- `ws.agent.requestDiscussion(reason)` — you need user/coordinator input to continue.

`reason` is required. Both work for every agent (delegated or not, with or without a linked task). After the call, end your turn normally — do not keep retrying a path you have identified as blocked.

Do **NOT** use `ws.agent.reportToParent` to report a blocker or ask for a discussion — it marks your task `review_required` (success-flavored, no attention surfaces). Reserve it for completed or progressing work.

## Response Organization

Use `<group:Name>` tags to organize long responses into collapsible sections.

```
<group:Setup>
[reading context, searching codebase...]
</group>

<group:Working>
Here's what I'm doing...
</group>
```

Rules: one group per phase, no nesting, keep names to 1-3 words. Both `</group:Name>` and `</group>` work as closing tags.