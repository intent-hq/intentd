# Context budget fixtures

Inputs for `crates/intent-services/src/context_budget.rs` (brief M8). The
test copies them into a temp home at run time, so no tool working in this
repository discovers them as real rules or skills.

| File | Source | Notes |
|---|---|---|
| `monorepo-AGENTS.md` | `intent-hq/intent` `AGENTS.md` at `4c34f80e4bb94f924d1510a3ffe5decfc9287dd9` (27,573 bytes) | Pinned verbatim. Laid out as `<repo>/AGENTS.md`. |
| `skills/user/*.md` | The developer's 14 synced Claude skills, `~/.claude/skills/synced/<id>/<name>/SKILL.md`, copied 2026-11-09 | Laid out under the same synced path. |
| `skills/project/repo-retrospective.md` | `intent-hq/intent` `.agents/skills/repo-retrospective/SKILL.md` at the same commit | Laid out as `<repo>/.agents/skills/repo-retrospective/SKILL.md`. |

Each skill keeps its `name`, `compatibility` and `license` frontmatter as is.
The repository is public and the synced skills are private or proprietary, so
no other original text is copied:

- In `skills/user/*.md`, the `description` is made-up placeholder text of the
  same rendered length (character count after the skills list's XML
  escaping), so the measured sizes match the original skills. It keeps every
  `mcp__…` tool name the original description named.
- The body is a stub. It keeps every `mcp__<server>__…` tool name the
  original body names, so skill hiding by MCP server sees the same references.
- `repo-retrospective` comes from this public repository and keeps its
  description.

The **realistic** fixture uses all 15 skills, the AGENTS.md and one exposed
remote machine. Repo rules have no "the CLI loads AGENTS.md natively" input
yet; the change that adds it must also set it here, or the 50% gate keeps
counting the AGENTS.md. Account and machine IDs are made up. The **clean**
fixture uses `docx`, `pdf` and `xlsx` only, with no repo rules and no remote
machine.

To refresh a skill, copy its `name`, `compatibility` and `license`, write a
placeholder `description` of the same rendered length, rebuild the stub the
same way, and record the new date here.
