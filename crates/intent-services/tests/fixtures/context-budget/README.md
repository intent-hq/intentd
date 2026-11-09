# Context budget fixtures

Inputs for `crates/intent-services/src/context_budget.rs` (brief M8). The
test copies them into a temp home at run time, so no tool working in this
repository discovers them as real rules or skills.

| File | Source | Notes |
|---|---|---|
| `monorepo-AGENTS.md` | `intent-hq/intent` `AGENTS.md` at `4c34f80e4bb94f924d1510a3ffe5decfc9287dd9` (27,573 bytes) | Pinned verbatim. Laid out as `<repo>/AGENTS.md`. |
| `skills/user/*.md` | The developer's 14 synced Claude skills, `~/.claude/skills/synced/<id>/<name>/SKILL.md`, copied 2026-11-09 | Laid out under the same synced path. |
| `skills/project/repo-retrospective.md` | `intent-hq/intent` `.agents/skills/repo-retrospective/SKILL.md` at the same commit | Laid out as `<repo>/.agents/skills/repo-retrospective/SKILL.md`. |

Each skill keeps its frontmatter (`name`, `description`, `compatibility`,
`license`) verbatim: that is all the skills list renders. The body is a stub
(the repository is public and some skills are proprietary). The stub keeps
every `mcp__<server>__…` tool name the original body names, so skill hiding
by MCP server sees the same references.

The **realistic** fixture uses all 15 skills, the AGENTS.md, one exposed
remote machine and a provider CLI mocked as one that loads AGENTS.md
natively. The **clean** fixture uses `docx`, `pdf` and `xlsx` only, with no
repo rules and no remote machine.

To refresh a skill, copy its `SKILL.md` frontmatter and rebuild the stub the
same way; record the new date here.
