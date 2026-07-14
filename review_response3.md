Fixed all 5 new review comments in ba5ce97:

1. **Trait doc** — Updated to describe linked-worktree resolution + parent-walk fallback.
2. **Comment in services** — Updated to match actual behavior (resolves worktrees, walks parents).
3. **Gitdir parsing** — Made tolerant of whitespace (strips `gitdir:` prefix without requiring space).
4. **WSS e2e non-repo** — Added test case that removes .git (handles both file and directory).
5. **Parent-walk constraint** — Limited to MAX_PARENT_LEVELS (5) to prevent data exposure.

All gates pass locally (fmt/clippy/test). WSS e2e `git_get_config_over_wss` now covers repo, remote, and non-repo cases.
