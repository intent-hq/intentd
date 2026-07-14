Addressed all review comments:

1. **Error message consistency**: Changed to use require_ws_note instead of require_workspace_id for consistent error messaging with other git.* methods.

2. **Linked-worktree handling**: Improved to properly resolve gitdir: pointer files and commondir references. Now correctly reads the main repo's .git/config for linked worktrees.

3. **WSS e2e test**: Added git_get_config_over_wss test covering the full WebSocket transport envelope, empty-string fallbacks for remote/non-repo workspaces, and error codes.
