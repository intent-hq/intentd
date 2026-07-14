Fixed both issues:

1. **Relative gitdir paths**: Now resolve relative gitdir paths against the worktree directory before joining commondir/config.

2. **Test coverage**: Note that full cargo test (including integration tests) was run. The new git_get_config_over_wss test is in crates/intentd/tests/ and is exercised by cargo test (not just --lib).
