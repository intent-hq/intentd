//! `suite_wss_repo`: the repository e2e tests: `e2e_wss_{workspace,git,github,pr,repo,gitignore,merged}_*` and `e2e_repository_*`, built as one test executable.
//!
//! Each integration-test file used to be its own target, and every target
//! statically links the whole daemon, so one test build produced ~200
//! executables of 180-360 MiB each. The files stay where they are and become
//! modules of a few suite targets instead (`autotests = false` in Cargo.toml);
//! a test keeps its name prefixed by its file, e.g. `e2e_repository_context_wire::<test>`.
//! `test_target_registration_lint` fails when a `tests/*.rs` file is neither a
//! target nor declared below. Keep the list sorted.

mod common;

mod e2e_repository_context_wire;
mod e2e_repository_resource_read;
mod e2e_repository_selection_wire;
mod e2e_wss_git_clone;
mod e2e_wss_git_discard;
mod e2e_wss_git_reads;
mod e2e_wss_git_writes;
mod e2e_wss_github_branches_cached;
mod e2e_wss_github_device_flow;
mod e2e_wss_github_pulls_get_fold;
mod e2e_wss_github_repo_config;
mod e2e_wss_github_search;
mod e2e_wss_gitignore_suppression;
mod e2e_wss_merged_pull_requests;
mod e2e_wss_pr_checkout;
mod e2e_wss_pr_events;
mod e2e_wss_pr_monitor;
mod e2e_wss_repo_config;
mod e2e_wss_repo_warm_cache;
mod e2e_wss_workspace_activity_debounce;
mod e2e_wss_workspace_cow;
mod e2e_wss_workspace_create_clone_errors;
mod e2e_wss_workspace_create_image_blocks;
mod e2e_wss_workspace_create_is_new_repo;
mod e2e_wss_workspace_create_progress;
mod e2e_wss_workspace_create_submodule_progress;
mod e2e_wss_workspace_create_worktree_errors;
mod e2e_wss_workspace_direct_survival;
mod e2e_wss_workspace_lifecycle_watchers;
mod e2e_wss_workspace_owner_derivation;
mod e2e_wss_workspace_worktree;
