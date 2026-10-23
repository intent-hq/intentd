//! `suite_uds`: the `uds_*` tests (daemon RPC over the Unix domain socket), built as one test executable.
//!
//! Each integration-test file used to be its own target, and every target
//! statically links the whole daemon, so one test build produced ~200
//! executables of 180-360 MiB each. The files stay where they are and become
//! modules of a few suite targets instead (`autotests = false` in Cargo.toml);
//! a test keeps its name prefixed by its file, e.g. `uds_accept_changes::<test>`.
//! `test_target_registration_lint` fails when a `tests/*.rs` file is neither a
//! target nor declared below. Keep the list sorted.

mod common;

mod uds_accept_changes;
mod uds_agent_runtime;
mod uds_channel_subscriptions;
mod uds_chat_subscription;
mod uds_chat_subscription_scale;
mod uds_chief_workspace;
mod uds_concurrent_dispatch;
mod uds_control;
mod uds_events;
mod uds_files;
mod uds_git;
mod uds_git_commit_events;
mod uds_git_credentials;
mod uds_git_pull_events;
mod uds_github;
mod uds_integration;
mod uds_line_attribution;
mod uds_linear;
mod uds_mcp_oauth;
mod uds_mcp_servers;
mod uds_note_subscription;
mod uds_repo_config_consumers;
mod uds_rpc_profile_warn;
mod uds_rules;
mod uds_sentry;
mod uds_server_control;
mod uds_settings;
mod uds_settings_atomic_rollback;
mod uds_settings_nonblocking;
mod uds_specialist;
