//! `suite_wss_a_f`: the remaining `e2e_wss_*` tests whose next word starts with a-f, built as one test executable.
//!
//! Each integration-test file used to be its own target, and every target
//! statically links the whole daemon, so one test build produced ~200
//! executables of 180-360 MiB each. The files stay where they are and become
//! modules of a few suite targets instead (`autotests = false` in Cargo.toml);
//! a test keeps its name prefixed by its file, e.g. `e2e_wss_archive_from_hook::<test>`.
//! `test_target_registration_lint` fails when a `tests/*.rs` file is neither a
//! target nor declared below. Keep the list sorted.

mod common;

mod e2e_wss_archive_from_hook;
mod e2e_wss_archive_interrupt;
mod e2e_wss_archive_parks_wakes;
mod e2e_wss_archive_self;
mod e2e_wss_auto_commit_llm;
mod e2e_wss_auto_unarchive;
mod e2e_wss_browser_client_pin;
mod e2e_wss_browser_exec;
mod e2e_wss_browser_tabs;
mod e2e_wss_change_events;
mod e2e_wss_chat_incremental_deltas;
mod e2e_wss_chat_lag_selfheal;
mod e2e_wss_chat_tool_result_ids;
mod e2e_wss_chief_workspace;
mod e2e_wss_codex_sandbox;
mod e2e_wss_codex_session_title;
mod e2e_wss_context_linkage;
mod e2e_wss_disabled_provider_turn;
mod e2e_wss_discussion_agents;
mod e2e_wss_disk_usage;
mod e2e_wss_display_status;
mod e2e_wss_display_status_git_root_prs;
mod e2e_wss_display_status_hooks;
mod e2e_wss_display_status_needs_attention;
mod e2e_wss_egress_conflation;
mod e2e_wss_event_subscription;
mod e2e_wss_executor_credentials;
mod e2e_wss_flush_queued_messages;
