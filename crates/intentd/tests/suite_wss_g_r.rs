//! `suite_wss_g_r`: the remaining `e2e_wss_*` tests whose next word starts with g-r, built as one test executable.
//!
//! Each integration-test file used to be its own target, and every target
//! statically links the whole daemon, so one test build produced ~200
//! executables of 180-360 MiB each. The files stay where they are and become
//! modules of a few suite targets instead (`autotests = false` in Cargo.toml);
//! a test keeps its name prefixed by its file, e.g. `e2e_wss_harness_wake::<test>`.
//! `test_target_registration_lint` fails when a `tests/*.rs` file is neither a
//! target nor declared below. Keep the list sorted.

#![expect(
    clippy::duplicate_mod,
    reason = "e2e_wss_invite_join and e2e_wss_invite_join_gitlab each compile \
              common/invitation_fixture_lifecycle.rs against their own `super::Daemon`"
)]

mod common;

mod e2e_wss_harness_wake;
mod e2e_wss_hook_mcp_tools;
mod e2e_wss_host;
mod e2e_wss_host_credential_env;
mod e2e_wss_installed_cli;
mod e2e_wss_interrupted_agents;
mod e2e_wss_invite_join;
mod e2e_wss_invite_join_gitlab;
mod e2e_wss_last_activity;
mod e2e_wss_linear_pagination;
mod e2e_wss_mcp_oauth_refresh;
mod e2e_wss_mcp_servers;
mod e2e_wss_note_set_content_merge;
mod e2e_wss_note_truncated_read;
mod e2e_wss_npx_launch_isolation;
mod e2e_wss_panic_safety;
mod e2e_wss_pending_questions;
mod e2e_wss_poisoned_session_recreate;
mod e2e_wss_pool_contention_stress;
mod e2e_wss_primary_identity_refresh;
mod e2e_wss_priority_lanes;
mod e2e_wss_prompt_error_transcript;
mod e2e_wss_provider_profiles;
mod e2e_wss_question_ask;
mod e2e_wss_report_debounce;
mod e2e_wss_resolve_interrupted;
mod e2e_wss_restart_tail_recap;
mod e2e_wss_resume_queue_order;
mod e2e_wss_rpc_overload_limit;
mod e2e_wss_rtk;
mod e2e_wss_runtime_control;
