//! `suite_wss_s_z`: the remaining `e2e_wss_*` tests whose next word starts with s-z, built as one test executable.
//!
//! Each integration-test file used to be its own target, and every target
//! statically links the whole daemon, so one test build produced ~200
//! executables of 180-360 MiB each. The files stay where they are and become
//! modules of a few suite targets instead (`autotests = false` in Cargo.toml);
//! a test keeps its name prefixed by its file, e.g. `e2e_wss_script_monitors::<test>`.
//! `test_target_registration_lint` fails when a `tests/*.rs` file is neither a
//! target nor declared below. Keep the list sorted.

mod common;

mod e2e_wss_script_monitors;
mod e2e_wss_script_persistence;
mod e2e_wss_script_restarting;
mod e2e_wss_script_run_cancellation;
mod e2e_wss_self_send_rejected;
mod e2e_wss_sentry_pagination;
mod e2e_wss_serve_resume_all;
mod e2e_wss_server_assigned_agent_ids;
mod e2e_wss_server_pairing;
mod e2e_wss_settings_atomic_rollback;
mod e2e_wss_settings_live_reload;
mod e2e_wss_setup_headless;
mod e2e_wss_setup_lifecycle;
mod e2e_wss_setup_script;
mod e2e_wss_shutdown_reaps_provider_tree;
mod e2e_wss_shutdown_reaps_pty_sessions;
mod e2e_wss_specialist_frontmatter_model;
mod e2e_wss_specialists_changed;
mod e2e_wss_stall_annotation;
mod e2e_wss_sticky_reverse;
mod e2e_wss_subscribe_snapshot_warn;
mod e2e_wss_system_prompt_fallback;
mod e2e_wss_tcp_origin_guard;
mod e2e_wss_tilde_paths;
mod e2e_wss_tool_payload_retention;
mod e2e_wss_truncation_redrive;
mod e2e_wss_tunnel;
mod e2e_wss_ui_context;
mod e2e_wss_unblocked_hints;
mod e2e_wss_voice_transcribe;
mod e2e_wss_wake_or_create;
mod e2e_wss_wake_resume;
