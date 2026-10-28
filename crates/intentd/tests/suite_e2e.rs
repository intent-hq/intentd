//! `suite_e2e`: every other integration test that uses `common`
//! and leaves process-global state alone, built as one test
//! executable.
//!
//! Each integration-test file used to be its own target, and every target
//! statically links the whole daemon, so one test build produced ~200
//! executables of 180-360 MiB each. The files stay where they are and become
//! modules of a few suite targets instead (`autotests = false` in Cargo.toml);
//! a test keeps its name prefixed by its file, e.g. `auggie_context_e2e::<test>`.
//! `test_target_registration_lint` fails when a `tests/*.rs` file is neither a
//! target nor declared below. Keep the list sorted.

mod common;

mod auggie_context_e2e;
mod durable_command;
mod e2e_config_precedence;
mod e2e_core_cli_commands;
mod e2e_core_hermetic_workspaces_guard;
mod e2e_core_orphan_trash_sweep;
mod e2e_detached_lifecycle;
mod e2e_doctor_codex;
mod e2e_doctor_database_safety;
mod e2e_hook_lifecycle;
mod e2e_mcp_bridge_resilience;
mod e2e_native_review_wire;
mod e2e_node_git;
mod e2e_note_comment_ops_bindings;
mod e2e_private_profile;
mod e2e_remote_launch;
mod e2e_services_agent_queue;
mod e2e_services_note_ops;
mod e2e_sitter_staged_restart;
mod e2e_startup_ordering;
mod e2e_transport;
mod hermetic_serve_command;
mod pi_mcp_extension_node;
mod security_hardening;
mod shutdown_timings;
mod wss_port_setting;
