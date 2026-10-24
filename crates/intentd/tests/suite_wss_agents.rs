//! `suite_wss_agents`: the agent e2e tests: `e2e_wss_agent_*` (except `e2e_wss_agent_lifecycle`), `e2e_wss_delegat*`, `e2e_agent_*` and `e2e_mock_agent*`, built as one test executable.
//!
//! Each integration-test file used to be its own target, and every target
//! statically links the whole daemon, so one test build produced ~200
//! executables of 180-360 MiB each. The files stay where they are and become
//! modules of a few suite targets instead (`autotests = false` in Cargo.toml);
//! a test keeps its name prefixed by its file, e.g. `e2e_agent_bindings_followup::<test>`.
//! `test_target_registration_lint` fails when a `tests/*.rs` file is neither a
//! target nor declared below. Keep the list sorted.

mod common;

mod e2e_agent_bindings_followup;
mod e2e_agent_features_gating;
mod e2e_agent_ops_bindings_followup;
mod e2e_mock_agent;
mod e2e_mock_agent_client_services;
mod e2e_mock_agent_workspace_api_bindings;
mod e2e_mock_agent_workspace_api_bindings2;
mod e2e_mock_agent_workspace_title;
mod e2e_mock_agent_ws_app;
mod e2e_wss_agent_config_option;
mod e2e_wss_agent_create_name_explicitly_set;
mod e2e_wss_agent_cross_provider_history;
mod e2e_wss_agent_idle_timeout;
mod e2e_wss_agent_list_active;
mod e2e_wss_agent_midturn_failure;
mod e2e_wss_agent_provider_pin;
mod e2e_wss_agent_quota_failure;
mod e2e_wss_agent_rehydration;
mod e2e_wss_agent_set_model;
mod e2e_wss_agent_spawn_retry;
mod e2e_wss_agent_state_snapshot;
mod e2e_wss_agent_turn_correlation;
mod e2e_wss_agent_watch;
mod e2e_wss_agent_workers;
mod e2e_wss_delegate_batch;
mod e2e_wss_delegate_provider_resolution;
mod e2e_wss_delegation_group_persist;
