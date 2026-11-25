//! Every startup step `intentd serve` takes is accounted for in the
//! orchestrator: wired (and the call is found in the orchestrator's or the
//! shared startup code), refused in orchestrator mode with a reason, or a
//! known gap with its follow-up. A step `intentd` gains that is in none of
//! these lists fails this test, so the orchestrator cannot silently skip it
//! again, as it skipped the agent manager (no agent turn ever ran) and head's
//! git receive hooks (every machine push was refused).
//!
//! "A step" is a call in `cmd_serve` / `cmd_serve_owned` (intentd's
//! `src/main.rs`): a method on the services, the agent manager, the store
//! or the MCP hub, a `with_*` builder call, or a free function call. Plain
//! values, parses, clones and test seams are listed as plumbing.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// How the orchestrator handles one of `intentd`'s startup steps.
enum Step {
    /// The orchestrator does it too: this call must appear (as `name(`) in
    /// the orchestrator's sources, the shared startup code in `intent-app`
    /// or the tenant self-fence in `intent-services`, outside tests.
    Wired(&'static str),
    /// Never in orchestrator mode, and why.
    Refused(&'static str),
    /// Not done yet: what the orchestrator lacks without it.
    Gap(&'static str),
    /// Not a startup step: a value, a parse, a clone, a teardown detail or a
    /// test seam.
    Plumbing,
}

use Step::{Gap, Plumbing, Refused, Wired};

/// Every startup call of `intentd serve`, keyed as [`startup_calls`] names
/// them.
const STEPS: &[(&str, Step)] = &[
    // The services the orchestrator builds per tenant (`tenant_services`).
    (".with_assets_root", Wired("with_assets_root")),
    (".with_chief_cwd_root", Wired("with_chief_cwd_root")),
    (".with_event_bus", Wired("with_event_bus")),
    (".with_models_cache_dir", Wired("with_models_cache_dir")),
    (".with_orchestrator_mode", Wired("with_orchestrator_mode")),
    (
        ".with_provider_profiles_dir",
        Wired("with_provider_profiles_dir"),
    ),
    (".with_reverse_dispatch", Wired("with_reverse_dispatch")),
    (".with_script_output_dir", Wired("with_script_output_dir")),
    (".with_settings_registry", Wired("with_settings_registry")),
    ("intent_app::compose", Wired("intent_app::compose")),
    (
        ".with_hooks_max_per_agent",
        Wired("with_hooks_max_per_agent"),
    ),
    (
        ".with_node_mode",
        Refused("an orchestrator is never in node mode"),
    ),
    (
        ".with_workspace_setup_states",
        Plumbing, // `Services` makes its own when none is shared.
    ),
    (
        ".with_suspend_tracker",
        Refused("host sleep and wake detection is for a desktop daemon"),
    ),
    (
        "services.with_suspend_tracker",
        Refused("host sleep and wake detection is for a desktop daemon"),
    ),
    // Restores and agents, before the tenant's work starts (`recover_tenant`).
    (
        "intent_services::run_boot_settings_migrations",
        Wired("run_boot_settings_migrations"),
    ),
    (
        "services.sweep_provider_profile_secrets",
        Wired("sweep_provider_profile_secrets"),
    ),
    (
        "services.migrate_remote_choices",
        Wired("migrate_remote_choices"),
    ),
    ("intent_app::agents::process_cap", Wired("process_cap")),
    (
        "intent_app::agents::start_agent_manager",
        Wired("start_agent_manager"),
    ),
    ("intent_core::agent_logs_root", Wired("agent_logs_root")),
    ("intent_core::chief_cwd_root", Wired("chief_cwd_root")),
    ("manager.registry().cap", Wired("registry().cap")),
    (
        "services.store().reset_all_active_flags",
        Wired("reset_all_active_flags"),
    ),
    (
        "services.heal_stale_agent_sessions",
        Wired("heal_stale_agent_sessions"),
    ),
    (
        "services.ensure_usage_activity_tracked_since",
        Wired("ensure_usage_activity_tracked_since"),
    ),
    (
        "services.configure_remote_machines",
        Wired("configure_remote_machines"),
    ),
    ("tunnel::resolve_tailcat_bin", Wired("resolve_tailcat_bin")),
    (
        "intent_git::node_git::configure_receive_hooks",
        Wired("configure_receive_hooks"),
    ),
    (
        "services.finish_pending_deletes",
        Wired("finish_pending_deletes"),
    ),
    ("services.rearm_delete_graces", Wired("rearm_delete_graces")),
    (
        "intent_app::agents::restore_startup_queues",
        Wired("restore_startup_queues"),
    ),
    (
        "manager.rehydrate_stop_redeliveries",
        Wired("rehydrate_stop_redeliveries"),
    ),
    (
        "services.prepare_startup_completion_recovery",
        Wired("prepare_startup_completion_recovery"),
    ),
    (
        "services.reconcile_startup_completions",
        Wired("reconcile_startup_completions"),
    ),
    // Shared with `intentd` through `TenantRuntime::start`.
    (
        "intent_app::tenant::restore_registries",
        Wired("restore_registries"),
    ),
    (
        "intent_app::tenant::start_background",
        Wired("start_background"),
    ),
    ("settle_startup_attachment_retention", Wired("into_tasks")),
    (
        "spawn_stream_retention_loop",
        Wired("spawn_tenant_retention_loop"),
    ),
    (
        "spawn_idempotency_reap_loop",
        Wired("spawn_tenant_reap_loop"),
    ),
    // The tenant's stop (its self-fence's order).
    (
        "services.shutdown_pty_sessions",
        Wired("shutdown_pty_sessions"),
    ),
    ("services.host_exec_runtime", Wired("host_exec_runtime")),
    (
        "manager.shutdown_with_native_outcome",
        Wired("manager.shutdown"),
    ),
    // A graceful stop drains the tenant first (`ServedTenant::drain`,
    // through the shared `intent_app::shutdown` steps).
    ("manager.begin_shutdown", Wired("manager.begin_shutdown")),
    (
        "intent_app::shutdown::close_admission",
        Wired("intent_app::shutdown::close_admission"),
    ),
    (
        "intent_app::shutdown::stop_host_commands",
        Wired("intent_app::shutdown::stop_host_commands"),
    ),
    (
        "intent_app::shutdown::checkpoint_agents",
        Wired("intent_app::shutdown::checkpoint_agents"),
    ),
    (
        "services.shutdown_store_writers",
        Wired("shutdown_store_writers"),
    ),
    (
        "intent_services::host_exec_stream::registry",
        Wired("host_exec_stream::registry"),
    ),
    // Client listeners (`crate::listen`).
    ("ensure_tls_certificate", Wired("ensure_tls_certificate")),
    ("get_or_create_token", Wired("get_or_create_token")),
    ("spawn_proc_usage_sampler", Wired("sample_usage")),
    (
        "resolve_token_store",
        Refused("the owner token is a tenant secret (`crate::listen`)"),
    ),
    (
        "boot_ws_listener",
        Refused("the listeners are configured from the environment (`ListenConfig`)"),
    ),
    (
        "resolve_ws_listener_port",
        Refused("the listeners are configured from the environment (`ListenConfig`)"),
    ),
    (
        "ws_options_from_env",
        Refused("the listeners are configured from the environment (`ListenConfig`)"),
    ),
    (
        "ensure_loopback_bind",
        Refused("the listeners bind what `ListenConfig` names"),
    ),
    (
        "services.attach_server_control",
        Refused("settings do not start or stop the orchestrator's listeners"),
    ),
    (
        "services.attach_machine_routes",
        Refused("an orchestrator is never a machine (`executor.mint` is refused)"),
    ),
    (
        "executor::start_in_daemon",
        Refused("an orchestrator is never a machine: it runs no backend's agents"),
    ),
    (
        "services.attach_invite_link_builder",
        Gap("`workspace.invite.list` rows carry no `url`"),
    ),
    (
        "services.attach_agent_rpc",
        Gap("the Assistant's `ws.rpc` has no loopback connection"),
    ),
    (
        "spawn_route_info_sampler",
        Gap("`system.status` reports no local addresses or hostname"),
    ),
    (
        "spawn_workspaces_disk_sampler",
        Gap("`system.status` reports no workspaces disk space"),
    ),
    (
        "intent_services::try_workspaces_provisioning_parent",
        Gap("`system.status` reports no workspaces disk space"),
    ),
    // Process-wide daemon setup.
    (
        "acquire_data_dir_lock",
        Refused("each tenant is held by its Postgres lease instead"),
    ),
    (
        "acquire_single_instance",
        Refused("each tenant is held by its Postgres lease instead"),
    ),
    (
        "apply_startup_pins",
        Refused("a tenant's paths are set explicitly, not pinned from the environment"),
    ),
    ("fd_limit::raise_at_startup", Wired("fd_limit::raise_at_startup")),
    ("probe_cow_at_startup", Wired("intent_git::cow_probe")),
    ("intent_core::workload::init", Wired("intent_core::workload::init")),
    (
        "intent_services::workload_memory_ceiling",
        Refused("one ceiling for the whole process: `INTENTD_WORKLOAD_MEMORY_MAX_MB` sets it, no tenant's `agents.workloadMemoryMaxMb`"),
    ),
    (
        "init_adapter_slots",
        Refused("an orchestrator spawns no adapter: one-shot completions run on machines"),
    ),
    (
        "max_concurrent_adapters",
        Refused("an orchestrator spawns no adapter: one-shot completions run on machines"),
    ),
    (
        "resolve_permission_policy",
        Refused("`INTENTD_PERMISSION_POLICY` is a desktop override; tenants use their setting"),
    ),
    // Local agent processes, which an orchestrator never runs.
    (
        "manager.start_unsloth_idle_reaper",
        Refused("local models: an orchestrator runs no agent itself"),
    ),
    (
        "manager.set_tree_probe",
        Refused("local agent process trees: an orchestrator runs no agent itself"),
    ),
    (
        "manager.registry().set_memory_budget",
        Refused("local agent memory: an orchestrator runs no agent itself"),
    ),
    (
        "agent_memory_budget_bytes",
        Refused("local agent memory: an orchestrator runs no agent itself"),
    ),
    (
        "recommended_memory_budget_bytes",
        Refused("local agent memory: an orchestrator runs no agent itself"),
    ),
    (
        "host_total_memory_bytes",
        Refused("local agent memory: an orchestrator runs no agent itself"),
    ),
    (
        "spawn_child_tree_sampler",
        Refused("local agent process trees: an orchestrator runs no agent itself"),
    ),
    (
        "intent_app::agents::spawn_idle_reap_loop",
        Wired("spawn_idle_reap_loop"),
    ),
    // Resume on start.
    (
        "services.prepare_startup_resume",
        Wired("prepare_startup_resume"),
    ),
    (
        "intent_app::resume::run_startup_resume_sweep",
        Wired("run_startup_resume_sweep"),
    ),
    (
        "should_resume_on_start",
        Refused("`--resume-all`, update restarts and display detection are the desktop daemon's: a claimed tenant resumes unless the setting is `off` (`resume_on_claim`)"),
    ),
    (
        "resolve_resume_setting",
        Refused("the desktop daemon's test default for `auto` (an e2e seam)"),
    ),
    (
        "detect_has_display",
        Refused("an orchestrator has no display: `auto` resumes, as on a headless daemon"),
    ),
    // Repository context.
    (
        "api_services.initialize_repository_wire",
        Gap("qualified repository reads are unavailable"),
    ),
    (
        "services.initialize_gitlab_repository_binding",
        Gap("the GitLab repository binding is not restored at start"),
    ),
    (
        "services.prewarm_repository_metadata",
        Gap("repository metadata is read on first use"),
    ),
    // Watchers.
    (
        "spawn_watcher_registry_init",
        Gap("file changes in head's worktrees do not refresh clients"),
    ),
    (
        "services.git_status_cache",
        Gap("file changes in head's worktrees do not refresh clients"),
    ),
    (
        "services.worktree_presence",
        Gap("file changes in head's worktrees do not refresh clients"),
    ),
    (
        "spawn_config_watcher_init",
        Gap("a tenant's `config.toml` is not reloaded when edited"),
    ),
    // External MCP servers: a tenant's hub refuses stdio (it starts no
    // process); agents get the tools of its http servers. The tenant's
    // self-fence stops them.
    ("services.mcp_hub", Wired("mcp_hub")),
    ("mcp_hub.spawn_health_monitor", Wired("spawn_health_monitor")),
    (
        "services.start_enabled_mcp_servers",
        Wired("start_enabled_mcp_servers"),
    ),
    (
        "mcp_hub.begin_background_shutdown",
        Wired("begin_background_shutdown"),
    ),
    ("mcp_hub.shutdown", Wired("mcp.shutdown")),
    (
        "settle_mcp_background",
        Plumbing, // The self-fence cancels the two tasks before its MCP step.
    ),
    (".with_host_exec", Wired("with_host_exec")),
    // `SQLite` only.
    ("store.activate_incremental_vacuum", Refused("SQLite only")),
    (
        "store.spawn_periodic_wal_checkpoint",
        Refused("SQLite only"),
    ),
    (
        "spawn_retired_note_paging_cleanup",
        Refused("SQLite only: Postgres never had the retired paging tables"),
    ),
    // The desktop host: sleep and wake, the sitter, the tunnel.
    (
        "suspend::spawn_suspend_detector",
        Refused("host sleep and wake detection is for a desktop daemon"),
    ),
    (
        "services.resume_suspend_interrupted_agents",
        Refused("host sleep and wake detection is for a desktop daemon"),
    ),
    (
        "tunnel::recycle_on_resume",
        Refused("host sleep and wake detection is for a desktop daemon"),
    ),
    (
        "spawn_staged_restart_watcher",
        Refused("the sitter's staged updates: an orchestrator is not run by the sitter"),
    ),
    (
        "spawn_idle_update_requester",
        Refused("the sitter's staged updates: an orchestrator is not run by the sitter"),
    ),
    (
        "services.command_script_activity",
        Refused("the sitter's staged updates: an orchestrator is not run by the sitter"),
    ),
    // Seen by the wider scan: constructors, fields and teardown.
    ("EventBus::new", Wired("EventBus::new")),
    ("PrimaryReverseRegistry::new", Wired("PrimaryReverseRegistry::new")),
    ("Services::new", Wired("Services::new_with_secret_file")),
    // Replaced: a tenant's settings are its Postgres row (`crate::settings`),
    // never a config.toml under its directory.
    ("intent_services::SettingsRegistry::load", Wired("SettingsRegistry::load_from_store")),
    ("Store::prepare_for_daemon", Wired("Store::try_open_postgres_owned")),
    ("prepared.open", Wired("Store::try_open_postgres_owned")),
    ("RpcLimiter::new", Wired("RpcLimiter::new")),
    ("AsyncTokenStore::new", Wired("AsyncTokenStore::new")),
    ("WsApiServer::new_startup", Wired("WsApiServer::new_with_reverse")),
    ("local.serve", Wired("serve_uds_with_connections")),
    ("intent_transport::listener::StartupLocalListener::bind", Wired("serve_uds_with_connections")),
    ("server.start", Wired("ws.start")),
    ("tenant_background.stop_loops", Wired("adopt_task")),
    // Replaced: the GitLab pair's handle goes to the tenant's encrypted
    // `TenantSecrets` rows, never a secrets.json file.
    ("intent_core::FileSecretStore::new", Wired("FileSecretStore::with_backend")),
    ("GitStatusRefresher::start_with_presence", Gap("file changes in head's worktrees do not refresh clients")),
    ("git_status_refresher.shutdown", Gap("file changes in head's worktrees do not refresh clients")),
    ("intent_services::SharedWatchHub::new", Gap("file changes in head's worktrees do not refresh clients")),
    ("intent_services::WatchHealth::default", Gap("`system.status` reports no file watch coverage")),
    ("intent_transport::SharedGuestLimits::new", Gap("`sharing.maxGuestConnections` and `maxConnectionsPerGuest` are not enforced")),
    ("guest_limits.follow", Gap("`sharing.maxGuestConnections` and `maxConnectionsPerGuest` are not enforced")),
    ("intent_transport::InviteLinkResolver::new", Gap("`workspace.invite.list` rows carry no `url`")),
    ("intent_transport::LoopbackAgentRpc::new", Gap("the Assistant's `ws.rpc` has no loopback connection")),
    ("host_exec_runtime.begin_shutdown", Wired("host_exec_runtime().begin_shutdown")),
    ("rpc_limiter.begin_shutdown", Gap("a stopping tenant's in-flight RPCs are not fenced and drained")),
    ("rpc_limiter.drain", Gap("a stopping tenant's in-flight RPCs are not fenced and drained")),
    ("reverse_registry.begin_shutdown", Gap("the tenant's client event publisher is not drained when it stops")),
    ("reverse_registry.shutdown_publisher", Gap("the tenant's client event publisher is not drained when it stops")),
    ("bus.shutdown", Gap("the tenant's event writer is not drained when it stops")),
    ("ChildTreeUsage::default", Refused("local agent process trees: an orchestrator runs no agent itself")),
    ("WorkspacesDiskUsage::default", Gap("`system.status` reports no workspaces disk space")),
    ("shutdown_store.close", Refused("the Postgres pool is shared by every tenant and closes with the process")),
    ("postgres.unavailable", Refused("intentd refuses Postgres; the orchestrator is the Postgres backend")),
    ("intent_core::process_policy::ProcessPolicy::current", Refused("`INTENTD_PRIVATE_TEST_PROFILE` guards intentd's `--insecure`; the orchestrator's listeners have their own")),
    ("WorkspaceSetupStates::default", Plumbing),
    ("startup::Bootstrap::new", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("startup::WorkAdmission::new", Refused("the tenant's admissions are its fence (`Unfenced`)")),
    ("bootstrap.stop", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("bootstrap.stop_local_listener", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("cancellation.own_services", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("cancellation.request_stop", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("cancellation.services_owned", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("cancellation.stopped", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("cancellation.stopping", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("dispatch.install", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("dispatch.retired", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("dispatch.set_policy_check", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("dispatch.status().cancelled", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("dispatch.status().fail", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("dispatch.status().finalizing", Refused("the desktop daemon's protected startup handoff: an orchestrator opens its tenants before it listens")),
    ("shutdown::Phase::start", Plumbing),
    ("control.start_ws_listener", Refused("settings do not start or stop the orchestrator's listeners")),
    ("control.stop_ws_listener", Refused("settings do not start or stop the orchestrator's listeners")),
    ("registry.persist_selected_ws_api_port", Refused("settings do not start or stop the orchestrator's listeners")),
    ("snapshot.ws_api_port_policy", Refused("settings do not start or stop the orchestrator's listeners")),
    ("server.start_with_port_assignment", Refused("settings do not start or stop the orchestrator's listeners")),
    ("settings_registry.snapshot", Plumbing),
    ("tunnel.start", Refused("the tailcat tunnel to the desktop daemon: an orchestrator is reached through its own ingress")),
    ("intent_core::ServerControl::stop_tunnel", Refused("the tailcat tunnel to the desktop daemon: an orchestrator is reached through its own ingress")),
    ("exact_update::ExactUpdate::default", Refused("the sitter's staged updates: an orchestrator is not run by the sitter")),
    ("IdleUpdateState::with_command_scripts", Refused("the sitter's staged updates: an orchestrator is not run by the sitter")),
    ("idle_update_state.latch_shutdown_cause", Refused("the sitter's staged updates: an orchestrator is not run by the sitter")),
    ("idle_update_state.restart_exit_fired", Refused("the sitter's staged updates: an orchestrator is not run by the sitter")),
    ("executor::ExecutorSlot::default", Refused("an orchestrator is never a machine: it runs no backend's agents")),
    ("intent_transport::node_link::ExecutorLinks::new", Refused("an orchestrator is never a machine: it runs no backend's agents")),
    ("server.install_executor_links", Refused("an orchestrator is never a machine: it runs no backend's agents")),
    ("slot.agent_roots", Refused("an orchestrator is never a machine: it runs no backend's agents")),
    ("slot.linked_backends", Refused("an orchestrator is never a machine: it runs no backend's agents")),
    ("service.shutdown", Refused("an orchestrator is never a machine: it runs no backend's agents")),
    ("intent_transport::MachineRoutesResolver::new", Refused("an orchestrator is never a machine (`executor.mint` is refused)")),
    ("suspend::ForcedSuspendOverlap::new", Refused("host sleep and wake detection is for a desktop daemon")),
    // Plumbing.
    ("cmd_serve", Plumbing),
    ("cmd_serve_owned", Plumbing),
    ("banner_build_commit", Plumbing),
    ("env_flag", Plumbing),
    ("parse_locality_mode", Plumbing),
    ("parse_resume_test_default", Plumbing),
    ("resolve_config", Plumbing),
    ("resolve_daemon_mode", Plumbing),
    ("shutdown_signal", Plumbing),
    ("signal", Plumbing),
    ("startup::admission_unchanged", Plumbing),
    ("startup::preparing_test_gate", Plumbing),
    ("initialize_gitlab_test_transports", Plumbing),
    ("intent_core::spawn_daemon", Plumbing),
    ("services.secret_source", Plumbing),
    ("services.remote_feature().map_or", Plumbing),
    ("services.mcp_servers_create", Plumbing),
    ("services.mcp_servers_toggle", Plumbing),
    ("services.settings_update", Plumbing),
    (".with_reverse_registry", Plumbing),
];

/// Calls never worth classifying: the standard library, Tokio, JSON.
const IGNORED_PREFIXES: &[&str] = &[
    "std::",
    "tokio::",
    "serde_json::",
    "u64::",
    "Arc::",
    "Duration::",
    "ExitCode::",
    "SignalKind::",
];

/// Methods too generic to be a startup step wherever they are called:
/// conversions, accessors, joins, channel and admission plumbing.
const GENERIC_METHODS: &[&str] = &[
    "clone",
    "map_err",
    "unwrap",
    "expect",
    "ok",
    "err",
    "as_ref",
    "iter",
    "map",
    "collect",
    "to_string",
    "into",
    "is_some",
    "is_none",
    "send",
    "abort",
    "unwrap_or",
    "unwrap_or_default",
    "unwrap_or_else",
    "join",
    "display",
    "and_then",
    "filter",
    "cloned",
    "get",
    "take",
    "set",
    "lock",
    "is_empty",
    "len",
    "as_str",
    "to_owned",
    "push",
    "is_ok",
    "is_err",
    "as_deref",
    "contains",
    "subscribe",
    "recv",
    "borrow",
    "wait_for",
    "changed",
    "next",
    "clone_into",
    "is_finished",
    "notified",
    "read_u8",
    "write_u8",
    "checked_add",
    "any",
    "extend",
    "then",
    "as_secs",
    "as_millis",
    "parse",
    "origin",
    "fingerprint256",
    "is_unspecified",
    "get_or_insert",
    "allowed",
    "spawn",
    "complete",
    "failed",
    "status",
];

/// Rust keywords the free-call scan sees followed by `(`.
const KEYWORDS: &[&str] = &[
    "if", "match", "while", "for", "loop", "return", "move", "async", "fn", "in", "as", "let",
    "mut", "ref", "drop", "else", "await",
];

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

/// `name`'s body in `source`: from `async fn name(` to the next top-level
/// closing brace.
fn body<'a>(source: &'a str, name: &str) -> &'a str {
    let start = source
        .find(&format!("async fn {name}("))
        .unwrap_or_else(|| panic!("intentd's main.rs has no `async fn {name}`"));
    let end = source[start..]
        .find("\n}\n")
        .map_or(source.len(), |end| start + end);
    &source[start..end]
}

/// Every startup call of `intentd serve`, keyed `receiver.method` (the
/// receiver normalized: any `*services*` binding is `services`), `.with_*`,
/// or the called path.
fn startup_calls() -> BTreeSet<String> {
    let main = std::fs::read_to_string(crates_dir().join("intentd/src/main.rs")).unwrap();
    let text = format!(
        "{}\n{}",
        body(&main, "cmd_serve"),
        body(&main, "cmd_serve_owned")
    );
    let comments = regex::Regex::new(r"//[^\n]*").unwrap();
    let attributes = regex::Regex::new(r"#\[[^\]]*\]").unwrap();
    let chains = regex::Regex::new(r"\n\s*\.").unwrap();
    let text = comments.replace_all(&text, "");
    let text = attributes.replace_all(&text, "");
    let text = chains.replace_all(&text, ".");

    let mut calls = BTreeSet::new();
    // A method call on a lowercase binding or field, through any chain of
    // argument-free calls (`a.b().c(`), unless it is a generic method.
    let method = regex::Regex::new(r"\b([a-z_]\w*)\.((?:\w+\(\)\.)*(\w+))\(").unwrap();
    for found in method.captures_iter(&text) {
        if GENERIC_METHODS.contains(&&found[3]) {
            continue;
        }
        let receiver = match &found[1] {
            "api_services" => "api_services",
            r if r.contains("services") => "services",
            r => r,
        };
        calls.insert(format!("{receiver}.{}", &found[2]));
    }
    let builder = regex::Regex::new(r"\.(with_\w+)\(").unwrap();
    for found in builder.captures_iter(&text) {
        calls.insert(format!(".{}", &found[1]));
    }
    // A free call (a path of lowercase segments) or a type's function
    // (`Type::name(`, `module::Type::name(`): called, and not itself after
    // `.` or `::`.
    let path = regex::Regex::new(r"[A-Za-z_]\w*(?:::[A-Za-z_]\w*)*").unwrap();
    for found in path.find_iter(&text) {
        let call = found.as_str();
        let called = text[found.end()..].starts_with('(');
        let after_dot_or_path = text[..found.start()].ends_with(['.', ':']);
        let segments: Vec<&str> = call.split("::").collect();
        let lower =
            |segment: &&str| segment.starts_with(|c: char| c.is_ascii_lowercase() || c == '_');
        let free = segments.iter().all(lower);
        // `Type::name`: one capitalized segment, right before a lowercase
        // function name.
        let associated = segments.len() >= 2
            && lower(&segments[segments.len() - 1])
            && !lower(&segments[segments.len() - 2])
            && segments[..segments.len() - 2].iter().all(lower);
        if !called
            || after_dot_or_path
            || !(free || associated)
            || KEYWORDS.contains(&call)
            || IGNORED_PREFIXES.iter().any(|p| call.starts_with(p))
        {
            continue;
        }
        calls.insert(call.to_owned());
    }
    calls
}

/// The orchestrator's sources and the shared startup code, tests and
/// comments left out, method chains joined onto one line.
fn orchestrator_sources() -> String {
    let crates = crates_dir();
    let comments = regex::Regex::new(r"//[^\n]*").unwrap();
    let chains = regex::Regex::new(r"\n\s*\.").unwrap();
    let files = [
        "intentd-orchestrator/src/lib.rs",
        "intentd-orchestrator/src/main.rs",
        "intentd-orchestrator/src/mcp.rs",
        "intentd-orchestrator/src/serve.rs",
        "intentd-orchestrator/src/listen.rs",
        "intentd-orchestrator/src/process.rs",
        "intentd-orchestrator/src/secrets.rs",
        "intent-app/src/lib.rs",
        "intent-app/src/tenant.rs",
        "intent-app/src/agents.rs",
        "intent-app/src/shutdown.rs",
        "intent-services/src/self_fence.rs",
    ];
    files
        .iter()
        .map(|file| {
            let text = std::fs::read_to_string(crates.join(file))
                .unwrap_or_else(|e| panic!("{file}: {e}"));
            let code = match text.find("#[cfg(test)]\nmod tests") {
                Some(tests) => &text[..tests],
                None => &text,
            };
            let code = comments.replace_all(code, "");
            chains.replace_all(&code, ".").into_owned()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Whether `sources` call `name` (`name(`), not just define it
/// (`fn name(`).
fn calls_in(sources: &str, name: &str) -> bool {
    let needle = format!("{name}(");
    sources.match_indices(&needle).any(|(at, _)| {
        let before = &sources[..at];
        let word_start = !before.ends_with(|c: char| c.is_alphanumeric() || c == '_');
        word_start && !before.ends_with("fn ")
    })
}

#[test]
fn every_intentd_startup_step_is_wired_refused_or_a_known_gap() {
    let calls = startup_calls();
    let steps: BTreeMap<&str, &Step> = STEPS.iter().map(|(key, step)| (*key, step)).collect();
    assert_eq!(steps.len(), STEPS.len(), "a step is listed twice");
    let sources = orchestrator_sources();

    let unlisted: Vec<&String> = calls
        .iter()
        .filter(|call| !steps.contains_key(call.as_str()))
        .collect();
    assert!(
        unlisted.is_empty(),
        "intentd's startup (crates/intentd/src/main.rs, cmd_serve_owned) calls {unlisted:?}, \
         which the orchestrator neither does nor refuses: wire it in \
         intentd-orchestrator (serve.rs) or the shared startup in intent-app, or list it in \
         this test's STEPS as Refused (with why) or a Gap (with its follow-up)"
    );
    let stale: Vec<&str> = steps
        .keys()
        .copied()
        .filter(|key| !calls.contains(*key))
        .collect();
    assert!(
        stale.is_empty(),
        "STEPS lists {stale:?}, which intentd's startup no longer calls: remove them"
    );
    let unwired: Vec<(&str, &str)> = STEPS
        .iter()
        .filter_map(|(key, step)| match step {
            Wired(call) if !calls_in(&sources, call) => Some((*key, *call)),
            _ => None,
        })
        .collect();
    assert!(
        unwired.is_empty(),
        "STEPS says these are wired, but the orchestrator's sources never call them: \
         {unwired:?}"
    );
    // The steps shared through `intent_app::tenant` run only because the
    // orchestrator starts a `TenantRuntime` for each tenant.
    let serve =
        std::fs::read_to_string(crates_dir().join("intentd-orchestrator/src/serve.rs")).unwrap();
    assert!(
        calls_in(&serve, "TenantRuntime::start_with_sweeps"),
        "serve.rs no longer starts a TenantRuntime: the shared steps do not run"
    );
    for (key, step) in STEPS {
        if let Refused(why) | Gap(why) = step {
            assert!(!why.is_empty(), "{key}: say why");
        }
    }
}

/// The guard sees the steps it must: the agent manager and the receive
/// hooks, which the orchestrator once skipped.
#[test]
fn the_startup_scan_finds_the_steps_the_orchestrator_once_skipped() {
    let calls = startup_calls();
    for step in [
        "intent_app::agents::start_agent_manager",
        "intent_git::node_git::configure_receive_hooks",
        "services.store().reset_all_active_flags",
        "intent_app::tenant::restore_registries",
        "init_adapter_slots",
        "mcp_hub.spawn_health_monitor",
    ] {
        assert!(calls.contains(step), "{step} not found in {calls:?}");
    }
}
