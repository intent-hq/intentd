//! The runtime feature for intentd: the `script.*`, `hook.*`, `terminal.*`,
//! `browser.*` and `sandbox.*` RPCs, the setup-script terminal, and the
//! script/hook registries they share.
//!
//! The feature sits above `intent-services`: it builds against
//! [`intent_services::feature_api`], and core reaches it only through the
//! `RuntimePort` it implements. The composition root registers it with
//! [`register`]; a `Services` without it fails every runtime RPC with the
//! `WorkspaceApi` default error.
//!
//! The former `impl Services` blocks are crate-private extension traits
//! (`RuntimeStateExt`, `HookServicesExt`, `ScriptMonitorExt`, ...), so their
//! call sites read as before. [`RuntimeServicesExt`] is the part the daemon
//! calls directly.

mod browser_tabs;
mod hook_manager;
mod host_execution;
mod runtime;
mod sandbox_ops;
mod script_monitor;
mod script_ops;
mod script_output_cleanup;
mod script_output_store;
mod setup_runner;
mod terminal_ops;

pub use runtime::{register, RuntimeServicesExt};
