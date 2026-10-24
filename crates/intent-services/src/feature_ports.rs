//! Feature ports: the seams through which core code (`WorkspaceApi`
//! forwarders, shared helpers) reaches a feature domain without naming its
//! types.
//!
//! A port is a `Send + Sync` trait defined here and implemented by the
//! feature. Its methods take `&Services` as an explicit context argument, so
//! the feature object never holds a `Services` handle (no `Arc` cycle).
//! Feature state (for example in-flight transfer sessions) lives in the
//! object that implements the port, not in `Services` fields.
//!
//! [`Services::new`] registers the in-crate implementation of every port, so
//! production wiring is unchanged.

use std::any::Any;
use std::sync::Arc;

use intent_core::transfer::TransferPlan;
use intent_core::{BoxFuture, Result, WorkspaceId};

use crate::Services;

/// Maximum DECODED bytes per transfer chunk (`workspace.import.chunk`, and
/// `workspace.export.read`). Base64 inflates this by 4/3 on the wire
/// (~21.4 MiB), keeping the full JSON-RPC frame comfortably under the
/// 40 MiB inbound cap (PROTOCOL §1.3). Core's file reads share the cap.
pub(crate) const IMPORT_MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;

/// The workspace transfer feature (`workspace.transferPlan`,
/// `workspace.import.*`, `workspace.export.*`). Each method performs the
/// full RPC, authorization included, exactly as the `WorkspaceApi` method it
/// backs.
pub trait TransferPort: Send + Sync + 'static {
    /// The concrete implementation, for the feature's own typed state lookup.
    fn as_any(&self) -> &dyn Any;

    fn transfer_plan<'a>(
        &'a self,
        svc: &'a Services,
        id: WorkspaceId,
    ) -> BoxFuture<'a, Result<TransferPlan>>;

    fn import_begin<'a>(
        &'a self,
        svc: &'a Services,
        manifest: serde_json::Value,
        archive_size_bytes: u64,
        archive_sha256: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn import_chunk<'a>(
        &'a self,
        svc: &'a Services,
        import_id: String,
        seq: u64,
        data: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn import_commit<'a>(
        &'a self,
        svc: &'a Services,
        import_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn import_abort<'a>(
        &'a self,
        svc: &'a Services,
        import_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn export_start<'a>(
        &'a self,
        svc: &'a Services,
        id: WorkspaceId,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn export_read<'a>(
        &'a self,
        svc: &'a Services,
        export_id: String,
        seq: u64,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn export_finalize<'a>(
        &'a self,
        svc: &'a Services,
        export_id: String,
        archive_source: bool,
        final_status_message: Option<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn export_abort<'a>(
        &'a self,
        svc: &'a Services,
        export_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// True while an export of `workspace_id` is in flight (between
    /// `workspace.export.start` and `finalize`/`abort`/a failed build).
    fn export_in_progress(&self, workspace_id: &str) -> bool;
}

/// The registered ports. Cloned with `Services`; every clone shares the
/// same feature objects (and so the same feature state).
#[derive(Clone)]
pub(crate) struct FeaturePorts {
    transfer: Arc<dyn TransferPort>,
}

impl FeaturePorts {
    pub(crate) fn new(transfer: Arc<dyn TransferPort>) -> Self {
        Self { transfer }
    }

    pub(crate) fn transfer(&self) -> &dyn TransferPort {
        self.transfer.as_ref()
    }
}
