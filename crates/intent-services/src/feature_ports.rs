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
//! [`Services::new`] starts with [`UnavailableTransfer`], which fails every
//! transfer RPC with the same error as the `WorkspaceApi` default. The
//! composition root registers the real implementation (`intent-svc-transfer`)
//! with [`Services::with_transfer_port`].

use std::any::Any;
#[cfg(any(test, feature = "test-hooks"))]
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::Mutex;

use intent_core::transfer::TransferPlan;
use intent_core::{BoxFuture, Error, Result, WorkspaceId};

use crate::Services;

/// Maximum DECODED bytes per transfer chunk (`workspace.import.chunk`, and
/// `workspace.export.read`). Base64 inflates this by 4/3 on the wire
/// (~21.4 MiB), keeping the full JSON-RPC frame comfortably under the
/// 40 MiB inbound cap (PROTOCOL §1.3). Core's file reads share the cap.
pub const IMPORT_MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;

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

    /// Test seam: register an export of `workspace_id` whose archive is
    /// still building, owned by `initiator`, under `export_id`. Core tests
    /// use it to hold an export open without running a build.
    ///
    /// The test seams have panicking defaults: whether they exist depends on
    /// this crate's `test-hooks` feature, which an implementing crate cannot
    /// observe, so it implements them only under its own `cfg(test)`.
    #[cfg(any(test, feature = "test-hooks"))]
    fn park_building_export_for_test(
        &self,
        export_id: &str,
        workspace_id: &WorkspaceId,
        initiator: Option<intent_core::PrincipalId>,
        staging_dir: std::path::PathBuf,
    ) {
        let _ = (export_id, workspace_id, initiator, staging_dir);
        unimplemented!("this transfer port has no test seams");
    }

    /// Test seam: drop the export registered under `export_id`, if any.
    #[cfg(any(test, feature = "test-hooks"))]
    fn remove_export_for_test(&self, export_id: &str) {
        let _ = export_id;
        unimplemented!("this transfer port has no test seams");
    }

    /// Test seam: how many exports are registered.
    #[cfg(any(test, feature = "test-hooks"))]
    fn export_count_for_test(&self) -> usize {
        unimplemented!("this transfer port has no test seams");
    }
}

/// The transfer port of a `Services` no transfer feature was registered on:
/// every RPC fails with the `WorkspaceApi` default "not implemented" error,
/// and no export is ever in flight.
///
/// Under `test-hooks` it records the exports parked through the test seams,
/// so core tests can hold an export open without the transfer feature.
#[derive(Default)]
pub(crate) struct UnavailableTransfer {
    /// Parked exports: `export_id` to workspace id.
    #[cfg(any(test, feature = "test-hooks"))]
    parked_exports: Mutex<HashMap<String, String>>,
}

fn not_implemented<'a, T: Send + 'a>(method: &str) -> BoxFuture<'a, Result<T>> {
    let error = Error::Internal(format!("WorkspaceApi::{method} not implemented"));
    Box::pin(async move { Err(error) })
}

impl TransferPort for UnavailableTransfer {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn transfer_plan<'a>(
        &'a self,
        _svc: &'a Services,
        _id: WorkspaceId,
    ) -> BoxFuture<'a, Result<TransferPlan>> {
        not_implemented("workspace_transfer_plan")
    }

    fn import_begin<'a>(
        &'a self,
        _svc: &'a Services,
        _manifest: serde_json::Value,
        _archive_size_bytes: u64,
        _archive_sha256: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_import_begin")
    }

    fn import_chunk<'a>(
        &'a self,
        _svc: &'a Services,
        _import_id: String,
        _seq: u64,
        _data: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_import_chunk")
    }

    fn import_commit<'a>(
        &'a self,
        _svc: &'a Services,
        _import_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_import_commit")
    }

    fn import_abort<'a>(
        &'a self,
        _svc: &'a Services,
        _import_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_import_abort")
    }

    fn export_start<'a>(
        &'a self,
        _svc: &'a Services,
        _id: WorkspaceId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_export_start")
    }

    fn export_read<'a>(
        &'a self,
        _svc: &'a Services,
        _export_id: String,
        _seq: u64,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_export_read")
    }

    fn export_finalize<'a>(
        &'a self,
        _svc: &'a Services,
        _export_id: String,
        _archive_source: bool,
        _final_status_message: Option<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_export_finalize")
    }

    fn export_abort<'a>(
        &'a self,
        _svc: &'a Services,
        _export_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_export_abort")
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn export_in_progress(&self, _workspace_id: &str) -> bool {
        false
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn export_in_progress(&self, workspace_id: &str) -> bool {
        self.parked_exports
            .lock()
            .expect("parked exports")
            .values()
            .any(|parked| parked == workspace_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn park_building_export_for_test(
        &self,
        export_id: &str,
        workspace_id: &WorkspaceId,
        _initiator: Option<intent_core::PrincipalId>,
        _staging_dir: std::path::PathBuf,
    ) {
        self.parked_exports
            .lock()
            .expect("parked exports")
            .insert(export_id.to_string(), workspace_id.as_str().to_string());
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn remove_export_for_test(&self, export_id: &str) {
        self.parked_exports
            .lock()
            .expect("parked exports")
            .remove(export_id);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn export_count_for_test(&self) -> usize {
        self.parked_exports.lock().expect("parked exports").len()
    }
}

/// The registered ports. Cloned with `Services`; every clone shares the
/// same feature objects (and so the same feature state).
#[derive(Clone)]
pub(crate) struct FeaturePorts {
    transfer: Arc<dyn TransferPort>,
}

impl Default for FeaturePorts {
    fn default() -> Self {
        Self {
            transfer: Arc::new(UnavailableTransfer::default()),
        }
    }
}

impl FeaturePorts {
    pub(crate) fn transfer(&self) -> &dyn TransferPort {
        self.transfer.as_ref()
    }
}

impl Services {
    /// Register the workspace transfer feature. Every clone made afterwards
    /// shares it; clones made before keep the port they had.
    #[must_use]
    pub fn with_transfer_port(mut self, port: Arc<dyn TransferPort>) -> Self {
        self.ports.transfer = port;
        self
    }
}

#[cfg(test)]
mod tests {
    use intent_core::{Error, WorkspaceApi, WorkspaceId};

    use crate::test_support::fixtures::TempDb;
    use crate::Services;

    #[tokio::test]
    async fn unregistered_transfer_port_fails_like_the_workspace_api_default() {
        let tmp = TempDb::new();
        let svc = Services::new(intent_store::Store::open(&tmp.path).await.unwrap());
        let ws = WorkspaceId::new();
        let expect = |result: intent_core::Result<serde_json::Value>, method: &str| {
            assert!(
                matches!(&result, Err(Error::Internal(m)) if m == &format!("WorkspaceApi::{method} not implemented")),
                "{method}: {result:?}"
            );
        };
        assert!(matches!(
            svc.workspace_transfer_plan(ws.clone()).await,
            Err(Error::Internal(m)) if m == "WorkspaceApi::workspace_transfer_plan not implemented"
        ));
        expect(
            svc.workspace_import_begin(serde_json::json!({}), 0, String::new())
                .await,
            "workspace_import_begin",
        );
        expect(
            svc.workspace_import_chunk("i".into(), 0, String::new())
                .await,
            "workspace_import_chunk",
        );
        expect(
            svc.workspace_import_commit("i".into()).await,
            "workspace_import_commit",
        );
        expect(
            svc.workspace_import_abort("i".into()).await,
            "workspace_import_abort",
        );
        expect(
            svc.workspace_export_start(ws.clone()).await,
            "workspace_export_start",
        );
        expect(
            svc.workspace_export_read("e".into(), 0).await,
            "workspace_export_read",
        );
        expect(
            svc.workspace_export_finalize("e".into(), false, None).await,
            "workspace_export_finalize",
        );
        expect(
            svc.workspace_export_abort("e".into()).await,
            "workspace_export_abort",
        );
        assert!(!svc.feature_transfer_port().export_in_progress(ws.as_str()));
    }
}
