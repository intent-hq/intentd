//! Feature ports: the seams through which core code (`WorkspaceApi`
//! forwarders, shared helpers) reaches a feature domain without naming its
//! types.
//!
//! A port is a `Send + Sync` trait defined here and implemented by the
//! feature. Its methods take `&Services` as an explicit context argument, so
//! the feature object never holds a `Services` handle (no `Arc` cycle).
//! Feature state (for example in-flight transfer sessions) lives in the
//! feature's own object, which the feature reads back through the typed
//! [`FeatureSet`] ([`Services::feature`]), not in `Services` fields.
//!
//! The ports are declared once, in the `feature_port_registry!` call below.
//! [`Services::new`] starts each with its "unavailable" default (for
//! transfer, [`UnavailableTransfer`], which fails every RPC with the same
//! error as the `WorkspaceApi` default). The composition root
//! (`intent_app::compose`) registers the real implementations
//! ([`Services::with_transfer_port`]) and then runs the startup check
//! [`Services::ensure_feature_ports_registered`], so a missing registration
//! fails at boot rather than at the first call. `port_forwarders!` writes
//! the one-line `WorkspaceApi` forwarders.

use std::any::{Any, TypeId};
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

/// Typed feature state on [`Services`], keyed by type.
///
/// A feature crate stores its state object here once, when the composition
/// root registers it ([`Services::with_feature`]), and its extension-trait
/// code reads it back with [`Services::feature`]. The set is frozen once the
/// `Services` is shared: it changes only through the owned builder methods,
/// and clones share the same objects.
#[derive(Clone, Default)]
pub(crate) struct FeatureSet {
    features: Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl FeatureSet {
    fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.features
            .get(&TypeId::of::<T>())
            .and_then(|feature| feature.downcast_ref::<T>())
    }

    fn insert<T: Any + Send + Sync>(&mut self, feature: Arc<T>) {
        let previous = Arc::make_mut(&mut self.features).insert(TypeId::of::<T>(), feature);
        assert!(
            previous.is_none(),
            "feature {} registered twice",
            std::any::type_name::<T>()
        );
    }
}

impl Services {
    /// Store a feature's state object. Every clone made afterwards shares
    /// it; clones made before do not see it.
    ///
    /// # Panics
    ///
    /// If a feature of the same type is already registered: each feature is
    /// registered once, by the composition root.
    #[must_use]
    pub fn with_feature<T: Any + Send + Sync>(mut self, feature: Arc<T>) -> Self {
        self.features.insert(feature);
        self
    }

    /// The registered feature state of type `T`, if any.
    #[must_use]
    pub fn feature<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.features.get::<T>()
    }
}

/// A registered port and whether the composition root registered it (as
/// opposed to the "unavailable" default [`Services::new`] starts with).
struct PortSlot<P: ?Sized> {
    port: Arc<P>,
    registered: bool,
}

impl<P: ?Sized> Clone for PortSlot<P> {
    fn clone(&self) -> Self {
        Self {
            port: Arc::clone(&self.port),
            registered: self.registered,
        }
    }
}

/// Declares the feature port registry: the [`FeaturePorts`] fields, their
/// accessors, the `Services::with_*_port` setters, and the list the startup
/// check walks.
///
/// Adding a port is one line here: `name: Trait = UnavailableDefault,
/// setter;`. Every port declared here is a production port, so
/// [`Services::ensure_feature_ports_registered`] fails until the composition
/// root registers it.
macro_rules! feature_port_registry {
    ($(
        $(#[$setter_doc:meta])*
        $name:ident: $port:ident = $default:ty, $setter:ident;
    )*) => {
        /// The registered ports. Cloned with `Services`; every clone shares
        /// the same feature objects (and so the same feature state).
        #[derive(Clone)]
        pub(crate) struct FeaturePorts {
            $($name: PortSlot<dyn $port>,)*
        }

        impl Default for FeaturePorts {
            fn default() -> Self {
                Self {
                    $($name: PortSlot {
                        port: Arc::new(<$default>::default()),
                        registered: false,
                    },)*
                }
            }
        }

        impl FeaturePorts {
            /// Every declared port, by name.
            pub(crate) const NAMES: &'static [&'static str] = &[$(stringify!($name)),*];

            $(
                pub(crate) fn $name(&self) -> &dyn $port {
                    self.$name.port.as_ref()
                }
            )*

            /// The declared ports no feature registered, in declaration
            /// order.
            pub(crate) fn unregistered(&self) -> Vec<&'static str> {
                let registered = [$(self.$name.registered),*];
                Self::NAMES
                    .iter()
                    .zip(registered)
                    .filter(|(_, registered)| !registered)
                    .map(|(name, _)| *name)
                    .collect()
            }
        }

        impl Services {
            $(
                $(#[$setter_doc])*
                #[must_use]
                pub fn $setter(mut self, port: Arc<dyn $port>) -> Self {
                    self.ports.$name = PortSlot {
                        port,
                        registered: true,
                    };
                    self
                }
            )*
        }
    };
}

feature_port_registry! {
    /// Register the workspace transfer feature. Every clone made afterwards
    /// shares it; clones made before keep the port they had.
    transfer: TransferPort = UnavailableTransfer, with_transfer_port;
}

/// Writes `WorkspaceApi` methods that forward to a feature port: each
/// `fn method(&self, args..) -> Ret => port_method;` line expands to
/// `self.ports.<port>().port_method(self, args..)`. Use it inside
/// `impl WorkspaceApi for Services`.
macro_rules! port_forwarders {
    ($port:ident {
        $(
            fn $method:ident(&self $(, $arg:ident: $ty:ty)* $(,)?) -> $ret:ty => $target:ident;
        )*
    }) => {
        $(
            fn $method(&self $(, $arg: $ty)*) -> $ret {
                self.ports.$port().$target(self $(, $arg)*)
            }
        )*
    };
}
pub(crate) use port_forwarders;

impl Services {
    /// The declared feature ports that no feature registered. Empty once the
    /// composition root has run.
    #[must_use]
    pub fn unregistered_feature_ports(&self) -> Vec<&'static str> {
        self.ports.unregistered()
    }

    /// The startup check: fails, naming every missing port, unless each
    /// declared feature port was registered. An unregistered port would
    /// otherwise surface only at its first call, as an "unavailable" error.
    ///
    /// # Errors
    ///
    /// [`Error::Internal`] listing the unregistered ports.
    pub fn ensure_feature_ports_registered(&self) -> Result<()> {
        let missing = self.unregistered_feature_ports();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(Error::Internal(format!(
                "feature ports not registered: {}",
                missing.join(", ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use intent_core::{Error, WorkspaceApi, WorkspaceId};

    use super::{FeaturePorts, TransferPort, UnavailableTransfer};

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

    async fn bare_services(tmp: &TempDb) -> Services {
        Services::new(intent_store::Store::open(&tmp.path).await.unwrap())
    }

    #[tokio::test]
    async fn the_startup_check_names_every_unregistered_port() {
        let tmp = TempDb::new();
        let svc = bare_services(&tmp).await;
        assert_eq!(svc.unregistered_feature_ports(), FeaturePorts::NAMES);
        assert_eq!(svc.unregistered_feature_ports(), ["transfer"]);
        assert!(matches!(
            svc.ensure_feature_ports_registered(),
            Err(Error::Internal(m)) if m == "feature ports not registered: transfer"
        ));
    }

    #[tokio::test]
    async fn a_registered_port_passes_the_startup_check_and_receives_calls() {
        let tmp = TempDb::new();
        let port = Arc::new(UnavailableTransfer::default());
        let svc = bare_services(&tmp)
            .await
            .with_transfer_port(Arc::clone(&port) as Arc<dyn TransferPort>);
        assert!(svc.unregistered_feature_ports().is_empty());
        svc.ensure_feature_ports_registered()
            .expect("every port registered");
        let ws = WorkspaceId::new();
        port.park_building_export_for_test("e1", &ws, None, std::path::PathBuf::new());
        assert!(
            svc.clone()
                .feature_transfer_port()
                .export_in_progress(ws.as_str()),
            "the forwarders and clones reach the registered object"
        );
    }

    #[derive(Debug, Default)]
    struct Counter(std::sync::atomic::AtomicUsize);

    #[tokio::test]
    async fn the_feature_set_returns_the_registered_object_to_every_later_clone() {
        let tmp = TempDb::new();
        let before = bare_services(&tmp).await;
        assert!(before.feature::<Counter>().is_none());
        let counter = Arc::new(Counter::default());
        let svc = before.clone().with_feature(Arc::clone(&counter));
        let clone = svc.clone();
        clone
            .feature::<Counter>()
            .expect("registered")
            .0
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(counter.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(std::ptr::eq(
            svc.feature::<Counter>().unwrap(),
            Arc::as_ptr(&counter)
        ));
        assert!(
            before.feature::<Counter>().is_none(),
            "a clone made before registration does not see it"
        );
        assert!(svc.feature::<String>().is_none());
    }

    #[tokio::test]
    #[should_panic(expected = "registered twice")]
    async fn a_feature_registers_once() {
        let tmp = TempDb::new();
        let _ = bare_services(&tmp)
            .await
            .with_feature(Arc::new(Counter::default()))
            .with_feature(Arc::new(Counter::default()));
    }
}
