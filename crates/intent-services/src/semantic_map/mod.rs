pub mod activity;
pub mod classifier;
pub mod manifest;
pub mod routes;
pub mod structural;

pub use activity::{project, MapActivity, MapActivityKind};
pub use classifier::{classify, Assignment, AssignmentConfidence, Classifier};
pub use manifest::{
    parse_manifest, Crossing, Manifest, ManifestError, ManifestLoadError, ManifestLoader,
    ManifestSource, Region, MANIFEST_TAG,
};
pub use routes::{derive_route, Route, RouteFilter, RouteTransition};
pub use structural::structural_manifest;
