pub mod activity;
pub mod classifier;
pub mod manifest;
pub mod paths;
pub mod routes;
pub mod structural;

pub use activity::{
    project, project_with_classifier, project_with_paths, MapActivity, MapActivityKind,
};
pub use classifier::{
    classify, Assignment, AssignmentConfidence, Classifier, ClassifyPath, MAX_CLASSIFY_BYTES,
    MAX_CLASSIFY_PATHS,
};
pub use manifest::{
    parse_manifest, Crossing, Manifest, ManifestError, ManifestLoadError, ManifestLoader,
    ManifestSource, Region, MANIFEST_TAG,
};
pub use paths::{
    invalidate_workspace_paths, invalidate_workspace_paths_on_event,
    normalize_workspace_relative_path, workspace_paths, WorkspacePathError, WorkspacePaths,
};
pub use routes::{derive_route, Route, RouteFilter, RouteTransition};
pub use structural::{scan_structural_paths, structural_manifest, StructuralScan};
