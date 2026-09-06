use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use serde::{Deserialize, Serialize};

use super::manifest::Manifest;
use super::paths::WorkspacePaths;

pub const UNSORTED_REGION_ID: &str = "unsorted";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssignmentConfidence {
    Curated,
    Unsorted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    pub region_id: String,
    pub confidence: AssignmentConfidence,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ClassifyPath {
    Path(String),
    Rooted {
        path: String,
        #[serde(
            rename = "gitRootId",
            alias = "git_root_id",
            default,
            skip_serializing_if = "Option::is_none"
        )]
        git_root_id: Option<String>,
    },
}

impl ClassifyPath {
    fn parts(&self) -> (&str, Option<&str>) {
        match self {
            Self::Path(path) => (path, None),
            Self::Rooted { path, git_root_id } => (path, git_root_id.as_deref()),
        }
    }
}

#[must_use]
pub fn classify(manifest: &Manifest, rel_path: &str) -> Assignment {
    Classifier::new(manifest).classify(rel_path)
}

struct RegionMatcher {
    region_id: String,
    gitignore: Gitignore,
}

pub struct Classifier {
    regions: Vec<RegionMatcher>,
    assignments: Mutex<HashMap<String, Assignment>>,
}

impl Classifier {
    #[must_use]
    pub fn new(manifest: &Manifest) -> Self {
        let regions = manifest
            .regions
            .iter()
            .filter_map(|region| {
                let mut builder = GitignoreBuilder::new("");
                for pattern in &region.paths {
                    let _ = builder.add_line(None, pattern);
                }
                builder.build().ok().map(|gitignore| RegionMatcher {
                    region_id: region.id.clone(),
                    gitignore,
                })
            })
            .collect();
        Self {
            regions,
            assignments: Mutex::new(HashMap::new()),
        }
    }

    pub fn classify(&self, rel_path: &str) -> Assignment {
        let normalized = normalize_path(rel_path);
        let mut assignments = self
            .assignments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assignments
            .entry(normalized.clone())
            .or_insert_with(|| self.classify_uncached(&normalized))
            .clone()
    }

    fn classify_uncached(&self, normalized: &str) -> Assignment {
        let path = Path::new(normalized);
        let mut assigned: Option<&str> = None;
        for region in &self.regions {
            let pattern_match = region.gitignore.matched_path_or_any_parents(path, false);
            if pattern_match.is_ignore() {
                assigned = Some(&region.region_id);
            } else if pattern_match.is_whitelist() && assigned == Some(region.region_id.as_str()) {
                assigned = None;
            }
        }
        assigned.map_or_else(
            || Assignment {
                region_id: UNSORTED_REGION_ID.to_string(),
                confidence: AssignmentConfidence::Unsorted,
            },
            |region_id| Assignment {
                region_id: region_id.to_string(),
                confidence: AssignmentConfidence::Curated,
            },
        )
    }

    pub fn classify_path(&self, paths: &WorkspacePaths, input: &ClassifyPath) -> Assignment {
        let (path, git_root_id) = input.parts();
        self.classify(&paths.normalize(path, git_root_id))
    }

    #[cfg(test)]
    fn cached_paths(&self) -> usize {
        self.assignments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

fn normalize_path(path: &str) -> String {
    crate::file_tracking::normalize_path(path)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use serde_json::json;

    use super::*;
    use crate::semantic_map::{ManifestSource, Region};

    fn manifest() -> Manifest {
        Manifest {
            version: 1,
            source: ManifestSource::Curated,
            regions: vec![
                Region {
                    id: "code".into(),
                    label: "Code".into(),
                    responsibility: "Code".into(),
                    parent: None,
                    anchor: [0.0, 0.0],
                    paths: vec!["src/**".into(), "!src/generated/**".into()],
                    color: None,
                },
                Region {
                    id: "generated".into(),
                    label: "Generated".into(),
                    responsibility: "Generated".into(),
                    parent: None,
                    anchor: [1.0, 1.0],
                    paths: vec!["src/generated/**".into()],
                    color: None,
                },
            ],
            crossings: vec![],
        }
    }

    #[test]
    fn later_match_wins_and_bang_excludes() {
        let mut manifest = manifest();
        assert_eq!(classify(&manifest, "src/main.rs").region_id, "code");
        assert_eq!(
            classify(&manifest, "src/generated/model.rs").region_id,
            "generated"
        );
        manifest.regions.pop();
        assert_eq!(
            classify(&manifest, "src/generated/model.rs").confidence,
            AssignmentConfidence::Unsorted
        );
    }

    #[test]
    fn reusable_classifier_memoizes_normalized_paths() {
        let manifest = manifest();
        let classifier = Classifier::new(&manifest);
        classifier.classify("./src/main.rs");
        classifier.classify("src/main.rs");
        assert_eq!(classifier.cached_paths(), 1);
    }

    #[test]
    fn compiled_classifier_preserves_precedence_golden() {
        let manifest = manifest();
        let classifier = Classifier::new(&manifest);
        let golden = [
            ("src/main.rs", "code", AssignmentConfidence::Curated),
            (
                "src/generated/model.rs",
                "generated",
                AssignmentConfidence::Curated,
            ),
            (
                "README.md",
                UNSORTED_REGION_ID,
                AssignmentConfidence::Unsorted,
            ),
        ];
        for (path, region_id, confidence) in golden {
            let assignment = classifier.classify(path);
            assert_eq!(assignment.region_id, region_id);
            assert_eq!(assignment.confidence, confidence);
        }
    }

    #[test]
    fn classifies_five_thousand_paths_with_precompiled_matchers() {
        let mut manifest = manifest();
        manifest.regions = (0..16)
            .map(|index| Region {
                id: format!("region-{index}"),
                label: format!("Region {index}"),
                responsibility: format!("Region {index}"),
                parent: None,
                anchor: [0.5, 0.5],
                paths: vec![format!("region-{index}/**")],
                color: None,
            })
            .collect();
        let classifier = Classifier::new(&manifest);
        let started = Instant::now();
        for index in 0..5_000 {
            let region = index % 16;
            assert_eq!(
                classifier
                    .classify(&format!("region-{region}/src/file-{index}.rs"))
                    .region_id,
                format!("region-{region}")
            );
        }
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn registered_root_makes_submodule_path_workspace_relative() {
        let mut manifest = manifest();
        manifest.regions[0].id = "transport-rpc".into();
        manifest.regions[0].paths = vec!["packages/intentd/crates/intent-transport/**".into()];
        manifest.regions.truncate(1);
        let classifier = Classifier::new(&manifest);
        let paths = WorkspacePaths::with_prefix("intentd-root", "packages/intentd");
        let rooted = ClassifyPath::Rooted {
            path: "crates/intent-transport/src/router.rs".into(),
            git_root_id: Some("intentd-root".into()),
        };
        assert_eq!(
            classifier.classify_path(&paths, &rooted).region_id,
            "transport-rpc"
        );
        assert_eq!(
            classifier
                .classify_path(
                    &paths,
                    &ClassifyPath::Path("crates/intent-transport/src/router.rs".into())
                )
                .confidence,
            AssignmentConfidence::Unsorted
        );
        assert_eq!(
            classifier
                .classify("packages/intentd/crates/intent-transport/src/router.rs")
                .region_id,
            "transport-rpc"
        );
    }

    #[test]
    fn classify_paths_accept_legacy_strings_and_root_aware_objects() {
        let input = json!([
            "src/lib.rs",
            {"path": "crates/intent-core/src/lib.rs", "gitRootId": "intentd-root"}
        ]);
        let paths: Vec<ClassifyPath> = serde_json::from_value(input.clone()).unwrap();

        assert_eq!(paths[0], ClassifyPath::Path("src/lib.rs".into()));
        assert_eq!(
            paths[1],
            ClassifyPath::Rooted {
                path: "crates/intent-core/src/lib.rs".into(),
                git_root_id: Some("intentd-root".into()),
            }
        );
        assert_eq!(serde_json::to_value(paths).unwrap(), input);
    }
}
