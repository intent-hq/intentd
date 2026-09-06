use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use ignore::gitignore::GitignoreBuilder;
use serde::{Deserialize, Serialize};

use super::manifest::Manifest;

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

pub fn classify(manifest: &Manifest, rel_path: &str) -> Assignment {
    let normalized = normalize_path(rel_path);
    let path = Path::new(&normalized);
    let mut assigned: Option<&str> = None;

    for region in &manifest.regions {
        for pattern in &region.paths {
            let mut builder = GitignoreBuilder::new("");
            if builder.add_line(None, pattern).is_err() {
                continue;
            }
            let Ok(matcher) = builder.build() else {
                continue;
            };
            let matched = matcher.matched_path_or_any_parents(path, false);
            if matched.is_ignore() {
                assigned = Some(&region.id);
            } else if matched.is_whitelist() && assigned == Some(region.id.as_str()) {
                assigned = None;
            }
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

pub struct Classifier<'a> {
    manifest: &'a Manifest,
    assignments: Mutex<HashMap<String, Assignment>>,
}

impl<'a> Classifier<'a> {
    #[must_use]
    pub fn new(manifest: &'a Manifest) -> Self {
        Self {
            manifest,
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
            .or_insert_with(|| classify(self.manifest, &normalized))
            .clone()
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
    path.replace('\\', "/")
        .trim_start_matches("./")
        .trim_start_matches('/')
        .to_string()
}

#[cfg(test)]
mod tests {
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
}
