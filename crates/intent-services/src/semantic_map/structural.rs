use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::manifest::{Manifest, ManifestSource, Region};

pub const STRUCTURAL_SCAN_MAX_FILES: usize = 100_000;
pub const STRUCTURAL_SCAN_MAX_DURATION: Duration = Duration::from_secs(2);

#[derive(Debug, Default)]
pub struct StructuralScan {
    pub paths: Vec<String>,
    pub walk_errors: usize,
    pub limit_reached: bool,
}

/// Walks a worktree for structural-map inputs, bounded by file count and wall time.
#[must_use]
pub fn scan_structural_paths(root: &Path) -> StructuralScan {
    record_structural_scan(root);
    let entries = ignore::WalkBuilder::new(root)
        .hidden(false)
        .filter_entry(|entry| {
            !matches!(
                entry.file_name().to_str(),
                Some(".git" | "target" | "node_modules")
            )
        })
        .build()
        .map(|entry| {
            entry.map(|entry| {
                entry
                    .file_type()
                    .is_some_and(|kind| kind.is_file())
                    .then(|| entry.path().to_path_buf())
            })
        });
    collect_structural_paths(
        root,
        entries,
        STRUCTURAL_SCAN_MAX_FILES,
        STRUCTURAL_SCAN_MAX_DURATION,
    )
}

#[cfg(test)]
fn record_structural_scan(root: &Path) {
    *structural_scan_counts()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(root.to_path_buf())
        .or_default() += 1;
}

#[cfg(not(test))]
fn record_structural_scan(_root: &Path) {}

fn collect_structural_paths<I, E>(
    root: &Path,
    entries: I,
    max_files: usize,
    max_duration: Duration,
) -> StructuralScan
where
    I: IntoIterator<Item = Result<Option<PathBuf>, E>>,
{
    let started = Instant::now();
    let mut scan = StructuralScan::default();
    for entry in entries {
        if scan.paths.len() >= max_files || started.elapsed() >= max_duration {
            scan.limit_reached = true;
            break;
        }
        match entry {
            Ok(Some(path)) => {
                if let Ok(relative) = path.strip_prefix(root) {
                    scan.paths
                        .push(relative.to_string_lossy().replace('\\', "/"));
                }
            }
            Ok(None) => {}
            Err(_) => scan.walk_errors += 1,
        }
    }
    scan
}

#[cfg(test)]
fn structural_scan_counts() -> &'static std::sync::Mutex<std::collections::HashMap<PathBuf, usize>>
{
    static COUNTS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<PathBuf, usize>>,
    > = std::sync::OnceLock::new();
    COUNTS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
pub(crate) fn reset_structural_scan_count(root: &Path) {
    structural_scan_counts()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(root);
}

#[cfg(test)]
pub(crate) fn structural_scan_count(root: &Path) -> usize {
    structural_scan_counts()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(root)
        .copied()
        .unwrap_or_default()
}

#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn structural_manifest<I, S>(paths: I) -> Manifest
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut units = BTreeSet::new();
    for path in paths {
        let normalized = path.as_ref().replace('\\', "/");
        let parts: Vec<&str> = normalized
            .trim_start_matches("./")
            .split('/')
            .filter(|part| !part.is_empty())
            .collect();
        if parts.len() < 2 {
            continue;
        }
        units.insert(parts[0].to_string());
        if parts[0] == "packages" && parts.len() >= 3 {
            units.insert(format!("packages/{}", parts[1]));
            if parts[2] == "crates" && parts.len() >= 5 {
                units.insert(format!("packages/{}/crates/{}", parts[1], parts[3]));
            }
        } else if parts[0] == "crates" && parts.len() >= 3 {
            units.insert(format!("crates/{}", parts[1]));
        }
    }

    let count = units.len();
    let columns = count.clamp(1, 4);
    let rows = count.div_ceil(columns).max(1);
    let regions = units
        .into_iter()
        .enumerate()
        .map(|(index, path)| {
            let column = index % columns;
            let row = index / columns;
            Region {
                id: slug(&path),
                label: label(&path),
                responsibility: format!("This is where files under {path} live."),
                parent: None,
                anchor: [
                    (column + 1) as f32 / (columns + 1) as f32,
                    (row + 1) as f32 / (rows + 1) as f32,
                ],
                paths: vec![format!("{path}/**")],
                color: None,
            }
        })
        .collect();

    Manifest {
        version: 1,
        regions,
        crossings: Vec::new(),
        source: ManifestSource::Structural,
    }
}

fn slug(path: &str) -> String {
    path.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn label(path: &str) -> String {
    path.rsplit('/')
        .next()
        .unwrap_or(path)
        .split(['-', '_'])
        .filter(|word| !word.is_empty())
        .map(|word| {
            let mut chars = word.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_uppercase().chain(chars).collect()
            })
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semantic_map::{classify, AssignmentConfidence};

    #[test]
    fn fallback_is_deterministic_and_classifies_package_and_crate_paths() {
        let first = structural_manifest([
            "packages/intentd/crates/intent-services/src/lib.rs",
            "packages/cloudlands-fe/src/app.ts",
            "docs/README.md",
        ]);
        let second = structural_manifest([
            "docs/README.md",
            "packages/cloudlands-fe/src/app.ts",
            "packages/intentd/crates/intent-services/src/lib.rs",
        ]);
        assert_eq!(first, second);
        assert_eq!(first.source, ManifestSource::Structural);
        assert_eq!(
            classify(&first, "packages/intentd/crates/intent-services/src/lib.rs").confidence,
            AssignmentConfidence::Curated
        );
        assert!(first.regions.iter().all(|region| region
            .anchor
            .iter()
            .all(|value| (0.0..=1.0).contains(value))));
    }

    #[test]
    fn walker_counts_injected_errors_and_honors_file_bound() {
        let root = Path::new("/workspace");
        let entries: Vec<Result<Option<PathBuf>, &str>> = vec![
            Ok(Some(root.join("src/lib.rs"))),
            Err("injected walk error"),
            Ok(Some(root.join("src/main.rs"))),
        ];
        let scan = collect_structural_paths(root, entries, 1, Duration::from_secs(60));
        assert_eq!(scan.paths, ["src/lib.rs"]);
        assert_eq!(scan.walk_errors, 0);
        assert!(scan.limit_reached);

        let errors = collect_structural_paths(
            root,
            vec![Err::<Option<PathBuf>, _>("injected walk error")],
            10,
            Duration::from_secs(60),
        );
        assert_eq!(errors.walk_errors, 1);
        assert!(!errors.limit_reached);
    }
}
