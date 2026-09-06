use std::collections::BTreeSet;

use super::manifest::{Manifest, ManifestSource, Region};

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
}
