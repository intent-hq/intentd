//! Dependency input for the note index profile revision.
//!
//! Only dependencies that can change indexed output participate: the audited
//! [`ROOTS`] and everything they resolve to in Cargo.lock (identity, source,
//! checksum and edges), their reverse dependents (which can unify features),
//! and local manifest declarations and feature chains reaching those packages. Workspace release numbers, unrelated packages and
//! lockfile layout are ignored, so a release without index changes keeps the
//! existing indexes. Shared by `build.rs` and the unit tests so the tested
//! code is the code that produces the fingerprint input. Never runs cargo.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use toml::{Table, Value};

/// The package whose note index output is fingerprinted.
pub const PACKAGE: &str = "intent-store";

/// Direct dependencies whose behavior reaches note index output. The index
/// source audit test keeps this list in sync with the index sources: `sha2`
/// only hashes the profile, `sqlx` stores rows, `uuid` mints opaque generation
/// ids and `intent-core` is workspace code. Their declarations still participate
/// when they can unify features; their implementation is not an output root.
pub const ROOTS: [&str; 5] = [
    "base64",
    "html5ever",
    "html5gum",
    "pulldown-cmark",
    "serde_json",
];

/// Bump when the canonical representation below changes meaning.
const FORMAT: &str = "note-index-dependencies:2";

const HELP: &str = "update crates/intent-store/build/note_index_dependencies.rs";

/// Files read from disk for the dependency profile.
pub struct Inputs {
    pub lock: String,
    pub manifests: Vec<String>,
    pub watched: Vec<PathBuf>,
}

/// Read the lockfile governing `manifest_dir` and every manifest of its
/// workspace (all can unify features of the fingerprinted packages).
///
/// # Errors
/// Returns an actionable message when a required file is missing, unreadable
/// or the workspace layout cannot be resolved without running cargo.
pub fn collect(manifest_dir: &Path) -> Result<Inputs, String> {
    // Workspace builds use the root lock; cargo package verification has its own
    // lock next to the extracted manifest. Never depend on the checkout layout.
    let lock_path = manifest_dir
        .ancestors()
        .map(|directory| directory.join("Cargo.lock"))
        .find(|path| path.is_file())
        .ok_or("note index profile: Cargo.lock is required next to or above the crate")?;
    let root = lock_path
        .parent()
        .ok_or("note index profile: Cargo.lock has no parent directory")?;
    let root_manifest = root.join("Cargo.toml");
    let root_text = read(&root_manifest)?;
    let root_table = parse_toml(&root_text, &root_manifest.display().to_string())?;
    let mut paths = vec![root_manifest];
    if let Some(workspace) = root_table.get("workspace") {
        paths.extend(workspace_members(root, workspace)?);
    } else if !same_path(root, manifest_dir)? {
        return Err(format!(
            "note index profile: {} has no [workspace] but governs {}; cannot resolve the workspace without cargo",
            root.display(),
            manifest_dir.display()
        ));
    }
    // Cargo also includes path dependencies implicitly. Follow all path
    // declarations (including excluded/outside-workspace dependencies and local
    // patches): their manifests can participate in feature unification too.
    let mut cursor = 0;
    let mut discovered = BTreeSet::new();
    while cursor < paths.len() {
        let path = paths[cursor].canonicalize().map_err(|error| {
            format!(
                "note index profile: cannot resolve {}: {error}",
                paths[cursor].display()
            )
        })?;
        cursor += 1;
        if !discovered.insert(path.clone()) {
            continue;
        }
        let manifest = parse_toml(&read(&path)?, &path.display().to_string())?;
        for (_, section) in dependency_sections(&manifest) {
            for spec in section
                .as_table()
                .into_iter()
                .flat_map(|table| table.values())
            {
                if let Some(relative) = spec.get("path") {
                    let relative = relative
                        .as_str()
                        .ok_or("note index profile: dependency path must be a string")?;
                    paths.push(path.parent().unwrap().join(relative).join("Cargo.toml"));
                }
            }
        }
    }
    let mut seen = BTreeSet::new();
    paths.retain(|path| seen.insert(path.canonicalize().unwrap_or_else(|_| path.clone())));
    let own = manifest_dir.join("Cargo.toml");
    if !paths
        .iter()
        .any(|path| same_path(path, &own).unwrap_or(false))
    {
        return Err(format!(
            "note index profile: {} is not a member of the workspace at {}",
            own.display(),
            root.display()
        ));
    }
    let manifests = paths
        .iter()
        .map(|path| read(path))
        .collect::<Result<_, _>>()?;
    let mut watched = vec![lock_path.clone()];
    watched.extend(paths);
    Ok(Inputs {
        lock: read(&lock_path)?,
        manifests,
        watched,
    })
}

fn workspace_members(root: &Path, workspace: &Value) -> Result<Vec<PathBuf>, String> {
    let strings = |key: &str| -> Result<Vec<String>, String> {
        match workspace.get(key) {
            None => Ok(Vec::new()),
            Some(Value::Array(values)) => values
                .iter()
                .map(|value| {
                    value.as_str().map(str::to_owned).ok_or_else(|| {
                        format!("note index profile: workspace.{key} must contain strings")
                    })
                })
                .collect(),
            Some(_) => Err(format!(
                "note index profile: workspace.{key} must be an array"
            )),
        }
    };
    let excluded: Vec<PathBuf> = strings("exclude")?
        .iter()
        .map(|path| root.join(path))
        .collect();
    let mut members = Vec::new();
    for pattern in strings("members")? {
        let (directory, glob) = match pattern.strip_suffix("/*") {
            Some(prefix) => (prefix, true),
            None => (pattern.as_str(), false),
        };
        if directory.contains(['*', '?', '[', ']']) {
            return Err(format!(
                "note index profile: unsupported workspace member pattern `{pattern}`; {HELP}"
            ));
        }
        let directory = root.join(directory);
        let mut candidates = Vec::new();
        if glob {
            let entries = std::fs::read_dir(&directory).map_err(|error| {
                format!(
                    "note index profile: cannot list {}: {error}",
                    directory.display()
                )
            })?;
            for entry in entries {
                let entry = entry.map_err(|error| {
                    format!(
                        "note index profile: cannot list {}: {error}",
                        directory.display()
                    )
                })?;
                if entry.path().join("Cargo.toml").is_file() {
                    candidates.push(entry.path());
                }
            }
            candidates.sort();
        } else {
            candidates.push(directory);
        }
        for candidate in candidates {
            if !excluded.iter().any(|path| candidate.starts_with(path)) {
                members.push(candidate.join("Cargo.toml"));
            }
        }
    }
    Ok(members)
}

fn same_path(left: &Path, right: &Path) -> Result<bool, String> {
    let canonical = |path: &Path| {
        path.canonicalize().map_err(|error| {
            format!(
                "note index profile: cannot resolve {}: {error}",
                path.display()
            )
        })
    };
    Ok(canonical(left)? == canonical(right)?)
}

fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|error| {
        format!(
            "note index profile: cannot read {}: {error}",
            path.display()
        )
    })
}

fn parse_toml(text: &str, label: &str) -> Result<Table, String> {
    text.parse::<Table>()
        .map_err(|error| format!("note index profile: cannot parse {label}: {error}"))
}

struct Package {
    name: String,
    version: String,
    source: Option<String>,
    checksum: Option<String>,
    dependencies: Vec<String>,
}

impl Package {
    // Workspace release versions never affect feature forwarding.
    fn activation_id(&self) -> String {
        if self.source.is_none() {
            format!("{} (local)", self.name)
        } else {
            self.id()
        }
    }

    fn id(&self) -> String {
        format!(
            "{} {} ({})",
            self.name,
            self.version,
            self.source.as_deref().unwrap_or("path")
        )
    }
}

/// Canonical dependency profile text hashed into the note index profile.
///
/// # Errors
/// Returns an actionable message for malformed, ambiguous or unsupported
/// lockfile or manifest data; inputs are never silently omitted.
pub fn dependency_profile(lock: &str, manifests: &[&str]) -> Result<String, String> {
    let packages = parse_lock(lock)?;
    let own = own_package(&packages)?;
    let mut lines = BTreeSet::new();
    let mut closure = BTreeSet::new();
    let mut pending = Vec::new();
    for root in ROOTS {
        let entries: Vec<&String> = packages[own]
            .dependencies
            .iter()
            .filter(|entry| entry_name(entry) == root)
            .collect();
        let [entry] = entries[..] else {
            return Err(format!(
                "note index profile: {PACKAGE} must resolve exactly one `{root}` in Cargo.lock, found {}; {HELP}",
                entries.len()
            ));
        };
        let index = resolve(&packages, entry)?;
        lines.insert(format!("root {root} -> {}", packages[index].id()));
        pending.push(index);
    }
    while let Some(index) = pending.pop() {
        if !closure.insert(index) {
            continue;
        }
        for entry in &packages[index].dependencies {
            let dependency = resolve(&packages, entry)?;
            lines.insert(format!(
                "edge {} -> {}",
                packages[index].id(),
                packages[dependency].id()
            ));
            pending.push(dependency);
        }
    }
    for &index in &closure {
        lines.insert(package_line(&packages[index])?);
    }
    // Any reverse dependent can enable a feature of a selected package.
    // Lockfiles pin external manifests by identity/checksum, so conservatively
    // include those identities instead of approximating Cargo's resolver.
    let edges = packages
        .iter()
        .map(|package| {
            package
                .dependencies
                .iter()
                .map(|entry| resolve(&packages, entry))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut relevant = closure.clone();
    loop {
        let before = relevant.len();
        for (index, dependencies) in edges.iter().enumerate() {
            if dependencies
                .iter()
                .any(|dependency| relevant.contains(dependency))
            {
                relevant.insert(index);
            }
        }
        if before == relevant.len() {
            break;
        }
    }
    let tables = manifests
        .iter()
        .enumerate()
        .map(|(position, manifest)| {
            parse_toml(manifest, &format!("workspace manifest #{position}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut local_names = BTreeSet::new();
    for name in tables
        .iter()
        .filter_map(|table| table.get("package")?.get("name")?.as_str())
    {
        if !local_names.insert(name) {
            return Err(format!("note index profile: multiple local manifests named `{name}`; cannot distinguish feature owners; {HELP}"));
        }
    }
    for &index in &relevant {
        let package = &packages[index];
        if package.source.is_some() {
            lines.insert(package_line(package)?);
        } else if index != own && !local_names.contains(package.name.as_str()) {
            return Err(format!("note index profile: local feature contributor `{}` has no collected manifest; {HELP}", package.name));
        }
        for &dependency in &edges[index] {
            if relevant.contains(&dependency) {
                lines.insert(format!(
                    "activation {} -> {}",
                    package.activation_id(),
                    packages[dependency].activation_id()
                ));
            }
        }
    }
    let names: BTreeSet<&str> = relevant
        .iter()
        .map(|&index| packages[index].name.as_str())
        .collect();
    let ambiguous_names: BTreeSet<&str> = names
        .iter()
        .copied()
        .filter(|name| {
            packages
                .iter()
                .filter(|package| package.name == *name)
                .count()
                > 1
        })
        .collect();
    let workspace_tables: Vec<&Table> = tables
        .iter()
        .filter_map(|table| table.get("workspace")?.as_table())
        .collect();
    if workspace_tables.len() > 1 {
        return Err(format!("note index profile: multiple workspace roots in collected manifests; cannot resolve inherited features; {HELP}"));
    }
    let workspace_dependencies = workspace_tables
        .first()
        .and_then(|workspace| workspace.get("dependencies")?.as_table());
    for (position, manifest) in tables.iter().enumerate() {
        let label = format!("workspace manifest #{position}");
        declarations(
            manifest,
            &label,
            &names,
            &ambiguous_names,
            workspace_dependencies,
            &mut lines,
        )?;
    }
    let mut out = String::from(FORMAT);
    for line in lines {
        out.push('\n');
        out.push_str(&line);
    }
    out.push('\n');
    Ok(out)
}

fn parse_lock(lock: &str) -> Result<Vec<Package>, String> {
    let table = parse_toml(lock, "Cargo.lock")?;
    match table.get("version").and_then(Value::as_integer) {
        Some(3 | 4) => {}
        other => {
            return Err(format!(
                "note index profile: unsupported Cargo.lock format version {other:?} (expected 3 or 4 with per-package checksums); {HELP}"
            ))
        }
    }
    let Some(Value::Array(entries)) = table.get("package") else {
        return Err("note index profile: Cargo.lock has no [[package]] entries".into());
    };
    entries
        .iter()
        .map(|entry| {
            let entry = entry
                .as_table()
                .ok_or("note index profile: Cargo.lock [[package]] must be a table")?;
            let text = |key: &str| -> Result<Option<String>, String> {
                match entry.get(key) {
                    None => Ok(None),
                    Some(Value::String(value)) => Ok(Some(value.clone())),
                    Some(_) => Err(format!(
                        "note index profile: Cargo.lock package field `{key}` must be a string"
                    )),
                }
            };
            let required = |key: &str| {
                text(key)?.ok_or_else(|| {
                    format!("note index profile: Cargo.lock package is missing `{key}`")
                })
            };
            let dependencies = match entry.get("dependencies") {
                None => Vec::new(),
                Some(Value::Array(values)) => values
                    .iter()
                    .map(|value| {
                        value.as_str().map(str::to_owned).ok_or_else(|| {
                            "note index profile: Cargo.lock dependencies must be strings".to_owned()
                        })
                    })
                    .collect::<Result<_, _>>()?,
                Some(_) => {
                    return Err(
                        "note index profile: Cargo.lock dependencies must be an array".into(),
                    )
                }
            };
            Ok(Package {
                name: required("name")?,
                version: required("version")?,
                source: text("source")?,
                checksum: text("checksum")?,
                dependencies,
            })
        })
        .collect()
}

fn own_package(packages: &[Package]) -> Result<usize, String> {
    let matches: Vec<usize> = (0..packages.len())
        .filter(|&index| packages[index].name == PACKAGE && packages[index].source.is_none())
        .collect();
    match matches[..] {
        [index] => Ok(index),
        _ => Err(format!(
            "note index profile: Cargo.lock must contain exactly one local `{PACKAGE}` package, found {}",
            matches.len()
        )),
    }
}

fn entry_name(entry: &str) -> &str {
    entry.split(' ').next().unwrap_or(entry)
}

/// Resolve a Cargo.lock dependency reference: `name`, `name version` or
/// `name version (source)`. Cargo adds the qualifiers only when needed to
/// disambiguate, so anything other than exactly one match is an error.
fn resolve(packages: &[Package], entry: &str) -> Result<usize, String> {
    let mut parts = entry.splitn(3, ' ');
    let name = parts.next().unwrap_or_default();
    let version = parts.next();
    let source = match parts.next() {
        None => None,
        Some(source) => Some(
            source
                .strip_prefix('(')
                .and_then(|source| source.strip_suffix(')'))
                .ok_or_else(|| {
                    format!("note index profile: malformed Cargo.lock dependency `{entry}`")
                })?,
        ),
    };
    let matches: Vec<usize> = (0..packages.len())
        .filter(|&index| {
            let package = &packages[index];
            package.name == name
                && version.is_none_or(|version| package.version == version)
                && source.is_none_or(|source| package.source.as_deref() == Some(source))
        })
        .collect();
    match matches[..] {
        [index] => Ok(index),
        _ => Err(format!(
            "note index profile: Cargo.lock dependency `{entry}` matches {} packages; regenerate Cargo.lock",
            matches.len()
        )),
    }
}

fn package_line(package: &Package) -> Result<String, String> {
    let id = package.id();
    match package.source.as_deref() {
        None => Err(format!(
            "note index profile: path or workspace package `{id}` is in the note index dependency graph and its sources are not fingerprinted; {HELP}"
        )),
        Some(source) if source.starts_with("registry+") || source.starts_with("sparse+") => {
            match package.checksum.as_deref() {
                Some(checksum) if !checksum.is_empty() => {
                    Ok(format!("package {id} checksum={checksum}"))
                }
                _ => Err(format!(
                    "note index profile: registry package `{id}` has no checksum in Cargo.lock; regenerate Cargo.lock"
                )),
            }
        }
        Some(source) if source.starts_with("git+") => match source.rsplit_once('#') {
            Some((_, commit)) if !commit.is_empty() => Ok(format!("package {id}")),
            _ => Err(format!(
                "note index profile: git package `{id}` has no pinned commit in Cargo.lock"
            )),
        },
        Some(_) => Err(format!(
            "note index profile: unsupported source for `{id}`; {HELP}"
        )),
    }
}

/// All places which declare dependency packages, including local overrides.
fn dependency_sections(manifest: &Table) -> Vec<(String, &Value)> {
    let mut sections: Vec<(String, &Value)> = Vec::new();
    for kind in [
        "dependencies",
        "dev-dependencies",
        "dev_dependencies",
        "build-dependencies",
        "build_dependencies",
    ] {
        if let Some(section) = manifest.get(kind) {
            sections.push((kind.to_owned(), section));
        }
        if let Some(Value::Table(targets)) = manifest.get("target") {
            for (target, table) in targets {
                if let Some(section) = table.get(kind) {
                    sections.push((format!("target.{target}.{kind}"), section));
                }
            }
        }
    }
    if let Some(section) = manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("dependencies"))
    {
        sections.push(("workspace.dependencies".to_owned(), section));
    }
    if let Some(Value::Table(patches)) = manifest.get("patch") {
        for (source, section) in patches {
            sections.push((format!("patch.{source}"), section));
        }
    }
    if let Some(section) = manifest.get("replace") {
        sections.push(("replace".to_owned(), section));
    }
    sections
}

/// Record feature-affecting declarations with their owner and dependency alias.
/// Version requirements are omitted unless duplicate locked identities need
/// disambiguation: Cargo.lock otherwise pins resolution.
fn declarations(
    manifest: &Table,
    label: &str,
    names: &BTreeSet<&str>,
    ambiguous_names: &BTreeSet<&str>,
    workspace_dependencies: Option<&Table>,
    lines: &mut BTreeSet<String>,
) -> Result<(), String> {
    let owner = manifest
        .get("package")
        .and_then(|package| package.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("workspace");
    // Resolver and package selection changes can change unification even when
    // every dependency declaration and lock entry stays the same.
    for (section, keys) in [
        (
            "workspace",
            &["resolver", "members", "exclude", "default-members"][..],
        ),
        ("package", &["resolver", "edition", "rust-version"][..]),
    ] {
        for key in keys {
            if let Some(value) = manifest.get(section).and_then(|table| table.get(*key)) {
                let value = if let Some(values) = value.as_array() {
                    let sorted: BTreeSet<String> = values.iter().map(ToString::to_string).collect();
                    format!("{sorted:?}")
                } else {
                    value.to_string()
                };
                lines.insert(format!("configuration {owner} {section}.{key} {value}"));
            }
        }
    }
    if let Some(package) = manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("package"))
    {
        for key in ["edition", "rust-version"] {
            if let Some(value) = package.get(key) {
                lines.insert(format!("configuration workspace.package.{key} {value}"));
            }
        }
    }
    let sections = dependency_sections(manifest);
    let mut keys = BTreeSet::new();
    for (section, value) in sections {
        let table = value
            .as_table()
            .ok_or_else(|| format!("note index profile: {label} [{section}] must be a table"))?;
        for (key, spec) in table {
            let inherited = if spec.get("workspace").and_then(Value::as_bool) == Some(true) {
                Some(workspace_dependencies.and_then(|dependencies| dependencies.get(key)).ok_or_else(|| {
                    format!("note index profile: {label} cannot resolve inherited dependency `{key}`")
                })?)
            } else {
                None
            };
            let package = spec
                .get("package")
                .or_else(|| inherited.and_then(|spec| spec.get("package")))
                .map(|package| {
                    package.as_str().ok_or_else(|| {
                        format!(
                            "note index profile: {label} {section}.{key}.package must be a string"
                        )
                    })
                })
                .transpose()?
                .unwrap_or(key);
            if names.contains(package) {
                keys.insert(key.clone());
                // With duplicate locked identities, constraints route each
                // alias's features to a particular version/source. Omitting
                // them would miss swaps which leave all lock edges unchanged.
                if ambiguous_names.contains(package) {
                    for field in [
                        "version",
                        "registry",
                        "registry-index",
                        "git",
                        "branch",
                        "tag",
                        "rev",
                        "path",
                    ] {
                        let value = if field == "version" && spec.is_str() {
                            Some(spec)
                        } else {
                            spec.get(field)
                                .or_else(|| inherited.and_then(|spec| spec.get(field)))
                        };
                        if let Some(value) = value {
                            lines.insert(format!(
                                "selection {owner} {section} {key}.{field} {value}"
                            ));
                        }
                    }
                }
                lines.insert(format!(
                    "declare {owner} {section} {key} -> {package} {}",
                    declaration(spec).map_err(|error| format!(
                        "note index profile: {label} {section}.{key}: {error}"
                    ))?
                ));
            }
        }
    }
    let Some(features) = manifest.get("features") else {
        return Ok(());
    };
    let features = features
        .as_table()
        .ok_or_else(|| format!("note index profile: {label} [features] must be a table"))?;
    let mut graph = BTreeMap::new();
    for (feature, values) in features {
        let values = values.as_array().ok_or_else(|| {
            format!("note index profile: {label} features.{feature} must be an array")
        })?;
        let values = values
            .iter()
            .map(|value| {
                value.as_str().ok_or_else(|| {
                    format!("note index profile: {label} features.{feature} must contain strings")
                })
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        graph.insert(feature.as_str(), values);
    }
    // Reachability only: include every feature that *could* reach a relevant
    // dependency. This intentionally ignores target/resolver activation rules,
    // conservatively covering default chains, cycles, dep: and weak dep?/feat.
    let mut relevant_features = BTreeSet::new();
    loop {
        let before = relevant_features.len();
        for (&feature, values) in &graph {
            if values.iter().any(|value| {
                let key = value.strip_prefix("dep:").unwrap_or(value);
                let key = key.split('/').next().unwrap_or(key).trim_end_matches('?');
                keys.contains(key) || relevant_features.contains(*value)
            }) {
                relevant_features.insert(feature);
            }
        }
        if before == relevant_features.len() {
            break;
        }
    }
    for feature in relevant_features {
        lines.insert(format!("feature {owner} {feature} {:?}", graph[feature]));
    }
    Ok(())
}

fn declaration(spec: &Value) -> Result<String, String> {
    let Value::Table(spec) = spec else {
        return match spec {
            Value::String(_) => {
                Ok("default=absent optional=absent workspace=absent features=[]".into())
            }
            _ => Err("dependency must be a string or table".into()),
        };
    };
    let flag = |keys: &[&str]| -> Result<String, String> {
        let mut found = None;
        for key in keys {
            match spec.get(*key) {
                None => {}
                Some(Value::Boolean(value)) if found.is_none() => found = Some(*value),
                Some(_) => return Err(format!("`{key}` must be a single boolean")),
            }
        }
        Ok(found.map_or_else(|| "absent".to_owned(), |value| value.to_string()))
    };
    let mut features = match spec.get("features") {
        None => Vec::new(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| "features must contain strings".to_owned())
            })
            .collect::<Result<_, _>>()?,
        Some(_) => return Err("features must be an array".into()),
    };
    features.sort();
    features.dedup();
    Ok(format!(
        "default={} optional={} workspace={} features=[{}]",
        flag(&["default-features", "default_features"])?,
        flag(&["optional"])?,
        flag(&["workspace"])?,
        features.join(",")
    ))
}
