use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

const MAX_DIRECTORIES: usize = 256;
const MAX_FILES: usize = 512;
const MAX_ENTRIES: usize = 4096;
const MAX_DEPTH: usize = 8;
const MAX_FILE_BYTES: u64 = 1_048_576;

pub(crate) fn user_root(intent_root: &Path) -> Option<PathBuf> {
    let parent = intent_root.parent()?;
    if parent.file_name()? != ".intent" || intent_root.file_name()? != "specialists" {
        return None;
    }
    Some(parent.parent()?.join(".claude/agents"))
}

pub(crate) fn collect(root: &Path, source: &str) -> BTreeMap<String, Value> {
    let mut scan = Scan::default();
    scan.walk(root, source, 0);
    scan.definitions
}

pub(crate) fn watch_directories(root: &Path) -> Vec<PathBuf> {
    let mut scan = Scan {
        watching: true,
        ..Scan::default()
    };
    scan.observe_link(root);
    scan.watch_parent(root);
    scan.walk(root, "user", 0);
    scan.watches.extend(scan.directories);
    scan.watches.into_iter().collect()
}

#[derive(Default)]
struct Scan {
    directories: BTreeSet<PathBuf>,
    files: BTreeSet<PathBuf>,
    definitions: BTreeMap<String, Value>,
    watches: BTreeSet<PathBuf>,
    entries: usize,
    watching: bool,
}

impl Scan {
    fn walk(&mut self, path: &Path, source: &str, depth: usize) {
        if depth > MAX_DEPTH
            || self.directories.len() >= MAX_DIRECTORIES
            || self.files.len() >= MAX_FILES
            || self.entries >= MAX_ENTRIES
        {
            return;
        }
        let Ok(canonical) = std::fs::canonicalize(path) else {
            return;
        };
        if !self.directories.insert(canonical) {
            return;
        }
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        let mut entries: Vec<_> = entries.flatten().take(MAX_ENTRIES - self.entries).collect();
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            if self.files.len() >= MAX_FILES || self.entries >= MAX_ENTRIES {
                break;
            }
            self.entries += 1;
            let path = entry.path();
            if self.watching {
                self.observe_link(&path);
            }
            let Ok(metadata) = std::fs::metadata(&path) else {
                continue;
            };
            if metadata.is_dir() {
                if !entry.file_name().to_string_lossy().starts_with('.') {
                    self.walk(&path, source, depth + 1);
                }
                continue;
            }
            if !metadata.is_file()
                || metadata.len() > MAX_FILE_BYTES
                || path.extension().and_then(|s| s.to_str()) != Some("md")
            {
                continue;
            }
            let Ok(canonical) = std::fs::canonicalize(&path) else {
                continue;
            };
            if !self.files.insert(canonical) {
                continue;
            }
            let Some(definition) = read_definition(&path, source) else {
                continue;
            };
            let id = definition["id"].as_str().unwrap().to_string();
            self.definitions.entry(id).or_insert(definition);
        }
    }

    fn watch_parent(&mut self, path: &Path) {
        let mut parent = path.parent();
        while let Some(path) = parent {
            if let Ok(canonical) = std::fs::canonicalize(path) {
                self.watches.insert(canonical);
                break;
            }
            parent = path.parent();
        }
    }

    fn observe_link(&mut self, path: &Path) {
        if std::fs::canonicalize(path).is_ok_and(|canonical| canonical == path) {
            return;
        }
        let mut link = path.to_path_buf();
        for _ in 0..40 {
            let mut alias = link.as_path();
            while let Some(parent) = alias.parent() {
                if let Ok(canonical) = std::fs::canonicalize(parent) {
                    let unchanged = canonical == parent;
                    self.watches.insert(canonical);
                    if unchanged {
                        break;
                    }
                }
                alias = parent;
            }
            let Ok(target) = std::fs::read_link(&link) else {
                break;
            };
            link = link.parent().unwrap_or(&link).join(target);
        }
        self.watch_parent(&link);
    }
}

fn read_definition(path: &Path, source: &str) -> Option<Value> {
    let content = std::fs::read_to_string(path).ok()?.replace("\r\n", "\n");
    let content = content.trim_start_matches('\u{feff}');
    let mut lines = content.lines();
    if lines.next()?.trim() != "---" {
        return None;
    }
    let mut frontmatter = String::new();
    let mut closed = false;
    for line in lines.by_ref() {
        if line.trim() == "---" {
            closed = true;
            break;
        }
        frontmatter.push_str(line);
        frontmatter.push('\n');
    }
    if !closed {
        return None;
    }
    let parsed: Value = serde_yaml::from_str(&frontmatter).ok()?;
    let metadata = parsed.as_object()?;
    let name = metadata.get("name")?.as_str()?.trim();
    super::validate_id(name).ok()?;
    let description = metadata.get("description")?.as_str()?.trim();
    if description.is_empty() {
        return None;
    }
    let mut unsupported: BTreeSet<String> = metadata
        .keys()
        .filter(|key| {
            !matches!(
                key.as_str(),
                "name" | "description" | "model" | "skills" | "color"
            )
        })
        .cloned()
        .collect();
    let mut prompt = lines.collect::<Vec<_>>().join("\n").trim().to_string();
    if let Some(skills) = metadata.get("skills") {
        if let Some(names) = skills
            .as_array()
            .and_then(|names| names.iter().map(Value::as_str).collect::<Option<Vec<_>>>())
        {
            if names
                .iter()
                .any(|name| name.trim().is_empty() || name.contains(['\n', '\r']))
            {
                unsupported.insert("skills".into());
            } else if !names.is_empty() {
                prompt.push_str("\n\n## Required skills\nBefore starting, load and follow these skills from the available skills catalog:\n");
                for name in names {
                    prompt.push_str("- ");
                    prompt.push_str(name);
                    prompt.push('\n');
                }
            }
        } else {
            unsupported.insert("skills".into());
        }
    }
    let mut definition = json!({
        "id":name, "name":name, "description":description,
        "codingAgent":"claude-code", "prompt":prompt, "behaviorPrompt":prompt,
        "source":source, "path":path.to_string_lossy(),
        "isCustomized":false, "importedFrom":"claude-code"
    });
    if let Some(model) = metadata.get("model") {
        match model.as_str().map(str::trim) {
            Some("inherit" | "") => {}
            Some(model) if !model.contains(':') => {
                definition["model"] = json!(model);
            }
            _ => {
                unsupported.insert("model".into());
            }
        }
    }
    if !unsupported.is_empty() {
        definition["unsupportedFields"] = json!(unsupported);
    }
    Some(definition)
}
