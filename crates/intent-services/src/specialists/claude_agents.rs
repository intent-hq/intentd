use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

const MAX_DIRECTORIES: usize = 256;
const MAX_FILES: usize = 512;
const MAX_ENTRIES: usize = 4096;
const MAX_DEPTH: usize = 8;
const MAX_FILE_BYTES: u64 = 1_048_576;
const MAX_TOTAL_FILE_BYTES: u64 = 32 * MAX_FILE_BYTES;
const MAX_DIAGNOSTICS: usize = 128;

pub(crate) fn user_root(intent_root: Option<&Path>) -> Option<PathBuf> {
    #[cfg(not(test))]
    if let Some(root) = std::env::var_os("CLAUDE_CONFIG_DIR").filter(|s| !s.is_empty()) {
        return Some(PathBuf::from(root).join("agents"));
    }
    let intent_root = intent_root?;
    let parent = intent_root.parent()?;
    if parent.file_name()? != ".intent" || intent_root.file_name()? != "specialists" {
        return None;
    }
    Some(parent.parent()?.join(".claude/agents"))
}

#[derive(Default)]
pub(crate) struct Catalog {
    pub definitions: BTreeMap<String, Value>,
    pub diagnostics: Vec<Value>,
}

pub(crate) fn add_diagnostic(diagnostics: &mut Vec<Value>, mut diagnostic: Value) {
    if diagnostics.len() >= MAX_DIAGNOSTICS {
        return;
    }
    if diagnostics.len() == MAX_DIAGNOSTICS - 1 {
        diagnostic = json!({
            "path":diagnostic["path"], "source":diagnostic["source"],
            "isDirectory":diagnostic.get("isDirectory").and_then(Value::as_bool).unwrap_or(false),
            "code":"scan-limit", "message":"Further import problems were omitted. Fix the listed files and refresh."
        });
    }
    diagnostics.push(diagnostic);
}

pub(crate) fn shadowed(definition: &Value, winner: &Value) -> Value {
    let mut diagnostic = json!({
        "path":definition["path"], "source":definition["source"],
        "code":"shadowed", "message":"Another definition with this name takes precedence.",
        "specialistId":definition["id"]
    });
    if let Some(path) = winner
        .get("path")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        diagnostic["winnerPath"] = json!(path);
    }
    diagnostic
}

pub(crate) fn collect(root: &Path, source: &str) -> Catalog {
    let mut scan = Scan::default();
    scan.walk(root, source, 0);
    Catalog {
        definitions: scan.definitions,
        diagnostics: scan.diagnostics,
    }
}

pub(crate) struct WatchDirectories {
    pub ordinary: Vec<PathBuf>,
    pub linked: Vec<PathBuf>,
}

pub(crate) fn watch_directories(root: &Path) -> WatchDirectories {
    let mut scan = Scan {
        watching: true,
        ..Scan::default()
    };
    scan.watch_parent(root);
    let mut ordinary: Vec<_> = std::mem::take(&mut scan.watches).into_iter().collect();
    scan.observe_link(root);
    scan.walk(root, "user", 0);
    ordinary.extend(scan.directories.into_keys());
    WatchDirectories {
        ordinary,
        linked: scan.watches.into_iter().collect(),
    }
}

#[derive(Default)]
struct Scan {
    directories: BTreeMap<PathBuf, usize>,
    files: BTreeSet<PathBuf>,
    definitions: BTreeMap<String, Value>,
    watches: BTreeSet<PathBuf>,
    entries: usize,
    file_bytes: u64,
    watching: bool,
    diagnostics: Vec<Value>,
}

impl Scan {
    fn problem(&mut self, path: &Path, source: &str, code: &str, message: &str) {
        if !self.watching {
            add_diagnostic(
                &mut self.diagnostics,
                json!({"path":path.to_string_lossy(), "source":source,"code":code,"message":message,"isDirectory":path.is_dir()}),
            );
        }
    }

    fn walk(&mut self, path: &Path, source: &str, depth: usize) {
        if depth > MAX_DEPTH {
            self.problem(
                path,
                source,
                "scan-limit",
                "This folder is nested too deeply. Move the agent closer to the agents folder.",
            );
            return;
        }
        let canonical = match std::fs::canonicalize(path) {
            Ok(canonical) => canonical,
            Err(error) => {
                if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
                    self.problem(
                        path,
                        source,
                        "broken-link",
                        "This link cannot be resolved. Restore its target or repair the link.",
                    );
                } else if error.kind() != std::io::ErrorKind::NotFound {
                    self.problem(
                        path,
                        source,
                        "unreadable",
                        "This folder cannot be read. Check its permissions.",
                    );
                }
                return;
            }
        };
        if self
            .directories
            .get(&canonical)
            .is_some_and(|previous| *previous <= depth)
        {
            return;
        }
        if self.directories.len() >= MAX_DIRECTORIES && !self.directories.contains_key(&canonical) {
            self.problem(
                path,
                source,
                "scan-limit",
                "The agents folder contains too many folders. Reduce the number of linked folders.",
            );
            return;
        }
        self.directories.insert(canonical, depth);
        let Ok(entries) = std::fs::read_dir(path) else {
            self.problem(
                path,
                source,
                "unreadable",
                "This folder cannot be read. Check its permissions.",
            );
            return;
        };
        let remaining = MAX_ENTRIES.saturating_sub(self.entries);
        let Ok(mut entries) = entries
            .take(remaining + 1)
            .collect::<std::io::Result<Vec<_>>>()
        else {
            self.problem(
                path,
                source,
                "unreadable",
                "This folder could not be fully read. Check its permissions and try again.",
            );
            return;
        };
        if entries.len() > remaining {
            self.entries = MAX_ENTRIES;
            self.problem(path, source, "scan-limit", "This folder exceeds the remaining discovery limit and was not scanned. Split or reduce the agents folder.");
            return;
        }
        self.entries += entries.len();
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            if self.watching {
                self.observe_link(&path);
            }
            let Ok(metadata) = std::fs::metadata(&path) else {
                let linked =
                    std::fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink());
                self.problem(
                    &path,
                    source,
                    if linked { "broken-link" } else { "unreadable" },
                    "This entry cannot be read. Check its target and permissions.",
                );
                continue;
            };
            if metadata.is_dir() {
                if !entry.file_name().to_string_lossy().starts_with('.') {
                    self.walk(&path, source, depth + 1);
                }
                continue;
            }
            if path.extension().and_then(|s| s.to_str()) != Some("md") {
                continue;
            }
            if !metadata.is_file() {
                self.problem(
                    &path,
                    source,
                    "invalid",
                    "An agent definition must be a regular Markdown file.",
                );
                continue;
            }
            let Ok(canonical) = std::fs::canonicalize(&path) else {
                continue;
            };
            if self.files.contains(&canonical) {
                continue;
            }
            if self.files.len() >= MAX_FILES {
                self.problem(&path, source, "scan-limit", "The agents folder contains too many Markdown files. Reduce the number of definitions.");
                continue;
            }
            if !self.files.insert(canonical) {
                continue;
            }
            if metadata.len() > MAX_FILE_BYTES {
                self.problem(
                    &path,
                    source,
                    "too-large",
                    "This definition exceeds 1 MiB. Reduce its size to import it.",
                );
                continue;
            }
            if self.watching {
                continue;
            }
            let definition = match read_definition(&path, source, &mut self.file_bytes) {
                Ok(definition) => definition,
                Err((code, message)) => {
                    self.problem(&path, source, code, message);
                    continue;
                }
            };
            let id = definition["id"].as_str().unwrap().to_string();
            if let Some(winner) = self.definitions.get(&id) {
                add_diagnostic(&mut self.diagnostics, shadowed(&definition, winner));
            } else {
                self.definitions.insert(id, definition);
            }
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

fn read_definition(
    path: &Path,
    source: &str,
    bytes_read: &mut u64,
) -> Result<Value, (&'static str, &'static str)> {
    let budget = MAX_TOTAL_FILE_BYTES
        .saturating_sub(*bytes_read)
        .min(MAX_FILE_BYTES);
    let exhausted = (
        "scan-limit",
        "This agents folder reached the 32 MiB read limit. Reduce the total size of its definitions.",
    );
    if budget == 0 {
        return Err(exhausted);
    }
    let mut bytes = Vec::new();
    let read =
        std::fs::File::open(path).and_then(|file| file.take(budget + 1).read_to_end(&mut bytes));
    *bytes_read = bytes_read.saturating_add(bytes.len() as u64);
    if bytes.len() as u64 > budget {
        if budget < MAX_FILE_BYTES {
            return Err(exhausted);
        }
        return Err((
            "too-large",
            "This definition exceeds 1 MiB. Reduce its size to import it.",
        ));
    }
    let unreadable = (
        "unreadable",
        "This definition cannot be read as UTF-8 text. Check its encoding and permissions.",
    );
    read.map_err(|_| unreadable)?;
    let content = String::from_utf8(bytes).map_err(|_| unreadable)?;
    let content = content.replace("\r\n", "\n");
    let content = content.trim_start_matches('\u{feff}');
    let mut lines = content.lines();
    if lines.next().is_none_or(|line| line.trim() != "---") {
        return Err((
            "invalid",
            "Add YAML frontmatter with a name and description, enclosed by --- lines.",
        ));
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
        return Err(("invalid", "Close the YAML frontmatter with a --- line."));
    }
    let parsed: Value = serde_yaml::from_str(&frontmatter).map_err(|_| {
        (
            "invalid",
            "The YAML frontmatter is invalid. Correct its syntax.",
        )
    })?;
    let metadata = parsed
        .as_object()
        .ok_or(("invalid", "The YAML frontmatter must contain named fields."))?;
    let name = metadata
        .get("name")
        .and_then(Value::as_str)
        .ok_or(("invalid", "Add a text name to the YAML frontmatter."))?
        .trim();
    super::validate_id(name).map_err(|_| {
        (
            "invalid",
            "The agent name is not a valid specialist identifier.",
        )
    })?;
    let description = metadata
        .get("description")
        .and_then(Value::as_str)
        .ok_or(("invalid", "Add a text description to the YAML frontmatter."))?
        .trim();
    if description.is_empty() {
        return Err(("invalid", "The description must not be empty."));
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
    let mut required_skills = Vec::new();
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
                    let name = name.trim();
                    if required_skills.iter().any(|s| s == name) {
                        continue;
                    }
                    required_skills.push(name.to_string());
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
    if !required_skills.is_empty() {
        definition["requiredSkills"] = json!(required_skills);
    }
    Ok(definition)
}
