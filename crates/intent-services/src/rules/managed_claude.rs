//! Explicit workspace instruction snapshot for the certified Claude route.
//! Native settings discovery stays disabled: this reads text, never executes
//! config, hooks, skills or commands. Unsupported import/rule scopes defer
//! before selection rather than quietly losing or broadening instructions.
use std::collections::BTreeSet;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use pulldown_cmark::{Event, Parser, Tag};
use regex::Regex;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkspaceInstructions {
    pub files: Vec<(String, String)>,
}

type Result<T> = std::result::Result<T, ()>;

struct Loader {
    boundary: PathBuf,
    visited: BTreeSet<PathBuf>,
    files: Vec<(String, String)>,
    bytes: usize,
}

impl WorkspaceInstructions {
    /// Run off the async executor. Repeating this capture also detects newly
    /// created sources/imports and is used before warm session reuse.
    pub fn capture(cwd: &Path) -> Result<Self> {
        let cwd = cwd.canonicalize().map_err(|_| ())?;
        let boundary = cwd
            .ancestors()
            .find(|dir| dir.join(".git").exists())
            .unwrap_or(&cwd)
            .to_owned();
        let mut loader = Loader {
            boundary,
            visited: BTreeSet::new(),
            files: Vec::new(),
            bytes: 0,
        };
        let ancestors: Vec<_> = cwd.ancestors().collect();
        if ancestors.len() > 1024 {
            return Err(());
        }
        // Claude's cumulative root-to-cwd order, including both file locations
        // and local instructions. Home skills/config are not enumerated here.
        for dir in ancestors.iter().rev() {
            for name in ["CLAUDE.md", ".claude/CLAUDE.md", "CLAUDE.local.md"] {
                loader.file(&dir.join(name), 0, false, false)?;
            }
            loader.rules(&dir.join(".claude/rules"), &mut BTreeSet::new())?;
        }
        // Keep Intent's ordinary workspace-rule selection in addition to the
        // native sources. Canonical dedup prevents CLAUDE.md or imported AGENTS
        // from appearing twice.
        if let Some((content, source)) = super::load_workspace_rules(&cwd, None) {
            let path = Path::new(&source);
            if path.is_file() {
                loader.file(path, 0, false, false)?;
            } else if !content.trim().is_empty() {
                loader.files.push((content, source));
            }
        }
        Ok(Self {
            files: loader.files,
        })
    }
}

impl Loader {
    fn file(&mut self, path: &Path, depth: usize, imported: bool, rule: bool) -> Result<()> {
        let path = match path.canonicalize() {
            Ok(path) => path,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(()),
        };
        // External imports need native project consent, which is deliberately
        // not inferred from the isolated profile. Never auto-approve them.
        if imported && !path.starts_with(&self.boundary) {
            return Err(());
        }
        if !self.visited.insert(path.clone()) {
            return Ok(());
        }
        if self.visited.len() > 1024 {
            return Err(());
        }
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(&path).map_err(|_| ())?;
        if !file.metadata().map_err(|_| ())?.is_file() {
            return Err(());
        }
        let mut content = String::new();
        file.take(1_048_577)
            .read_to_string(&mut content)
            .map_err(|_| ())?;
        self.bytes += content.len();
        if content.len() > 1_048_576 || self.bytes > 4_194_304 {
            return Err(());
        }
        self.files
            .push((content.clone(), path.to_string_lossy().into_owned()));
        if rule && content.trim_start().starts_with("---") {
            return Err(());
        }
        // Pinned native Claude includes four imported hops (root is zero).
        if depth < 4 {
            for import in imports(&content) {
                if import.starts_with('~') {
                    // Expanding HOME here would bypass the sealed environment
                    // and native external-import consent.
                    return Err(());
                }
                self.file(
                    &path.parent().ok_or(())?.join(import),
                    depth + 1,
                    true,
                    false,
                )?;
            }
        }
        Ok(())
    }

    fn rules(&mut self, path: &Path, directories: &mut BTreeSet<PathBuf>) -> Result<()> {
        let real = match path.canonicalize() {
            Ok(path) => path,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(()),
        };
        if !directories.insert(real.clone()) {
            return Ok(());
        }
        if directories.len() > 1024 {
            return Err(());
        }
        let mut files = std::fs::read_dir(real)
            .map_err(|_| ())?
            .map(|entry| entry.map(|e| e.path()).map_err(|_| ()))
            .collect::<Result<Vec<_>>>()?;
        files.sort();
        for path in files {
            if path.is_dir() {
                self.rules(&path, directories)?;
            } else if path.extension().is_some_and(|ext| ext == "md") {
                // Path-scoped/dynamic rules cannot be flattened into a global
                // prompt. Keep these workspaces on their native route for now.
                self.file(&path, 0, false, true)?;
            }
        }
        Ok(())
    }
}

/// Claude imports exclude code spans, fences and indented code. Paths end at
/// whitespace; escaped spaces work, fragments are not filenames, and quoted
/// paths stay text.
fn imports(content: &str) -> Vec<String> {
    static IMPORT: std::sync::LazyLock<Regex> =
        std::sync::LazyLock::new(|| Regex::new(r"(?:^|\s)@((?:\\ |[^\s\x22'`])+)").unwrap());
    // Preserve original spelling for escaped spaces/fragments, but exclude
    // complete Markdown code/HTML ranges. Block context distinguishes indented
    // code from lazy paragraph continuations and nested list paragraphs.
    let mut text = content.as_bytes().to_vec();
    for (event, range) in Parser::new(content).into_offset_iter() {
        if matches!(
            event,
            Event::Start(Tag::CodeBlock(_))
                | Event::Code(_)
                | Event::Html(_)
                | Event::InlineHtml(_)
        ) {
            text[range].fill(b' ');
        }
    }
    let text = String::from_utf8(text).expect("Markdown ranges preserve UTF-8 boundaries");
    IMPORT
        .captures_iter(&text)
        .map(|c| {
            c[1].split('#')
                .next()
                .unwrap_or_default()
                .replace("\\ ", " ")
        })
        .filter(|path| !path.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn write(root: &Path, path: &str, text: &str) {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    #[test]
    fn preserves_ancestor_alternate_local_imports_and_intent_rules() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join(".git")).unwrap();
        write(root.path(), "CLAUDE.md", "ANCESTOR\n@docs/root.md");
        write(root.path(), "docs/root.md", "IMPORTED\n@nested.md");
        write(root.path(), "docs/nested.md", "NESTED\n@root.md");
        write(
            root.path(),
            "app/.claude/CLAUDE.md",
            "ALTERNATE\n@../AGENTS.md",
        );
        write(root.path(), "app/AGENTS.md", "AGENTS-OWNED");
        write(root.path(), "app/CLAUDE.local.md", "LOCAL");
        write(root.path(), "app/.claude/rules/tests/a.md", "UNSCOPED");
        let snapshot = WorkspaceInstructions::capture(&root.path().join("app")).unwrap();
        let all = snapshot
            .files
            .iter()
            .map(|(text, _)| text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        for marker in [
            "ANCESTOR",
            "IMPORTED",
            "NESTED",
            "ALTERNATE",
            "LOCAL",
            "UNSCOPED",
            "AGENTS-OWNED",
        ] {
            assert_eq!(all.matches(marker).count(), 1, "{marker}");
        }
        assert!(all.find("ANCESTOR") < all.find("ALTERNATE"));
        write(root.path(), "docs/nested.md", "CHANGED");
        assert_ne!(
            snapshot,
            WorkspaceInstructions::capture(&root.path().join("app")).unwrap()
        );
    }

    #[test]
    fn imports_skip_code_and_comments_and_support_escaped_spaces() {
        assert_eq!(imports("@one.md\n- see @dir/a\\ b.md\n`@inline`\n```md\n@fenced\n```\n<!-- @comment -->\n@\"quoted\"\nemail@example.org"), vec!["one.md", "dir/a b.md"]);
    }

    #[test]
    fn fragments_and_indented_code_match_native_imports() {
        assert_eq!(
            imports("@docs/policy.md#section\n\n    @indented.md\n\t@tabbed.md\n\n@other.md"),
            vec!["docs/policy.md", "other.md"]
        );
    }

    #[test]
    fn paragraph_and_list_continuations_are_not_indented_code() {
        assert_eq!(imports("Paragraph\n    @paragraph.md\n\n- List item\n\n    @list.md\n\n        @list-code.md\n\nOutside paragraph\n\n    @code.md"), vec!["paragraph.md", "list.md"]);
    }

    #[test]
    fn native_import_depth_stops_after_four_hops() {
        let root = tempfile::tempdir().unwrap();
        write(root.path(), "CLAUDE.md", "@depth-1.md");
        for depth in 1..=5 {
            write(
                root.path(),
                &format!("depth-{depth}.md"),
                &format!("SENTINEL-{depth}\n@depth-{}.md", depth + 1),
            );
        }
        let snapshot = WorkspaceInstructions::capture(root.path()).unwrap();
        let all = snapshot
            .files
            .iter()
            .map(|(text, _)| text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(all.contains("SENTINEL-4"));
        assert!(!all.contains("SENTINEL-5"));
    }

    #[test]
    fn unresolved_scope_defers_instead_of_dropping_or_broadening_rules() {
        let root = tempfile::tempdir().unwrap();
        let cwd = root.path().join("repo");
        write(
            &cwd,
            ".claude/rules/scoped.md",
            "---\npaths: [src/**]\n---\nSCOPED",
        );
        assert!(WorkspaceInstructions::capture(&cwd).is_err());
        std::fs::remove_file(cwd.join(".claude/rules/scoped.md")).unwrap();
        write(root.path(), "external.md", "EXTERNAL");
        write(&cwd, ".claude/CLAUDE.md", "@../../external.md");
        assert!(WorkspaceInstructions::capture(&cwd).is_err());
    }
}
