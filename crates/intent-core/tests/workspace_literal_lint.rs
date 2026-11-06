//! Full `Workspace` literal lint.
//!
//! Adding one optional field to `intent_core::Workspace` used to mean
//! hand-editing every full struct literal in the tree: intentd#2688
//! (`backgroundTasksRunning`) inserted `background_tasks_running: false` into
//! 148 literals across 129 files, and intentd#2637 (`scriptsRunning`) and
//! intentd#2605 (`retiredAt`) repeated the same pass. `Workspace` now derives
//! `Default`, and every builder sets only the fields it cares about with
//! struct-update syntax (`Workspace { id, title, ..Default::default() }`), so
//! a new field compiles without touching any of them.
//!
//! This source-scanning test keeps it that way. It fails, naming `file:line`,
//! on any `Workspace { … }` struct expression (or pattern) whose top level has
//! no `..` rest element, in every `*.rs` file under `crates/` — production
//! code, `#[cfg(test)]` modules, `tests/` directories, and fixtures alike,
//! because the fan-out came from tests. Rules:
//!
//! - The scan runs on comment- and literal-blanked text (the shared
//!   `intentd_test_support::source_lint::lex`), so a quoted or commented
//!   example never counts.
//! - `Workspace` must be a whole identifier (optionally path-qualified, e.g.
//!   `intent_core::Workspace`) followed by `{`. Item headers are not literals:
//!   the token before it being `->` (a return type), `struct`, `enum`,
//!   `union`, `trait`, `impl`, or `for` (`impl … for Workspace`) skips it.
//! - Only a body that names every field of `Workspace` counts: the field list
//!   is read from the struct definition in `model.rs`, and a body's names are
//!   the leading identifiers of its depth-0, comma-separated elements
//!   (`name: value` or shorthand `name`). A literal without a rest element
//!   compiles only when it lists every field, so this matches exactly the
//!   full literals and full destructuring patterns, while enum variants that
//!   share the name (`Context::Workspace { workspace_id }`) never match.
//! - Such a body passes when one of its elements starts with `..` (the struct
//!   base). A range such as `0..5` inside a field value is not a rest element.
//! - Every file is scanned, `model.rs` included (it is also where the field
//!   list comes from). Unbalanced brackets after a `Workspace {` are reported,
//!   never silently skipped.
//! - Opt-out: `// workspace-literal: allow — <reason>` on the line
//!   immediately above the literal's `Workspace` token, in the shared marker
//!   grammar. A malformed marker never suppresses the hit, and a reasoned
//!   marker that covers no full literal is stale; the report says so.
//!
//! Known blind spots (none occur today): `Self { … }` inside `impl Workspace`,
//! a renamed import or type alias (`use … Workspace as W`), and a full pattern
//! right after `for` (`for Workspace { … } in rows`, read as an
//! `impl … for` header). Each still compiles only with every field listed, so
//! the next added field surfaces it.

use std::fs;
use std::path::{Path, PathBuf};

use intentd_test_support::source_lint::{
    lex, markers_by_line, rust_files, skip_whitespace, tokenize, workspace_root, Marker, Token,
};

const TYPE_NAME: &str = "Workspace";
const OPT_OUT_TAG: &str = "workspace-literal";
const MODEL_FILE: &[&str] = &["crates", "intent-core", "src", "model.rs"];
const ITEM_KEYWORDS: &[&str] = &["struct", "enum", "union", "trait", "impl", "for"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HitKind {
    /// A full literal with no (or only a malformed) opt-out marker.
    Literal { marker_malformed: bool },
    /// A reasoned marker whose next line starts no full literal.
    StaleMarker,
    /// No bracket closes the `Workspace {` on this line.
    Unbalanced,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    kind: HitKind,
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Whether the text before `start` (exclusive) ends in an item header that
/// makes `Workspace {` a type position rather than a struct expression.
fn is_item_header(chars: &[char], start: usize) -> bool {
    let mut j = start;
    // Walk back over a path qualifier (`intent_core::`, `crate::model::`).
    loop {
        let mut k = j;
        while k > 0 && chars[k - 1].is_whitespace() {
            k -= 1;
        }
        if k >= 2 && chars[k - 1] == ':' && chars[k - 2] == ':' {
            k -= 2;
            while k > 0 && chars[k - 1].is_whitespace() {
                k -= 1;
            }
            while k > 0 && is_ident_char(chars[k - 1]) {
                k -= 1;
            }
            j = k;
        } else {
            j = k;
            break;
        }
    }
    if j >= 2 && chars[j - 1] == '>' && chars[j - 2] == '-' {
        return true;
    }
    let end = j;
    while j > 0 && is_ident_char(chars[j - 1]) {
        j -= 1;
    }
    let word: String = chars[j..end].iter().collect();
    ITEM_KEYWORDS.contains(&word.as_str())
}

/// Char index of the bracket closing the one opened at char `open`, if any:
/// the first closer of any kind back at the opener's bracket depth (shared
/// reader's [`Token::depth`]), so a stray closer of another kind ends the
/// body early instead of being skipped.
fn close_of(tokens: &[Token], open: usize) -> Option<usize> {
    let at = tokens.partition_point(|t| t.start < open);
    if tokens.get(at)?.start != open {
        return None;
    }
    let depth = tokens[at].depth;
    tokens[at + 1..]
        .iter()
        .find(|t| matches!(t.text.as_str(), ")" | "]" | "}") && t.depth == depth)
        .map(|t| t.start)
}

/// The depth-0, comma-separated elements of a brace body, trimmed.
fn elements(body: &[char]) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (idx, &c) in body.iter().enumerate() {
        match c {
            '{' | '(' | '[' => depth += 1,
            '}' | ')' | ']' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                out.push(
                    body[start..idx]
                        .iter()
                        .collect::<String>()
                        .trim()
                        .to_string(),
                );
                start = idx + 1;
            }
            _ => {}
        }
    }
    out.push(body[start..].iter().collect::<String>().trim().to_string());
    out.retain(|e| !e.is_empty());
    out
}

/// The leading identifier of a struct element (`name: value` / `name`).
fn leading_ident(element: &str) -> &str {
    let end = element
        .char_indices()
        .find(|&(_, c)| !is_ident_char(c))
        .map_or(element.len(), |(idx, _)| idx);
    &element[..end]
}

/// Whether a brace body is a full `Workspace` literal: it names every field
/// and has no `..` rest element.
fn is_full_literal(body: &[char], fields: &[String]) -> bool {
    let elements = elements(body);
    if elements.iter().any(|e| e.starts_with("..")) {
        return false;
    }
    let names: Vec<&str> = elements.iter().map(|e| leading_ident(e)).collect();
    fields.iter().all(|f| names.contains(&f.as_str()))
}

/// The field names of `pub struct Workspace { … }` in `model.rs` source.
fn struct_fields(model_src: &str) -> Vec<String> {
    let chars: Vec<char> = lex(model_src).blanked.chars().collect();
    let header: Vec<char> = "pub struct Workspace {".chars().collect();
    let start = chars
        .windows(header.len())
        .position(|w| w == header.as_slice())
        .expect("`pub struct Workspace {` in model.rs");
    let open = start + header.len() - 1;
    let close = close_of(&tokenize(&chars), open).expect("balanced Workspace struct body");
    elements(&chars[open + 1..close])
        .iter()
        .filter_map(|e| {
            let tokens: Vec<&str> = e.split_whitespace().collect();
            let pub_at = tokens
                .iter()
                .rposition(|t| *t == "pub" || t.starts_with("pub("))?;
            let name = tokens.get(pub_at + 1)?.trim_end_matches(':');
            Some(name.to_string())
        })
        .collect()
}

fn line_of(chars: &[char], offset: usize) -> usize {
    1 + chars[..offset].iter().filter(|&&c| c == '\n').count()
}

/// Every full `Workspace` literal in one file's source not covered by a
/// reasoned opt-out, plus stale markers and unbalanced literals.
fn scan_source(src: &str, fields: &[String]) -> Vec<Hit> {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, OPT_OUT_TAG);
    let chars: Vec<char> = lexed.blanked.chars().collect();
    // Tokenized on the first `Workspace {`: most files have none.
    let mut tokens: Option<Vec<Token>> = None;
    let name: Vec<char> = TYPE_NAME.chars().collect();
    let mut hits = Vec::new();
    let mut used_markers = Vec::new();
    let mut i = 0;
    while i + name.len() <= chars.len() {
        let is_token = chars[i..i + name.len()] == name[..]
            && (i == 0 || !is_ident_char(chars[i - 1]))
            && chars.get(i + name.len()).is_none_or(|&c| !is_ident_char(c));
        if !is_token {
            i += 1;
            continue;
        }
        let open = skip_whitespace(&chars, i + name.len());
        if chars.get(open) != Some(&'{') || is_item_header(&chars, i) {
            i += name.len();
            continue;
        }
        let line = line_of(&chars, i);
        let tokens = tokens.get_or_insert_with(|| tokenize(&chars));
        let Some(close) = close_of(tokens, open) else {
            hits.push(Hit {
                line,
                kind: HitKind::Unbalanced,
            });
            break;
        };
        if is_full_literal(&chars[open + 1..close], fields) {
            // `markers` is addressed by 1-based line; the marker sits above.
            let marker_line = line - 1;
            let marker = markers.get(marker_line).copied().unwrap_or(Marker::Absent);
            if marker == Marker::WithReason {
                used_markers.push(marker_line);
            } else {
                hits.push(Hit {
                    line,
                    kind: HitKind::Literal {
                        marker_malformed: marker == Marker::Malformed,
                    },
                });
            }
        }
        i = open + 1;
    }
    for (idx, marker) in markers.iter().enumerate() {
        if *marker == Marker::WithReason && !used_markers.contains(&idx) {
            hits.push(Hit {
                line: idx,
                kind: HitKind::StaleMarker,
            });
        }
    }
    hits.sort_by_key(|h| h.line);
    hits
}

fn display_rel(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[test]
fn workspace_is_only_built_with_struct_update_syntax() {
    let root = workspace_root();
    let model: PathBuf = MODEL_FILE.iter().collect();
    assert!(
        root.join(&model).is_file(),
        "{} moved; update MODEL_FILE so the field list keeps coming from the Workspace type",
        display_rel(&model)
    );
    let model_src = fs::read_to_string(root.join(&model))
        .unwrap_or_else(|e| panic!("read {}: {e}", display_rel(&model)));
    let fields = struct_fields(&model_src);
    assert!(
        fields.len() > 20 && fields.iter().any(|f| f == "id"),
        "parsed only {fields:?} from `pub struct Workspace` in model.rs; the field parser is stale"
    );

    let files = rust_files(&root.join("crates"));
    assert!(
        !files.is_empty(),
        "no Rust sources found under {}",
        root.join("crates").display()
    );

    let mut report = Vec::new();
    for file in &files {
        let rel = file
            .strip_prefix(&root)
            .expect("source path under the workspace root");
        let src =
            fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        for hit in scan_source(&src, &fields) {
            let note = match hit.kind {
                HitKind::Literal {
                    marker_malformed: false,
                } => "",
                HitKind::Literal {
                    marker_malformed: true,
                } => {
                    "  (opt-out marker is malformed: expected `// workspace-literal: allow — <reason>`)"
                }
                HitKind::StaleMarker => {
                    "  (stale opt-out marker: the next line starts no full Workspace literal)"
                }
                HitKind::Unbalanced => "  (no bracket closes this `Workspace {`; lexer gap?)",
            };
            report.push(format!("{}:{}{note}", display_rel(rel), hit.line));
        }
    }

    assert!(
        report.is_empty(),
        "full `Workspace` struct literals (or a marker problem):\n\n{}\n\n\
         Set only the fields that matter and end the literal with `..Default::default()` \
         (or spread a fixture such as `intent_services::test_support::fixtures::workspace`), \
         so adding a field to `Workspace` needs no edits here. A site that must list every \
         field may opt out with `// workspace-literal: allow — <reason>` on the line \
         immediately above the `Workspace` token; the reason is required.",
        report.join("\n")
    );
}

#[cfg(test)]
mod fixtures {
    use super::{scan_source, struct_fields, Hit, HitKind};

    fn fields() -> Vec<String> {
        vec!["id".to_string(), "title".to_string()]
    }

    fn lines(src: &str) -> Vec<usize> {
        scan_source(src, &fields())
            .iter()
            .filter(|h| matches!(h.kind, HitKind::Literal { .. }))
            .map(|h| h.line)
            .collect()
    }

    #[test]
    fn parses_the_struct_field_names() {
        let model = "/// Doc.\n#[derive(Default)]\npub struct Workspace {\n    pub id: WorkspaceId,\n    \
                     /// A doc with a: colon.\n    #[serde(default, skip_serializing_if = \"Option::is_none\")]\n    \
                     pub base_ref: Option<String>,\n    pub tags: Vec<(String, u8)>,\n}\n\
                     pub struct WorkspaceMembership {\n    pub can_manage: bool,\n}\n";
        assert_eq!(struct_fields(model), vec!["id", "base_ref", "tags"]);
    }

    #[test]
    fn flags_a_literal_without_a_rest_element() {
        let src = "fn f() -> Workspace {\n    Workspace {\n        id,\n        title: t(a, b),\n    }\n}\n";
        assert_eq!(lines(src), vec![2]);
    }

    #[test]
    fn flags_path_qualified_and_nested_literals() {
        let src =
            "let a = vec![intent_core::Workspace { id, title }];\nlet b = Some(Workspace {\n    \
                   title: x(),\n    id: Some(Workspace { id, title }).unwrap().id,\n});\n";
        assert_eq!(lines(src), vec![1, 2, 4]);
    }

    #[test]
    fn flags_a_full_destructuring_pattern() {
        assert_eq!(lines("let Workspace { id, title: t } = w;\n"), vec![1]);
    }

    #[test]
    fn accepts_struct_update_syntax() {
        let src =
            "let w = Workspace {\n    id,\n    title: vec![a, b],\n    ..Default::default()\n};\n\
                   let v = Workspace { ..fixtures::workspace(&id) };\n";
        assert!(lines(src).is_empty());
    }

    #[test]
    fn a_range_in_a_field_value_is_not_a_rest_element() {
        let src = "let w = Workspace { title: v[0..2].to_vec(), id: (0..1).map(f).collect() };\n";
        assert_eq!(lines(src), vec![1]);
    }

    #[test]
    fn accepts_rest_patterns() {
        let src = "let Workspace { id, .. } = w;\nif let Workspace { id, title, .. } = &w {}\n";
        assert!(lines(src).is_empty());
    }

    #[test]
    fn ignores_enum_variants_named_workspace() {
        let src = "enum Context {\n    Workspace {\n        workspace_id: WorkspaceId,\n    },\n}\n\
                   let c = Context::Workspace { workspace_id };\nmatch c { Self::Workspace { workspace_id } => {} }\n";
        assert!(lines(src).is_empty());
    }

    #[test]
    fn skips_item_headers_and_type_positions() {
        let src = "pub struct Workspace {\n    id: u8,\n    title: u8,\n}\nimpl<T> From<T> for Workspace {\n}\n\
                   fn h(w: Workspace) {}\nstruct MyWorkspace { id: u8, title: u8 }\n";
        assert!(lines(src).is_empty());
    }

    #[test]
    fn ignores_comments_and_string_literals() {
        let src = "// Workspace { id, title }\nlet s = \"Workspace { id, title }\";\n/* Workspace { id, title } */\n";
        assert!(lines(src).is_empty());
    }

    #[test]
    fn honours_a_reasoned_marker_and_reports_a_malformed_one() {
        let src = "// workspace-literal: allow — exhaustive on purpose\nlet a = Workspace { id, title };\n\
                   // workspace-literal: allow\nlet b = Workspace { id, title };\n";
        assert_eq!(
            scan_source(src, &fields()),
            vec![Hit {
                line: 4,
                kind: HitKind::Literal {
                    marker_malformed: true
                }
            }]
        );
    }

    #[test]
    fn reports_a_marker_that_covers_no_full_literal() {
        let src = "// workspace-literal: allow — was exhaustive\nlet a = Workspace { id, ..w };\n";
        assert_eq!(
            scan_source(src, &fields()),
            vec![Hit {
                line: 1,
                kind: HitKind::StaleMarker
            }]
        );
    }

    #[test]
    fn reports_unbalanced_brackets_instead_of_stopping_silently() {
        let src = "let a = Workspace { id, title: f(\n";
        assert_eq!(
            scan_source(src, &fields()),
            vec![Hit {
                line: 1,
                kind: HitKind::Unbalanced
            }]
        );
    }

    #[test]
    fn parses_restricted_visibility_fields() {
        let model =
            "pub struct Workspace {\n    pub(crate) id: WorkspaceId,\n    pub title: String,\n}\n";
        assert_eq!(struct_fields(model), vec!["id", "title"]);
    }
}
