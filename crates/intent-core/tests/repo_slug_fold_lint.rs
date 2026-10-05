//! Repo-slug fold lint.
//!
//! Forge owner/name slugs are case-insensitive, and every site that folded or
//! compared them by hand outside `intent_core::RepoRef` was a site where the
//! fold could be missed (intent-hq/intentd#1809 → #1815). This source-scanning
//! test fails, naming `file:line`, whenever slug identity is case-folded or
//! compared anywhere under `crates/*/src/**/*.rs` other than
//! `crates/intent-core/src/repo_ref.rs`.
//!
//! The pre-processing — blanking comments, string literals, and `#[cfg(test)]`
//! items, cutting the text into statements (a block expression used as an
//! operand — its `}` followed by `.`, `?`, or `else` — chains with the text
//! around it: `let repo = if c { a } else { b }.to_lowercase()` is one
//! statement while the block bodies stay their own), locating the opt-out
//! marker, and walking `crates/*/src/**/*.rs` minus `tests/` directories and
//! `tests.rs` files — is the shared `intentd_test_support::source_lint`
//! scaffolding; its module doc spells out those semantics. The rule itself is
//! deliberately small:
//!
//! - A statement is flagged when it contains a fold call (`to_lowercase`,
//!   `to_ascii_lowercase`, `eq_ignore_ascii_case`, `make_ascii_lowercase`)
//!   AND a slug identifier: an identifier token with an underscore-delimited
//!   component equal to `owner`, `repo`, `repository`, or `slug`, or the bare
//!   token `name` when the same statement also carries an `owner` component.
//! - Skipped: `crates/intent-core/src/repo_ref.rs`, on top of the test code
//!   the shared walker and `#[cfg(test)]` blanking leave out.
//! - Opt-out: `// repo-slug-fold: allow — <reason>` on the line immediately
//!   above the statement's first line, in the shared marker grammar (a
//!   standalone `//` line comment, the exact token, whitespace, an em dash or
//!   hyphen, and a nonempty reason). A malformed marker never suppresses the
//!   hit; the report says so.

use std::fs;
use std::path::{Component, Path, PathBuf};

use intentd_test_support::source_lint::{
    blank_cfg_test_items, crate_src_files, lex, markers_by_line, split_statements, workspace_root,
    Marker,
};

const FOLD_CALLS: &[&str] = &[
    "to_lowercase",
    "to_ascii_lowercase",
    "eq_ignore_ascii_case",
    "make_ascii_lowercase",
];
const SLUG_COMPONENTS: &[&str] = &["owner", "repo", "repository", "slug"];
const OPT_OUT_TAG: &str = "repo-slug-fold";
const EXEMPT_FILE: &[&str] = &["crates", "intent-core", "src", "repo_ref.rs"];
const EXCERPT_CHARS: usize = 120;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    excerpt: String,
    /// The line above carried something that starts like the opt-out marker
    /// but is malformed (longer token, or no reason).
    marker_malformed: bool,
}

/// ASCII identifier tokens (`[A-Za-z_][A-Za-z0-9_]*`) in `text`.
fn identifiers(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    for (idx, c) in text.char_indices() {
        let word_char = c.is_ascii_alphanumeric() || c == '_';
        match (start, word_char) {
            (None, true) => start = Some(idx),
            (Some(s), false) => {
                out.push(&text[s..idx]);
                start = None;
            }
            _ => {}
        }
    }
    if let Some(s) = start {
        out.push(&text[s..]);
    }
    out.retain(|id| id.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_'));
    out
}

fn has_component(ident: &str, component: &str) -> bool {
    ident.split('_').any(|part| part == component)
}

/// Whether one statement folds a slug identifier.
fn is_flagged(statement: &str) -> bool {
    let idents = identifiers(statement);
    if !idents.iter().any(|id| FOLD_CALLS.contains(id)) {
        return false;
    }
    let has_owner = idents.iter().any(|id| has_component(id, "owner"));
    idents.iter().any(|id| {
        SLUG_COMPONENTS.iter().any(|c| has_component(id, c)) || (*id == "name" && has_owner)
    })
}

fn excerpt(text: &str) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out: String = collapsed.chars().take(EXCERPT_CHARS).collect();
    if out.len() < collapsed.len() {
        out.push('…');
    }
    out
}

/// Scans one Rust source file's text and returns every flagged statement
/// that is not suppressed by a reasoned opt-out marker.
fn scan_source(src: &str) -> Vec<Hit> {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, OPT_OUT_TAG);
    let blanked = blank_cfg_test_items(&lexed.blanked);
    split_statements(&blanked)
        .into_iter()
        .filter(|s| is_flagged(&s.text))
        .filter_map(|s| {
            let marker = markers.get(s.line - 1).copied().unwrap_or(Marker::Absent);
            match marker {
                Marker::WithReason => None,
                Marker::Malformed | Marker::Absent => Some(Hit {
                    line: s.line,
                    excerpt: excerpt(&s.text),
                    marker_malformed: marker == Marker::Malformed,
                }),
            }
        })
        .collect()
}

fn is_exempt(rel: &Path) -> bool {
    let parts: Vec<_> = rel.components().map(Component::as_os_str).collect();
    parts.len() == EXEMPT_FILE.len()
        && parts
            .iter()
            .zip(EXEMPT_FILE)
            .all(|(have, want)| *have == *want)
}

fn display_rel(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[test]
fn slug_identity_is_only_folded_inside_repo_ref() {
    let root = workspace_root();
    let exempt: PathBuf = EXEMPT_FILE.iter().collect();
    assert!(
        root.join(&exempt).is_file(),
        "{} moved; update EXEMPT_FILE so the exemption keeps pointing at RepoRef",
        display_rel(&exempt)
    );

    let files = crate_src_files(&root);
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
        if is_exempt(rel) {
            continue;
        }
        let src =
            fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        for hit in scan_source(&src) {
            let note = if hit.marker_malformed {
                "  (opt-out marker is malformed: expected `// repo-slug-fold: allow — <reason>`)"
            } else {
                ""
            };
            report.push(format!(
                "{}:{}: {}{note}",
                display_rel(rel),
                hit.line,
                hit.excerpt
            ));
        }
    }

    assert!(
        report.is_empty(),
        "owner/name slug identity is case-folded or compared outside intent_core::RepoRef:\n\n{}\n\n\
         Compare slugs as `RepoRef` values (its PartialEq/Hash fold ASCII case) or take the \
         folded form from `RepoRef::identity_parts()` / `identity_key()`. A site that is not \
         slug identity (e.g. a folder-name slugifier) may opt out with \
         `// repo-slug-fold: allow — <reason>` on the line immediately above the statement; \
         the reason is required.",
        report.join("\n")
    );
}

// ---- scanner fixtures -------------------------------------------------------

/// 1-based line of the first line containing `needle`.
fn line_of(src: &str, needle: &str) -> usize {
    src.lines()
        .position(|l| l.contains(needle))
        .map_or_else(|| panic!("fixture lacks {needle:?}"), |i| i + 1)
}

fn hit_lines(src: &str) -> Vec<usize> {
    scan_source(src).into_iter().map(|h| h.line).collect()
}

/// `lookup_known_repo_local_path` as it stood before intentd commit 31dc6cc7
/// (`crates/intent-acp/src/mcp_server/bindings/app/workspaces.rs`).
const LOOKUP_KNOWN_REPO_LOCAL_PATH_PRE_31DC6CC7: &str = r#"
async fn lookup_known_repo_local_path(
    api: &Arc<dyn WorkspaceApi>,
    github_url: &str,
) -> Option<String> {
    let (owner, repo) = parse_github_owner_repo(github_url)?;
    let workspaces = api.list_workspaces(true).await.ok()?;

    let mut strict = Vec::new();
    let mut name_only = Vec::new();
    let mut basename_only = Vec::new();
    for ws in &workspaces {
        // Skip deleted workspaces: their repositoryPath may no longer exist
        // on disk (the FE lookup consults user-visible checkouts only).
        if ws.status == WorkspaceStatus::Deleted {
            continue;
        }
        let Some(path) = ws.repository_path.as_deref().filter(|p| !p.is_empty()) else {
            continue;
        };
        if path.contains("/.clones/") || path.contains("\\.clones\\") {
            continue;
        }
        let entry_owner = ws
            .repository_owner
            .as_deref()
            .filter(|o| !o.is_empty())
            .map(str::to_lowercase);
        let entry_name = ws
            .repository_name
            .as_deref()
            .filter(|n| !n.is_empty())
            .map(|n| strip_git_suffix(&n.to_lowercase()).to_string());
        let entry_basename = path
            .rsplit(['/', '\\'])
            .next()
            .map(|b| strip_git_suffix(&b.to_lowercase()).to_string());

        if entry_name.as_deref() == Some(repo.as_str()) {
            if entry_owner.as_deref() == Some(owner.as_str()) {
                strict.push(path.to_string());
            } else if entry_owner.is_none() {
                name_only.push(path.to_string());
            }
        } else if entry_basename.as_deref() == Some(repo.as_str()) && entry_owner.is_none() {
            basename_only.push(path.to_string());
        }
    }
    None
}
"#;

/// `origin_is_github_slot` as it stood on `main` before intentd#1815
/// (`crates/intent-git/src/repo_cache.rs`).
const ORIGIN_IS_GITHUB_SLOT_PRE_1815: &str = r#"
fn origin_is_github_slot(url: &str, owner: &str, repo: &str) -> bool {
    let trimmed = url.trim().trim_end_matches('/');
    let (rest, scp_like) = match trimmed.split_once("://") {
        Some((scheme, rest)) => {
            let known = ["https", "http", "ssh", "git"]
                .iter()
                .any(|s| scheme.eq_ignore_ascii_case(s));
            if !known {
                return false;
            }
            (rest, false)
        }
        // No scheme: only the scp-like `user@host:owner/repo` form qualifies.
        None => (trimmed, true),
    };
    let rest = rest.rsplit_once('@').map_or(rest, |(_, r)| r);
    let (authority, path) = if scp_like {
        match rest.split_once(':') {
            Some(pair) => pair,
            None => return false,
        }
    } else {
        match rest.split_once('/') {
            Some(pair) => pair,
            None => return false,
        }
    };
    let host = authority.split(':').next().unwrap_or(authority);
    if !host.eq_ignore_ascii_case("github.com") {
        return false;
    }
    let mut segments = path.split('/').filter(|s| !s.is_empty());
    let (Some(o), Some(r)) = (segments.next(), segments.next()) else {
        return false;
    };
    if segments.next().is_some() {
        return false;
    }
    let r = r.strip_suffix(".git").unwrap_or(r);
    o.eq_ignore_ascii_case(owner) && r.eq_ignore_ascii_case(repo)
}
"#;

/// `parse_github_owner_repo` as it stood on `main` before intentd#1815
/// (`crates/intent-acp/src/mcp_server/bindings/app/workspaces.rs`).
const PARSE_GITHUB_OWNER_REPO_PRE_1815: &str = r#"
/// Parse a GitHub URL (https, www, or git@ form) into lowercase
/// `(owner, repo)` (TS `parseGithubOwnerRepo`).
fn parse_github_owner_repo(github_url: &str) -> Option<(String, String)> {
    let t = github_url.trim();
    let stripped = strip_prefix_ci(t, "https://www.github.com/")
        .or_else(|| strip_prefix_ci(t, "http://www.github.com/"))
        .or_else(|| strip_prefix_ci(t, "https://github.com/"))
        .or_else(|| strip_prefix_ci(t, "http://github.com/"))
        .or_else(|| strip_prefix_ci(t, "git@github.com:"))
        .unwrap_or(t);
    let stripped = strip_git_suffix(stripped);
    let mut segs = stripped.split('/').filter(|s| !s.is_empty());
    let owner = segs.next()?;
    let repo = segs.next()?;
    Some((owner.to_lowercase(), repo.to_lowercase()))
}
"#;

#[test]
fn flags_pre_31dc6cc7_lookup_known_repo_local_path() {
    let src = LOOKUP_KNOWN_REPO_LOCAL_PATH_PRE_31DC6CC7;
    assert_eq!(
        hit_lines(src),
        vec![
            line_of(src, "let entry_owner = ws"),
            line_of(src, "let entry_name = ws"),
        ]
    );
    let hits = scan_source(src);
    assert!(hits[0].excerpt.contains("repository_owner"), "{hits:?}");
    assert!(hits.iter().all(|h| !h.marker_malformed), "{hits:?}");
}

#[test]
fn flags_pre_1815_origin_is_github_slot_but_not_its_scheme_and_host_checks() {
    let src = ORIGIN_IS_GITHUB_SLOT_PRE_1815;
    assert_eq!(
        hit_lines(src),
        vec![line_of(src, "o.eq_ignore_ascii_case(owner)")]
    );
}

#[test]
fn flags_pre_1815_parse_github_owner_repo() {
    let src = PARSE_GITHUB_OWNER_REPO_PRE_1815;
    assert_eq!(
        hit_lines(src),
        vec![line_of(
            src,
            "Some((owner.to_lowercase(), repo.to_lowercase()))"
        )]
    );
}

#[test]
fn ignores_folds_that_are_not_slug_identity() {
    let src = r#"
fn not_identity(host: &str, scheme: &str, name: &str, specialist_name: &str) -> bool {
    if !host.eq_ignore_ascii_case("github.com") {
        return false;
    }
    let known = ["https", "http"].iter().any(|s| scheme.eq_ignore_ascii_case(s));
    let auth = name.eq_ignore_ascii_case("authorization");
    let implementor = specialist_name.eq_ignore_ascii_case("implementor");
    known && auth && implementor
}

fn ordered(left: &str, right: &str) -> std::cmp::Ordering {
    left.to_lowercase().cmp(&right.to_lowercase())
}

fn header_name(name: &str) -> String {
    name.to_ascii_lowercase()
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn bare_name_counts_only_next_to_an_owner() {
    let src = r"
fn key(owner: &str, name: &str) -> (String, String) {
    (owner.to_lowercase(), name.to_lowercase())
}

fn lone(name: &str) -> String {
    name.to_lowercase()
}
";
    assert_eq!(
        hit_lines(src),
        vec![line_of(src, "(owner.to_lowercase(), name")]
    );
}

#[test]
fn slug_identity_hides_in_strings_and_comments_is_ignored() {
    let src = r#"
fn describe(url: &str) -> String {
    // owner and repo are folded elsewhere: see repo.to_lowercase()
    /* owner.to_lowercase() */
    let label = "owner.to_lowercase()";
    format!("{label}: {}", url.to_lowercase())
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn c_string_literals_are_stripped_like_other_strings() {
    // Raw C string: a fold inside its body is not a hit.
    let raw_false_positive =
        "fn f(owner: &str) {\n    let _ = cr#\"\" owner.to_lowercase() \"\"#;\n}\n";
    assert_eq!(hit_lines(raw_false_positive), Vec::<usize>::new());

    // Raw C string ending in a quote: quote scanning must stay in sync so the
    // live fold after it is still seen.
    let raw_hidden_fold =
        "fn f(owner: &str) {\n    let _ = cr#\"ends with a quote \"\"#;\n    owner.to_lowercase();\n}\n";
    assert_eq!(hit_lines(raw_hidden_fold), vec![3]);

    // Unhashed raw C string, and cooked C / byte strings.
    let cooked = "fn f(owner: &str) {\n    let _ = cr\"owner.to_lowercase()\";\n    let _ = c\"owner.to_lowercase() \\\" \";\n    let _ = b\"owner.to_lowercase()\";\n    owner.to_lowercase();\n}\n";
    assert_eq!(hit_lines(cooked), vec![5]);
}

#[test]
fn opt_out_marker_with_a_reason_suppresses_the_statement() {
    let src = r"
pub(crate) fn worktree_folder_slug(repo_name: &str) -> String {
    let mut slug = String::new();
    // repo-slug-fold: allow — folder-name slugifier, not repo identity
    for c in repo_name.chars().flat_map(char::to_lowercase) {
        slug.push(c);
    }
    slug
}
";
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn opt_out_marker_without_a_reason_still_fails() {
    for marker in [
        "// repo-slug-fold: allow",
        "// repo-slug-fold: allow —",
        "// repo-slug-fold: allow -",
        "// repo-slug-fold: allow —   ",
        "// repo-slug-fold: allow reason without a dash",
        "// repo-slug-fold: allow—no space before the dash",
    ] {
        let src = format!(
            "fn slugify(repo_name: &str) -> String {{\n    {marker}\n    repo_name.to_lowercase()\n}}\n"
        );
        let hits = scan_source(&src);
        assert_eq!(hits.len(), 1, "{marker:?}: {hits:?}");
        assert_eq!(hits[0].line, 3, "{marker:?}");
        assert!(hits[0].marker_malformed, "{marker:?}");
    }
}

#[test]
fn opt_out_marker_must_sit_immediately_above_the_statement() {
    let src = "fn slugify(repo_name: &str) -> String {\n    // repo-slug-fold: allow — reason\n\n    repo_name.to_lowercase()\n}\n";
    let hits = scan_source(src);
    assert_eq!(hit_lines(src), vec![4]);
    assert!(!hits[0].marker_malformed);
}

#[test]
fn cfg_test_items_are_skipped_but_code_after_them_is_not() {
    let src = r"
#[cfg(test)]
mod tests {
    fn folds(owner: &str, repo: &str) -> bool {
        owner.to_lowercase() == repo.to_lowercase()
    }
}

#[cfg(test)]
mod more_tests;

#[cfg(test)]
fn helper(owner: &str) -> String {
    owner.to_ascii_lowercase()
}

fn real(owner: &str) -> String {
    owner.to_lowercase()
}
";
    assert_eq!(hit_lines(src), vec![line_of(src, "fn real(owner") + 1]);
}

#[test]
fn operand_blocks_chain_with_the_surrounding_statement() {
    let src = r"
fn f(c: bool, a: &str, b: &str, owner_ref: Kind, x: &str) {
    let repo = if c { a } else { b }.to_lowercase();
    let n = match owner_ref {
        Kind::A => x,
        Kind::B => a,
    }.to_lowercase();
    let s = unsafe { raw(x) }.eq_ignore_ascii_case(slug);
    let t = { Some(x) }?.to_lowercase();
}
";
    assert_eq!(
        hit_lines(src),
        vec![
            line_of(src, "let repo = if c"),
            line_of(src, "let n = match owner_ref"),
            line_of(src, "let s = unsafe"),
        ]
    );

    // A fold inside the block body is still its own statement.
    let src = r"
fn f(c: bool, owner: &str, b: &str) -> String {
    let h = if c { owner.to_lowercase() } else { b.to_string() }.trim().to_string();
    h
}
";
    assert_eq!(hit_lines(src), vec![line_of(src, "let h = if c")]);

    // No slug identifier anywhere: the chained statement is not flagged.
    let src = r"
fn f(c: bool, x: &str, y: &str) -> String {
    let h = if c { x } else { y }.to_lowercase();
    h
}
";
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn only_repo_ref_is_exempt() {
    let exempt: PathBuf = EXEMPT_FILE.iter().collect();
    assert!(is_exempt(&exempt));
    let sibling: PathBuf = ["crates", "intent-core", "src", "slug.rs"].iter().collect();
    assert!(!is_exempt(&sibling));
    let elsewhere: PathBuf = ["crates", "intent-git", "src", "repo_ref.rs"]
        .iter()
        .collect();
    assert!(!is_exempt(&elsewhere));
    assert_eq!(display_rel(&exempt), "crates/intent-core/src/repo_ref.rs");
}
