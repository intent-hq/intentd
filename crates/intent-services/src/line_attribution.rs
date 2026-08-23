//! Per-line attribution algorithm (§5.2.1).
//!
//! Rust port of the FE `attributeLines` in
//! `cloudlands-fe/src/features/notes/line-attribution.ts`. Given a note's
//! current markdown content and its full-snapshot version history (oldest to
//! newest), determines which stored version last modified each line, so the
//! `LineAttributionGutter` can render who touched what. Whitespace-only
//! changes are recorded on the attribution but not treated as "the change"
//! that redirects attribution (the FE parity behaviour tested below).
//!
//! Version equality uses trimmed-line comparison, mirroring the FE's
//! `diffTrimmedLines` from the `diff` npm package: internally we tokenize
//! each note into `\n`-split lines and diff a trimmed view via
//! [`similar::capture_diff_slices`] with the Myers algorithm.

use similar::{capture_diff_slices, Algorithm, DiffOp};

use intent_core::NoteVersion;

/// One line's attribution result. Mirrors the FE `LineAttribution` struct.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LineAttribution {
    /// 1-based line number in the current note content.
    pub line_number: usize,
    /// The line's current text (no trailing newline).
    pub line_content: String,
    /// Version that last modified this line (`None` when the line is older
    /// than the retained history).
    pub version: Option<NoteVersion>,
    /// `true` iff the line differs from the attributed version only in
    /// whitespace (FE `isWhitespaceOnly` flag).
    pub is_whitespace_only: bool,
}

/// JS `String.split('\n')` behaviour: an empty string yields `[""]` (length 1);
/// a trailing `\n` yields an extra empty last element. `str::split('\n')` on
/// Rust matches this — we alias it here for readability.
fn split_lines_js(s: &str) -> Vec<&str> {
    s.split('\n').collect()
}

/// FE `normalizeContent`: pad with a trailing `\n` unless empty. Ensures the
/// diff sees consistent line counts for `"Line 1"` vs `"Line 1\nLine 2"`.
fn normalize(content: &str) -> String {
    if content.is_empty() {
        String::new()
    } else if content.ends_with('\n') {
        content.to_string()
    } else {
        format!("{content}\n")
    }
}

/// Build a map: `current-line (1-based) -> Some(version-line 1-based) | None`.
///
/// Uses trimmed-line equality (FE `diffTrimmedLines`) so pure indentation
/// changes stay aligned. Only entries for the raw current line count are
/// populated; any phantom trailing empty line from normalization is left in
/// the map but never queried by the caller.
fn build_line_mapping(version_content: &str, current_content: &str) -> Vec<Option<usize>> {
    let v_norm = normalize(version_content);
    let c_norm = normalize(current_content);
    let v_lines = split_lines_js(&v_norm);
    let c_lines = split_lines_js(&c_norm);
    let v_trim: Vec<String> = v_lines.iter().map(|s| s.trim().to_string()).collect();
    let c_trim: Vec<String> = c_lines.iter().map(|s| s.trim().to_string()).collect();

    let ops = capture_diff_slices(Algorithm::Myers, &v_trim, &c_trim);

    let mut mapping: Vec<Option<usize>> = vec![None; c_lines.len()];
    let mut version_idx: usize = 0; // 0-based cursor into version lines
    let mut current_idx: usize = 0; // 0-based cursor into current lines
    for op in ops {
        match op {
            DiffOp::Equal { len, .. } => {
                for _ in 0..len {
                    if current_idx < mapping.len() {
                        mapping[current_idx] = Some(version_idx + 1);
                    }
                    version_idx += 1;
                    current_idx += 1;
                }
            }
            DiffOp::Delete { old_len, .. } => {
                version_idx += old_len;
            }
            DiffOp::Insert { new_len, .. } => {
                for _ in 0..new_len {
                    if current_idx < mapping.len() {
                        mapping[current_idx] = None;
                    }
                    current_idx += 1;
                }
            }
            DiffOp::Replace {
                old_len, new_len, ..
            } => {
                version_idx += old_len;
                for _ in 0..new_len {
                    if current_idx < mapping.len() {
                        mapping[current_idx] = None;
                    }
                    current_idx += 1;
                }
            }
        }
    }
    mapping
}

/// True iff the two lines differ only in leading/trailing whitespace (FE
/// `isWhitespaceOnlyChange`).
fn is_whitespace_only_change(version_line: &str, current_line: &str) -> bool {
    version_line.trim() == current_line.trim() && version_line != current_line
}

/// Attribute each line of `current_content` to the version that last modified
/// it (FE `attributeLines`). `versions` must be ordered from oldest to newest.
///
/// See [`LineAttribution`] for the returned per-line record. Empty content
/// returns an empty vec (FE parity); a note with no versions returns one
/// unattributed entry per current line.
pub(crate) fn attribute_lines(
    current_content: &str,
    versions: &[NoteVersion],
) -> Vec<LineAttribution> {
    let current_lines: Vec<&str> = split_lines_js(current_content);

    // Handle empty content up front (FE early return).
    if current_lines.is_empty() || (current_lines.len() == 1 && current_lines[0].is_empty()) {
        return Vec::new();
    }

    let mut attributions: Vec<LineAttribution> = current_lines
        .iter()
        .enumerate()
        .map(|(i, line)| LineAttribution {
            line_number: i + 1,
            line_content: (*line).to_string(),
            version: None,
            is_whitespace_only: false,
        })
        .collect();

    if versions.is_empty() {
        return attributions;
    }

    // Walk newest -> oldest (FE `for v in versions.len()-1 down to 0`).
    for v in (0..versions.len()).rev() {
        let version = &versions[v];
        let prev_version = if v > 0 { Some(&versions[v - 1]) } else { None };
        let current_to_version = build_line_mapping(&version.content, current_content);
        let version_to_prev =
            prev_version.map(|pv| build_line_mapping(&pv.content, &version.content));
        let version_lines: Vec<&str> = split_lines_js(&version.content);
        let prev_version_lines: Vec<&str> = prev_version
            .map(|pv| split_lines_js(&pv.content))
            .unwrap_or_default();

        for (i, current_line) in current_lines.iter().enumerate() {
            if attributions[i].version.is_some() {
                continue;
            }
            let mapped = current_to_version.get(i).copied().flatten();
            let Some(version_line_num) = mapped else {
                continue;
            };
            let version_line = version_lines
                .get(version_line_num.saturating_sub(1))
                .copied()
                .unwrap_or("");

            let mut was_changed_in_this_version = false;
            let mut is_whitespace_change = false;

            if prev_version.is_none() {
                was_changed_in_this_version = true;
            } else if let Some(v2p) = version_to_prev.as_ref() {
                let prev_line_num = v2p.get(version_line_num - 1).copied().flatten();
                match prev_line_num {
                    None => {
                        was_changed_in_this_version = true;
                    }
                    Some(pl) => {
                        let prev_line = prev_version_lines
                            .get(pl.saturating_sub(1))
                            .copied()
                            .unwrap_or("");
                        if version_line != prev_line {
                            if version_line.trim() == prev_line.trim() {
                                is_whitespace_change = true;
                            } else {
                                was_changed_in_this_version = true;
                            }
                        }
                    }
                }
            }

            if was_changed_in_this_version {
                attributions[i].version = Some(version.clone());
                // Preserve any whitespace-only flag already set by a later pass.
            } else if is_whitespace_change && attributions[i].version.is_none() {
                attributions[i].is_whitespace_only = true;
            } else if prev_version.is_none() && attributions[i].version.is_none() {
                attributions[i].version = Some(version.clone());
                attributions[i].is_whitespace_only =
                    is_whitespace_only_change(version_line, current_line);
            }
        }
    }

    attributions
}

#[cfg(test)]
mod tests {
    //! Rust ports of `cloudlands-fe/src/features/notes/__tests__/line-attribution.test.ts`.
    //! Names and expectations mirror the FE cases so parity regressions surface
    //! immediately.
    use super::*;
    use intent_core::{NoteVersion, NoteVersionAuthor};

    #[derive(Clone, Copy)]
    enum AuthorType {
        User,
        Agent,
        System,
    }

    impl AuthorType {
        fn as_str(self) -> &'static str {
            match self {
                AuthorType::User => "user",
                AuthorType::Agent => "agent",
                AuthorType::System => "system",
            }
        }
    }

    fn make_version(
        v: i64,
        content: &str,
        timestamp: &str,
        author_type: AuthorType,
        author_id: Option<&str>,
        author_name: Option<&str>,
    ) -> NoteVersion {
        let (default_id, default_name) = match author_type {
            AuthorType::User => ("user-1", "Test User"),
            AuthorType::Agent => ("agent-1", "Test Agent"),
            AuthorType::System => ("system", "System"),
        };
        NoteVersion {
            entry_type: "snapshot".to_string(),
            v,
            date: timestamp.to_string(),
            author: NoteVersionAuthor {
                id: author_id.unwrap_or(default_id).to_string(),
                name: author_name.unwrap_or(default_name).to_string(),
                author_type: author_type.as_str().to_string(),
            },
            title: "Test Note".to_string(),
            content: content.to_string(),
        }
    }

    fn v_user(v: i64, content: &str, ts: &str) -> NoteVersion {
        make_version(v, content, ts, AuthorType::User, None, None)
    }

    fn v_of(v: i64, content: &str, ts: &str, at: AuthorType) -> NoteVersion {
        make_version(v, content, ts, at, None, None)
    }

    fn expect_v(attr: &LineAttribution) -> i64 {
        attr.version
            .as_ref()
            .expect("attribution missing version")
            .v
    }

    #[test]
    fn basic_all_lines_attributed_to_only_version() {
        let versions = vec![v_user(1, "Line 1\nLine 2\nLine 3", "2024-01-01T10:00:00Z")];
        let attributions = attribute_lines("Line 1\nLine 2\nLine 3", &versions);
        assert_eq!(attributions.len(), 3);
        assert_eq!(expect_v(&attributions[0]), 1);
        assert_eq!(expect_v(&attributions[1]), 1);
        assert_eq!(expect_v(&attributions[2]), 1);
    }

    #[test]
    fn basic_new_lines_attributed_to_latest_version() {
        let versions = vec![
            v_user(1, "Line 1", "2024-01-01T10:00:00Z"),
            v_user(2, "Line 1\nLine 2\nLine 3", "2024-01-01T10:05:00Z"),
        ];
        let attributions = attribute_lines("Line 1\nLine 2\nLine 3", &versions);
        assert_eq!(expect_v(&attributions[0]), 1);
        assert_eq!(expect_v(&attributions[1]), 2);
        assert_eq!(expect_v(&attributions[2]), 2);
    }

    #[test]
    fn basic_modified_line_attributed_to_change_version() {
        let versions = vec![
            v_user(1, "Line 1\nLine 2\nLine 3", "2024-01-01T10:00:00Z"),
            v_user(2, "Line 1\nLine 2 modified\nLine 3", "2024-01-01T10:05:00Z"),
        ];
        let attributions = attribute_lines("Line 1\nLine 2 modified\nLine 3", &versions);
        assert_eq!(expect_v(&attributions[0]), 1);
        assert_eq!(expect_v(&attributions[1]), 2);
        assert_eq!(expect_v(&attributions[2]), 1);
    }

    #[test]
    fn duplicate_lines_position_aware() {
        let versions = vec![
            v_user(
                1,
                "logger.info(\"hello\");\nlogger.info(\"world\");",
                "2024-01-01T10:00:00Z",
            ),
            v_user(
                2,
                "logger.info(\"hello\");\nlogger.info(\"hello\");\nlogger.info(\"world\");",
                "2024-01-01T10:05:00Z",
            ),
        ];
        let attributions = attribute_lines(
            "logger.info(\"hello\");\nlogger.info(\"hello\");\nlogger.info(\"world\");",
            &versions,
        );
        assert_eq!(expect_v(&attributions[0]), 1);
        assert_eq!(expect_v(&attributions[1]), 2);
        assert_eq!(expect_v(&attributions[2]), 1);
    }

    #[test]
    fn duplicate_lines_modified_at_different_times() {
        let versions = vec![
            v_user(1, "Line A", "2024-01-01T10:00:00Z"),
            v_user(2, "Line A\nLine A", "2024-01-01T10:05:00Z"),
            v_user(3, "Line A\nLine A\nLine A", "2024-01-01T10:10:00Z"),
        ];
        let attributions = attribute_lines("Line A\nLine A\nLine A", &versions);
        assert_eq!(expect_v(&attributions[0]), 1);
        assert_eq!(expect_v(&attributions[1]), 2);
        assert_eq!(expect_v(&attributions[2]), 3);
    }

    #[test]
    fn whitespace_only_change_ignored() {
        let versions = vec![
            v_user(1, "Line 1\nLine 2\nLine 3", "2024-01-01T10:00:00Z"),
            v_user(2, "Line 1\n  Line 2\nLine 3", "2024-01-01T10:05:00Z"),
        ];
        let attributions = attribute_lines("Line 1\n  Line 2\nLine 3", &versions);
        // Line 2 stays attributed to v1 even though v2 changed only whitespace.
        assert_eq!(expect_v(&attributions[1]), 1);
        assert!(attributions[1].is_whitespace_only);
    }

    #[test]
    fn content_change_with_whitespace_diff_still_tracks_change() {
        let versions = vec![
            v_user(1, "Line 1\nLine 2\nLine 3", "2024-01-01T10:00:00Z"),
            v_user(
                2,
                "Line 1\n  Line 2 modified\nLine 3",
                "2024-01-01T10:05:00Z",
            ),
        ];
        let attributions = attribute_lines("Line 1\n  Line 2 modified\nLine 3", &versions);
        assert_eq!(expect_v(&attributions[1]), 2);
        assert!(!attributions[1].is_whitespace_only);
    }

    #[test]
    fn handles_deleted_lines() {
        let versions = vec![
            v_user(1, "Line 1\nLine 2\nLine 3", "2024-01-01T10:00:00Z"),
            v_user(2, "Line 1\nLine 3", "2024-01-01T10:05:00Z"),
        ];
        let attributions = attribute_lines("Line 1\nLine 3", &versions);
        assert_eq!(attributions.len(), 2);
        assert_eq!(expect_v(&attributions[0]), 1);
        assert_eq!(expect_v(&attributions[1]), 1);
    }

    #[test]
    fn insertion_in_middle() {
        let versions = vec![
            v_user(1, "Line 1\nLine 3\nLine 4", "2024-01-01T10:00:00Z"),
            v_user(2, "Line 1\nLine 2\nLine 3\nLine 4", "2024-01-01T10:05:00Z"),
        ];
        let attributions = attribute_lines("Line 1\nLine 2\nLine 3\nLine 4", &versions);
        assert_eq!(expect_v(&attributions[0]), 1);
        assert_eq!(expect_v(&attributions[1]), 2);
        assert_eq!(expect_v(&attributions[2]), 1);
        assert_eq!(expect_v(&attributions[3]), 1);
    }

    #[test]
    fn multiple_edits_attribute_to_latest() {
        let versions = vec![
            v_user(1, "Line 1\nLine 2\nLine 3", "2024-01-01T10:00:00Z"),
            v_user(2, "Line 1\nLine 2 v2\nLine 3", "2024-01-01T10:05:00Z"),
            v_user(3, "Line 1\nLine 2 v3\nLine 3", "2024-01-01T10:10:00Z"),
        ];
        let attributions = attribute_lines("Line 1\nLine 2 v3\nLine 3", &versions);
        assert_eq!(expect_v(&attributions[1]), 3);
    }

    #[test]
    fn empty_content_returns_empty() {
        let versions = vec![
            v_user(1, "Line 1", "2024-01-01T10:00:00Z"),
            v_user(2, "", "2024-01-01T10:05:00Z"),
        ];
        let attributions = attribute_lines("", &versions);
        assert!(attributions.is_empty());
    }

    #[test]
    fn no_versions_returns_unattributed() {
        let attributions = attribute_lines("Line 1\nLine 2", &[]);
        assert_eq!(attributions.len(), 2);
        assert!(attributions[0].version.is_none());
        assert!(attributions[1].version.is_none());
    }

    #[test]
    fn lines_older_than_history_attribute_to_oldest() {
        let versions = vec![v_user(1, "Line 1\nLine 2\nLine 3", "2024-01-01T10:00:00Z")];
        let attributions = attribute_lines("Line 1\nLine 2\nLine 3", &versions);
        assert_eq!(expect_v(&attributions[0]), 1);
        assert_eq!(expect_v(&attributions[1]), 1);
        assert_eq!(expect_v(&attributions[2]), 1);
    }

    #[test]
    fn author_types_user() {
        let versions = vec![
            v_of(1, "Line 1", "2024-01-01T10:00:00Z", AuthorType::User),
            v_of(
                2,
                "Line 1\nLine 2",
                "2024-01-01T10:05:00Z",
                AuthorType::User,
            ),
        ];
        let attributions = attribute_lines("Line 1\nLine 2", &versions);
        assert_eq!(
            attributions[0].version.as_ref().unwrap().author.author_type,
            "user"
        );
        assert_eq!(
            attributions[1].version.as_ref().unwrap().author.author_type,
            "user"
        );
    }

    #[test]
    fn author_types_agent() {
        let versions = vec![
            v_of(1, "Line 1", "2024-01-01T10:00:00Z", AuthorType::User),
            make_version(
                2,
                "Line 1\nLine 2",
                "2024-01-01T10:05:00Z",
                AuthorType::Agent,
                Some("agent-123"),
                Some("Code Assistant"),
            ),
        ];
        let attributions = attribute_lines("Line 1\nLine 2", &versions);
        assert_eq!(
            attributions[0].version.as_ref().unwrap().author.author_type,
            "user"
        );
        let a = attributions[1].version.as_ref().unwrap();
        assert_eq!(a.author.author_type, "agent");
        assert_eq!(a.author.id, "agent-123");
        assert_eq!(a.author.name, "Code Assistant");
    }

    #[test]
    fn author_types_system() {
        let versions = vec![
            v_of(1, "Line 1", "2024-01-01T10:00:00Z", AuthorType::System),
            v_of(
                2,
                "Line 1\nLine 2",
                "2024-01-01T10:05:00Z",
                AuthorType::User,
            ),
        ];
        let attributions = attribute_lines("Line 1\nLine 2", &versions);
        assert_eq!(
            attributions[0].version.as_ref().unwrap().author.author_type,
            "system"
        );
        assert_eq!(
            attributions[1].version.as_ref().unwrap().author.author_type,
            "user"
        );
    }

    #[test]
    fn author_types_mixed_preserved_on_unmodified_lines() {
        let versions = vec![
            make_version(
                1,
                "Line 1\nLine 2",
                "2024-01-01T10:00:00Z",
                AuthorType::Agent,
                Some("agent-1"),
                Some("Assistant"),
            ),
            v_of(
                2,
                "Line 1\nLine 2\nLine 3",
                "2024-01-01T10:05:00Z",
                AuthorType::User,
            ),
        ];
        let attributions = attribute_lines("Line 1\nLine 2\nLine 3", &versions);
        assert_eq!(
            attributions[0].version.as_ref().unwrap().author.author_type,
            "agent"
        );
        assert_eq!(
            attributions[1].version.as_ref().unwrap().author.author_type,
            "agent"
        );
        assert_eq!(
            attributions[2].version.as_ref().unwrap().author.author_type,
            "user"
        );
    }
}
