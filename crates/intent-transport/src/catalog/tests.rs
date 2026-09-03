//! Golden freeze tests for the v2.0 protocol catalog.
//!
//! These tests enforce that any surface drift (added/removed/renamed methods)
//! fails CI with a clear "update the catalog + docs/protocol/ + bump protocol
//! version" message.
//!
//! Router and fast-path methods are mechanically extracted from source at test
//! runtime to detect drift. Notifications and reverse RPCs use count-based
//! checks only (extracting reverse `.request("...")` call sites would be
//! fragile); renames are a lower-risk edge case caught during code review.

use super::{FASTPATH_METHODS, METHOD_ALIASES, NOTIFICATIONS, REVERSE_METHODS, ROUTER_METHODS};
use std::collections::HashSet;
use std::fmt::Write as _;

/// Extract router methods from the actual source code at test runtime.
///
/// Assumptions: router.rs uses single-line match arms of the form `"method.name" => ...`
/// or `"alias1" | "alias2" => ...`; no escaped quotes in method names; assumes inline
/// comments (if any) appear after the match arm and are tolerated.
fn extract_router_methods() -> HashSet<String> {
    // Read source at runtime to detect drift even after the test is compiled
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let router_path = std::path::Path::new(manifest_dir).join("src/router.rs");
    let source =
        std::fs::read_to_string(&router_path).expect("Failed to read router.rs at test time");

    let mut methods = HashSet::new();

    // Match patterns like: "method.name" => or "git.diffs" | "git.diff" =>
    for line in source.lines() {
        // Look for quoted method names followed by => (with possible | for aliases)
        if let Some(start) = line.find('"') {
            if line[start..].contains("=>") {
                // Extract all quoted strings before =>
                let before_arrow = &line[..line.find("=>").unwrap_or(line.len())];
                for part in before_arrow.split('"') {
                    let trimmed = part.trim();
                    // Check if it looks like a method name (has a dot and alphanumeric)
                    if trimmed.contains('.')
                        && trimmed
                            .chars()
                            .all(|c| c.is_alphanumeric() || c == '.' || c == '_')
                    {
                        methods.insert(trimmed.to_string());
                    }
                }
            }
        }
    }

    methods
}

/// Extract fast-path methods from source files at test runtime.
///
/// Assumption: each fast-path module (events.rs, client.rs, etc.) contains methods
/// exclusively from one namespace prefix (e.g., events.rs contains only events.* methods).
fn extract_fastpath_methods() -> HashSet<String> {
    let mut methods = HashSet::new();
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let base_path = std::path::Path::new(manifest_dir).join("src");

    // Extract from each fast-path module
    // Look for match arms and direct comparisons (like client.hello)
    for (filename, prefix) in [
        ("events.rs", "events."),
        ("client.rs", "client."),
        ("drafts.rs", "drafts."),
        ("browser.rs", "browser."),
        ("forward.rs", "forward."),
        ("host.rs", "host."),
        ("control.rs", "system."),
        ("pairing.rs", "pairing."),
        ("server.rs", "server."),
    ] {
        let source = std::fs::read_to_string(base_path.join(filename))
            .unwrap_or_else(|_| panic!("Failed to read {filename} at test time"));
        for line in source.lines() {
            let trimmed = line.trim();
            // Skip comments
            if trimmed.starts_with("//") {
                continue;
            }
            // Only process lines that are match arms (contain =>) or comparisons (!= or ==)
            let is_match_arm =
                trimmed.contains("=>") && !trimmed.contains("//") && trimmed.contains('"');
            let is_comparison = (trimmed.contains("!=") || trimmed.contains("=="))
                && trimmed.contains('"')
                && trimmed.contains(prefix);

            if is_match_arm || is_comparison {
                // Extract quoted strings
                let mut start_idx = 0;
                while let Some(start) = line[start_idx..].find('"') {
                    let abs_start = start_idx + start;
                    if let Some(end) = line[abs_start + 1..].find('"') {
                        let method = &line[abs_start + 1..abs_start + 1 + end];
                        if method.starts_with(prefix)
                            && method
                                .chars()
                                .all(|c| c.is_alphanumeric() || c == '.' || c == '_')
                        {
                            methods.insert(method.to_string());
                        }
                        start_idx = abs_start + 1 + end + 1;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    methods
}

/// Golden count: total client-callable methods (router + fastpath + aliases).
///
/// This constant must match the sum below. If it doesn't, you've added or
/// removed methods without updating the catalog. The catalog freeze is
/// intentional: any surface change requires a protocol version bump and a
/// docs/protocol/ update.
const EXPECTED_TOTAL_METHODS: usize = 340;

/// Golden count: router methods (canonical + canonical forms of aliases).
/// This includes both git.diffs and git.commits (the canonical forms) even
/// though git.diff→git.diffs and git.log→git.commits are listed as aliases.
const EXPECTED_ROUTER_METHODS: usize = 298;

/// Golden count: fast-path methods (intercepted before router).
const EXPECTED_FASTPATH_METHODS: usize = 40;

/// Golden count: method aliases.
const EXPECTED_ALIASES: usize = 2;

/// Golden count: server→client notifications.
const EXPECTED_NOTIFICATIONS: usize = 1;

/// Golden count: client-served reverse RPCs.
const EXPECTED_REVERSE_METHODS: usize = 4;

#[test]
fn router_methods_match_actual_source() {
    let actual = extract_router_methods();
    let catalog: HashSet<String> = ROUTER_METHODS
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    // Combine catalog + aliases for comparison
    let mut expected = catalog.clone();
    for (alias, _) in METHOD_ALIASES {
        expected.insert(alias.to_string());
    }

    // Find methods in router.rs but not in catalog
    let missing: Vec<_> = actual
        .difference(&expected)
        .map(std::string::String::as_str)
        .collect();

    // Find methods in catalog but not in router.rs
    let extra: Vec<_> = catalog
        .difference(&actual)
        .map(std::string::String::as_str)
        .collect();

    if !missing.is_empty() || !extra.is_empty() {
        let mut msg = String::from(
            "Router method drift detected!\n\
             Update ROUTER_METHODS in catalog.rs, update docs/protocol/05-method-catalog.md, and bump the protocol version.\n"
        );

        if !missing.is_empty() {
            let _ = writeln!(
                msg,
                "\nMethods in router.rs but NOT in catalog ({}):",
                missing.len()
            );
            for m in missing.iter().take(10) {
                let _ = writeln!(msg, "  - {m}");
            }
            if missing.len() > 10 {
                let _ = writeln!(msg, "  ... and {} more", missing.len() - 10);
            }
        }

        if !extra.is_empty() {
            let _ = writeln!(
                msg,
                "\nMethods in catalog but NOT in router.rs ({}):",
                extra.len()
            );
            for m in extra.iter().take(10) {
                let _ = writeln!(msg, "  - {m}");
            }
            if extra.len() > 10 {
                let _ = writeln!(msg, "  ... and {} more", extra.len() - 10);
            }
        }

        panic!("{}", msg);
    }
}

#[test]
fn fastpath_methods_match_actual_source() {
    let actual = extract_fastpath_methods();
    let catalog: HashSet<String> = FASTPATH_METHODS
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    let missing: Vec<_> = actual
        .difference(&catalog)
        .map(std::string::String::as_str)
        .collect();

    let extra: Vec<_> = catalog
        .difference(&actual)
        .map(std::string::String::as_str)
        .collect();

    if !missing.is_empty() || !extra.is_empty() {
        let mut msg = String::from(
            "Fast-path method drift detected!\n\
             Update FASTPATH_METHODS in catalog.rs, update docs/protocol/05-method-catalog.md, and bump the protocol version.\n"
        );

        if !missing.is_empty() {
            let _ = writeln!(
                msg,
                "\nMethods in source but NOT in catalog ({}):",
                missing.len()
            );
            for m in &missing {
                let _ = writeln!(msg, "  - {m}");
            }
        }

        if !extra.is_empty() {
            let _ = writeln!(
                msg,
                "\nMethods in catalog but NOT in source ({}):",
                extra.len()
            );
            for m in &extra {
                let _ = writeln!(msg, "  - {m}");
            }
        }

        panic!("{}", msg);
    }
}

#[test]
fn catalog_counts_frozen() {
    let router_count = ROUTER_METHODS.len();
    let fastpath_count = FASTPATH_METHODS.len();
    let alias_count = METHOD_ALIASES.len();
    let total = router_count + fastpath_count + alias_count;

    assert_eq!(
        router_count, EXPECTED_ROUTER_METHODS,
        "Router method count drift detected: expected {EXPECTED_ROUTER_METHODS}, got {router_count}. \
         If you added/removed router methods, update ROUTER_METHODS in catalog.rs, \
         bump EXPECTED_ROUTER_METHODS, update docs/protocol/05-method-catalog.md, and bump the protocol version."
    );

    assert_eq!(
        fastpath_count, EXPECTED_FASTPATH_METHODS,
        "Fast-path method count drift detected: expected {EXPECTED_FASTPATH_METHODS}, got {fastpath_count}. \
         If you added/removed fast-path methods, update FASTPATH_METHODS in catalog.rs, \
         bump EXPECTED_FASTPATH_METHODS, update docs/protocol/05-method-catalog.md, and bump the protocol version."
    );

    assert_eq!(
        alias_count, EXPECTED_ALIASES,
        "Alias count drift detected: expected {EXPECTED_ALIASES}, got {alias_count}. \
         If you added/removed aliases, update METHOD_ALIASES in catalog.rs, \
         bump EXPECTED_ALIASES, update docs/protocol/05-method-catalog.md, and bump the protocol version."
    );

    assert_eq!(
        total, EXPECTED_TOTAL_METHODS,
        "Total method count drift detected: expected {EXPECTED_TOTAL_METHODS}, got {total} ({router_count} router + {fastpath_count} fastpath + {alias_count} aliases). \
         Update the catalog, docs/protocol/05-method-catalog.md, and bump the protocol version."
    );

    let notification_count = NOTIFICATIONS.len();
    assert_eq!(
        notification_count, EXPECTED_NOTIFICATIONS,
        "Notification count drift detected: expected {EXPECTED_NOTIFICATIONS}, got {notification_count}. \
         Update NOTIFICATIONS in catalog.rs, docs/protocol/05-method-catalog.md, and bump the protocol version."
    );

    let reverse_count = REVERSE_METHODS.len();
    assert_eq!(
        reverse_count, EXPECTED_REVERSE_METHODS,
        "Reverse RPC count drift detected: expected {EXPECTED_REVERSE_METHODS}, got {reverse_count}. \
         Update REVERSE_METHODS in catalog.rs, docs/protocol/05-method-catalog.md, and bump the protocol version."
    );
}

#[test]
fn router_methods_are_sorted() {
    let mut sorted = ROUTER_METHODS.to_vec();
    sorted.sort_unstable();
    assert_eq!(
        ROUTER_METHODS,
        &sorted[..],
        "ROUTER_METHODS must be sorted alphabetically for readability"
    );
}

#[test]
fn fastpath_methods_are_sorted() {
    let mut sorted = FASTPATH_METHODS.to_vec();
    sorted.sort_unstable();
    assert_eq!(
        FASTPATH_METHODS,
        &sorted[..],
        "FASTPATH_METHODS must be sorted alphabetically for readability"
    );
}

#[test]
fn no_duplicate_router_methods() {
    let mut seen = std::collections::HashSet::new();
    for method in ROUTER_METHODS {
        assert!(seen.insert(method), "Duplicate router method: {method}");
    }
}

#[test]
fn no_duplicate_fastpath_methods() {
    let mut seen = std::collections::HashSet::new();
    for method in FASTPATH_METHODS {
        assert!(seen.insert(method), "Duplicate fast-path method: {method}");
    }
}

#[test]
fn no_overlap_between_router_and_fastpath() {
    let router_set: std::collections::HashSet<_> = ROUTER_METHODS.iter().collect();
    for method in FASTPATH_METHODS {
        assert!(
            !router_set.contains(method),
            "Method {method} appears in both ROUTER_METHODS and FASTPATH_METHODS"
        );
    }
}

#[test]
fn aliases_point_to_router_methods() {
    let router_set: std::collections::HashSet<_> = ROUTER_METHODS.iter().collect();
    for (alias, canonical) in METHOD_ALIASES {
        assert!(
            router_set.contains(canonical),
            "Alias {alias} points to {canonical}, but {canonical} is not in ROUTER_METHODS"
        );
    }
}
