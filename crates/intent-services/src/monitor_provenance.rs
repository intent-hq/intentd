//! Classify an authentic pre-B monitor without consulting today's remotes.
//!
//! Evidence at the pre-B input 56c7cfe3: migrations 0085/0089 persisted the
//! captured `repo_owner`/`repo_name`/`pr_number`; `pr_monitor` registration copied
//! these after resolution; `pr_ops` resolved the default GitHub-only registry.
//! An original built-in github.com registration can therefore be qualified
//! from that row even after the workspace moves to GitLab. Imported rows or
//! other unknown provider semantics require explicit repair, not a guess.
//!
//! This is classification only. The later store migration must retain the
//! row and its original owner, lifecycle, baselines and pending notification
//! bytes; it must not re-register, poll, dispatch a pending wake or rewrite a
//! hook while classifying. Unresolved rows remain inspectable/cancellable by
//! their original IDs and authority rules.

use intent_core::{PrMonitor, RepoRef};

/// Established from the database's original writer/schema, never from a
/// workspace's CURRENT remote or an account's current connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegacyProviderSemantics {
    BuiltinGithubDotCom,
    Unproven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnresolvedMonitor {
    ProviderProvenanceMissing,
    ProjectProvenanceMissing,
    InvalidNumber,
}

/// The original row is borrowed verbatim even when no target can be proven.
#[derive(Debug)]
pub(crate) struct LegacyMonitor<'a, T> {
    pub(crate) row: &'a PrMonitor,
    pub(crate) target: Result<T, UnresolvedMonitor>,
}

/// `github_target` adapts the proven github.com PR identity to routing's
/// canonical `ReviewTarget` DTO. Keeping the constructor injected avoids a
/// second target model/parser and keeps this helper usable before the shared
/// core export lease. It must construct a GitHub pull request, not a live
/// default target. It is never called when provenance is insufficient.
pub(crate) fn classify_legacy_monitor<T>(
    row: &PrMonitor,
    semantics: LegacyProviderSemantics,
    github_target: impl FnOnce(&RepoRef, u64) -> T,
) -> LegacyMonitor<'_, T> {
    let target = if semantics != LegacyProviderSemantics::BuiltinGithubDotCom {
        Err(UnresolvedMonitor::ProviderProvenanceMissing)
    } else if !legacy_slug_part(&row.repo_owner) || !legacy_slug_part(&row.repo_name) {
        Err(UnresolvedMonitor::ProjectProvenanceMissing)
    } else {
        u64::try_from(row.pr_number)
            .ok()
            .filter(|number| *number > 0)
            .map(|number| github_target(&row.repo(), number))
            .ok_or(UnresolvedMonitor::InvalidNumber)
    };
    LegacyMonitor { row, target }
}

// Validate only the captured legacy two-component slug. A nested GitLab
// namespace, URL, credentials, blank component or traversal is not evidence
// of an old GitHub target. Case identity is delegated to the existing RepoRef.
fn legacy_slug_part(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}
