//! Portable checkpoint policy is separate from ACP's advertised session/load.
//! A provider may load sessions on the same machine without having a verified
//! portable file format. Never turn an ACP capability into a HOME-directory copy.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointPolicy {
    /// Exact bundle-relative files, never directories, globs or auth stores.
    /// A non-empty list requires provider-aware relocation and fixture coverage.
    pub session_files: &'static [&'static str],
}

/// No shipped adapter currently verifies portable session relocation at the
/// captured journal watermark. Fail closed for known, legacy and unknown IDs:
/// recover with the existing bounded transcript formatter. Add individual exact
/// session files here only alongside a tested provider-aware migration.
#[must_use]
pub fn checkpoint_policy(_provider: &str) -> CheckpointPolicy {
    CheckpointPolicy { session_files: &[] }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portable_sessions_are_explicitly_allowlisted_not_inferred_from_load() {
        for provider in crate::all_provider_ids().into_iter().chain(["unknown"]) {
            assert!(
                checkpoint_policy(provider).session_files.is_empty(),
                "{provider}"
            );
        }
    }
}
