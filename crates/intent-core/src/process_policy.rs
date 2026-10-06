//! Immutable process-start restrictions for an explicitly isolated test app.
//!
//! These are not settings: an RPC or file reload cannot relax them. The binary
//! freezes the environment before creating its runtime; library owners freeze
//! it on first use. Neither policy changes ordinary processes when absent.

use std::sync::OnceLock;

pub const PRIVATE_TEST_PROFILE_ENV: &str = "INTENTD_PRIVATE_TEST_PROFILE";
pub const DISABLE_GH_CREDENTIALS_ENV: &str = "INTENTD_DISABLE_GH_CREDENTIALS";

#[derive(Clone, Copy, Debug)]
pub struct ProcessPolicy {
    private_test_profile: bool,
    gh_credentials: bool,
}

static POLICY: OnceLock<ProcessPolicy> = OnceLock::new();

impl ProcessPolicy {
    fn from_presence(private_test_profile: bool, disable_gh: bool) -> Self {
        Self {
            private_test_profile,
            gh_credentials: !private_test_profile && !disable_gh,
        }
    }

    /// Presence (even an empty value) opts in. Later environment changes do not
    /// change this snapshot, including after a settings reset or live reload.
    pub fn current() -> &'static Self {
        POLICY.get_or_init(|| {
            Self::from_presence(
                std::env::var_os(PRIVATE_TEST_PROFILE_ENV).is_some(),
                std::env::var_os(DISABLE_GH_CREDENTIALS_ENV).is_some(),
            )
        })
    }

    #[must_use]
    pub fn private_test_profile(self) -> bool {
        self.private_test_profile
    }

    #[must_use]
    pub fn gh_credentials_allowed(self) -> bool {
        self.gh_credentials
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_profile_implies_credential_containment() {
        for private in [false, true] {
            for disable_gh in [false, true] {
                let policy = ProcessPolicy::from_presence(private, disable_gh);
                assert_eq!(policy.private_test_profile(), private);
                assert_eq!(policy.gh_credentials_allowed(), !private && !disable_gh);
            }
        }
    }
}
