//! Actual adapters over disposable `SQLite` and Git; authority writer coverage
//! and transport entry registration are separate integration obligations.

use crate::repository_admission::{
    AdmissionError, AdmissionResult, RepositoryOperationFacts, RepositoryRetirement,
};

#[path = "source_tests/fixtures.rs"]
pub(crate) mod fixtures;
#[path = "source_tests/git.rs"]
mod git;
#[path = "git_source.rs"]
mod git_source;
