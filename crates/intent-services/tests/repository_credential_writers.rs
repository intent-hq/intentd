//! Compile real private adapters and directory without production registration.
#[expect(
    dead_code,
    reason = "test-local writer includes the auth-owner handoff check"
)]
#[path = "../src/repository_credential_writers.rs"]
mod repository_credential_writers;
#[expect(
    dead_code,
    reason = "inactive directory has additional stage-facing seams"
)]
#[path = "../src/repository_credentials.rs"]
mod repository_credentials;

#[path = "repository_credential_writers/support.rs"]
mod support;
#[path = "repository_credential_writers/mutation.rs"]
mod writer_mutation;
#[path = "repository_credential_writers/policy.rs"]
mod writer_policy;
