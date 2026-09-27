//! Test-local compilation of the inactive private admission engine.
//! Does not register it in Services or install entry/auth-writer hooks.

#[expect(
    dead_code,
    unused_imports,
    reason = "concrete Store provenance is exercised in the Services unit target; this harness retains injected engine coverage"
)]
#[path = "../src/repository_admission.rs"]
mod repository_admission;

#[path = "../src/repository_credentials.rs"]
mod repository_credentials;
