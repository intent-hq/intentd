//! Test-local compilation of the inactive private admission engine.
//! Does not register it in Services or install entry/auth-writer hooks.

#[path = "../src/repository_admission.rs"]
mod repository_admission;

#[path = "../src/repository_credentials.rs"]
mod repository_credentials;
