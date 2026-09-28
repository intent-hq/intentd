//! Actual Store and local Git source adapters, compiled without registration.
//! No listener, provider, credential writer or native Git effect is started.

#[expect(
    dead_code,
    unused_imports,
    reason = "concrete Store provenance is exercised in the Services unit target; this harness retains injected engine coverage"
)]
#[path = "../src/repository_admission.rs"]
mod repository_admission;
#[expect(
    dead_code,
    reason = "NativeRead root/group consumers are exercised in Services; this harness preserves original source adapter coverage"
)]
#[path = "../src/repository_admission/git_source.rs"]
mod repository_admission_git_source;
#[path = "../src/repository_context_reader.rs"]
mod repository_context_reader;
#[path = "../src/repository_credentials.rs"]
#[expect(
    dead_code,
    reason = "test-local directory includes auth-writer seams exercised by owner tests"
)]
mod repository_credentials;
#[path = "../src/repository_admission/source_tests.rs"]
mod source_tests;
#[path = "../src/test_support.rs"]
mod test_support;
