//! Actual Store and local Git source adapters, compiled without registration.
//! No listener, provider, credential writer or native Git effect is started.

#[path = "../src/repository_admission.rs"]
mod repository_admission;
#[path = "../src/repository_context_reader.rs"]
mod repository_context_reader;
#[path = "../src/repository_credentials.rs"]
mod repository_credentials;
#[path = "../src/repository_admission/source_tests.rs"]
mod source_tests;
#[path = "../src/test_support.rs"]
mod test_support;
