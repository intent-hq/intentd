//! Compile the actual private engine without registering it in Services.
#[expect(
    dead_code,
    reason = "inactive service engine includes future stage-facing seams"
)]
#[path = "../src/repository_credentials.rs"]
mod repository_credentials;
