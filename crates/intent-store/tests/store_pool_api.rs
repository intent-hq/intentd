//! The owned pool must not export the `SQLx` configuration/initialization bypasses.
#[test]
fn pool_escape_apis_are_rejected() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/store_pool_*.rs");
}
