//! A metadata-only `AgentSessionSummary` can never reach a full-row store
//! writer: writing one back would clear the stored `system_prompt`.
#[test]
fn summary_write_back_is_rejected() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/agent_session_summary_*.rs");
}
