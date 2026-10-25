//! Writing a note's text requires that note's `NoteWriteLock`, and the only
//! way to get one is `Store::lock_note_content` (intent-hq/intent#7184).
#[test]
fn note_text_writes_without_the_lock_are_rejected() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/note_write_lock_*.rs");
}
