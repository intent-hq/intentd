fn forbidden(lock: &intent_store::NoteWriteLock) -> intent_store::NoteWriteLock {
    lock.clone()
}
fn main() {}
