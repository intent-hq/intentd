fn forbidden(pool: &intent_store::StorePool) {
    let _ = pool.options().connect_lazy("sqlite::memory:");
}
fn main() {}
