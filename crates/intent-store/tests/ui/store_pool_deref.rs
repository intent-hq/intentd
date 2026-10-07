fn forbidden(pool: &intent_store::StorePool) {
    let _: &sqlx::SqlitePool = &**pool;
}
fn main() {}
