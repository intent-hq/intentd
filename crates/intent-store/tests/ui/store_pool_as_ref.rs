fn forbidden(pool: &intent_store::StorePool) {
    let _: &sqlx::SqlitePool = pool.as_ref();
}
fn main() {}
