fn forbidden(pool: &intent_store::StorePool) {
    pool.set_connect_options(sqlx::sqlite::SqliteConnectOptions::new());
}
fn main() {}
