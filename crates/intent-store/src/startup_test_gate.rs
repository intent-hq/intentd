//! Debug-build subprocess barrier; never compiled into release daemons.
use intent_core::{Error, Result};
use sqlx::SqliteConnection;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) async fn reach(stage: u8, conn: &mut SqliteConnection) -> Result<()> {
    let Some(path) = std::env::var_os("INTENTD_TEST_INDEX_STARTUP_GATE") else {
        return Ok(());
    };
    let mut gate = tokio::net::UnixStream::connect(path)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
    gate.write_u8(stage)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
    match gate
        .read_u8()
        .await
        .map_err(|e| Error::Internal(e.to_string()))?
    {
        b'f' => {
            // A real deferred foreign-key violation proves COMMIT failure rolls
            // back the entire rebuild and cannot publish ready.
            for statement in [
                "CREATE TEMP TABLE startup_test_parent(id INTEGER PRIMARY KEY)",
                "CREATE TEMP TABLE startup_test_child(parent_id INTEGER REFERENCES startup_test_parent(id) DEFERRABLE INITIALLY DEFERRED)",
                "INSERT INTO startup_test_child VALUES(1)",
            ] {
                sqlx::query(statement).execute(&mut *conn).await.map_err(|e| Error::Internal(e.to_string()))?;
            }
        }
        b'c' => {}
        _ => return Err(Error::Internal("startup test barrier rejected".into())),
    }
    Ok(())
}

// Deliberately block the actual CPU worker. The single-runtime-thread subprocess
// test can still query status while this projection is paused.
pub(crate) fn projection() -> Result<()> {
    use std::io::{Read, Write};
    let Some(path) = std::env::var_os("INTENTD_TEST_INDEX_STARTUP_GATE") else {
        return Ok(());
    };
    let run = || -> std::io::Result<()> {
        let mut gate = std::os::unix::net::UnixStream::connect(path)?;
        gate.set_read_timeout(Some(std::time::Duration::from_secs(60)))?;
        gate.write_all(b"n")?;
        let mut command = [0];
        gate.read_exact(&mut command)?;
        if command != *b"c" {
            return Err(std::io::Error::other("projection barrier rejected"));
        }
        Ok(())
    };
    run().map_err(|e| Error::Internal(e.to_string()))
}
