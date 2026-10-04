//! Attest the exact arena schema before admitting fixed-operation transactions.
//! This is an SQL-shape prerequisite, not a physical allocation reservation.
use super::{db_error, Error, Result};
use sha2::{Digest, Sha256};
use sqlx::SqliteConnection;

// Manifest of artifact_arena.sql as stored in sqlite_schema by the pinned
// SQLite. All objects, including automatic indexes, participate. Each ordered
// type/name/tbl_name/sql field is prefixed by its big-endian u64 UTF-8 length.
// Intentional schema changes must update this manifest and the arena version.
const OBJECTS: i64 = 32;
const FIELD_BYTES: i64 = 14_640;
const FINGERPRINT: &str = "6a9741e65b4ebcb1d85348dabb08cf5cece92d81791d4bd85dd03f773990c4b6";

pub(super) async fn verify(connection: &mut SqliteConnection) -> Result<()> {
    let shape: (i64, i64) = sqlx::query_as(
        "SELECT count(*),coalesce(sum(length(CAST(type AS BLOB))+length(CAST(name AS BLOB))+length(CAST(tbl_name AS BLOB))+length(CAST(coalesce(sql,'') AS BLOB))),0) FROM sqlite_schema",
    ).fetch_one(&mut *connection).await.map_err(db_error)?;
    if shape != (OBJECTS, FIELD_BYTES) {
        return Err(Error::Internal("Artifact arena schema mismatch".into()));
    }
    // Reject oversized metadata before transferring SQL text into Rust. SQLite
    // has already opened its schema: this is not a pre-open memory/disk bound.
    let objects: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT type,name,tbl_name,coalesce(sql,'') FROM sqlite_schema ORDER BY type,name",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(db_error)?;
    let mut digest = Sha256::new();
    for (kind, name, table, sql) in objects {
        for field in [kind, name, table, sql] {
            digest.update(
                u64::try_from(field.len())
                    .map_err(|_| Error::Internal("Arena schema field overflow".into()))?
                    .to_be_bytes(),
            );
            digest.update(field.as_bytes());
        }
    }
    if format!("{:x}", digest.finalize()) != FINGERPRINT {
        return Err(Error::Internal("Artifact arena schema mismatch".into()));
    }
    for (pragma, expected) in [
        ("PRAGMA foreign_keys", 1_i64),
        ("PRAGMA synchronous", 2),
        ("PRAGMA mmap_size", 0),
    ] {
        if sqlx::query_scalar::<_, i64>(pragma)
            .fetch_one(&mut *connection)
            .await
            .map_err(db_error)?
            != expected
        {
            return Err(Error::Internal(
                "Artifact arena connection configuration mismatch".into(),
            ));
        }
    }
    for (pragma, expected) in [
        ("PRAGMA journal_mode", "delete"),
        ("PRAGMA locking_mode", "exclusive"),
    ] {
        if sqlx::query_scalar::<_, String>(pragma)
            .fetch_one(&mut *connection)
            .await
            .map_err(db_error)?
            != expected
        {
            return Err(Error::Internal(
                "Artifact arena journal configuration mismatch".into(),
            ));
        }
    }
    Ok(())
}
