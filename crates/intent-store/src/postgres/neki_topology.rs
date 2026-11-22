//! The `PlanetScale` Neki data topology check [`migrate`](super::migrate)
//! runs before it marks a migration complete.
//!
//! Neki places a table's rows by the data topology
//! (`__neki.get_data_topology()`): a table bound to a shard group lives on
//! that group's shards, split by its shard index; an unbound table lives on
//! the topology's default group. A store transaction touches one tenant, and
//! in single-shard mode (`__neki.tx_mode = 'single'`) Neki refuses one that
//! reaches a second shard (NK313). So every table with a `tenant_id` column
//! must sit in the same shard group: a new tenant table that the topology
//! does not bind lands on the default group, and the first transaction that
//! touches it and an existing tenant table (for `tenant_lease`: every claim,
//! with `tenant_writer`) fails for every tenant off the default group's
//! shard. Its rows are also on the wrong shard from the first write, and
//! moving them later is a resharding job.
//!
//! The migrate job does not bind tables itself: changing the topology needs
//! the `neki_operator` role, which the job's role should not hold. It refuses
//! instead, before the schema gate would let daemons write, and the operator
//! binds the tables it names and re-runs it (see [`migrate`](super::migrate)).

use std::fmt;

use serde_json::Value;

use super::{db, PgPool, MULTI_SHARD_SQL};
use intent_core::Result;

/// Tenant tables of a schema that the Neki data topology does not keep in
/// the shard group of the schema's other tenant tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NekiTopologyGap {
    pub database: String,
    pub schema: String,
    /// Where the schema's tenant tables belong: `tenant_writer`'s group
    /// (every writer locks its row), else the first bound tenant table's.
    pub shard_group: String,
    /// Tenant tables the topology does not bind, so on the default group.
    pub unbound: Vec<String>,
    /// Tenant tables bound to another group: `(table, group)`.
    pub misplaced: Vec<(String, String)>,
}

impl fmt::Display for NekiTopologyGap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the Neki data topology does not put every tenant table of {}.{} in shard group {}",
            self.database, self.schema, self.shard_group
        )?;
        if !self.unbound.is_empty() {
            write!(f, "; not bound: {}", self.unbound.join(", "))?;
        }
        if !self.misplaced.is_empty() {
            let misplaced: Vec<String> = self
                .misplaced
                .iter()
                .map(|(table, group)| format!("{table} (in {group})"))
                .collect();
            write!(f, "; bound to another group: {}", misplaced.join(", "))?;
        }
        Ok(())
    }
}

/// The tenant tables of the pool's current schema that the Neki data
/// topology does not keep together; `None` when it does, when the schema has
/// no bound tenant table (nothing is sharded), or on vanilla Postgres (no
/// `__neki` schema). For operator tooling and the Neki test harness: bind
/// what this names to [`NekiTopologyGap::shard_group`] before the first
/// write, then re-run [`migrate`](super::migrate).
///
/// # Errors
/// `Error::Internal` when the topology or the catalog cannot be read.
pub async fn neki_topology_gap(pool: &PgPool) -> Result<Option<NekiTopologyGap>> {
    let mut conn = pool.acquire().await.map_err(db)?.detach();
    // The catalog read spans shards: undo a single-shard startup mode.
    let gap = match sqlx::query(MULTI_SHARD_SQL).execute(&mut conn).await {
        Ok(_) => topology_gap(&mut conn).await.map_err(db),
        Err(e) => Err(db(e)),
    };
    let closed = sqlx::Connection::close(conn).await.map_err(db);
    gap.and_then(|gap| closed.map(|()| gap))
}

/// [`neki_topology_gap`] on `conn`, whose `search_path` names the schema.
pub(super) async fn topology_gap(
    conn: &mut sqlx::PgConnection,
) -> std::result::Result<Option<NekiTopologyGap>, sqlx::Error> {
    let doc: String = match sqlx::query_scalar("SELECT __neki.get_data_topology()::text")
        .fetch_one(&mut *conn)
        .await
    {
        Ok(doc) => doc,
        // invalid_schema_name: no `__neki` schema, so not Neki. Anything
        // else (a missing or changed function, no privilege) is an error, as
        // for `__neki.ddl_versions()`: skipping the check on Neki would let a
        // new tenant table land on the wrong shard.
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("3F000") => return Ok(None),
        Err(e) => return Err(e),
    };
    let topology: Value =
        serde_json::from_str(&doc).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
    let (database, schema): (String, String) =
        sqlx::query_as("SELECT current_database()::text, current_schema()::text")
            .fetch_one(&mut *conn)
            .await?;
    let tenant_tables: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_catalog.pg_class c \
         WHERE c.relnamespace = current_schema()::regnamespace \
           AND c.relkind IN ('r', 'p') AND NOT c.relispartition \
           AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid = c.oid \
                         AND a.attname = 'tenant_id' AND NOT a.attisdropped) \
         ORDER BY 1",
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(gap_in(&topology, &database, &schema, &tenant_tables))
}

/// The gap in `topology` for `tenant_tables` of `database.schema`.
pub(super) fn gap_in(
    topology: &Value,
    database: &str,
    schema: &str,
    tenant_tables: &[String],
) -> Option<NekiTopologyGap> {
    let bound = &topology["databases"][database]["schemas"][schema]["tables"];
    let group_of = |table: &str| bound[table]["shard_group"].as_str();
    // No bound tenant table: the schema is not sharded, and every tenant
    // table shares the default group.
    let shard_group = group_of("tenant_writer")
        .or_else(|| tenant_tables.iter().find_map(|t| group_of(t)))?
        .to_owned();
    // An unbound table lives on the default group: together with the others
    // only when that is their group.
    let default_is_home = topology["default_shard_group"].as_str() == Some(shard_group.as_str());
    let mut unbound = Vec::new();
    let mut misplaced = Vec::new();
    for table in tenant_tables {
        match group_of(table) {
            None if !default_is_home => unbound.push(table.clone()),
            Some(group) if group != shard_group => {
                misplaced.push((table.clone(), group.to_owned()));
            }
            _ => {}
        }
    }
    (!unbound.is_empty() || !misplaced.is_empty()).then(|| NekiTopologyGap {
        database: database.to_owned(),
        schema: schema.to_owned(),
        shard_group,
        unbound,
        misplaced,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn tables(names: &[&str]) -> Vec<String> {
        names.iter().map(|&n| n.to_owned()).collect()
    }

    /// The shape of topology revision 106152 on the sharded test branch.
    fn topology(bound: &Value) -> Value {
        json!({
            "shard_indexes": {"xxhash_tenant_id": {"type": "xxhash", "columns": ["tenant_id"]}},
            "shard_groups": [
                {"uid": "sh1", "key_ranges": [{"shard_uid": "sh1"}]},
                {"uid": "tenant_data", "default_shard_index": "xxhash_tenant_id",
                 "key_ranges": [{"shard_uid": "sh2", "end": "80"}, {"shard_uid": "sh3", "start": "80"}]}
            ],
            "databases": {"postgres": {"schemas": {"intent_shard_ci": {"tables": bound}}}},
            "authoritative_shard_group": "sh1",
            "default_shard_group": "sh1"
        })
    }

    /// The run that found this: migration 0011 added `tenant_lease`, and the
    /// topology bound every tenant table but it.
    #[test]
    fn a_new_tenant_table_missing_from_a_sharded_schema_is_a_gap() {
        let topology = topology(&json!({
            "tenant_writer": {"shard_group": "tenant_data"},
            "settings": {"shard_group": "tenant_data"},
            "intent_schema_migrations": {"shard_group": "sh1"}
        }));
        let gap = gap_in(
            &topology,
            "postgres",
            "intent_shard_ci",
            &tables(&["settings", "tenant_lease", "tenant_writer"]),
        )
        .expect("tenant_lease is unbound");
        assert_eq!(gap.shard_group, "tenant_data");
        assert_eq!(gap.unbound, tables(&["tenant_lease"]));
        assert!(gap.misplaced.is_empty());
        let message = gap.to_string();
        assert!(
            message.contains("postgres.intent_shard_ci")
                && message.contains("tenant_data")
                && message.contains("not bound: tenant_lease"),
            "{message}"
        );
    }

    #[test]
    fn a_tenant_table_in_another_group_is_a_gap() {
        let topology = topology(&json!({
            "tenant_writer": {"shard_group": "tenant_data"},
            "tenant_lease": {"shard_group": "sh1"}
        }));
        let gap = gap_in(
            &topology,
            "postgres",
            "intent_shard_ci",
            &tables(&["tenant_lease", "tenant_writer"]),
        )
        .expect("tenant_lease is in another group");
        assert_eq!(gap.misplaced, vec![("tenant_lease".into(), "sh1".into())]);
        assert!(gap.to_string().contains("tenant_lease (in sh1)"));
    }

    #[test]
    fn a_fully_bound_schema_has_no_gap() {
        let topology = topology(&json!({
            "tenant_writer": {"shard_group": "tenant_data"},
            "tenant_lease": {"shard_group": "tenant_data"}
        }));
        let all = tables(&["tenant_lease", "tenant_writer"]);
        assert_eq!(gap_in(&topology, "postgres", "intent_shard_ci", &all), None);
    }

    /// A schema the topology does not name (a fresh `intent_ci_*` schema, a
    /// one-shard database): every table is on the default group, together.
    #[test]
    fn an_unsharded_schema_has_no_gap() {
        let topology = topology(&json!({}));
        let all = tables(&["tenant_lease", "tenant_writer"]);
        assert_eq!(gap_in(&topology, "postgres", "intent_shard_ci", &all), None);
        assert_eq!(gap_in(&topology, "postgres", "intent_ci_1_x", &all), None);
        assert_eq!(gap_in(&json!({}), "postgres", "public", &all), None);
    }

    /// Tenant tables bound to the default group share it with unbound ones.
    #[test]
    fn unbound_tables_are_together_when_the_default_group_is_home() {
        let topology = topology(&json!({"tenant_writer": {"shard_group": "sh1"}}));
        let all = tables(&["tenant_lease", "tenant_writer"]);
        assert_eq!(gap_in(&topology, "postgres", "intent_shard_ci", &all), None);
    }

    /// Without a bound `tenant_writer`, the first bound tenant table names
    /// the group, and `tenant_writer` itself is reported.
    #[test]
    fn an_unbound_tenant_writer_is_a_gap_too() {
        let topology = topology(&json!({"settings": {"shard_group": "tenant_data"}}));
        let gap = gap_in(
            &topology,
            "postgres",
            "intent_shard_ci",
            &tables(&["settings", "tenant_writer"]),
        )
        .expect("tenant_writer is unbound");
        assert_eq!(gap.shard_group, "tenant_data");
        assert_eq!(gap.unbound, tables(&["tenant_writer"]));
    }
}
