//! Capture portable authors inside the row export's existing WAL snapshot.

use std::collections::{HashMap, HashSet};

use intent_core::{
    human_author::{historical_human_author, HumanAuthor, HUMAN_AUTHOR_KEY},
    is_human_authored_metadata, lift_from_principal_id, Error, Principal, PrincipalId, Result,
};
use serde_json::Value;

use crate::principal_repo::{map_principal_row, PRINCIPAL_COLUMNS};

type Rows = [(String, Vec<Value>)];

#[cfg(test)]
#[derive(Default)]
pub(crate) struct ExportAuthorBarrier {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

fn decode(value: Option<&Value>) -> Result<Option<Value>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => serde_json::from_str(raw)
            .map(Some)
            .map_err(|e| Error::Internal(format!("invalid exported attribution metadata: {e}"))),
        _ => Err(Error::Internal("exported JSON column is not text".into())),
    }
}

fn metadata(table: &str, row: &Value) -> Result<Option<Option<Value>>> {
    match table {
        "agent_message" if row["role"] == "user" => Ok(Some(decode(row.get("metadata"))?)),
        "agent_queue" => Ok(Some(
            decode(row.get("payload"))?.and_then(|p| p.get("messageMetadata").cloned()),
        )),
        _ => Ok(None),
    }
}

fn needs_snapshot(metadata: Option<&Value>) -> bool {
    historical_human_author(metadata).is_none()
        && (lift_from_principal_id(metadata).is_some() || is_human_authored_metadata(metadata))
}

pub(crate) async fn capture(
    connection: &mut sqlx::SqliteConnection,
    rows: &mut Rows,
) -> Result<()> {
    let fallback = rows
        .iter()
        .find(|(table, _)| table == "workspace")
        .and_then(|(_, rows)| rows.first())
        .and_then(|row| {
            row["legacy_author_principal_id"]
                .as_str()
                .or(row["owner_principal_id"].as_str())
        })
        .map(|id| PrincipalId(id.to_string()));
    let mut ids = HashSet::new();
    for (table, objects) in rows.iter() {
        for row in objects {
            if let Some(metadata) = metadata(table, row)? {
                if needs_snapshot(metadata.as_ref()) {
                    if let Some(id) =
                        lift_from_principal_id(metadata.as_ref()).or_else(|| fallback.clone())
                    {
                        ids.insert(id);
                    }
                }
            }
        }
    }
    let ids: Vec<_> = ids.into_iter().collect();
    let mut people: HashMap<PrincipalId, Principal> = HashMap::new();
    for chunk in ids.chunks(32_000) {
        let sql = format!(
            "SELECT {PRINCIPAL_COLUMNS} FROM principal WHERE id IN ({})",
            vec!["?"; chunk.len()].join(",")
        );
        let mut query = sqlx::query(&sql);
        for id in chunk {
            query = query.bind(id.as_str());
        }
        for row in query
            .fetch_all(&mut *connection)
            .await
            .map_err(|e| Error::Internal(format!("export author snapshot failed: {e}")))?
        {
            let person = map_principal_row(&row);
            people.insert(person.id.clone(), person);
        }
    }
    for (table, objects) in rows.iter_mut() {
        for row in objects {
            let Some(mut metadata) = metadata(table, row)? else {
                continue;
            };
            if !needs_snapshot(metadata.as_ref()) {
                continue;
            }
            let id = lift_from_principal_id(metadata.as_ref()).or_else(|| fallback.clone());
            let snapshot =
                HumanAuthor::from_source(id.clone(), id.as_ref().and_then(|id| people.get(id)));
            let value = metadata.get_or_insert_with(|| serde_json::json!({}));
            let object = value.as_object_mut().ok_or_else(|| {
                Error::InvalidParams(
                    "human message metadata must be an object to preserve transfer attribution"
                        .into(),
                )
            })?;
            object.insert(HUMAN_AUTHOR_KEY.into(), serde_json::json!(snapshot));
            if table == "agent_message" {
                row["metadata"] = Value::String(value.to_string());
            } else {
                let mut payload =
                    decode(row.get("payload"))?.unwrap_or_else(|| serde_json::json!({}));
                let object = payload.as_object_mut().ok_or_else(|| {
                    Error::InvalidParams("queued payload must be an object".into())
                })?;
                object.insert("messageMetadata".into(), value.clone());
                row["payload"] = Value::String(payload.to_string());
            }
        }
    }
    Ok(())
}
