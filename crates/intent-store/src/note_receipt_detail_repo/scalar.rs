use super::{budget, db, frame_len, invalid, token};
use intent_core::{note_receipt_detail::ReceiptDetailQuery, Result};
use serde_json::{json, Value};
use sqlx::{Row, SqliteConnection};

pub(super) struct Scalar {
    root: String,
    id: String,
    field: String,
    phase: String,
    length: i64,
    offset: i64,
}

// A derived suffix is meaningful only when the exact root is both registered
// reachable and owns a scalar row. Ordinary directory refs cannot be stripped.
pub(super) async fn resolve(
    conn: &mut SqliteConnection,
    operation: &str,
    query: &ReceiptDetailQuery,
) -> Result<Option<Scalar>> {
    let (root, derived) = match query.reference.split_once('@') {
        Some((root, suffix)) => {
            let offset = suffix.parse::<u64>().map_err(|_| invalid())?;
            if offset > 9_007_199_254_740_991 || offset.to_string() != suffix {
                return Err(invalid());
            }
            (root, Some(offset))
        }
        None => (query.reference.as_str(), None),
    };
    let registered: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_operation_reference WHERE operation_key=? AND reference=?)")
        .bind(operation).bind(root).fetch_one(&mut *conn).await.map_err(db)?;
    if !registered {
        return Err(invalid());
    }
    let row = sqlx::query("SELECT id,field,phase,length FROM note_operation_scalar WHERE operation_key=? AND reference=?")
        .bind(operation).bind(root).fetch_optional(&mut *conn).await.map_err(db)?;
    let Some(row) = row else {
        return if derived.is_some() || query.offset.is_some() {
            Err(invalid())
        } else {
            Ok(None)
        };
    };
    if query.cursor.is_some() || derived.zip(query.offset).is_some_and(|(a, b)| a != b) {
        return Err(invalid());
    }
    let scalar = Scalar {
        root: root.into(),
        id: row.get("id"),
        field: row.get("field"),
        phase: row.get("phase"),
        length: row.get("length"),
        offset: i64::try_from(derived.or(query.offset).unwrap_or(0)).map_err(|_| invalid())?,
    };
    if [&scalar.root, &scalar.id, &scalar.field, &scalar.phase]
        .into_iter()
        .any(|s| !token(&json!(s)))
        || scalar.length < 0
        || scalar.length > 9_007_199_254_740_991
        || scalar.offset > scalar.length
    {
        return Err(invalid());
    }
    Ok(Some(scalar))
}

const PIECE_SQL: &str = "SELECT start,end,text FROM note_operation_source WHERE operation_key=? AND phase=? AND start<=? ORDER BY start DESC LIMIT 1";

impl Scalar {
    async fn text(
        &self,
        conn: &mut SqliteConnection,
        operation: &str,
        max_bytes: usize,
    ) -> Result<String> {
        let mut at = self.offset;
        let mut text = String::new();
        // Even an EOF request validates the retained scalar endpoint.
        let mut first = self.length > 0;
        while first || (at < self.length && text.len() < max_bytes) {
            first = false;
            let row = sqlx::query(PIECE_SQL)
                .bind(operation)
                .bind(&self.phase)
                .bind(at)
                .fetch_optional(&mut *conn)
                .await
                .map_err(db)?
                .ok_or_else(invalid)?;
            let start: i64 = row.get("start");
            let end: i64 = row.get("end");
            let value: String = row.get("text");
            if start < 0
                || end <= start
                || start > at
                || end < at
                || end > self.length
                || value.len() > 4096
                || end - start != i64::try_from(value.encode_utf16().count()).map_err(db)?
            {
                return Err(invalid());
            }
            let byte = super::detail::utf16_byte(&value, u64::try_from(at - start).map_err(db)?)?;
            if at == self.length {
                break;
            }
            if at == end {
                return Err(invalid());
            }
            for ch in value[byte..].chars() {
                if text.len() + ch.len_utf8() > max_bytes {
                    return Ok(text);
                }
                text.push(ch);
                at += i64::try_from(ch.len_utf16()).map_err(db)?;
            }
        }
        Ok(text)
    }

    pub(super) async fn page(
        &self,
        conn: &mut SqliteConnection,
        operation: &str,
        query: &ReceiptDetailQuery,
        out: &mut Value,
        rpc_id: &Value,
    ) -> Result<()> {
        let mut text = self.text(conn, operation, query.max_source_bytes).await?;
        loop {
            let next = self.offset + i64::try_from(text.encode_utf16().count()).map_err(db)?;
            let next_ref = if next < self.length {
                let r = format!("{}@{next}", self.root);
                if !token(&json!(r)) {
                    return Err(budget());
                }
                json!(r)
            } else {
                Value::Null
            };
            out["items"] = json!([{"kind":"fragment","id":self.id,"field":self.field,"offset":self.offset,"text":text,"nextRef":next_ref}]);
            out["nextCursor"] = Value::Null;
            if frame_len(out, rpc_id) <= query.max_wire_bytes {
                if next == self.offset && next < self.length {
                    return Err(budget());
                }
                return Ok(());
            }
            let mut cut = text.len() / 2;
            while !text.is_char_boundary(cut) {
                cut -= 1;
            }
            if cut == 0 {
                return Err(budget());
            }
            text.truncate(cut);
        }
    }
}
