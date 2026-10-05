use super::{budget, cursor, db, digest, fields, frame_len, invalid, safe, token};
use intent_core::{note_receipt_detail::ReceiptDetailQuery, Error, Result};
use serde_json::{json, Value};
use sqlx::{Row, SqliteConnection};

// Only the published context/metadata envelopes cross this boundary. Provenance
// leaves remain writer-owned data; this reader never invents native identities.
pub(super) fn valid_record(item: &Value) -> bool {
    if item["kind"] == "fragment" {
        return fields(item, 6)
            && token(&item["id"])
            && token(&item["field"])
            && safe(&item["offset"])
            && item["text"].as_str().is_some_and(|s| s.len() <= 16384)
            && (item["nextRef"].is_null() || token(&item["nextRef"]));
    }
    if !token(&item["id"]) || !(item["parentId"].is_null() || token(&item["parentId"])) {
        return false;
    }
    let Some(object) = item.as_object() else {
        return false;
    };
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "id" | "parentId"
                | "key"
                | "keyRef"
                | "index"
                | "type"
                | "value"
                | "valueRef"
                | "childrenRef"
        )
    }) {
        return false;
    }
    if item
        .get("key")
        .is_some_and(|v| !v.as_str().is_some_and(|s| s.len() <= 1024))
        || ["keyRef", "valueRef", "childrenRef"]
            .into_iter()
            .any(|f| item.get(f).is_some_and(|v| !token(v)))
        || item.get("index").is_some_and(|v| !safe(v))
    {
        return false;
    }
    match item["type"].as_str() {
        Some("string") => {
            (item.get("value").is_some() != item.get("valueRef").is_some())
                && item
                    .get("value")
                    .is_none_or(|v| v.as_str().is_some_and(|s| s.len() <= 1024))
        }
        Some("object" | "array") => token(&item["childrenRef"]) && item.get("value").is_none(),
        Some("number") => item["value"].is_number(),
        Some("boolean") => item["value"].is_boolean(),
        Some("null") => item.get("value").is_some_and(Value::is_null),
        _ => false,
    }
}

fn utf16_byte(text: &str, units: u64) -> Result<usize> {
    let mut at = 0;
    for (byte, ch) in text.char_indices() {
        if at == units {
            return Ok(byte);
        }
        at += u64::try_from(ch.len_utf16()).map_err(db)?;
        if at > units {
            return Err(invalid());
        }
    }
    if at == units {
        Ok(text.len())
    } else {
        Err(invalid())
    }
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn inverse_text(
    conn: &mut SqliteConnection,
    operation: &str,
    query: &ReceiptDetailQuery,
    offset: i64,
    out: &mut Value,
    key: &[u8],
    binding: &[u8; 32],
    rpc_id: &Value,
) -> Result<()> {
    let text_id = query.text_id.as_deref().ok_or_else(invalid)?;
    let replacement=sqlx::query("SELECT json_extract(value,'$.replacement.length') AS length,json_extract(value,'$.replacement.utf8Bytes') AS utf8_bytes,json_extract(value,'$.replacement.sha256') AS sha256 FROM note_operation_item WHERE operation_key=? AND kind='inverse' AND json_extract(value,'$.replacement.textId')=? LIMIT 1")
        .bind(operation).bind(text_id).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
    let row=sqlx::query("SELECT phase,start,end,length,utf8_bytes,sha256 FROM note_operation_text WHERE operation_key=? AND text_id=?")
        .bind(operation).bind(text_id).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
    let start: i64 = row.get("start");
    let end: i64 = row.get("end");
    let length: i64 = row.get("length");
    if start < 0
        || end < start
        || end - start != length
        || offset < 0
        || offset > length
        || row.get::<i64, _>("utf8_bytes") < 0
        || !digest(&json!(row.get::<String, _>("sha256")))
    {
        return Err(invalid());
    }
    if row.get::<i64, _>("length") != replacement.try_get::<i64, _>("length").map_err(db)?
        || row.get::<i64, _>("utf8_bytes")
            != replacement.try_get::<i64, _>("utf8_bytes").map_err(db)?
        || row.get::<String, _>("sha256")
            != replacement.try_get::<String, _>("sha256").map_err(db)?
    {
        return Err(invalid());
    }
    let phase: String = row.get("phase");
    let mut source_at = start + offset;
    let mut text = String::new();
    while source_at < end && text.len() < query.max_source_bytes {
        let piece=sqlx::query("SELECT start,end,text FROM note_operation_source WHERE operation_key=? AND phase=? AND start<=? ORDER BY start DESC LIMIT 1")
            .bind(operation).bind(&phase).bind(source_at).fetch_optional(&mut *conn).await.map_err(db)?
            .ok_or_else(||Error::Internal("missing receipt source piece".into()))?;
        let piece_start: i64 = piece.get("start");
        let piece_end: i64 = piece.get("end");
        let value: String = piece.get("text");
        if piece_end <= source_at
            || piece_start > source_at
            || piece_end - piece_start != i64::try_from(value.encode_utf16().count()).map_err(db)?
        {
            return Err(invalid());
        }
        let from = utf16_byte(&value, u64::try_from(source_at - piece_start).map_err(db)?)?;
        let to = utf16_byte(
            &value,
            u64::try_from(end.min(piece_end) - piece_start).map_err(db)?,
        )?;
        let mut advanced = false;
        for ch in value[from..to].chars() {
            if text.len() + ch.len_utf8() > query.max_source_bytes {
                break;
            }
            text.push(ch);
            source_at += i64::try_from(ch.len_utf16()).map_err(db)?;
            advanced = true;
        }
        if !advanced || source_at < end.min(piece_end) {
            break;
        }
    }
    if offset < length && text.is_empty() {
        return Err(budget());
    }
    loop {
        let units = i64::try_from(text.encode_utf16().count()).map_err(db)?;
        let next = offset + units;
        out["items"] = json!([{"textId":text_id,"offset":offset,"text":text}]);
        out["nextCursor"] = if next < length {
            json!(cursor(key, binding, u64::try_from(next).map_err(db)?)?)
        } else {
            Value::Null
        };
        if frame_len(out, rpc_id) <= query.max_wire_bytes {
            return Ok(());
        }
        if text.chars().count() <= 1 {
            return Err(budget());
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
