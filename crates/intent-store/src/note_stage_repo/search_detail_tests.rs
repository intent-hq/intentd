use super::*;
use serde_json::json;
use sqlx::Connection;

const KEY: &[u8] = &[42; 32];

fn binding(length: u64) -> Context {
    Context {
        rendered: None,
        operation: "op".into(),
        view: "view".into(),
        payload: "payload".into(),
        expires: "2030-01-01T00:00:00.123Z".into(),
        generation: 0,
        length,
        binding: [19; 32],
        key: KEY.to_vec(),
        query: "query".into(),
    }
}

fn issue(context: &Context, start: u64, end: u64, offset: u64) -> String {
    sign(
        "nsh1.",
        &context.key,
        &context.binding,
        &[start, end, offset],
    )
    .unwrap()
}

async fn fixture(text: &str) -> (SqliteConnection, u64) {
    let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE note_stage_root(root_key TEXT PRIMARY KEY,workspace_id TEXT,note_id TEXT,content_generation TEXT,source_length INTEGER);
         CREATE TABLE note_stage(operation_key TEXT PRIMARY KEY,root_key TEXT);
         CREATE TABLE note_stage_view(operation_key TEXT,generation INTEGER,length INTEGER,PRIMARY KEY(operation_key,generation));
         CREATE TABLE note_stage_view_piece(operation_key TEXT,generation INTEGER,start INTEGER,end INTEGER,origin_kind TEXT,origin_id TEXT,origin_start INTEGER,PRIMARY KEY(operation_key,generation,start));
         CREATE TABLE note_stage_base_piece(root_key TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(root_key,start));
         CREATE TABLE note_page_piece(workspace_id TEXT,note_id TEXT,content_generation TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(workspace_id,note_id,content_generation,start));
         CREATE TABLE note_stage_text(operation_key TEXT,text_id TEXT,length INTEGER,PRIMARY KEY(operation_key,text_id));
         CREATE TABLE note_stage_text_piece(operation_key TEXT,text_id TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(operation_key,text_id,start));",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let length = u64::try_from(text.encode_utf16().count()).unwrap();
    sqlx::query("INSERT INTO note_stage_root VALUES('root','ws','note','old',?)")
        .bind(i64::try_from(length).unwrap())
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_stage VALUES('op','root')")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_stage_view VALUES('op',0,?)")
        .bind(i64::try_from(length).unwrap())
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_stage_view_piece VALUES('op',0,0,?,'root','root',0)")
        .bind(i64::try_from(length).unwrap())
        .execute(&mut conn)
        .await
        .unwrap();
    // Small pieces force actual indexed reads across supplementary scalars and
    // source seams, without a special concatenating test reader.
    let mut start = 0_i64;
    for c in text.chars() {
        let end = start + i64::try_from(c.len_utf16()).unwrap();
        sqlx::query("INSERT INTO note_page_piece VALUES('ws','note','old',?,?,?)")
            .bind(start)
            .bind(end)
            .bind(c.to_string())
            .execute(&mut conn)
            .await
            .unwrap();
        start = end;
    }
    (conn, length)
}

#[test]
fn search_detail_refs_bind_span_position_and_immutable_context() {
    let bound = binding(30);
    let reference = issue(&bound, 3, 20, 0);
    assert_eq!(reference.len(), 123);
    assert_eq!(
        decode_ref(&bound, &reference).unwrap(),
        SearchDetailPosition {
            start: 3,
            end: 20,
            offset: 0
        }
    );
    // Context::load owns hashing every immutable identity field. This helper
    // verifies that exact digest and the persisted backend key, not a new hash.
    let other = Context {
        binding: [20; 32],
        ..binding(30)
    };
    assert!(decode_ref(&other, &reference).is_err());
    let other = Context {
        key: vec![43; 32],
        ..binding(30)
    };
    assert!(decode_ref(&other, &reference).is_err());
    for invalid in [
        format!("{reference}="),
        "nsh1.!".into(),
        "x".repeat(1000),
        reference.replacen("nsh1.", "nsd1.", 1),
    ] {
        assert!(decode_ref(&bound, &invalid).is_err());
    }
    let mut tampered = reference.clone().into_bytes();
    tampered[10] = if tampered[10] == b'A' { b'B' } else { b'A' };
    assert!(decode_ref(&bound, &String::from_utf8(tampered).unwrap()).is_err());
    for values in [
        [0, 0, 0],
        [10, 9, 0],
        [0, 31, 0],
        [3, 20, 17],
        [3, 20, 18],
        [3, 20, u64::MAX],
    ] {
        assert!(decode_ref(&bound, &issue(&bound, values[0], values[1], values[2])).is_err());
    }
}

#[tokio::test]
async fn search_detail_streams_only_raw_match_and_preserves_relative_offsets() {
    let raw = "😀Σσ\"\\\r\n界🙂";
    let source = format!("before{raw}after");
    let (mut conn, length) = fixture(&source).await;
    let bound = binding(length);
    let start = 6;
    let end = start + u64::try_from(raw.encode_utf16().count()).unwrap();
    for budget in [4, 5, 7, 16384] {
        let mut reference = issue(&bound, start, end, 0);
        let mut output = String::new();
        let mut offset = 0;
        let mut id = None;
        loop {
            let item = read_fragment(
                &mut conn,
                &SearchDetailRead {
                    context: &bound,
                    reference: &reference,
                    offset: Some(offset),
                    max_source_bytes: budget,
                },
                |_| Ok(true),
            )
            .await
            .unwrap();
            assert_eq!(item["kind"], "fragment");
            assert_eq!(item["field"], "source");
            assert_eq!(item["offset"], offset);
            assert_eq!(item["id"], hit_id(&bound.binding, start, end));
            if let Some(id) = &id {
                assert_eq!(&item["id"], id);
            } else {
                id = Some(item["id"].clone());
            }
            let text = item["text"].as_str().unwrap();
            assert!(!text.is_empty() && text.len() <= budget);
            offset += u64::try_from(text.encode_utf16().count()).unwrap();
            output.push_str(text);
            if item["nextRef"].is_null() {
                break;
            }
            reference = item["nextRef"].as_str().unwrap().into();
            assert_eq!(decode_ref(&bound, &reference).unwrap().offset, offset);
        }
        assert_eq!(output, raw);
        assert_eq!(offset, end - start);
    }
}

#[tokio::test]
async fn search_detail_rejects_surrogate_endpoints_and_off_position_requests() {
    let (mut conn, length) = fixture("A😀B🙂C").await;
    let bound = binding(length);
    for (start, end) in [(2, 3), (1, 2), (0, 5), (5, 7)] {
        let reference = issue(&bound, start, end, 0);
        assert!(read_fragment(
            &mut conn,
            &SearchDetailRead {
                context: &bound,
                reference: &reference,
                offset: None,
                max_source_bytes: 4,
            },
            |_| Ok(true)
        )
        .await
        .is_err());
    }
    let middle = issue(&bound, 1, 6, 1);
    assert!(read_fragment(
        &mut conn,
        &SearchDetailRead {
            context: &bound,
            reference: &middle,
            offset: Some(1),
            max_source_bytes: 4,
        },
        |_| Ok(true),
    )
    .await
    .is_err());
    let reference = issue(&bound, 1, 6, 0);
    for offset in [1, 2, 5, u64::MAX] {
        assert!(read_fragment(
            &mut conn,
            &SearchDetailRead {
                context: &bound,
                reference: &reference,
                offset: Some(offset),
                max_source_bytes: 4,
            },
            |_| Ok(true)
        )
        .await
        .is_err());
    }
}

#[tokio::test]
async fn search_detail_fits_complete_escaped_frame_and_never_publishes_empty() {
    let (mut conn, length) = fixture(&"😀\"\\\n界more".repeat(64)).await;
    let bound = binding(length);
    let reference = issue(&bound, 0, length, 0);
    let request = SearchDetailRead {
        context: &bound,
        reference: &reference,
        offset: None,
        max_source_bytes: 16384,
    };
    let first = read_fragment(
        &mut conn,
        &SearchDetailRead {
            max_source_bytes: 4,
            ..request
        },
        |_| Ok(true),
    )
    .await
    .unwrap();
    let frame = |item: &Value| {
        json!({"jsonrpc":"2.0","id":"\"\\\n","result":{
        "kind":"noteOperationPage","sourceLength":length,"outputKind":"detail",
        "items":[item],"nextCursor":null}})
        .to_string()
        .len()
    };
    let exact = frame(&first);
    let whole = read_fragment(&mut conn, &request, |_| Ok(true))
        .await
        .unwrap();
    assert!(whole["nextRef"].is_null());
    assert!(
        frame(&whole) > exact,
        "fixture must force a nonterminal frame cut"
    );
    let item = read_fragment(&mut conn, &request, |item| Ok(frame(item) <= exact))
        .await
        .unwrap();
    assert_eq!(item, first);
    assert!(matches!(
        read_fragment(&mut conn, &request, |item| Ok(frame(item) < exact)).await,
        Err(Error::NoteMutation(NoteMutationError::Budget))
    ));
    assert!(read_fragment(&mut conn, &request, |_| Err(invalid()))
        .await
        .is_err());
    for budget in [0, 3, 16385] {
        assert!(read_fragment(
            &mut conn,
            &SearchDetailRead {
                max_source_bytes: budget,
                ..request
            },
            |_| Ok(true)
        )
        .await
        .is_err());
    }
}

#[tokio::test]
async fn search_detail_requires_retained_operation_generation_and_exact_extent() {
    let (mut conn, length) = fixture("abc").await;
    for bound in [
        Context {
            operation: "foreign".into(),
            ..binding(length)
        },
        Context {
            generation: 1,
            ..binding(length)
        },
        binding(length + 1),
    ] {
        // Even a correctly signed internal ref cannot turn a missing or wrongly
        // described retained view into a successful detail response.
        let reference = issue(&bound, 0, 2, 0);
        assert!(read_fragment(
            &mut conn,
            &SearchDetailRead {
                context: &bound,
                reference: &reference,
                offset: None,
                max_source_bytes: 4,
            },
            |_| Ok(true)
        )
        .await
        .is_err());
    }
    let bound = binding(length);
    let reference = issue(&bound, 2, 3, 0);
    let item = read_fragment(
        &mut conn,
        &SearchDetailRead {
            context: &bound,
            reference: &reference,
            offset: None,
            max_source_bytes: 4,
        },
        |_| Ok(true),
    )
    .await
    .unwrap();
    assert_eq!(item["text"], "c");
    assert_eq!(item["offset"], 0);
    assert!(item["nextRef"].is_null());
}

#[tokio::test]
async fn search_detail_terminal_frame_can_be_smaller_than_first_fragment() {
    let (mut conn, length) = fixture("😀small").await;
    let bound = binding(length);
    let reference = issue(&bound, 0, length, 0);
    let request = SearchDetailRead {
        context: &bound,
        reference: &reference,
        offset: None,
        max_source_bytes: 4,
    };
    let first = read_fragment(&mut conn, &request, |_| Ok(true))
        .await
        .unwrap();
    let frame = |item: &Value| {
        json!({"jsonrpc":"2.0","id":1,"result":{"items":[item],"nextCursor":null}})
            .to_string()
            .len()
    };
    let exact = frame(&first);
    let whole = read_fragment(
        &mut conn,
        &SearchDetailRead {
            max_source_bytes: 16384,
            ..request
        },
        |item| Ok(frame(item) <= exact),
    )
    .await
    .unwrap();
    assert!(frame(&whole) < exact);
    assert_eq!(whole["text"], "😀small");
    assert!(whole["nextRef"].is_null());
}
