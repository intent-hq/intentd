use super::*;
use serde_json::json;
use sqlx::Connection;

const KEY: &[u8] = &[42; 32];

fn binding(length: u64) -> SearchDetailBinding<'static> {
    SearchDetailBinding {
        operation: "op",
        generation: 0,
        view_length: length,
        digest: [19; 32],
    }
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
    let reference = issue_ref(KEY, &bound, 3, 20).unwrap();
    assert!(reference.len() < 256);
    assert_eq!(
        decode_ref(KEY, &bound, &reference).unwrap(),
        SearchDetailPosition {
            start: 3,
            end: 20,
            offset: 0
        }
    );
    for other in [
        SearchDetailBinding {
            operation: "other",
            ..binding(30)
        },
        SearchDetailBinding {
            generation: 1,
            ..binding(30)
        },
        binding(31),
        SearchDetailBinding {
            digest: [20; 32],
            ..binding(30)
        },
    ] {
        assert!(decode_ref(KEY, &other, &reference).is_err());
    }
    assert!(decode_ref(&[43; 32], &bound, &reference).is_err());
    for invalid in [format!("{reference}="), "nsd1.!".into(), "x".repeat(1000)] {
        assert!(decode_ref(KEY, &bound, &invalid).is_err());
    }
    let mut bytes = URL_SAFE_NO_PAD
        .decode(reference.strip_prefix("nsd1.").unwrap())
        .unwrap();
    bytes[0] ^= 1;
    assert!(decode_ref(
        KEY,
        &bound,
        &format!("nsd1.{}", URL_SAFE_NO_PAD.encode(bytes))
    )
    .is_err());
    for (start, end) in [(0, 0), (10, 9), (0, 31), (u64::MAX, u64::MAX)] {
        assert!(issue_ref(KEY, &bound, start, end).is_err());
    }
    for offset in [17, 18, u64::MAX] {
        assert!(encode_ref(
            KEY,
            &bound,
            SearchDetailPosition {
                start: 3,
                end: 20,
                offset
            }
        )
        .is_err());
    }
    assert!(issue_ref(&[], &bound, 3, 20).is_err());
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
        let mut reference = issue_ref(KEY, &bound, start, end).unwrap();
        let mut output = String::new();
        let mut offset = 0;
        let mut id = None;
        loop {
            let item = read_fragment(
                &mut conn,
                &SearchDetailRead {
                    key: KEY,
                    binding: &bound,
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
            assert_eq!(decode_ref(KEY, &bound, &reference).unwrap().offset, offset);
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
        let reference = issue_ref(KEY, &bound, start, end).unwrap();
        assert!(read_fragment(
            &mut conn,
            &SearchDetailRead {
                key: KEY,
                binding: &bound,
                reference: &reference,
                offset: None,
                max_source_bytes: 4,
            },
            |_| Ok(true)
        )
        .await
        .is_err());
    }
    let reference = issue_ref(KEY, &bound, 1, 6).unwrap();
    for offset in [1, 2, 5, u64::MAX] {
        assert!(read_fragment(
            &mut conn,
            &SearchDetailRead {
                key: KEY,
                binding: &bound,
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
    let (mut conn, length) = fixture("😀\"\\\n界more").await;
    let bound = binding(length);
    let reference = issue_ref(KEY, &bound, 0, length).unwrap();
    let request = SearchDetailRead {
        key: KEY,
        binding: &bound,
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
        SearchDetailBinding {
            operation: "foreign",
            ..binding(length)
        },
        SearchDetailBinding {
            generation: 1,
            ..binding(length)
        },
        binding(length + 1),
    ] {
        // Even a correctly signed internal ref cannot turn a missing or wrongly
        // described retained view into a successful detail response.
        let reference = issue_ref(KEY, &bound, 0, 2).unwrap();
        assert!(read_fragment(
            &mut conn,
            &SearchDetailRead {
                key: KEY,
                binding: &bound,
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
    let reference = issue_ref(KEY, &bound, 2, 3).unwrap();
    let item = read_fragment(
        &mut conn,
        &SearchDetailRead {
            key: KEY,
            binding: &bound,
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
