//! Unit-style integration coverage for `intent-core::error`.

use intent_core::error::{Error, Result};
use serde_json::json;

#[test]
fn invalid_params_code_and_display() {
    let e = Error::InvalidParams("missing `id`".to_string());
    assert_eq!(e.code(), -32602);
    assert_eq!(e.to_string(), "invalid params: missing `id`");
}

#[test]
fn not_found_code_and_display() {
    let e = Error::NotFound("workspace ws-1".to_string());
    assert_eq!(e.code(), -32602);
    assert_eq!(e.to_string(), "not found: workspace ws-1");
}

#[test]
fn internal_code_and_display() {
    let e = Error::Internal("boom".to_string());
    assert_eq!(e.code(), -32603);
    assert_eq!(e.to_string(), "internal error: boom");
}

#[test]
fn conflict_code_and_display_and_payload() {
    let current = json!({ "rev": 7 });
    let e = Error::Conflict {
        current: current.clone(),
    };
    assert_eq!(e.code(), -32009);
    assert_eq!(e.to_string(), "conflict: version mismatch");
    if let Error::Conflict { current: c } = e {
        assert_eq!(c, current);
    } else {
        panic!("variant should be Conflict");
    }
}

#[test]
fn base_ref_unresolvable_code_and_display_and_payload() {
    let e = Error::BaseRefUnresolvable {
        base_ref: "no-such-ref".to_string(),
    };
    assert_eq!(e.code(), -32602);
    assert_eq!(
        e.to_string(),
        "invalid params: cannot resolve base ref 'no-such-ref'"
    );
    if let Error::BaseRefUnresolvable { base_ref } = e {
        assert_eq!(base_ref, "no-such-ref");
    } else {
        panic!("variant should be BaseRefUnresolvable");
    }
}

#[test]
fn debug_includes_variant_name() {
    let e = Error::InvalidParams("x".to_string());
    let dbg = format!("{e:?}");
    assert!(
        dbg.contains("InvalidParams"),
        "debug missing variant: {dbg}"
    );
}

#[test]
fn result_alias_is_usable() {
    #[allow(clippy::unnecessary_wraps)] // exercising the Result alias is the point of the test
    fn ok() -> Result<u32> {
        Ok(42)
    }
    fn err() -> Result<u32> {
        Err(Error::NotFound("nope".to_string()))
    }
    assert_eq!(ok().unwrap(), 42);
    assert!(matches!(err(), Err(Error::NotFound(_))));
}
