//! Import historical display authors without importing a local identity grant.

use intent_core::{
    human_author::{HumanAuthor, HUMAN_AUTHOR_KEY},
    is_human_authored_metadata, lift_from_principal_id, Error, PrincipalIdentity, Result,
    FROM_PRINCIPAL_ID_KEY,
};
use serde_json::{json, Map, Value};

fn invalid(message: impl std::fmt::Display) -> Error {
    Error::InvalidParams(format!("invalid transfer author: {message}"))
}

fn identity(value: &Value) -> Result<PrincipalIdentity> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("identity must be an object"))?;
    if object.len() != 3
        || !["provider", "host", "externalUserId"]
            .iter()
            .all(|k| object.contains_key(*k))
    {
        return Err(invalid(
            "identity must contain only provider, host and externalUserId",
        ));
    }
    let identity: PrincipalIdentity = serde_json::from_value(value.clone()).map_err(invalid)?;
    if identity.external_user_id.is_empty()
        || identity.external_user_id.trim() != identity.external_user_id
    {
        return Err(invalid("externalUserId must be nonblank"));
    }
    let canonical = match identity.provider.as_str() {
        "github" => identity.host == "github.com",
        "gitlab" => {
            intent_sourcecontrol::gitlab_auth::GitlabHost::parse(&identity.host).is_ok_and(|host| {
                host.host() == identity.host
                    && host.base_url() == format!("https://{}", identity.host)
            })
        }
        _ => false,
    };
    if !canonical {
        return Err(invalid("identity provider/host is not canonical"));
    }
    Ok(identity)
}

fn snapshot(value: &Value) -> Result<HumanAuthor> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("humanAuthor must be an object"))?;
    for key in ["login", "displayName", "avatarUrl"] {
        if !object
            .get(key)
            .is_some_and(|v| v.is_null() || v.is_string())
        {
            return Err(invalid(format!(
                "humanAuthor.{key} must be a string or null"
            )));
        }
    }
    if let Some(id) = object.get("sourcePrincipalId") {
        if id.as_str().is_none_or(str::is_empty) {
            return Err(invalid("sourcePrincipalId must be a nonempty string"));
        }
    }
    let mut author: HumanAuthor = serde_json::from_value(value.clone()).map_err(invalid)?;
    if let Some(value) = object.get("identity") {
        author.identity = Some(identity(value)?);
    }
    Ok(author)
}

fn metadata(value: Option<Value>, version: u32, user: bool) -> Result<Option<Value>> {
    let human = user
        && (lift_from_principal_id(value.as_ref()).is_some()
            || is_human_authored_metadata(value.as_ref())
            || (version == 2
                && value
                    .as_ref()
                    .and_then(|v| v.get(HUMAN_AUTHOR_KEY))
                    .is_some()));
    let mut object = match value {
        None => Map::new(),
        Some(Value::Object(object)) => object,
        Some(value) if !human => return Ok(Some(value)),
        Some(original) => Map::from_iter([("humanAuthorOriginalMetadata".into(), original)]),
    };
    object.remove(FROM_PRINCIPAL_ID_KEY);
    let raw = object.remove(HUMAN_AUTHOR_KEY);
    if human {
        let author = if version == 2 {
            raw.as_ref().map(snapshot).transpose()?.unwrap_or_default()
        } else {
            HumanAuthor::default()
        };
        object.insert(HUMAN_AUTHOR_KEY.into(), json!(author));
    }
    Ok((!object.is_empty()).then_some(Value::Object(object)))
}

fn decode(value: Option<&Value>) -> Result<Option<Value>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => serde_json::from_str(raw).map(Some).map_err(invalid),
        _ => Err(invalid("JSON column must be text")),
    }
}

fn encode(value: Option<Value>) -> Value {
    value.map_or(Value::Null, |v| Value::String(v.to_string()))
}

pub(crate) fn prepare_import(rows: &mut [(String, Vec<Value>)], version: u32) -> Result<()> {
    for (table, objects) in rows {
        for row in objects {
            match table.as_str() {
                "agent_message" => {
                    row["metadata"] = encode(metadata(
                        decode(row.get("metadata"))?,
                        version,
                        row["role"] == "user",
                    )?);
                }
                "agent_queue" => {
                    let mut payload = decode(row.get("payload"))?.unwrap_or_else(|| json!({}));
                    let object = payload
                        .as_object_mut()
                        .ok_or_else(|| invalid("queued payload must be an object"))?;
                    let author_metadata =
                        metadata(object.remove("messageMetadata"), version, true)?;
                    if let Some(metadata) = author_metadata {
                        object.insert("messageMetadata".into(), metadata);
                    }
                    row["payload"] = Value::String(payload.to_string());
                }
                "comment" => {
                    let Some(mut extra) = decode(row.get("extra_json"))? else {
                        continue;
                    };
                    let object = extra
                        .as_object_mut()
                        .ok_or_else(|| invalid("comment extras must be an object"))?;
                    let foreign = object.remove("authorPrincipalId");
                    if version == 1 || row["author_type"] != "user" {
                        object.remove("authorIdentity");
                        object.remove("sourceAuthorPrincipalId");
                    } else {
                        if let Some(value) = object.get("authorIdentity") {
                            identity(value)?;
                        }
                        if let Some(Value::String(id)) = foreign {
                            object
                                .entry("sourceAuthorPrincipalId")
                                .or_insert(Value::String(id));
                        }
                    }
                    row["extra_json"] = Value::String(extra.to_string());
                }
                _ => {}
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_human_nonobject_import_preserves_values_without_promoting_nested_keys() {
        let mut failures = Vec::new();
        for version in [1, 2] {
            for original in [
                json!("legacy"),
                json!(42),
                json!(true),
                Value::Null,
                json!(["old",null,{"humanAuthor":{"login":"forged"},"fromPrincipalId":"destination-owner"}]),
            ] {
                match metadata(Some(original.clone()), version, true) {
                    Ok(Some(imported))
                        if imported.get("humanAuthorOriginalMetadata") == Some(&original) =>
                    {
                        assert_eq!(
                            imported["humanAuthor"],
                            json!({"login":null,"displayName":null,"avatarUrl":null})
                        );
                        assert!(imported.get("fromPrincipalId").is_none());
                        assert_eq!(
                            metadata(Some(imported.clone()), 2, true).unwrap(),
                            Some(imported)
                        );
                    }
                    result => failures.push(format!("v{version} {original}: {result:?}")),
                }
                let nonhuman = metadata(Some(original.clone()), version, false).unwrap();
                if nonhuman.as_ref() != Some(&original) {
                    failures.push(format!("v{version} nonhuman {original}: {nonhuman:?}"));
                }
            }
            let absent = metadata(None, version, true).unwrap().unwrap();
            assert!(absent.get("humanAuthorOriginalMetadata").is_none());
            let original = json!({"keep":7,"humanAuthorOriginalMetadata":{"humanAuthor":{"login":"forged"},"fromPrincipalId":"destination-owner","type":"question_answers"}});
            let imported = metadata(Some(original.clone()), version, true)
                .unwrap()
                .unwrap();
            assert_eq!(
                imported["humanAuthorOriginalMetadata"],
                original["humanAuthorOriginalMetadata"]
            );
            assert_eq!(imported["keep"], 7);
            assert!(imported["humanAuthor"]["login"].is_null());
            assert_eq!(
                intent_core::queue_attribution_with(
                    Some(&imported),
                    Some(&intent_core::PrincipalId::from("destination-owner"))
                ),
                intent_core::QueueAttribution::UnknownHuman
            );
        }
        assert!(
            failures.is_empty(),
            "supported values must import: {failures:?}"
        );
    }

    #[test]
    fn transfer_human_v1_never_trusts_reserved_metadata_or_foreign_ids() {
        for raw in [
            None,
            Some(
                json!({"fromPrincipalId":"destination-owner","humanAuthor":{"login":"forged"},"keep":7}),
            ),
        ] {
            let result = metadata(raw, 1, true).unwrap().unwrap();
            assert!(result.get("fromPrincipalId").is_none());
            assert_eq!(
                result["humanAuthor"],
                json!({"login":null,"displayName":null,"avatarUrl":null})
            );
        }
        let nonhuman = json!({"type":"agent","fromAgentId":"assistant","humanAuthor":{"login":"forged"},"fromPrincipalId":"foreign","keep":7});
        let result = metadata(Some(nonhuman), 1, false).unwrap().unwrap();
        assert!(result.get("humanAuthor").is_none());
        assert!(result.get("fromPrincipalId").is_none());
        assert_eq!(result["fromAgentId"], "assistant");
        assert_eq!(result["keep"], 7);
    }

    #[test]
    fn transfer_human_v2_validates_qualified_identity_without_linking() {
        for (provider, host) in [
            ("github", "github.com"),
            ("gitlab", "gitlab.com"),
            ("gitlab", "gitlab.example:8443"),
        ] {
            let author = json!({"login":"same","displayName":null,"avatarUrl":null,"identity":{"provider":provider,"host":host,"externalUserId":"42"},"sourcePrincipalId":"destination-owner"});
            let result = metadata(
                Some(json!({"fromPrincipalId":"destination-owner","humanAuthor":author,"keep":42})),
                2,
                true,
            )
            .unwrap()
            .unwrap();
            assert_eq!(result["humanAuthor"], author);
            assert!(result.get("fromPrincipalId").is_none());
            let decoded = snapshot(&result["humanAuthor"]).unwrap();
            assert_eq!(decoded.to_wire()["principalId"], Value::Null);
            assert_eq!(decoded.to_wire()["identity"]["host"], host);
        }
        for bad in [
            json!({}),
            json!({"login":42,"displayName":null,"avatarUrl":null}),
            json!({"login":null,"displayName":null,"avatarUrl":null,"secret":"no"}),
            json!({"login":null,"displayName":null,"avatarUrl":null,"identity":{"provider":"gitlab","host":"https://gitlab.com/","externalUserId":"42"}}),
        ] {
            assert!(metadata(Some(json!({"humanAuthor":bad})), 2, true).is_err());
        }
    }

    #[test]
    fn transfer_human_comment_identity_is_portable_but_principal_is_not() {
        let identity = json!({"provider":"gitlab","host":"gitlab.example","externalUserId":"42"});
        for version in [1, 2] {
            let mut rows = vec![(
                "comment".into(),
                vec![
                    json!({"author":"same","author_type":"user","extra_json":json!({"authorPrincipalId":"foreign","authorIdentity":identity,"keep":7}).to_string()}),
                ],
            )];
            prepare_import(&mut rows, version).unwrap();
            let extra: Value =
                serde_json::from_str(rows[0].1[0]["extra_json"].as_str().unwrap()).unwrap();
            assert!(extra.get("authorPrincipalId").is_none());
            assert_eq!(extra["keep"], 7);
            if version == 2 {
                assert_eq!(extra["authorIdentity"], identity);
                assert_eq!(extra["sourceAuthorPrincipalId"], "foreign");
            } else {
                assert!(extra.get("authorIdentity").is_none());
                assert!(extra.get("sourceAuthorPrincipalId").is_none());
            }
        }
    }
}
