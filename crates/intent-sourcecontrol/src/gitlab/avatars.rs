//! Optional display metadata from the already-admitted project response.
use reqwest::Url;
use serde_json::Value;

use crate::GitlabInstance;

const MAX_AVATAR_URL_BYTES: usize = 8192;

pub(super) fn owner_avatar(
    instance: &GitlabInstance,
    project: &Value,
    owner: &str,
) -> Option<String> {
    let namespace = project.get("namespace")?;
    if namespace.get("full_path")?.as_str()? != owner {
        return None;
    }
    let raw = namespace.get("avatar_url")?.as_str()?;
    if raw.is_empty()
        || raw.len() > MAX_AVATAR_URL_BYTES
        || raw
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || c == '\\')
    {
        return None;
    }
    // Resolve against logical identity, never the API endpoint (which can be a
    // transport override). Treat an installation prefix as a directory.
    let root = Url::parse(&format!("{}/", instance.as_str().trim_end_matches('/'))).ok()?;
    let url = root.join(raw).ok()?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.as_str().len() > MAX_AVATAR_URL_BYTES
    {
        return None;
    }
    // Do not repair malformed absolute forms such as `https:host/image`.
    // Scheme-relative references still carry their own explicit authority.
    let authority = if let Some((scheme, rest)) = raw.split_once(':') {
        if scheme.eq_ignore_ascii_case("https") {
            Some(rest.strip_prefix("//")?)
        } else if Url::parse(raw).is_ok() {
            return None;
        } else {
            raw.strip_prefix("//")
        }
    } else {
        raw.strip_prefix("//")
    };
    if let Some(authority) = authority {
        let authority = authority.split(['/', '?', '#']).next()?;
        if authority.is_empty() || authority.contains('@') {
            return None;
        }
    }
    Some(url.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn project(avatar: &Value) -> Value {
        json!({
            "path_with_namespace":"Team/Sub/Project",
            "avatar_url":"https://images.example/project.png",
            "namespace":{"full_path":"Team/Sub","avatar_url":avatar}
        })
    }

    #[test]
    fn owner_images_preserve_logical_origin_prefix_and_explicit_cdn_locations() {
        let instance = GitlabInstance::parse("https://forge.test:8443/install").unwrap();
        for (raw, expected) in [
            (
                "uploads/owner.png",
                "https://forge.test:8443/install/uploads/owner.png",
            ),
            (
                "/uploads/owner.png",
                "https://forge.test:8443/uploads/owner.png",
            ),
            (
                "https://cdn.example:9443/avatar.png?size=80#image",
                "https://cdn.example:9443/avatar.png?size=80#image",
            ),
            ("//cdn.example/avatar.png", "https://cdn.example/avatar.png"),
        ] {
            let repo = super::super::to_repo(&instance, project(&json!(raw))).unwrap();
            assert_eq!(repo.owner_avatar_url.as_deref(), Some(expected), "{raw}");
            // This internal metadata must not leak through legacy Repo serializers.
            assert_eq!(
                serde_json::to_value(repo).unwrap(),
                json!({"owner":"Team/Sub","name":"Project"})
            );
        }
        let public = GitlabInstance::parse("https://gitlab.com").unwrap();
        assert_eq!(
            owner_avatar(&public, &project(&json!("uploads/avatar.png")), "Team/Sub").as_deref(),
            Some("https://gitlab.com/uploads/avatar.png")
        );
    }

    #[test]
    fn absent_malformed_or_other_namespace_metadata_never_becomes_an_owner_image() {
        let instance = GitlabInstance::parse("https://forge.test:8443/install").unwrap();
        for avatar in [
            Value::Null,
            json!(3),
            json!({"url":"https://images.example/a"}),
            json!(""),
        ] {
            assert_eq!(
                super::super::to_repo(&instance, project(&avatar))
                    .unwrap()
                    .owner_avatar_url,
                None
            );
        }
        for namespace in [
            Value::Null,
            json!("Team/Sub"),
            json!({}),
            json!({"full_path":"Other/Sub","avatar_url":"/owner.png"}),
            json!({"full_path":"Team/Sub/Project","avatar_url":"/owner.png"}),
        ] {
            let mut value = project(&json!("/owner.png"));
            value["namespace"] = namespace;
            assert_eq!(
                super::super::to_repo(&instance, value)
                    .unwrap()
                    .owner_avatar_url,
                None
            );
        }
        let mut value = project(&json!("/owner.png"));
        value.as_object_mut().unwrap().remove("namespace");
        value["user"] = json!({"avatar_url":"https://images.example/current-user.png"});
        assert_eq!(
            super::super::to_repo(&instance, value)
                .unwrap()
                .owner_avatar_url,
            None
        );
    }

    #[test]
    fn invalid_image_locations_are_omitted_without_rejecting_the_project() {
        let instance = GitlabInstance::parse("https://forge.test:8443/install").unwrap();
        for raw in [
            "http://images.example/a.png",
            "data:image/png;base64,AAAA",
            "file:///tmp/a.png",
            "javascript:alert(1)",
            "https://user:secret@images.example/a.png",
            "//user@images.example/a.png",
            "https://@images.example/a.png",
            "https:images.example/a.png",
            "https:///images.example/a.png",
            "https://",
            "https://images.example/with space.png",
            "https://images.example/a\n.png",
            "https://images.example\\a.png",
            " https://images.example/a.png",
        ] {
            let repo = super::super::to_repo(&instance, project(&json!(raw))).unwrap();
            assert_eq!(repo.owner_avatar_url, None, "{raw}");
            assert_eq!(repo.owner, "Team/Sub");
        }
        let oversized = format!("/{}", "a".repeat(MAX_AVATAR_URL_BYTES));
        assert_eq!(
            owner_avatar(&instance, &project(&json!(oversized)), "Team/Sub"),
            None
        );
        let expanded = format!("/{}", "a".repeat(MAX_AVATAR_URL_BYTES - 1));
        assert_eq!(
            owner_avatar(&instance, &project(&json!(expanded)), "Team/Sub"),
            None
        );
    }
}
