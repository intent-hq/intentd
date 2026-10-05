//! Strict, non-executing imports of the MCP sections in supported project files.

use super::{error, CatalogError, ExecutionOptions, NormalizedMcpServer};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Format {
    Pi,
    Augment,
    OpenCode,
    Grok,
    Factory,
    Codex,
    Common,
}

pub(super) const SOURCES: &[(&str, Format)] = &[
    (".pi/mcp.json", Format::Pi),
    (".augment/settings.json", Format::Augment),
    (".opencode/opencode.json", Format::OpenCode),
    (".opencode/opencode.jsonc", Format::OpenCode),
    ("opencode.json", Format::OpenCode),
    ("opencode.jsonc", Format::OpenCode),
    (".grok/config.toml", Format::Grok),
    (".factory/mcp.json", Format::Factory),
    (".codex/config.toml", Format::Codex),
    (".mcp.json", Format::Common),
];

pub(super) struct Definition {
    pub enabled: bool,
    pub server: Option<NormalizedMcpServer>,
    pub execution: ExecutionOptions,
}

pub(super) struct ProjectDefinition {
    pub definition: Definition,
    raw: Value,
    format: Format,
    source: String,
    directory: PathBuf,
}

impl ProjectDefinition {
    pub(super) fn resolve(
        &self,
        environment: &BTreeMap<String, String>,
        expanded_bytes: &Cell<usize>,
    ) -> Result<Definition, CatalogError> {
        parse_definition(
            &self.raw,
            &Context {
                format: self.format,
                source: &self.source,
                directory: &self.directory,
                environment,
                expanded_bytes,
                resolve: true,
            },
        )
    }
}

pub(super) fn parse(
    bytes: &[u8],
    format: Format,
    source: &str,
    directory: &Path,
) -> Result<BTreeMap<String, ProjectDefinition>, CatalogError> {
    let section = match format {
        Format::Codex | Format::Grok => "mcp_servers",
        Format::OpenCode => "mcp",
        _ => "mcpServers",
    };
    let value = if matches!(format, Format::Codex | Format::Grok) {
        let text =
            std::str::from_utf8(bytes).map_err(|_| error(source, "file", "invalid UTF-8"))?;
        let document = text
            .parse::<toml_edit::DocumentMut>()
            .map_err(|_| error(source, "file", "invalid TOML"))?;
        let Some(item) = document.get(section) else {
            return Ok(BTreeMap::new());
        };
        toml_item(item, 0)
            .ok_or_else(|| error(source, section, "unsupported TOML value or nesting"))?
    } else {
        let bytes = if Path::new(source)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("jsonc"))
        {
            jsonc(bytes).ok_or_else(|| error(source, "file", "invalid JSONC"))?
        } else {
            bytes.to_vec()
        };
        let value = serde_json::from_slice::<UniqueValue>(&bytes)
            .map_err(|_| error(source, "file", "invalid JSON or duplicate key"))?
            .0;
        let object = value
            .as_object()
            .ok_or_else(|| error(source, "file", "expected object"))?;
        let Some(value) = object.get(section) else {
            return Ok(BTreeMap::new());
        };
        value.clone()
    };
    let map = value
        .as_object()
        .ok_or_else(|| error(source, section, "expected server map"))?;
    if map.len() > super::MAX_SERVERS {
        return Err(error(source, section, "server limit exceeded"));
    }
    let environment = BTreeMap::new();
    let expanded_bytes = Cell::new(0);
    map.iter()
        .map(|(name, value)| {
            let context = Context {
                format,
                source,
                directory,
                environment: &environment,
                expanded_bytes: &expanded_bytes,
                resolve: false,
            };
            parse_definition(value, &context)
                .map(|definition| {
                    (
                        name.clone(),
                        ProjectDefinition {
                            definition,
                            raw: value.clone(),
                            format,
                            source: source.into(),
                            directory: directory.into(),
                        },
                    )
                })
                .map_err(|mut error| {
                    error.field = format!("{name}.{}", error.field);
                    error
                })
        })
        .collect()
}

struct Context<'a> {
    format: Format,
    source: &'a str,
    directory: &'a Path,
    environment: &'a BTreeMap<String, String>,
    expanded_bytes: &'a Cell<usize>,
    resolve: bool,
}

impl Context<'_> {
    fn err(&self, field: &str, reason: &'static str) -> CatalogError {
        error(self.source, field, reason)
    }

    fn bounded(&self, value: String) -> Result<String, CatalogError> {
        let total = self.expanded_bytes.get().saturating_add(value.len());
        if total > super::MAX_TOTAL_BYTES {
            return Err(self.err("expansion", "total expanded size limit exceeded"));
        }
        self.expanded_bytes.set(total);
        Ok(value)
    }

    fn expand(&self, input: &str, enabled: bool) -> Result<String, CatalogError> {
        if !enabled {
            return self.bounded(input.into());
        }
        let mut output = String::new();
        let mut rest = input;
        let marker = match self.format {
            Format::Common | Format::Augment => "${",
            Format::OpenCode => "{",
            _ => return self.bounded(input.into()),
        };
        while let Some(index) = rest.find(marker) {
            output.push_str(&rest[..index]);
            if self.format == Format::OpenCode
                && !rest[index..].starts_with("{env:")
                && !rest[index..].starts_with("{file:")
            {
                output.push('{');
                rest = &rest[index + 1..];
                continue;
            }
            rest = &rest[index + marker.len()..];
            let Some(end) = rest.find('}') else {
                return Err(self.err("expansion", "unterminated variable"));
            };
            let expression = &rest[..end];
            rest = &rest[end + 1..];
            let replacement = match self.format {
                Format::Augment if expression == "workspaceFolder" => {
                    self.directory.to_string_lossy().into_owned()
                }
                Format::Common => {
                    let (key, default) = expression
                        .split_once(":-")
                        .map_or((expression, None), |(key, default)| (key, Some(default)));
                    self.environment
                        .get(key)
                        .map(String::as_str)
                        .or(default)
                        .ok_or_else(|| self.err("expansion", "unresolved environment variable"))?
                        .into()
                }
                Format::OpenCode if expression.starts_with("file:") => {
                    return Err(self.err("expansion", "file includes are unsupported"))
                }
                Format::OpenCode if expression.starts_with("env:") => self
                    .environment
                    .get(&expression[4..])
                    .cloned()
                    .ok_or_else(|| self.err("expansion", "unresolved environment variable"))?,
                Format::Augment => {
                    return Err(self.err("expansion", "unsupported workspace variable"));
                }
                _ => {
                    // Not a recognized variable in this format; retain literally.
                    format!("{marker}{expression}}}")
                }
            };
            if output.len() + replacement.len() + rest.len() > super::MAX_SOURCE_BYTES {
                return Err(self.err("expansion", "expanded value size limit exceeded"));
            }
            output.push_str(&replacement);
        }
        output.push_str(rest);
        if output.len() > super::MAX_SOURCE_BYTES {
            return Err(self.err("expansion", "expanded value size limit exceeded"));
        }
        self.bounded(output)
    }
}

fn parse_definition(value: &Value, cx: &Context<'_>) -> Result<Definition, CatalogError> {
    let object = value
        .as_object()
        .ok_or_else(|| cx.err("server", "expected definition object"))?;
    let common = [
        "command", "args", "env", "url", "headers", "type", "enabled",
    ];
    let extra: &[&str] = match cx.format {
        Format::Codex | Format::Grok => &[
            "cwd",
            "env_vars",
            "http_headers",
            "env_http_headers",
            "bearer_token_env_var",
            "required",
            "enabled_tools",
            "disabled_tools",
            "startup_timeout_sec",
            "startup_timeout_ms",
            "tool_timeout_sec",
        ],
        Format::Factory => &["disabled", "disabledTools", "timeout", "connectTimeout"],
        Format::OpenCode => &["environment", "timeout"],
        _ => &[],
    };
    for key in object.keys() {
        if !common.contains(&key.as_str()) && !extra.contains(&key.as_str()) {
            return Err(cx.err(key, "unsupported MCP field"));
        }
    }
    let enabled = boolean(object, "enabled", cx)?.unwrap_or(true)
        && !boolean(object, "disabled", cx)?.unwrap_or(false);
    let active = enabled && cx.resolve;
    // Validate disabled's type even when enabled=false short-circuits above.
    boolean(object, "disabled", cx)?;
    let kind = string(object, "type", cx)?;
    let has_command = object.contains_key("command");
    let has_url = object.contains_key("url");
    if has_command && has_url {
        return Err(cx.err("transport", "conflicting command and URL"));
    }
    let stdio = match kind.as_deref() {
        None => has_command,
        Some("stdio") => true,
        Some("http" | "sse") => false,
        Some("local") if cx.format == Format::OpenCode => true,
        Some("remote") if cx.format == Format::OpenCode => false,
        _ => return Err(cx.err("type", "unsupported transport")),
    };
    if (stdio && has_url) || (!stdio && has_command) {
        return Err(cx.err("transport", "type conflicts with definition"));
    }
    validate_field_types(object, cx)?;
    let mut execution = execution_options(object, cx)?;
    if !has_command && !has_url {
        if enabled {
            return Err(cx.err("transport", "enabled definition requires command or URL"));
        }
        return Ok(Definition {
            enabled,
            server: None,
            execution,
        });
    }
    let server = if stdio {
        execution
            .cwd
            .get_or_insert_with(|| cx.directory.to_path_buf());
        for key in [
            "url",
            "headers",
            "http_headers",
            "env_http_headers",
            "bearer_token_env_var",
        ] {
            if object.contains_key(key) {
                return Err(cx.err(key, "field is not valid for stdio"));
            }
        }
        let (command, args) = if cx.format == Format::OpenCode {
            if object.contains_key("args") || object.contains_key("env") {
                return Err(cx.err("command", "OpenCode requires command array and environment"));
            }
            let mut command = strings(object, "command", cx)?
                .ok_or_else(|| cx.err("command", "missing command"))?;
            if command.is_empty() {
                return Err(cx.err("command", "empty command array"));
            }
            (command.remove(0), command)
        } else {
            (
                string(object, "command", cx)?
                    .ok_or_else(|| cx.err("command", "missing command"))?,
                strings(object, "args", cx)?.unwrap_or_default(),
            )
        };
        let command = cx.expand(&command, active)?;
        if command.trim().is_empty() {
            return Err(cx.err("command", "empty executable"));
        }
        let command = if command.contains('/') || command.contains('\\') {
            resolve_path(cx.directory, &command)
                .to_string_lossy()
                .into_owned()
        } else {
            command
        };
        let args = args
            .iter()
            .map(|arg| cx.expand(arg, active))
            .collect::<Result<Vec<_>, _>>()?;
        let env_key = if cx.format == Format::OpenCode {
            "environment"
        } else {
            "env"
        };
        let mut env = expand_map(
            string_map(object, env_key, cx)?.unwrap_or_default(),
            cx,
            active,
        )?;
        if let Some(keys) = strings(object, "env_vars", cx)? {
            for key in keys {
                if active && !env.contains_key(&key) {
                    let value = cx
                        .environment
                        .get(&key)
                        .ok_or_else(|| cx.err("env_vars", "unresolved environment variable"))?;
                    // Explicit env wins over inherited variables.
                    env.insert(key, cx.bounded(value.clone())?);
                }
            }
        }
        NormalizedMcpServer::Stdio { command, args, env }
    } else {
        for key in ["args", "env", "env_vars", "environment", "cwd"] {
            if object.contains_key(key) {
                return Err(cx.err(key, "field is not valid for remote transport"));
            }
        }
        let url = cx.expand(
            &string(object, "url", cx)?.ok_or_else(|| cx.err("url", "missing URL"))?,
            active,
        )?;
        if active
            && !reqwest::Url::parse(&url).is_ok_and(|url| {
                matches!(url.scheme(), "http" | "https") && url.host_str().is_some()
            })
        {
            return Err(cx.err("url", "expected HTTP or HTTPS URL"));
        }
        if object.contains_key("headers") && object.contains_key("http_headers") {
            return Err(cx.err("headers", "conflicting header maps"));
        }
        let header_key = if object.contains_key("http_headers") {
            "http_headers"
        } else {
            "headers"
        };
        let mut headers = expand_map(
            string_map(object, header_key, cx)?.unwrap_or_default(),
            cx,
            active,
        )?;
        if let Some(env_headers) = string_map(object, "env_http_headers", cx)? {
            for (header, variable) in env_headers {
                if active {
                    let value = cx.environment.get(&variable).ok_or_else(|| {
                        cx.err("env_http_headers", "unresolved environment variable")
                    })?;
                    insert_header(&mut headers, header, cx.bounded(value.clone())?, cx)?;
                }
            }
        }
        if let Some(variable) = string(object, "bearer_token_env_var", cx)? {
            if active {
                let value = cx.environment.get(&variable).ok_or_else(|| {
                    cx.err("bearer_token_env_var", "unresolved environment variable")
                })?;
                insert_header(
                    &mut headers,
                    "Authorization".into(),
                    cx.bounded(format!("Bearer {value}"))?,
                    cx,
                )?;
            }
        }
        let headers = (!headers.is_empty()).then_some(headers);
        if kind.as_deref() == Some("sse") {
            NormalizedMcpServer::Sse { url, headers }
        } else {
            NormalizedMcpServer::Http { url, headers }
        }
    };
    Ok(Definition {
        enabled,
        server: Some(server),
        execution,
    })
}

fn validate_field_types(object: &Map<String, Value>, cx: &Context<'_>) -> Result<(), CatalogError> {
    for field in ["url", "type", "cwd", "bearer_token_env_var"] {
        string(object, field, cx)?;
    }
    for field in ["enabled", "disabled", "required"] {
        boolean(object, field, cx)?;
    }
    for field in [
        "args",
        "env_vars",
        "enabled_tools",
        "disabled_tools",
        "disabledTools",
    ] {
        strings(object, field, cx)?;
    }
    for field in [
        "env",
        "environment",
        "headers",
        "http_headers",
        "env_http_headers",
    ] {
        string_map(object, field, cx)?;
    }
    if cx.format == Format::OpenCode {
        strings(object, "command", cx)?;
    } else {
        string(object, "command", cx)?;
    }
    Ok(())
}

fn insert_header(
    headers: &mut BTreeMap<String, String>,
    key: String,
    value: String,
    cx: &Context<'_>,
) -> Result<(), CatalogError> {
    if headers
        .keys()
        .any(|existing| existing.eq_ignore_ascii_case(&key))
    {
        return Err(cx.err("headers", "conflicting header definitions"));
    }
    headers.insert(key, value);
    Ok(())
}

fn execution_options(
    object: &Map<String, Value>,
    cx: &Context<'_>,
) -> Result<ExecutionOptions, CatalogError> {
    let cwd = string(object, "cwd", cx)?.map(|path| resolve_path(cx.directory, &path));
    if object.contains_key("startup_timeout_ms") && object.contains_key("startup_timeout_sec") {
        return Err(cx.err("startup_timeout", "conflicting timeout units"));
    }
    let mut startup_timeout_ms = duration(object, "startup_timeout_ms", 1, cx)?
        .or(duration(object, "startup_timeout_sec", 1000, cx)?)
        .or(duration(object, "connectTimeout", 1, cx)?);
    let mut tool_timeout_ms =
        duration(object, "tool_timeout_sec", 1000, cx)?.or(duration(object, "timeout", 1, cx)?);
    if cx.format == Format::OpenCode {
        startup_timeout_ms = tool_timeout_ms.take();
    }
    Ok(ExecutionOptions {
        cwd,
        startup_timeout_ms,
        tool_timeout_ms,
        required: boolean(object, "required", cx)?.unwrap_or(false),
        enabled_tools: strings(object, "enabled_tools", cx)?,
        disabled_tools: strings(object, "disabled_tools", cx)?
            .or(strings(object, "disabledTools", cx)?)
            .unwrap_or_default(),
    })
}

fn duration(
    object: &Map<String, Value>,
    field: &str,
    multiplier: u64,
    cx: &Context<'_>,
) -> Result<Option<u64>, CatalogError> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if let Some(integer) = value.as_u64() {
        return integer
            .checked_mul(multiplier)
            .map(Some)
            .ok_or_else(|| cx.err(field, "duration exceeds millisecond range"));
    }
    let number = value
        .as_f64()
        .ok_or_else(|| cx.err(field, "expected numeric duration"))?;
    let seconds = if multiplier == 1 {
        number / 1000.
    } else {
        number
    };
    let duration = std::time::Duration::try_from_secs_f64(seconds)
        .map_err(|_| cx.err(field, "invalid duration"))?;
    if duration.subsec_nanos() % 1_000_000 != 0 {
        return Err(cx.err(field, "duration must be representable in milliseconds"));
    }
    u64::try_from(duration.as_millis())
        .map(Some)
        .map_err(|_| cx.err(field, "duration exceeds millisecond range"))
}

fn boolean(
    object: &Map<String, Value>,
    field: &str,
    cx: &Context<'_>,
) -> Result<Option<bool>, CatalogError> {
    object
        .get(field)
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| cx.err(field, "expected boolean"))
        })
        .transpose()
}
fn string(
    object: &Map<String, Value>,
    field: &str,
    cx: &Context<'_>,
) -> Result<Option<String>, CatalogError> {
    object
        .get(field)
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| cx.err(field, "expected string"))
        })
        .transpose()
}
fn strings(
    object: &Map<String, Value>,
    field: &str,
    cx: &Context<'_>,
) -> Result<Option<Vec<String>>, CatalogError> {
    object
        .get(field)
        .map(|value| {
            value
                .as_array()
                .ok_or_else(|| cx.err(field, "expected string array"))?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| cx.err(field, "expected string array"))
                })
                .collect()
        })
        .transpose()
}
fn string_map(
    object: &Map<String, Value>,
    field: &str,
    cx: &Context<'_>,
) -> Result<Option<BTreeMap<String, String>>, CatalogError> {
    object
        .get(field)
        .map(|value| {
            value
                .as_object()
                .ok_or_else(|| cx.err(field, "expected string map"))?
                .iter()
                .map(|(key, value)| {
                    value
                        .as_str()
                        .map(|value| (key.clone(), value.into()))
                        .ok_or_else(|| cx.err(field, "expected string map"))
                })
                .collect()
        })
        .transpose()
}
fn expand_map(
    map: BTreeMap<String, String>,
    cx: &Context<'_>,
    enabled: bool,
) -> Result<BTreeMap<String, String>, CatalogError> {
    map.into_iter()
        .map(|(key, value)| cx.expand(&value, enabled).map(|value| (key, value)))
        .collect()
}

fn resolve_path(directory: &Path, path: &str) -> PathBuf {
    let path = directory.join(path);
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                clean.pop();
            }
            _ => clean.push(component),
        }
    }
    clean
}

pub(super) fn transport_value(server: &NormalizedMcpServer) -> Value {
    match server {
        NormalizedMcpServer::Stdio { command, args, env } => {
            serde_json::json!({"type":"stdio","command":command,"args":args,"env":env})
        }
        NormalizedMcpServer::Http { url, headers } => {
            serde_json::json!({"type":"http","url":url,"headers":headers})
        }
        NormalizedMcpServer::Sse { url, headers } => {
            serde_json::json!({"type":"sse","url":url,"headers":headers})
        }
    }
}

fn toml_item(item: &toml_edit::Item, depth: usize) -> Option<Value> {
    if depth > 64 {
        return None;
    }
    if let Some(table) = item.as_table_like() {
        return table
            .iter()
            .map(|(key, value)| toml_item(value, depth + 1).map(|value| (key.into(), value)))
            .collect::<Option<Map<_, _>>>()
            .map(Value::Object);
    }
    toml_value(item.as_value()?, depth)
}
fn toml_value(value: &toml_edit::Value, depth: usize) -> Option<Value> {
    if depth > 64 {
        return None;
    }
    match value {
        toml_edit::Value::String(v) => Some(Value::String(v.value().clone())),
        toml_edit::Value::Integer(v) => Some(Value::from(*v.value())),
        toml_edit::Value::Float(v) => serde_json::Number::from_f64(*v.value()).map(Value::Number),
        toml_edit::Value::Boolean(v) => Some(Value::Bool(*v.value())),
        toml_edit::Value::Array(v) => v
            .iter()
            .map(|v| toml_value(v, depth + 1))
            .collect::<Option<Vec<_>>>()
            .map(Value::Array),
        toml_edit::Value::InlineTable(v) => v
            .iter()
            .map(|(k, v)| toml_value(v, depth + 1).map(|v| (k.into(), v)))
            .collect::<Option<Map<_, _>>>()
            .map(Value::Object),
        toml_edit::Value::Datetime(_) => None,
    }
}

/// Only JSON comments and trailing commas, not arbitrary JavaScript/JSON5.
fn jsonc(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut out = bytes.to_vec();
    let mut i = 0;
    let mut quoted = false;
    while i < out.len() {
        match out[i] {
            b'\\' if quoted => i += 1,
            b'"' => quoted = !quoted,
            b'/' if !quoted && out.get(i + 1) == Some(&b'/') => {
                while i < out.len() && out[i] != b'\n' {
                    out[i] = b' ';
                    i += 1;
                }
                continue;
            }
            b'/' if !quoted && out.get(i + 1) == Some(&b'*') => {
                out[i] = b' ';
                out[i + 1] = b' ';
                i += 2;
                while i + 1 < out.len() && !(out[i] == b'*' && out[i + 1] == b'/') {
                    out[i] = b' ';
                    i += 1;
                }
                if i + 1 >= out.len() {
                    return None;
                }
                out[i] = b' ';
                out[i + 1] = b' ';
                i += 2;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    quoted = false;
    i = 0;
    while i < out.len() {
        match out[i] {
            b'\\' if quoted => i += 1,
            b'"' => quoted = !quoted,
            b',' if !quoted => {
                let next = out[i + 1..].iter().find(|c| !c.is_ascii_whitespace());
                let previous = out[..i].iter().rev().find(|c| !c.is_ascii_whitespace());
                if matches!(next, Some(b'}' | b']'))
                    && !matches!(previous, None | Some(b'[' | b'{' | b',' | b':'))
                {
                    out[i] = b' ';
                }
            }
            _ => {}
        }
        i += 1;
    }
    Some(out)
}

/// `serde_json::Value` silently replaces duplicate keys; configuration must not.
struct UniqueValue(Value);
impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueValue;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("JSON value with unique keys")
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(UniqueValue(v)) = seq.next_element()? {
                    values.push(v);
                }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some((key, UniqueValue(value))) =
                    map.next_entry::<String, UniqueValue>()?
                {
                    if values.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate key"));
                    }
                }
                Ok(UniqueValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}
