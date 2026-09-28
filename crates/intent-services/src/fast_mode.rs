//! Session-only Fast mode controls. The client does not negotiate boolean
//! config options, so the pinned adapters advertise their on/off selects.
use intent_acp::Connection;
use intent_core::{Error, Result};
use serde_json::Value;

/// An explicit list replaces the old metadata, including when a model change
/// removes Fast mode. Older adapters omitting the field preserve it.
pub(crate) fn refresh_options(options: &mut Option<Value>, response: Option<&Value>) {
    if let Some(value) = response
        .and_then(|r| r.get("configOptions"))
        .filter(|v| v.is_array())
    {
        *options = Some(value.clone());
    }
}

pub(crate) async fn apply(
    conn: &Connection,
    session_id: &str,
    provider: &str,
    enabled: bool,
    options: &mut Option<Value>,
) -> Result<()> {
    let Some(config_id) = intent_providers::find_provider(provider)
        .and_then(intent_providers::ProviderConfig::fast_mode_config_id)
    else {
        return Ok(());
    };
    // The adapter can switch models itself (/model or SDK fallback). Read the
    // ordered transport snapshot after model/effort setup at this turn boundary,
    // even if its notification has already been consumed by another router.
    if let Some(latest) = conn.session_config_options(session_id) {
        *options = Some(latest);
    }
    let option = options
        .as_ref()
        .and_then(Value::as_array)
        .and_then(|options| options.iter().find(|o| o["id"] == config_id));
    let Some(option) = option else {
        // Claude omits the option for ineligible models and rejects even off.
        // Keep the preference and re-check metadata at the next boundary.
        tracing::debug!(
            provider,
            enabled,
            "Fast mode unavailable for the selected model; using ordinary processing"
        );
        return Ok(());
    };
    let value = if enabled { "on" } else { "off" };
    let accepts = option
        .get("options")
        .and_then(Value::as_array)
        .is_some_and(|values| {
            values.iter().any(|v| {
                v["value"] == value
                    || v.get("options")
                        .and_then(Value::as_array)
                        .is_some_and(|group| group.iter().any(|v| v["value"] == value))
            })
        });
    if !accepts {
        return Err(Error::Internal(format!(
            "{provider}: Fast mode {value} is not an advertised session option"
        )));
    }
    let response =
        intent_acp::session::set_session_config_option_response(conn, session_id, config_id, value)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "{provider}: could not apply Fast mode {value}: {e}"
                ))
            })?;
    if let Some(actual) = response
        .get("configOptions")
        .and_then(Value::as_array)
        .and_then(|options| options.iter().find(|o| o["id"] == config_id))
        .and_then(|o| o.get("currentValue"))
    {
        if actual != value {
            return Err(Error::Internal(format!(
                "{provider}: provider did not confirm Fast mode {value}"
            )));
        }
    }
    refresh_options(options, Some(&response));
    Ok(())
}
