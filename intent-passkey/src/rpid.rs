//! Re-check that the RP ID is valid for the caller origin (`WebAuthn` L3 §5.1.3 step 8,
//! the rule Chromium applies): the RP ID equals the origin host or is a registrable
//! suffix of it, using the public suffix list. Main runs the same check first.

use url::{Host, Url};

use crate::protocol::{ErrorCode, PasskeyError};

fn security(message: impl Into<String>) -> PasskeyError {
    PasskeyError::new(ErrorCode::Security, message)
}

/// Returns the origin host once the origin is a secure-context origin we accept.
fn origin_host(origin: &str) -> Result<String, PasskeyError> {
    let url = Url::parse(origin).map_err(|_| security("origin is not a valid URL"))?;
    if url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || origin.trim_end_matches('/') != url.origin().ascii_serialization()
    {
        return Err(security(
            "origin must be a bare scheme://host[:port] origin",
        ));
    }
    let host = match url.host() {
        Some(Host::Domain(domain)) => domain.to_ascii_lowercase(),
        Some(Host::Ipv4(_) | Host::Ipv6(_)) => {
            return Err(security("IP-address origins cannot use passkeys"))
        }
        None => return Err(security("origin has no host")),
    };
    match url.scheme() {
        "https" => Ok(host),
        "http" if host == "localhost" => Ok(host),
        _ => Err(security("origin is not a secure context")),
    }
}

fn is_public_suffix(domain: &str) -> bool {
    psl::suffix_str(domain).is_none_or(|suffix| suffix == domain)
}

/// # Errors
/// `Security` when the origin is not acceptable or `rp_id` is not valid for it, and
/// `NotAllowed` for a cross-origin frame (`top_origin` differs), which v1 rejects.
pub fn check(origin: &str, top_origin: Option<&str>, rp_id: &str) -> Result<(), PasskeyError> {
    let host = origin_host(origin)?;
    if let Some(top) = top_origin {
        if origin_host(top).is_err() || top.trim_end_matches('/') != origin.trim_end_matches('/') {
            return Err(PasskeyError::new(
                ErrorCode::NotAllowed,
                "passkeys in cross-origin frames are not supported",
            ));
        }
    }
    if rp_id.is_empty()
        || !rp_id.is_ascii()
        || rp_id != rp_id.to_ascii_lowercase()
        || rp_id.starts_with('.')
        || rp_id.ends_with('.')
        || rp_id.parse::<std::net::IpAddr>().is_ok()
    {
        return Err(security("RP ID is not a lowercase ASCII domain"));
    }
    if rp_id == host {
        return Ok(());
    }
    let is_suffix = host
        .strip_suffix(rp_id)
        .is_some_and(|prefix| prefix.ends_with('.'));
    if !is_suffix {
        return Err(security(format!("RP ID {rp_id} is not valid for {host}")));
    }
    if is_public_suffix(rp_id) {
        return Err(security(format!("RP ID {rp_id} is a public suffix")));
    }
    Ok(())
}
