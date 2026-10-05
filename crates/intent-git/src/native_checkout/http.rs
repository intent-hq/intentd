//! Bounded smart HTTP v0 transport. libgit2's redirect NONE permits same-origin
//! redirects, including another installation prefix. Keep HTTP here with no
//! redirects/retries; libgit2 still validates, imports and builds Git packs.
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant, SystemTime};

use git2::{Oid, Repository};
use intent_core::{Error, Result};
use reqwest::blocking::{Body, Client, RequestBuilder, Response};
use reqwest::header::{HeaderMap, CONTENT_TYPE};

use super::{
    source_matches, unavailable, NativeCheckoutCredentials, NativeCheckoutSelection,
    NativeCheckoutSource,
};

const MAX_REFS_BYTES: u64 = 8 * 1024 * 1024;

struct Advertisement {
    heads: BTreeMap<String, Oid>,
    capabilities: BTreeSet<String>,
    default: Option<String>,
}

fn client() -> Result<Client> {
    let mut builder = Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .referer(false)
        .no_proxy()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(300));
    // Scope platform trust to this native client. A reqwest feature would
    // silently alter unrelated clients through Cargo feature unification.
    for certificate in rustls_native_certs::load_native_certs().certs {
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_der(certificate.as_ref()).map_err(|_| unavailable())?,
        );
    }
    builder.build().map_err(|_| unavailable())
}

fn backoff(headers: &HeaderMap, status: u16) -> Option<Instant> {
    let number = |name| headers.get(name)?.to_str().ok()?.parse::<u64>().ok();
    if status != 429 && number("ratelimit-remaining") != Some(0) {
        return None;
    }
    let reset = number("ratelimit-reset").and_then(|seconds| {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .map(|now| Duration::from_secs(seconds).saturating_sub(now))
    });
    let retry = (status == 429)
        .then(|| number("retry-after").map(Duration::from_secs))
        .flatten();
    reset
        .max(retry)
        .and_then(|delay| Instant::now().checked_add(delay))
}

fn send(
    client: &Client,
    request: RequestBuilder,
    source: &NativeCheckoutSource,
    credential: &mut dyn NativeCheckoutCredentials,
    push: bool,
) -> Result<Response> {
    let mut pending = Some(request);
    let mut prepared = None;
    credential.with_basic_auth(source.url(), &mut |username, password| {
        let request = pending.take().ok_or_else(unavailable)?;
        prepared = Some(
            request
                .basic_auth(username, Some(password))
                .build()
                .map_err(|_| unavailable())?,
        );
        Ok(())
    })?;
    // Admission transferred this one request. IO is outside all caller/P locks.
    let response = client
        .execute(prepared.ok_or_else(unavailable)?)
        .map_err(|_| if push { uncertain() } else { unavailable() })?;
    let status = response.status().as_u16();
    credential.observe(status, backoff(response.headers(), status));
    match status {
        200 => Ok(response),
        401 | 403 | 404 => Err(Error::GitAuthorization(
            "The original repository connection was refused".into(),
        )),
        429 => Err(Error::Internal(
            "The original repository connection is rate limited".into(),
        )),
        500..=599 if push => Err(uncertain()),
        // No response body/location can become another URL, credential or retry.
        _ => Err(unavailable()),
    }
}

fn packet(reader: &mut impl Read) -> Result<Option<Vec<u8>>> {
    let mut length = [0; 4];
    reader.read_exact(&mut length).map_err(|_| unavailable())?;
    let length = std::str::from_utf8(&length)
        .ok()
        .and_then(|s| usize::from_str_radix(s, 16).ok())
        .ok_or_else(unavailable)?;
    if length == 0 {
        return Ok(None);
    }
    if !(4..=65520).contains(&length) {
        return Err(unavailable());
    }
    let mut data = vec![0; length - 4];
    reader.read_exact(&mut data).map_err(|_| unavailable())?;
    Ok(Some(data))
}

fn write_packet(writer: &mut impl Write, data: &[u8]) -> Result<()> {
    if data.len() > 65516 {
        return Err(unavailable());
    }
    write!(writer, "{:04x}", data.len() + 4).map_err(|_| unavailable())?;
    writer.write_all(data).map_err(|_| unavailable())
}

fn content_type(response: &Response, expected: &str) -> Result<()> {
    if response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        != Some(expected)
    {
        return Err(unavailable());
    }
    Ok(())
}

fn discover(
    client: &Client,
    source: &NativeCheckoutSource,
    service: &str,
    credential: &mut dyn NativeCheckoutCredentials,
) -> Result<Advertisement> {
    let response = send(
        client,
        client.get(format!("{}/info/refs?service={service}", source.url())),
        source,
        credential,
        false,
    )?;
    content_type(&response, &format!("application/x-{service}-advertisement"))?;
    let mut reader = response.take(MAX_REFS_BYTES);
    if packet(&mut reader)?.as_deref() != Some(format!("# service={service}\n").as_bytes())
        || packet(&mut reader)?.is_some()
    {
        return Err(unavailable());
    }
    let mut heads = BTreeMap::new();
    let mut capabilities = BTreeSet::new();
    let mut first = true;
    while let Some(line) = packet(&mut reader)? {
        let line = std::str::from_utf8(&line)
            .map_err(|_| unavailable())?
            .trim_end_matches('\n');
        let reference = if first {
            first = false;
            let (reference, caps) = line.split_once('\0').ok_or_else(unavailable)?;
            capabilities.extend(caps.split(' ').map(String::from));
            reference
        } else {
            line
        };
        let (oid, name) = reference.split_once(' ').ok_or_else(unavailable)?;
        if oid.len() != 40 {
            return Err(unavailable());
        }
        let oid = Oid::from_str(oid).map_err(|_| unavailable())?;
        if let Some(branch) = name.strip_prefix("refs/heads/") {
            if oid.is_zero()
                || !git2::Reference::is_valid_name(name)
                || heads.insert(branch.to_owned(), oid).is_some()
            {
                return Err(unavailable());
            }
        }
    }
    if capabilities
        .iter()
        .any(|c| c.starts_with("object-format=") && c != "object-format=sha1")
    {
        return Err(unavailable());
    }
    let default = capabilities
        .iter()
        .find_map(|c| c.strip_prefix("symref=HEAD:refs/heads/"))
        .filter(|branch| heads.contains_key(*branch))
        .map(String::from);
    Ok(Advertisement {
        heads,
        capabilities,
        default,
    })
}

pub(super) fn fetch(
    repo: &Repository,
    source: &NativeCheckoutSource,
    branch: &str,
    expected: Option<&str>,
    credential: &mut dyn NativeCheckoutCredentials,
) -> Result<NativeCheckoutSelection> {
    let client = client()?;
    let advertisement = discover(&client, source, "git-upload-pack", credential)?;
    let oid = *advertisement.heads.get(branch).ok_or_else(unavailable)?;
    let selected = NativeCheckoutSelection::new(branch, &oid.to_string())?;
    if expected.is_some_and(|sha| sha != selected.commit_sha) {
        return Err(unavailable());
    }
    fetch_pack(repo, source, &client, &advertisement, credential)?;
    // Every published ref names an object from this exact advertisement/pack.
    // No filesystem/worktree action is performed inside credential admission.
    if !source_matches(repo, source) {
        return Err(unavailable());
    }
    let mut admitted = false;
    credential.with_current(&mut || {
        admitted = true;
        Ok(())
    })?;
    if !admitted {
        return Err(unavailable());
    }
    for (branch, oid) in &advertisement.heads {
        repo.find_commit(*oid).map_err(|_| unavailable())?;
        repo.reference(
            &format!("refs/remotes/origin/{branch}"),
            *oid,
            true,
            "qualified native fetch",
        )
        .map_err(|_| unavailable())?;
    }
    let mut refs = repo
        .references_glob("refs/remotes/origin/*")
        .map_err(|_| unavailable())?;
    for reference in &mut refs {
        let mut reference = reference.map_err(|_| unavailable())?;
        if let Some(branch) = reference
            .name()
            .ok()
            .and_then(|n| n.strip_prefix("refs/remotes/origin/"))
        {
            if branch != "HEAD" && !advertisement.heads.contains_key(branch) {
                reference.delete().map_err(|_| unavailable())?;
            }
        }
    }
    if let Some(default) = &advertisement.default {
        repo.reference_symbolic(
            "refs/remotes/origin/HEAD",
            &format!("refs/remotes/origin/{default}"),
            true,
            "observed remote HEAD",
        )
        .map_err(|_| unavailable())?;
    } else if let Ok(mut old) = repo.find_reference("refs/remotes/origin/HEAD") {
        old.delete().map_err(|_| unavailable())?;
    }
    Ok(selected)
}

fn fetch_pack(
    repo: &Repository,
    source: &NativeCheckoutSource,
    client: &Client,
    advertisement: &Advertisement,
    credential: &mut dyn NativeCheckoutCredentials,
) -> Result<()> {
    if !advertisement.capabilities.contains("side-band-64k") {
        return Err(unavailable());
    }
    let mut wants = Vec::new();
    let unique: BTreeSet<_> = advertisement.heads.values().copied().collect();
    for (index, oid) in unique.iter().enumerate() {
        let caps = if index == 0 {
            if advertisement.capabilities.contains("ofs-delta") {
                " side-band-64k ofs-delta"
            } else {
                " side-band-64k"
            }
        } else {
            ""
        };
        write_packet(&mut wants, format!("want {oid}{caps}\n").as_bytes())?;
    }
    if unique.is_empty() {
        return Err(unavailable());
    }
    wants.extend_from_slice(b"0000");
    write_packet(&mut wants, b"done\n")?;
    let mut response = send(
        client,
        client
            .post(format!("{}/git-upload-pack", source.url()))
            .header(CONTENT_TYPE, "application/x-git-upload-pack-request")
            .body(wants),
        source,
        credential,
        false,
    )?;
    content_type(&response, "application/x-git-upload-pack-result")?;
    if packet(&mut response)?.as_deref() != Some(b"NAK\n") {
        return Err(unavailable());
    }
    let odb = repo.odb().map_err(|_| unavailable())?;
    let mut pack = odb.packwriter().map_err(|_| unavailable())?;
    let mut wrote = false;
    while let Some(data) = packet(&mut response)? {
        match data.split_first() {
            Some((1, bytes)) => {
                pack.write_all(bytes).map_err(|_| unavailable())?;
                wrote = true;
            }
            Some((2, _)) => (), // Progress is not output/private server text.
            _ => return Err(unavailable()),
        }
    }
    if !wrote {
        return Err(unavailable());
    }
    pack.commit().map_err(|_| unavailable())?;
    Ok(())
}

fn local_head(repo: &Repository, branch: &str) -> Result<Oid> {
    let head = repo.head().map_err(|_| unavailable())?;
    if head.name().ok() != Some(format!("refs/heads/{branch}").as_str()) {
        return Err(unavailable());
    }
    head.target().ok_or_else(unavailable)
}

fn uncertain() -> Error {
    Error::Internal("Native push response was not confirmed; the remote outcome is unknown".into())
}

pub(super) fn push(
    repo: &Repository,
    source: &NativeCheckoutSource,
    branch: &str,
    force: bool,
    credential: &mut dyn NativeCheckoutCredentials,
) -> Result<NativeCheckoutSelection> {
    let new = local_head(repo, branch)?;
    let selection = NativeCheckoutSelection::new(branch, &new.to_string())?;
    let tracking_ref = format!("refs/remotes/origin/{branch}");
    let previous_tracking = match repo.find_reference(&tracking_ref) {
        Ok(reference) => reference.target().map(Some),
        Err(error) if error.code() == git2::ErrorCode::NotFound => Some(None),
        Err(_) => None,
    };
    let client = client()?;
    let advertised = discover(&client, source, "git-receive-pack", credential)?;
    if !advertised.capabilities.contains("report-status") {
        return Err(unavailable());
    }
    let old = advertised
        .heads
        .get(branch)
        .copied()
        .unwrap_or(Oid::ZERO_SHA1);
    if !force && !old.is_zero() && old != new {
        if repo.find_commit(old).is_err() {
            let upload = discover(&client, source, "git-upload-pack", credential)?;
            if upload.heads.get(branch) != Some(&old) {
                return Err(unavailable());
            }
            fetch_pack(repo, source, &client, &upload, credential)?;
        }
        if !repo
            .graph_descendant_of(new, old)
            .map_err(|_| unavailable())?
        {
            return Err(Error::InvalidParams(
                "The original branch requires an explicit force push".into(),
            ));
        }
    }
    let mut body = tempfile::tempfile_in(repo.path()).map_err(|_| unavailable())?;
    write_packet(
        &mut body,
        format!("{old} {new} refs/heads/{branch}\0report-status\n").as_bytes(),
    )?;
    body.write_all(b"0000").map_err(|_| unavailable())?;
    let mut walk = repo.revwalk().map_err(|_| unavailable())?;
    walk.push(new).map_err(|_| unavailable())?;
    for oid in advertised.heads.values() {
        if repo.find_commit(*oid).is_ok() {
            walk.hide(*oid).map_err(|_| unavailable())?;
        }
    }
    let mut pack = repo.packbuilder().map_err(|_| unavailable())?;
    pack.insert_walk(&mut walk).map_err(|_| unavailable())?;
    pack.foreach(|bytes| body.write_all(bytes).is_ok())
        .map_err(|_| unavailable())?;
    body.seek(SeekFrom::Start(0)).map_err(|_| unavailable())?;
    if local_head(repo, branch)? != new || !source_matches(repo, source) {
        return Err(unavailable());
    }
    let length = body.metadata().map_err(|_| unavailable())?.len();
    let mut response = send(
        &client,
        client
            .post(format!("{}/git-receive-pack", source.url()))
            .header(CONTENT_TYPE, "application/x-git-receive-pack-request")
            .body(Body::sized(body, length)),
        source,
        credential,
        true,
    )?;
    content_type(&response, "application/x-git-receive-pack-result").map_err(|_| uncertain())?;
    let unpack = packet(&mut response)
        .map_err(|_| uncertain())?
        .ok_or_else(uncertain)?;
    if unpack != b"unpack ok\n" {
        return Err(Error::Internal("Native push pack was refused".into()));
    }
    let status = packet(&mut response)
        .map_err(|_| uncertain())?
        .ok_or_else(uncertain)?;
    if status != format!("ok refs/heads/{branch}\n").as_bytes() {
        return Err(Error::Internal("Native push branch was refused".into()));
    }
    // This one requested ref is now confirmed; later caller retirement cannot
    // turn it into an unsent operation or trigger an automatic retry.
    // Admit local bookkeeping under the original operation, then perform IO
    // outside its metadata locks. Ref/admission failure cannot undo the ACK.
    if source_matches(repo, source) {
        let mut admitted = false;
        let current = credential.with_current(&mut || {
            admitted = true;
            Ok(())
        });
        if current.is_ok() && admitted {
            // Other worktrees share this ref. Publish only if it still has the
            // value captured before the push, including an initially absent ref.
            match previous_tracking {
                Some(Some(previous)) => {
                    let _ = repo.reference_matching(
                        &tracking_ref,
                        new,
                        true,
                        previous,
                        "confirmed native push",
                    );
                }
                Some(None) => {
                    let _ = repo.reference(&tracking_ref, new, false, "confirmed native push");
                }
                None => (),
            }
        }
    }
    Ok(selection)
}
