//! Octocrab's standard transport stack with accounting immediately above the
//! socket client, below retry/redirect. The pool/timeouts/auth policy are shared;
//! only the cheap buffered facade captures a task's attribution on each call.
use crate::{
    error::{Error, Result},
    github::{CONNECT_TIMEOUT, READ_WRITE_TIMEOUT},
    traffic::{self, Operation},
};
use http::{HeaderValue, Uri};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use octocrab::{
    service::middleware::{
        auth_header::AuthHeaderLayer, base_uri::BaseUriLayer, extra_headers::ExtraHeadersLayer,
        retry::RetryConfig,
    },
    OctoBody,
};
use std::sync::Arc;
use tower::{Layer, Service, ServiceExt};

type Pool =
    Client<hyper_timeout::TimeoutConnector<hyper_rustls::HttpsConnector<HttpConnector>>, OctoBody>;

pub(crate) struct Transport {
    pool: Pool,
    base: Uri,
    auth: Option<HeaderValue>,
}

impl Transport {
    pub fn new(token: Option<&str>, base: Option<&str>) -> Result<Self> {
        let base: Uri = base
            .unwrap_or("https://api.github.com")
            .parse()
            .map_err(|_| Error::Config("invalid github apiBaseUrl".into()))?;
        let auth = token
            .map(|t| HeaderValue::from_str(&format!("Bearer {t}")))
            .transpose()
            .map_err(|_| Error::Config("invalid github credential header".into()))?;
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .map_err(|_| Error::Config("cannot load TLS roots".into()))?
            .https_or_http()
            .enable_http1()
            .build();
        let mut connector = hyper_timeout::TimeoutConnector::new(connector);
        connector.set_connect_timeout(Some(CONNECT_TIMEOUT));
        connector.set_read_timeout(Some(READ_WRITE_TIMEOUT));
        connector.set_write_timeout(Some(READ_WRITE_TIMEOUT));
        Ok(Self {
            pool: Client::builder(TokioExecutor::new()).build(connector),
            base,
            auth,
        })
    }

    pub fn client(
        &self,
        quota_probe: bool,
        operation_override: Option<Operation>,
    ) -> octocrab::Octocrab {
        let pool = self.pool.clone();
        // Octocrab buffers requests on another task; capture before that hop.
        let context = traffic::context();
        let counted = tower::service_fn(move |request: http::Request<OctoBody>| {
            let mut pool = pool.clone();
            let mut context = context.clone();
            let (operation, graphql, page, continuation) =
                classify(request.uri(), request.method(), quota_probe);
            let operation = operation_override.unwrap_or(operation);
            let page = page && operation != Operation::Other && operation != Operation::QuotaProbe;
            context.continuation |= continuation;
            async move {
                context.start(operation, graphql, page);
                let result = pool.call(request).await;
                context.finish(
                    operation,
                    result.as_ref().ok().map(|r| (r.status(), r.headers())),
                );
                result
            }
        });
        // Match Octocrab's existing retry policy, including no retries for
        // the gate-owned quota probes. Every retry enters `counted` separately.
        let retry = tower::retry::Retry::new(
            if quota_probe {
                RetryConfig::None
            } else {
                RetryConfig::Simple(3)
            },
            counted,
        );
        let redirects = tower_http::follow_redirect::FollowRedirectLayer::new().layer(retry);
        let headers = ExtraHeadersLayer::new(Arc::new(vec![(
            http::header::USER_AGENT,
            HeaderValue::from_static("octocrab"),
        )]))
        .layer(redirects);
        let base = BaseUriLayer::new(self.base.clone()).layer(headers);
        let auth = AuthHeaderLayer::new(
            self.auth.clone(),
            self.base.clone(),
            "https://uploads.github.com".parse().unwrap(),
        )
        .layer(base);
        // The builder maps response bodies and transport errors just as the
        // default builder does; preserve the original concrete transport error.
        octocrab::OctocrabBuilder::new_empty()
            .with_service(
                auth.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) }),
            )
            .with_auth(octocrab::AuthState::None)
            .build()
            .unwrap()
    }
}

fn classify(uri: &Uri, method: &http::Method, probe: bool) -> (Operation, bool, bool, bool) {
    let path = uri.path();
    let graphql = path.ends_with("/graphql");
    let parts: Vec<_> = path.split('/').filter(|s| !s.is_empty()).collect();
    let suffix = parts
        .iter()
        .position(|p| *p == "repos")
        .and_then(|i| parts.get(i + 3..));
    let operation = if probe {
        Operation::QuotaProbe
    } else if graphql {
        Operation::PrDetail
    } else if *method != http::Method::GET {
        Operation::Other
    } else {
        match suffix {
            Some(["pulls"]) => Operation::Discovery,
            Some(["rules", "branches", ..] | ["branches", _, "protection", ..]) => Operation::Rules,
            Some(
                ["pulls", _, ..]
                | ["issues", _, "comments", ..]
                | ["commits", _, "check-runs" | "status", ..],
            ) => Operation::PrDetail,
            _ => Operation::Other,
        }
    };
    let page = uri
        .query()
        .and_then(|q| q.split('&').find_map(|p| p.strip_prefix("page=")))
        .and_then(|v| v.parse::<u64>().ok());
    (
        operation,
        graphql,
        graphql || page.is_some(),
        page.is_some_and(|p| p > 1),
    )
}
