//! Octocrab's standard transport stack with accounting immediately above the
//! socket client, below retry/redirect. The pool/timeouts/auth policy are shared;
//! only the cheap buffered facade captures a task's attribution on each call.
use crate::{
    error::{Error, Result},
    github::{CONNECT_TIMEOUT, READ_WRITE_TIMEOUT},
    traffic::{self, Operation},
};
use http::{HeaderValue, Uri};
use http_body_util::BodyExt;
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
        Self::with_timeouts(token, base, CONNECT_TIMEOUT, READ_WRITE_TIMEOUT)
    }

    fn with_timeouts(
        token: Option<&str>,
        base: Option<&str>,
        connect_timeout: std::time::Duration,
        read_write_timeout: std::time::Duration,
    ) -> Result<Self> {
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
        connector.set_connect_timeout(Some(connect_timeout));
        connector.set_read_timeout(Some(read_write_timeout));
        connector.set_write_timeout(Some(read_write_timeout));
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
        let graphql_path = format!("{}/graphql", self.base.path().trim_end_matches('/'));
        // Octocrab buffers requests on another task; capture before that hop.
        let context = traffic::context();
        let counted = tower::service_fn(move |request: http::Request<OctoBody>| {
            let mut pool = pool.clone();
            let mut context = context.clone();
            let (operation, graphql, page, continuation) =
                classify(request.uri(), request.method(), quota_probe, &graphql_path);
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
                result.map(|response| {
                    response.map(|body| {
                        // Bodies are consumed after the service future (and often
                        // its caller scope) ends. Observe errors without buffering
                        // or turning a failed body into another HTTP attempt.
                        let mut reported = false;
                        body.map_err(move |error| {
                            if !reported {
                                context.body_failed(operation);
                                reported = true;
                            }
                            error
                        })
                    })
                })
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

fn classify(
    uri: &Uri,
    method: &http::Method,
    probe: bool,
    graphql_path: &str,
) -> (Operation, bool, bool, bool) {
    let path = uri.path();
    let graphql = path == graphql_path;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traffic::{with_caller, with_traffic, Caller, Traffic};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    async fn request(stream: &mut TcpStream) -> Option<String> {
        let mut bytes = Vec::new();
        loop {
            let mut byte = [0];
            if stream.read(&mut byte).await.ok()? == 0 {
                return None;
            }
            bytes.push(byte[0]);
            if bytes.ends_with(b"\r\n\r\n") {
                return String::from_utf8(bytes).ok();
            }
        }
    }

    #[tokio::test]
    async fn caller_facades_share_connections_and_preserve_headers() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let server = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            while let Ok((mut stream, _)) = listener.accept().await {
                count.fetch_add(1, Ordering::SeqCst);
                children.spawn(async move {
                    while let Some(head) = request(&mut stream).await {
                        assert!(head.contains("authorization: Bearer test-secret"));
                        assert!(head.contains("user-agent: octocrab"));
                        stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\ncontent-type: application/json\r\n\r\n[]").await.unwrap();
                    }
                });
            }
        });
        let transport = Transport::new(Some("test-secret"), Some(&base)).unwrap();
        let traffic = Traffic::default();
        with_traffic(traffic.clone(), async {
            for caller in [
                Caller::PrMonitor,
                Caller::WorkspaceRefresh,
                Caller::GitRootRefresh,
            ] {
                with_caller(caller, async {
                    let _: serde_json::Value = transport
                        .client(false, None)
                        .get("/repos/o/r/pulls", None::<&()>)
                        .await
                        .unwrap();
                })
                .await;
            }
        })
        .await;
        assert_eq!(
            connections.load(Ordering::SeqCst),
            1,
            "facades must reuse the shared pool"
        );
        assert_eq!(
            traffic.snapshot().counts.len(),
            3,
            "attribution remains caller-scoped"
        );
        server.abort();
    }

    #[tokio::test]
    async fn a_stalled_http_read_still_times_out_and_counts_every_retry() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut connections = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                connections.push(stream);
            }
        });
        let transport = Transport::with_timeouts(
            None,
            Some(&base),
            Duration::from_secs(1),
            Duration::from_millis(25),
        )
        .unwrap();
        let traffic = Traffic::default();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            with_traffic(traffic.clone(), async {
                transport
                    .client(false, None)
                    .get::<serde_json::Value, _, ()>("/repos/o/r/pulls", None)
                    .await
            }),
        )
        .await
        .expect("socket read timeout must end the request");
        server.abort();
        assert!(result.is_err());
        let snapshot = traffic.snapshot();
        let counts = &snapshot.counts[&(Caller::OnDemand, Operation::Discovery)];
        assert_eq!(counts.rest_requests, 4);
        assert_eq!(counts.transport_errors, 4);
    }
    async fn body_failure_keeps_captured_context(stall: bool) {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let server = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            while let Ok((mut stream, _)) = listener.accept().await {
                count.fetch_add(1, Ordering::SeqCst);
                children.spawn(async move {
                    request(&mut stream).await.unwrap();
                    stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 50\r\ncontent-type: application/json\r\nx-ratelimit-resource: core\r\n\r\n[").await.unwrap();
                    if stall { std::future::pending::<()>().await; }
                });
            }
        });
        let transport = Transport::with_timeouts(
            None,
            Some(&base),
            Duration::from_secs(1),
            Duration::from_millis(50),
        )
        .unwrap();
        let traffic = Traffic::default();
        let response = with_traffic(
            traffic.clone(),
            with_caller(Caller::WorkspaceRefresh, async {
                transport
                    .client(false, None)
                    ._get_with_headers("/repos/o/r/pulls?page=1", None)
                    .await
                    .unwrap()
            }),
        )
        .await;
        // Consumption happens after both task-local scopes have exited.
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            transport.client(false, None).body_to_string(response),
        )
        .await
        .expect("body read timeout remains bounded");
        server.abort();
        assert!(error.is_err());
        assert_eq!(
            connections.load(Ordering::SeqCst),
            1,
            "body IO errors must not retry the request"
        );
        let snapshot = traffic.snapshot();
        assert_eq!(snapshot.counts.len(), 1);
        let counts = &snapshot.counts[&(Caller::WorkspaceRefresh, Operation::Discovery)];
        assert_eq!(
            (
                counts.rest_requests,
                counts.page_requests,
                counts.transport_errors
            ),
            (1, 1, 1)
        );
        assert_eq!((counts.http_errors, counts.graphql_errors), (0, 0));
        assert_eq!(
            snapshot.quotas.values().map(|q| q.responses).sum::<u64>(),
            1
        );
    }

    #[tokio::test]
    async fn traffic_truncated_body_counts_one_captured_transport_error() {
        body_failure_keeps_captured_context(false).await;
    }

    #[tokio::test]
    async fn traffic_stalled_body_counts_one_error_without_retrying() {
        body_failure_keeps_captured_context(true).await;
    }
}
