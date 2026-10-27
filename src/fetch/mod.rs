//! Fetch layer: runs the `gh` CLI as a subprocess and decodes JSON.

pub mod check_history;
pub mod continuation;
pub mod git_files;
pub mod issues;
pub mod pull_requests;
pub mod rate_limit;
pub mod user_search;

use std::process::Command;
use std::thread::sleep;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::error::{Error, Result};
use rate_limit::RateLimitTracker;

/// Abstraction over the `gh` subprocess so tests can substitute fixtures.
pub trait GhTransport: Send + Sync {
    /// Run `gh` with the given arguments and return stdout on success.
    fn run(&self, arguments: &[String]) -> Result<String>;
}

/// Real transport: shells out to the `gh` binary on PATH.
pub struct GhCommandLine;

impl GhTransport for GhCommandLine {
    fn run(&self, arguments: &[String]) -> Result<String> {
        let output = Command::new("gh")
            .args(arguments)
            .output()
            .map_err(|error| Error::GhUnavailable(format!("failed to run gh: {error}")))?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(Error::GhCommand {
                exit_code: output.status.code(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            })
        }
    }
}

/// Maximum retry attempts for retryable errors.
const RETRY_ATTEMPTS: u32 = 7;
/// Exponential backoff schedule in seconds (attempt N sleeps SCHEDULE[N-1]).
const BACKOFF_SECONDS: [u64; 6] = [2, 4, 8, 16, 32, 64];

/// Minimum spacing between GitHub *search* calls.
///
/// Search has its own budget — 30 requests per minute for an authenticated
/// user — an order of magnitude tighter than the 5,000 points/hour the
/// repository path spends on ordinary GraphQL. The observed `rateLimit` block
/// tracks the latter and says nothing about the former, so search is paced on
/// its own clock: 2.1s between calls leaves ~28 requests/minute, under the cap
/// with room for clock skew.
const SEARCH_MIN_INTERVAL: Duration = Duration::from_millis(2100);

/// Client for the GitHub API via the `gh` CLI.
pub struct GhClient {
    transport: Box<dyn GhTransport>,
    rate_limit: RateLimitTracker,
    /// Optional pacing delay between requests (bulk-sync politeness).
    inter_request_delay: Option<Duration>,
    /// When false (tests), never actually sleep.
    sleeping_enabled: bool,
    /// When the last search call was issued, for search-specific pacing.
    last_search_at: Option<Instant>,
    /// Requests (attempts, retries included) this client may still issue
    /// before refusing locally with [`Error::QuotaReserve`]. `None` = no cap.
    request_allowance: Option<u64>,
    /// Requests (attempts, retries included) issued over the transport.
    requests_issued: u64,
    /// When set, every call reads the provider's `x-ratelimit-*` response
    /// headers and refuses locally before spending into the reserve.
    quota_guard: Option<QuotaGuard>,
}

/// Rate-limit state observed from data responses (never from the separate
/// `rate_limit` endpoint, which was seen reporting a different bucket than
/// the data calls draw from on the same token).
#[derive(Debug, Clone)]
struct QuotaGuard {
    reserve: i64,
    /// resource (`core`, `graphql`) -> (remaining, reset epoch).
    observed: std::collections::BTreeMap<String, (i64, i64)>,
}

/// Rate-limit facts parsed from one response's headers. Only these values are
/// read; no other header (and never an auth header) is kept or logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitHeaders {
    pub resource: String,
    pub remaining: i64,
    pub reset: i64,
}

/// Split `gh api --include` output into its rate-limit headers and body.
/// Returns `(None, raw)` when the output carries no status line. Header
/// names are matched case-insensitively; a present but unparsable value is
/// reported as `Err` so a caller enforcing a budget can fail closed.
pub fn split_included(raw: &str) -> Result<(Option<RateLimitHeaders>, &str)> {
    if !raw.starts_with("HTTP/") {
        return Ok((None, raw));
    }
    let (head, body) = match raw.find("\r\n\r\n") {
        Some(index) => (&raw[..index], &raw[index + 4..]),
        None => match raw.find("\n\n") {
            Some(index) => (&raw[..index], &raw[index + 2..]),
            None => (raw, ""),
        },
    };
    let mut remaining = None;
    let mut reset = None;
    let mut resource = None;
    for line in head.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "x-ratelimit-remaining" => remaining = Some(value),
            "x-ratelimit-reset" => reset = Some(value),
            "x-ratelimit-resource" => resource = Some(value),
            _ => {}
        }
    }
    let headers = match (remaining, reset, resource) {
        (None, None, None) => None,
        (Some(remaining), Some(reset), Some(resource)) => {
            let parse = |value: &str, what: &str| {
                value.parse::<i64>().map_err(|_| {
                    Error::QuotaReserve(format!("malformed x-ratelimit-{what} header"))
                })
            };
            if resource.is_empty()
                || !resource
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
            {
                return Err(Error::QuotaReserve(
                    "malformed x-ratelimit-resource header".into(),
                ));
            }
            Some(RateLimitHeaders {
                resource: resource.to_ascii_lowercase(),
                remaining: parse(remaining, "remaining")?,
                reset: parse(reset, "reset")?,
            })
        }
        _ => {
            return Err(Error::QuotaReserve(
                "incomplete x-ratelimit-* headers".into(),
            ));
        }
    };
    Ok((headers, body))
}

impl GhClient {
    /// Create a client over the real `gh` binary.
    pub fn new() -> Self {
        Self::with_transport(Box::new(GhCommandLine))
    }

    /// Create a client over a custom transport (fixtures in tests).
    pub fn with_transport(transport: Box<dyn GhTransport>) -> Self {
        Self {
            transport,
            rate_limit: RateLimitTracker::new(),
            inter_request_delay: None,
            sleeping_enabled: true,
            last_search_at: None,
            request_allowance: None,
            requests_issued: 0,
            quota_guard: None,
        }
    }

    /// Enforce a quota reserve from the provider's own response headers:
    /// every call is issued with `--include`, its `x-ratelimit-*` headers
    /// update the observed remaining quota per resource, and a call is
    /// refused locally ([`Error::QuotaReserve`]) when one more attempt would
    /// reach the reserve. Before anything is observed for a resource, a
    /// single attempt (no retry) is allowed to learn it; a success without
    /// usable rate-limit headers fails closed. Best-effort only: other
    /// consumers of the same token can spend between two responses, so this
    /// is never an atomic reservation.
    pub fn enforce_quota_reserve(&mut self, reserve: i64) {
        self.quota_guard = Some(QuotaGuard {
            reserve,
            observed: Default::default(),
        });
    }

    /// Requests left above the reserve for `resource`, when observed.
    pub fn quota_headroom(&self, resource: &str) -> Option<i64> {
        let guard = self.quota_guard.as_ref()?;
        guard
            .observed
            .get(resource)
            .map(|(remaining, _)| remaining - guard.reserve)
    }

    /// Last observed `(remaining, reset)` for `resource`.
    pub fn observed_quota(&self, resource: &str) -> Option<(i64, i64)> {
        self.quota_guard.as_ref()?.observed.get(resource).copied()
    }

    /// Cap how many more requests (every attempt, retries included) this
    /// client may issue. Past the cap a call fails with
    /// [`Error::QuotaReserve`] without reaching the provider. `None` lifts it.
    pub fn set_request_allowance(&mut self, allowance: Option<u64>) {
        self.request_allowance = allowance;
    }

    /// Requests this client may still issue, when capped.
    pub fn request_allowance(&self) -> Option<u64> {
        self.request_allowance
    }

    /// Requests issued over the transport so far (every attempt).
    pub fn requests_issued(&self) -> u64 {
        self.requests_issued
    }

    /// Disable real sleeping (test fixtures).
    pub fn without_sleeping(mut self) -> Self {
        self.sleeping_enabled = false;
        self
    }

    /// Set an inter-request pacing delay.
    pub fn with_inter_request_delay(mut self, delay: Duration) -> Self {
        self.inter_request_delay = Some(delay);
        self
    }

    /// Verify `gh` is installed and authenticated. Call once at startup.
    pub fn preflight(&self) -> Result<()> {
        self.transport
            .run(&["auth".into(), "status".into()])
            .map(|_| ())
            .map_err(|error| {
                Error::GhUnavailable(format!(
                    "`gh auth status` failed — install the GitHub CLI and run `gh auth login` \
                     (set GH_HOST for GitHub Enterprise): {error}"
                ))
            })
    }

    /// Execute a GraphQL query with string variables, returning the `data` value.
    pub fn graphql(&mut self, query: &str, variables: &[(&str, &str)]) -> Result<Value> {
        let mut arguments: Vec<String> = vec![
            "api".into(),
            "graphql".into(),
            "-f".into(),
            format!("query={query}"),
        ];
        for (name, value) in variables {
            arguments.push("-F".into());
            arguments.push(format!("{name}={value}"));
        }
        let response: Value = self.run_with_retry(&arguments)?;
        if let Some(errors) = response.get("errors").and_then(Value::as_array)
            && !errors.is_empty()
        {
            let joined = errors
                .iter()
                .map(|error| {
                    error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown GraphQL error")
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join("; ");
            return Err(Error::GitHubApi(joined));
        }
        let data = response
            .get("data")
            .cloned()
            .ok_or_else(|| Error::GitHubApi("response has no data field".into()))?;
        self.observe_rate_limit(&data);
        Ok(data)
    }

    /// Execute a GraphQL query against the **search** connection.
    ///
    /// Identical to [`Self::graphql`] except it first waits out a minimum
    /// interval since the previous search call, so a long
    /// user-scoped walk stays inside search's own 30-requests-per-minute budget
    /// instead of tripping a secondary rate limit halfway through.
    pub fn graphql_search(&mut self, query: &str, variables: &[(&str, &str)]) -> Result<Value> {
        if let Some(previous) = self.last_search_at {
            let elapsed = previous.elapsed();
            if elapsed < SEARCH_MIN_INTERVAL && self.sleeping_enabled {
                sleep(SEARCH_MIN_INTERVAL - elapsed);
            }
        }
        self.last_search_at = Some(Instant::now());
        self.graphql(query, variables)
    }

    /// Execute a REST call (`gh api <path>`), returning the parsed JSON.
    pub fn rest(&mut self, path: &str) -> Result<Value> {
        let arguments: Vec<String> = vec!["api".into(), path.into()];
        self.run_with_retry(&arguments)
    }

    fn run_with_retry(&mut self, arguments: &[String]) -> Result<Value> {
        if let Some(delay) = self.inter_request_delay
            && self.sleeping_enabled
        {
            sleep(delay);
        }
        // Primary rate limit: sleep until reset before issuing the call.
        if let Some(wait) = self.rate_limit.wait_needed()
            && self.sleeping_enabled
        {
            sleep(wait);
        }
        let mut attempt: u32 = 0;
        // With a quota guard: which bucket this call draws from, and the
        // arguments with `--include` so the response headers come back.
        let resource = if arguments.get(1).map(String::as_str) == Some("graphql") {
            "graphql"
        } else {
            "core"
        };
        let included: Vec<String>;
        let sent: &[String] = if self.quota_guard.is_some() {
            let mut with_headers = Vec::with_capacity(arguments.len() + 1);
            with_headers.push(arguments[0].clone());
            with_headers.push("--include".to_string());
            with_headers.extend(arguments[1..].iter().cloned());
            included = with_headers;
            &included
        } else {
            arguments
        };
        loop {
            // A first call against an unobserved bucket is a single probe.
            let mut probe = false;
            if let Some(guard) = &mut self.quota_guard {
                match guard.observed.get_mut(resource) {
                    Some((remaining, reset)) => {
                        if *remaining - 1 < guard.reserve {
                            return Err(Error::QuotaReserve(format!(
                                "{resource} quota {remaining} would reach reserve {} \
                                 (resets at epoch {reset}); not sent",
                                guard.reserve
                            )));
                        }
                        // Count the attempt now: an error response carries no
                        // headers we can read, so assume it was charged.
                        *remaining -= 1;
                    }
                    None if attempt == 0 => probe = true,
                    None => {
                        return Err(Error::QuotaReserve(format!(
                            "{resource} quota unknown after a failed probe; not retried"
                        )));
                    }
                }
            }
            if let Some(allowance) = self.request_allowance {
                if allowance == 0 {
                    return Err(Error::QuotaReserve(format!(
                        "request allowance spent before `gh {}`",
                        arguments.get(1).map(String::as_str).unwrap_or_default()
                    )));
                }
                self.request_allowance = Some(allowance - 1);
            }
            self.requests_issued += 1;
            match self.transport.run(sent) {
                Ok(stdout) => {
                    let Some(guard) = &mut self.quota_guard else {
                        return serde_json::from_str(&stdout).map_err(Error::from);
                    };
                    let (headers, body) = split_included(&stdout)?;
                    let headers = headers.ok_or_else(|| {
                        Error::QuotaReserve(format!(
                            "{resource} response carried no rate-limit headers; failing closed"
                        ))
                    })?;
                    if headers.resource != resource {
                        return Err(Error::QuotaReserve(format!(
                            "response drew from the {} bucket, expected {resource}; failing closed",
                            headers.resource
                        )));
                    }
                    guard
                        .observed
                        .insert(headers.resource, (headers.remaining, headers.reset));
                    return serde_json::from_str(body).map_err(Error::from);
                }
                Err(error) => {
                    attempt += 1;
                    if probe || attempt >= RETRY_ATTEMPTS || !is_retryable(&error) {
                        return Err(error);
                    }
                    let backoff_index = (attempt as usize - 1).min(BACKOFF_SECONDS.len() - 1);
                    if self.sleeping_enabled {
                        sleep(Duration::from_secs(BACKOFF_SECONDS[backoff_index]));
                    }
                }
            }
        }
    }

    fn observe_rate_limit(&mut self, data: &Value) {
        if let Some(block) = data.get("rateLimit") {
            self.rate_limit.observe(block);
        }
    }
}

impl Default for GhClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Classify whether an error is worth retrying (throttles, transient failures).
fn is_retryable(error: &Error) -> bool {
    match error {
        Error::GhCommand { stderr, .. } => {
            let lowered = stderr.to_lowercase();
            lowered.contains("secondary rate limit")
                || lowered.contains("rate limit")
                || lowered.contains("http 403")
                || lowered.contains("http 429")
                || lowered.contains("http 5")
                || lowered.contains("timeout")
                || lowered.contains("connection")
        }
        _ => false,
    }
}

#[cfg(test)]
pub mod test_support {
    use super::*;
    use std::sync::Mutex;

    /// Transport that replays queued responses and records invocations.
    pub struct FixtureTransport {
        pub responses: Mutex<Vec<Result<String>>>,
        pub calls: Mutex<Vec<Vec<String>>>,
    }

    impl FixtureTransport {
        pub fn new(responses: Vec<Result<String>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl GhTransport for FixtureTransport {
        fn run(&self, arguments: &[String]) -> Result<String> {
            self.calls.lock().unwrap().push(arguments.to_vec());
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                return Err(Error::GitHubApi("no more fixture responses".into()));
            }
            responses.remove(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::FixtureTransport;
    use super::*;

    #[test]
    fn the_request_allowance_counts_retries_and_refuses_without_calling() {
        let transport = FixtureTransport::new(vec![
            Err(Error::GhCommand {
                exit_code: Some(1),
                stderr: "HTTP 502".into(),
            }),
            Ok("{}".into()),
            Ok("{}".into()),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        client.set_request_allowance(Some(2));
        client.rest("a").unwrap(); // one retry: both attempts are spent
        assert_eq!(client.request_allowance(), Some(0));
        assert!(matches!(client.rest("b"), Err(Error::QuotaReserve(_))));
        assert_eq!(client.requests_issued(), 2, "the refused call never ran");
        client.set_request_allowance(None);
        client.rest("c").unwrap();
    }

    fn included(resource: &str, remaining: i64, body: &str) -> Result<String> {
        // Shape of `gh api --include`, with an auth-like header that must
        // never surface anywhere.
        Ok(format!(
            "HTTP/2.0 200 OK\r\nAuthorization: token SECRET123\r\nX-Ratelimit-Limit: 5000\r\n\
             X-Ratelimit-Remaining: {remaining}\r\nX-Ratelimit-Reset: 1791386933\r\n\
             X-Ratelimit-Resource: {resource}\r\nX-Ratelimit-Used: 651\r\n\r\n{body}"
        ))
    }

    #[test]
    fn included_output_yields_only_rate_limit_facts_and_the_body() {
        let raw = included("core", 4349, "{\"a\":1}").unwrap();
        let (headers, body) = split_included(&raw).unwrap();
        assert_eq!(
            headers,
            Some(RateLimitHeaders {
                resource: "core".into(),
                remaining: 4349,
                reset: 1791386933
            })
        );
        assert_eq!(body, "{\"a\":1}");
        assert!(!format!("{headers:?}").contains("SECRET"));
        assert_eq!(split_included("{}").unwrap(), (None, "{}"));
        let malformed = "HTTP/2.0 200 OK\r\nX-Ratelimit-Remaining: lots\r\n\
                         X-Ratelimit-Reset: 1\r\nX-Ratelimit-Resource: core\r\n\r\n{}";
        assert!(matches!(
            split_included(malformed),
            Err(Error::QuotaReserve(_))
        ));
        let partial = "HTTP/2.0 200 OK\r\nX-Ratelimit-Remaining: 5\r\n\r\n{}";
        assert!(matches!(
            split_included(partial),
            Err(Error::QuotaReserve(_))
        ));
    }

    #[test]
    fn the_reserve_is_enforced_from_data_response_headers() {
        let transport = std::sync::Arc::new(FixtureTransport::new(vec![
            included("core", 503, "{}"),
            included("core", 502, "{}"),
            included("core", 501, "{}"),
            included("core", 500, "{}"),
        ]));
        struct Shared(std::sync::Arc<FixtureTransport>);
        impl GhTransport for Shared {
            fn run(&self, arguments: &[String]) -> Result<String> {
                self.0.run(arguments)
            }
        }
        let mut client =
            GhClient::with_transport(Box::new(Shared(transport.clone()))).without_sleeping();
        client.enforce_quota_reserve(500);
        client.rest("a").unwrap(); // probe: learns 503
        client.rest("b").unwrap(); // 503-1 >= 500
        client.rest("c").unwrap(); // 502-1 >= 500, observes 501
        client.rest("d").unwrap(); // 501-1 == 500: lands exactly on the reserve
        let error = client.rest("e").unwrap_err();
        assert!(matches!(error, Error::QuotaReserve(_)), "{error}");
        assert!(!error.to_string().contains("SECRET"));
        let calls = transport.calls.lock().unwrap();
        assert_eq!(calls.len(), 4, "the refused call never ran");
        assert_eq!(calls[0], vec!["api", "--include", "a"]);
        assert_eq!(client.observed_quota("core"), Some((500, 1791386933)));
    }

    #[test]
    fn a_guarded_client_fails_closed_on_missing_or_foreign_headers() {
        let mut client =
            GhClient::with_transport(Box::new(FixtureTransport::new(vec![Ok("{}".into())])))
                .without_sleeping();
        client.enforce_quota_reserve(500);
        assert!(matches!(client.rest("a"), Err(Error::QuotaReserve(_))));
        let mut client = GhClient::with_transport(Box::new(FixtureTransport::new(vec![included(
            "search", 29, "{}",
        )])))
        .without_sleeping();
        client.enforce_quota_reserve(500);
        assert!(matches!(client.rest("a"), Err(Error::QuotaReserve(_))));
    }

    #[test]
    fn an_unobserved_bucket_is_probed_once_without_retries() {
        let transport = FixtureTransport::new(vec![
            Err(Error::GhCommand {
                exit_code: Some(1),
                stderr: "HTTP 502".into(),
            }),
            included("core", 4000, "{}"),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        client.enforce_quota_reserve(500);
        assert!(matches!(client.rest("a"), Err(Error::GhCommand { .. })));
        assert_eq!(
            client.requests_issued(),
            1,
            "no retry before quota is known"
        );
    }

    #[test]
    fn retries_and_pages_each_spend_observed_quota() {
        let transport = FixtureTransport::new(vec![
            included("core", 502, "{}"),
            Err(Error::GhCommand {
                exit_code: Some(1),
                stderr: "HTTP 502".into(),
            }),
            Err(Error::GhCommand {
                exit_code: Some(1),
                stderr: "HTTP 502".into(),
            }),
            included("core", 900, "{}"),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        client.enforce_quota_reserve(500);
        client.rest("a").unwrap(); // observes 502
        // 502 -> 501 (attempt 1 fails) -> 500 (attempt 2 fails); a third
        // attempt would spend below the reserve: refused locally.
        assert!(matches!(client.rest("b"), Err(Error::QuotaReserve(_))));
        assert_eq!(client.requests_issued(), 3);
    }

    #[test]
    fn an_unguarded_client_sends_the_original_arguments() {
        let transport = std::sync::Arc::new(FixtureTransport::new(vec![Ok("{}".into())]));
        struct Shared(std::sync::Arc<FixtureTransport>);
        impl GhTransport for Shared {
            fn run(&self, arguments: &[String]) -> Result<String> {
                self.0.run(arguments)
            }
        }
        let mut client =
            GhClient::with_transport(Box::new(Shared(transport.clone()))).without_sleeping();
        client.rest("a").unwrap();
        assert_eq!(transport.calls.lock().unwrap()[0], vec!["api", "a"]);
    }

    #[test]
    fn graphql_extracts_data_and_surfaces_errors() {
        let transport = FixtureTransport::new(vec![
            Ok(r#"{"data": {"ok": true, "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"}}}"#.into()),
            Ok(r#"{"data": null, "errors": [{"message": "Could not resolve"}]}"#.into()),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let data = client.graphql("query {}", &[]).expect("first call ok");
        assert_eq!(data.get("ok"), Some(&Value::Bool(true)));
        let error = client.graphql("query {}", &[]).unwrap_err();
        assert!(matches!(error, Error::GitHubApi(_)));
    }

    #[test]
    fn retries_on_secondary_rate_limit_then_succeeds() {
        let transport = FixtureTransport::new(vec![
            Err(Error::GhCommand {
                exit_code: Some(1),
                stderr: "HTTP 403: You have exceeded a secondary rate limit".into(),
            }),
            Ok(r#"{"data": {"ok": true}}"#.into()),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let data = client.graphql("query {}", &[]).expect("retried call ok");
        assert_eq!(data.get("ok"), Some(&Value::Bool(true)));
    }

    #[test]
    fn does_not_retry_fatal_errors() {
        let transport = FixtureTransport::new(vec![
            Err(Error::GhCommand {
                exit_code: Some(1),
                stderr: "HTTP 404: Not Found".into(),
            }),
            Ok(r#"{"data": {"ok": true}}"#.into()),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        assert!(client.graphql("query {}", &[]).is_err());
    }
}
