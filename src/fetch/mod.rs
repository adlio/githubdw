//! Fetch layer: runs the `gh` CLI as a subprocess and decodes JSON.

pub mod check_history;
pub mod continuation;
pub mod git_files;
pub mod issues;
pub mod pull_requests;
pub mod rate_limit;
pub mod user_search;

use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::error::{Error, Result};
use rate_limit::RateLimitTracker;

/// Abstraction over the `gh` subprocess so tests can substitute fixtures.
pub trait GhTransport: Send + Sync {
    /// Run `gh` with the given arguments and return stdout on success.
    fn run(&self, arguments: &[String]) -> Result<String>;
}

/// Default wall-clock limit for one `gh` invocation.
const GH_TIMEOUT: Duration = Duration::from_secs(120);
/// Environment variable overriding [`GH_TIMEOUT`], in whole seconds.
pub const GH_DEADLINE_ENV: &str = "GITHUBDW_GH_DEADLINE_SECS";

/// Real transport: shells out to the `gh` binary on PATH.
pub struct GhCommandLine {
    program: std::ffi::OsString,
    timeout: Duration,
}

impl GhCommandLine {
    /// `gh` on PATH, killed after 120s (or `GITHUBDW_GH_DEADLINE_SECS`).
    pub fn new() -> Self {
        let timeout = std::env::var(GH_DEADLINE_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|seconds| *seconds > 0)
            .map_or(GH_TIMEOUT, Duration::from_secs);
        Self {
            program: "gh".into(),
            timeout,
        }
    }
}

impl Default for GhCommandLine {
    fn default() -> Self {
        Self::new()
    }
}

/// Turn a finished `gh` process into the transport result.
///
/// `gh api graphql` exits 1 when the response carries GraphQL `errors`, yet
/// still prints the whole body. When that body is a GraphQL response (a
/// `data` or `errors` key) it is returned so the caller decides: a search
/// skips inaccessible nodes, while `GhClient::graphql` still rejects any
/// `errors`. A rate-limited response stays an error so it is retried.
pub(crate) fn interpret_gh_output(
    arguments: &[String],
    exit_code: Option<i32>,
    stdout: &str,
    stderr: &str,
) -> Result<String> {
    if exit_code == Some(0) {
        return Ok(stdout.to_string());
    }
    if arguments.get(1).map(String::as_str) == Some("graphql")
        || arguments.get(2).map(String::as_str) == Some("graphql")
    {
        // `--include` puts response headers before the JSON body.
        let body = split_included(stdout).map_or(stdout, |(_, body)| body);
        let response = serde_json::from_str::<Value>(body).ok();
        let graphql_body = response
            .as_ref()
            .is_some_and(|r| r.get("data").is_some() || r.get("errors").is_some());
        let throttled = response
            .as_ref()
            .and_then(|r| r.get("errors"))
            .and_then(Value::as_array)
            .is_some_and(|errors| {
                errors
                    .iter()
                    .any(|e| e.get("type").and_then(Value::as_str) == Some("RATE_LIMITED"))
            });
        let error = Error::GhCommand {
            exit_code,
            stderr: stderr.to_string(),
        };
        // A throttle stays an error so the retry/backoff path handles it.
        if graphql_body && !throttled && !is_rate_limit(&error) {
            return Ok(stdout.to_string());
        }
        return Err(error);
    }
    Err(Error::GhCommand {
        exit_code,
        stderr: stderr.to_string(),
    })
}

impl GhTransport for GhCommandLine {
    fn run(&self, arguments: &[String]) -> Result<String> {
        let mut child = Command::new(&self.program)
            .args(arguments)
            .env("GH_PROMPT_DISABLED", "1")
            .env_remove("GH_DEBUG")
            .env_remove("DEBUG")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| Error::GhUnavailable(format!("failed to run gh: {error}")))?;
        let mut stdout = child.stdout.take().expect("stdout is piped");
        let mut stderr = child.stderr.take().expect("stderr is piped");
        let (out_tx, out_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut buffer = Vec::new();
            let _ = stdout.read_to_end(&mut buffer);
            let _ = out_tx.send(buffer);
        });
        let (err_tx, err_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut buffer = Vec::new();
            let _ = stderr.read_to_end(&mut buffer);
            let _ = err_tx.send(buffer);
        });
        let deadline = Instant::now() + self.timeout;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {}
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(Error::GhUnavailable(format!(
                        "failed to wait for gh: {error}"
                    )));
                }
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                // Not worded as a "timeout": a hung gh is not retried.
                return Err(Error::GhCommand {
                    exit_code: None,
                    stderr: format!(
                        "gh {} killed after {}s without finishing (set {GH_DEADLINE_ENV} to change)",
                        arguments.first().map(String::as_str).unwrap_or_default(),
                        self.timeout.as_secs()
                    ),
                });
            }
            thread::sleep(Duration::from_millis(10));
        };
        // A grandchild can keep a pipe open after gh exits; do not wait on it.
        let drain = |rx: &mpsc::Receiver<Vec<u8>>| {
            rx.recv_timeout(Duration::from_secs(5))
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .unwrap_or_default()
        };
        let stdout = drain(&out_rx);
        let stderr = drain(&err_rx);
        interpret_gh_output(arguments, status.code(), &stdout, &stderr)
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
    /// Checked before every request and during every wait.
    cancel: Option<Arc<AtomicBool>>,
    /// When set, a rate-limit wait longer than this is not slept: the call
    /// returns [`Error::RateLimited`] with the wake time instead.
    defer_waits_over: Option<Duration>,
    /// Response bytes received (stdout length), for cost measurement.
    bytes_received: u64,
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
        Self::with_transport(Box::new(GhCommandLine::new()))
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
            cancel: None,
            defer_waits_over: None,
            bytes_received: 0,
        }
    }

    /// Observe `flag`: once it is set, the next request (or the current wait)
    /// returns [`Error::Cancelled`].
    pub fn set_cancellation(&mut self, flag: Option<Arc<AtomicBool>>) {
        self.cancel = flag;
    }

    /// Return [`Error::RateLimited`] instead of sleeping through a rate-limit
    /// wait longer than `limit`. `None` restores the blocking behaviour.
    pub fn set_defer_waits_over(&mut self, limit: Option<Duration>) {
        self.defer_waits_over = limit;
    }

    /// Response bytes received over the transport so far.
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received
    }

    fn cancelled(&self) -> bool {
        self.cancel
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::SeqCst))
    }

    /// Sleep in short slices so a cancellation is noticed within ~200 ms.
    fn wait(&self, duration: Duration) -> Result<()> {
        if !self.sleeping_enabled {
            return if self.cancelled() {
                Err(Error::Cancelled)
            } else {
                Ok(())
            };
        }
        let deadline = Instant::now() + duration;
        loop {
            if self.cancelled() {
                return Err(Error::Cancelled);
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(());
            }
            std::thread::sleep((deadline - now).min(Duration::from_millis(200)));
        }
    }

    /// Wait out a rate limit, or report it when the wait is too long.
    fn wait_for_rate_limit(&self, resource: &str, wait: Duration, reason: &str) -> Result<()> {
        if let Some(limit) = self.defer_waits_over
            && wait > limit
        {
            return Err(Error::RateLimited {
                resource: resource.to_string(),
                wake_at: chrono::Utc::now()
                    + chrono::Duration::from_std(wait).unwrap_or(chrono::Duration::hours(1)),
                reason: reason.to_string(),
            });
        }
        self.wait(wait)
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

    fn graphql_arguments(query: &str, variables: &[(&str, &str)]) -> Vec<String> {
        let mut arguments: Vec<String> = vec![
            "api".into(),
            "graphql".into(),
            "-f".into(),
            format!("query={query}"),
        ];
        for (name, value) in variables {
            // `-F` coerces `123`, `true`, `null` to JSON and reads `@file`;
            // only variables the query declares Int or Boolean may use it.
            let typed = ["Int", "Boolean"]
                .iter()
                .any(|kind| query.contains(&format!("${name}: {kind}")));
            arguments.push(if typed { "-F" } else { "-f" }.into());
            arguments.push(format!("{name}={value}"));
        }
        arguments
    }

    /// Execute a search query and return `data` together with any GraphQL
    /// `errors`, without failing on them. The caller classifies each error
    /// (an inaccessible node is an expected skip; anything else is a
    /// failure). Fails only when the response carries no `data` at all.
    pub fn graphql_search_with_errors(
        &mut self,
        query: &str,
        variables: &[(&str, &str)],
    ) -> Result<(Value, Vec<Value>)> {
        self.pace_search()?;
        let arguments = Self::graphql_arguments(query, variables);
        let response: Value = self.run_with_retry(&arguments)?;
        let errors = response
            .get("errors")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let data = response
            .get("data")
            .filter(|data| !data.is_null())
            .cloned()
            .ok_or_else(|| {
                Error::GitHubApi(format!(
                    "response has no data: {}",
                    errors
                        .iter()
                        .filter_map(|e| e.get("message").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("; ")
                ))
            })?;
        self.observe_rate_limit(&data);
        Ok((data, errors))
    }

    fn pace_search(&mut self) -> Result<()> {
        if let Some(previous) = self.last_search_at {
            let elapsed = previous.elapsed();
            if elapsed < SEARCH_MIN_INTERVAL {
                self.wait(SEARCH_MIN_INTERVAL - elapsed)?;
            }
        }
        self.last_search_at = Some(Instant::now());
        Ok(())
    }

    /// Execute a GraphQL query with string variables, returning the `data` value.
    pub fn graphql(&mut self, query: &str, variables: &[(&str, &str)]) -> Result<Value> {
        let arguments = Self::graphql_arguments(query, variables);
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
        self.pace_search()?;
        self.graphql(query, variables)
    }

    /// Execute a REST call (`gh api <path>`), returning the parsed JSON.
    pub fn rest(&mut self, path: &str) -> Result<Value> {
        let arguments: Vec<String> = vec!["api".into(), path.into()];
        self.run_with_retry(&arguments)
    }

    fn run_with_retry(&mut self, arguments: &[String]) -> Result<Value> {
        if self.cancelled() {
            return Err(Error::Cancelled);
        }
        if let Some(delay) = self.inter_request_delay {
            self.wait(delay)?;
        }
        // Primary rate limit: sleep until reset before issuing the call, or
        // report the wake time when the caller defers long waits.
        if let Some(wait) = self.rate_limit.wait_needed() {
            self.wait_for_rate_limit(
                "graphql",
                wait,
                "primary GraphQL budget below its safety floor",
            )?;
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
            if self.cancelled() {
                return Err(Error::Cancelled);
            }
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
                    self.bytes_received += stdout.len() as u64;
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
                    let backoff = Duration::from_secs(BACKOFF_SECONDS[backoff_index]);
                    if is_rate_limit(&error) {
                        // GitHub asks for at least a minute after a secondary
                        // limit; a deferring caller gets that wake time.
                        self.wait_for_rate_limit(
                            resource,
                            backoff.max(Duration::from_secs(60)),
                            "provider rate limit response",
                        )?;
                    } else {
                        self.wait(backoff)?;
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

/// A throttle response (primary or secondary rate limit).
fn is_rate_limit(error: &Error) -> bool {
    match error {
        Error::GhCommand { stderr, .. } => {
            let lowered = stderr.to_lowercase();
            lowered.contains("rate limit")
                || lowered.contains("abuse detection")
                || lowered.contains("http 429")
        }
        _ => false,
    }
}

/// Classify whether an error is worth retrying (throttles, transient failures).
/// A 403 is retried only when it is a rate limit; SAML, permission and
/// blocked-repository 403s are permanent and fail on the first attempt.
fn is_retryable(error: &Error) -> bool {
    match error {
        Error::GhCommand { stderr, .. } => {
            let lowered = stderr.to_lowercase();
            is_rate_limit(error)
                || lowered.contains("http 5")
                || lowered.contains("timeout")
                || lowered.contains("timed out")
                || lowered.contains("connection reset")
                || lowered.contains("connection refused")
                || lowered.contains("connection closed")
                || lowered.contains("unexpected eof")
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
    fn a_cancelled_client_sends_nothing() {
        let transport = std::sync::Arc::new(FixtureTransport::new(vec![Ok("{}".into())]));
        struct Shared(std::sync::Arc<FixtureTransport>);
        impl GhTransport for Shared {
            fn run(&self, arguments: &[String]) -> Result<String> {
                self.0.run(arguments)
            }
        }
        let mut client =
            GhClient::with_transport(Box::new(Shared(transport.clone()))).without_sleeping();
        client.set_cancellation(Some(Arc::new(AtomicBool::new(true))));
        assert!(matches!(client.rest("a"), Err(Error::Cancelled)));
        assert!(transport.calls.lock().unwrap().is_empty());
        assert_eq!(client.requests_issued(), 0);
    }

    #[test]
    fn the_inter_request_delay_is_waited_before_each_call() {
        let transport = FixtureTransport::new(vec![Ok("{}".into()), Ok("{}".into())]);
        let mut client = GhClient::with_transport(Box::new(transport))
            .with_inter_request_delay(Duration::from_millis(5));
        let started = Instant::now();
        client.rest("a").unwrap();
        client.rest("b").unwrap();
        assert!(started.elapsed() >= Duration::from_millis(10));
        assert_eq!(client.requests_issued(), 2);
    }

    #[test]
    fn cancellation_interrupts_the_search_pacing_wait() {
        // The first search sets the flag; the second must stop in its pacing
        // wait (2.1 s) without reaching the transport.
        struct CancelOnFirst {
            flag: Arc<AtomicBool>,
            calls: std::sync::Mutex<usize>,
        }
        impl GhTransport for CancelOnFirst {
            fn run(&self, _arguments: &[String]) -> Result<String> {
                *self.calls.lock().unwrap() += 1;
                self.flag.store(true, Ordering::SeqCst);
                Ok(r#"{"data": {"search": {}}}"#.into())
            }
        }
        let flag = Arc::new(AtomicBool::new(false));
        let transport = Arc::new(CancelOnFirst {
            flag: flag.clone(),
            calls: std::sync::Mutex::new(0),
        });
        struct Shared(Arc<CancelOnFirst>);
        impl GhTransport for Shared {
            fn run(&self, arguments: &[String]) -> Result<String> {
                self.0.run(arguments)
            }
        }
        let mut client = GhClient::with_transport(Box::new(Shared(transport.clone())));
        client.set_cancellation(Some(flag));
        let started = Instant::now();
        client.graphql_search_with_errors("query {}", &[]).unwrap();
        let error = client
            .graphql_search_with_errors("query {}", &[])
            .unwrap_err();
        assert!(matches!(error, Error::Cancelled), "{error}");
        assert!(started.elapsed() < SEARCH_MIN_INTERVAL);
        assert_eq!(*transport.calls.lock().unwrap(), 1);
    }

    #[test]
    fn a_search_response_without_data_reports_its_errors() {
        let transport = FixtureTransport::new(vec![
            Ok(r#"{"data": null, "errors": [{"message": "first"}, {"type": "X"}, {"message": "second"}]}"#.into()),
            Ok(r#"{"data": {"search": {"issueCount": 0}}, "errors": [{"message": "partial"}]}"#.into()),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let error = client
            .graphql_search_with_errors("query {}", &[])
            .unwrap_err();
        match error {
            Error::GitHubApi(message) => assert_eq!(message, "response has no data: first; second"),
            other => panic!("unexpected {other}"),
        }
        let (data, errors) = client.graphql_search_with_errors("query {}", &[]).unwrap();
        assert_eq!(data["search"]["issueCount"], 0);
        assert_eq!(
            errors.len(),
            1,
            "errors beside data are returned, not raised"
        );
    }

    #[test]
    fn an_exhausted_primary_budget_is_deferred_or_waited() {
        let exhausted = r#"{"data": {"rateLimit": {"limit": 5000, "cost": 1, "remaining": 0, "resetAt": "2099-01-01T00:00:00Z"}}}"#;
        let transport = FixtureTransport::new(vec![Ok(exhausted.into()), Ok(exhausted.into())]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        client.graphql("query {}", &[]).unwrap();
        client.set_defer_waits_over(Some(Duration::from_secs(1)));
        match client.graphql("query {}", &[]).unwrap_err() {
            Error::RateLimited {
                resource, reason, ..
            } => {
                assert_eq!(resource, "graphql");
                assert!(reason.contains("primary GraphQL budget"), "{reason}");
            }
            other => panic!("unexpected {other}"),
        }
        assert_eq!(client.requests_issued(), 1, "the deferred call never ran");
        // Without deferral the (non-sleeping) client waits, then sends.
        client.set_defer_waits_over(None);
        client.graphql("query {}", &[]).unwrap();
        assert_eq!(client.requests_issued(), 2);
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

    fn gh_failure(stderr: &str) -> Result<String> {
        Err(Error::GhCommand {
            exit_code: Some(1),
            stderr: stderr.into(),
        })
    }

    #[test]
    fn a_permanent_403_costs_one_attempt() {
        for stderr in [
            "HTTP 403: Resource protected by organization SAML enforcement.",
            "HTTP 403: Must have admin rights to Repository.",
            "HTTP 403: Repository access blocked",
        ] {
            let transport = FixtureTransport::new(vec![gh_failure(stderr), Ok("{}".into())]);
            let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
            assert!(client.rest("a").is_err(), "{stderr}");
            assert_eq!(client.requests_issued(), 1, "{stderr}");
        }
    }

    #[test]
    fn throttles_and_transient_network_errors_are_retried() {
        for stderr in [
            "HTTP 403: You have exceeded a secondary rate limit.",
            "HTTP 403: API rate limit exceeded for user ID 1.",
            "HTTP 403: You have triggered an abuse detection mechanism.",
            "HTTP 429: Too Many Requests",
            "HTTP 502: Bad Gateway",
            "Post \"https://api.github.com/graphql\": read: connection reset by peer",
            "dial tcp: connect: connection refused",
            "net/http: TLS handshake timeout",
        ] {
            let transport = FixtureTransport::new(vec![gh_failure(stderr), Ok("{}".into())]);
            let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
            client.rest("a").unwrap_or_else(|e| panic!("{stderr}: {e}"));
            assert_eq!(client.requests_issued(), 2, "{stderr}");
        }
        // "connection" alone is not a transient signal.
        let transport = FixtureTransport::new(vec![
            gh_failure("GraphQL: Field 'connection' doesn't exist on type 'Query'"),
            Ok("{}".into()),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        assert!(client.rest("a").is_err());
        assert_eq!(client.requests_issued(), 1);
    }

    #[test]
    fn string_variables_are_sent_raw_and_declared_ints_typed() {
        let query = "query($owner: String!, $name: String!, $number: Int!, $flag: Boolean) { x }";
        let arguments = GhClient::graphql_arguments(
            query,
            &[
                ("owner", "123"),
                ("name", "true"),
                ("number", "42"),
                ("flag", "false"),
                ("cursor", "@etc/passwd"),
                ("q", "null"),
            ],
        );
        let expected: Vec<String> = [
            "api",
            "graphql",
            "-f",
            &format!("query={query}"),
            "-f",
            "owner=123",
            "-f",
            "name=true",
            "-F",
            "number=42",
            "-F",
            "flag=false",
            "-f",
            "cursor=@etc/passwd",
            "-f",
            "q=null",
        ]
        .iter()
        .map(|a| a.to_string())
        .collect();
        assert_eq!(arguments, expected);
    }

    #[test]
    fn a_graphql_exit_1_with_a_body_returns_the_body() {
        let graphql = ["api".to_string(), "graphql".to_string()];
        let partial =
            r#"{"data":{"a":{"name":"githubdw"},"b":null},"errors":[{"type":"NOT_FOUND"}]}"#;
        assert_eq!(
            interpret_gh_output(&graphql, Some(1), partial, "gh: Could not resolve").unwrap(),
            partial
        );
        // A body with errors and null data is also the caller's to classify.
        let no_data = r#"{"data":null,"errors":[{"message":"bad cursor"}]}"#;
        assert_eq!(
            interpret_gh_output(&graphql, Some(1), no_data, "gh: bad cursor").unwrap(),
            no_data
        );
        // With `--include`, headers precede the body.
        let included = [
            "api".to_string(),
            "--include".to_string(),
            "graphql".to_string(),
        ];
        let with_headers = format!("HTTP/2.0 200 OK\r\nX-Foo: 1\r\n\r\n{partial}");
        assert_eq!(
            interpret_gh_output(&included, Some(1), &with_headers, "").unwrap(),
            with_headers
        );
        // Not a GraphQL body, a throttle, or a REST call: still an error.
        for (arguments, stdout, stderr) in [
            (&graphql[..], "", "HTTP 401: Bad credentials"),
            (&graphql[..], r#"{"message":"Bad credentials"}"#, "HTTP 401"),
            (
                &graphql[..],
                r#"{"errors":[{"type":"RATE_LIMITED","message":"slow"}]}"#,
                "gh: slow",
            ),
            (
                &graphql[..],
                r#"{"data":null,"errors":[{"message":"x"}]}"#,
                "HTTP 403: secondary rate limit",
            ),
            (
                &["api".to_string(), "repos/a/b".to_string()][..],
                partial,
                "HTTP 404",
            ),
        ] {
            match interpret_gh_output(arguments, Some(1), stdout, stderr) {
                Err(Error::GhCommand {
                    exit_code,
                    stderr: got,
                }) => {
                    assert_eq!((exit_code, got.as_str()), (Some(1), stderr));
                }
                other => panic!("{stdout} / {stderr}: {other:?}"),
            }
        }
    }

    /// A `gh` stand-in: a shell script at a temp path.
    #[cfg(unix)]
    fn fake_gh(body: &str, timeout: Duration) -> (tempfile::TempDir, GhCommandLine) {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gh");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let gh = GhCommandLine {
            program: path.into_os_string(),
            timeout,
        };
        (dir, gh)
    }

    #[cfg(unix)]
    #[test]
    fn real_transport_keeps_partial_data_from_an_exit_1() {
        let (_dir, gh) = fake_gh(
            r#"printf '%s' '{"data":{"a":{"name":"githubdw"},"b":null},"errors":[{"type":"NOT_FOUND"}]}'
echo 'gh: Could not resolve to a Repository' >&2
exit 1"#,
            Duration::from_secs(30),
        );
        let mut client = GhClient::with_transport(Box::new(gh)).without_sleeping();
        let (data, errors) = client
            .graphql_search_with_errors("query { a b }", &[])
            .unwrap();
        assert_eq!(data["a"]["name"], "githubdw");
        assert_eq!(errors.len(), 1);
        // The strict path still refuses any errors, without a retry.
        assert!(matches!(
            client.graphql("query { a b }", &[]),
            Err(Error::GitHubApi(_))
        ));
        assert_eq!(client.requests_issued(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn a_hung_gh_is_killed_at_the_deadline() {
        let (_dir, gh) = fake_gh("exec sleep 30", Duration::from_millis(300));
        let started = Instant::now();
        let error = gh.run(&["api".into(), "graphql".into()]).unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );
        match &error {
            Error::GhCommand { exit_code, stderr } => {
                assert_eq!(*exit_code, None);
                assert!(stderr.contains("killed after 0s"), "{stderr}");
                assert!(stderr.contains(GH_DEADLINE_ENV), "{stderr}");
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(!is_retryable(&error), "a hung gh is not retried");
    }

    #[cfg(unix)]
    #[test]
    fn gh_runs_without_prompts_or_debug_output() {
        let (_dir, gh) = fake_gh(
            r#"printf '%s|%s|%s' "${GH_PROMPT_DISABLED-unset}" "${GH_DEBUG-unset}" "${DEBUG-unset}""#,
            Duration::from_secs(30),
        );
        // SAFETY: no other test reads or writes GH_DEBUG or DEBUG.
        unsafe {
            std::env::set_var("GH_DEBUG", "api");
            std::env::set_var("DEBUG", "1");
        }
        let stdout = gh.run(&["version".into()]);
        unsafe {
            std::env::remove_var("GH_DEBUG");
            std::env::remove_var("DEBUG");
        }
        assert_eq!(stdout.unwrap(), "1|unset|unset");
    }
}
