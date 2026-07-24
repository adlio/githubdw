//! Fetch layer: runs the `gh` CLI as a subprocess and decodes JSON.

pub mod issues;
pub mod pull_requests;
pub mod rate_limit;

use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

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

/// Client for the GitHub API via the `gh` CLI.
pub struct GhClient {
    transport: Box<dyn GhTransport>,
    rate_limit: RateLimitTracker,
    /// Optional pacing delay between requests (bulk-sync politeness).
    inter_request_delay: Option<Duration>,
    /// When false (tests), never actually sleep.
    sleeping_enabled: bool,
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
        }
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
        loop {
            match self.transport.run(arguments) {
                Ok(stdout) => {
                    return serde_json::from_str(&stdout).map_err(Error::from);
                }
                Err(error) => {
                    attempt += 1;
                    if attempt >= RETRY_ATTEMPTS || !is_retryable(&error) {
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
