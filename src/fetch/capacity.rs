//! Capacity cap: leave part of the GitHub budget to the token's owner.
//!
//! githubdw collects through the user's own `gh` sign-in, so every sync draws
//! on the same hourly budget as that person's own `gh` commands and any other
//! collector on the token. A client with a cap
//! ([`super::GhClient::set_max_rate_use`]) checks the budget GitHub reports
//! before each request and, once the share used reaches the cap, waits for
//! the reset instead of spending more.
//!
//! The readings are GitHub's own (response headers, a GraphQL `rateLimit`
//! block, or the free `rate_limit` endpoint), never a local count, because
//! other processes spend the same budget between two requests. GitHub was
//! seen answering from two views of one GraphQL budget at once (2026-11-23:
//! 912 used with reset 16:40Z and 3,913 used with reset 16:34Z, alternating
//! between consecutive calls), so every reading is kept until its own reset
//! and the cap is judged against the most used one.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::error::{Error, Result};

/// Share of the hourly budget a client may use unless told otherwise.
pub const DEFAULT_MAX_RATE_USE: f64 = 0.95;

/// Config key (`githubdw config set max_rate_use 0.9`) read by the CLI.
pub const CONFIG_KEY: &str = "max_rate_use";

/// A reading of one budget, as GitHub reported it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    pub limit: i64,
    pub used: i64,
    /// Epoch seconds when this budget resets.
    pub reset: i64,
}

/// Check a cap given as a fraction of the limit: above 0, at most 1.
pub fn validate(max_use: f64) -> Result<f64> {
    if max_use.is_finite() && max_use > 0.0 && max_use <= 1.0 {
        Ok(max_use)
    } else {
        Err(Error::InvalidArgument(format!(
            "max rate use must be a fraction above 0 and at most 1 (0.95 = 95%), got {max_use}"
        )))
    }
}

/// Parse a cap written as a fraction (`0.95`) or a percentage (`95%`).
pub fn parse(text: &str) -> Result<f64> {
    let text = text.trim();
    let value = match text.strip_suffix('%') {
        Some(percent) => percent.trim().parse::<f64>().map(|p| p / 100.0),
        None => text.parse::<f64>(),
    }
    .map_err(|_| {
        Error::InvalidArgument(format!(
            "max rate use must be a fraction such as 0.95 or a percentage such as 95%, got {text:?}"
        ))
    })?;
    validate(value)
}

/// Whether `budget` has reached the cap.
pub fn over_cap(budget: &Budget, max_use: f64) -> bool {
    budget.limit > 0 && budget.used as f64 >= max_use * budget.limit as f64
}

/// How long to wait before spending more, given every live reading of one
/// resource: until the reset of the most used reading, when that reading is
/// at or over the cap. Readings whose reset has passed are ignored.
pub fn wait_needed(readings: &[Budget], max_use: f64, now: i64) -> Option<(Budget, Duration)> {
    let worst = readings.iter().filter(|b| b.reset > now).max_by(|a, b| {
        let share = |x: &Budget| x.used as f64 / x.limit.max(1) as f64;
        share(a).total_cmp(&share(b))
    })?;
    over_cap(worst, max_use).then(|| (*worst, Duration::from_secs((worst.reset - now) as u64)))
}

/// One resource's readings, keyed by reset time, newest kept per key.
#[derive(Debug, Default)]
pub(crate) struct Readings {
    by_resource: BTreeMap<String, BTreeMap<i64, Budget>>,
    /// When a reading last arrived per resource, to decide whether to probe.
    last_seen: BTreeMap<String, std::time::Instant>,
}

impl Readings {
    pub(crate) fn record(&mut self, resource: &str, budget: Budget) {
        let window = self.by_resource.entry(resource.to_string()).or_default();
        let entry = window.entry(budget.reset).or_insert(budget);
        // Within one window, usage only grows; keep the higher reading.
        if budget.used >= entry.used {
            *entry = budget;
        }
        self.last_seen
            .insert(resource.to_string(), std::time::Instant::now());
    }

    /// Live readings of `resource`; drops those already reset.
    pub(crate) fn live(&mut self, resource: &str, now: i64) -> Vec<Budget> {
        let Some(window) = self.by_resource.get_mut(resource) else {
            return Vec::new();
        };
        window.retain(|reset, _| *reset > now);
        window.values().copied().collect()
    }

    /// Whether the newest reading of `resource` is younger than `max_age`.
    pub(crate) fn fresh(&self, resource: &str, max_age: Duration) -> bool {
        self.last_seen
            .get(resource)
            .is_some_and(|at| at.elapsed() < max_age)
    }
}

/// The `rateLimit { limit used remaining resetAt }` block of a GraphQL
/// response.
pub fn from_graphql_block(block: &Value) -> Option<Budget> {
    let limit = block.get("limit")?.as_i64()?;
    let used = block
        .get("used")
        .and_then(Value::as_i64)
        .or_else(|| Some(limit - block.get("remaining")?.as_i64()?))?;
    let reset = block
        .get("resetAt")?
        .as_str()?
        .parse::<DateTime<Utc>>()
        .ok()?
        .timestamp();
    Some(Budget { limit, used, reset })
}

/// One resource of a `gh api rate_limit` response
/// (`{"resources": {"graphql": {"limit", "used", "remaining", "reset"}}}`).
pub fn from_rate_limit_endpoint(body: &Value, resource: &str) -> Option<Budget> {
    let entry = body.get("resources")?.get(resource)?;
    let limit = entry.get("limit")?.as_i64()?;
    let used = entry
        .get("used")
        .and_then(Value::as_i64)
        .or_else(|| Some(limit - entry.get("remaining")?.as_i64()?))?;
    Some(Budget {
        limit,
        used,
        reset: entry.get("reset")?.as_i64()?,
    })
}

/// The `x-ratelimit-{resource,limit,used,remaining,reset}` headers of a
/// `gh api --include` response, when all needed are present and numeric.
pub fn from_headers(raw: &str) -> Option<(String, Budget)> {
    if !raw.starts_with("HTTP/") {
        return None;
    }
    let head = raw.split("\r\n\r\n").next()?.split("\n\n").next()?;
    let (mut resource, mut limit, mut used, mut remaining, mut reset) =
        (None, None, None, None, None);
    for line in head.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "x-ratelimit-resource" => resource = Some(value.to_ascii_lowercase()),
            "x-ratelimit-limit" => limit = value.parse::<i64>().ok(),
            "x-ratelimit-used" => used = value.parse::<i64>().ok(),
            "x-ratelimit-remaining" => remaining = value.parse::<i64>().ok(),
            "x-ratelimit-reset" => reset = value.parse::<i64>().ok(),
            _ => {}
        }
    }
    let limit = limit?;
    let used = used.or_else(|| Some(limit - remaining?))?;
    Some((
        resource?,
        Budget {
            limit,
            used,
            reset: reset?,
        },
    ))
}

/// A wait for the reset that a capped client is in right now.
#[derive(Debug, Clone, PartialEq)]
pub struct CapacityPause {
    /// `graphql` or `core`.
    pub resource: String,
    pub used: i64,
    pub limit: i64,
    /// The cap, as a fraction of `limit`.
    pub max_use: f64,
    pub until: DateTime<Utc>,
}

static PAUSES: Mutex<Vec<(u64, CapacityPause)>> = Mutex::new(Vec::new());
static NEXT_PAUSE: AtomicU64 = AtomicU64::new(0);

/// The pause any capped client in this process is waiting in, if one is
/// (the one that ends last, when several are). For a status display: a
/// paused client is waiting on purpose, not failing.
pub fn current_pause() -> Option<CapacityPause> {
    let pauses = PAUSES.lock().unwrap_or_else(|p| p.into_inner());
    pauses
        .iter()
        .map(|(_, p)| p)
        .max_by_key(|p| p.until)
        .cloned()
}

/// Registers a pause for [`current_pause`] until dropped.
pub(crate) struct PauseGuard(u64);

impl PauseGuard {
    pub(crate) fn start(pause: CapacityPause) -> Self {
        let id = NEXT_PAUSE.fetch_add(1, Ordering::Relaxed);
        PAUSES
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((id, pause));
        Self(id)
    }
}

impl Drop for PauseGuard {
    fn drop(&mut self) {
        PAUSES
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(id, _)| *id != self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: i64 = 1_800_000_000;

    fn budget(used: i64, reset_in: i64) -> Budget {
        Budget {
            limit: 5000,
            used,
            reset: NOW + reset_in,
        }
    }

    #[test]
    fn under_the_cap_needs_no_wait() {
        assert_eq!(wait_needed(&[budget(4749, 600)], 0.95, NOW), None);
        assert_eq!(wait_needed(&[], 0.95, NOW), None);
    }

    #[test]
    fn at_the_cap_waits_until_the_reset() {
        let (worst, wait) = wait_needed(&[budget(4750, 600)], 0.95, NOW).unwrap();
        assert_eq!(worst.used, 4750);
        assert_eq!(wait, Duration::from_secs(600));
        assert!(wait_needed(&[budget(3999, 600)], 0.8, NOW).is_none());
        assert!(wait_needed(&[budget(4000, 600)], 0.8, NOW).is_some());
    }

    #[test]
    fn the_most_used_view_of_a_budget_decides() {
        // GitHub answering from two views at once: the busier one decides,
        // and the wait runs to that view's reset.
        let readings = [budget(912, 1800), budget(4200, 300)];
        let (worst, wait) = wait_needed(&readings, 0.8, NOW).unwrap();
        assert_eq!(worst.used, 4200);
        assert_eq!(wait, Duration::from_secs(300));
    }

    #[test]
    fn a_reading_past_its_reset_is_ignored() {
        assert_eq!(wait_needed(&[budget(5000, 0)], 0.8, NOW), None);
        let mut readings = Readings::default();
        readings.record("graphql", budget(4900, -5));
        readings.record("graphql", budget(100, 3000));
        assert_eq!(readings.live("graphql", NOW), vec![budget(100, 3000)]);
    }

    #[test]
    fn a_window_keeps_its_highest_reading() {
        let mut readings = Readings::default();
        readings.record("core", budget(300, 600));
        readings.record("core", budget(200, 600));
        assert_eq!(readings.live("core", NOW), vec![budget(300, 600)]);
        assert!(readings.live("graphql", NOW).is_empty());
    }

    #[test]
    fn caps_parse_as_fractions_or_percentages() {
        assert_eq!(parse("0.8").unwrap(), 0.8);
        assert_eq!(parse("80%").unwrap(), 0.8);
        assert_eq!(parse("1").unwrap(), 1.0);
        for bad in ["0", "1.5", "-0.2", "abc", "NaN", "150%"] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn readings_come_from_each_of_githubs_answers() {
        let block = json!({"limit": 5000, "used": 3913, "remaining": 1087,
                           "resetAt": "2026-11-23T16:34:46Z", "cost": 1});
        let b = from_graphql_block(&block).unwrap();
        assert_eq!((b.limit, b.used, b.reset), (5000, 3913, 1795451686));
        let without_used = json!({"limit": 5000, "remaining": 1000,
                                  "resetAt": "2026-11-23T16:34:46Z"});
        assert_eq!(from_graphql_block(&without_used).unwrap().used, 4000);

        let endpoint = json!({"resources": {
            "core": {"limit": 5000, "used": 133, "remaining": 4867, "reset": 1795452483},
            "graphql": {"limit": 5000, "used": 889, "remaining": 4111, "reset": 1795452017}}});
        assert_eq!(
            from_rate_limit_endpoint(&endpoint, "graphql"),
            Some(Budget {
                limit: 5000,
                used: 889,
                reset: 1795452017
            })
        );
        assert_eq!(from_rate_limit_endpoint(&endpoint, "search"), None);

        let raw = "HTTP/2.0 200 OK\r\nAuthorization: token SECRET\r\nX-Ratelimit-Limit: 5000\r\n\
                   X-Ratelimit-Remaining: 1104\r\nX-Ratelimit-Reset: 1795451686\r\n\
                   X-Ratelimit-Resource: graphql\r\nX-Ratelimit-Used: 3896\r\n\r\n{}";
        let (resource, b) = from_headers(raw).unwrap();
        assert_eq!(resource, "graphql");
        assert_eq!((b.limit, b.used, b.reset), (5000, 3896, 1795451686));
        assert_eq!(from_headers("{}"), None);
        assert_eq!(
            from_headers("HTTP/2.0 200 OK\r\nX-Ratelimit-Limit: 5000\r\n\r\n{}"),
            None
        );
    }

    #[test]
    fn a_pause_is_visible_only_while_it_lasts() {
        let until = Utc::now() + chrono::Duration::minutes(30);
        let pause = CapacityPause {
            resource: "graphql".into(),
            used: 4000,
            limit: 5000,
            max_use: 0.8,
            until,
        };
        {
            let _guard = PauseGuard::start(pause.clone());
            assert!(current_pause().is_some_and(|p| p.until >= until));
        }
        assert!(current_pause().is_none_or(|p| p.until != until));
    }
}
