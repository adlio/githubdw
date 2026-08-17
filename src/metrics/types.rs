//! Metrics value types.

use serde::Serialize;

use crate::query::{display_identity, entity_namespace as entity_namespace_of, to_bare_login};

/// A single measure compared against the previous period.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MetricWithDelta {
    pub current: u64,
    pub previous: u64,
    pub delta: i64,
    pub delta_percent: Option<f64>,
}

impl MetricWithDelta {
    pub fn new(current: u64, previous: u64) -> Self {
        let delta = current as i64 - previous as i64;
        let delta_percent = if previous == 0 {
            None
        } else {
            Some((delta as f64 / previous as f64) * 100.0)
        };
        Self {
            current,
            previous,
            delta,
            delta_percent,
        }
    }

    /// Render like `42 (+8, +23%)`.
    pub fn render(&self) -> String {
        if self.delta == 0 {
            format!("{} (no change)", self.current)
        } else {
            match self.delta_percent {
                Some(percent) => format!("{} ({:+}, {:+.0}%)", self.current, self.delta, percent),
                None => format!("{} ({:+})", self.current, self.delta),
            }
        }
    }
}

/// A ranked entity (user or repo) for leaderboards.
#[derive(Debug, Clone, Serialize)]
pub struct EntityMetric {
    pub entity_key: String,
    pub entity_name: Option<String>,
    pub current: u64,
    pub previous: u64,
    pub delta: i64,
    pub rank_current: u32,
    pub rank_previous: Option<u32>,
}

impl EntityMetric {
    /// Render like `#1 alice — 42 (+8) [was #3]`.
    ///
    /// With no display name to fall back on, the key is reduced to its bare
    /// login rather than shown verbatim: a leaderboard row is read by a person,
    /// so it should never be the first place a namespaced key surfaces.
    /// Repository keys carry no namespace and pass through untouched.
    pub fn render(&self) -> String {
        let fallback = display_identity(
            &to_bare_login(&self.entity_key),
            &entity_namespace_of(&self.entity_key),
        );
        let name = self.entity_name.as_deref().unwrap_or(fallback.as_str());
        let movement = match self.rank_previous {
            Some(previous_rank) if previous_rank != self.rank_current => {
                format!(" [was #{previous_rank}]")
            }
            Some(_) => String::new(),
            None => " [new]".to_string(),
        };
        format!(
            "#{} {name} — {} ({:+}){movement}",
            self.rank_current, self.current, self.delta
        )
    }
}

/// Headline metrics for one user.
#[derive(Debug, Serialize)]
pub struct UserMetrics {
    /// The login, exactly as GitHub spells it — never namespaced.
    pub login: String,
    /// The namespace that login was stored under: `user` or `bot`.
    pub entity_type: String,
    /// The warehouse key the report is labeled with, for callers joining back
    /// to the star schema. Always `entity_type` + `:` + `login`. A bare login
    /// that resolves to more than one entity is measured across all of them and
    /// labeled with the first.
    pub entity_key: String,
    pub period_key: String,
    pub previous_period_key: String,
    pub current_date_range: (String, String),
    pub previous_date_range: (String, String),
    pub prs_opened: MetricWithDelta,
    pub prs_merged: MetricWithDelta,
    pub reviews_given: MetricWithDelta,
    pub reviews_received: MetricWithDelta,
    pub comments_given: MetricWithDelta,
    pub lines_added: MetricWithDelta,
    pub lines_removed: MetricWithDelta,
}

impl UserMetrics {
    /// The report's subject for a single-line header: the bare login, with a
    /// bot marked so it cannot be read as a person of the same name.
    pub fn display_login(&self) -> String {
        display_identity(&self.login, &self.entity_type)
    }
}

/// Leaderboards for one user.
#[derive(Debug, Serialize)]
pub struct UserAggregations {
    pub top_repos: Vec<EntityMetric>,
    pub top_reviewers: Vec<EntityMetric>,
    pub top_reviewed_authors: Vec<EntityMetric>,
}

/// Headline metrics for one repository.
#[derive(Debug, Serialize)]
pub struct RepoMetrics {
    pub repository: String,
    pub period_key: String,
    pub previous_period_key: String,
    pub current_date_range: (String, String),
    pub previous_date_range: (String, String),
    pub prs_opened: MetricWithDelta,
    pub prs_merged: MetricWithDelta,
    pub total_reviews: MetricWithDelta,
    pub total_comments: MetricWithDelta,
    pub check_failure_rate: Option<f64>,
}

/// Leaderboards for one repository.
#[derive(Debug, Serialize)]
pub struct RepoAggregations {
    pub top_contributors: Vec<EntityMetric>,
    pub top_mergers: Vec<EntityMetric>,
    pub top_reviewers: Vec<EntityMetric>,
    pub top_commenters: Vec<EntityMetric>,
}

/// Headline metrics for a user group.
#[derive(Debug, Serialize)]
pub struct GroupMetrics {
    pub group_name: String,
    pub kind: String, // 'user' | 'repo'
    pub member_count: u64,
    pub period_key: String,
    pub previous_period_key: String,
    pub prs_opened: MetricWithDelta,
    pub prs_merged: MetricWithDelta,
    pub reviews_given: Option<MetricWithDelta>,
    pub total_reviews: Option<MetricWithDelta>,
    pub total_comments: Option<MetricWithDelta>,
}

/// Leaderboards for a group.
#[derive(Debug, Serialize)]
pub struct GroupAggregations {
    pub top_members: Vec<EntityMetric>,
    pub top_repos: Vec<EntityMetric>,
    pub top_external_reviewers: Vec<EntityMetric>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_math_and_rendering() {
        let metric = MetricWithDelta::new(42, 34);
        assert_eq!(metric.delta, 8);
        assert!((metric.delta_percent.unwrap() - 23.5).abs() < 0.1);
        assert_eq!(metric.render(), "42 (+8, +24%)");

        let no_change = MetricWithDelta::new(5, 5);
        assert_eq!(no_change.render(), "5 (no change)");

        let from_zero = MetricWithDelta::new(3, 0);
        assert_eq!(from_zero.delta_percent, None);
        assert_eq!(from_zero.render(), "3 (+3)");
    }

    #[test]
    fn leaderboard_rendering() {
        let riser = EntityMetric {
            entity_key: "user:alice".into(),
            entity_name: Some("alice".into()),
            current: 42,
            previous: 34,
            delta: 8,
            rank_current: 1,
            rank_previous: Some(3),
        };
        assert_eq!(riser.render(), "#1 alice — 42 (+8) [was #3]");

        let newcomer = EntityMetric {
            entity_key: "user:bob".into(),
            entity_name: Some("bob".into()),
            current: 10,
            previous: 0,
            delta: 10,
            rank_current: 2,
            rank_previous: None,
        };
        assert_eq!(newcomer.render(), "#2 bob — 10 (+10) [new]");
    }

    /// A leaderboard row whose entity has no display name still must not fall
    /// back to the namespaced key — the row is read by a person either way.
    #[test]
    fn render_falls_back_to_the_bare_login() {
        let nameless = EntityMetric {
            entity_key: "user:carol".into(),
            entity_name: None,
            current: 5,
            previous: 5,
            delta: 0,
            rank_current: 3,
            rank_previous: Some(3),
        };
        assert_eq!(nameless.render(), "#3 carol — 5 (+0)");

        let nameless_bot = EntityMetric {
            entity_key: "bot:builder".into(),
            entity_name: None,
            current: 7,
            previous: 2,
            delta: 5,
            rank_current: 1,
            rank_previous: Some(1),
        };
        assert_eq!(nameless_bot.render(), "#1 builder [bot] — 7 (+5)");

        // A repository key is not namespaced and passes through unchanged.
        let repository = EntityMetric {
            entity_key: "octo/alpha".into(),
            entity_name: None,
            current: 3,
            previous: 3,
            delta: 0,
            rank_current: 2,
            rank_previous: Some(2),
        };
        assert_eq!(repository.render(), "#2 octo/alpha — 3 (+0)");
    }
}
