//! Metrics value types.

use serde::Serialize;

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
    pub fn render(&self) -> String {
        let name = self
            .entity_name
            .as_deref()
            .unwrap_or(self.entity_key.as_str());
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
    pub login: String,
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
}
