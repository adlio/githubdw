//! Diff retention: whether a sync stores the raw diff text it fetches.
//!
//! Every sync entry point takes a [`DiffRetention`] from its caller. The
//! library reads no file and no environment to decide it: the CLI resolves
//! `--diff-retention`, then the warehouse's `diff_retention` config value
//! ([`CONFIG_KEY`]), then [`DEFAULT`]; an application passes its own setting.
//!
//! Callers that disagree accumulate rather than undo each other:
//!
//! * A `Retain` run stores the diff text it fetches, and
//!   [`crate::collect::tick`] fills in diff text missing for pull requests
//!   already stored, a bounded number per tick under the rate cap.
//! * A `Transient` or `Never` run fetches no diff text and removes nothing.
//!
//! Every write preserves stored payloads whatever the retention: no stored
//! title, body, review, comment or patch is replaced by an empty value, and
//! no stored file row is deleted. Titles, descriptions, review bodies and
//! comments are always kept when fetched; only raw diff text
//! (`fact_file_diffs.patch`) is governed here.

use serde::{Deserialize, Serialize};

/// Config key (`githubdw config set diff_retention transient`) read by the CLI.
pub const CONFIG_KEY: &str = "diff_retention";

/// The CLI's retention when neither the flag nor the config key is set. It
/// keeps the behaviour of earlier releases, which stored every patch fetched.
pub const DEFAULT: DiffRetention = DiffRetention::Retain;

/// How newly fetched raw diff text may be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiffRetention {
    /// Do not fetch diff text at all.
    #[serde(alias = "none")]
    Never,
    /// Diff text may be fetched for viewing or AI and held in memory only;
    /// a sync fetches none.
    Transient,
    /// Diff text is fetched and written to the warehouse.
    Retain,
}

impl DiffRetention {
    pub fn as_str(self) -> &'static str {
        match self {
            DiffRetention::Never => "never",
            DiffRetention::Transient => "transient",
            DiffRetention::Retain => "retain",
        }
    }

    /// Parse `never` (or `none`), `transient` or `retain`, ignoring case and
    /// surrounding space.
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "none" | "never" => Some(DiffRetention::Never),
            "transient" => Some(DiffRetention::Transient),
            "retain" => Some(DiffRetention::Retain),
            _ => None,
        }
    }

    /// [`Self::parse`], with an error that names the accepted values.
    pub fn parse_setting(text: &str) -> crate::Result<Self> {
        Self::parse(text).ok_or_else(|| {
            crate::Error::InvalidArgument(format!(
                "diff_retention must be never, transient or retain, not '{}'",
                text.trim()
            ))
        })
    }
}

impl std::fmt::Display for DiffRetention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_names_round_trip_and_unknown_names_are_rejected() {
        for retention in [
            DiffRetention::Never,
            DiffRetention::Transient,
            DiffRetention::Retain,
        ] {
            assert_eq!(DiffRetention::parse(retention.as_str()), Some(retention));
            assert_eq!(retention.to_string(), retention.as_str());
        }
        assert_eq!(DiffRetention::parse(" none "), Some(DiffRetention::Never));
        assert_eq!(DiffRetention::parse("RETAIN"), Some(DiffRetention::Retain));
        assert_eq!(DiffRetention::parse("forever"), None);
        assert_eq!(DiffRetention::parse(""), None);
        let error = DiffRetention::parse_setting("forever").unwrap_err();
        assert!(error.to_string().contains("never, transient or retain"));
    }

    #[test]
    fn serde_uses_the_setting_names_and_reads_the_old_none() {
        assert_eq!(
            serde_json::to_string(&DiffRetention::Never).unwrap(),
            "\"never\""
        );
        let old: DiffRetention = serde_json::from_str("\"none\"").unwrap();
        assert_eq!(old, DiffRetention::Never);
    }
}
