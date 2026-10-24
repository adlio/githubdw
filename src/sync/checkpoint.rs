//! Durable resume positions and committable cursors for the walks.

/// `sync_metadata.source_type` under which an unfinished repository PR walk
/// keeps its resume position. Distinct from `repo`, so the real high-water
/// cursor is never touched until a walk completes.
pub const REPO_PR_CHECKPOINT: &str = "repo_pr_checkpoint";

/// What a newest-first walk may safely commit as its high-water mark.
///
/// A walk serves items `updated_at` DESC and later runs stop at the first item
/// strictly older than the committed cursor. So the committable value is the
/// newest landed item, capped at the OLDEST failed item: anything below that
/// either landed or sat below the previous cursor, and the failed item itself
/// (equal to the cap) is re-read because the stop is strict.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct WalkWatermark {
    pub(super) max_landed: Option<String>,
    pub(super) min_failed: Option<String>,
    /// A failure whose `updated_at` is unknown cannot be bounded, so it pins
    /// the cursor where it was.
    #[serde(default)]
    pub(super) unbounded_failure: bool,
}

impl WalkWatermark {
    pub(super) fn landed(&mut self, updated: Option<&str>) {
        if let Some(updated) = updated
            && self.max_landed.as_deref().is_none_or(|max| updated > max)
        {
            self.max_landed = Some(updated.to_string());
        }
    }

    pub(super) fn failed(&mut self, updated: Option<&str>) {
        match updated {
            Some(updated) => {
                if self.min_failed.as_deref().is_none_or(|min| updated < min) {
                    self.min_failed = Some(updated.to_string());
                }
            }
            None => self.unbounded_failure = true,
        }
    }

    /// The cursor this walk may commit, or `None` to leave it unchanged.
    /// `advance_cursor` never lowers a stored cursor, so this is an upper
    /// bound on the move, never a rewind.
    pub(super) fn committable(&self) -> Option<String> {
        if self.unbounded_failure {
            return None;
        }
        match (&self.max_landed, &self.min_failed) {
            (Some(landed), Some(failed)) => Some(landed.min(failed).clone()),
            (Some(landed), None) => Some(landed.clone()),
            (None, Some(failed)) => Some(failed.clone()),
            (None, None) => None,
        }
    }
}

/// Durable resume position of an unfinished repository PR walk.
///
/// Bound to the high-water cursor the walk started from (`base`): a checkpoint
/// whose base no longer matches the stored cursor belongs to a different walk
/// and is discarded rather than trusted.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PullRequestWalkCheckpoint {
    pub(super) version: u32,
    pub(super) base: Option<String>,
    /// GraphQL `endCursor` of the last page whose items were all processed.
    pub(super) after: String,
    pub(super) watermark: WalkWatermark,
    #[serde(default)]
    pub(super) pages: i64,
    #[serde(default)]
    pub(super) items: i64,
    /// Times this walk restarted from the newest page after GitHub rejected
    /// the saved cursor.
    #[serde(default)]
    pub(super) restarts: u32,
}

pub(super) const CHECKPOINT_VERSION: u32 = 1;

/// Search offsets are mutable. Persist completed time windows instead, splitting
/// until every leaf fits in one response (no offset can skip a moved record).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct UserUpdateCheckpoint {
    pub(super) version: u32,
    pub(super) base: String,
    pub(super) upper: i64,
    pub(super) pending: Vec<(i64, i64)>,
}
