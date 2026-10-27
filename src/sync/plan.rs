//! `sync all` planning over the monitored source list.

use super::*;

/// What a `sync all` run will do with the monitored source list.
///
/// The plan exists so no enabled source can be dropped without a word: every
/// source is either in a bucket this build can serve, or named in `notices`.
#[derive(Debug, Default, PartialEq)]
pub struct SyncPlan {
    /// Repositories to sync, in list order.
    pub repos: Vec<String>,
    /// Monitored users to sync, in list order.
    pub users: Vec<String>,
    /// Lines the caller must print: sources this run will not serve, and why.
    pub notices: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SyncSource {
    User(String),
    Repository(String),
}

impl SyncSource {
    pub(super) fn key(&self) -> String {
        match self {
            Self::User(login) => format!("user:{login}"),
            Self::Repository(repo) => format!("repo:{repo}"),
        }
    }
}

impl SyncPlan {
    /// Interleave users and repositories, resuming after the last source
    /// attempted. The caller records the next source BEFORE starting work, so
    /// an interrupted expensive source cannot starve every subsequent run.
    pub fn fair_sources(&self, conn: &Connection) -> Result<Vec<SyncSource>> {
        let mut sources = Vec::new();
        for index in 0..self.users.len().max(self.repos.len()) {
            if let Some(login) = self.users.get(index) {
                sources.push(SyncSource::User(login.clone()));
            }
            if let Some(repo) = self.repos.get(index) {
                sources.push(SyncSource::Repository(repo.clone()));
            }
        }
        if let Some(next) = sync_state::load_checkpoint(conn, "sync_all", "next_source")?
            && let Some(position) = sources.iter().position(|source| source.key() == next)
        {
            sources.rotate_left(position);
        }
        Ok(sources)
    }

    pub fn checkpoint_next_source(conn: &Connection, next: &SyncSource) -> Result<()> {
        sync_state::save_checkpoint(conn, "sync_all", "next_source", &next.key())
    }

    /// True when there is nothing at all to do.
    pub fn is_empty(&self) -> bool {
        self.repos.is_empty() && self.users.is_empty()
    }
}

/// Build the plan for `sync all` from the monitored source list.
pub fn plan_all(sources: &[MonitoredSource]) -> SyncPlan {
    let mut plan = SyncPlan::default();
    let mut unsupported: Vec<(String, String)> = Vec::new();
    let mut disabled: Vec<String> = Vec::new();

    for source in sources {
        if !source.sync_enabled {
            disabled.push(source.identifier.clone());
            continue;
        }
        match source.source_type.as_str() {
            "repo" => plan.repos.push(source.identifier.clone()),
            "user" => plan.users.push(source.identifier.clone()),
            other => unsupported.push((other.to_string(), source.identifier.clone())),
        }
    }

    for (source_type, identifier) in &unsupported {
        plan.notices.push(format!(
            "skipping {source_type} '{identifier}': {source_type}-scoped sync is not implemented \
             — add the repositories or users you want covered"
        ));
    }
    if !disabled.is_empty() {
        plan.notices.push(format!(
            "skipping {} disabled source(s): {}",
            disabled.len(),
            disabled.join(", ")
        ));
    }
    if plan.is_empty() {
        plan.notices.push(
            "nothing enabled to sync — use `githubdw monitor add-repo` or `monitor add-user`"
                .to_string(),
        );
    }
    plan
}
