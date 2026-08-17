//! Monitored users/repos/orgs management.

use rusqlite::Connection;

use crate::error::{Error, Result};
use crate::query::to_bare_login;

/// A monitored source, uniformly described.
#[derive(Debug, Clone, PartialEq)]
pub struct MonitoredSource {
    pub source_type: String, // 'user' | 'repo' | 'org'
    pub identifier: String,
    pub last_sync_at: Option<String>,
    pub sync_enabled: bool,
}

pub fn add_repo(conn: &Connection, repository: &str) -> Result<()> {
    if !repository.contains('/') {
        return Err(Error::InvalidArgument(format!(
            "expected owner/name, got '{repository}'"
        )));
    }
    conn.execute(
        "INSERT OR IGNORE INTO monitored_repos (repo_key) VALUES (?1)",
        [repository.to_lowercase()],
    )?;
    Ok(())
}

pub fn add_user(conn: &Connection, login: &str) -> Result<()> {
    // `monitored_users.login` is declared bare, so a prefixed spelling has to be
    // reduced here — stored verbatim it would be a row no read path can reach.
    conn.execute(
        "INSERT OR IGNORE INTO monitored_users (login) VALUES (?1)",
        [to_bare_login(login)],
    )?;
    Ok(())
}

pub fn add_org(conn: &Connection, org: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO monitored_orgs (org_login) VALUES (?1)",
        [org.to_lowercase()],
    )?;
    Ok(())
}

/// Remove a source by identifier from whichever table matches. Returns how
/// many rows were removed.
pub fn remove(conn: &Connection, identifier: &str) -> Result<usize> {
    let identifier = identifier.to_lowercase();
    let mut removed = 0;
    removed += conn.execute(
        "DELETE FROM monitored_repos WHERE repo_key = ?1",
        [&identifier],
    )?;
    removed += conn.execute(
        "DELETE FROM monitored_users WHERE login = ?1",
        // Same reduction as `add_user`, so a source added by either spelling is
        // removable by either spelling.
        [to_bare_login(&identifier)],
    )?;
    removed += conn.execute(
        "DELETE FROM monitored_orgs WHERE org_login = ?1",
        [&identifier],
    )?;
    Ok(removed)
}

/// All monitored sources across the three tables.
pub fn list(conn: &Connection) -> Result<Vec<MonitoredSource>> {
    let mut sources = Vec::new();
    let mut statement = conn.prepare(
        "SELECT 'repo', repo_key, last_sync_at, sync_enabled FROM monitored_repos
         UNION ALL
         SELECT 'user', login, last_sync_at, sync_enabled FROM monitored_users
         UNION ALL
         SELECT 'org', org_login, last_sync_at, sync_enabled FROM monitored_orgs
         ORDER BY 1, 2",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(MonitoredSource {
            source_type: row.get(0)?,
            identifier: row.get(1)?,
            last_sync_at: row.get(2)?,
            sync_enabled: row.get::<_, i64>(3)? == 1,
        })
    })?;
    for row in rows {
        sources.push(row?);
    }
    Ok(sources)
}

/// Stamp last_sync_at for a repo source.
pub fn touch_repo(conn: &Connection, repository: &str) -> Result<()> {
    conn.execute(
        "UPDATE monitored_repos SET last_sync_at = datetime('now') WHERE repo_key = ?1",
        [repository.to_lowercase()],
    )?;
    Ok(())
}

/// Stamp last_sync_at for a user source.
///
/// The login is reduced the same way `add_user` reduces it, so a row added by
/// either spelling is the row this stamps.
pub fn touch_user(conn: &Connection, login: &str) -> Result<()> {
    conn.execute(
        "UPDATE monitored_users SET last_sync_at = datetime('now') WHERE login = ?1",
        [to_bare_login(login)],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GithubDW;

    #[test]
    fn add_list_remove_round_trip() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        add_repo(conn, "Octocat/Hello").unwrap();
        add_user(conn, "octocat").unwrap();
        add_org(conn, "acme").unwrap();

        let sources = list(conn).unwrap();
        assert_eq!(sources.len(), 3);
        assert!(sources.iter().any(|s| s.identifier == "octocat/hello"));

        assert_eq!(remove(conn, "octocat/hello").unwrap(), 1);
        assert_eq!(list(conn).unwrap().len(), 2);
    }

    #[test]
    fn add_repo_requires_owner_name_form() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        assert!(add_repo(warehouse.connection(), "not-a-repo").is_err());
    }

    /// A namespace-prefixed login must be reduced to the bare form the column is
    /// declared to hold, or the row is unreachable by every read path.
    #[test]
    fn prefixed_user_input_is_stored_bare() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        add_user(conn, "user:Octocat").unwrap();
        add_user(conn, "bot:GitHub-Actions").unwrap();

        let stored: Vec<String> = list(conn)
            .unwrap()
            .into_iter()
            .filter(|source| source.source_type == "user")
            .map(|source| source.identifier)
            .collect();
        assert_eq!(stored, vec!["github-actions", "octocat"]);

        // Adding the bare spelling of an existing row is a no-op, not a duplicate.
        add_user(conn, "octocat").unwrap();
        assert_eq!(
            list(conn)
                .unwrap()
                .iter()
                .filter(|source| source.source_type == "user")
                .count(),
            2
        );

        // Removal accepts either spelling.
        assert_eq!(remove(conn, "user:octocat").unwrap(), 1);
        assert_eq!(remove(conn, "github-actions").unwrap(), 1);
        assert!(list(conn).unwrap().is_empty());
    }
}
