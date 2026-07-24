//! Monitored users/repos/orgs management.

use rusqlite::Connection;

use crate::error::{Error, Result};

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
    conn.execute(
        "INSERT OR IGNORE INTO monitored_users (login) VALUES (?1)",
        [login.to_lowercase()],
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
        [&identifier],
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
}
