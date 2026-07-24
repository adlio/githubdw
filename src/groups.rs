//! Custom local groups of users or repositories.

use rusqlite::{Connection, OptionalExtension, params};

use crate::error::{Error, Result};

/// Which kind a named group is.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GroupKind {
    User,
    Repo,
}

fn tables(kind: GroupKind) -> (&'static str, &'static str, &'static str) {
    match kind {
        GroupKind::User => ("user_group", "user_group_member", "login"),
        GroupKind::Repo => ("repo_group", "repo_group_member", "repo"),
    }
}

pub fn create_group(
    conn: &Connection,
    kind: GroupKind,
    name: &str,
    description: Option<&str>,
) -> Result<()> {
    let (group_table, _, _) = tables(kind);
    conn.execute(
        &format!(
            "INSERT INTO {group_table} (name, description) VALUES (?1, ?2)
             ON CONFLICT (name) DO UPDATE SET description = COALESCE(excluded.description, {group_table}.description)"
        ),
        params![name, description],
    )?;
    Ok(())
}

pub fn delete_group(conn: &Connection, kind: GroupKind, name: &str) -> Result<()> {
    let (group_table, member_table, _) = tables(kind);
    conn.execute(
        &format!("DELETE FROM {member_table} WHERE group_name = ?1"),
        [name],
    )?;
    let removed = conn.execute(
        &format!("DELETE FROM {group_table} WHERE name = ?1"),
        [name],
    )?;
    if removed == 0 {
        return Err(Error::NotFound(format!("group '{name}'")));
    }
    Ok(())
}

pub fn add_members(
    conn: &Connection,
    kind: GroupKind,
    name: &str,
    members: &[String],
) -> Result<()> {
    let (group_table, member_table, member_column) = tables(kind);
    let exists: Option<String> = conn
        .query_row(
            &format!("SELECT name FROM {group_table} WHERE name = ?1"),
            [name],
            |row| row.get(0),
        )
        .optional()?;
    if exists.is_none() {
        return Err(Error::NotFound(format!("group '{name}'")));
    }
    for member in members {
        conn.execute(
            &format!(
                "INSERT OR IGNORE INTO {member_table} (group_name, {member_column}) VALUES (?1, ?2)"
            ),
            params![name, member.to_lowercase()],
        )?;
    }
    Ok(())
}

pub fn remove_members(
    conn: &Connection,
    kind: GroupKind,
    name: &str,
    members: &[String],
) -> Result<()> {
    let (_, member_table, member_column) = tables(kind);
    for member in members {
        conn.execute(
            &format!("DELETE FROM {member_table} WHERE group_name = ?1 AND {member_column} = ?2"),
            params![name, member.to_lowercase()],
        )?;
    }
    Ok(())
}

/// Replace the entire member set.
pub fn set_members(
    conn: &Connection,
    kind: GroupKind,
    name: &str,
    members: &[String],
) -> Result<()> {
    let (_, member_table, _) = tables(kind);
    conn.execute(
        &format!("DELETE FROM {member_table} WHERE group_name = ?1"),
        [name],
    )?;
    add_members(conn, kind, name, members)
}

pub fn members(conn: &Connection, kind: GroupKind, name: &str) -> Result<Vec<String>> {
    let (group_table, member_table, member_column) = tables(kind);
    let exists: Option<String> = conn
        .query_row(
            &format!("SELECT name FROM {group_table} WHERE name = ?1"),
            [name],
            |row| row.get(0),
        )
        .optional()?;
    if exists.is_none() {
        return Err(Error::NotFound(format!("group '{name}'")));
    }
    let mut statement = conn.prepare(&format!(
        "SELECT {member_column} FROM {member_table} WHERE group_name = ?1 ORDER BY 1"
    ))?;
    let rows = statement.query_map([name], |row| row.get(0))?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

pub fn list_groups(
    conn: &Connection,
    kind: GroupKind,
) -> Result<Vec<(String, Option<String>, i64)>> {
    let (group_table, member_table, _) = tables(kind);
    let mut statement = conn.prepare(&format!(
        "SELECT g.name, g.description, COUNT(m.group_name)
         FROM {group_table} g
         LEFT JOIN {member_table} m ON m.group_name = g.name
         GROUP BY g.name ORDER BY g.name"
    ))?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// Find which kind a group name refers to (user first, then repo).
pub fn kind_of(conn: &Connection, name: &str) -> Result<GroupKind> {
    let user: Option<String> = conn
        .query_row(
            "SELECT name FROM user_group WHERE name = ?1",
            [name],
            |row| row.get(0),
        )
        .optional()?;
    if user.is_some() {
        return Ok(GroupKind::User);
    }
    let repo: Option<String> = conn
        .query_row(
            "SELECT name FROM repo_group WHERE name = ?1",
            [name],
            |row| row.get(0),
        )
        .optional()?;
    if repo.is_some() {
        return Ok(GroupKind::Repo);
    }
    Err(Error::NotFound(format!("group '{name}'")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GithubDW;

    #[test]
    fn group_crud_round_trip() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();

        create_group(conn, GroupKind::User, "core", Some("core team")).unwrap();
        add_members(
            conn,
            GroupKind::User,
            "core",
            &["Alice".into(), "bob".into()],
        )
        .unwrap();
        assert_eq!(
            members(conn, GroupKind::User, "core").unwrap(),
            vec!["alice", "bob"]
        );

        remove_members(conn, GroupKind::User, "core", &["alice".into()]).unwrap();
        assert_eq!(members(conn, GroupKind::User, "core").unwrap(), vec!["bob"]);

        set_members(conn, GroupKind::User, "core", &["carol".into()]).unwrap();
        assert_eq!(
            members(conn, GroupKind::User, "core").unwrap(),
            vec!["carol"]
        );

        create_group(conn, GroupKind::Repo, "backend", None).unwrap();
        add_members(conn, GroupKind::Repo, "backend", &["octo/alpha".into()]).unwrap();
        assert_eq!(kind_of(conn, "core").unwrap(), GroupKind::User);
        assert_eq!(kind_of(conn, "backend").unwrap(), GroupKind::Repo);
        assert!(kind_of(conn, "missing").is_err());

        let listed = list_groups(conn, GroupKind::User).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].2, 1);

        delete_group(conn, GroupKind::User, "core").unwrap();
        assert!(members(conn, GroupKind::User, "core").is_err());
        assert!(add_members(conn, GroupKind::User, "core", &["x".into()]).is_err());
    }
}
