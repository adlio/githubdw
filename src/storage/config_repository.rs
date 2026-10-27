//! Warehouse settings stored in the `config` key/value table.

use rusqlite::{Connection, OptionalExtension};

use crate::error::Result;

/// The value stored under `key`, if any.
pub fn get(conn: &Connection, key: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row("SELECT value FROM config WHERE key = ?1", [key], |row| {
            row.get(0)
        })
        .optional()?)
}

/// Store `value` under `key`, replacing any previous value.
pub fn set(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO config (key, value) VALUES (?1, ?2)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        [key, value],
    )?;
    Ok(())
}

/// Every setting, ordered by key.
pub fn list(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut statement = conn.prepare("SELECT key, value FROM config ORDER BY key")?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GithubDW;

    #[test]
    fn set_then_get_round_trips_and_overwrites() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        assert_eq!(get(conn, "example").unwrap(), None);
        set(conn, "example", "one").unwrap();
        set(conn, "example", "two").unwrap();
        assert_eq!(get(conn, "example").unwrap().as_deref(), Some("two"));
        assert!(
            list(conn)
                .unwrap()
                .contains(&("example".into(), "two".into()))
        );
    }
}
