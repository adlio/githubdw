//! The `GithubDW` facade: owns the SQLite connection and wires components.

use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::error::{Error, Result};
use crate::storage::schema;

/// Default database directory name under the user's home directory.
const DEFAULT_DIRECTORY: &str = ".githubdw";
/// Default database file name.
const DEFAULT_FILE_NAME: &str = "githubdw.db";

/// Facade over the warehouse: opens the database, applies migrations, and
/// exposes the library API surface.
pub struct GithubDW {
    connection: Connection,
    database_path: PathBuf,
}

impl GithubDW {
    /// Open (creating if needed) the warehouse at the default path
    /// `~/.githubdw/githubdw.db`.
    pub fn open_default() -> Result<Self> {
        Self::open(Self::default_database_path()?)
    }

    /// Open (creating if needed) the warehouse at an explicit path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let mut connection = Connection::open(&path)?;
        schema::init(&mut connection)?;
        Ok(Self {
            connection,
            database_path: path,
        })
    }

    /// Open an existing warehouse without creating directories or applying migrations.
    /// Mutating SQL is rejected by SQLite on this connection.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let database_path = path.as_ref().to_path_buf();
        let connection = Connection::open_with_flags(
            &database_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        Ok(Self {
            connection,
            database_path,
        })
    }

    /// Open an in-memory warehouse (used by tests).
    pub fn open_in_memory() -> Result<Self> {
        let mut connection = Connection::open_in_memory()?;
        schema::init(&mut connection)?;
        Ok(Self {
            connection,
            database_path: PathBuf::from(":memory:"),
        })
    }

    /// The default database path: `~/.githubdw/githubdw.db`.
    pub fn default_database_path() -> Result<PathBuf> {
        let home = dirs::home_dir()
            .ok_or_else(|| Error::Config("could not determine home directory".into()))?;
        Ok(home.join(DEFAULT_DIRECTORY).join(DEFAULT_FILE_NAME))
    }

    /// Path of the open database file.
    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    /// Borrow the underlying connection (library-internal plumbing).
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Mutably borrow the underlying connection.
    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_open_never_migrates_or_accepts_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE original(value TEXT); INSERT INTO original VALUES ('kept');",
        )
        .unwrap();
        drop(conn);
        let warehouse = GithubDW::open_read_only(&path).unwrap();
        assert_eq!(
            warehouse
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert!(
            warehouse
                .connection()
                .execute("DELETE FROM original", [])
                .is_err()
        );
        assert_eq!(
            warehouse
                .connection()
                .query_row("SELECT value FROM original", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "kept"
        );
    }

    #[test]
    fn read_only_open_does_not_create_a_missing_warehouse() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("warehouse.db");
        assert!(GithubDW::open_read_only(&path).is_err());
        assert!(!path.parent().unwrap().exists());
    }

    #[test]
    fn opens_at_explicit_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.db");
        let warehouse = GithubDW::open(&path).expect("open warehouse");
        assert!(path.exists());
        assert_eq!(warehouse.database_path(), path.as_path());
    }

    #[test]
    fn opens_in_memory() {
        let warehouse = GithubDW::open_in_memory().expect("open in-memory");
        let count: i64 = warehouse
            .connection()
            .query_row("SELECT COUNT(*) FROM config", [], |row| row.get(0))
            .expect("query config");
        assert!(count >= 4);
    }
}
