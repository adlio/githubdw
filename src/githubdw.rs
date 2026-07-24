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
