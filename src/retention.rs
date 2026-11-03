//! Collection policy: how much of what is fetched may be kept.
//!
//! The authority is a small JSON file shared by every process that writes a
//! warehouse (the dashboard and the scheduled CLI syncs):
//! `$XDG_CONFIG_HOME/adlio-collection/policy.json`, falling back to
//! `~/.config/adlio-collection/policy.json`.
//!
//! ```json
//! {"version":1,"mode":"counts|basic|full","ai":false,
//!  "diff_retention":"none|transient|retain","updated_at":"RFC3339"}
//! ```
//!
//! Rules, applied at every write boundary and never cached:
//!
//! * **File missing** -> standalone mode. The caller's own request decides,
//!   which keeps the pre-policy CLI behaviour byte for byte.
//! * **File present but unreadable or invalid** -> fail closed: no new diff
//!   text is written.
//! * **File present and valid** -> it is a *ceiling*. A caller (or CLI flag)
//!   asking for `Retain` is a request constrained by the ceiling, never an
//!   override. While a valid file is present the warehouse is in app mode and
//!   every write preserves existing payloads: no stored title, body, comment
//!   or patch is replaced by an empty value, and no stored row is deleted.
//!
//! Titles, descriptions, review bodies and comments are always kept when they
//! are fetched. Only raw diff text (`fact_file_diffs.patch`) is governed.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// How newly fetched raw diff text may be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiffRetention {
    /// Do not fetch diff text at all.
    #[serde(rename = "none")]
    Never,
    /// Diff text may be fetched for viewing or AI and held in memory only.
    Transient,
    /// Diff text may be written to the warehouse.
    Retain,
}

impl DiffRetention {
    pub fn as_str(self) -> &'static str {
        match self {
            DiffRetention::Never => "none",
            DiffRetention::Transient => "transient",
            DiffRetention::Retain => "retain",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "none" | "never" => Some(DiffRetention::Never),
            "transient" => Some(DiffRetention::Transient),
            "retain" => Some(DiffRetention::Retain),
            _ => None,
        }
    }
}

/// The three dashboard presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CollectionMode {
    Counts,
    Basic,
    Full,
}

/// The parsed policy file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyFile {
    pub version: u32,
    pub mode: CollectionMode,
    #[serde(default)]
    pub ai: bool,
    pub diff_retention: DiffRetention,
    #[serde(default)]
    pub updated_at: Option<String>,
}

/// What the policy file said at one instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyState {
    /// No file: standalone mode, legacy behaviour.
    Missing,
    /// A valid file.
    Present(PolicyFile),
    /// A file that exists but cannot be read or parsed. Fails closed.
    Invalid(String),
}

impl PolicyState {
    /// True when a policy file exists (valid or not): the warehouse belongs to
    /// an initialized app, so every write must preserve existing payloads.
    pub fn app_mode(&self) -> bool {
        !matches!(self, PolicyState::Missing)
    }

    /// The ceiling on diff retention, or `None` in standalone mode.
    pub fn ceiling(&self) -> Option<DiffRetention> {
        match self {
            PolicyState::Missing => None,
            PolicyState::Present(file) => Some(file.diff_retention),
            PolicyState::Invalid(_) => Some(DiffRetention::Never),
        }
    }

    /// The retention actually allowed for a caller that asked for `requested`.
    pub fn allow(&self, requested: DiffRetention) -> DiffRetention {
        match self.ceiling() {
            None => requested,
            Some(ceiling) => requested.min(ceiling),
        }
    }
}

/// The default policy file path: `$XDG_CONFIG_HOME/adlio-collection/policy.json`,
/// else `~/.config/adlio-collection/policy.json`.
pub fn default_policy_path() -> Option<PathBuf> {
    let base = match std::env::var_os("XDG_CONFIG_HOME") {
        Some(value) if !value.is_empty() => PathBuf::from(value),
        _ => dirs::home_dir()?.join(".config"),
    };
    Some(base.join("adlio-collection").join("policy.json"))
}

/// Read the policy file at `path` right now. Never cached.
pub fn read_policy(path: &Path) -> PolicyState {
    match std::fs::read(path) {
        Ok(bytes) => match serde_json::from_slice::<PolicyFile>(&bytes) {
            Ok(file) if file.version == 1 => PolicyState::Present(file),
            Ok(file) => PolicyState::Invalid(format!("unsupported version {}", file.version)),
            Err(error) => PolicyState::Invalid(format!("unparsable policy file: {error}")),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => PolicyState::Missing,
        Err(error) => PolicyState::Invalid(format!("unreadable policy file: {error}")),
    }
}

/// Write a policy file atomically (temporary file in the same directory, then
/// rename). Provided for tests and tools; the dashboard owns the live file.
pub fn write_policy(path: &Path, file: &PolicyFile) -> crate::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension(format!("json.tmp{}", std::process::id()));
    std::fs::write(&temporary, serde_json::to_vec(file)?)?;
    std::fs::rename(&temporary, path)?;
    Ok(())
}

/// Where a writer reads the policy from at each write boundary.
#[derive(Clone)]
pub enum PolicySource {
    /// The default shared file path, resolved at each read.
    DefaultFile,
    /// A specific file (tests, alternative deployments).
    File(PathBuf),
    /// No file: always standalone. For tests and legacy library callers that
    /// must not observe a shared file.
    Standalone,
    /// A caller-supplied reader (for example an engdash in-memory gate).
    Custom(Arc<dyn Fn() -> PolicyState + Send + Sync>),
}

impl std::fmt::Debug for PolicySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicySource::DefaultFile => write!(f, "DefaultFile"),
            PolicySource::File(path) => write!(f, "File({})", path.display()),
            PolicySource::Standalone => write!(f, "Standalone"),
            PolicySource::Custom(_) => write!(f, "Custom(..)"),
        }
    }
}

impl PolicySource {
    /// Read the current state. Called at every write boundary.
    pub fn current(&self) -> PolicyState {
        match self {
            PolicySource::DefaultFile => match default_policy_path() {
                Some(path) => read_policy(&path),
                // No home directory to look in: there is no shared file.
                None => PolicyState::Missing,
            },
            PolicySource::File(path) => read_policy(path),
            PolicySource::Standalone => PolicyState::Missing,
            PolicySource::Custom(reader) => reader(),
        }
    }
}

/// A writer's policy gate: the caller's request plus the live ceiling.
#[derive(Debug, Clone)]
pub struct RetentionGate {
    /// What the caller asked for (from `skip_diffs`, a CLI flag, or the API).
    pub requested: DiffRetention,
    pub source: PolicySource,
}

impl RetentionGate {
    pub fn new(requested: DiffRetention, source: PolicySource) -> Self {
        Self { requested, source }
    }

    /// Re-read the source and decide. Never cached.
    pub fn decide(&self) -> GateDecision {
        let state = self.source.current();
        GateDecision {
            allowed: state.allow(self.requested),
            app_mode: state.app_mode(),
            invalid: match &state {
                PolicyState::Invalid(reason) => Some(reason.clone()),
                _ => None,
            },
        }
    }

    /// True when new diff text may be written right now.
    pub fn retain_diffs_now(&self) -> bool {
        self.decide().allowed == DiffRetention::Retain
    }
}

/// One reading of the gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateDecision {
    pub allowed: DiffRetention,
    /// Preserve every stored payload and row (a policy file exists).
    pub app_mode: bool,
    /// Why the file was rejected, when it was.
    pub invalid: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(mode: CollectionMode, retention: DiffRetention) -> PolicyFile {
        PolicyFile {
            version: 1,
            mode,
            ai: mode != CollectionMode::Counts,
            diff_retention: retention,
            updated_at: Some("2026-11-03T13:00:00Z".into()),
        }
    }

    #[test]
    fn parses_the_contract_shape() {
        let text = r#"{"version":1,"mode":"basic","ai":true,"diff_retention":"transient","updated_at":"2026-11-03T13:16:00Z"}"#;
        let parsed: PolicyFile = serde_json::from_str(text).unwrap();
        assert_eq!(parsed.mode, CollectionMode::Basic);
        assert_eq!(parsed.diff_retention, DiffRetention::Transient);
    }

    #[test]
    fn missing_file_is_standalone_and_follows_the_request() {
        let dir = tempfile::tempdir().unwrap();
        let state = read_policy(&dir.path().join("policy.json"));
        assert_eq!(state, PolicyState::Missing);
        assert!(!state.app_mode());
        assert_eq!(state.allow(DiffRetention::Retain), DiffRetention::Retain);
    }

    #[test]
    fn invalid_file_fails_closed_and_is_app_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("policy.json");
        std::fs::write(&path, b"{not json").unwrap();
        let state = read_policy(&path);
        assert!(matches!(state, PolicyState::Invalid(_)));
        assert!(state.app_mode());
        assert_eq!(state.allow(DiffRetention::Retain), DiffRetention::Never);
        std::fs::write(
            &path,
            br#"{"version":2,"mode":"full","diff_retention":"retain"}"#,
        )
        .unwrap();
        assert_eq!(
            read_policy(&path).allow(DiffRetention::Retain),
            DiffRetention::Never
        );
    }

    #[test]
    fn a_valid_file_is_a_ceiling_not_an_override() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("policy.json");
        write_policy(
            &path,
            &file(CollectionMode::Basic, DiffRetention::Transient),
        )
        .unwrap();
        let gate = RetentionGate::new(DiffRetention::Retain, PolicySource::File(path.clone()));
        assert_eq!(gate.decide().allowed, DiffRetention::Transient);
        assert!(!gate.retain_diffs_now());
        // A Never request under a Full file stays Never.
        write_policy(&path, &file(CollectionMode::Full, DiffRetention::Retain)).unwrap();
        let gate = RetentionGate::new(DiffRetention::Never, PolicySource::File(path.clone()));
        assert_eq!(gate.decide().allowed, DiffRetention::Never);
    }

    #[test]
    fn the_gate_rereads_the_file_on_every_decision() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("policy.json");
        write_policy(&path, &file(CollectionMode::Full, DiffRetention::Retain)).unwrap();
        let gate = RetentionGate::new(DiffRetention::Retain, PolicySource::File(path.clone()));
        assert!(gate.retain_diffs_now());
        write_policy(&path, &file(CollectionMode::Counts, DiffRetention::Never)).unwrap();
        assert!(!gate.retain_diffs_now(), "no cached allow-retain");
        std::fs::remove_file(&path).unwrap();
        assert!(gate.retain_diffs_now(), "file removed: standalone again");
    }
}
