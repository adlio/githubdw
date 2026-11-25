//! Batch note writing: [`summarize_changes`].

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::model::{NoteModel, NoteRequest};
use crate::GithubDW;
use crate::collection::{CollectionControl, fetch_pull_request_diff_live};
use crate::error::{Error, Result};
use crate::fetch::GhClient;
use crate::notes::{
    DIFF_PROMPT, NOTE_PROMPT, NOTE_RULES, NewNote, NoteInput, change_notes, excerpt, note_inputs,
    save_notes,
};

/// Pull requests per first-pass model request.
pub const BATCH: usize = 25;
/// Model requests in flight at once.
pub const PARALLEL: usize = 8;
/// Pull requests per second-pass (diff) request; diffs make each one larger.
const DIFF_BATCH: usize = 8;
const DIFF_FILES: usize = 6;
const DIFF_CHARS: usize = 2000;
/// Longest note kept; a model that ignores the 25-word limit is cut here.
const NOTE_CHARS: usize = 400;

/// CLI configuration key: the AWS profile for Bedrock.
pub const CONFIG_AWS_PROFILE: &str = "summaries.aws_profile";
/// CLI configuration key: the Bedrock model ID, inference profile or ARN.
pub const CONFIG_MODEL: &str = "summaries.model";
/// CLI configuration key: the Bedrock Region (optional).
pub const CONFIG_REGION: &str = "summaries.region";

/// Builds the GitHub client used for live diff reads.
pub type ClientFactory = Arc<dyn Fn() -> GhClient + Send + Sync>;

/// How [`summarize_changes`] runs.
#[derive(Clone)]
pub struct SummarizeOptions {
    /// Stored beside each note, for example `githubdw` or an application name.
    pub written_by: String,
    /// The client for live diff reads in the second pass. `None` skips that
    /// pass: unclear notes are stored with the model's best guess.
    pub diff_client: Option<ClientFactory>,
}

impl Default for SummarizeOptions {
    fn default() -> Self {
        Self {
            written_by: "githubdw".into(),
            diff_client: Some(Arc::new(GhClient::new)),
        }
    }
}

/// What one [`summarize_changes`] call did.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SummarizeReport {
    pub requested: usize,
    /// Already had a current note; nothing was asked.
    pub already_current: usize,
    /// Keys with no stored pull request; nothing can be written for them.
    pub unknown: Vec<String>,
    pub written: usize,
    /// Written, but the model still could not tell what the change does.
    pub unclear: usize,
    /// Live diff reads that returned at least one patch.
    pub diffs_fetched: usize,
    /// Model requests or diff reads that failed; their pull requests are
    /// tried again on the next call.
    pub errors: Vec<String>,
    /// Asked for, but no usable note came back.
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct Reply {
    id: String,
    note: String,
    #[serde(default)]
    unclear: bool,
}

fn schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["notes"],
        "properties": {"notes": {"type": "array", "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["id", "note", "unclear"],
            "properties": {
                "id": {"type": "string"},
                "note": {"type": "string"},
                "unclear": {"type": "boolean"},
            },
        }}},
    })
}

/// Ask the model about each batch, `PARALLEL` at a time. Replies are kept
/// only for ids that were in their own batch and carry a non-empty note.
async fn ask_all(
    model: &dyn NoteModel,
    purpose: &'static str,
    prompt: &str,
    batches: Vec<Vec<Value>>,
    report: &mut SummarizeReport,
) -> Vec<Reply> {
    use futures::stream::{self, StreamExt};
    let system = format!("{prompt}\n{NOTE_RULES}");
    let outcomes: Vec<(HashSet<String>, Result<Value>)> = stream::iter(batches)
        .map(|batch| {
            let ids: HashSet<String> = batch
                .iter()
                .filter_map(|v| v["id"].as_str().map(str::to_string))
                .collect();
            let request = NoteRequest {
                purpose,
                system: system.clone(),
                input: json!({"changes": batch}),
                schema: schema(),
            };
            async move {
                let outcome = model.complete(&request).await;
                (ids, outcome)
            }
        })
        .buffer_unordered(PARALLEL)
        .collect()
        .await;
    let mut replies = Vec::new();
    for (ids, outcome) in outcomes {
        match outcome {
            Ok(value) => {
                let batch: Vec<Reply> =
                    serde_json::from_value(value["notes"].clone()).unwrap_or_default();
                replies.extend(batch.into_iter().filter_map(|mut r| {
                    r.note = excerpt(r.note.trim(), NOTE_CHARS);
                    (ids.contains(&r.id) && !r.note.is_empty()).then_some(r)
                }));
            }
            Err(error) => report.errors.push(format!("{purpose}: {error}")),
        }
    }
    replies
}

/// Live diff excerpts for these pull requests, read one at a time on a
/// blocking thread. Diff text stays in memory.
async fn live_excerpts(
    factory: ClientFactory,
    keys: Vec<String>,
    report: &mut SummarizeReport,
) -> HashMap<String, Vec<Value>> {
    let outcome = tokio::task::spawn_blocking(move || {
        let mut client = factory();
        let mut found = HashMap::new();
        let mut errors = Vec::new();
        if let Err(error) = client.preflight() {
            errors.push(format!("live diffs skipped: {error}"));
            return (found, errors);
        }
        let control = CollectionControl {
            cancel: None,
            defer_waits_over: Some(std::time::Duration::from_secs(60)),
            ..Default::default()
        };
        for key in keys {
            let Some((repo, number)) = key
                .rsplit_once('#')
                .and_then(|(r, n)| Some((r.to_string(), n.parse::<i64>().ok()?)))
            else {
                continue;
            };
            match fetch_pull_request_diff_live(&mut client, &repo, number, &control) {
                Ok(diff) => {
                    let files: Vec<Value> = diff
                        .files
                        .into_iter()
                        .filter_map(|f| {
                            let patch = f.patch.filter(|p| !p.is_empty())?;
                            Some(json!({"path": f.path, "patch": excerpt(&patch, DIFF_CHARS)}))
                        })
                        .take(DIFF_FILES)
                        .collect();
                    if !files.is_empty() {
                        found.insert(key, files);
                    }
                }
                // A spent budget or a lost sign-in fails every later read too.
                Err(error @ (Error::RateLimited { .. } | Error::GhUnavailable(_))) => {
                    errors.push(format!("live diffs stopped at {key}: {error}"));
                    break;
                }
                Err(error) => errors.push(format!("live diff {key}: {error}")),
            }
        }
        (found, errors)
    })
    .await;
    match outcome {
        Ok((found, errors)) => {
            report.errors.extend(errors);
            report.diffs_fetched += found.len();
            found
        }
        Err(error) => {
            report
                .errors
                .push(format!("live diffs: task stopped: {error}"));
            HashMap::new()
        }
    }
}

/// Write notes for the pull requests among `pr_keys` (`owner/repo#number`)
/// that have no current note, and store them in the warehouse at `db`.
///
/// Notes come from the title, body and file list, 25 pull requests per model
/// request and up to 8 requests at once. Those the model marks unclear get
/// a second pass with diff excerpts read live from GitHub. A failed request
/// is reported and its pull requests are left for the next call; nothing
/// stored is deleted or blanked. The warehouse connection is never held
/// across a model call.
pub async fn summarize_changes(
    db: &Path,
    pr_keys: &[String],
    model: &dyn NoteModel,
    options: &SummarizeOptions,
) -> Result<SummarizeReport> {
    let mut report = SummarizeReport {
        requested: pr_keys.len(),
        ..Default::default()
    };
    let todo: Vec<(String, NoteInput)> = {
        let warehouse = GithubDW::open(db)?;
        let conn = warehouse.connection();
        let mut inputs = note_inputs(conn, pr_keys)?;
        let stored = change_notes(conn, pr_keys)?;
        let mut seen = HashSet::new();
        let mut todo = Vec::new();
        for key in pr_keys {
            if !seen.insert(key.as_str()) {
                continue;
            }
            match inputs.remove(key) {
                None => report.unknown.push(key.clone()),
                Some(_) if stored.get(key).is_some_and(|n| n.current) => {
                    report.already_current += 1
                }
                Some(input) => todo.push((key.clone(), input)),
            }
        }
        todo
    };
    if todo.is_empty() {
        return Ok(report);
    }
    let batches: Vec<Vec<Value>> = todo
        .chunks(BATCH)
        .map(|b| b.iter().map(|(_, i)| i.value.clone()).collect())
        .collect();
    let mut replies = ask_all(model, "change_notes", NOTE_PROMPT, batches, &mut report).await;

    let unclear: Vec<String> = replies
        .iter()
        .filter(|r| r.unclear)
        .map(|r| r.id.clone())
        .collect();
    if let (false, Some(factory)) = (unclear.is_empty(), options.diff_client.clone()) {
        let diffs = live_excerpts(factory, unclear, &mut report).await;
        let by_key: HashMap<&str, &NoteInput> = todo.iter().map(|(k, i)| (k.as_str(), i)).collect();
        let inputs: Vec<Value> = diffs
            .into_iter()
            .filter_map(|(key, files)| {
                let mut input = by_key.get(key.as_str())?.value.clone();
                input["diff"] = json!(files);
                Some(input)
            })
            .collect();
        let batches: Vec<Vec<Value>> = inputs.chunks(DIFF_BATCH).map(<[Value]>::to_vec).collect();
        let better: HashMap<String, String> = ask_all(
            model,
            "change_notes_diff",
            DIFF_PROMPT,
            batches,
            &mut report,
        )
        .await
        .into_iter()
        .map(|r| (r.id, r.note))
        .collect();
        for reply in &mut replies {
            if let Some(note) = better.get(&reply.id) {
                reply.note = note.clone();
                reply.unclear = false;
            }
        }
    }

    let hashes: HashMap<&str, &str> = todo
        .iter()
        .map(|(k, i)| (k.as_str(), i.hash.as_str()))
        .collect();
    let label = model.label();
    let mut written = HashSet::new();
    let notes: Vec<NewNote> = replies
        .into_iter()
        .filter_map(|r| {
            let hash = hashes.get(r.id.as_str())?;
            written.insert(r.id.clone()).then(|| NewNote {
                input_hash: hash.to_string(),
                pr_key: r.id,
                note: r.note,
                unclear: r.unclear,
                model: Some(label.clone()),
                written_by: Some(options.written_by.clone()),
            })
        })
        .collect();
    report.unclear = notes.iter().filter(|n| n.unclear).count();
    {
        let warehouse = GithubDW::open(db)?;
        report.written = save_notes(warehouse.connection(), &notes)?;
    }
    report.missing = todo
        .into_iter()
        .map(|(k, _)| k)
        .filter(|k| !written.contains(k))
        .collect();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::summaries::NoteFuture;
    use std::sync::Mutex;

    /// Answers each request from its input, recording what it was asked.
    struct Fake {
        asked: Mutex<Vec<(String, usize)>>,
        unclear: bool,
        fail_first: Mutex<bool>,
    }

    impl NoteModel for Fake {
        fn label(&self) -> String {
            "fake-model".into()
        }
        fn complete<'a>(&'a self, request: &'a NoteRequest) -> NoteFuture<'a> {
            Box::pin(async move {
                let changes = request.input["changes"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                self.asked
                    .lock()
                    .unwrap()
                    .push((request.purpose.to_string(), changes.len()));
                if std::mem::take(&mut *self.fail_first.lock().unwrap()) {
                    return Err(Error::Llm("synthetic failure".into()));
                }
                let notes: Vec<Value> = changes
                    .iter()
                    .map(|c| {
                        json!({"id": c["id"], "note": format!("Adds {}.", c["title"].as_str().unwrap_or("")), "unclear": self.unclear})
                    })
                    .chain([json!({"id": "someone/else#1", "note": "Not asked.", "unclear": false})])
                    .collect();
                Ok(json!({"notes": notes}))
            })
        }
    }

    fn fake(unclear: bool, fail_first: bool) -> Arc<Fake> {
        Arc::new(Fake {
            asked: Mutex::new(vec![]),
            unclear,
            fail_first: Mutex::new(fail_first),
        })
    }

    fn warehouse(count: i64) -> (tempfile::TempDir, std::path::PathBuf, Vec<String>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("githubdw.db");
        let warehouse = GithubDW::open(&path).unwrap();
        let conn = warehouse.connection();
        conn.execute_batch(
            "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
             VALUES ('user:octocat', 'user', 'octocat', 1, 0, 'Octocat');
             INSERT INTO dim_repositories (repo_key, owner, name) VALUES ('octocat/hello', 'octocat', 'hello');
             INSERT INTO dim_date (date_key, year, quarter, month, day_of_month, day_of_week,
                 is_weekend, week_of_year, week_key, month_key, quarter_key, year_key, half_key)
             VALUES ('2026-01-05', 2026, 1, 1, 5, 1, 0, 2, '2026-W02', '2026-01', '2026-Q1', '2026', '2026-H1');
             INSERT INTO dim_time (time_key, hour, hour_12, am_pm, time_bucket, is_core_hours)
             VALUES ('10:00', 10, 10, 'AM', 'Morning', 1);",
        )
        .unwrap();
        let keys = (1..=count)
            .map(|n| {
                let key = format!("octocat/hello#{n}");
                conn.execute(
                    "INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state, title,
                         created_at, created_date_key, created_time_key)
                     VALUES (?1, ?2, 'octocat/hello', 'user:octocat', 'MERGED', ?3, '2026-01-05T18:00:00Z',
                         '2026-01-05', '10:00')",
                    rusqlite::params![key, n, format!("thing {n}")],
                )
                .unwrap();
                key
            })
            .collect();
        (dir, path, keys)
    }

    fn no_diffs() -> SummarizeOptions {
        SummarizeOptions {
            written_by: "test".into(),
            diff_client: None,
        }
    }

    #[tokio::test]
    async fn writes_notes_in_batches_and_skips_current_ones_next_time() {
        let (_dir, path, mut keys) = warehouse(30);
        keys.push("nobody/none#9".into());
        let model = fake(false, false);
        let report = summarize_changes(&path, &keys, &*model, &no_diffs())
            .await
            .unwrap();
        assert_eq!(report.written, 30);
        assert_eq!(report.unknown, ["nobody/none#9"]);
        let mut sizes: Vec<usize> = model.asked.lock().unwrap().iter().map(|a| a.1).collect();
        sizes.sort();
        assert_eq!(sizes, [5, 25]);
        let again = summarize_changes(&path, &keys, &*model, &no_diffs())
            .await
            .unwrap();
        assert_eq!((again.already_current, again.written), (30, 0));
        let warehouse = GithubDW::open(&path).unwrap();
        let notes = change_notes(warehouse.connection(), &keys).unwrap();
        assert_eq!(
            notes.len(),
            30,
            "the reply for a key nobody asked about is dropped"
        );
        assert_eq!(notes["octocat/hello#1"].note, "Adds thing 1.");
        assert_eq!(
            notes["octocat/hello#1"].model.as_deref(),
            Some("fake-model")
        );
        assert_eq!(notes["octocat/hello#1"].written_by.as_deref(), Some("test"));
    }

    #[tokio::test]
    async fn a_failed_batch_is_reported_and_retried_on_the_next_call() {
        let (_dir, path, keys) = warehouse(3);
        let model = fake(false, true);
        let first = summarize_changes(&path, &keys, &*model, &no_diffs())
            .await
            .unwrap();
        assert_eq!(first.written, 0);
        assert_eq!(first.missing.len(), 3);
        assert_eq!(first.errors.len(), 1);
        let second = summarize_changes(&path, &keys, &*model, &no_diffs())
            .await
            .unwrap();
        assert_eq!(second.written, 3);
    }

    #[tokio::test]
    async fn unclear_notes_without_a_diff_client_keep_the_best_guess() {
        let (_dir, path, keys) = warehouse(2);
        let report = summarize_changes(&path, &keys, &*fake(true, false), &no_diffs())
            .await
            .unwrap();
        assert_eq!((report.written, report.unclear), (2, 2));
    }
}
