use super::*;
use rusqlite::Connection;

fn test_conn() -> Arc<Mutex<Connection>> {
    let mut conn = Connection::open_in_memory().expect("in-memory db");
    crate::storage::schema::init(&mut conn).expect("schema init");
    Arc::new(Mutex::new(conn))
}

/// Seed the dimension rows plus one PR so FK-checked inserts succeed.
fn seed_pr(conn: &Connection) {
    conn.execute_batch(
        r#"
        INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
        VALUES ('user:octocat', 'user', 'octocat', 1, 0, 'Octo Cat');
        INSERT INTO dim_repositories (repo_key, owner, name)
        VALUES ('octo/hello', 'octo', 'hello');
        INSERT INTO dim_date (date_key, year, quarter, month, day_of_month,
                              day_of_week, is_weekend, week_of_year, week_key,
                              month_key, quarter_key, year_key, half_key)
        VALUES ('2026-08-15', 2026, 3, 8, 15, 6, 1, 33, '2026-W33',
                '2026-08', '2026-Q3', '2026', '2026-H2');
        INSERT INTO dim_time (time_key, hour, hour_12, am_pm, time_bucket, is_core_hours)
        VALUES ('10:00', 10, 10, 'AM', 'morning', 1);
        INSERT INTO fact_pull_requests
            (pr_key, number, repo_key, author_key, state, created_at, updated_at,
             created_date_key, created_time_key)
        VALUES ('octo/hello#42', 42, 'octo/hello', 'user:octocat', 'MERGED',
                '2026-08-15T10:00:00Z', '2026-08-15T12:34:56Z',
                '2026-08-15', '10:00');
        "#,
    )
    .expect("seed");
}

/// Decode a ToolResult::Json into (columns, values, truncated).
fn decode(result: ToolResult) -> (Vec<String>, Vec<Vec<String>>, bool) {
    match result {
        ToolResult::Json(v) => (
            serde_json::from_value(v["columns"].clone()).unwrap(),
            serde_json::from_value(v["values"].clone()).unwrap(),
            v["truncated"].as_bool().unwrap(),
        ),
        other => panic!("expected Json result, got {other:?}"),
    }
}

fn summary_input(pr_key: &str) -> WritePrSummaryInput {
    WritePrSummaryInput {
        pr_key: pr_key.to_string(),
        headline: "Adds a widget".to_string(),
        what_changed: "A widget was added".to_string(),
        why_it_matters: "Widgets matter".to_string(),
        notability_score: 5,
        notability_justification: "meaningful feature".to_string(),
        change_types: vec!["new_feature".to_string()],
        impact_areas: vec!["user_facing".to_string()],
        complexity_signal: "straightforward".to_string(),
        notable_aspects: Some(vec![NotableAspect {
            aspect_type: "innovation".to_string(),
            text: "novel widget".to_string(),
        }]),
        technical_keywords: Some(vec!["widget".to_string()]),
        domain_keywords: None,
        context_tags: None,
        runtime_ms: Some(1234),
    }
}

// ---- read-only enforcement: prefix layer -------------------------------

#[test]
fn read_only_guard_allows_select_with_explain() {
    assert!(QueryDatabaseTool::is_read_only("SELECT 1"));
    assert!(QueryDatabaseTool::is_read_only(
        "  select * from fact_pull_requests"
    ));
    assert!(QueryDatabaseTool::is_read_only(
        "WITH x AS (SELECT 1) SELECT * FROM x"
    ));
    assert!(QueryDatabaseTool::is_read_only("EXPLAIN SELECT 1"));
}

#[test]
fn read_only_guard_rejects_writes() {
    for sql in [
        "INSERT INTO pr_summaries (pr_key) VALUES ('x')",
        "UPDATE fact_pull_requests SET state = 'OPEN'",
        "DELETE FROM fact_pull_requests",
        "DROP TABLE pr_summaries",
        "PRAGMA journal_mode = DELETE",
        "CREATE TABLE evil (x)",
        "ATTACH DATABASE '/tmp/x.db' AS x",
    ] {
        assert!(!QueryDatabaseTool::is_read_only(sql), "allowed: {sql}");
    }
}

// ---- read-only enforcement: statement layer (the one that matters) -----

#[tokio::test]
async fn query_tool_rejects_cte_masked_delete() {
    let conn = test_conn();
    seed_pr(&*conn.lock().await);
    let tool = QueryDatabaseTool::new(conn.clone());
    // Passes the prefix check (starts with WITH), embeds LIMIT so the old
    // text-rewrite path would not have altered it. Must be rejected by the
    // statement-level readonly check -- and the data must survive.
    let result = tool
        .execute(QueryDatabaseInput {
            sql: "WITH x AS (SELECT 1 LIMIT 1) DELETE FROM fact_pull_requests".to_string(),
            limit: 10,
        })
        .await;
    assert!(result.is_err(), "CTE-masked DELETE must be rejected");

    let guard = conn.lock().await;
    let n: i64 = guard
        .query_row("SELECT COUNT(*) FROM fact_pull_requests", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1, "the PR row must survive the rejected DELETE");
}

#[tokio::test]
async fn query_tool_rejects_cte_masked_insert() {
    let conn = test_conn();
    let tool = QueryDatabaseTool::new(conn.clone());
    let result = tool
        .execute(QueryDatabaseInput {
            sql: "WITH x AS (SELECT 1) \
                  INSERT INTO dim_time (time_key, hour, hour_12, am_pm, time_bucket, is_core_hours) \
                  SELECT '23:59', 23, 11, 'PM', 'night', 0 FROM x"
                .to_string(),
            limit: 10,
        })
        .await;
    assert!(result.is_err(), "CTE-masked INSERT must be rejected");

    let guard = conn.lock().await;
    let n: i64 = guard
        .query_row("SELECT COUNT(*) FROM dim_time", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0, "nothing may be inserted");
}

#[tokio::test]
async fn query_tool_rejects_plain_mutation_and_data_survives() {
    let conn = test_conn();
    seed_pr(&*conn.lock().await);
    let tool = QueryDatabaseTool::new(conn.clone());
    let result = tool
        .execute(QueryDatabaseInput {
            sql: "DELETE FROM fact_pull_requests".to_string(),
            limit: 10,
        })
        .await;
    assert!(result.is_err());
    let guard = conn.lock().await;
    let n: i64 = guard
        .query_row("SELECT COUNT(*) FROM fact_pull_requests", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
}

// ---- query results and row capping --------------------------------------

#[tokio::test]
async fn query_tool_returns_decoded_rows() {
    let conn = test_conn();
    seed_pr(&*conn.lock().await);
    let tool = QueryDatabaseTool::new(conn);
    let result = tool
        .execute(QueryDatabaseInput {
            sql: "SELECT pr_key, number FROM fact_pull_requests".to_string(),
            limit: 10,
        })
        .await
        .expect("query");
    let (columns, values, truncated) = decode(result);
    assert_eq!(columns, vec!["pr_key", "number"]);
    assert_eq!(
        values,
        vec![vec!["octo/hello#42".to_string(), "42".to_string()]]
    );
    assert!(!truncated);
}

fn seed_times(conn: &Connection, n: usize) {
    for i in 0..n {
        conn.execute(
            "INSERT INTO dim_time (time_key, hour, hour_12, am_pm, time_bucket, is_core_hours)
             VALUES (?1, 1, 1, 'AM', 'night', 0)",
            rusqlite::params![format!("01:{i:02}")],
        )
        .unwrap();
    }
}

#[tokio::test]
async fn query_tool_caps_rows_at_limit() {
    let conn = test_conn();
    seed_times(&*conn.lock().await, 6);
    let tool = QueryDatabaseTool::new(conn);
    let result = tool
        .execute(QueryDatabaseInput {
            sql: "SELECT time_key FROM dim_time".to_string(),
            limit: 3,
        })
        .await
        .expect("query");
    let (_, values, truncated) = decode(result);
    assert_eq!(values.len(), 3, "row count must be capped at the limit");
    assert!(truncated, "truncation must be reported");
}

#[tokio::test]
async fn query_tool_caps_rows_even_when_sql_mentions_limit() {
    // The old implementation skipped enforcement whenever the SQL text
    // contained the substring LIMIT -- e.g. inside a string literal.
    let conn = test_conn();
    seed_times(&*conn.lock().await, 6);
    let tool = QueryDatabaseTool::new(conn);
    let result = tool
        .execute(QueryDatabaseInput {
            sql: "SELECT 'has LIMIT inside' AS note, time_key FROM dim_time".to_string(),
            limit: 2,
        })
        .await
        .expect("query");
    let (_, values, truncated) = decode(result);
    assert_eq!(values.len(), 2);
    assert!(truncated);
}

#[tokio::test]
async fn query_tool_accepts_trailing_semicolon() {
    // The old implementation appended `LIMIT n` after the semicolon,
    // producing invalid SQL.
    let conn = test_conn();
    seed_times(&*conn.lock().await, 2);
    let tool = QueryDatabaseTool::new(conn);
    let result = tool
        .execute(QueryDatabaseInput {
            sql: "SELECT time_key FROM dim_time;".to_string(),
            limit: 10,
        })
        .await;
    assert!(
        result.is_ok(),
        "trailing semicolon must not break: {result:?}"
    );
    let (_, values, truncated) = decode(result.unwrap());
    assert_eq!(values.len(), 2);
    assert!(!truncated);
}

#[tokio::test]
async fn query_tool_honors_explicit_sql_limit_below_cap() {
    let conn = test_conn();
    seed_times(&*conn.lock().await, 6);
    let tool = QueryDatabaseTool::new(conn);
    let result = tool
        .execute(QueryDatabaseInput {
            sql: "SELECT time_key FROM dim_time LIMIT 1".to_string(),
            limit: 10,
        })
        .await
        .expect("query");
    let (_, values, truncated) = decode(result);
    assert_eq!(values.len(), 1, "SQL's own LIMIT still applies");
    assert!(!truncated);
}

// ---- parse_pr_key --------------------------------------------------------

#[test]
fn parse_pr_key_valid_and_invalid() {
    assert_eq!(
        parse_pr_key("octo/hello#42"),
        Some(("octo/hello".to_string(), 42))
    );
    assert_eq!(parse_pr_key("no-number#"), None);
    assert_eq!(parse_pr_key("no-hash-at-all"), None);
    assert_eq!(parse_pr_key("bad#num#x"), None);
    // Documented current behavior (rsplit on '#'): a key with multiple
    // '#' parses from the LAST separator. GitHub owner/repo names cannot
    // contain '#', so this is unreachable for real keys.
    assert_eq!(parse_pr_key("a#b#7"), Some(("a#b".to_string(), 7)));
}

// ---- WritePrSummaryTool ---------------------------------------------------

#[tokio::test]
async fn write_summary_derives_fields_from_warehouse() {
    let conn = test_conn();
    seed_pr(&*conn.lock().await);
    let tool = WritePrSummaryTool::new(conn.clone(), PROMPT_VERSION);
    let result = tool.execute(summary_input("octo/hello#42")).await;
    assert!(result.is_ok(), "write failed: {result:?}");

    let guard = conn.lock().await;
    let (repo_key, number, author_key, source_updated_at, prompt_version): (
        String,
        i64,
        Option<String>,
        String,
        i32,
    ) = guard
        .query_row(
            "SELECT repo_key, number, author_key, source_updated_at, prompt_version
             FROM pr_summaries WHERE pr_key = 'octo/hello#42'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .expect("summary row");
    assert_eq!(repo_key, "octo/hello");
    assert_eq!(number, 42);
    assert_eq!(author_key.as_deref(), Some("user:octocat"));
    assert_eq!(source_updated_at, "2026-08-15T12:34:56Z");
    assert_eq!(prompt_version, PROMPT_VERSION);
}

#[tokio::test]
async fn write_summary_falls_back_when_pr_not_in_warehouse() {
    let conn = test_conn();
    let tool = WritePrSummaryTool::new(conn.clone(), PROMPT_VERSION);
    let result = tool.execute(summary_input("ghost/repo#7")).await;
    assert!(result.is_ok(), "write failed: {result:?}");

    let guard = conn.lock().await;
    let (repo_key, number, author_key): (String, i64, Option<String>) = guard
        .query_row(
            "SELECT repo_key, number, author_key FROM pr_summaries
             WHERE pr_key = 'ghost/repo#7'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .expect("summary row");
    assert_eq!(repo_key, "ghost/repo");
    assert_eq!(number, 7);
    assert_eq!(author_key, None);
}

#[tokio::test]
async fn write_summary_upsert_preserves_created_at() {
    let conn = test_conn();
    seed_pr(&*conn.lock().await);
    let tool = WritePrSummaryTool::new(conn.clone(), PROMPT_VERSION);
    tool.execute(summary_input("octo/hello#42")).await.unwrap();

    // Pin created_at to a sentinel so a clobber is unambiguous.
    {
        let guard = conn.lock().await;
        guard
            .execute(
                "UPDATE pr_summaries SET created_at = '2000-01-01T00:00:00Z'
                 WHERE pr_key = 'octo/hello#42'",
                [],
            )
            .unwrap();
    }

    let mut second = summary_input("octo/hello#42");
    second.headline = "Revised headline".to_string();
    second.notability_score = 8;
    tool.execute(second).await.unwrap();

    let guard = conn.lock().await;
    let (count, headline, score, created_at, updated_at): (i64, String, i64, String, String) =
        guard
            .query_row(
                "SELECT COUNT(*), MAX(headline), MAX(notability_score),
                        MAX(created_at), MAX(updated_at)
                 FROM pr_summaries",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
    assert_eq!(count, 1, "upsert must not duplicate");
    assert_eq!(headline, "Revised headline");
    assert_eq!(score, 8);
    assert_eq!(
        created_at, "2000-01-01T00:00:00Z",
        "created_at must survive a re-summarize"
    );
    assert_ne!(updated_at, "2000-01-01T00:00:00Z", "updated_at must move");
}

// ---- helper ---------------------------------------------------------------

#[test]
fn pr_summary_tools_exposes_both_tools_by_name() {
    let conn = test_conn();
    let tools = pr_summary_tools(conn);
    let names: Vec<&str> = tools.iter().map(|t| t.name()).collect();
    assert_eq!(names, vec!["query_database", "write_pr_summary"]);
}
