//! Live smoke check of the counts path against GitHub through `gh`.
//!
//! `cargo run --example live_counts -- <db-path> <login> <days>` collects the
//! last `<days>` warehouse days newest-first into a scratch warehouse and
//! prints each outcome and the resulting day counts as JSON. Read-only
//! against GitHub; writes only the named scratch database.

fn main() -> githubdw::Result<()> {
    let mut args = std::env::args().skip(1);
    let db = args.next().expect("db path");
    let login = args.next().expect("login");
    let days: i64 = args.next().map(|d| d.parse().expect("days")).unwrap_or(2);
    let warehouse = githubdw::GithubDW::open(&db)?;
    let conn = warehouse.connection();
    let mut client = githubdw::fetch::GhClient::new();
    client.preflight()?;
    let now = chrono::Utc::now();
    let today = githubdw::storage::time_dimension::today_as_of(conn, now)?;
    let control = githubdw::collection::CollectionControl {
        cancel: None,
        defer_waits_over: Some(std::time::Duration::from_secs(30)),
    };
    for back in 0..days {
        let day = today - chrono::Duration::days(back);
        let outcome = githubdw::collection::collect_day(
            conn,
            &mut client,
            &login,
            day,
            &githubdw::collection::CountRole::all(),
            &control,
            now,
        )?;
        println!("{}", serde_json::to_string(&outcome)?);
    }
    let counts = githubdw::collection::day_counts(
        conn,
        &[login.as_str()],
        today - chrono::Duration::days(days - 1),
        today,
    )?;
    println!("{}", serde_json::to_string_pretty(&counts)?);
    Ok(())
}
