//! Run one collection tick against a warehouse and print its report, for
//! measuring what a pass costs.
//!
//! `cargo run --release --example tick -- <db> [--detect-now] [--no-backfill] [--json]`
//!
//! `--detect-now` detects even when the last detection is recent (a quiet
//! pass); `--no-backfill` spends nothing on history.

fn main() -> githubdw::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or_else(|| githubdw::Error::InvalidArgument("usage: tick <db> [flags]".into()))?;
    let flags: Vec<String> = args.collect();
    let flag = |name: &str| flags.iter().any(|f| f == name);
    let warehouse = githubdw::GithubDW::open(&path)?;
    let mut client = githubdw::fetch::GhClient::new();
    client.preflight()?;
    let mut policy = githubdw::collect::TickPolicy {
        program: "githubdw example tick".into(),
        skip_diffs: true,
        ..Default::default()
    };
    if flag("--detect-now") {
        policy.detect_every = chrono::Duration::zero();
    }
    if flag("--no-backfill") {
        policy.backfill_points = 0;
    }
    let report = githubdw::collect::tick(
        warehouse.connection(),
        &mut client,
        &policy,
        &Default::default(),
    )?;
    if flag("--json") {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{:?}", report.outcome);
        println!(
            "detected {}, queued {}, fetched {:?}",
            report.detected, report.queued, report.fetch
        );
        println!("backfill {:?}", report.backfill);
        for note in &report.notes {
            println!("  {note}");
        }
        println!("{}", report.cost_line());
    }
    Ok(())
}
