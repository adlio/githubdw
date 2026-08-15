//! Feature-gated AI summarization of pull requests (`feature = "summaries"`).
//!
//! This module mirrors the CRUX data warehouse's summary agent so that PR and
//! CR notability scores stay comparable, but it reads githubdw's own star
//! schema and persists results into the `pr_summaries` table. The design
//! principle: **each data warehouse owns the summaries of the data it holds**,
//! stored alongside that data rather than in a downstream consumer.
//!
//! Summaries are produced by a [`mixtape_core`] agent backed by Amazon Bedrock
//! (Claude), given two trusted tools: a read-only `query_database` tool for
//! exploring the schema and a `write_pr_summary` tool that persists the
//! structured result. Bedrock and `mixtape-core` are public dependencies, so
//! enabling this feature adds no Amazon-internal coupling to the crate.
//!
//! Entry point: [`summarize_pr`]. Scope is intentionally single-PR only; there
//! are no period-level (user / team / repo) summary functions here.

pub mod pr_summary;
pub mod tools;

pub use pr_summary::{
    PrSummaryData, PrSummaryResult, get_pr_summary, has_pr_summary, summarize_pr,
};
pub use tools::{
    NotableAspect, PROMPT_VERSION, QueryDatabaseTool, WritePrSummaryTool, pr_summary_tools,
};
