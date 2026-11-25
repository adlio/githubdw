//! Feature-gated note writing for pull requests (`feature = "summaries"`).
//!
//! [`summarize_changes`](crate::summaries::summarize_changes) writes one
//! plain-English line per pull request into the warehouse's `change_notes`
//! table (see [`crate::notes`]). It works in batches: 25 pull requests per
//! model request, up to 8 requests in flight. Pull requests whose title and
//! description are too thin get a second pass with diff excerpts fetched live
//! from GitHub; diff text is never stored. A pull request that already has a
//! current note is skipped.
//!
//! The model is anything that implements
//! [`NoteModel`](crate::summaries::NoteModel).
//! [`BedrockNoteModel`](crate::summaries::BedrockNoteModel) calls Amazon
//! Bedrock's Converse API with a named AWS profile and model, the way the CLI
//! does; an application with its own model settings implements `NoteModel`
//! instead. Bedrock and `mixtape-core` are public dependencies, so this
//! feature adds no vendor-internal coupling.

mod model;
mod summarize;

pub use model::{BedrockNoteModel, NoteFuture, NoteModel, NoteRequest};
pub use summarize::{
    BATCH, CONFIG_AWS_PROFILE, CONFIG_MODEL, CONFIG_REGION, PARALLEL, SummarizeOptions,
    SummarizeReport, summarize_changes,
};
