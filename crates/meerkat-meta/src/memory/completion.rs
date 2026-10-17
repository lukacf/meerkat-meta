//! How a memory stage reads the stop of its completion.
//!
//! The distiller, the hygienist and the steward each send one bounded
//! completion per call and strict-parse its text. This is the one place that
//! decides whether a successful stream's stop reason makes that text the
//! stage's answer, so the three stages cannot drift apart.

use meerkat_core::StopReason;

/// What a memory stage does with a successful completion's stop reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionStop {
    /// The text is the stage's answer.
    Answered,
    /// The model hit the stage's output ceiling. The body is partial (often
    /// empty); parsing it as a short answer would blame the model's JSON for
    /// a truncation.
    Truncated,
}

/// Read a successful completion's stop reason.
pub(crate) fn completion_stop(stop_reason: StopReason) -> CompletionStop {
    match stop_reason {
        StopReason::MaxTokens => CompletionStop::Truncated,
        _ => CompletionStop::Answered,
    }
}
