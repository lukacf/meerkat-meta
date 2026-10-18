//! Deterministic fault points for process-level acceptance tests.
//!
//! These compile to nothing unless the `announcement-test-faults` feature is
//! on. With it, `META_ANNOUNCEMENT_FAULT=<point>` makes the process abort at
//! that point, so a test can kill a gateway between two durable commits
//! without racing it. No production build carries them.

#[cfg(feature = "announcement-test-faults")]
fn hit(point: &str) {
    if std::env::var("META_ANNOUNCEMENT_FAULT").is_ok_and(|value| value == point) {
        eprintln!("announcement fault point {point}: aborting the process");
        std::process::abort();
    }
}

/// After the native owner committed a batch, before the announcement owner
/// records the receipt: the acknowledgment is lost.
pub(crate) fn after_native_commit() {
    #[cfg(feature = "announcement-test-faults")]
    hit("after_native_commit");
}

/// After the intent to submit is durable, before any native submission.
pub(crate) fn after_begin_submit() {
    #[cfg(feature = "announcement-test-faults")]
    hit("after_begin_submit");
}
