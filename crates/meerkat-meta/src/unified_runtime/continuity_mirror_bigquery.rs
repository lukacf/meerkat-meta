//! A [`ContinuityMirrorSink`] that streams the mirror into BigQuery.
//!
//! BigQuery's streaming inserts only append. Each `put` tags its rows with
//! one `snapshot_ms` and writes the changed sessions, then every record, and
//! last one row into the snapshots table that names this `snapshot_ms` and
//! the row counts. A `put` is complete once its snapshots row is in; the rows
//! of a `put` that failed part way have none, so no reader takes them, and
//! the mirror offers the changed sessions again. `snapshot_ms` is the wall
//! clock in milliseconds, kept strictly above the previous `put`'s and, from
//! the first `put` of a process on, above the newest snapshots row the table
//! already holds, so a clock that stepped back across a restart still
//! numbers new `put`s above the old ones.
//!
//! A row carries at most 4 MiB of a session's base64 `data` (BigQuery refuses
//! a streamed request over 10 MB), so a longer value goes out as several
//! rows, numbered by `part` from 0, that all carry the number of rows in
//! `parts`.
//!
//! The tables must exist, with these columns (`<p>` is the table prefix),
//! and the credentials must allow streaming inserts into them and queries
//! over `<p>_snapshots`:
//!
//! - `<p>_records`: `identity`, `agent_runtime_id`, `session_id` (STRING),
//!   `generation`, `checkpoint_version`, `snapshot_ms` (INT64);
//! - `<p>_sessions`: `session_id`, `data` (STRING), `part`, `parts`,
//!   `snapshot_ms` (INT64);
//! - `<p>_snapshots`: `snapshot_ms`, `records`, `sessions`,
//!   `changed_sessions` (INT64).
//!
//! A restore reads the newest complete `put`, `@s` = `SELECT MAX(snapshot_ms)
//! FROM <p>_snapshots`, back into the files the continuity export reads: the
//! records of that `put`, and for each session they name, its newest copy
//! from a complete `put` at or before it.
//!
//! ```sql
//! -- records.jsonl
//! SELECT identity, agent_runtime_id, session_id, generation, checkpoint_version
//! FROM <p>_records WHERE snapshot_ms = @s;
//! -- sessions.jsonl
//! SELECT session_id, STRING_AGG(data, '' ORDER BY part) AS data
//! FROM <p>_sessions
//! WHERE snapshot_ms IN (SELECT snapshot_ms FROM <p>_snapshots WHERE snapshot_ms <= @s)
//!   AND session_id IN (SELECT session_id FROM <p>_records WHERE snapshot_ms = @s)
//! GROUP BY session_id, snapshot_ms
//! QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY snapshot_ms DESC) = 1;
//! ```
//!
//! Keep every `<p>_snapshots` row: it marks a complete `put`. A session's
//! rows may be deleted once a newer complete `put` holds a copy of it, and a
//! record row once a newer `put` is complete.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

use super::continuity_mirror::{ContinuityMirrorSink, ContinuityMirrorUpdate};
use crate::runtime::BigQuerySessionStoreAdapter;

/// BigQuery refuses an `insertAll` request over 10 MB, so the rows of one
/// table go out in requests of at most this many bytes of row JSON.
const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;

/// BigQuery refuses an `insertAll` request of more than 50,000 rows.
const MAX_REQUEST_ROWS: usize = 10_000;

/// The most base64 `data` one row carries.
const MAX_PART_BYTES: usize = 4 * 1024 * 1024;

/// Streams the continuity mirror into tables of one BigQuery dataset named
/// after a prefix (see the module documentation for their columns and for
/// reading them back).
pub struct BigQueryMirrorSink {
    adapter: BigQuerySessionStoreAdapter,
    records_table: String,
    sessions_table: String,
    snapshots_table: String,
    put_deadline: Duration,
    last_snapshot_ms: AtomicU64,
    /// Whether `last_snapshot_ms` has been raised to the newest snapshots
    /// row the table held when this process first put.
    snapshot_ms_seeded: tokio::sync::Mutex<bool>,
}

impl BigQueryMirrorSink {
    /// How long one `put` may take unless
    /// [`Self::with_put_deadline`] sets another bound.
    pub const DEFAULT_PUT_DEADLINE: Duration = Duration::from_mins(5);

    /// A sink over `adapter`'s project, dataset, credentials and endpoint,
    /// writing the tables named after `table_prefix`; the adapter's own
    /// session table is not used. The tables must exist.
    pub fn new(adapter: BigQuerySessionStoreAdapter, table_prefix: &str) -> Self {
        Self {
            adapter,
            records_table: format!("{table_prefix}_records"),
            sessions_table: format!("{table_prefix}_sessions"),
            snapshots_table: format!("{table_prefix}_snapshots"),
            put_deadline: Self::DEFAULT_PUT_DEADLINE,
            last_snapshot_ms: AtomicU64::new(0),
            snapshot_ms_seeded: tokio::sync::Mutex::new(false),
        }
    }

    /// Fail a `put` that has not completed within `deadline`.
    /// Each request also has the adapter's own HTTP timeout.
    pub fn with_put_deadline(mut self, deadline: Duration) -> Self {
        self.put_deadline = deadline;
        self
    }

    /// The wall clock in milliseconds, kept strictly above the previous
    /// value so two `put`s never share an id.
    fn next_snapshot_ms(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            });
        let mut last = self.last_snapshot_ms.load(Ordering::Relaxed);
        loop {
            let next = now.max(last.saturating_add(1));
            match self.last_snapshot_ms.compare_exchange_weak(
                last,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(current) => last = current,
            }
        }
    }

    fn table_error(table: &str, error: impl std::fmt::Debug) -> String {
        format!("BigQuery table {table}: {error:?}")
    }

    async fn insert(&self, table: &str, rows: Vec<Value>) -> Result<(), String> {
        for chunk in chunks_under(rows, MAX_REQUEST_BYTES, MAX_REQUEST_ROWS)? {
            self.adapter
                .insert_json_rows(table, chunk)
                .await
                .map_err(|error| Self::table_error(table, error))?;
        }
        Ok(())
    }

    /// The first column of `query`'s rows; `table` names the table it reads
    /// in errors.
    async fn query(&self, table: &str, query: &str) -> Result<Vec<String>, String> {
        self.adapter
            .query_first_column(query)
            .await
            .map_err(|error| Self::table_error(table, error))
    }

    fn qualified(&self, table: &str) -> Result<String, String> {
        self.adapter
            .qualified_table_name(table)
            .map_err(|error| Self::table_error(table, error))
    }

    /// Raise the snapshot clock to the newest snapshots row the table holds,
    /// once per process.
    async fn seed_snapshot_ms(&self) -> Result<(), String> {
        let mut seeded = self.snapshot_ms_seeded.lock().await;
        if *seeded {
            return Ok(());
        }
        let table = self.qualified(&self.snapshots_table)?;
        let newest = self
            .query(
                &self.snapshots_table,
                &format!("SELECT MAX(snapshot_ms) FROM `{table}`"),
            )
            .await?;
        if let Some(newest) = newest.first() {
            let newest = newest.parse::<u64>().map_err(|error| {
                Self::table_error(
                    &self.snapshots_table,
                    format!("snapshot_ms {newest}: {error}"),
                )
            })?;
            self.last_snapshot_ms.fetch_max(newest, Ordering::Relaxed);
        }
        *seeded = true;
        Ok(())
    }

    async fn put_within_deadline(&self, update: &ContinuityMirrorUpdate) -> Result<(), String> {
        self.seed_snapshot_ms().await?;
        let snapshot_ms = self.next_snapshot_ms();
        let mut sessions = Vec::new();
        for session in &update.changed_sessions {
            let mut fields = Map::new();
            fields.insert(
                "session_id".to_string(),
                Value::String(session.session_id.clone()),
            );
            sessions.extend(data_rows(
                &fields,
                &session.data,
                MAX_PART_BYTES,
                snapshot_ms,
            ));
        }
        let records = update
            .records
            .iter()
            .map(|row| tagged(row, snapshot_ms))
            .collect::<Result<Vec<_>, _>>()?;
        self.insert(&self.sessions_table, sessions).await?;
        self.insert(&self.records_table, records).await?;
        self.insert(
            &self.snapshots_table,
            vec![serde_json::json!({
                "snapshot_ms": snapshot_ms.to_string(),
                "records": update.records.len(),
                "sessions": update.session_ids.len(),
                "changed_sessions": update.changed_sessions.len(),
            })],
        )
        .await
    }
}

/// `row` as a JSON object with `snapshot_ms` added.
fn tagged<T: serde::Serialize>(row: &T, snapshot_ms: u64) -> Result<Value, String> {
    let mut value = serde_json::to_value(row).map_err(|error| error.to_string())?;
    let Some(object) = value.as_object_mut() else {
        return Err("a mirror row did not serialize to a JSON object".to_string());
    };
    object.insert(
        "snapshot_ms".to_string(),
        Value::String(snapshot_ms.to_string()),
    );
    Ok(value)
}

/// The rows of one value: one row per part of `data` of at most
/// `max_part_bytes`, each with `fields`, the part, its number from 0 in
/// `part`, the number of parts in `parts`, and `snapshot_ms`.
fn data_rows(
    fields: &Map<String, Value>,
    data: &str,
    max_part_bytes: usize,
    snapshot_ms: u64,
) -> Vec<Value> {
    let pieces = split_data(data, max_part_bytes);
    let parts = pieces.len();
    pieces
        .into_iter()
        .enumerate()
        .map(|(part, piece)| {
            let mut row = fields.clone();
            row.insert("data".to_string(), Value::String(piece.to_string()));
            row.insert("part".to_string(), Value::from(part));
            row.insert("parts".to_string(), Value::from(parts));
            row.insert(
                "snapshot_ms".to_string(),
                Value::String(snapshot_ms.to_string()),
            );
            Value::Object(row)
        })
        .collect()
}

/// `data` split, in order, into pieces of at most `max_bytes` bytes that end
/// on character boundaries (base64 is ASCII, so any byte is one; a character
/// longer than `max_bytes` is a piece of its own). An empty `data` is one
/// empty piece, so every value has a row.
fn split_data(data: &str, max_bytes: usize) -> Vec<&str> {
    let mut pieces = Vec::new();
    let mut rest = data;
    while rest.len() > max_bytes {
        let mut end = max_bytes;
        while end > 0 && !rest.is_char_boundary(end) {
            end -= 1;
        }
        if end == 0 {
            end = rest.chars().next().map_or(rest.len(), char::len_utf8);
        }
        let (piece, tail) = rest.split_at(end);
        pieces.push(piece);
        rest = tail;
    }
    pieces.push(rest);
    pieces
}

/// `rows` split, in order, into requests of at most `max_bytes` of row JSON
/// and `max_rows` rows. A single row over the byte limit fails: BigQuery
/// would refuse it.
fn chunks_under(
    rows: Vec<Value>,
    max_bytes: usize,
    max_rows: usize,
) -> Result<Vec<Vec<Value>>, String> {
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0;
    for row in rows {
        let bytes = row.to_string().len();
        if bytes > max_bytes {
            return Err(format!(
                "a mirror row of {bytes} bytes exceeds the {max_bytes}-byte BigQuery request limit"
            ));
        }
        if !current.is_empty() && (current_bytes + bytes > max_bytes || current.len() == max_rows) {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += bytes;
        current.push(row);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    Ok(chunks)
}

#[async_trait::async_trait]
impl ContinuityMirrorSink for BigQueryMirrorSink {
    async fn put(&self, update: &ContinuityMirrorUpdate) -> Result<(), String> {
        tokio::time::timeout(self.put_deadline, self.put_within_deadline(update))
            .await
            .unwrap_or_else(|_| {
                Err(format!(
                    "the BigQuery mirror put did not complete within {:?}",
                    self.put_deadline
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_split_into_requests_under_the_byte_and_row_limits_in_order() {
        let rows: Vec<Value> = (0..5).map(|n| serde_json::json!({ "n": n })).collect();
        let row_bytes = rows[0].to_string().len();
        let chunks = chunks_under(rows.clone(), row_bytes * 2, 10).expect("rows fit");
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks.concat(), rows);
        let chunks = chunks_under(rows.clone(), row_bytes * 10, 2).expect("rows fit");
        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
        assert_eq!(chunks.concat(), rows);
        assert!(chunks_under(rows, row_bytes - 1, 10).is_err());
    }

    #[test]
    fn data_longer_than_a_part_goes_out_as_numbered_rows_that_rejoin() {
        let mut fields = Map::new();
        fields.insert("session_id".to_string(), Value::from("s"));
        let row = |data: &str, part: usize, parts: usize| {
            serde_json::json!({
                "session_id": "s",
                "data": data,
                "part": part,
                "parts": parts,
                "snapshot_ms": "7",
            })
        };
        assert_eq!(
            data_rows(&fields, "abcdefg", 3, 7),
            vec![row("abc", 0, 3), row("def", 1, 3), row("g", 2, 3)]
        );
        assert_eq!(data_rows(&fields, "", 3, 7), vec![row("", 0, 1)]);
        assert_eq!(data_rows(&fields, "abc", 3, 7), vec![row("abc", 0, 1)]);
    }

    #[test]
    fn data_parts_end_on_character_boundaries() {
        let data = "a\u{e9}\u{1f600}b";
        let pieces = split_data(data, 2);
        assert_eq!(pieces, vec!["a", "\u{e9}", "\u{1f600}", "b"]);
        assert_eq!(split_data(data, 1).concat(), data);
    }

    #[test]
    fn snapshot_ids_strictly_increase() {
        let sink = BigQueryMirrorSink::new(
            BigQuerySessionStoreAdapter::new_native("dataset", "unused"),
            "continuity",
        );
        let first = sink.next_snapshot_ms();
        let second = sink.next_snapshot_ms();
        let third = sink.next_snapshot_ms();
        assert!(first < second && second < third);
    }
}
