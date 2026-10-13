CREATE TABLE continuity_records (
                identity       TEXT PRIMARY KEY,
                agent_runtime_id TEXT NOT NULL,
                session_id     TEXT NOT NULL,
                generation     INTEGER NOT NULL,
                checkpoint_version INTEGER NOT NULL,
                fencing_token  INTEGER NOT NULL
            );
CREATE TABLE continuity_session_heads (
        session_id     TEXT PRIMARY KEY,
        identity       TEXT NOT NULL,
        generation     INTEGER NOT NULL,
        checkpoint_version INTEGER NOT NULL,
        fencing_token  INTEGER NOT NULL,
        head_revision  TEXT NOT NULL,
        message_count  INTEGER NOT NULL,
        rewrite_count  INTEGER NOT NULL,
        head_json      BLOB NOT NULL,
        cas_token      TEXT NOT NULL
    );
CREATE TABLE continuity_session_rewrites (
        session_id     TEXT NOT NULL,
        rewrite_idx    INTEGER NOT NULL,
        parent_strand  TEXT NOT NULL,
        parent_len     INTEGER NOT NULL,
        strand         TEXT NOT NULL,
        strand_len     INTEGER NOT NULL,
        commit_json    BLOB NOT NULL,
        identity       TEXT NOT NULL,
        generation     INTEGER NOT NULL,
        created_at_ms  INTEGER NOT NULL,
        PRIMARY KEY (session_id, rewrite_idx)
    );
CREATE TABLE continuity_strand_messages (
        session_id     TEXT NOT NULL,
        strand         TEXT NOT NULL,
        seq            INTEGER NOT NULL,
        message_json   BLOB NOT NULL,
        identity       TEXT NOT NULL,
        generation     INTEGER NOT NULL,
        created_at_ms  INTEGER NOT NULL,
        PRIMARY KEY (session_id, strand, seq)
    );
CREATE TABLE meerkat_schema (
    domain TEXT PRIMARY KEY,
    version INTEGER NOT NULL
);
CREATE TABLE session_snapshots (
                session_id     TEXT PRIMARY KEY,
                identity       TEXT NOT NULL,
                generation     INTEGER NOT NULL,
                checkpoint_version INTEGER NOT NULL,
                fencing_token  INTEGER NOT NULL,
                data           BLOB NOT NULL
            );
