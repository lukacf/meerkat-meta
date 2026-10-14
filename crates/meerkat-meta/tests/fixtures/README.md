Frozen released-corpus fixtures. Never regenerate these with current code:
current writers cannot and must not mint released envelopes, and a
fixture re-synthesized by the pinned writer silently passes exactly the
writer-drift bugs these tests exist to catch (the 0.8.11 fleet-import
regression shipped past a synthetic 26-chain test for that reason).

All free-text content in these fixtures (system prompts, instructions,
message bodies, labels) is synthetic. Only the structural shape (keys, key
order, row layout, ids, timestamps, digests) is preserved, because that
shape is what the tests pin.

- v0_8_10_released_session.json - SYNTHETIC: a meerkat 0.8.10 (producer
  version) system-only session envelope, byte-identical to Meerkat core's
  synthetic recovery-migration fixture. It keeps the released envelope
  shape: version 2, exactly one system message, no transcript-history key,
  the seven released metadata keys, a legacy recovery_migration checkpoint
  stamp (schema 1, generation 0, revision 6) and the four usage token keys.
  Its system prompt, roughly 12 KB build state, tool catalogue, labels and
  identifiers are synthetic; the prompt carries the marker phrase
  `Example Review Agent`. Excluded from the published crate. Used by the
  adapter import-on-load regression and the released-v2 import leg of
  identity_first_lazy_recall_continuity.rs.

- v0_8_10_zero_rewrite_supervisor_session.json - a 0.8.10-shaped
  mob-supervisor snapshot envelope (zero-rewrite transcript graph: one
  revision, no commits key on the wire, singleton live-head body equal to
  the live transcript; session 019f2bdc-a781-7060-bff8-0b97b7a4fcee) with
  SYNTHETIC content. The released envelope's keys, key order, ids,
  timestamps and spelling are kept; every free-text value (system prompt,
  build-state prompts) is deterministic synthetic text, the mob namespace
  is generic, and the transcript revision digest (and head) is computed
  with meerkat_core::released_0810_transcript_serialized_rows_digest.
  The retired checkpoint stamp is kept verbatim: the importer strips it as
  untrusted metadata. The strict importer refuses it with a typed error.
  Excluded from the published crate. Used by
  `released_zero_rewrite_history_refuses_typed_on_adapter_load` in
  `src/identity_first/adapters.rs`, which pins typed, session-scoped import
  refusal, byte preservation, and unrelated-session usability.

- released_0_8_8_realms/ - four COMPLETE realms (continuity.sqlite3 v2 +
  runtime.sqlite v1 + mobkit satellites + WAL/SHM/.mfence sidecars, no
  realm_manifest.json) minted by the released mobkit 0.8.8 rpc_gateway
  (embedding meerkat SDK 0.8.10), driven over stdin JSONL with synthetic
  turns. Consumed by tests/released_realm_upgrade_drive.rs, which boots the
  CURRENT gateway over a staged copy. Never regenerate with current code,
  and never open these files with sqlite in place: a read/write open
  checkpoints and truncates the WAL, destroying the released byte shape
  (the test pins the continuity main/WAL sizes and fails loudly). The empty
  blobs/ directory is part of the released shape; git cannot track it, so
  the test's staging helper restores it.
    - baseline/       multi-boot clean-shutdown history, 5 turns
                      (head: 11 messages)
    - burst_drain/    pipelined 6-send burst fully drained by
                      mobkit/shutdown (head: 21 messages)
    - crash_sigkill/  SIGKILLed after the burst drained: un-checkpointed
                      WALs, no shutdown attestation; all 10 inputs consumed
                      and the legacy runtime snapshot in sync with the head
                      (the kill landed at idle)
    - deploy_cycles/  4 boot/turn/clean-shutdown cycles (head: 9
                      messages); rewrite_count is 0 - the released binary
                      minted no resume rewrites for an unchanged system
                      prompt

- released_0_8_26_store/ - one COMPLETE persistent store (`state/`) minted
  by the released MobKit 0.8.26 rpc_gateway (release archive sha256
  fdac071b0a1b3983c67e6a75fc279135aa9bff9dcae56d8caaaaa8bb9dfbafb1) over
  stdin JSONL with no provider key (`runtime_options.demo_llm`):
  mob_storage persistent, so `event_log.sqlite3` holds the mob's events
  and `event_log.sqlite3.composition.json` carries the predecessor
  creator field `created_by_mobkit`; one durable identity
  (`learner:fixture-0001`, session 01a13aaa-4918-71e2-b44c-366e9b2d4de3)
  with one driven turn; two agent memories in
  `agent-memory/default.sqlite3`; one memory ledger assertion; the
  predecessor database names and ledger domains and the `mobkit/` realm.
  `mint_released_0_8_26_store.py` beside it is the capture driver (the
  definition it used includes `tools.mob_tasks`, which 0.8.26 accepted,
  and no profile `system_prompt`, which 0.8.26 ignored and 0.9 refuses);
  `checksums.sha256` pins every file. Two things were left out of the
  capture: the empty `blobs/` directory (git cannot track it; the test's
  staging restores it) and the gateway peer key `peer_key.ed25519` (key
  material; an installation without host pairings mints a new one on
  boot). The mob supervisor key inside `event_log.sqlite3` was minted by
  this capture and authorizes nothing outside it. All content is
  synthetic. Never regenerate with current code and never open these files
  in place. Consumed by tests/released_0_8_26_store_adoption.rs.

- ledger_v1_closure/ - the continuity closure of one member session
  (019fae11-4dd7-7301-9754-67b646603fb3) carrying a 26-rewrite
  resume-system-prompt-refresh chain, with SYNTHETIC content. The row
  layout, strand topology (7 strands, 26 rewrites, a 57-message head),
  session ids, envelope timestamps and every non-content column have the
  released shape; every free-text value is deterministic synthetic text,
  the mob namespace is generic, action argument dates are synthetic dates
  (time zone UTC), and provider call, response and reasoning ids are
  consistent synthetic ids (same prefix and length, every reference kept
  equal). Every strand id, commit digest (revision, parent revision,
  original and replacement spans) and the head revision are computed with
  meerkat_core::released_0810_transcript_serialized_rows_digest; the
  cas_token is computed over head_json. Retired checkpoint stamps stay
  verbatim (the importer strips them). The leg goes red without the
  adoption fix, with the class-3 refusal. JSON encoding: every TEXT/BLOB
  value is lossless base64 {b64,len}, numbers/nulls verbatim, per-table
  column lists; sha256 pinned in checksums.sha256, source DDL in
  continuity-schema.sql. Excluded from the published crate. Consumed by
  identity_first_lazy_recall_continuity.rs
  (downstream_rewrite_carrying_closure_adopts_resumes_and_takes_a_turn),
  which reconstitutes it at test time VERBATIM - every row of every table
  through the bundle's own DDL, zero document surgery - and boots the
  harness under the bundle's OWN identity space (the mob id read from the
  head's comms name, the profile and member identity read from the
  bundle), so the persisted mob_member_binding and comms_name match the
  booting mob by construction (the harness adapts to the bundle, never the
  reverse). The class-3 property the head carries: released envelope
  version 2, rewrite_count 26, and NONE of the current authority fields
  (graph_prefix / rewrite_prefix / message_row_prefix) - a head that
  structurally cannot authorize a current mutation and must be ADOPTED
  under the import receipt on the first projected write.

- security_idempotency/ - a three-state head+snapshot evolution of one
  member session (019fae11-4e87-7482-8796-54b2dac1f410): the untouched
  gen-20 corpus, the row after ONE boot of the fixed binary on a fresh
  seed, and the row after a SECOND boot (the exactly-once violation:
  identical head_revision, same length, different bytes) - with SYNTHETIC
  content. Keys, ids, timestamps, the tool-visibility state and the
  boot-to-boot drift keep their structural shape; every free-text value
  (prompts, instructions, message bodies) is deterministic synthetic text,
  the mob namespace is generic, and each cas_token is computed over its
  head_json. sha256 pinned in checksums.sha256. Excluded from the
  published crate. Consumed by
  identity_first::adapters::tests::downstream_security_boot_drift_is_zero_durable_change,
  which pins that strict head equality SEES the two-boot drift (updated_at +
  the HashSet-ordered tool-visibility Allow arrays, filed upstream as S5)
  while the scoped exact-resave equality reads it as zero durable change.

The current wire-contract fixtures below are shared across languages and are
not part of the frozen corpus. They are expected to change when the wire
changes, following each fixture's maintenance rules. The freeze above applies
only to the released corpora.

- role_migrations_init_params.json - the hand-authored wire contract for
  boot-scoped member role migrations: one gateway init-params object
  carrying a top-level role_migrations array of {identity, from_role}
  declarations (synthetic example values: identity domain:automation
  migrating from role domain). Read by BOTH languages from this ONE file.
  Rust: identity_first::bridge::tests::
  the_committed_wire_fixture_deserializes_into_declarations include_str!s it
  and deserializes params["role_migrations"] into Vec<RoleMigrationDeclaration>.
  Python: sdk/python/tests/test_role_migrations.py::
  test_builder_output_matches_the_committed_wire_fixture asserts that
  MobKit.builder().role_migrations([...]) puts exactly this array in the
  gateway init params. Renaming a key on either side goes red on that side
  instead of both sides staying green while a host arms nothing.

- application_tool_policies_init_params.json - generated gateway init params
  shared by `src/member_tool_policy.rs` and
  `sdk/python/tests/test_application_tool_policies.py`. Rust's
  `the_committed_wire_fixture_installs_its_carried_provider` test verifies
  installation of the carried policy; Python checks the builder's wire output.
  The canonical policy bytes contain a computed digest, so do not hand-edit
  this fixture into validity. Regenerate it by setting `MOBKIT_WRITE_FIXTURE=1`
  when running that Rust test from the repository root:

  ```bash
  MOBKIT_WRITE_FIXTURE=1 ./scripts/repo-cargo test -p meerkat-mobkit --lib \
    the_committed_wire_fixture_installs_its_carried_provider
  ```

- live_contracts_v1.json - shared live wire-contract cases (synthetic
  realm, principal, auth binding and session instructions) read by Rust
  `crates/meerkat-meta/tests/live_contracts.rs` and
  `src/public_live_config.rs`, Python
  `sdk/python/tests/test_live_contracts.py`, and TypeScript
  `sdk/typescript/tests/live.test.ts`. Update the current contract and its
  cross-language expectations together; this is not a frozen released capture.

- console_voice_v1.json - shared console voice wire-contract cases (typed
  voice error payloads, context-status and captions methods with synthetic
  request and channel ids) read by the Rust `src/console_voice/` modules and
  `console/src/lib/voice-session.test.ts`. Update it together with both
  readers when the console voice wire changes.
