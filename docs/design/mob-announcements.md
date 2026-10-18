# Mob announcements

Status: active only when a gateway enables it at startup
(`runtime_options.mob_announcements`). When it is off, nothing opens the
announcement database and the methods answer `mob_announcements_unavailable`.

Developments are announced to a mob, and members subscribe to the topics they
care about in the mobs they name. Each subscriber receives what it is owed
reliably, in bounded batches instead of one wake per announcement, including
announcements from agents it has never contacted. There is no new top-level
concept: the set of agents that hear each other is the mob, and the
announcement store follows the mob owner's records instead of keeping a
membership of its own.

## Ownership

| Fact | Owner |
| --- | --- |
| Mobs, their members and their lifecycle | Meerkat (mob roster and lifecycle) and Meta's identity runtime (registered identities) |
| Announcement: mob, sequence, source key, announcer, immutable content, digest | Mob announcements |
| Member subscription: member, topics, mobs, state, revision, start frontier | Mob announcements |
| Capture of an announcement for a subscription | Mob announcements, in the announcement's own transaction |
| Sealed batch: exact items, immutable body, content digest | Mob announcements |
| Batch delivery progress (sealed, submitting, committed, delivered, ...) | Mob announcements, moved only by native evidence |
| Commit into the member's runtime inbox, binding to its incarnation | Meerkat continuation owner |
| Admission as one input of the member's session | Meerkat runtime |
| What the agent did with that input | Meerkat runtime (input state), then the agent |

Announcements live in Meta's own store, keyed by mob id, beside the mob
rather than inside Meerkat's structural mob event ledger. The ledger is the
mob's structural record, which Meerkat reads back to rebuild mob state;
announcements need their own retention and a capture that commits with the
announcement. Nothing is mirrored into the ledger, and nothing in the store
decides who belongs to a mob: a subscription ends, and a mob leaves the
subscriptions that name it, only from the mob owner's own records.

A sealed batch is delivered as one Meerkat host continuation in the
`meta.mob_announcements` namespace, keyed by the batch id. Meerkat commits it
into the member's durable runtime inbox together with the first binding of
that key, binds it to the member incarnation current at the first
submission, answers an identical resubmission with the original receipt,
refuses other content under the same key, and admits it as one input when
the member is served. The announcement owner never decides or mirrors any of
that; it reads the native status back.

## Delivery lifecycle

The owner persists its intent before every native effect and moves a batch
only from native evidence:

- `sealed`: items and body are fixed. A cancelled or ended subscription may
  still cancel it here, because nothing native exists yet.
- `submitting`: a native submission may have started. If the answer is lost,
  the native status settles it: not committed means the identical delivery is
  submitted again; anything else is recorded as found.
- `committed`: the member's inbox holds it.
- `delivered`: admitted as one input. This says nothing about processing.
- `refused`, `stranded`, `cancelled`, `blocked`: settled. A stranded batch
  (its incarnation retired before admission) hands its announcements to a
  successor batch with a new id that names its predecessor; a key is never
  reused for other content.

## Subscription lifecycle

A subscription is `active`, `paused`, `cancelled` (by a caller) or `ended`.
It ends, with a typed reason, only on a final fact read from the owner of
its member or its mobs:

- `member_left`: the member's identity is no longer registered with this
  gateway.
- `member_unusable`: the member's runtime is not served here (it is placed on
  a host, or bound to an external runtime), so a delivery could never be
  admitted.
- `mobs_ended`: every mob the subscription named was destroyed.
- `peer_unsupported`: the member's peer gateway predates keyed inject.

A peer gateway's signed `unknown_member` or `unusable_member` answer ends
the subscription as `member_left` or `member_unusable`.

The member checks run when a subscription is created and before every
submission, so a subscription whose member left ends at its next delivery
instead of retrying forever. A member without a live incarnation for a while
(being respawned, for example) is not an end: its batch waits and is
retried. When a subscription ends, what it captured and no batch holds
settles, and its sealed batches nothing else wants are cancelled.

A destroyed mob leaves every subscription that names it, at a new revision,
and is listed in the subscription's `ended_mobs`; what was already captured
from it stays owed. The owner reconciles the mobs it knows against the mob
owner's records at startup and whenever the set of mobs changes, and right
after a subscription change. A mob created later under the same id is
another mob: nothing captured for the old one reaches its subscribers.

## Attention policy

A member's owed announcements are sealed into at most one outstanding batch
at a time, and at most one batch per wake interval, when the backlog reaches
`max_count` announcements or `max_bytes`, when the oldest has waited
`max_delay_ms`, or when a paused subscription resumes. An announcement
captured by several subscriptions of one member appears once in the batch
body. No model call happens per announcement.

The batch body tells the agent that announcement bodies come from their
announcers and are data, not instructions. Each announcement is introduced
by one line naming its mob, sequence, announcer, topics and source key, every
name quoted, and each line of its body is quoted with `> `, so a body cannot
pass as the batch's own structure.

The dispatcher walks unsettled batches in laps: a lap ends at the batch that
was newest when it started, so new batches cannot crowd out an older one,
and no batch is read again before its own settle pause. A batch that keeps
failing is retried at the slowest settle pause. While retention is behind,
its passes repeat after a short pause and delivery rounds keep their own
pace.

## Scope and principals

- The host reaches every mob this gateway hosts, the same reach as
  `meta/send`. A host call announces as `{"kind": "host"}`; it cannot
  announce as a member. Members announce as themselves, into their own mob
  only, through the member tools. Every mob an announcement or a subscription names
  must be hosted here: managed and not destroyed, or restored and held until
  its owner resolves.
- A member is `{mob_id, identity}`: identities are unique only within one
  mob. Subscribers are registered identities of this gateway's mob that its
  runtime serves, and members of mobs on peer gateways. A member of another
  mob hosted here is refused with `unsupported_member`.
- A subscription names its mobs explicitly (this gateway's mob by default).
  Nothing reaches it from a mob it does not name.
- The announcer is part of an announcement's content digest, so a replay
  under the same source key must name the same announcer.
- The announce result counts the subscriptions that captured an
  announcement; it never names them.

## Member tools

A member profile opts into the `mob_announcements` tool bundle by listing it
in `tools.rust_bundles`. The bundle is host-only: agent-created child mobs
never receive it. It has two tools:

- `mob_announce` `{topics, body, key?, correlation_id?}`: announce into the
  caller's own mob, as `{"kind": "member", "identity": ...}`. A retry with
  the same key and content is not announced twice; without a key, the call
  itself is the key. The result counts the subscriptions that captured the
  announcement and never names them.
- `mob_subscription` `{action, subscription?, topics?, expected_revision?}`:
  `subscribe` (create, or replace the topics at `expected_revision`),
  `pause`, `resume`, `cancel` and `list`, for the caller's own
  subscriptions to its own mob.

Every call is attributed to exactly one member: the member of this gateway's
mob whose session the call comes from, as both the mob roster and the
identity runtime record it. A call that cannot be attributed so is refused
and changes nothing. A member's keys and subscription names live in a
namespace of their own, so no member can reach or learn another member's.
Per-member limits (`member_limits`, defaults shown below) bound announcements
and bytes per minute, live subscriptions, and topics per subscription; the
minute windows live in memory and start over on a restart.

## Members on peer gateways

A member of a mob on a peer gateway is delivered to with `inject_keyed` over
the signed cross-mob control channel, and its status read with
`inject_status`. The peer is the gateway this gateway's contact directory
files the member's mob id under, with a pinned key; this gateway signs as its
own gateway key. The peer commits the batch to the member's continuation
owner under the batch id, scoped to the grant that admitted the call, and
every answer it gives is signed: only an answer the pinned key verifies is
evidence. A lost answer leaves the batch submitting until `inject_status`
settles it, exactly as for a local member. A body whose JSON encoding would
not fit one control frame is never sealed for a peer member: batches for
peer members are bounded by the frame as well as by `max_bytes`.

The serving side needs a control listener with a grant that names
`inject_keyed` and `inject_status` for this gateway's key (see the access
control guide); `runtime_options.control_request_window_ms` bounds how far a
request's issue time may be from the serving gateway's clock.

## Configuration

`rpc_gateway` takes it through `runtime_options.mob_announcements` on a
persistent, identity-first launch:

```json
{
  "mob_announcements": {
    "enabled": true,
    "batch": {
      "max_count": 100,
      "max_bytes": 65536,
      "max_delay_ms": 60000,
      "min_wake_interval_ms": 60000
    },
    "retention_ms": 604800000,
    "member_limits": {
      "announcements_per_minute": 30,
      "announcement_bytes_per_minute": 65536,
      "subscriptions": 8,
      "topics_per_subscription": 16
    }
  }
}
```

The values shown are the defaults. Announcements are retained until every
subscription that captured them has settled them and the retention window has
passed; a mob's retained announcements never have holes. `retention_ms` is
at least 3600000 (one hour), so a retried announcement or subscription change
inside it still meets its original result. Retention runs hourly; one pass
removes at most 1000 announcements (oldest first across mobs), 1000 settled
batches, 1000 operation receipts and 1000 cancelled or ended subscriptions
that changed before the window, with the member rows and the logs of
destroyed mobs nothing refers to any more. While one kind stops at a limit
the passes repeat after a short pause, not an hour.

A subscription that asks to start from a frontier older than a mob's
retained announcements gets a typed `gap` error, never a silent jump to the
latest announcement. One that names a frontier beyond a mob's head (0 for a
mob that has announced nothing yet) gets a typed `frontier_beyond_head`
error, never a clamp, and a frontier may name only the mobs the subscription
names.

The announcement database is `mob_announcements.sqlite3` in the state
directory, at schema version 1. It is not a storage slot (it has no legacy
spelling to reconcile), and storage maintenance fences it in every build. A
gateway refuses an announcement database with a newer schema and fails
`meta/init` closed, so after a later schema change, going back to an older
binary means disabling mob announcements or keeping the newer binary.

## RPC methods

| Method | Purpose |
| --- | --- |
| `meta/mob_announcements/announce` | Announce into a mob hosted here (this gateway's by default); the same source key with the same content replays, other content conflicts. |
| `meta/mob_announcements/get` | Read an announcement by mob and source key. |
| `meta/mob_announcements/subscriptions/put` | Create a member subscription, or replace its topics and mobs at its current revision. |
| `meta/mob_announcements/subscriptions/pause`, `resume`, `cancel` | Change a subscription at its current revision. |
| `meta/mob_announcements/subscriptions/get`, `list` | Read subscriptions. |
| `meta/mob_announcements/deliveries/list` | List batches for a member or a subscription. |
| `meta/mob_announcements/deliveries/observe` | The batch, the native status, and the admitted input's own state. |
| `meta/mob_announcements/deliveries/reconcile` | Advance one batch from native evidence now, then observe it. |

Every subscription mutation carries an `operation_id`: a replay returns the
original result and the same id with another request is refused. The methods
are reached only through the host's stdio unified RPC: the HTTP console has
no route for them. There is no principal binding beyond the scope rules
above: any host caller can announce, subscribe any registered identity and
read any delivery, the same reach as `meta/send`.

## Limits

- A runtime with a native work authorization host refuses host
  continuations, so subscriptions there are refused with a typed
  `governed_runtime` error.
- A sealed batch whose subscriptions are all paused waits for a resume, and
  while it waits the member gets no other batch.
- A paused subscription, or one whose member keeps a batch unsettled, keeps
  capturing. What it has captured is not capped and holds retention for every
  mob it names; after a resume the backlog drains at `max_count` per wake
  interval (100 a minute by default). Cancel a subscription that will not
  resume.
- Members of other mobs hosted here (child mobs) cannot subscribe yet:
  Meerkat records no final fact that such a member left its mob (a respawn
  removes the roster entry and adds it back), so their subscriptions could
  not end safely.
- Topics match exactly. Semantic matching belongs above this layer.
- The dispatcher's walk position and next-look times live in memory: a
  restart starts a new lap and reads every unsettled batch once.
