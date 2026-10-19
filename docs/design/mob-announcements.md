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
- `expired`: settled. It was sealed before the retention window and nothing
  native ever held it: still sealed, or submitting while the native status
  reads not committed. For a member on a peer gateway it expires only once
  its last request, stamped when it was issued, is older than
  `control_request_window_ms` plus the slowest settle pause, and it is not
  submitted again meanwhile, so a request still in flight is refused by the
  peer as stale instead of landing after the expiry. Gateways that deliver
  to each other should use the same window: a peer with a larger one, or a
  clock behind this gateway's, may still admit such a request, and then the
  announcements are delivered and also reported as expired, a false gap,
  never a loss.
- `expired_unverified`: settled. It was submitting, and no status read
  brought its outcome before the retention window, and, for a member on a
  peer gateway, the quiet window since its last request, had passed. It is
  never submitted again, and its announcements are reported as
  `delivery_not_confirmed`: neither delivered nor lost. A peer commits a
  delivery it admitted only by the request's deadline, its issue time plus
  the window: it bounds the member's preparation by the time left and checks
  the deadline again just before the commit. So a commit after the batch
  settled is left to one already under way at the deadline, or to a peer
  whose clock or window differs; that delivery then arrives after later
  announcements, and order is not kept across this state.
- `committed_unconfirmed`: settled. The member's inbox committed it, its
  receipt is kept, and its admission was never confirmed: past the retention
  window since the commit a verified status still read it as pending (a
  gateway keeps a delivery waiting for a member of a mob it does not host),
  or no status could be read for the quiet window after that. It is never
  submitted again, and its announcements are reported as
  `admission_not_confirmed`. The member's next batch goes on without passing
  it: it was committed first.
- Of a batch in either state, what a status read finds later is kept on it
  as its `late` outcome, read once per quiet window until it is final or
  retention removes the batch. It never reopens the batch.

What a member was owed and never got is reported, never dropped silently:
the member's next batch opens with a gap line per mob saying how many
announcements it did not receive and why (`expired`, `not_retained`,
`delivery_not_confirmed` or `admission_not_confirmed`), and
the batch's `gaps` field carries the same counts. A batch for a member on a
peer gateway carries as many gap lines as fit what its items leave of one
request; the rest wait for the next batch. A successor does the same, since
a gap line can be longer than the item it stands for. A batch that ends without
delivering (cancelled, refused, blocked, or stranded with no successor)
hands its gap lines to the member's next batch, and so does one that settles
with its delivery not confirmed: a member may then read those lines twice,
never not at all.

## Subscription lifecycle

A subscription is `active`, `paused`, `cancelled` (by a caller) or `ended`.
It ends, with a typed reason, only on a final fact read from the owner of
its member or its mobs:

- `member_departed` (with its evidence): the member departed for good. A
  departure is built only from a departure fact; today that is the member's
  peer gateway's signed `member_departed` answer.
- `mobs_ended`: every mob the subscription named was destroyed.
- `member_mob_ended` (with `why`): the member's own mob, a mob hosted here
  other than this gateway's, was destroyed, created again, completed or
  reset after the subscription was made.

Everything else is a state that can change, so it suspends and ends
nothing: a member not registered where it is served, a member placed on a
host or bound to an external runtime, a governed runtime, a member with no
live incarnation, a peer that is starting, does not serve keyed inject,
skews its clock, refuses a request for its timing or does not answer. Each is classified once, with the class
of its evidence (verified, unverified or local), and the owner keeps the
latest per member as a marker for `deliveries/observe` and
`subscriptions/get`. The marker is a projection: it is rebuilt from the next
attempt after a restart, and no transition reads it. A signed "nothing was
committed" returns the batch to `sealed`, to be submitted again at the
slowest settle pace and to expire after the retention window as any owed
batch does; an unknown outcome keeps the batch `submitting` until a status
read settles it, or until it settles as `expired_unverified`.

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
  runtime serves, members of mobs hosted here (the mobs members created,
  see Members of mobs members created), and members of mobs on peer
  gateways. A mob id that names a mob hosted here and a peer in the contact
  directory alike is refused with `ambiguous_mob`.
- A subscription names its mobs explicitly (this gateway's mob by default).
  Nothing reaches it from a mob it does not name.
- The announcer is part of an announcement's content digest, so a replay
  under the same source key must name the same announcer.
- The announce result counts the subscriptions that captured an
  announcement; it never names them.

## Member tools

A member profile opts into the `mob_announcements` tool bundle by listing it
in `tools.rust_bundles`. The bundle is host-only: mobs members create never
receive it, and the host RPC subscribes their members (see Members of mobs
members created). Members of those mobs, which untrusted content can steer,
may get the tools only through a per-profile opt-in that this release
cannot offer yet, so `member_tools_in_child_mobs`, a grant to every inline
profile of every child mob, is refused at launch. It has two tools:

- `mob_announce` `{topics, body, key?, correlation_id?}`: announce into the
  caller's own mob, as `{"kind": "member", "identity": ...}`. A retry with
  the same key and content is not announced twice; without a key, the call
  itself is the key. The result counts the subscriptions that captured the
  announcement and never names them.
- `mob_subscription` `{action, subscription?, topics?, mobs?, expected_revision?}`:
  `subscribe` (create, or replace the topics and mobs at
  `expected_revision`), `pause`, `resume`, `cancel` and `list`, for the
  caller's own subscriptions. `mobs` defaults to the caller's own mob and
  may name only mobs in its reach (see Mob lineage); any other mob is
  refused with `unrelated_mob` and nothing changes.

Every call is attributed to exactly one member: the member of this gateway's
mob whose session the call comes from, as both the mob roster and the
identity runtime record it, or, for a member of a mob a member created
once its profile can opt in, the member that the session's persisted member
binding names and that mob's roster binds to the same session under the
same role. A call that cannot be attributed so is refused and changes
nothing. A member's keys and subscription names live in a
namespace of their own, so no member can reach or learn another member's.
Per-member limits (`member_limits`, defaults shown below) bound announcements
and bytes per minute, live subscriptions, and topics per subscription; the
minute windows live in memory and start over on a restart.

## Mob lineage

A member reaches its own mob, the chain of mobs above it, and every mob
below it, among the mobs members created; never a sibling's or a cousin's.
The host RPC is not bound by this and reaches every mob hosted here. For a
member of this gateway's own mob, the mobs below it are every mob any of
its members created, `delegate`'s helper mobs (`implicit-{session}`)
included, not only the ones that member created.

Which mob created which is a record the announcement store keeps, one per
incarnation of a hosted mob. It is derived once, from three durable
records Meerkat keeps, and never rewritten from live state:

1. The mob's owner binding: the session of the member that created it,
   bound once at creation (`mob_create`, and `delegate`'s helper mobs).
2. That session's own persisted member binding: the parent mob and the
   member.
3. The parent's member-creation journal, which must record the same
   session for the same member and role.

The live roster is not read, so a repoint of the creator to a new session
loses nothing. The record is derived when the mob set changes, at startup,
and when a member's subscription first names the mob.

A mob gets a record with no parent, which no member reaches, only on a
proof: it has no owner binding, its owner session's metadata is absent or
names no member, the parent's journal disagrees, or its parent is not
hosted here after this gateway restored every mob it hosts. A read that
fails writes nothing and is tried again. One window fails closed: a mob
first seen after its creator respawned finds the creator's archived session
unreadable and stays unreached until Meerkat tears it down at the next
restart (a child mob does not outlive its creator's respawn there).

A record belongs to one incarnation: the owner session and the time of the
first event in the mob's own log. A mob destroyed and created again under
the same id starts a new log and gets a new record. A record also keeps the
incarnation of its parent, and the walk follows it only to the parent's
record of that incarnation, so a parent created again under the same id
adopts no mob of the old one. This gateway's own mob is the root: it has no
record.

The walk reads the live records only. A subscription naming a mob below
another that has no record yet is refused with `unrelated_mob` until the
next reconcile records the mob in between: it fails closed. When a mob is no longer
hosted its record ends; ended records are removed by retention once no
live record names the mob as parent. A mob held after a restart keeps its
record; a held mob whose parent's record ended loses its reach upward.

Every reader lists the mobs hosted here in one order. The managed listing
comes first only to finish the restore of persisted mobs: it fails rather
than answer before the restore, where the held listing answers empty. Held
mobs come next, then running ones: activation adds a held mob to the
running ones before it drops the hold, so a mob that activates between two
reads is in one of them. Today a held mob stays held until a later restart:
Meerkat activates or tears down a held mob only within its restore, or when
its host calls for that, which this gateway never does. The order protects
activation paths still to come.

## Members of mobs members created

A member of a mob hosted here other than this gateway's own (a mob a member
created, or any other mob this gateway hosts) is `{mob_id, identity}` with
its plain roster id, and is delivered to through this gateway's runtime: the
batch is a continuation for that mob's member, with the native key, binding
and status rules of members of this gateway's own mob.

How a member is reached (this gateway's mob, a mob hosted here, or a peer)
is decided when it is first subscribed and kept with it, together with the
incarnation of its mob; every batch carries the route it was sealed with.
No route is ever assumed: a stored batch without one does not decode, the
store refuses a subscription the owner resolved no route for, a member with
no kept route is not sealed, and a local route names the member's own mob;
on a gateway that names no mob of its own, no local route holds.
The owner checks a batch's route against its member before anything else,
on every path, so a batch whose member names another mob is blocked: nothing
is prepared, ended, submitted or read for this gateway's member of that
name.
A member is never rerouted: a put whose route would differ from the kept
one is refused with `ambiguous_mob`, as is a mob id that names a mob hosted
here and a peer in the contact directory alike.

A put for such a member checks that its mob runs here and has not
completed, and that the member is on its roster now (`unknown_member`
otherwise). A mob held after a restart until its owner resolves is
`native_unavailable`, never an end: its members' batches wait. Roster
absence later ends nothing: a member being respawned is off the roster for
a while, and its batch waits and is retried.

Its subscriptions end with `member_mob_ended` only on the mob's own durable
records:

- `completed` or `reset`: a `MobCompleted` or `MobReset` event in the mob's
  log after the cursor the subscription was made at. The log is read when
  the member's backlog is sealed, before a batch's first submission and
  before every resubmission, and the owner records how far it read, so each
  event is read once per member. A batch therefore never carries what was
  captured for a subscription the event ended next to what one made after
  it wants. A member subscribed again after a reset is delivered to again;
  a completed mob refuses new puts. The completion of a mob named in a
  selector ends nothing: only the subscriber's own mob does.
- `created again`: the mob runs under the same id with another incarnation
  (owner session and first event time). What the member had in the old
  incarnation ends at the mob set reconcile, the read before a delivery, or
  the next put for the member, which then subscribes it in the new one.
- `destroyed`: the mob is neither running nor held after this gateway
  restored every mob it hosts. The mob set reconcile ends the member's
  subscriptions, as does the next delivery.

No public surface completes or resets a mob a member created: the agent
mob tools have no lifecycle verb. The `completed` and `reset` ends are covered
by the owner's tests against a scripted native owner, not by a process test.

Subscriptions for these members are bounded, whoever makes them:
`child_member_limits.subscriptions` live ones on the gateway (1024 by
default), and `child_member_limits.per_mob` live ones naming any one mob
(256), so one announcement is captured for at most that many of them. A put
past either is refused with `child_member_limit` and changes nothing; an
ended or cancelled subscription no longer counts.

A delivery already committed to a member of a mob that is then destroyed
stays pending in Meerkat, which keeps a delivery to a mob it does not host
waiting in case that mob is restored. Once the mob is destroyed for good,
the owner settles the batch `stranded` after ending the member's
subscriptions, so it has no successor and retention is not held. Meerkat's
member delivery address has no mob incarnation: if a mob is created again
under the same id with the same member at the same generation before the
row settles, Meerkat may still deliver it there.

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
    },
    "member_tools_in_child_mobs": false,
    "child_member_limits": {
      "subscriptions": 1024,
      "per_mob": 256
    }
  }
}
```

The values shown are the defaults. Announcements are retained until every
subscription that captured them has settled them and the retention window has
passed; a mob's retained announcements never have holes. What waits longer
than the window for a delivery that never started expires instead of holding
retention: a capture no batch holds settles `expired` once it is older than
the window, and so does a batch nothing native holds once it was sealed
before it (so an announcement captured into such a batch waits at most about
twice the window). A batch the native owner may already hold keeps its owed
record but not the announcement, since its own body carries the content; if
it later strands, its successor reports what is no longer retained as a gap. `retention_ms` is
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
  `governed_runtime` error. A member whose runtime becomes governed after it
  was subscribed, here or on a peer, waits with the marker `governed` until
  its runtime no longer is; its owed batches expire after the retention
  window meanwhile.
- A sealed batch whose subscriptions are all paused waits for a resume, and
  while it waits the member gets no other batch.
- A paused subscription, or one whose member keeps a batch unsettled, keeps
  capturing. What it captured expires after the retention window, so a
  subscription paused longer than that gets a gap line on resume instead of
  those announcements; a shorter pause gets everything, and after a resume
  the backlog drains at `max_count` per wake interval (100 a minute by
  default).
- Members of other mobs hosted here (child mobs) subscribe, but Meerkat
  records no final fact yet that such a member left its mob (a respawn
  removes the roster entry and adds it back). A retired member's
  subscriptions therefore wait until its mob completes, resets, is created
  again or is destroyed, or until they are cancelled.
- Topics match exactly. Semantic matching belongs above this layer.
- The dispatcher's walk position and next-look times live in memory: a
  restart starts a new lap and reads every unsettled batch once.
