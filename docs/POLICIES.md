# Tape coalescing policies — the consumer contract

Tape applies **at most one write per record per frame**. When the feed
outruns the frame rate, multiple updates to the same record are merged
("coalesced") into that single write. The per-field policy you declare at
subscribe time decides *how* they merge — and therefore precisely what your
UI can and cannot lose under load.

```ts
client.subscribe('quotes', {
  last: 'latest',
  volume: 'accumulate',
  trades: { policy: 'sequence', capacity: 64 },
});
```

Fields you do not list default to `latest`.

## The three policies

### `latest` — newest value wins

Every pending update overwrites the previous pending value; the flush writes
the newest one.

- **Kept:** the most recent value when this channel flushes. Deferral can make
  the rendered value lag by more than one frame.
- **Lost under load:** every intermediate value. If `last` ticks
  100.1 → 100.2 → 100.3 inside one frame, subscribers observe only 100.3;
  the intermediates are **never observable**, not even transiently.
- **Use for:** anything that answers "what is it now?" — prices, statuses,
  sizes, labels.

### `accumulate` — numeric deltas sum

Updates carry numeric **deltas**; pending deltas are summed and the flush
adds the sum to the stored value.

- **Kept:** everything. Ten `+5` deltas in one frame flush as `+50`. If a
  low-priority channel is deferred past its frame, its pending sum keeps
  growing and applies whole later. **Nothing is lost — arrival granularity
  is.** You cannot see the individual increments, only their total.
- **Lost under load:** nothing (numerically).
- **Use for:** counters and totals — volume, message counts, PnL deltas.

### `sequence` — every entry, in order, bounded

Each update appends one entry object to a per-record ring in arrival order,
bounded by `capacity` (default 256).

- **Kept:** every entry, in order, up to `capacity`.
- **Lost under load:** only the **oldest** entries once the ring is full —
  never a middle entry, never ordering.
- **Use for:** event tapes — trade prints, alerts, audit trails. Size
  `capacity` to what the UI actually renders.

## Wire convention

The wire does not know about policies; two conventions make the policies
work:

- **`accumulate` fields** are **delta-encoded in updates** and **absolute in
  snapshots**. An update's `volume: 50` means "+50"; a snapshot's
  `volume: 10_000` means "the total is 10 000". After a resync the client
  takes the snapshot's absolute value and resumes adding deltas — which is
  why totals survive reconnects exactly.
- **`sequence` fields** carry **one entry object per update**; snapshots
  carry an **array of recent entries** (capped to your `capacity` on load).

## How backpressure sheds

One frame loop drains all channels, ordered by subscription **priority,
descending** (subscribe with `{ priority: n }`; default 0). After each
channel is applied, elapsed time is checked against the flush budget
(`flushBudgetMs`, default 8 ms). Once the budget is exceeded, remaining
channels **defer to the next frame** — which is requested immediately. A
deferred channel's pending data is not dropped: it keeps coalescing under
its field policies and applies whole when its turn comes.

The next frame resumes with the first deferred channel, so sustained work on a
high-priority channel cannot starve another channel. The budget is **soft**: one
channel flush is atomic and can exceed 8 ms; there is no hard per-frame cap.
Subscriber callbacks that enqueue updates leave that work for a subsequent frame.

Shedding is **never silent**. Watch two numbers in `getMetrics()`:

- **`deferredFrames`** — frames where the budget forced at least one channel
  to wait. Persistent growth means the budget is too small or a subscription
  is too heavy.
- **`coalesceRatio`** — `updatesIn / updatesApplied`. 1.0 means every update
  got its own write; 10 means ten updates merged per write. This is what
  coalescing bought you.

Practical consequences of the rules above:

- A `latest` field on a deferred channel skips more intermediates — but is
  still exactly current when it flushes.
- An `accumulate` field on a deferred channel loses **nothing**.
- A `sequence` field on a deferred channel loses only oldest-beyond-capacity
  entries.

## Choosing a policy: a market-data row

| Field    | Policy       | Why                                                                                                        |
| -------- | ------------ | ---------------------------------------------------------------------------------------------------------- |
| `last`   | `latest`     | Only the current price matters; missed intermediates are invisible and harmless.                            |
| `volume` | `accumulate` | A total. Dropping an intermediate would corrupt it forever; summing deltas is lossless at any frame rate.   |
| `trades` | `sequence`   | A tape. Users read individual prints in order; bound it to what the blotter shows (e.g. `capacity: 64`).    |

Rule of thumb: **state → `latest`**, **totals → `accumulate`**,
**events → `sequence`**. If losing an intermediate value would leave the UI
*wrong* (not merely less granular), the field is not `latest`.

## Resync semantics (gaps, reconnects)

When a sequence gap is detected or the socket reconnects, the channel
reconciles against a REST snapshot: pending coalesced data is discarded (the
snapshot supersedes it), the snapshot replaces the store wholesale — records
absent from the snapshot are removed — and live updates that arrived during
the fetch are replayed in seq order, skipping anything the snapshot already
contains. Gaps are visible via `onGap(...)` and `gapsDetected`;
reconciliation is visible as the `resyncing` connection state.


Snapshots are runtime-validated before replacing the store. Malformed wire frames
are rejected and counted in `messagesRejected`. Each fetch has a 10-second deadline
(including response parsing), an abort signal, and exponential retry capped by the
configured reconnect backoff. Failures increment `snapshotFailures`; the last valid
store remains available while the channel is resyncing.

The live-update buffer during resync is bounded by `resync.maxMessages` (10,000),
`maxBytes` (8 MiB of decoded JSON text, estimated as two bytes per character), and
`maxAgeMs` (10 seconds from the first buffered update). Crossing any limit clears
the incomplete recovery, reconnects, and starts a fresh snapshot; it never silently
drops a delta and then declares the channel live. `resyncRestarts` counts these
restarts. These limits bound the recovery buffer, not total process memory.

Each `subscribe` call owns an independent, idempotent `close()` handle. Closing one
handle twice cannot unsubscribe another consumer of the shared channel store.
