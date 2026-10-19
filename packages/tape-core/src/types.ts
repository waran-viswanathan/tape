/**
 * Public API surface of tape-core.
 *
 * Everything environment-specific — sockets, fetch, frame scheduling,
 * timers, clocks, randomness — enters through an injectable seam on
 * TapeClientOptions, with browser-appropriate defaults. tape-core itself
 * never touches React or the DOM and runs unmodified in Node.
 */

import type { PolicySpec } from './policy';
import type { FieldValue, SequenceEntry } from './protocol';

export type Unsubscribe = () => void;

/**
 * Connection lifecycle.
 *
 * - `idle`          created; connect() not yet called.
 * - `connecting`    an attempt is in flight, or a jittered backoff wait is
 *                   pending before the next attempt.
 * - `live`          socket open, every channel synced.
 * - `degraded`      socket nominally open but heartbeat-stale — no server
 *                   message within the staleness window. The transport
 *                   force-closes and reconnects; consumers should surface
 *                   this before the socket ever errors.
 * - `resyncing`     socket open; at least one channel is reconciling
 *                   against a REST snapshot (after connect, reconnect, or
 *                   a detected sequence gap).
 * - `disconnected`  close() was called; no auto-reconnect.
 */
export type ConnectionState =
  | 'idle'
  | 'connecting'
  | 'live'
  | 'degraded'
  | 'resyncing'
  | 'disconnected';

export interface GapEvent {
  channel: string;
  /** Seq we expected next. */
  expected: number;
  /** Seq that actually arrived. */
  received: number;
}

/** The client-side shape of a record's fields. */
export type Fields = Readonly<
  Record<string, FieldValue | readonly SequenceEntry[]>
>;

export interface TapeRecord<F extends Fields = Fields> {
  readonly id: string;
  readonly fields: F;
  /** Data timestamp of the last update applied to this record. */
  readonly ts: number;
}

/**
 * Store contract, designed for useSyncExternalStore-style consumers:
 * - `get(id)` returns a reference that is stable between flushes and
 *   replaced (copy-on-write) when the record changes.
 * - `ids()` returns a cached array replaced only when membership changes.
 * - Per-record subscribers are notified only when THEIR record changed in
 *   a flush — a tick on row 400 must not wake rows 1–399.
 */
export interface RecordStore<F extends Fields = Fields> {
  get(id: string): TapeRecord<F> | undefined;
  ids(): readonly string[];
  size(): number;
  subscribeRecord(id: string, cb: () => void): Unsubscribe;
  subscribeIds(cb: () => void): Unsubscribe;
  /** Fires once per flush with the set of changed record ids. */
  onFlush(cb: (changedIds: ReadonlySet<string>) => void): Unsubscribe;
}

export interface SubscribeOptions {
  /**
   * Higher priority starts a new drain first. Deferred channels resume
   * before already-served channels to prevent starvation. Default 0.
   */
  priority?: number;
  /** Reconcile against the REST snapshot on subscribe. Default true. */
  snapshot?: boolean;
}

export interface Subscription<F extends Fields = Fields> {
  readonly channel: string;
  readonly store: RecordStore<F>;
  /** Release this consumer once; subsequent calls have no effect. */
  close(): void;
}

export interface MetricsSnapshot {
  /** Server messages decoded (all types). */
  messagesIn: number;
  /** Malformed or unrecognized WebSocket messages skipped. */
  messagesRejected: number;
  /** Snapshot network, deadline, or validation failures. */
  snapshotFailures: number;
  /** Reconnects requested when a resync buffer reaches its limits. */
  resyncRestarts: number;
  /** RecordUpdates ingested into the coalescing buffer. */
  updatesIn: number;
  /** Record writes applied to stores at flush. */
  updatesApplied: number;
  /** Frames in which at least one channel flushed. */
  framesFlushed: number;
  /** updatesIn / updatesApplied — what coalescing bought. 1 = none. */
  coalesceRatio: number;
  /** Frames where the budget forced at least one channel to defer. */
  deferredFrames: number;
  /** p95 flush duration (ms) over a recent window. */
  p95FlushMs: number;
  gapsDetected: number;
  /** Out-of-order messages healed by the holdback buffer. */
  reordersHealed: number;
  /** Duplicate/stale messages dropped. */
  staleDropped: number;
  reconnects: number;
  snapshotsLoaded: number;
  /** Times heartbeat staleness demoted live → degraded. */
  staleTransitions: number;
}

// ---------------------------------------------------------------------------
// Injectable seams

export interface WebSocketLike {
  readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: ((ev?: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onclose: ((ev?: { code?: number; reason?: string }) => void) | null;
  onerror: ((ev?: unknown) => void) | null;
}

export type WebSocketFactory = (url: string) => WebSocketLike;

/** Frame-aligned scheduling (requestAnimationFrame in browsers). */
export interface FrameScheduler {
  request(cb: (now: number) => void): unknown;
  cancel(handle: unknown): void;
}

/** Delay-based scheduling (backoff waits, heartbeats, holdback timeouts). */
export interface TimerScheduler {
  schedule(cb: () => void, delayMs: number): unknown;
  cancel(handle: unknown): void;
}

export interface FetchResponseLike {
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
}

/** Injected implementations should honor the signal to release network resources. */
export type FetchLike = (url: string, options?: { signal?: AbortSignal }) => Promise<FetchResponseLike>;

// ---------------------------------------------------------------------------
// Client

export interface BackoffOptions {
  /** First retry delay ceiling (ms). Default 250. */
  baseMs?: number;
  /** Backoff ceiling (ms). Default 10_000. */
  capMs?: number;
}

export interface HeartbeatOptions {
  /** Ping cadence (ms). Default 1_000. */
  intervalMs?: number;
  /**
   * Staleness window (ms): if no server message of any kind arrives within
   * it, the connection is `degraded` and force-reconnected. Default 2_500.
   */
  timeoutMs?: number;
}

export interface ReorderOptions {
  /** Max out-of-order messages held back awaiting a missing seq. Default 16. */
  window?: number;
  /** Max time (ms) to hold before declaring a gap. Default 250. */
  timeoutMs?: number;
}

export interface TapeClientOptions {
  /** WebSocket url, e.g. ws://localhost:4400 */
  url: string;
  /** REST base for snapshots. Default: `url` with ws→http scheme swap. */
  snapshotUrl?: string;
  backoff?: BackoffOptions;
  heartbeat?: HeartbeatOptions;
  reorder?: ReorderOptions;
  /** Bounds while waiting for a snapshot or initial subscription ack. */
  resync?: {
    /** Deadline per snapshot attempt, including JSON decoding. Default 10_000 ms. */
    timeoutMs?: number;
    /** Maximum buffered update messages per channel. Default 10_000. */
    maxMessages?: number;
    /** Maximum buffered JSON text size (UTF-16 bytes), per channel. Default 8 MiB. */
    maxBytes?: number;
    /** Maximum age of the oldest buffered update. Default 10_000 ms. */
    maxAgeMs?: number;
  };
  /**
   * Per-frame flush budget (ms). When a flush exceeds it, remaining
   * channels defer and resume first next frame. A channel flush and its
   * subscriber callbacks are atomic, so this is a soft budget, not a
   * hard execution-time limit. Default 8.
   */
  flushBudgetMs?: number;
  // Seams — all optional, browser defaults.
  webSocketFactory?: WebSocketFactory;
  fetchFn?: FetchLike;
  scheduler?: FrameScheduler;
  timers?: TimerScheduler;
  now?: () => number;
  /** Source of jitter for backoff. Default Math.random. */
  random?: () => number;
}

export interface TapeClient {
  connect(): void;
  /** Tear down: closes the socket, cancels timers, stops reconnecting. */
  close(): void;
  getState(): ConnectionState;
  onStateChange(cb: (state: ConnectionState) => void): Unsubscribe;
  onGap(cb: (gap: GapEvent) => void): Unsubscribe;
  /**
   * Subscribe to a channel with a per-field coalescing policy. One live
   * store per channel: each consumer gets its own idempotent close handle;
   * the policy must match the existing channel or this throws.
   */
  subscribe<F extends Fields = Fields>(
    channel: string,
    policy: PolicySpec,
    opts?: SubscribeOptions,
  ): Subscription<F>;
  getMetrics(): MetricsSnapshot;
}

