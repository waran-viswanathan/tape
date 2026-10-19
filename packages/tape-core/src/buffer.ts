/**
 * The coalescing buffer + flush scheduler.
 *
 * Ingest NEVER touches subscribers: updates land in a per-channel pending
 * map keyed by record id, merged per field by the channel's compiled policy:
 * - latest      overwrites the pending value
 * - accumulate  numerically sums into the pending delta
 * - sequence    appends the entry, evicting the oldest beyond capacity
 *
 * ONE frame loop drains all channels in subscription-priority order
 * (descending). After each channel's pending map is applied to its store,
 * elapsed time is measured with the injected now(); once it exceeds
 * flushBudgetMs and channels with pending work remain, the rest defer to the
 * next frame, starting at the first deferred channel to prevent starvation.
 * The budget is soft: a channel flush is atomic. Pending keeps coalescing —
 * deferredFrames counts once for the frame, and another frame is requested
 * immediately. Frames are only requested while pending work exists: an idle
 * client schedules nothing.
 *
 * Hot-path discipline: no sorting (the channel list is re-sorted only when
 * subscriptions change) and no array spreads inside ingest; per-field pending
 * cells are created once per record per frame and mutated in place.
 */

import type { FieldValue, RecordUpdate, SequenceEntry } from './protocol';
import type { CompiledPolicy, FieldPolicy } from './policy';
import type { FrameScheduler } from './types';
import type { Metrics } from './metrics';
import type { TapeStore } from './store';

/** Pending per-field coalesce state. Exactly one branch is used per policy. */
export interface PendingField {
  policy: FieldPolicy;
  capacity: number;
  /** latest: the newest value wins. */
  value: FieldValue;
  /** accumulate: deltas summed since the last flush. */
  sum: number;
  /** sequence: entries in arrival order, capped at capacity. */
  entries: SequenceEntry[] | null;
}

export interface PendingRecord {
  /** Data timestamp of the newest update merged into this pending entry. */
  ts: number;
  fields: Map<string, PendingField>;
}

interface BufferChannel {
  name: string;
  store: TapeStore;
  compiled: CompiledPolicy;
  priority: number;
  pending: Map<string, PendingRecord>;
}

export interface BufferOptions {
  scheduler: FrameScheduler;
  now: () => number;
  flushBudgetMs: number;
  metrics: Metrics;
}

export class CoalescingBuffer {
  private readonly channels = new Map<string, BufferChannel>();
  /** Priority-descending; rebuilt only when subscriptions change. */
  private sorted: BufferChannel[] = [];
  private frameHandle: unknown = null;
  private resumeAt: string | null = null;

  constructor(private readonly opts: BufferOptions) {}

  addChannel(
    name: string,
    store: TapeStore,
    compiled: CompiledPolicy,
    priority: number,
  ): void {
    this.channels.set(name, {
      name,
      store,
      compiled,
      priority,
      pending: new Map(),
    });
    this.resort();
  }

  removeChannel(name: string): void {
    this.channels.delete(name);
    this.resort();
  }

  /**
   * Drop a channel's pending coalesce state. Called when a snapshot is about
   * to apply: the snapshot supersedes everything staged before it.
   */
  clearPending(name: string): void {
    this.channels.get(name)?.pending.clear();
  }

  dispose(): void {
    if (this.frameHandle !== null) {
      this.opts.scheduler.cancel(this.frameHandle);
      this.frameHandle = null;
    }
    this.channels.clear();
    this.sorted = [];
    this.resumeAt = null;
  }

  ingest(channelName: string, updates: readonly RecordUpdate[]): void {
    const ch = this.channels.get(channelName);
    if (ch === undefined) return;
    const metrics = this.opts.metrics;
    for (const update of updates) {
      metrics.updatesIn++;
      let pend = ch.pending.get(update.id);
      if (pend === undefined) {
        pend = { ts: update.ts, fields: new Map() };
        ch.pending.set(update.id, pend);
      } else {
        pend.ts = update.ts;
      }
      const fields = update.fields;
      for (const key in fields) {
        const value = fields[key];
        if (value === undefined) continue;
        const resolved = ch.compiled.for(key);
        let cell = pend.fields.get(key);
        if (cell === undefined) {
          cell = {
            policy: resolved.policy,
            capacity: resolved.capacity,
            value: null,
            sum: 0,
            entries: null,
          };
          pend.fields.set(key, cell);
        }
        if (resolved.policy === 'latest') {
          cell.value = value as FieldValue;
        } else if (resolved.policy === 'accumulate') {
          cell.sum += typeof value === 'number' ? value : 0;
        } else {
          if (cell.entries === null) cell.entries = [];
          cell.entries.push(value as SequenceEntry);
          if (cell.entries.length > resolved.capacity) cell.entries.shift();
        }
      }
    }
    if (ch.pending.size > 0) this.scheduleFrame();
  }

  private scheduleFrame(): void {
    if (this.frameHandle === null) {
      this.frameHandle = this.opts.scheduler.request(this.onFrame);
    }
  }

  private readonly onFrame = (): void => {
    this.frameHandle = null;
    const { now, metrics, flushBudgetMs } = this.opts;
    const start = now();
    const sorted = this.sorted;
    const resumeIndex = this.resumeAt === null ? -1 : sorted.findIndex(ch => ch.name === this.resumeAt);
    const first = Math.max(0, resumeIndex);
    this.resumeAt = null;
    let flushed = false;
    let deferred = false;
    for (let i = 0; i < sorted.length; i++) {
      const ch = sorted[(first + i) % sorted.length]!;
      if (ch.pending.size === 0) continue;
      // Detach the batch before callbacks run: reentrant ingestion belongs
      // to the next batch and must not be erased after notification.
      const pending = ch.pending;
      ch.pending = new Map();
      const written = ch.store.flushPending(pending);
      metrics.updatesApplied += written;
      flushed = true;
      const elapsed = now() - start;
      if (elapsed > flushBudgetMs) {
        for (let j = i + 1; j < sorted.length; j++) {
          const next = sorted[(first + j) % sorted.length]!;
          if (next.pending.size > 0) {
            deferred = true;
            this.resumeAt = next.name;
            break;
          }
        }
        if (deferred) break;
      }
    }
    if (flushed) {
      metrics.framesFlushed++;
      metrics.recordFlush(now() - start);
    }
    if (deferred) {
      metrics.deferredFrames++; // once per frame, however many channels defer
      this.scheduleFrame();
      return;
    }
    // Subscriber callbacks run during flush may have ingested more work.
    for (const ch of sorted) {
      if (ch.pending.size > 0) {
        this.scheduleFrame();
        return;
      }
    }
  };

  private resort(): void {
    this.sorted = Array.from(this.channels.values()).sort(
      (a, b) => b.priority - a.priority,
    );
  }
}

