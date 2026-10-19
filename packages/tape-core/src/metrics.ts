/**
 * Internal metrics collection. Counters are bumped inline on the hot path
 * (plain property increments, no allocation); anything that costs — the p95
 * over the flush-duration ring — is computed lazily inside snapshot(), which
 * is only ever called from getMetrics().
 */

import type { MetricsSnapshot } from './types';

/** Flush durations retained for the p95 window. */
const FLUSH_RING_SIZE = 512;

export class Metrics {
  messagesIn = 0;
  messagesRejected = 0;
  snapshotFailures = 0;
  resyncRestarts = 0;
  updatesIn = 0;
  updatesApplied = 0;
  framesFlushed = 0;
  deferredFrames = 0;
  gapsDetected = 0;
  reordersHealed = 0;
  staleDropped = 0;
  reconnects = 0;
  snapshotsLoaded = 0;
  staleTransitions = 0;

  private readonly flushDurations = new Float64Array(FLUSH_RING_SIZE);
  private flushCount = 0;
  private flushIndex = 0;

  /** O(1), allocation-free: called once per flushed frame. */
  recordFlush(durationMs: number): void {
    this.flushDurations[this.flushIndex] = durationMs;
    this.flushIndex = (this.flushIndex + 1) % FLUSH_RING_SIZE;
    if (this.flushCount < FLUSH_RING_SIZE) this.flushCount++;
  }

  /** Returns a plain object; sorting for p95 happens here, off the hot path. */
  snapshot(): MetricsSnapshot {
    let p95FlushMs = 0;
    if (this.flushCount > 0) {
      const window = Array.from(
        this.flushDurations.subarray(0, this.flushCount),
      );
      window.sort((a, b) => a - b);
      const rank = Math.min(
        window.length - 1,
        Math.max(0, Math.ceil(window.length * 0.95) - 1),
      );
      p95FlushMs = window[rank] ?? 0;
    }
    return {
      messagesIn: this.messagesIn,
      messagesRejected: this.messagesRejected,
      snapshotFailures: this.snapshotFailures,
      resyncRestarts: this.resyncRestarts,
      updatesIn: this.updatesIn,
      updatesApplied: this.updatesApplied,
      framesFlushed: this.framesFlushed,
      coalesceRatio:
        this.updatesApplied > 0 ? this.updatesIn / this.updatesApplied : 1,
      deferredFrames: this.deferredFrames,
      p95FlushMs,
      gapsDetected: this.gapsDetected,
      reordersHealed: this.reordersHealed,
      staleDropped: this.staleDropped,
      reconnects: this.reconnects,
      snapshotsLoaded: this.snapshotsLoaded,
      staleTransitions: this.staleTransitions,
    };
  }
}

