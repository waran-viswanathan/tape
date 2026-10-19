import { describe, expect, it } from 'vitest';
import { makeHarness, settle, snap, upd } from './helpers';

describe('untrusted protocol data', () => {
  it('rejects malformed known messages and continues processing valid updates', () => {
    const h = makeHarness();
    const sub = h.client.subscribe('q', { price: 'latest' }, { snapshot: false });
    h.client.connect(); const socket = h.sockets.latest(); socket.open();
    socket.push({ type: 'subscribed', channel: 'q', seq: 0 });
    for (const value of [
      { type: 'update', channel: 'q', seq: 1 },
      { type: 'update', channel: 'q', seq: -1, updates: [] },
      { type: 'update', channel: 'q', seq: 1, updates: [null] },
      { type: 'update', channel: 'q', seq: 1, updates: [{ id: 'A', ts: 0, fields: [] }] },
      { type: 'subscribed', channel: 'q', seq: 'bad' },
    ]) expect(() => socket.push(JSON.stringify(value))).not.toThrow();
    socket.push(upd('q', 1, [{ id: 'A', fields: { price: 42 } }])); h.frames.fire();
    expect(sub.store.get('A')!.fields['price']).toBe(42);
    expect(h.client.getMetrics().messagesRejected).toBe(5);
    h.client.close();
  });

  it.each([{ channel: 'wrong', seq: 2, serverTime: 0, records: [] }, { channel: 'q', seq: 2, serverTime: 0 }, { channel: 'q', seq: 2, serverTime: 0, records: [null] }])('does not clear good state for an invalid snapshot (%j)', async (invalid) => {
    let response: unknown = snap('q', 0, [{ id: 'A', fields: { price: 42 } }]);
    const h = makeHarness({ fetchFn: async () => ({ ok: true, status: 200, json: async () => response }) });
    const sub = h.client.subscribe('q', { price: 'latest' });
    h.client.connect(); h.sockets.latest().open(); await settle();
    response = invalid;
    h.sockets.latest().serverClose(); h.timers.advance(125); h.sockets.latest().open(); await settle();
    expect(sub.store.get('A')!.fields['price']).toBe(42);
    expect(h.client.getState()).toBe('resyncing');
    expect(h.client.getMetrics().snapshotFailures).toBe(1);
    response = snap('q', 3, [{ id: 'A', fields: { price: 43 } }]);
    h.timers.advance(250); await settle();
    expect(sub.store.get('A')!.fields['price']).toBe(43);
    h.client.close();
  });
});

describe('bounded snapshot recovery', () => {
  it('times out a stalled request, aborts it, retries, and cancels all work on close', async () => {
    const signals: AbortSignal[] = [];
    const h = makeHarness({ resync: { timeoutMs: 20 }, fetchFn: (_url, options) => {
      signals.push(options!.signal!); return new Promise(() => {});
    } });
    h.client.subscribe('q', {}); h.client.connect(); h.sockets.latest().open();
    h.timers.advance(20); await settle();
    expect(signals[0]!.aborted).toBe(true);
    expect(h.client.getMetrics().snapshotFailures).toBe(1);
    h.timers.advance(250); await settle(); expect(signals).toHaveLength(2);
    h.client.close(); await settle();
    expect(signals[1]!.aborted).toBe(true);
    expect(h.timers.pendingCount).toBe(0);
  });

  it('backs off repeated failures up to the configured cap', async () => {
    const h = makeHarness({ backoff: { baseMs: 10, capMs: 20 }, fetchFn: async () => { throw new Error('offline'); } });
    h.client.subscribe('q', {}); h.client.connect(); h.sockets.latest().open(); await settle();
    expect(h.timers.lastScheduledDelay()).toBe(10);
    h.timers.advance(10); await settle(); expect(h.timers.lastScheduledDelay()).toBe(20);
    h.timers.advance(20); await settle(); expect(h.timers.lastScheduledDelay()).toBe(20);
    h.client.close();
  });

  it.each([{ maxMessages: 1 }, { maxBytes: 1 }, { maxAgeMs: 10 }])('restarts an overflowing or old stream without replaying discarded deltas (%j)', async (resync) => {
    const h = makeHarness({ resync }); h.fetch.hold = true;
    const sub = h.client.subscribe('q', { volume: 'accumulate' });
    h.client.connect(); const socket = h.sockets.latest(); socket.open();
    socket.push(upd('q', 1, [{ id: 'A', fields: { volume: 4 } }]));
    socket.push(upd('q', 2, [{ id: 'A', fields: { volume: 6 } }]));
    if ('maxAgeMs' in resync) h.timers.advance(10);
    expect(socket.closedByClient).toBe(true);
    expect(h.client.getMetrics().resyncRestarts).toBe(1);
    // Late completion of the abandoned request must be ignored.
    h.fetch.queueSnapshot(snap('q', 0, [{ id: 'A', fields: { volume: 900 } }]));
    h.fetch.release(); await settle(); expect(sub.store.size()).toBe(0);
    h.timers.advance(125); h.sockets.latest().open();
    h.fetch.queueSnapshot(snap('q', 2, [{ id: 'A', fields: { volume: 10 } }]));
    h.fetch.release(); await settle(); h.frames.fire();
    expect(sub.store.get('A')!.fields['volume']).toBe(10);
    expect(h.client.getState()).toBe('live'); h.client.close();
  });
});

describe('overload fairness', () => {
  it('services all priorities even when high-priority work exceeds every frame budget', () => {
    let time = 0;
    const h = makeHarness({ now: () => time });
    const hi = h.client.subscribe('hi', { n: 'accumulate' }, { priority: 10, snapshot: false });
    const lo = h.client.subscribe('lo', { n: 'accumulate' }, { snapshot: false });
    hi.store.onFlush(() => { time += 9; });
    h.client.connect(); const socket = h.sockets.latest(); socket.open();
    for (const channel of ['hi', 'lo']) socket.push({ type: 'subscribed', channel, seq: 0 });
    socket.push(upd('lo', 1, [{ id: 'L', fields: { n: 7 } }]));
    for (let seq = 1; seq <= 3; seq++) {
      socket.push(upd('hi', seq, [{ id: 'H', fields: { n: 1 } }])); h.frames.fire();
    }
    expect(lo.store.get('L')!.fields['n']).toBe(7);
    expect(hi.store.get('H')!.fields['n']).toBe(3); h.client.close();
  });

  it('preserves updates ingested synchronously by a flush subscriber', () => {
    const h = makeHarness(); const sub = h.client.subscribe('q', { n: 'accumulate' }, { snapshot: false });
    h.client.connect(); const socket = h.sockets.latest(); socket.open();
    socket.push({ type: 'subscribed', channel: 'q', seq: 0 });
    let first = true;
    sub.store.onFlush(() => { if (first) { first = false; socket.push(upd('q', 2, [{ id: 'A', fields: { n: 7 } }])); } });
    socket.push(upd('q', 1, [{ id: 'A', fields: { n: 5 } }])); h.frames.fire(); h.frames.fire();
    expect(sub.store.get('A')!.fields['n']).toBe(12); h.client.close();
  });
});
