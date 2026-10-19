/**
 * Plain-Node smoke: the full subscribe → snapshot → update → flush cycle
 * with injected fakes and zero browser globals involved.
 */
import { describe, expect, it } from 'vitest';
import { TAPE_DEFAULTS } from '../src/client';
import { makeHarness, settle, snap, upd } from './helpers';

describe('plain-Node smoke', () => {
  it('runs a full cycle without any browser global', async () => {
    // This suite runs in Node: no DOM, no window, no requestAnimationFrame.
    const g = globalThis as Record<string, unknown>;
    expect(typeof g['window']).toBe('undefined');
    expect(typeof g['document']).toBe('undefined');
    expect(typeof g['requestAnimationFrame']).toBe('undefined');

    const h = makeHarness();
    expect(h.client.getState()).toBe('idle');
    const sub = h.client.subscribe('quotes', {
      last: 'latest',
      volume: 'accumulate',
    });
    h.client.connect();
    expect(h.client.getState()).toBe('connecting');
    const s = h.sockets.latest();
    h.fetch.queueSnapshot(
      snap('quotes', 3, [{ id: 'AAPL', fields: { last: 190, volume: 1000 } }]),
    );
    s.open();
    expect(s.sentOfType('subscribe').map((f) => f.channel)).toEqual(['quotes']);
    expect(h.fetch.calls[0]).toBe(
      'http://feed.test:4400/snapshot?channel=quotes',
    ); // ws:// → http:// scheme swap
    await settle();
    expect(h.client.getState()).toBe('live');
    expect(sub.store.get('AAPL')!.fields['last']).toBe(190);

    s.push(upd('quotes', 4, [{ id: 'AAPL', fields: { last: 191, volume: 50 } }]));
    h.frames.fire();
    const rec = sub.store.get('AAPL')!;
    expect(rec.fields['last']).toBe(191);
    expect(rec.fields['volume']).toBe(1050);

    const m = h.client.getMetrics();
    expect(m.snapshotsLoaded).toBe(1);
    expect(m.messagesIn).toBeGreaterThanOrEqual(1);
    expect(m.updatesIn).toBe(1);
    expect(m.updatesApplied).toBe(1);

    h.client.close();
    expect(h.client.getState()).toBe('disconnected');
    expect(s.closedByClient).toBe(true);
    expect(h.timers.pendingCount).toBe(0); // no timer left behind
  });

  it('refcounts one live subscription per channel', async () => {
    const h = makeHarness();
    h.fetch.queueSnapshot(snap('quotes', 0, []));
    const a = h.client.subscribe('quotes', { last: 'latest' });
    // Deep-equal policy joins the same store with an independent close handle.
    const b = h.client.subscribe('quotes', { last: 'latest' });
    expect(b).not.toBe(a);
    expect(b.store).toBe(a.store);
    // A different policy is a programming error.
    expect(() => h.client.subscribe('quotes', { last: 'accumulate' })).toThrow();
    expect(() =>
      h.client.subscribe('quotes', { last: 'latest', extra: 'sequence' }),
    ).toThrow();

    h.client.connect();
    const s = h.sockets.latest();
    s.open();
    await settle();
    a.close(); // refcount 2 → 1: still live
    a.close(); // duplicate cleanup must not release b
    expect(s.sentOfType('unsubscribe').length).toBe(0);
    s.push(upd('quotes', 1, [{ id: 'AAPL', fields: { last: 42 } }]));
    h.frames.fire();
    expect(b.store.get('AAPL')!.fields['last']).toBe(42);
    b.close(); // refcount 1 → 0: unsubscribe and drop channel state
    expect(s.sentOfType('unsubscribe').map((f) => f.channel)).toEqual([
      'quotes',
    ]);
    // A fresh subscribe after teardown starts a new subscription.
    const c = h.client.subscribe('quotes', { vol: 'accumulate' });
    expect(c).not.toBe(a);
  });

  it('exports the documented defaults', () => {
    expect(TAPE_DEFAULTS).toEqual({
      backoff: { baseMs: 250, capMs: 10_000 },
      heartbeat: { intervalMs: 1_000, timeoutMs: 2_500 },
      reorder: { window: 16, timeoutMs: 250 },
      flushBudgetMs: 8,
      resync: { timeoutMs: 10_000, maxMessages: 10_000, maxBytes: 8 * 1024 * 1024, maxAgeMs: 10_000 },
    });
  });
});

