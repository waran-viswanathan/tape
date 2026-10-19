/**
 * Socket lifecycle + connection state machine over the injected
 * WebSocketFactory.
 *
 * State semantics (see ConnectionState in types.ts):
 * - 'connecting'   an attempt is in flight OR a jittered backoff wait is
 *                  pending before the next attempt.
 * - 'live'         socket open, no channel resyncing, heartbeat fresh.
 * - 'degraded'     socket nominally open but heartbeat-stale. The transport
 *                  force-closes the socket and enters the reconnect path —
 *                  silent-death sockets are the production failure this
 *                  exists for.
 * - 'resyncing'    socket open and at least one channel reconciling a
 *                  snapshot (the owner reports the count via setResyncing).
 * - 'disconnected' only after close(); no auto-reconnect.
 *
 * Reconnect backoff is FULL JITTER:
 *   delay = random() * min(capMs, baseMs * 2^attempt)
 * `attempt` resets to 0 on every successful open.
 *
 * All timing goes through the injected TimerScheduler and now(); the
 * heartbeat is a self-rescheduling one-shot timer, never setInterval.
 */

import { decodeServerMessage, encodeMessage } from './protocol';
import type { ClientMessage, ServerMessage } from './protocol';
import type {
  ConnectionState,
  TimerScheduler,
  Unsubscribe,
  WebSocketFactory,
  WebSocketLike,
} from './types';
import type { Metrics } from './metrics';

const SOCKET_OPEN = 1;

export interface TransportOptions {
  url: string;
  webSocketFactory: WebSocketFactory;
  timers: TimerScheduler;
  now: () => number;
  random: () => number;
  backoff: { baseMs: number; capMs: number };
  heartbeat: { intervalMs: number; timeoutMs: number };
  metrics: Metrics;
  /** Socket opened (initial or reconnect). Resubscribe + resync here. */
  onOpen: () => void;
  onMessage: (msg: ServerMessage, wireBytes: number) => void;
  /** Socket lost (any reason other than close()). */
  onDown: () => void;
}

type Phase = 'idle' | 'connecting' | 'open' | 'closed';

export class Transport {
  private socket: WebSocketLike | null = null;
  /** Bumped whenever the current socket is abandoned; stale events ignored. */
  private generation = 0;
  private phase: Phase = 'idle';
  private state: ConnectionState = 'idle';
  private readonly stateSubs = new Set<(state: ConnectionState) => void>();
  private attempt = 0;
  private reconnectTimer: unknown = null;
  private heartbeatTimer: unknown = null;
  private lastMessageAt = 0;
  private resyncCount = 0;
  private stale = false;
  private everOpened = false;

  constructor(private readonly opts: TransportOptions) {}

  connect(): void {
    if (this.phase !== 'idle' && this.phase !== 'closed') return;
    this.openSocket();
  }

  /** User teardown: cancels all timers, closes the socket, no reconnect. */
  close(): void {
    this.generation++;
    this.stopHeartbeat();
    if (this.reconnectTimer !== null) {
      this.opts.timers.cancel(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    const socket = this.socket;
    this.socket = null;
    this.phase = 'closed';
    if (socket !== null) {
      try {
        socket.close();
      } catch {
        // Socket may already be dead; teardown must not throw.
      }
    }
    this.setState('disconnected');
  }

  isOpen(): boolean {
    return this.socket !== null && this.socket.readyState === SOCKET_OPEN;
  }

  /** Abandon a stream that cannot be reconciled within the owner's limits. */
  restart(): void {
    if (this.phase === 'open') this.forceReconnect();
  }

  send(msg: ClientMessage): boolean {
    const socket = this.socket;
    if (socket === null || socket.readyState !== SOCKET_OPEN) return false;
    socket.send(encodeMessage(msg));
    return true;
  }

  getState(): ConnectionState {
    return this.state;
  }

  onStateChange(cb: (state: ConnectionState) => void): Unsubscribe {
    this.stateSubs.add(cb);
    return () => this.stateSubs.delete(cb);
  }

  /** Owner reports how many channels are reconciling a snapshot. */
  setResyncing(count: number): void {
    this.resyncCount = count;
    if (this.phase === 'open') this.recompute();
  }

  private openSocket(): void {
    const gen = ++this.generation;
    this.phase = 'connecting';
    this.setState('connecting');
    const socket = this.opts.webSocketFactory(this.opts.url);
    this.socket = socket;
    socket.onopen = () => {
      if (gen === this.generation) this.handleOpen();
    };
    socket.onmessage = (ev) => {
      if (gen === this.generation) this.handleMessage(ev.data);
    };
    socket.onclose = () => {
      if (gen === this.generation) this.handleDown();
    };
    socket.onerror = () => {
      // An error is always followed by close; handleDown covers it.
    };
  }

  private handleOpen(): void {
    this.attempt = 0; // successful open resets backoff
    this.stale = false;
    if (this.everOpened) this.opts.metrics.reconnects++;
    this.everOpened = true;
    this.phase = 'open';
    this.lastMessageAt = this.opts.now();
    this.startHeartbeat();
    this.opts.onOpen();
    this.recompute();
  }

  private handleMessage(data: unknown): void {
    // Any traffic proves the socket is alive, decodable or not.
    this.lastMessageAt = this.opts.now();
    const msg = decodeServerMessage(data);
    if (msg === null) { this.opts.metrics.messagesRejected++; return; }
    this.opts.metrics.messagesIn++;
    this.opts.onMessage(msg, typeof data === 'string' ? data.length * 2 : 0);
  }

  private handleDown(): void {
    this.stopHeartbeat();
    this.socket = null;
    this.generation++;
    this.phase = 'connecting';
    this.opts.onDown();
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    this.setState('connecting');
    const { baseMs, capMs } = this.opts.backoff;
    // FULL JITTER: delay = random() * min(cap, base * 2^attempt)
    const delay = this.opts.random() * Math.min(capMs, baseMs * 2 ** this.attempt);
    this.attempt++;
    this.reconnectTimer = this.opts.timers.schedule(() => {
      this.reconnectTimer = null;
      this.openSocket();
    }, delay);
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = this.opts.timers.schedule(
      this.heartbeatTick,
      this.opts.heartbeat.intervalMs,
    );
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== null) {
      this.opts.timers.cancel(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private readonly heartbeatTick = (): void => {
    this.heartbeatTimer = null;
    if (this.phase !== 'open') return;
    const t = this.opts.now();
    if (t - this.lastMessageAt >= this.opts.heartbeat.timeoutMs) {
      // Silent death: the socket looks open but nothing has arrived for the
      // whole staleness window. Surface it, then force the reconnect path.
      this.opts.metrics.staleTransitions++;
      this.stale = true;
      this.setState('degraded');
      this.forceReconnect();
      return;
    }
    this.send({ type: 'ping', t });
    this.heartbeatTimer = this.opts.timers.schedule(
      this.heartbeatTick,
      this.opts.heartbeat.intervalMs,
    );
  };

  private forceReconnect(): void {
    const socket = this.socket;
    this.socket = null;
    this.generation++; // silence any late events from the dead socket
    this.stopHeartbeat();
    if (socket !== null) {
      try {
        socket.close();
      } catch {
        // A silently-dead socket may throw on close; proceed regardless.
      }
    }
    this.phase = 'connecting';
    this.opts.onDown();
    this.scheduleReconnect();
  }

  private recompute(): void {
    if (this.phase !== 'open') return;
    this.setState(
      this.stale ? 'degraded' : this.resyncCount > 0 ? 'resyncing' : 'live',
    );
  }

  private setState(next: ConnectionState): void {
    if (next === this.state) return;
    this.state = next;
    for (const cb of this.stateSubs) cb(next);
  }
}

