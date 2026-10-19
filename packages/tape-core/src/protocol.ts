/**
 * Wire protocol between a tape feed and the tape-core transport.
 *
 * Framing rules:
 * - Transport is JSON text frames over WebSocket, one message per frame.
 * - Every `update` message carries a per-channel `seq`, monotonically
 *   increasing by exactly 1 from the `seq` in the `subscribed` ack.
 *   Gap detection, reorder healing, and snapshot reconciliation key off it.
 * - A REST snapshot (`GET {base}/snapshot?channel=...`) returns the full
 *   record set for a channel together with the `seq` of the last update
 *   applied to it. A client that applies the snapshot and then replays only
 *   updates with `seq > snapshot.seq` converges on server state.
 *
 * Field semantics:
 * - Updates are plain field maps. How a field is merged client-side is not
 *   a wire concern — consumers declare a per-field coalescing policy at
 *   subscribe time (see policy.ts). One wire-level convention exists:
 *   fields consumed with the `accumulate` policy are delta-encoded in
 *   updates and absolute in snapshots.
 * - Fields consumed with the `sequence` policy carry one entry object per
 *   update; snapshots carry an array of recent entries.
 */

export type FieldValue = number | string | boolean | null;

/** One entry of a sequence-policy field (e.g. a trade on a tape). */
export type SequenceEntry = Readonly<Record<string, FieldValue>>;

export type WireFieldValue = FieldValue | SequenceEntry;

/** One record change inside an `update` message. */
export interface RecordUpdate {
  id: string;
  fields: Readonly<Record<string, WireFieldValue>>;
  /** Data timestamp (feed clock, epoch ms). */
  ts: number;
}

/** One record in a snapshot. Sequence fields appear as entry arrays. */
export interface SnapshotRecord {
  id: string;
  fields: Readonly<Record<string, FieldValue | readonly SequenceEntry[]>>;
  ts: number;
}

/** Response body of `GET {base}/snapshot?channel=...`. */
export interface SnapshotResponse {
  channel: string;
  /** Seq of the last update applied to this snapshot. */
  seq: number;
  serverTime: number;
  records: SnapshotRecord[];
}

export const SNAPSHOT_PATH = '/snapshot';

// ---------------------------------------------------------------------------
// Server → client

export interface HelloMessage {
  type: 'hello';
  serverTime: number;
  channels: string[];
}

export interface SubscribedMessage {
  type: 'subscribed';
  channel: string;
  /** Seq of the last update sent before this ack; updates resume at seq + 1. */
  seq: number;
}

export interface UnsubscribedMessage {
  type: 'unsubscribed';
  channel: string;
}

export interface UpdateMessage {
  type: 'update';
  channel: string;
  seq: number;
  updates: RecordUpdate[];
}

export interface PongMessage {
  type: 'pong';
  /** Echo of the ping's `t`. */
  t: number;
  serverTime: number;
}

export interface ErrorMessage {
  type: 'error';
  code: string;
  message: string;
}

export type ServerMessage =
  | HelloMessage
  | SubscribedMessage
  | UnsubscribedMessage
  | UpdateMessage
  | PongMessage
  | ErrorMessage;

// ---------------------------------------------------------------------------
// Client → server

export interface SubscribeMessage {
  type: 'subscribe';
  channel: string;
}

export interface UnsubscribeMessage {
  type: 'unsubscribe';
  channel: string;
}

export interface PingMessage {
  type: 'ping';
  t: number;
}

export type ClientMessage = SubscribeMessage | UnsubscribeMessage | PingMessage;

// ---------------------------------------------------------------------------
// Codec. Decoding is lenient on unknown message types (forward
// compatibility): decode returns null for anything unrecognized; callers
// count and skip rather than crash.

const SERVER_TYPES: ReadonlySet<string> = new Set([
  'hello',
  'subscribed',
  'unsubscribed',
  'update',
  'pong',
  'error',
]);

const CLIENT_TYPES: ReadonlySet<string> = new Set([
  'subscribe',
  'unsubscribe',
  'ping',
]);

export function encodeMessage(msg: ServerMessage | ClientMessage): string {
  return JSON.stringify(msg);
}

function decode(data: unknown, types: ReadonlySet<string>): unknown {
  if (typeof data !== 'string') return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    return null;
  }
  if (parsed === null || typeof parsed !== 'object') return null;
  const type = (parsed as { type?: unknown }).type;
  if (typeof type !== 'string' || !types.has(type)) return null;
  return parsed;
}

export function decodeServerMessage(data: unknown): ServerMessage | null {
  const msg = decode(data, SERVER_TYPES);
  if (!object(msg)) return null;
  switch (msg['type']) {
    case 'hello': return finite(msg['serverTime']) && Array.isArray(msg['channels']) && msg['channels'].every(text) ? msg as unknown as HelloMessage : null;
    case 'subscribed': return text(msg['channel']) && sequence(msg['seq']) ? msg as unknown as SubscribedMessage : null;
    case 'unsubscribed': return text(msg['channel']) ? msg as unknown as UnsubscribedMessage : null;
    case 'update': return text(msg['channel']) && sequence(msg['seq']) && Array.isArray(msg['updates']) && msg['updates'].every((row: unknown) => record(row, false)) ? msg as unknown as UpdateMessage : null;
    case 'pong': return finite(msg['t']) && finite(msg['serverTime']) ? msg as unknown as PongMessage : null;
    case 'error': return typeof msg['code'] === 'string' && typeof msg['message'] === 'string' ? msg as unknown as ErrorMessage : null;
    default: return null;
  }
}

export function decodeClientMessage(data: unknown): ClientMessage | null {
  const msg = decode(data, CLIENT_TYPES);
  if (!object(msg)) return null;
  if (msg['type'] === 'ping') return finite(msg['t']) ? msg as unknown as PingMessage : null;
  return text(msg['channel']) ? msg as unknown as SubscribeMessage | UnsubscribeMessage : null;
}

const object = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);
const text = (value: unknown): value is string => typeof value === 'string' && value.length > 0;
const finite = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value);
const sequence = (value: unknown): value is number => finite(value) && Number.isSafeInteger(value) && value >= 0;
const scalar = (value: unknown): boolean => value === null || typeof value === 'string' || typeof value === 'boolean' || finite(value);
const entry = (value: unknown): boolean => object(value) && Object.values(value).every(scalar);
function record(value: unknown, snapshot: boolean): boolean {
  if (!object(value) || !text(value['id']) || !finite(value['ts']) || !object(value['fields'])) return false;
  return Object.values(value['fields']).every(field => scalar(field) || (snapshot ? Array.isArray(field) && field.every(entry) : entry(field)));
}

/** Validate the full snapshot before clearing or changing any stored records. */
export function isSnapshotResponse(value: unknown, channel: string): value is SnapshotResponse {
  return object(value) && value['channel'] === channel && sequence(value['seq']) && finite(value['serverTime'])
    && Array.isArray(value['records']) && value['records'].every((row: unknown) => record(row, true));
}

