export type Channel = "airbnb" | "booking" | "vrbo";
export type Phase = "idle" | "starting" | "storm" | "converging" | "verified" | "failed";

export const CHANNELS: Channel[] = ["airbnb", "booking", "vrbo"];
export const CHANNEL_NAMES: Record<Channel, string> = {
  airbnb: "Airbnb",
  booking: "Booking.com",
  vrbo: "Vrbo",
};

// [owner in db, held by >1 guest across channels, channels whose calendar is stale]
export type Cell = [Channel | null, boolean, Channel[]];

export interface Sample {
  t: number;
  events: number;
  conflicts: number;
  queue: number;
  db_double: number;
  oversold_nights: number;
  down: Channel[];
}

export interface ChannelState {
  bookings_accepted: number;
  bookings_refused: number;
  webhooks_sent: number;
  webhooks_duplicated: number;
  webhooks_dropped: number;
  api_errors_injected: number;
  corrections_applied: number;
  down: boolean;
  inflight_webhooks: number;
  breaker: "closed" | "open" | "half_open";
  breaker_opens: number;
}

export interface Check {
  ok: boolean;
  [k: string]: number | boolean;
}

export interface ConflictRow {
  at: string;
  kind: "guest_relocated" | "booking_rejected" | "modification_rejected";
  unit: string;
  moved_to: string | null;
  winner: Channel | null;
  loser: Channel;
  check_in: string;
  check_out: string;
}

export interface InboxMessage {
  id: number;
  guest: string | null;
  channel: Channel;
  check_in: string;
  check_out: string;
  from_unit: string;
  to_unit: string;
  upgrade: boolean;
  guest_cancelled: boolean;
  body: string | null;
  drafted_by: string | null;
}

export interface Inbox {
  counts: Record<string, number>;
  messages: InboxMessage[];
}

export interface BoardRow {
  unit: string;
  room_type: string;
  cells: Cell[];
}

export interface Snapshot {
  phase: Phase;
  error?: string;
  params?: { actions: number; duration: number; seed: number; scheduled_outage: boolean };
  elapsed?: number;
  engine?: {
    double_bookings: number;
    events: Record<string, number>;
    reservations: Record<string, number>;
    conflicts: Record<string, number>;
    guest_messages: Record<string, number>;
    outbox: Record<string, number>;
    push_latency_ms: { p50: number; p95: number; p99: number };
    push_retries: number;
    ingest_deadlock_retries: number;
  };
  channels?: Record<Channel, ChannelState>;
  healed?: { missed_events_ingested: number; calendars_resynced: number; corrections_requeued: number };
  timeline?: Sample[];
  board?: BoardRow[];
  conflicts?: ConflictRow[];
  inbox?: Inbox;
  checks?: Record<string, Check> | null;
  base_date?: string;
}
