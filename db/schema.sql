-- postgres 15+ (supabase compatible)
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tenants (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name       text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE units (
  id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  name      text NOT NULL,
  room_type text NOT NULL DEFAULT 'standard',
  bedrooms  integer NOT NULL DEFAULT 1
);

-- a unit's listing on one channel
CREATE TABLE channel_listings (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id           uuid NOT NULL REFERENCES tenants(id),
  unit_id             uuid NOT NULL REFERENCES units(id),
  channel             text NOT NULL,
  external_listing_id text NOT NULL,
  UNIQUE (channel, external_listing_id),
  UNIQUE (unit_id, channel)
);

-- unit_id is the unit the guest actually sleeps in. Usually that's the listing's unit, but a
-- guest who lost a clash can be moved to another unit and keep their channel booking.
CREATE TABLE reservations (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id       uuid NOT NULL REFERENCES tenants(id),
  unit_id         uuid NOT NULL REFERENCES units(id),
  listing_id      uuid NOT NULL REFERENCES channel_listings(id),
  channel         text NOT NULL,
  external_id     text NOT NULL,
  stay            daterange NOT NULL CHECK (NOT isempty(stay)),
  status          text NOT NULL CHECK (status IN ('confirmed', 'cancelled', 'rejected')),
  channel_version integer NOT NULL,
  guest_name      text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (channel, external_id),
  -- the whole point of this project: two confirmed stays on one unit can't overlap
  CONSTRAINT no_double_booking
    EXCLUDE USING gist (unit_id WITH =, stay WITH &&) WHERE (status = 'confirmed')
);
CREATE INDEX reservations_unit_confirmed ON reservations (unit_id) WHERE status = 'confirmed';

-- dedupe for channel webhooks
CREATE TABLE inbound_events (
  channel           text NOT NULL,
  external_event_id text NOT NULL,
  payload           jsonb NOT NULL,
  outcome           text,
  received_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (channel, external_event_id)
);

-- loser = whichever booking committed second
--   guest_relocated:       loser moved to moved_to_unit_id, keeps their booking
--   booking_rejected:      nothing comparable was free, loser cancelled on its channel
--   modification_rejected: date change onto sold nights, old dates kept
CREATE TABLE conflicts (
  id                     bigserial PRIMARY KEY,
  tenant_id              uuid NOT NULL REFERENCES tenants(id),
  unit_id                uuid NOT NULL REFERENCES units(id),
  kind                   text NOT NULL
                         CHECK (kind IN ('guest_relocated', 'booking_rejected', 'modification_rejected')),
  winner_reservation_id  uuid REFERENCES reservations(id),
  loser_reservation_id   uuid NOT NULL REFERENCES reservations(id),
  moved_to_unit_id       uuid REFERENCES units(id),
  created_at             timestamptz NOT NULL DEFAULT now(),
  CHECK ((kind = 'guest_relocated') = (moved_to_unit_id IS NOT NULL))
);

-- What we tell a moved guest. Drafted outside the ingest txn, sent only once an operator
-- approves it.
CREATE TABLE guest_messages (
  id             bigserial PRIMARY KEY,
  tenant_id      uuid NOT NULL REFERENCES tenants(id),
  conflict_id    bigint NOT NULL REFERENCES conflicts(id),
  reservation_id uuid NOT NULL REFERENCES reservations(id),
  status         text NOT NULL DEFAULT 'drafting'
                 CHECK (status IN ('drafting', 'awaiting_approval', 'approved', 'sent', 'discarded')),
  body           text,
  drafted_by     text,
  locked_until   timestamptz,
  created_at     timestamptz NOT NULL DEFAULT now(),
  decided_at     timestamptz,
  sent_at        timestamptz
);
CREATE INDEX guest_messages_open ON guest_messages (status) WHERE status IN ('drafting', 'awaiting_approval');

-- transactional outbox, written in the same txn as the change it describes
--   sync_calendar: push the listing's full blocked calendar
--   reservation_correction: set one reservation to a given state on its channel
--   guest_message: an approved message to a guest, sent through the channel
CREATE TABLE outbox (
  id           bigserial PRIMARY KEY,
  tenant_id    uuid NOT NULL REFERENCES tenants(id),
  listing_id   uuid NOT NULL REFERENCES channel_listings(id),
  kind         text NOT NULL CHECK (kind IN ('sync_calendar', 'reservation_correction', 'guest_message')),
  payload      jsonb NOT NULL DEFAULT '{}'::jsonb,
  status       text NOT NULL DEFAULT 'pending'
               CHECK (status IN ('pending', 'inflight', 'done', 'superseded', 'dead')),
  attempts     integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_until timestamptz,
  last_error   text,
  created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
  processed_at timestamptz
);
-- one pending calendar sync per listing, so a burst of changes becomes one push
CREATE UNIQUE INDEX outbox_one_pending_sync
  ON outbox (listing_id) WHERE status = 'pending' AND kind = 'sync_calendar';
CREATE INDEX outbox_claimable ON outbox (available_at) WHERE status IN ('pending', 'inflight');

CREATE TABLE audit_log (
  id        bigserial PRIMARY KEY,
  tenant_id uuid NOT NULL,
  entity    text NOT NULL,
  entity_id uuid,
  action    text NOT NULL,
  detail    jsonb NOT NULL DEFAULT '{}'::jsonb,
  at        timestamptz NOT NULL DEFAULT now()
);

-- RLS per tenant. The engine runs as table owner so it bypasses this; operator-facing
-- roles set app.tenant_id per transaction.
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['units','channel_listings','reservations','conflicts','guest_messages','outbox','audit_log']
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I USING (tenant_id = current_setting(''app.tenant_id'', true)::uuid)', t);
  END LOOP;
END $$;
