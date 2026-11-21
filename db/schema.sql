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
  name      text NOT NULL
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
CREATE TABLE conflicts (
  id                     bigserial PRIMARY KEY,
  tenant_id              uuid NOT NULL REFERENCES tenants(id),
  unit_id                uuid NOT NULL REFERENCES units(id),
  kind                   text NOT NULL CHECK (kind IN ('booking_rejected', 'modification_rejected')),
  winner_reservation_id  uuid REFERENCES reservations(id),
  loser_reservation_id   uuid NOT NULL REFERENCES reservations(id),
  created_at             timestamptz NOT NULL DEFAULT now()
);

-- transactional outbox, written in the same txn as the change it describes
--   sync_calendar: push the listing's full blocked calendar
--   reservation_correction: set one reservation to a given state on its channel
CREATE TABLE outbox (
  id           bigserial PRIMARY KEY,
  tenant_id    uuid NOT NULL REFERENCES tenants(id),
  listing_id   uuid NOT NULL REFERENCES channel_listings(id),
  kind         text NOT NULL CHECK (kind IN ('sync_calendar', 'reservation_correction')),
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
  FOREACH t IN ARRAY ARRAY['units','channel_listings','reservations','conflicts','outbox','audit_log']
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I USING (tenant_id = current_setting(''app.tenant_id'', true)::uuid)', t);
  END LOOP;
END $$;
