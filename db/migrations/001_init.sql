-- ---------------------------------------------------------------------------
-- 001_init: inventory, reservations, and the overlap guard.
--
-- The correctness argument of this service lives in this file. Everything in
-- src/ is an attempt to *use* these constraints efficiently; nothing in src/ is
-- trusted to enforce them. If the application layer is buggy, malicious, or
-- simply raced, the database still refuses to double-book.
-- ---------------------------------------------------------------------------

-- gist indexes natively understand range overlap (&&) but not scalar equality
-- (=). btree_gist adds btree operator classes to gist so a single index can mix
-- `unit_id WITH =` and `period WITH &&` in one EXCLUDE constraint.
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- Inventory
-- ---------------------------------------------------------------------------

-- A class of interchangeable bookable things: "Deluxe Garden Suite".
-- Callers book a *resource*; the allocator picks a unit.
CREATE TABLE resources (
   id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
   slug        text NOT NULL UNIQUE,
   name        text NOT NULL,
   created_at  timestamptz NOT NULL DEFAULT now()
);

-- One physically bookable item: room 204. Capacity is modelled as row count,
-- not as an integer column, because an integer counter cannot express *which*
-- nights are taken and cannot be defended by an exclusion constraint.
CREATE TABLE resource_units (
   id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
   resource_id  uuid NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
   label        text NOT NULL,
   created_at   timestamptz NOT NULL DEFAULT now(),
   UNIQUE (resource_id, label)
);

CREATE INDEX resource_units_resource_idx ON resource_units (resource_id);

-- ---------------------------------------------------------------------------
-- Reservations
-- ---------------------------------------------------------------------------

CREATE TYPE reservation_state AS ENUM ('held', 'confirmed', 'cancelled', 'expired');

CREATE TABLE reservations (
   id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
   unit_id          uuid NOT NULL REFERENCES resource_units(id) ON DELETE RESTRICT,
   resource_id      uuid NOT NULL REFERENCES resources(id) ON DELETE RESTRICT,
   guest_ref        text NOT NULL,
   state            reservation_state NOT NULL,

   -- Half-open [check-in, check-out). Half-open is what makes back-to-back
   -- bookings legal: a stay ending 09-05 and one starting 09-05 do not overlap,
   -- which is the real-world rule and the one `&&` implements for '[)' ranges.
   period           tstzrange NOT NULL,

   -- Only meaningful while state = 'held'. NULL once the hold resolves.
   hold_expires_at  timestamptz,

   -- Optimistic lock. Every state transition asserts the version it read and
   -- bumps it, so two concurrent confirm/cancel calls cannot both win.
   version          integer NOT NULL DEFAULT 1,

   created_at       timestamptz NOT NULL DEFAULT now(),
   updated_at       timestamptz NOT NULL DEFAULT now(),

   -- A zero-width or unbounded stay is meaningless and would also break the
   -- overlap semantics (empty ranges overlap nothing, so they would slip past
   -- the exclusion constraint entirely).
   CONSTRAINT reservations_period_nonempty CHECK (NOT isempty(period)),
   CONSTRAINT reservations_period_bounded  CHECK (lower(period) IS NOT NULL
                                              AND upper(period) IS NOT NULL),

   -- A hold without a deadline never releases its inventory.
   CONSTRAINT reservations_hold_has_deadline CHECK (
      (state = 'held' AND hold_expires_at IS NOT NULL)
      OR (state <> 'held')
   )
);

-- ===========================================================================
-- THE GUARD.
--
-- Reject any INSERT/UPDATE that would leave two *inventory-occupying* rows on
-- the same unit with overlapping periods. Postgres evaluates this inside the
-- index, holding a lock on the candidate key range, so two concurrent
-- transactions inserting the same night cannot both commit: the second blocks
-- until the first commits and is then rejected with SQLSTATE 23P01.
--
-- The WHERE clause is what makes cancellation and expiry work. Only 'held' and
-- 'confirmed' rows occupy inventory; cancelled and expired rows fall out of the
-- index and stop blocking, while remaining on the table as history.
--
-- The predicate deliberately does NOT mention hold_expires_at or now(). Index
-- predicates must be IMMUTABLE, and a row cannot silently leave an index as the
-- clock advances. Expiry is therefore *materialised* by flipping state to
-- 'expired' -- done eagerly inside the allocation transaction (see
-- src/repo/reservations.ts) and lazily by the background reaper. That is a
-- design consequence of this line, not an oversight.
-- ===========================================================================
ALTER TABLE reservations
   ADD CONSTRAINT reservations_no_overlap
   EXCLUDE USING gist (unit_id WITH =, period WITH &&)
   WHERE (state IN ('held', 'confirmed'))
   DEFERRABLE INITIALLY IMMEDIATE;

-- Supports availability scans and the reaper's "which holds are stale" query.
CREATE INDEX reservations_resource_period_idx
   ON reservations USING gist (resource_id, period)
   WHERE state IN ('held', 'confirmed');

CREATE INDEX reservations_expiring_holds_idx
   ON reservations (hold_expires_at)
   WHERE state = 'held';

CREATE INDEX reservations_guest_idx ON reservations (guest_ref, created_at DESC);

-- ---------------------------------------------------------------------------
-- Audit trail
-- ---------------------------------------------------------------------------

-- Append-only record of every state transition. Written in the same transaction
-- as the transition itself, so the log cannot disagree with the row.
CREATE TABLE reservation_events (
   id              bigserial PRIMARY KEY,
   reservation_id  uuid NOT NULL REFERENCES reservations(id) ON DELETE CASCADE,
   from_state      reservation_state,
   to_state        reservation_state NOT NULL,
   detail          jsonb NOT NULL DEFAULT '{}'::jsonb,
   occurred_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX reservation_events_reservation_idx
   ON reservation_events (reservation_id, occurred_at);

-- ---------------------------------------------------------------------------
-- Idempotency
-- ---------------------------------------------------------------------------

-- A retried POST /v1/holds (mobile client on a flaky network, user
-- double-tapping) must not consume a second unit. The unique key makes the
-- second attempt collide; the stored response is replayed instead.
CREATE TABLE idempotency_keys (
   key             text PRIMARY KEY,
   request_digest  text NOT NULL,
   reservation_id  uuid REFERENCES reservations(id) ON DELETE CASCADE,
   response_body   jsonb,
   created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idempotency_keys_created_idx ON idempotency_keys (created_at);

-- ---------------------------------------------------------------------------
-- Invariant check, callable from tests and from the load harness.
--
-- This is the independent verifier. The concurrency tests do not assert "the
-- application reported no conflicts"; they assert this function returns zero
-- rows after the dust settles. It reads the table directly and knows nothing
-- about the code that wrote it.
-- ---------------------------------------------------------------------------
CREATE FUNCTION find_double_bookings()
RETURNS TABLE (
   unit_id     uuid,
   left_id     uuid,
   right_id    uuid,
   overlap     tstzrange
)
LANGUAGE sql STABLE AS $$
   SELECT a.unit_id, a.id, b.id, a.period * b.period
   FROM reservations a
   JOIN reservations b
     ON a.unit_id = b.unit_id
    AND a.id < b.id
    AND a.period && b.period
   WHERE a.state IN ('held', 'confirmed')
     AND b.state IN ('held', 'confirmed');
$$;
