-- ---------------------------------------------------------------------------
-- 002_naive_baseline: the control group.
--
-- A load test that reports "0 double-bookings" proves nothing unless the same
-- harness can produce double-bookings when the guard is removed. This table is
-- schema-identical to `reservations` minus the EXCLUDE constraint, and is
-- written by the deliberately-unsafe `naive` allocation strategy
-- (check-then-insert under READ COMMITTED -- the bug almost every first-draft
-- booking system ships).
--
-- It exists to be broken. Nothing in the production request path touches it.
-- ---------------------------------------------------------------------------

CREATE TABLE naive_reservations (
   id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
   unit_id          uuid NOT NULL REFERENCES resource_units(id) ON DELETE RESTRICT,
   resource_id      uuid NOT NULL REFERENCES resources(id) ON DELETE RESTRICT,
   guest_ref        text NOT NULL,
   state            reservation_state NOT NULL,
   period           tstzrange NOT NULL,
   hold_expires_at  timestamptz,
   version          integer NOT NULL DEFAULT 1,
   created_at       timestamptz NOT NULL DEFAULT now(),
   updated_at       timestamptz NOT NULL DEFAULT now()

   -- NO exclusion constraint. That is the entire point.
);

CREATE INDEX naive_reservations_unit_idx ON naive_reservations (unit_id);
CREATE INDEX naive_reservations_resource_idx ON naive_reservations (resource_id);

-- Same verifier shape as find_double_bookings(), pointed at the unguarded table.
CREATE FUNCTION find_naive_double_bookings()
RETURNS TABLE (
   unit_id     uuid,
   left_id     uuid,
   right_id    uuid,
   overlap     tstzrange
)
LANGUAGE sql STABLE AS $$
   SELECT a.unit_id, a.id, b.id, a.period * b.period
   FROM naive_reservations a
   JOIN naive_reservations b
     ON a.unit_id = b.unit_id
    AND a.id < b.id
    AND a.period && b.period
   WHERE a.state IN ('held', 'confirmed')
     AND b.state IN ('held', 'confirmed');
$$;
