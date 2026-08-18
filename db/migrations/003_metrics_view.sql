-- ---------------------------------------------------------------------------
-- 003_metrics_view: occupancy reporting straight from the range column.
-- ---------------------------------------------------------------------------

-- Per-resource inventory snapshot for an arbitrary window. Used by
-- GET /v1/resources/:id/availability and by the load harness to assert that the
-- number of successful bookings exactly equals unit count for a contested night
-- -- not one fewer (lost capacity) and not one more (double-booking).
CREATE FUNCTION resource_availability(
   p_resource_id uuid,
   p_period      tstzrange
)
RETURNS TABLE (
   unit_id      uuid,
   label        text,
   is_free      boolean,
   taken_by     uuid
)
LANGUAGE sql STABLE AS $$
   SELECT u.id,
          u.label,
          r.id IS NULL AS is_free,
          r.id         AS taken_by
   FROM resource_units u
   LEFT JOIN reservations r
          ON r.unit_id = u.id
         AND r.state IN ('held', 'confirmed')
         AND r.period && p_period
   WHERE u.resource_id = p_resource_id
   ORDER BY u.label;
$$;
