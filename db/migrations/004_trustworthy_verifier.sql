-- ---------------------------------------------------------------------------
-- 004_trustworthy_verifier: stop the double-booking check trusting the index
-- it is supposed to be checking.
-- ---------------------------------------------------------------------------
--
-- find_double_bookings() is the whole verification story of this project: the
-- tests and the load harness never ask the service whether it double-booked,
-- they ask the database. That only means something if the check is capable of
-- seeing a violation.
--
-- It was not. The planner resolved `a.period && b.period` with an index scan
-- over reservations_no_overlap -- the exclusion index itself:
--
--    Nested Loop
--      ->  Seq Scan on reservations a
--      ->  Index Scan using reservations_no_overlap on reservations b
--            Index Cond: ((unit_id = a.unit_id) AND (period && a.period))
--
-- So the verifier asked the index "are there any overlaps?", and the index --
-- the very thing whose entries had gone missing -- answered no. On a table
-- holding four genuine violations it reported two. Forcing a sequential scan
-- made the same query on the same rows return all four, and REINDEX refused to
-- rebuild the index at all, which is what a heap holding rows the index never
-- recorded looks like.
--
-- A verifier must not share a failure mode with the thing it verifies. These
-- functions run in tests and benchmarks, never on a request path, so reading
-- every row is exactly the trade we want: correctness of the answer over speed
-- of getting it.
--
-- The settings are attached to the functions rather than issued by callers so
-- the guarantee travels with the check instead of depending on every caller
-- remembering it.

CREATE OR REPLACE FUNCTION find_double_bookings()
RETURNS TABLE (
   unit_id     uuid,
   left_id     uuid,
   right_id    uuid,
   overlap     tstzrange
)
LANGUAGE sql STABLE
SET enable_indexscan = off
SET enable_bitmapscan = off
SET enable_indexonlyscan = off
AS $$
   SELECT a.unit_id, a.id, b.id, a.period * b.period
   FROM reservations a
   JOIN reservations b
     ON a.unit_id = b.unit_id
    AND a.id < b.id
    AND a.period && b.period
   WHERE a.state IN ('held', 'confirmed')
     AND b.state IN ('held', 'confirmed');
$$;

-- The naive table has no exclusion constraint to be blinded by, but the control
-- group's numbers are only meaningful next to the guarded table's if both were
-- counted the same way.
CREATE OR REPLACE FUNCTION find_naive_double_bookings()
RETURNS TABLE (
   unit_id     uuid,
   left_id     uuid,
   right_id    uuid,
   overlap     tstzrange
)
LANGUAGE sql STABLE
SET enable_indexscan = off
SET enable_bitmapscan = off
SET enable_indexonlyscan = off
AS $$
   SELECT a.unit_id, a.id, b.id, a.period * b.period
   FROM naive_reservations a
   JOIN naive_reservations b
     ON a.unit_id = b.unit_id
    AND a.id < b.id
    AND a.period && b.period
   WHERE a.state IN ('held', 'confirmed')
     AND b.state IN ('held', 'confirmed');
$$;
