/**
 * All SQL lives here. Every function takes an explicit `Executor` so the caller
 * decides the transaction and isolation level -- the allocator's whole job is
 * choosing those, and it cannot do that if the repository opens its own.
 */
import type { Executor } from "../db/pool.ts";
import { NotFoundError, VersionMismatchError } from "../domain/errors.ts";
import type {
   AvailabilityRow,
   Period,
   Reservation,
   ReservationState,
   Resource,
   ResourceUnit,
} from "../domain/types.ts";

// `period` is decomposed into lower()/upper() rather than selected raw so we
// never have to parse Postgres's range literal syntax in JavaScript.
const COLUMN_LIST = [
   "id",
   "unit_id",
   "resource_id",
   "guest_ref",
   "state",
   "lower(period) as period_from",
   "upper(period) as period_to",
   "hold_expires_at",
   "version",
   "created_at",
   "updated_at",
];

/** Column list for statements that alias the table as `r`. */
const RESERVATION_COLUMNS = COLUMN_LIST.map((c) =>
   c.startsWith("lower(") || c.startsWith("upper(")
      ? c.replace("(", "(r.")
      : `r.${c}`,
).join(", ");

/** Column list for RETURNING clauses, where no alias is in scope. */
const RETURNING_COLUMNS = COLUMN_LIST.join(", ");

interface ReservationRow {
   id: string;
   unit_id: string;
   resource_id: string;
   guest_ref: string;
   state: ReservationState;
   period_from: Date;
   period_to: Date;
   hold_expires_at: Date | null;
   version: number;
   created_at: Date;
   updated_at: Date;
   unit_label?: string;
}

function toReservation(row: ReservationRow): Reservation {
   return {
      id: row.id,
      unitId: row.unit_id,
      ...(row.unit_label !== undefined ? { unitLabel: row.unit_label } : {}),
      resourceId: row.resource_id,
      guestRef: row.guest_ref,
      state: row.state,
      from: row.period_from,
      to: row.period_to,
      holdExpiresAt: row.hold_expires_at,
      version: row.version,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
   };
}

// ---------------------------------------------------------------------------
// Inventory
// ---------------------------------------------------------------------------

export async function createResource(
   tx: Executor,
   input: { slug: string; name: string; unitLabels: string[] },
): Promise<Resource> {
   const { rows } = await tx.query<{ id: string; slug: string; name: string }>(
      "insert into resources (slug, name) values ($1, $2) returning id, slug, name",
      [input.slug, input.name],
   );
   const resource = rows[0]!;

   // unnest turns the label array into rows so all units insert in one round
   // trip regardless of how many there are.
   await tx.query(
      `insert into resource_units (resource_id, label)
       select $1, label from unnest($2::text[]) as label`,
      [resource.id, input.unitLabels],
   );

   return { ...resource, unitCount: input.unitLabels.length };
}

export async function getResourceBySlug(
   tx: Executor,
   slug: string,
): Promise<Resource | null> {
   const { rows } = await tx.query<{
      id: string;
      slug: string;
      name: string;
      unit_count: number;
   }>(
      `select r.id, r.slug, r.name, count(u.id)::int as unit_count
       from resources r
       left join resource_units u on u.resource_id = r.id
       where r.slug = $1
       group by r.id`,
      [slug],
   );
   const row = rows[0];
   return row ? { id: row.id, slug: row.slug, name: row.name, unitCount: row.unit_count } : null;
}

export async function listUnits(tx: Executor, resourceId: string): Promise<ResourceUnit[]> {
   const { rows } = await tx.query<{ id: string; resource_id: string; label: string }>(
      "select id, resource_id, label from resource_units where resource_id = $1 order by label",
      [resourceId],
   );
   return rows.map((r) => ({ id: r.id, resourceId: r.resource_id, label: r.label }));
}

export async function availability(
   tx: Executor,
   resourceId: string,
   period: Period,
): Promise<AvailabilityRow[]> {
   const { rows } = await tx.query<{
      unit_id: string;
      label: string;
      is_free: boolean;
      taken_by: string | null;
   }>(
      `select * from resource_availability($1, tstzrange($2::timestamptz, $3::timestamptz, '[)'))`,
      [resourceId, period.from, period.to],
   );
   return rows.map((r) => ({
      unitId: r.unit_id,
      label: r.label,
      isFree: r.is_free,
      takenBy: r.taken_by,
   }));
}

// ---------------------------------------------------------------------------
// Hold expiry
// ---------------------------------------------------------------------------

/**
 * Flip holds that have outlived their deadline to 'expired', releasing their
 * inventory.
 *
 * Scoped to the resource and window the caller actually cares about, so a
 * booking request pays only for the rows blocking *it* rather than sweeping the
 * whole table. The background reaper does the unscoped version on a timer.
 *
 * `skip locked` matters: if a concurrent transaction is already expiring the
 * same row we step over it instead of queueing behind its lock. Whoever gets
 * there first does the work; we just need the row gone by the time we insert.
 */
export async function reclaimExpiredHolds(
   tx: Executor,
   resourceId: string,
   period: Period,
): Promise<number> {
   const { rowCount } = await tx.query(
      `with stale as (
          select id
          from reservations
          where resource_id = $1
            and state = 'held'
            and hold_expires_at <= now()
            and period && tstzrange($2::timestamptz, $3::timestamptz, '[)')
          for update skip locked
       ), flipped as (
          update reservations r
          set state = 'expired', hold_expires_at = null, version = r.version + 1, updated_at = now()
          from stale
          where r.id = stale.id
          returning r.id, r.state
       )
       insert into reservation_events (reservation_id, from_state, to_state, detail)
       select id, 'held', 'expired', '{"by":"allocator"}'::jsonb from flipped`,
      [resourceId, period.from, period.to],
   );
   return rowCount ?? 0;
}

/** Unscoped sweep used by the background reaper. Returns rows expired. */
export async function reapExpiredHolds(tx: Executor, limit: number): Promise<number> {
   const { rowCount } = await tx.query(
      `with stale as (
          select id
          from reservations
          where state = 'held' and hold_expires_at <= now()
          order by hold_expires_at
          limit $1
          for update skip locked
       ), flipped as (
          update reservations r
          set state = 'expired', hold_expires_at = null, version = r.version + 1, updated_at = now()
          from stale
          where r.id = stale.id
          returning r.id
       )
       insert into reservation_events (reservation_id, from_state, to_state, detail)
       select id, 'held', 'expired', '{"by":"reaper"}'::jsonb from flipped`,
      [limit],
   );
   return rowCount ?? 0;
}

// ---------------------------------------------------------------------------
// Allocation primitives
// ---------------------------------------------------------------------------

export type CandidateOrder = "random" | "label";

/**
 * Units of `resourceId` with nothing occupying `period`.
 *
 * The result is a *hint*, not a reservation. Between this SELECT and the
 * INSERT that follows, another transaction may take one of these units; the
 * exclusion constraint is what turns that race into an error instead of a
 * double-booking.
 *
 * Ordering is the single biggest lever on throughput under contention. With
 * 'label', every concurrent request picks the same lowest-labelled unit and
 * they all collide; with 'random' they fan out across free inventory and the
 * conflict rate collapses. bench/loadtest.ts measures exactly this.
 */
export async function findFreeUnits(
   tx: Executor,
   resourceId: string,
   period: Period,
   order: CandidateOrder = "random",
   // Wide enough that random selection actually spreads a large burst across
   // free inventory. With a narrow candidate list, thousands of concurrent
   // requests pile onto a handful of units and manufacture conflicts that the
   // inventory did not require.
   limit = 64,
): Promise<string[]> {
   const { rows } = await tx.query<{ id: string }>(
      `select u.id
       from resource_units u
       where u.resource_id = $1
         and not exists (
            select 1
            from reservations r
            where r.unit_id = u.id
              and r.state in ('held', 'confirmed')
              and r.period && tstzrange($2::timestamptz, $3::timestamptz, '[)')
         )
       order by ${order === "random" ? "random()" : "u.label"}
       limit $4`,
      [resourceId, period.from, period.to, limit],
   );
   return rows.map((r) => r.id);
}

/**
 * Insert a hold on a specific unit.
 *
 * Throws SQLSTATE 23P01 if the unit was taken since findFreeUnits() ran. That
 * throw is the load-bearing part of this service: it is the moment a
 * double-booking is prevented.
 */
export async function insertHold(
   tx: Executor,
   input: {
      unitId: string;
      resourceId: string;
      guestRef: string;
      period: Period;
      ttlSeconds: number;
   },
): Promise<Reservation> {
   // The reservation and its audit event are written by one statement. Two
   // reasons: they commit atomically by construction rather than by convention,
   // and it halves the round trips on the hottest path in the service.
   const { rows } = await tx.query<ReservationRow>(
      `with created as (
          insert into reservations
             (unit_id, resource_id, guest_ref, state, period, hold_expires_at)
          values
             ($1, $2, $3, 'held',
              tstzrange($4::timestamptz, $5::timestamptz, '[)'),
              now() + make_interval(secs => $6::double precision))
          returning ${RETURNING_COLUMNS}
       ), logged as (
          insert into reservation_events (reservation_id, from_state, to_state, detail)
          select id, null, 'held', jsonb_build_object('ttlSeconds', $6::double precision)
          from created
       )
       select * from created`,
      [
         input.unitId,
         input.resourceId,
         input.guestRef,
         input.period.from,
         input.period.to,
         input.ttlSeconds,
      ],
   );
   return toReservation(rows[0]!);
}

/** Serializes allocation for one resource. Used by the `pessimistic` strategy. */
export async function lockResourceForAllocation(
   tx: Executor,
   resourceId: string,
): Promise<void> {
   // A transaction-scoped advisory lock rather than `SELECT ... FOR UPDATE` on
   // the resources row: this is a mutex over the *act of allocating*, not a
   // claim on resource metadata, so it does not block an unrelated rename. It
   // releases automatically at commit or rollback, which means a crashed
   // backend cannot strand the lock.
   await tx.query("select pg_advisory_xact_lock(hashtextextended($1, 0))", [resourceId]);
}

// ---------------------------------------------------------------------------
// State transitions (optimistic locking)
// ---------------------------------------------------------------------------

/**
 * Move a reservation between states, asserting the version the caller read.
 *
 * `where id = $1 and version = $2` is the whole optimistic-locking mechanism:
 * if another request already transitioned this row, its version moved and this
 * UPDATE matches zero rows. We then read the row back to tell "you had a stale
 * version" apart from "that id does not exist".
 */
async function transition(
   tx: Executor,
   id: string,
   expectedVersion: number,
   from: ReservationState[],
   to: ReservationState,
   detail: Record<string, unknown> = {},
): Promise<Reservation> {
   // The `prev` CTE reads the row before the UPDATE runs, which is the only way
   // to get the *actual* prior state into the audit log. Using the first
   // permitted from-state instead would mislabel every cancellation of a
   // confirmed booking as a cancellation of a hold.
   //
   // Folding the event insert into the same statement also keeps the log
   // atomic with the transition by construction, and costs one round trip
   // instead of two on a hot path.
   const { rows } = await tx.query<ReservationRow & { previous_state: ReservationState }>(
      `with prev as (
          select id, state from reservations where id = $1
       ), updated as (
          update reservations r
          set state = $4,
              version = r.version + 1,
              hold_expires_at = case
                 when $4::reservation_state = 'held' then r.hold_expires_at
                 else null
              end,
              updated_at = now()
          from prev
          where r.id = prev.id
            and r.version = $2
            and r.state = any($3::reservation_state[])
          returning ${RESERVATION_COLUMNS}, prev.state as previous_state
       ), logged as (
          insert into reservation_events (reservation_id, from_state, to_state, detail)
          select id, previous_state, $4::reservation_state, $5::jsonb from updated
       )
       select * from updated`,
      [id, expectedVersion, from, to, JSON.stringify(detail)],
   );

   const row = rows[0];
   if (row) return toReservation(row);

   const current = await getReservation(tx, id);
   if (!current) throw new NotFoundError("Reservation");
   throw new VersionMismatchError({
      reservationId: id,
      expectedVersion,
      actualVersion: current.version,
      currentState: current.state,
   });
}

export function confirmHold(
   tx: Executor,
   id: string,
   expectedVersion: number,
): Promise<Reservation> {
   return transition(tx, id, expectedVersion, ["held"], "confirmed");
}

export function cancelReservation(
   tx: Executor,
   id: string,
   expectedVersion: number,
   reason = "client_request",
): Promise<Reservation> {
   return transition(tx, id, expectedVersion, ["held", "confirmed"], "cancelled", { reason });
}

export async function getReservation(
   tx: Executor,
   id: string,
): Promise<Reservation | null> {
   const { rows } = await tx.query<ReservationRow>(
      `select ${RESERVATION_COLUMNS}, u.label as unit_label
       from reservations r
       join resource_units u on u.id = r.unit_id
       where r.id = $1`,
      [id],
   );
   const row = rows[0];
   return row ? toReservation(row) : null;
}

// ---------------------------------------------------------------------------
// Verification -- used by tests and the load harness, never by request handlers
// ---------------------------------------------------------------------------

export interface DoubleBooking {
   unitId: string;
   leftId: string;
   rightId: string;
   overlap: string;
}

export async function findDoubleBookings(
   tx: Executor,
   table: "reservations" | "naive_reservations" = "reservations",
): Promise<DoubleBooking[]> {
   const fn =
      table === "reservations" ? "find_double_bookings()" : "find_naive_double_bookings()";
   const { rows } = await tx.query<{
      unit_id: string;
      left_id: string;
      right_id: string;
      overlap: string;
   }>(`select * from ${fn}`);
   return rows.map((r) => ({
      unitId: r.unit_id,
      leftId: r.left_id,
      rightId: r.right_id,
      overlap: r.overlap,
   }));
}
