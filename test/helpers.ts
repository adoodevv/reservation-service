/** Shared fixtures: a clean database and a resource with N units. */
import { migrate } from "../src/db/migrate.ts";
import { pool, withTransaction } from "../src/db/pool.ts";
import type { Period, Resource } from "../src/domain/types.ts";
import * as repo from "../src/repo/reservations.ts";

let migrated = false;

export async function ensureSchema(): Promise<void> {
   if (migrated) return;
   await migrate(() => {});
   migrated = true;
}

/**
 * Wipe all mutable state. `restart identity` resets the event sequence so
 * assertions about event ordering are stable across runs.
 */
export async function truncateAll(): Promise<void> {
   await pool.query(`
      truncate reservation_events, reservations, naive_reservations,
               idempotency_keys, resource_units, resources
      restart identity cascade
   `);
}

export async function seedResource(unitCount: number, slug = "test-resource"): Promise<Resource> {
   const unitLabels = Array.from({ length: unitCount }, (_, i) =>
      String(i + 1).padStart(3, "0"),
   );
   return withTransaction((tx) =>
      repo.createResource(tx, { slug, name: `Test resource (${unitCount} units)`, unitLabels }),
   );
}

/** A fixed future window, so tests never depend on the wall clock's date. */
export function period(fromDay = 1, toDay = 5): Period {
   return {
      from: new Date(Date.UTC(2030, 8, fromDay)),
      to: new Date(Date.UTC(2030, 8, toDay)),
   };
}

export async function countDoubleBookings(
   table: "reservations" | "naive_reservations" = "reservations",
): Promise<number> {
   const conflicts = await withTransaction((tx) => repo.findDoubleBookings(tx, table));
   if (conflicts.length > 0 && table === "reservations") {
      const { rows } = await pool.query(
         `select a.xmin::text as inserting_xid, a.id, a.guest_ref, a.state::text,
                 a.version, r.slug, a.unit_id,
                 lower(a.period) lo, upper(a.period) hi, a.created_at, a.updated_at,
                 (select json_agg(json_build_object('from', e.from_state::text,
                                                    'to', e.to_state::text,
                                                    'at', e.occurred_at,
                                                    'detail', e.detail) order by e.id)
                  from reservation_events e where e.reservation_id = a.id) as events
          from reservations a join resources r on r.id = a.resource_id
          where a.id = any($1::uuid[]) order by a.unit_id, a.created_at`,
         [conflicts.flatMap((c) => [c.leftId, c.rightId])],
      );
      console.error("CONFLICT ROWS:", JSON.stringify(rows, null, 1));
   }
   return conflicts.length;
}

export async function countByState(): Promise<Record<string, number>> {
   const { rows } = await pool.query<{ state: string; n: number }>(
      "select state::text as state, count(*)::int as n from reservations group by state",
   );
   return Object.fromEntries(rows.map((r) => [r.state, r.n]));
}

/**
 * Release `count` tasks as close to simultaneously as the runtime allows.
 *
 * Building every promise before awaiting any is what creates the race: all
 * `count` transactions are handed to the pool in one synchronous pass, so they
 * arrive at Postgres interleaved rather than one at a time. A `for await` loop
 * would serialise the whole test and prove nothing.
 */
export async function stampede<T>(
   count: number,
   task: (index: number) => Promise<T>,
): Promise<PromiseSettledResult<T>[]> {
   const gate = Promise.withResolvers<void>();
   const tasks = Array.from({ length: count }, (_, i) => gate.promise.then(() => task(i)));
   gate.resolve();
   return Promise.allSettled(tasks);
}

/** Splits settled results into fulfilled values and rejection reasons. */
export function partition<T>(results: PromiseSettledResult<T>[]): {
   ok: T[];
   failed: unknown[];
} {
   const ok: T[] = [];
   const failed: unknown[] = [];
   for (const r of results) {
      if (r.status === "fulfilled") ok.push(r.value);
      else failed.push(r.reason);
   }
   return { ok, failed };
}
