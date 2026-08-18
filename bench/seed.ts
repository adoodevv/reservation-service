/**
 * Bulk data generation for the load harness.
 *
 * Table size is not cosmetic here. On a 20-row `reservations` table the planner
 * seq-scans, and under SERIALIZABLE a seq scan takes a *relation-level*
 * predicate lock -- so every reader conflicts with every writer and SSI aborts
 * essentially everything. Benchmarking on an empty database therefore measures
 * an artefact of table size rather than the strategy. Padding the table until
 * the planner switches to index scans is what makes the comparison honest.
 */
import pg from "pg";
import { config } from "../src/config.ts";
import { pool } from "../src/db/pool.ts";

export interface SeedOptions {
   /** Filler resources to create. Each gets `unitsPerResource` units. */
   resources: number;
   unitsPerResource: number;
   /** Reservations per unit, placed in far-future non-overlapping windows. */
   reservationsPerUnit: number;
}

export interface SeedStats {
   reservations: number;
   units: number;
   tableSize: string;
   seconds: number;
}

/**
 * Seeding legitimately exceeds the request-path `statement_timeout`, so it runs
 * on its own session with the timeout disabled rather than relaxing the limit
 * that protects production queries.
 */
export async function seedFiller(options: SeedOptions): Promise<SeedStats> {
   const started = performance.now();
   const client = new pg.Client({
      connectionString: config.databaseUrl,
      statement_timeout: 0,
   });
   await client.connect();

   try {
      await client.query(
         `insert into resources (slug, name)
          select 'filler-' || g, 'Filler resource ' || g
          from generate_series(1, $1) g
          on conflict (slug) do nothing`,
         [options.resources],
      );

      await client.query(
         `insert into resource_units (resource_id, label)
          select r.id, 'u' || u
          from resources r, generate_series(1, $1) u
          where r.slug like 'filler-%'
          on conflict (resource_id, label) do nothing`,
         [options.unitsPerResource],
      );

      // Windows are spaced 40 days apart with 30-day stays, so filler rows never
      // overlap each other and never touch the contested window under test.
      await client.query(
         `insert into reservations (unit_id, resource_id, guest_ref, state, period)
          select u.id, u.resource_id, 'filler', 'confirmed',
                 tstzrange(
                    timestamptz '2031-01-01' + (n * 40 || ' days')::interval,
                    timestamptz '2031-01-01' + (n * 40 + 30 || ' days')::interval,
                    '[)')
          from resource_units u, generate_series(0, $1 - 1) n
          where u.resource_id in (select id from resources where slug like 'filler-%')
            -- Seeding must be re-runnable: without this the second run inserts
            -- the same windows again and trips the exclusion constraint.
            and not exists (
               select 1 from reservations r
               where r.unit_id = u.id and r.guest_ref = 'filler'
            )`,
         [options.reservationsPerUnit],
      );

      // Without fresh statistics the planner keeps its old row estimates and
      // stays on the seq-scan plan, defeating the entire point of seeding.
      await client.query("analyze");
   } finally {
      await client.end();
   }

   const { rows } = await pool.query<{
      reservations: number;
      units: number;
      size: string;
   }>(`select (select count(*)::int from reservations)   as reservations,
              (select count(*)::int from resource_units) as units,
              pg_size_pretty(pg_relation_size('reservations')) as size`);

   const row = rows[0]!;
   return {
      reservations: row.reservations,
      units: row.units,
      tableSize: row.size,
      seconds: (performance.now() - started) / 1000,
   };
}

/** Removes contended fixtures between runs while leaving filler data in place. */
export async function resetContended(): Promise<void> {
   await pool.query("delete from reservations where guest_ref <> 'filler'");
   await pool.query("delete from naive_reservations");
   await pool.query("delete from idempotency_keys");
   await pool.query("delete from resource_units where resource_id in (select id from resources where slug like 'contended-%')");
   await pool.query("delete from resources where slug like 'contended-%'");
}

/** Reports whether the candidate-selection query uses an index or a seq scan. */
export async function candidatePlan(resourceId: string): Promise<string> {
   const { rows } = await pool.query<{ "QUERY PLAN": string }>(
      `explain (costs off)
       select u.id from resource_units u
       where u.resource_id = $1
         and not exists (
            select 1 from reservations r
            where r.unit_id = u.id and r.state in ('held','confirmed')
              and r.period && tstzrange($2::timestamptz, $3::timestamptz, '[)'))
       limit 16`,
      [resourceId, "2030-09-01", "2030-09-11"],
   );
   return rows.map((r) => r["QUERY PLAN"]).join("\n");
}
