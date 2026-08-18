/**
 * Schema-level guarantees.
 *
 * These bypass the service layer entirely and write straight to the table. If
 * they pass, the invariant holds even for a caller that ignores every line of
 * application code -- a buggy migration, an ops engineer at a psql prompt, a
 * future endpoint nobody has written yet.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { closePool, pool, withTransaction } from "../src/db/pool.ts";
import { PG_EXCLUSION_VIOLATION } from "../src/domain/errors.ts";
import * as repo from "../src/repo/reservations.ts";
import { countDoubleBookings, ensureSchema, seedResource, truncateAll } from "./helpers.ts";

/** Raw insert, so the test exercises the constraint rather than the allocator. */
async function rawInsert(
   unitId: string,
   resourceId: string,
   state: string,
   from: string,
   to: string,
): Promise<void> {
   await pool.query(
      `insert into reservations (unit_id, resource_id, guest_ref, state, period, hold_expires_at)
       values ($1, $2, 'raw', $3::reservation_state,
               tstzrange($4::timestamptz, $5::timestamptz, '[)'),
               case when $3 = 'held' then now() + interval '1 hour' end)`,
      [unitId, resourceId, state, from, to],
   );
}

async function expectSqlState(fn: () => Promise<unknown>, state: string): Promise<void> {
   await expect(fn()).rejects.toMatchObject({ code: state });
}

describe("exclusion constraint", () => {
   let resourceId: string;
   let unitA: string;
   let unitB: string;

   beforeAll(ensureSchema);
   afterAll(closePool);

   beforeEach(async () => {
      await truncateAll();
      const resource = await seedResource(2, "constraint-test");
      resourceId = resource.id;
      const units = await withTransaction((tx) => repo.listUnits(tx, resourceId));
      unitA = units[0]!.id;
      unitB = units[1]!.id;
   });

   it("rejects an overlapping confirmed reservation on the same unit", async () => {
      await rawInsert(unitA, resourceId, "confirmed", "2030-09-01", "2030-09-05");
      await expectSqlState(
         () => rawInsert(unitA, resourceId, "confirmed", "2030-09-03", "2030-09-07"),
         PG_EXCLUSION_VIOLATION,
      );
   });

   it("rejects an overlap between a held and a confirmed reservation", async () => {
      await rawInsert(unitA, resourceId, "held", "2030-09-01", "2030-09-05");
      await expectSqlState(
         () => rawInsert(unitA, resourceId, "confirmed", "2030-09-04", "2030-09-06"),
         PG_EXCLUSION_VIOLATION,
      );
   });

   it("rejects a reservation fully contained within another", async () => {
      await rawInsert(unitA, resourceId, "confirmed", "2030-09-01", "2030-09-10");
      await expectSqlState(
         () => rawInsert(unitA, resourceId, "held", "2030-09-04", "2030-09-05"),
         PG_EXCLUSION_VIOLATION,
      );
   });

   it("rejects a reservation that fully contains another", async () => {
      await rawInsert(unitA, resourceId, "confirmed", "2030-09-04", "2030-09-05");
      await expectSqlState(
         () => rawInsert(unitA, resourceId, "held", "2030-09-01", "2030-09-10"),
         PG_EXCLUSION_VIOLATION,
      );
   });

   it("allows back-to-back stays: checkout day equals the next check-in day", async () => {
      // The half-open '[)' bound is what makes this legal. With '[]' bounds
      // these two would overlap on 09-05 and the second would be rejected --
      // silently costing a night of inventory on every turnover, every day.
      await rawInsert(unitA, resourceId, "confirmed", "2030-09-01", "2030-09-05");
      await expect(
         rawInsert(unitA, resourceId, "confirmed", "2030-09-05", "2030-09-08"),
      ).resolves.toBeUndefined();
   });

   it("allows the same period on a different unit", async () => {
      await rawInsert(unitA, resourceId, "confirmed", "2030-09-01", "2030-09-05");
      await expect(
         rawInsert(unitB, resourceId, "confirmed", "2030-09-01", "2030-09-05"),
      ).resolves.toBeUndefined();
   });

   it("allows an overlap once the blocking reservation is cancelled", async () => {
      // Cancelled rows leave the partial index and stop occupying inventory,
      // while staying on the table as history.
      await rawInsert(unitA, resourceId, "cancelled", "2030-09-01", "2030-09-05");
      await rawInsert(unitA, resourceId, "expired", "2030-09-02", "2030-09-06");
      await expect(
         rawInsert(unitA, resourceId, "confirmed", "2030-09-01", "2030-09-10"),
      ).resolves.toBeUndefined();
      expect(await countDoubleBookings()).toBe(0);
   });

   it("rejects re-activating a cancelled reservation over a live one", async () => {
      // The constraint guards UPDATE, not just INSERT: a row cannot be brought
      // back into the index on top of an existing booking.
      await rawInsert(unitA, resourceId, "cancelled", "2030-09-01", "2030-09-05");
      await rawInsert(unitA, resourceId, "confirmed", "2030-09-02", "2030-09-04");
      await expectSqlState(
         () =>
            pool.query(
               "update reservations set state = 'confirmed' where state = 'cancelled'",
            ),
         PG_EXCLUSION_VIOLATION,
      );
   });

   it("rejects an empty period", async () => {
      // A zero-width range is the dangerous case: Postgres normalises
      // tstzrange(x, x, '[)') to the singleton 'empty' value, and `empty && r`
      // is false for *every* r -- so an empty period would slip past the
      // exclusion constraint entirely and book a unit for no nights.
      //
      // Two CHECKs cover it. 'empty' also reports NULL for lower()/upper(), so
      // reservations_period_bounded happens to fire first; the nonempty check
      // remains as defence for ranges that are empty without being NULL-bounded.
      await expect(
         rawInsert(unitA, resourceId, "confirmed", "2030-09-05", "2030-09-05"),
      ).rejects.toThrow(/reservations_period_(nonempty|bounded)/);
   });

   it("rejects an unbounded period", async () => {
      // An open-ended range would overlap every future booking on the unit.
      await expect(
         pool.query(
            `insert into reservations (unit_id, resource_id, guest_ref, state, period)
             values ($1, $2, 'raw', 'confirmed', tstzrange($3::timestamptz, null, '[)'))`,
            [unitA, resourceId, "2030-09-01"],
         ),
      ).rejects.toThrow(/reservations_period_bounded/);
   });

   it("rejects a hold with no expiry deadline", async () => {
      // A hold that never expires is a permanent inventory leak.
      await expect(
         pool.query(
            `insert into reservations (unit_id, resource_id, guest_ref, state, period)
             values ($1, $2, 'raw', 'held', tstzrange($3::timestamptz, $4::timestamptz, '[)'))`,
            [unitA, resourceId, "2030-09-01", "2030-09-05"],
         ),
      ).rejects.toThrow(/reservations_hold_has_deadline/);
   });
});
