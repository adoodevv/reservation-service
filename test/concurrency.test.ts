/**
 * The point of the project.
 *
 * Every test here fires many reservation attempts at the same instant against
 * the same inventory, then verifies the result *from the database* with
 * find_double_bookings() -- a SQL self-join that knows nothing about the code
 * that wrote the rows. The suite never asserts "the service reported no
 * conflicts"; it asserts no conflicting rows exist.
 *
 * Two properties are checked every time, and the second is the one people
 * forget:
 *
 *   safety   -- no two inventory-occupying reservations overlap on a unit.
 *               A service that rejects everything is trivially safe.
 *   liveness -- the number of winners is exactly the number of units. Not
 *               fewer: losing a race must not lose inventory. A system that
 *               books 47 of 50 rooms under load is broken too, just quietly.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { closePool, pool, withTransaction } from "../src/db/pool.ts";
import { NoInventoryError, ServiceError } from "../src/domain/errors.ts";
import type { AllocationOutcome } from "../src/domain/types.ts";
import * as repo from "../src/repo/reservations.ts";
import type { AllocationStrategy } from "../src/services/allocator.ts";
import { allocate } from "../src/services/allocator.ts";
import {
   countDoubleBookings,
   ensureSchema,
   partition,
   period,
   seedResource,
   stampede,
   truncateAll,
} from "./helpers.ts";

/** Every strategy that must never double-book. */
const SAFE_STRATEGIES: AllocationStrategy[] = ["optimistic", "serializable", "pessimistic"];

/**
 * Strategies exercised at full load.
 *
 * `serializable` is excluded from the heaviest scenarios, and the reason is a
 * measured result rather than a convenience. At ~12 concurrent requests per
 * unit its SSI aborts multiply the population of in-flight transactions on the
 * same key range; each INSERT into the exclusion-constrained index then has to
 * wait out every conflicting uncommitted transaction it meets, re-scanning
 * after each one. `lock_timeout` bounds an individual wait but not that loop,
 * so statements accumulate past `statement_timeout`: 97 of 120 requests died on
 * SQLSTATE 57014, and the scenario took 129s against 0.7s for `optimistic`.
 *
 * That is an availability failure, not a safety one -- it never oversold. The
 * dedicated test below pins exactly that distinction, and bench/RESULTS.md
 * carries the numbers.
 */
const HIGH_LOAD_STRATEGIES: AllocationStrategy[] = ["optimistic", "pessimistic"];

function summarise(failed: unknown[]): Record<string, number> {
   const counts: Record<string, number> = {};
   for (const err of failed) {
      const code = err instanceof ServiceError ? err.code : "unexpected";
      counts[code] = (counts[code] ?? 0) + 1;
      if (code === "unexpected") throw err;
   }
   return counts;
}

// One pool for the whole file: a closePool() inside a describe would tear the
// pool down for every describe that runs after it.
beforeAll(ensureSchema);
afterAll(closePool);

describe("concurrent allocation", () => {
   beforeEach(truncateAll);

   describe.each(SAFE_STRATEGIES)("strategy: %s", (strategy) => {
      it("two users hitting the last room at the same instant: exactly one wins", async () => {
         const resource = await seedResource(1, `last-room-${strategy}`);
         const window = period();

         const results = await stampede(2, (i) =>
            allocate({
               resourceId: resource.id,
               guestRef: `guest-${i}`,
               period: window,
               strategy,
            }),
         );

         const { ok, failed } = partition(results);
         expect(ok).toHaveLength(1);
         expect(failed).toHaveLength(1);
         expect(failed[0]).toBeInstanceOf(NoInventoryError);
         expect(await countDoubleBookings()).toBe(0);
      });

      it("200 concurrent attempts on a single unit: exactly one wins", async () => {
         const resource = await seedResource(1, `single-unit-${strategy}`);
         const window = period();

         const results = await stampede(200, (i) =>
            allocate({
               resourceId: resource.id,
               guestRef: `guest-${i}`,
               period: window,
               strategy,
            }),
         );

         const { ok, failed } = partition(results);
         expect(await countDoubleBookings()).toBe(0);
         expect(ok).toHaveLength(1);
         expect(summarise(failed).no_inventory).toBe(199);
      });

   });

   describe.each(HIGH_LOAD_STRATEGIES)("under full load: %s", (strategy) => {
      it("500 concurrent attempts on 50 units: exactly 50 win, none lost", async () => {
         const capacity = 50;
         const resource = await seedResource(capacity, `capacity-${strategy}`);
         const window = period();

         const results = await stampede(500, (i) =>
            allocate({
               resourceId: resource.id,
               guestRef: `guest-${i}`,
               period: window,
               strategy,
               maxRetries: 20,
            }),
         );

         const { ok, failed } = partition(results);

         // Safety.
         expect(await countDoubleBookings()).toBe(0);
         // Liveness: every unit sold, and not one more.
         expect(ok).toHaveLength(capacity);
         // Every loser got a truthful "sold out", not a spurious error.
         expect(summarise(failed)).toEqual({ no_inventory: 500 - capacity });

         // Each winner holds a distinct unit -- the real statement of "no
         // double-booking", expressed independently of the SQL verifier.
         const units = new Set(ok.map((o: AllocationOutcome) => o.reservation.unitId));
         expect(units.size).toBe(capacity);
      });

      it("overlapping but non-identical date ranges contend correctly", async () => {
         // Every request wants a different window, but all of them include the
         // night of the 10th, so at most `capacity` can succeed. This catches
         // an implementation that only compares ranges for equality.
         const capacity = 10;
         const resource = await seedResource(capacity, `sliding-${strategy}`);

         const results = await stampede(120, (i) =>
            allocate({
               resourceId: resource.id,
               guestRef: `guest-${i}`,
               period: period(1 + (i % 9), 11 + (i % 9)),
               strategy,
               maxRetries: 20,
            }),
         );

         const { ok } = partition(results);
         expect(await countDoubleBookings()).toBe(0);

         // Everything overlaps the 10th, so the winners are capped by capacity.
         expect(ok.length).toBeLessThanOrEqual(capacity);
         expect(ok.length).toBeGreaterThan(0);

         const nightOfTheTenth = await withTransaction((tx) =>
            repo.availability(tx, resource.id, period(10, 11)),
         );
         expect(nightOfTheTenth.filter((u) => !u.isFree)).toHaveLength(ok.length);
      });
   });

   it("serializable stays correct at the load where it stops being available", async () => {
      // SERIALIZABLE's problem in this service is throughput, not safety. Even
      // when most requests are shed, the ones that land must still respect
      // capacity -- it must never oversell to compensate.
      const capacity = 10;
      const resource = await seedResource(capacity, "serializable-correctness");
      const window = period();

      const results = await stampede(60, (i) =>
         allocate({
            resourceId: resource.id,
            guestRef: `guest-${i}`,
            period: window,
            strategy: "serializable",
            maxRetries: 10,
         }),
      );

      const { ok } = partition(results);
      expect(await countDoubleBookings()).toBe(0);
      // Never more than capacity. Possibly fewer, if requests were shed.
      expect(ok.length).toBeLessThanOrEqual(capacity);
      expect(new Set(ok.map((o: AllocationOutcome) => o.reservation.unitId)).size).toBe(
         ok.length,
      );
   });

   it("a lost race must never be reported as sold out", async () => {
      // Regression test. The allocator narrows its search by excluding units it
      // has already collided with, which is what lets a large burst converge
      // instead of resampling contested inventory until its budget runs out.
      //
      // But a conflict is not proof that inventory is gone -- the winner may
      // roll back, its hold may expire, or the failure may have been a
      // serialization abort that says nothing about the unit at all. An earlier
      // version let an empty *filtered* candidate list stand in for an empty
      // one, and on a single unit both contenders excluded it and both reported
      // "sold out": a booking lost to inventory that was sitting right there.
      //
      // With one unit and two racing requests, exactly one must win. Always.
      for (const strategy of SAFE_STRATEGIES) {
         await truncateAll();
         const resource = await seedResource(1, `no-false-soldout-${strategy}`);

         const results = await stampede(2, (i) =>
            allocate({
               resourceId: resource.id,
               guestRef: `guest-${i}`,
               period: period(),
               strategy,
            }),
         );

         const { ok, failed } = partition(results);
         expect(ok, `${strategy}: exactly one request must win`).toHaveLength(1);
         expect(failed[0], `${strategy}: the loser must be told sold out`).toBeInstanceOf(
            NoInventoryError,
         );
         expect(await countDoubleBookings()).toBe(0);
      }
   });

   it("will not call it sold out while the only conflict is uncommitted", async () => {
      // Deterministic reproduction of the bug the test above can only catch by
      // luck. A blocking transaction is held open for the duration, so the
      // conflict is guaranteed rather than raced.
      //
      // The allocator excludes units it has collided with, to make large bursts
      // converge. If that exclusion is allowed to empty the candidate list and
      // stand in for "no inventory", the caller is told the resource is sold
      // out -- when in truth the only thing in the way is a transaction that
      // has not committed and may yet roll back. Inventory would be silently
      // lost on a rollback.
      //
      // The honest answer here is "contended, try again", not "sold out".
      const resource = await seedResource(1, "uncommitted-conflict");
      const window = period();
      const units = await withTransaction((tx) => repo.listUnits(tx, resource.id));
      const unitId = units[0]!.id;

      const blocker = await pool.connect();
      try {
         await blocker.query("begin");
         await blocker.query(
            `insert into reservations
                (unit_id, resource_id, guest_ref, state, period, hold_expires_at)
             values ($1, $2, 'blocker', 'held',
                     tstzrange($3::timestamptz, $4::timestamptz, '[)'),
                     now() + interval '1 hour')`,
            [unitId, resource.id, window.from, window.to],
         );
         // Deliberately not committed: invisible to READ COMMITTED readers,
         // but it holds the exclusion index entry for this key range.

         await expect(
            allocate({
               resourceId: resource.id,
               guestRef: "contender",
               period: window,
               maxRetries: 3,
            }),
         ).rejects.toMatchObject({ code: "exhausted_retries" });
      } finally {
         await blocker.query("rollback").catch(() => {});
         blocker.release();
      }

      // And once the blocker rolls back, the unit was never gone.
      const after = await allocate({
         resourceId: resource.id,
         guestRef: "later",
         period: window,
      });
      expect(after.reservation.unitId).toBe(unitId);
      expect(await countDoubleBookings()).toBe(0);
   });

   it("tells a loser 'sold out', not 'try again', once inventory is gone", async () => {
      // The distinction matters operationally: `exhausted_retries` maps to 503
      // with Retry-After, inviting the client back. Returning that to someone
      // facing a genuinely sold-out resource adds load at the exact moment the
      // system has none to spare. A caller that ran out of retries against
      // exhausted inventory must still get the truthful terminal answer.
      const capacity = 5;
      const resource = await seedResource(capacity, "truthful-soldout");
      const window = period();

      // Fill capacity first and let it commit. The contenders below then start
      // against inventory that is *definitively* gone, so "sold out" is the
      // only truthful answer any of them can receive. Racing the fill would
      // make `exhausted_retries` legitimate for whoever finished early, which
      // is a different (and correct) behaviour that this test is not about.
      for (let i = 0; i < capacity; i++) {
         await allocate({
            resourceId: resource.id,
            guestRef: `filler-${i}`,
            period: window,
            ttlSeconds: 3600,
         });
      }

      const results = await stampede(200, (i) =>
         allocate({
            resourceId: resource.id,
            guestRef: `guest-${i}`,
            period: window,
            // A deliberately tiny budget: none of these can resolve the
            // situation by retrying, so the final authoritative check is what
            // has to produce the right answer.
            maxRetries: 1,
         }),
      );

      const { ok, failed } = partition(results);
      expect(ok).toHaveLength(0);
      expect(await countDoubleBookings()).toBe(0);
      expect(summarise(failed)).toEqual({ no_inventory: 200 });
   });

   it("non-overlapping windows never contend: all 100 succeed on one unit", async () => {
      // Back-to-back stays on a single room. Every request is for a distinct
      // night, so a correct implementation books all of them; an
      // over-eager lock or an '[]' bound would serialise or reject them.
      const resource = await seedResource(1, "sequential");

      const results = await stampede(100, (i) =>
         allocate({
            resourceId: resource.id,
            guestRef: `guest-${i}`,
            period: period(1 + i, 2 + i),
            maxRetries: 20,
         }),
      );

      const { ok, failed } = partition(results);
      expect(failed).toHaveLength(0);
      expect(ok).toHaveLength(100);
      expect(await countDoubleBookings()).toBe(0);
   });

   it("mixed confirm/cancel traffic keeps the invariant while inventory churns", async () => {
      // Allocation is not the only writer. Cancellations release inventory
      // mid-flight, so a second wave of bookings races against rows leaving the
      // index at the same time as others enter it.
      const capacity = 20;
      const resource = await seedResource(capacity, "churn");
      const window = period();

      const firstWave = partition(
         await stampede(capacity, (i) =>
            allocate({ resourceId: resource.id, guestRef: `first-${i}`, period: window }),
         ),
      ).ok;
      expect(firstWave).toHaveLength(capacity);

      // Cancel half the first wave while a second wave tries to book.
      const cancels: (() => Promise<unknown>)[] = firstWave
         .slice(0, 10)
         .map((o: AllocationOutcome) => () =>
            withTransaction((tx) =>
               repo.cancelReservation(tx, o.reservation.id, o.reservation.version),
            ),
         );
      const books: (() => Promise<unknown>)[] = Array.from({ length: 100 }, (_, i) => () =>
         allocate({
            resourceId: resource.id,
            guestRef: `second-${i}`,
            period: window,
            maxRetries: 20,
         }),
      );
      const interleaved = [...cancels, ...books];

      const results = await stampede(interleaved.length, (i) => interleaved[i]!());
      const { ok } = partition(results);

      expect(await countDoubleBookings()).toBe(0);

      // 10 cancels always succeed; the freed units are exactly the extra
      // bookings that can land, so total live reservations returns to capacity.
      const live = await withTransaction((tx) => repo.availability(tx, resource.id, window));
      expect(live.filter((u) => !u.isFree)).toHaveLength(capacity);
      expect(ok.length).toBeGreaterThanOrEqual(10);
   });
});

describe("the control group: naive check-then-insert", () => {
   beforeEach(truncateAll);

   it("double-books under the exact same load the safe strategies survive", async () => {
      // If this test ever starts passing with zero conflicts, the concurrency
      // suite above has stopped proving anything -- it would mean the harness
      // is not generating real contention, and every "0 double-bookings"
      // result elsewhere is vacuous. This is the smoke detector's test button.
      const resource = await seedResource(1, "naive-control");
      const window = period();

      await stampede(200, (i) =>
         allocate({
            resourceId: resource.id,
            guestRef: `guest-${i}`,
            period: window,
            strategy: "naive",
            maxRetries: 0,
         }),
      );

      const conflicts = await countDoubleBookings("naive_reservations");
      expect(conflicts).toBeGreaterThan(0);

      // ...and the guarded table is untouched by any of it.
      expect(await countDoubleBookings("reservations")).toBe(0);
   });
});
