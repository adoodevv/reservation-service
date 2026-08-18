/**
 * Hold expiry: abandoned checkouts must release inventory.
 *
 * The subtlety this suite exists for: the exclusion constraint's predicate is
 * `state IN ('held','confirmed')` and cannot mention `now()`, because index
 * predicates must be IMMUTABLE. A hold therefore does not stop occupying
 * inventory when its deadline passes -- it stops when something *writes*
 * 'expired' to it. Two mechanisms do that, and both are tested here:
 *
 *   - the allocator, inline, before it will ever answer "sold out";
 *   - the reaper, on a timer, so availability reads are honest without
 *     someone first attempting a booking.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { closePool, pool, withTransaction } from "../src/db/pool.ts";
import * as repo from "../src/repo/reservations.ts";
import { allocate } from "../src/services/allocator.ts";
import * as bookings from "../src/services/bookings.ts";
import { reapOnce, startReaper } from "../src/services/reaper.ts";
import {
   countByState,
   countDoubleBookings,
   ensureSchema,
   partition,
   period,
   seedResource,
   stampede,
   truncateAll,
} from "./helpers.ts";

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Drives the deadline into the past without waiting on the wall clock. */
async function expireDeadline(reservationId: string): Promise<void> {
   await pool.query(
      "update reservations set hold_expires_at = now() - interval '1 second' where id = $1",
      [reservationId],
   );
}

beforeAll(ensureSchema);
afterAll(closePool);

describe("hold expiry", () => {
   beforeEach(truncateAll);

   it("an unexpired hold still blocks the unit", async () => {
      const resource = await seedResource(1, "ttl-blocks");
      await allocate({
         resourceId: resource.id,
         guestRef: "abandoner",
         period: period(),
         ttlSeconds: 3600,
      });

      await expect(
         allocate({ resourceId: resource.id, guestRef: "next", period: period() }),
      ).rejects.toMatchObject({ code: "no_inventory" });
   });

   it("the allocator reclaims an expired hold rather than reporting sold out", async () => {
      const resource = await seedResource(1, "ttl-reclaim");
      const abandoned = await allocate({
         resourceId: resource.id,
         guestRef: "abandoner",
         period: period(),
         ttlSeconds: 3600,
      });
      await expireDeadline(abandoned.reservation.id);

      // No reaper has run. The row is still state='held' and still in the
      // exclusion index, so this only succeeds if the allocator expires it
      // inline before concluding there is no inventory.
      const next = await allocate({
         resourceId: resource.id,
         guestRef: "next",
         period: period(),
      });

      expect(next.reclaimedHolds).toBe(1);
      expect(next.reservation.unitId).toBe(abandoned.reservation.unitId);
      expect(await countDoubleBookings()).toBe(0);
      expect(await countByState()).toEqual({ expired: 1, held: 1 });
   });

   it("expiry by real elapsed time, not just by clock manipulation", async () => {
      const resource = await seedResource(1, "ttl-real");
      await allocate({
         resourceId: resource.id,
         guestRef: "abandoner",
         period: period(),
         ttlSeconds: 0.2,
      });

      await expect(
         allocate({ resourceId: resource.id, guestRef: "early", period: period() }),
      ).rejects.toMatchObject({ code: "no_inventory" });

      await wait(300);

      const late = await allocate({
         resourceId: resource.id,
         guestRef: "late",
         period: period(),
      });
      expect(late.reservation.state).toBe("held");
   });

   it("a confirmed reservation never expires", async () => {
      const resource = await seedResource(1, "ttl-confirmed");
      const held = await allocate({
         resourceId: resource.id,
         guestRef: "guest",
         period: period(),
         ttlSeconds: 0.1,
      });
      const confirmed = await bookings.confirmHold(held.reservation.id, held.reservation.version);
      expect(confirmed.holdExpiresAt).toBeNull();

      await wait(250);
      await reapOnce();

      await expect(
         allocate({ resourceId: resource.id, guestRef: "next", period: period() }),
      ).rejects.toMatchObject({ code: "no_inventory" });
      expect((await countByState()).confirmed).toBe(1);
   });

   it("an expired hold cannot be confirmed", async () => {
      const resource = await seedResource(1, "ttl-confirm-race");
      const held = await allocate({
         resourceId: resource.id,
         guestRef: "guest",
         period: period(),
         ttlSeconds: 3600,
      });
      await expireDeadline(held.reservation.id);
      await reapOnce();

      // The reaper bumped the version and moved the row to 'expired', so the
      // stale version the client holds is rejected -- it cannot resurrect a
      // hold whose inventory may already have been resold.
      await expect(
         bookings.confirmHold(held.reservation.id, held.reservation.version),
      ).rejects.toMatchObject({ code: "version_mismatch" });
   });

   it("the reaper expires stale holds and logs each transition", async () => {
      const resource = await seedResource(5, "ttl-reaper");
      const holds = await Promise.all(
         Array.from({ length: 5 }, (_, i) =>
            allocate({
               resourceId: resource.id,
               guestRef: `guest-${i}`,
               period: period(),
               ttlSeconds: 3600,
            }),
         ),
      );
      for (const h of holds.slice(0, 3)) await expireDeadline(h.reservation.id);

      expect(await reapOnce()).toBe(3);
      expect(await countByState()).toEqual({ expired: 3, held: 2 });
      // A second sweep must be a no-op, not re-expire the same rows.
      expect(await reapOnce()).toBe(0);

      const { rows } = await pool.query<{ n: number }>(
         `select count(*)::int as n from reservation_events
          where to_state = 'expired' and detail->>'by' = 'reaper'`,
      );
      expect(rows[0]!.n).toBe(3);
   });

   it("the background reaper runs on its timer", async () => {
      const resource = await seedResource(1, "ttl-timer");
      const held = await allocate({
         resourceId: resource.id,
         guestRef: "guest",
         period: period(),
         ttlSeconds: 3600,
      });
      await expireDeadline(held.reservation.id);

      const reaper = startReaper(50, () => {});
      try {
         const deadline = Date.now() + 5000;
         while (Date.now() < deadline) {
            if ((await countByState()).expired === 1) break;
            await wait(25);
         }
      } finally {
         reaper?.stop();
      }

      expect((await countByState()).expired).toBe(1);
   });

   it("100 requests racing a batch of expiring holds still never double-book", async () => {
      // The hardest ordering in the service: rows leaving the exclusion index
      // (expiry) while other transactions race to insert into the space they
      // vacate. Every reclaim is itself a write that can lose a race.
      const capacity = 10;
      const resource = await seedResource(capacity, "ttl-stampede");
      const window = period();

      const holds = await Promise.all(
         Array.from({ length: capacity }, (_, i) =>
            allocate({
               resourceId: resource.id,
               guestRef: `stale-${i}`,
               period: window,
               ttlSeconds: 3600,
            }),
         ),
      );
      for (const h of holds) await expireDeadline(h.reservation.id);

      const results = await stampede(100, (i) =>
         allocate({
            resourceId: resource.id,
            guestRef: `fresh-${i}`,
            period: window,
            maxRetries: 20,
         }),
      );

      const { ok } = partition(results);
      expect(await countDoubleBookings()).toBe(0);
      // Exactly the freed capacity is resold: not less, and never more.
      expect(ok).toHaveLength(capacity);

      const live = await withTransaction((tx) => repo.availability(tx, resource.id, window));
      expect(live.filter((u) => !u.isFree)).toHaveLength(capacity);
      expect((await countByState()).expired).toBe(capacity);
   });
});
