/**
 * Optimistic locking on state transitions.
 *
 * The exclusion constraint stops two reservations occupying one unit. It says
 * nothing about two requests transitioning the *same* reservation at once --
 * confirm racing cancel, or a double-submitted confirm. That is what the
 * `version` column is for: every transition asserts the version it read.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { closePool, withTransaction } from "../src/db/pool.ts";
import { VersionMismatchError } from "../src/domain/errors.ts";
import * as repo from "../src/repo/reservations.ts";
import { allocate } from "../src/services/allocator.ts";
import * as bookings from "../src/services/bookings.ts";
import {
   countDoubleBookings,
   ensureSchema,
   partition,
   period,
   seedResource,
   stampede,
   truncateAll,
} from "./helpers.ts";

beforeAll(ensureSchema);
afterAll(closePool);

async function freshHold() {
   const resource = await seedResource(1, `opt-${Math.random().toString(36).slice(2, 8)}`);
   const outcome = await allocate({
      resourceId: resource.id,
      guestRef: "guest",
      period: period(),
   });
   return outcome.reservation;
}

describe("optimistic locking", () => {
   beforeEach(truncateAll);

   it("confirming with the version you read succeeds and bumps it", async () => {
      const hold = await freshHold();
      expect(hold.version).toBe(1);

      const confirmed = await bookings.confirmHold(hold.id, hold.version);
      expect(confirmed.state).toBe("confirmed");
      expect(confirmed.version).toBe(2);
      // Confirming clears the deadline: a confirmed booking is not on a timer.
      expect(confirmed.holdExpiresAt).toBeNull();
   });

   it("confirming twice with the same version fails the second time", async () => {
      const hold = await freshHold();
      await bookings.confirmHold(hold.id, hold.version);

      await expect(bookings.confirmHold(hold.id, hold.version)).rejects.toBeInstanceOf(
         VersionMismatchError,
      );
   });

   it("reports the version it actually found, so the client can re-read", async () => {
      const hold = await freshHold();
      await bookings.confirmHold(hold.id, hold.version);

      await expect(bookings.confirmHold(hold.id, hold.version)).rejects.toMatchObject({
         code: "version_mismatch",
         detail: { expectedVersion: 1, actualVersion: 2, currentState: "confirmed" },
      });
   });

   it("50 concurrent confirms of one hold: exactly one wins", async () => {
      // The classic double-submit. Without the version predicate every one of
      // these would report success and the audit log would record 50 confirms.
      const hold = await freshHold();

      const results = await stampede(50, () => bookings.confirmHold(hold.id, hold.version));
      const { ok, failed } = partition(results);

      expect(ok).toHaveLength(1);
      expect(failed).toHaveLength(49);
      expect(failed.every((e) => e instanceof VersionMismatchError)).toBe(true);

      const final = await bookings.getReservation(hold.id);
      expect(final.state).toBe("confirmed");
      expect(final.version).toBe(2);
   });

   it("confirm racing cancel: one wins and the loser is told which", async () => {
      const hold = await freshHold();

      const results = await stampede(2, (i) =>
         i === 0
            ? bookings.confirmHold(hold.id, hold.version)
            : bookings.cancelReservation(hold.id, hold.version),
      );
      const { ok, failed } = partition(results);

      expect(ok).toHaveLength(1);
      expect(failed).toHaveLength(1);

      const final = await bookings.getReservation(hold.id);
      expect(["confirmed", "cancelled"]).toContain(final.state);
      expect(final.version).toBe(2);
   });

   it("cancelling releases the unit for a new booking", async () => {
      const resource = await seedResource(1, "release");
      const window = period();
      const first = await allocate({ resourceId: resource.id, guestRef: "a", period: window });

      // Sold out while the first hold stands.
      await expect(
         allocate({ resourceId: resource.id, guestRef: "b", period: window }),
      ).rejects.toMatchObject({ code: "no_inventory" });

      await bookings.cancelReservation(first.reservation.id, first.reservation.version);

      const second = await allocate({ resourceId: resource.id, guestRef: "b", period: window });
      expect(second.reservation.unitId).toBe(first.reservation.unitId);
      expect(await countDoubleBookings()).toBe(0);
   });

   it("records every transition in the audit log, in order", async () => {
      const hold = await freshHold();
      const confirmed = await bookings.confirmHold(hold.id, hold.version);
      await bookings.cancelReservation(confirmed.id, confirmed.version);

      const { rows } = await withTransaction((tx) =>
         tx.query<{ from_state: string | null; to_state: string }>(
            `select from_state::text, to_state::text
             from reservation_events where reservation_id = $1 order by id`,
            [hold.id],
         ),
      );

      expect(rows).toEqual([
         { from_state: null, to_state: "held" },
         { from_state: "held", to_state: "confirmed" },
         { from_state: "confirmed", to_state: "cancelled" },
      ]);
   });

   it("cannot confirm a reservation that was already cancelled", async () => {
      const hold = await freshHold();
      const cancelled = await bookings.cancelReservation(hold.id, hold.version);

      // Even with the *correct* current version, the state guard rejects it:
      // 'cancelled' is not in the set of states a confirm may start from.
      await expect(
         bookings.confirmHold(cancelled.id, cancelled.version),
      ).rejects.toBeInstanceOf(VersionMismatchError);

      const final = await bookings.getReservation(hold.id);
      expect(final.state).toBe("cancelled");
   });

   it("cannot cancel a reservation that does not exist", async () => {
      await expect(
         withTransaction((tx) =>
            repo.cancelReservation(tx, "00000000-0000-0000-0000-000000000000", 1),
         ),
      ).rejects.toMatchObject({ code: "not_found" });
   });
});
