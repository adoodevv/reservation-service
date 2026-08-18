/**
 * Request-level orchestration: idempotency, allocation, and the optimistic-lock
 * state transitions that follow.
 */
import { createHash } from "node:crypto";
import { withTransaction } from "../db/pool.ts";
import { NotFoundError, ServiceError, isUniqueViolation } from "../domain/errors.ts";
import type { AllocationOutcome, Period, Reservation } from "../domain/types.ts";
import { increment } from "../metrics.ts";
import * as repo from "../repo/reservations.ts";
import type { AllocateRequest } from "./allocator.ts";
import { allocate } from "./allocator.ts";

function digest(payload: unknown): string {
   return createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}

/**
 * Place a hold, optionally under an idempotency key.
 *
 * The key is claimed in its own committed transaction *before* allocation runs.
 * That ordering is what makes it work: a duplicate request arriving while the
 * first is still allocating hits the unique violation and is told to wait,
 * rather than sailing past and consuming a second unit.
 */
export async function createHold(
   request: AllocateRequest,
   idempotencyKey?: string,
): Promise<{ outcome: AllocationOutcome; replayed: boolean }> {
   if (!idempotencyKey) {
      return { outcome: await allocate(request), replayed: false };
   }

   const requestDigest = digest({
      resourceId: request.resourceId,
      guestRef: request.guestRef,
      from: request.period.from.toISOString(),
      to: request.period.to.toISOString(),
   });

   // Step 1: claim the key.
   try {
      await withTransaction((tx) =>
         tx.query(
            "insert into idempotency_keys (key, request_digest) values ($1, $2)",
            [idempotencyKey, requestDigest],
         ),
      );
   } catch (err) {
      if (!isUniqueViolation(err)) throw err;
      return { outcome: await replay(idempotencyKey, requestDigest), replayed: true };
   }

   // Step 2: allocate. On failure the claim is released so the caller can retry
   // the same key -- an idempotency key must not permanently poison itself
   // because the first attempt hit a sold-out resource.
   let outcome: AllocationOutcome;
   try {
      outcome = await allocate(request);
   } catch (err) {
      await withTransaction((tx) =>
         tx.query("delete from idempotency_keys where key = $1", [idempotencyKey]),
      ).catch(() => {
         // Best effort. A stranded claim expires with the row-retention sweep;
         // surfacing the original allocation error matters more.
      });
      throw err;
   }

   // Step 3: record the result so a later duplicate replays it.
   await withTransaction((tx) =>
      tx.query(
         `update idempotency_keys
          set reservation_id = $2, response_body = $3::jsonb
          where key = $1`,
         [idempotencyKey, outcome.reservation.id, JSON.stringify(outcome.reservation)],
      ),
   );

   return { outcome, replayed: false };
}

async function replay(key: string, requestDigest: string): Promise<AllocationOutcome> {
   const { rows } = await withTransaction((tx) =>
      tx.query<{ request_digest: string; reservation_id: string | null }>(
         "select request_digest, reservation_id from idempotency_keys where key = $1",
         [key],
      ),
   );
   const row = rows[0];

   // The row was deleted between our failed insert and this read: a concurrent
   // attempt failed and released the claim. Telling the client to retry is
   // honest and safe.
   if (!row) {
      throw new ServiceError("conflict", "Idempotency key is being retried; try again", { key });
   }

   if (row.request_digest !== requestDigest) {
      throw new ServiceError(
         "invalid_request",
         "Idempotency key was already used with a different request body",
         { key },
      );
   }

   if (!row.reservation_id) {
      // Claimed but not yet resolved: the original request is still in flight.
      throw new ServiceError("conflict", "A request with this idempotency key is in flight", {
         key,
      });
   }

   const reservation = await withTransaction((tx) => repo.getReservation(tx, row.reservation_id!));
   if (!reservation) throw new NotFoundError("Reservation");

   increment("idempotency.replayed");
   return {
      reservation,
      attempts: 0,
      exclusionConflicts: 0,
      serializationFailures: 0,
      reclaimedHolds: 0,
   };
}

export function confirmHold(id: string, expectedVersion: number): Promise<Reservation> {
   return withTransaction(async (tx) => {
      const reservation = await repo.confirmHold(tx, id, expectedVersion);
      increment("hold.confirmed");
      return reservation;
   });
}

export function cancelReservation(
   id: string,
   expectedVersion: number,
): Promise<Reservation> {
   return withTransaction(async (tx) => {
      const reservation = await repo.cancelReservation(tx, id, expectedVersion);
      increment("hold.cancelled");
      return reservation;
   });
}

export async function getReservation(id: string): Promise<Reservation> {
   const reservation = await withTransaction((tx) => repo.getReservation(tx, id));
   if (!reservation) throw new NotFoundError("Reservation");
   return reservation;
}

export function getAvailability(resourceId: string, period: Period) {
   return withTransaction((tx) => repo.availability(tx, resourceId, period));
}
