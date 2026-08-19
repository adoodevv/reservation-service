/**
 * Allocation: turning "give me any free unit of resource X for these dates"
 * into exactly one reservation row, under arbitrary concurrency.
 *
 * Four strategies are implemented against the same interface so they can be run
 * head-to-head by bench/loadtest.ts. Three are correct and differ only in cost;
 * the fourth is wrong on purpose.
 *
 *   optimistic    READ COMMITTED. Pick a free unit, insert, and let the
 *                 exclusion constraint referee. Retry on 23P01. No locks held
 *                 across round trips, so throughput is highest and the cost of
 *                 contention is paid only by requests that actually collide.
 *
 *   serializable  SERIALIZABLE. Same body, but Postgres also guarantees the
 *                 read ("which units are free") was consistent with some serial
 *                 order. Retry on 40001/40P01. Note that the *safety* property
 *                 does not come from this isolation level -- the constraint
 *                 already provides it. What SERIALIZABLE buys is that a
 *                 "sold out" answer is truthful rather than merely current.
 *
 *   pessimistic   READ COMMITTED plus a per-resource advisory lock, which
 *                 serialises allocation for that resource. Zero conflicts by
 *                 construction and therefore zero retries, at the price of
 *                 turning concurrent bookings for one resource into a queue.
 *
 *   naive         The bug. Check-then-insert under READ COMMITTED against a
 *                 table with no exclusion constraint -- what a booking service
 *                 looks like before anyone has thought about races. Exists so
 *                 the load test can demonstrate it detects real double-bookings
 *                 rather than merely failing to find any.
 */
import { config } from "../config.ts";
import type { AllocationStrategy } from "../config.ts";
import type { Executor, IsolationLevel } from "../db/pool.ts";
import { withTransaction } from "../db/pool.ts";
import {
   ExhaustedRetriesError,
   NoInventoryError,
   isExclusionViolation,
   isLockTimeout,
   isOverloadError,
   isRetryableTransactionError,
} from "../domain/errors.ts";
import type { AllocationOutcome, Period, Reservation } from "../domain/types.ts";
import { increment, observe } from "../metrics.ts";
import type { CandidateOrder } from "../repo/reservations.ts";
import * as repo from "../repo/reservations.ts";

export interface AllocateRequest {
   resourceId: string;
   guestRef: string;
   period: Period;
   ttlSeconds?: number;
   /** Overrides the process default. The load harness sweeps this. */
   strategy?: AllocationStrategy;
   candidateOrder?: CandidateOrder;
   maxRetries?: number;
}

// Re-exported so callers can name a strategy without importing the config module.
export type { AllocationStrategy } from "../config.ts";

/** Per-attempt bookkeeping threaded through a single allocate() call. */
interface AttemptTally {
   attempts: number;
   exclusionConflicts: number;
   serializationFailures: number;
   reclaimedHolds: number;
   /** Unit the in-flight attempt targeted, so a conflict knows what to exclude. */
   lastUnitId?: string;
}

/**
 * Full-jitter exponential backoff, with a ceiling that depends on *why* we lost.
 *
 * Jitter is not decoration. Without it, N transactions that collide at the same
 * instant all sleep the same duration and collide again on wake -- the retry
 * loop converts one thundering herd into a periodic one. Randomising across the
 * whole window spreads them out.
 *
 * The two failure modes need very different ceilings, which measurement made
 * obvious:
 *
 *   23P01 (exclusion conflict) resolves the moment the winning transaction
 *   commits, and the loser then picks a *different* unit. Contention is already
 *   spread across free inventory, so a tight ceiling keeps latency low.
 *
 *   40001 (serialization failure) arrives in herds -- SSI aborts whole batches
 *   of transactions that conflicted on the same index pages. Retrying them
 *   quickly just reforms the herd, so the ceiling is an order of magnitude
 *   larger. With a 50ms cap this path spent 249s on a workload the optimistic
 *   strategy finished in 1.3s.
 */
const CONFLICT_BACKOFF_CEILING_MS = 25;
const SERIALIZATION_BACKOFF_CEILING_MS = 400;

/**
 * How long an attempt waits to acquire a lock before giving up and retrying.
 *
 * Inserting into the exclusion-constrained index blocks until whichever
 * transaction holds the conflicting key range commits or aborts. Under READ
 * COMMITTED the candidate SELECT cannot see those uncommitted rows, so during a
 * burst many requests pick a unit that *looks* free and then queue behind the
 * transaction actually taking it.
 *
 * Waiting is the wrong response: the useful information ("that unit is going")
 * is already known the moment we block, and there are other free units to try.
 * So the timeout is deliberately short -- long enough to ride out an ordinary
 * commit, short enough that a losing request bails and re-picks rather than
 * holding a pooled connection idle.
 *
 * Measured on the 3,000-request / 200-unit scenario: at 750ms the herd spent
 * its whole retry budget queueing and the run took 272s with widespread pool
 * timeouts. At 150ms the same workload settles in seconds.
 */
const LOCK_TIMEOUT_MS = 150;

function backoffMs(attempt: number, ceiling: number): number {
   return Math.random() * Math.min(ceiling, 2 ** attempt * 5);
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * One transaction: reclaim anything stale, find a free unit, take it.
 *
 * Returns null when no unit is free -- a genuine sell-out, distinct from
 * throwing, which means we raced and should try again.
 */
async function attemptAllocation(
   tx: Executor,
   request: AllocateRequest,
   tally: AttemptTally,
   order: CandidateOrder,
   attemptIndex: number,
   collided: Set<string>,
): Promise<Reservation | null> {
   let candidates = await repo.findFreeUnits(
      tx,
      request.resourceId,
      request.period,
      order,
      [...collided],
   );

   // Running out of *non-excluded* candidates says nothing about inventory.
   // Exclusions are a convergence heuristic built from conflicts, and a
   // conflict can be transient: the transaction that beat us may since have
   // rolled back, or its hold may have expired. Concluding "sold out" from an
   // empty filtered list would report a sell-out that never happened -- with
   // one unit and two contending requests, it loses the booking entirely.
   //
   // So when the filter empties the list, drop it and look at reality.
   if (candidates.length === 0 && collided.size > 0) {
      collided.clear();
      candidates = await repo.findFreeUnits(tx, request.resourceId, request.period, order);
   }

   // Reclaiming expired holds only matters when we are otherwise about to
   // report "sold out", so it runs on the slow path only. That ordering is not
   // a micro-optimisation: reclaim is a range scan plus an UPDATE, and doing it
   // on every attempt widens the transaction's read/write footprint enormously.
   // Under SERIALIZABLE that footprint is exactly what SSI takes predicate
   // locks on, and running it eagerly turned a 1.3s workload into a 249s one.
   //
   // Correctness is unaffected: we never answer "sold out" without first
   // reclaiming, so an abandoned checkout still releases inventory to the very
   // next request that needs it rather than waiting for the reaper's next tick.
   if (candidates.length === 0) {
      const reclaimed = await repo.reclaimExpiredHolds(tx, request.resourceId, request.period);
      tally.reclaimedHolds += reclaimed;
      if (reclaimed === 0) return null;

      candidates = await repo.findFreeUnits(tx, request.resourceId, request.period, order);
      if (candidates.length === 0) return null;
   }

   // Rotate the starting offset by attempt number so a retry does not
   // immediately re-pick the unit that just rejected us.
   const unitId = candidates[attemptIndex % candidates.length]!;
   tally.lastUnitId = unitId;

   return repo.insertHold(tx, {
      unitId,
      resourceId: request.resourceId,
      guestRef: request.guestRef,
      period: request.period,
      ttlSeconds: request.ttlSeconds ?? config.holdTtlSeconds,
   });
}

/** The `naive` path. Same shape, no constraint, no retry, no safety. */
async function attemptNaiveAllocation(
   tx: Executor,
   request: AllocateRequest,
): Promise<Reservation | null> {
   // CHECK: which units look free right now?
   const { rows: free } = await tx.query<{ id: string }>(
      `select u.id
       from resource_units u
       where u.resource_id = $1
         and not exists (
            select 1 from naive_reservations r
            where r.unit_id = u.id
              and r.state in ('held', 'confirmed')
              and r.period && tstzrange($2::timestamptz, $3::timestamptz, '[)')
         )
       order by u.label
       limit 1`,
      [request.resourceId, request.period.from, request.period.to],
   );
   const unitId = free[0]?.id;
   if (!unitId) return null;

   // ...and THEN insert. Under READ COMMITTED every concurrent transaction
   // reads the same pre-insert snapshot, so they all see the same unit as free
   // and they all take it. Nothing stops them. This is the bug.
   const { rows } = await tx.query<{
      id: string;
      unit_id: string;
      resource_id: string;
      guest_ref: string;
      state: "held";
      period_from: Date;
      period_to: Date;
      hold_expires_at: Date | null;
      version: number;
      created_at: Date;
      updated_at: Date;
   }>(
      `insert into naive_reservations
          (unit_id, resource_id, guest_ref, state, period, hold_expires_at)
       values ($1, $2, $3, 'held',
               tstzrange($4::timestamptz, $5::timestamptz, '[)'),
               now() + make_interval(secs => $6::double precision))
       returning id, unit_id, resource_id, guest_ref, state,
                 lower(period) as period_from, upper(period) as period_to,
                 hold_expires_at, version, created_at, updated_at`,
      [
         unitId,
         request.resourceId,
         request.guestRef,
         request.period.from,
         request.period.to,
         request.ttlSeconds ?? config.holdTtlSeconds,
      ],
   );
   const row = rows[0]!;
   return {
      id: row.id,
      unitId: row.unit_id,
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

/**
 * Place a hold on any free unit of a resource, retrying through contention.
 *
 * Throws NoInventoryError when the resource is genuinely sold out for the
 * window, and ExhaustedRetriesError when free inventory existed but every
 * attempt lost its race. Callers should treat the first as terminal and the
 * second as "try again shortly" -- the HTTP layer maps them to 409 and 503.
 */
export async function allocate(request: AllocateRequest): Promise<AllocationOutcome> {
   const strategy = request.strategy ?? config.allocationStrategy;
   const maxRetries = request.maxRetries ?? config.allocationMaxRetries;
   const order = request.candidateOrder ?? "random";
   const startedAt = performance.now();

   const tally: AttemptTally = {
      attempts: 0,
      exclusionConflicts: 0,
      serializationFailures: 0,
      reclaimedHolds: 0,
   };

   // Units this call has already lost a race for. A conflict proves the unit is
   // taken by a transaction READ COMMITTED cannot show us, so re-offering it
   // burns a retry on a known-dead candidate. Excluding them makes the loop
   // converge -- each attempt narrows the search -- instead of resampling
   // contested inventory at random until the budget runs out.
   const collided = new Set<string>();

   const isolation: IsolationLevel =
      strategy === "serializable" ? "serializable" : "read committed";

   for (let attempt = 0; attempt <= maxRetries; attempt++) {
      tally.attempts++;
      increment("allocation.attempts");

      try {
         const reservation = await withTransaction(async (tx) => {
            if (strategy === "naive") {
               return attemptNaiveAllocation(tx, request);
            }
            if (strategy === "pessimistic") {
               // Taken before any read, so the read cannot be invalidated by a
               // writer we then race. Lock ordering is trivially consistent
               // because there is exactly one lock per allocation.
               await repo.lockResourceForAllocation(tx, request.resourceId);
            }
            return attemptAllocation(tx, request, tally, order, attempt, collided);
         }, isolation, LOCK_TIMEOUT_MS);

         if (reservation === null) {
            increment("allocation.no_inventory");
            observe("allocation.latency_ms", performance.now() - startedAt);
            throw new NoInventoryError({
               resourceId: request.resourceId,
               from: request.period.from.toISOString(),
               to: request.period.to.toISOString(),
            });
         }

         increment("allocation.success");
         if (tally.attempts > 1) increment("allocation.success_after_retry");
         observe("allocation.latency_ms", performance.now() - startedAt);

         return {
            reservation,
            attempts: tally.attempts,
            exclusionConflicts: tally.exclusionConflicts,
            serializationFailures: tally.serializationFailures,
            reclaimedHolds: tally.reclaimedHolds,
         };
      } catch (err) {
         // A sold-out resource is an answer, not a race. Do not retry it.
         if (err instanceof NoInventoryError) throw err;

         let ceiling: number;
         if (isLockTimeout(err)) {
            // We queued behind a conflicting transaction and gave up waiting.
            // Nothing was written, so this is the cheapest possible retry --
            // and on the next pass we pick a different candidate unit.
            tally.exclusionConflicts++;
            increment("allocation.lock_timeout");
            // Timing out on this key range means someone is mid-insert on it.
            if (tally.lastUnitId) collided.add(tally.lastUnitId);
            ceiling = CONFLICT_BACKOFF_CEILING_MS;
         } else if (isExclusionViolation(err)) {
            // Another transaction inserted an overlapping row on the unit we
            // chose, between our SELECT and our INSERT. This is the constraint
            // preventing a double-booking, observed from the losing side.
            tally.exclusionConflicts++;
            increment("allocation.exclusion_conflict");
            // The constraint rejected us: that unit is definitively taken.
            if (tally.lastUnitId) collided.add(tally.lastUnitId);
            ceiling = CONFLICT_BACKOFF_CEILING_MS;
         } else if (isRetryableTransactionError(err)) {
            tally.serializationFailures++;
            increment("allocation.serialization_failure");
            // Deliberately no exclusion. A serialization failure means the
            // transactions could not be ordered -- it is not evidence about
            // the unit, which may well still be free.
            ceiling = SERIALIZATION_BACKOFF_CEILING_MS;
         } else if (isOverloadError(err)) {
            // statement_timeout or predicate-lock exhaustion: the server ran
            // out of time or memory for our statement. Nothing was written, so
            // this is safe to retry -- and it must be handled here, because an
            // uncaught overload error reaches the client as a 500 when the
            // truthful answer is "sold out" or "overloaded, try again".
            //
            // Backed off hard: the system is already past capacity, and a tight
            // retry is precisely what it cannot absorb.
            tally.serializationFailures++;
            increment("allocation.overload");
            ceiling = SERIALIZATION_BACKOFF_CEILING_MS;
         } else {
            throw err;
         }

         if (attempt < maxRetries) await sleep(backoffMs(attempt, ceiling));
      }
   }

   // The retry budget is gone, but "I kept losing races" and "there is nothing
   // left to win" are different answers deserving different status codes, and
   // the loop above cannot tell them apart: it works from an exclusion set and
   // from snapshots that could not see uncommitted winners.
   //
   // So ask once, authoritatively, with no exclusions and after reclaiming any
   // expired holds. By now the transactions we lost to have committed, so this
   // sees the true state. Returning 503 "retry" to a caller facing a sold-out
   // resource is the worst possible answer -- it invites more load at exactly
   // the moment the system has none to give.
   let freeUnits: number;
   try {
      freeUnits = await withTransaction(async (tx) => {
         await repo.reclaimExpiredHolds(tx, request.resourceId, request.period);
         return repo.countFreeUnits(tx, request.resourceId, request.period);
      });
   } catch (err) {
      // If even this check cannot run, the server is saturated. Say so, rather
      // than guessing at inventory we failed to read.
      if (!isOverloadError(err)) throw err;
      increment("allocation.exhausted_retries");
      throw new ExhaustedRetriesError(tally.attempts, {
         resourceId: request.resourceId,
         reason: "server overloaded; inventory state could not be confirmed",
      });
   }

   observe("allocation.latency_ms", performance.now() - startedAt);

   if (freeUnits === 0) {
      increment("allocation.no_inventory");
      throw new NoInventoryError({
         resourceId: request.resourceId,
         from: request.period.from.toISOString(),
         to: request.period.to.toISOString(),
         contendedAttempts: tally.attempts,
      });
   }

   increment("allocation.exhausted_retries");
   throw new ExhaustedRetriesError(tally.attempts, {
      resourceId: request.resourceId,
      exclusionConflicts: tally.exclusionConflicts,
      serializationFailures: tally.serializationFailures,
      freeUnits,
   });
}
