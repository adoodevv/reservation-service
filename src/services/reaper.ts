/**
 * Background sweep that expires abandoned holds.
 *
 * The allocator already reclaims stale holds that stand in its own way, so this
 * is not on the critical path for correctness. It exists so that inventory
 * shows as available in GET /availability without someone first *trying* to
 * book it, and so that expiry work is bounded and amortised rather than
 * arriving all at once on whichever unlucky request hits a cold resource.
 */
import { config } from "../config.ts";
import { withTransaction } from "../db/pool.ts";
import { increment } from "../metrics.ts";
import * as repo from "../repo/reservations.ts";

/** Bounded per tick so one sweep cannot hold locks over an unbounded row set. */
const BATCH_SIZE = 500;

export async function reapOnce(): Promise<number> {
   let total = 0;
   // Drain in batches until a tick comes back short, so a large backlog clears
   // promptly instead of BATCH_SIZE rows per interval.
   for (;;) {
      const expired = await withTransaction((tx) => repo.reapExpiredHolds(tx, BATCH_SIZE));
      total += expired;
      if (expired < BATCH_SIZE) break;
   }
   if (total > 0) increment("reaper.holds_expired", total);
   return total;
}

export interface Reaper {
   stop(): void;
}

export function startReaper(
   intervalMs: number = config.reaperIntervalMs,
   log: (msg: string) => void = () => {},
): Reaper | null {
   if (intervalMs <= 0) return null;

   let running = false;
   const timer = setInterval(() => {
      // Skip rather than queue: if a sweep is still going, the next tick's work
      // is already covered by the one in flight.
      if (running) return;
      running = true;
      reapOnce()
         .then((n) => {
            if (n > 0) log(`reaper expired ${n} hold(s)`);
         })
         .catch((err: unknown) => {
            // A failed sweep is recoverable -- the next tick retries, and the
            // allocator reclaims anything urgent inline regardless.
            console.error("[reaper]", (err as Error).message);
            increment("reaper.errors");
         })
         .finally(() => {
            running = false;
         });
   }, intervalMs);

   // Do not keep the process alive purely to run the reaper.
   timer.unref();

   return { stop: () => clearInterval(timer) };
}
