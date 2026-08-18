/** Shared connection pool plus the transaction helpers everything else builds on. */
import pg from "pg";
import { config } from "../config.ts";

const { Pool, types } = pg;

// node-postgres hands back `numeric` and `bigint` as strings to avoid silent
// precision loss. The only bigints here are event ids and counts, all well
// inside Number.MAX_SAFE_INTEGER, so parse them for ergonomics.
types.setTypeParser(types.builtins.INT8, (v) => Number.parseInt(v, 10));

export type IsolationLevel = "read committed" | "repeatable read" | "serializable";

export const pool = new Pool({
   connectionString: config.databaseUrl,
   max: config.poolMax,
   idleTimeoutMillis: 30_000,
   // Under a deliberate stampede, requests queue for a connection by design.
   // A short timeout here would surface as a spurious error and hide the real
   // result, so allow a generous wait and let statement_timeout catch hangs.
   connectionTimeoutMillis: 30_000,
   // Nothing in the request path should legitimately run for a whole second;
   // a query that does is a lock wait we want to fail fast on rather than pile
   // connections up behind.
   statement_timeout: 10_000,
});

pool.on("error", (err) => {
   // An idle client erroring out (server restart, network blip) must not take
   // the process down -- the pool will transparently open a fresh connection.
   console.error("[pg] idle client error:", err.message);
});

export type Executor = pg.PoolClient;

/**
 * Runs `fn` inside a transaction at the requested isolation level, committing
 * on success and rolling back on any throw.
 *
 * This intentionally does NOT retry. Retry policy is a decision for the caller,
 * because "what counts as a retryable failure" differs per strategy: the
 * serializable allocator retries on 40001, the optimistic one on 23P01, and a
 * confirm/cancel retries on neither.
 */
export async function withTransaction<T>(
   fn: (tx: Executor) => Promise<T>,
   isolation: IsolationLevel = "read committed",
   lockTimeoutMs?: number,
): Promise<T> {
   const client = await pool.connect();
   try {
      // BEGIN and the lock timeout go in a single simple query rather than two
      // round trips. Round trips are the scarce resource here: under load,
      // pg_stat_activity shows backends sitting `idle in transaction` on
      // ClientRead -- Postgres waiting on the single-threaded Node client, not
      // the other way round. Every statement removed from the transaction path
      // is latency removed from every concurrent request.
      //
      // `lockTimeoutMs` is interpolated rather than bound because SET does not
      // accept parameters; the integer check below is what makes that safe.
      let begin = `begin isolation level ${isolation}`;
      if (lockTimeoutMs !== undefined) {
         if (!Number.isInteger(lockTimeoutMs) || lockTimeoutMs < 0) {
            throw new Error(`lockTimeoutMs must be a non-negative integer, got ${lockTimeoutMs}`);
         }
         // SET LOCAL is scoped to this transaction, so the pooled connection
         // returns to the pool with its normal settings.
         begin += `; set local lock_timeout = ${lockTimeoutMs}`;
      }
      await client.query(begin);
      const result = await fn(client);
      await client.query("commit");
      return result;
   } catch (err) {
      // Rollback can itself fail if the connection died mid-transaction; the
      // original error is the interesting one, so swallow this.
      await client.query("rollback").catch(() => {});
      throw err;
   } finally {
      client.release();
   }
}

export async function closePool(): Promise<void> {
   await pool.end();
}
