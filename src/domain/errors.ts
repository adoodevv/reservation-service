/** Typed failures the HTTP layer maps to status codes. */

export type ErrorCode =
   | "no_inventory"
   | "conflict"
   | "not_found"
   | "invalid_request"
   | "version_mismatch"
   | "exhausted_retries";

export class ServiceError extends Error {
   readonly code: ErrorCode;
   readonly detail: Record<string, unknown>;

   constructor(code: ErrorCode, message: string, detail: Record<string, unknown> = {}) {
      super(message);
      this.name = "ServiceError";
      this.code = code;
      this.detail = detail;
   }
}

/** No unit of the requested resource is free for the whole window. */
export class NoInventoryError extends ServiceError {
   constructor(detail: Record<string, unknown> = {}) {
      super("no_inventory", "No unit is available for the requested period", detail);
   }
}

/** The caller's `version` no longer matches the stored row: someone else won. */
export class VersionMismatchError extends ServiceError {
   constructor(detail: Record<string, unknown> = {}) {
      super("version_mismatch", "Reservation was modified by another request", detail);
   }
}

export class NotFoundError extends ServiceError {
   constructor(what: string) {
      super("not_found", `${what} not found`);
   }
}

/**
 * Every candidate unit lost a race and the retry budget ran out. Distinct from
 * NoInventoryError: inventory may well exist, we just kept colliding. Surfaced
 * as 503 + Retry-After rather than 409, because retrying is the right move.
 */
export class ExhaustedRetriesError extends ServiceError {
   constructor(attempts: number, detail: Record<string, unknown> = {}) {
      super("exhausted_retries", `Gave up after ${attempts} contended attempts`, {
         attempts,
         ...detail,
      });
   }
}

// ---------------------------------------------------------------------------
// Postgres error classification
// ---------------------------------------------------------------------------

/** exclusion_violation -- our EXCLUDE constraint rejected an overlap. */
export const PG_EXCLUSION_VIOLATION = "23P01";
/** unique_violation -- e.g. a duplicate idempotency key. */
export const PG_UNIQUE_VIOLATION = "23505";
/** serialization_failure -- SERIALIZABLE could not order the transactions. */
export const PG_SERIALIZATION_FAILURE = "40001";
/** deadlock_detected -- two transactions grabbed locks in opposite orders. */
export const PG_DEADLOCK_DETECTED = "40P01";
/** lock_not_available -- our lock_timeout fired while waiting on a conflict. */
export const PG_LOCK_NOT_AVAILABLE = "55P03";
/** query_canceled -- statement_timeout fired. Not retried: it signals overload. */
export const PG_STATEMENT_TIMEOUT = "57014";
/**
 * out_of_memory -- in practice, exhaustion of the predicate-lock shared memory
 * that SERIALIZABLE uses to track SIREAD locks. Raising
 * max_pred_locks_per_transaction defers it; it is a real ceiling on how much
 * concurrency SSI can track, not a bug in the query.
 */
export const PG_OUT_OF_SHARED_MEMORY = "53200";

function sqlState(err: unknown): string | undefined {
   if (typeof err === "object" && err !== null && "code" in err) {
      const { code } = err as { code?: unknown };
      return typeof code === "string" ? code : undefined;
   }
   return undefined;
}

export function isExclusionViolation(err: unknown): boolean {
   return sqlState(err) === PG_EXCLUSION_VIOLATION;
}

export function isUniqueViolation(err: unknown): boolean {
   return sqlState(err) === PG_UNIQUE_VIOLATION;
}

/**
 * True for the two errors Postgres documents as "retry the whole transaction":
 * serialization failures and deadlocks. Both are expected under contention and
 * are not bugs -- they are the database doing its job.
 */
export function isRetryableTransactionError(err: unknown): boolean {
   const state = sqlState(err);
   return state === PG_SERIALIZATION_FAILURE || state === PG_DEADLOCK_DETECTED;
}

/**
 * True when we gave up waiting for a lock. Retryable, and cheap to retry: the
 * transaction did no work, it only queued.
 */
export function isLockTimeout(err: unknown): boolean {
   return sqlState(err) === PG_LOCK_NOT_AVAILABLE;
}

/**
 * True for failures that mean "the system is past its capacity right now"
 * rather than "this request is wrong". None of them can corrupt state -- the
 * transaction is gone -- so they are availability events, not safety events,
 * and the load report counts them separately from correctness failures.
 */
export function isLoadShedding(err: unknown): boolean {
   const state = sqlState(err);
   if (state === PG_STATEMENT_TIMEOUT || state === PG_OUT_OF_SHARED_MEMORY) return true;
   // node-postgres pool exhaustion arrives as a plain Error with no SQLSTATE.
   return (
      state === undefined &&
      err instanceof Error &&
      /timeout exceeded when trying to connect/i.test(err.message)
   );
}

/** Label for reporting: the SQLSTATE, or a synthetic name for client-side failures. */
export function failureLabel(err: unknown): string {
   const state = sqlState(err);
   if (state) return state;
   if (err instanceof Error && /timeout exceeded when trying to connect/i.test(err.message)) {
      return "pool_timeout";
   }
   return "unknown";
}

