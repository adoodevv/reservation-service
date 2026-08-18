/** Process configuration, read once at startup and validated eagerly. */

export type AllocationStrategy =
   | "optimistic"
   | "serializable"
   | "pessimistic"
   | "naive";

const STRATEGIES: readonly AllocationStrategy[] = [
   "optimistic",
   "serializable",
   "pessimistic",
   "naive",
];

function required(name: string): string {
   const value = process.env[name];
   if (!value) {
      throw new Error(
         `Missing required environment variable ${name}. ` +
            `Either copy .env.example to .env, or export it directly ` +
            `(the npm scripts load .env only if it exists, so both work).`,
      );
   }
   return value;
}

function int(name: string, fallback: number): number {
   const raw = process.env[name];
   if (raw === undefined || raw === "") return fallback;
   const parsed = Number.parseInt(raw, 10);
   if (!Number.isFinite(parsed)) {
      throw new Error(`Environment variable ${name} must be an integer, got "${raw}"`);
   }
   return parsed;
}

function strategy(): AllocationStrategy {
   const raw = (process.env.ALLOCATION_STRATEGY ?? "optimistic") as AllocationStrategy;
   if (!STRATEGIES.includes(raw)) {
      throw new Error(
         `ALLOCATION_STRATEGY must be one of ${STRATEGIES.join(" | ")}, got "${raw}"`,
      );
   }
   return raw;
}

export const config = {
   databaseUrl: required("DATABASE_URL"),
   port: int("PORT", 3000),
   host: process.env.HOST ?? "0.0.0.0",
   poolMax: int("PG_POOL_MAX", 32),
   holdTtlSeconds: int("HOLD_TTL_SECONDS", 120),
   reaperIntervalMs: int("REAPER_INTERVAL_MS", 5000),
   allocationStrategy: strategy(),
   allocationMaxRetries: int("ALLOCATION_MAX_RETRIES", 8),
} as const;
