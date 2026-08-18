import { existsSync } from "node:fs";
import { defineConfig } from "vitest/config";

// Vitest does not read .env the way `node --env-file` does, so load it here.
// Tests run against the same Postgres the service uses; CI can instead export
// DATABASE_URL directly and skip the file.
if (existsSync(".env")) process.loadEnvFile(".env");

export default defineConfig({
   test: {
      // Every suite talks to the same Postgres instance and truncates between
      // cases, so files must not run concurrently or they would clear each
      // other's fixtures mid-test.
      fileParallelism: false,
      // The concurrency suites deliberately saturate the pool; the default 5s
      // is not enough headroom for a few thousand contended transactions.
      testTimeout: 180_000,
      hookTimeout: 60_000,
      env: { LOG_LEVEL: "silent" },
      reporters: ["verbose"],
   },
});
