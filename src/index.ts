/** Process entry point: migrate, serve, reap, and shut down cleanly. */
import { config } from "./config.ts";
import { closePool } from "./db/pool.ts";
import { migrate } from "./db/migrate.ts";
import { buildServer } from "./http/server.ts";
import { startReaper } from "./services/reaper.ts";

const app = buildServer();

// Running migrations at boot keeps `docker compose up && npm start` a
// single-command setup. A real deployment would run this as a separate step so
// N replicas do not race; the advisory-lock-free runner here is fine for one.
await migrate((msg) => app.log.info(msg));

const reaper = startReaper(config.reaperIntervalMs, (msg) => app.log.debug(msg));

await app.listen({ port: config.port, host: config.host });
app.log.info(
   `reservation-service listening on ${config.host}:${config.port} ` +
      `(strategy=${config.allocationStrategy}, hold TTL=${config.holdTtlSeconds}s)`,
);

async function shutdown(signal: string): Promise<void> {
   app.log.info(`${signal} received, shutting down`);
   reaper?.stop();
   // Close the server first so in-flight requests finish before the pool goes.
   await app.close();
   await closePool();
   process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
