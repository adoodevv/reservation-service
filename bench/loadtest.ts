/**
 * The load harness.
 *
 * Fires thousands of simultaneous reservation attempts at fixed inventory and,
 * after each scenario, asks the *database* whether any two inventory-occupying
 * reservations overlap. The pass condition is that SQL self-join returning zero
 * rows -- not the service reporting success.
 *
 * The `naive` control runs the identical workload against a table with no
 * exclusion constraint. If it stops producing double-bookings, the harness has
 * stopped generating real contention and every other zero in the report is
 * meaningless.
 *
 *   npm run bench            -- run and print
 *   npm run report           -- run and write bench/RESULTS.md + update README
 *   node --env-file=.env bench/loadtest.ts --attempts 5000 --units 200
 */
import { parseArgs } from "node:util";
import { writeFile, readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { closePool, pool, withTransaction } from "../src/db/pool.ts";
import { migrate } from "../src/db/migrate.ts";
import { ServiceError, failureLabel, isLoadShedding } from "../src/domain/errors.ts";
import type { AllocationOutcome, Period } from "../src/domain/types.ts";
import { summarize } from "../src/metrics.ts";
import type { LatencySummary } from "../src/metrics.ts";
import * as repo from "../src/repo/reservations.ts";
import type { AllocationStrategy } from "../src/services/allocator.ts";
import { allocate } from "../src/services/allocator.ts";
import { candidatePlan, resetContended, seedFiller } from "./seed.ts";

const HERE = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const { values } = parseArgs({
   options: {
      attempts: { type: "string", default: "2000" },
      units: { type: "string", default: "100" },
      "seed-resources": { type: "string", default: "2000" },
      "max-retries": { type: "string", default: "25" },
      // Comma-separated subset, e.g. --strategies optimistic,naive. Useful for
      // CI and for quick runs: `serializable` is by far the slowest under
      // contention and dominates the wall time of a full sweep.
      strategies: { type: "string" },
      "write-report": { type: "boolean", default: false },
      "skip-seed": { type: "boolean", default: false },
   },
});

const ATTEMPTS = Number.parseInt(values.attempts!, 10);
const UNITS = Number.parseInt(values.units!, 10);
const MAX_RETRIES = Number.parseInt(values["max-retries"]!, 10);

// ---------------------------------------------------------------------------
// Scenario definitions
// ---------------------------------------------------------------------------

interface Scenario {
   name: string;
   description: string;
   units: number;
   attempts: number;
   /** Expected winners, or null when the workload does not pin it exactly. */
   expectedWinners: number | null;
   period: (index: number) => Period;
}

const day = (n: number) => new Date(Date.UTC(2030, 8, n));

function scenarios(): Scenario[] {
   return [
      {
         name: "last-room",
         description: `${ATTEMPTS} requests, 1 unit, identical window`,
         units: 1,
         attempts: ATTEMPTS,
         expectedWinners: 1,
         period: () => ({ from: day(1), to: day(5) }),
      },
      {
         name: "full-house",
         description: `${ATTEMPTS} requests, ${UNITS} units, identical window`,
         units: UNITS,
         attempts: ATTEMPTS,
         expectedWinners: UNITS,
         period: () => ({ from: day(1), to: day(5) }),
      },
      {
         name: "sliding-windows",
         description: `${ATTEMPTS} requests, ${UNITS} units, overlapping date ranges`,
         units: UNITS,
         attempts: ATTEMPTS,
         // Every window straddles the night of the 10th, so winners are capped
         // by unit count, but the exact figure depends on how the non-shared
         // nights pack -- so it is bounded, not pinned.
         expectedWinners: null,
         period: (i) => ({ from: day(1 + (i % 9)), to: day(11 + (i % 9)) }),
      },
   ];
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

interface Result {
   scenario: string;
   strategy: AllocationStrategy;
   attempts: number;
   units: number;
   winners: number;
   soldOut: number;
   exhausted: number;
   timeouts: number;
   unexpectedErrors: number;
   errorStates: Record<string, number>;
   doubleBookings: number;
   exclusionConflicts: number;
   serializationFailures: number;
   transactions: number;
   wallMs: number;
   throughput: number;
   latency: LatencySummary;
}

async function runScenario(
   scenario: Scenario,
   strategy: AllocationStrategy,
): Promise<Result> {
   await resetContended();

   const resource = await withTransaction((tx) =>
      repo.createResource(tx, {
         slug: `contended-${scenario.name}-${strategy}`,
         name: scenario.name,
         unitLabels: Array.from({ length: scenario.units }, (_, i) => `u${i}`),
      }),
   );

   const latencies: number[] = [];
   let winners = 0;
   let soldOut = 0;
   let exhausted = 0;
   let timeouts = 0;
   let unexpectedErrors = 0;
   const errorStates: Record<string, number> = {};
   let exclusionConflicts = 0;
   let serializationFailures = 0;
   let transactions = 0;

   // Build every promise before awaiting any, so the whole batch is handed to
   // the pool in one synchronous pass and genuinely interleaves at the server.
   const gate = Promise.withResolvers<void>();
   const tasks = Array.from({ length: scenario.attempts }, (_, i) =>
      gate.promise.then(async () => {
         const started = performance.now();
         try {
            const outcome: AllocationOutcome = await allocate({
               resourceId: resource.id,
               guestRef: `guest-${i}`,
               period: scenario.period(i),
               strategy,
               maxRetries: strategy === "naive" ? 0 : MAX_RETRIES,
               // Long enough that no hold expires mid-run; otherwise a slow
               // strategy silently re-sells inventory and reports more winners
               // than there are units.
               ttlSeconds: 3600,
            });
            winners++;
            exclusionConflicts += outcome.exclusionConflicts;
            serializationFailures += outcome.serializationFailures;
            transactions += outcome.attempts;
         } catch (err) {
            if (err instanceof ServiceError) {
               if (err.code === "no_inventory") soldOut++;
               else if (err.code === "exhausted_retries") {
                  exhausted++;
                  const d = err.detail as Record<string, number>;
                  exclusionConflicts += d.exclusionConflicts ?? 0;
                  serializationFailures += d.serializationFailures ?? 0;
               } else unexpectedErrors++;
            } else if (isLoadShedding(err)) {
               // Statement timeout, predicate-lock exhaustion, or pool
               // starvation. Nothing was written, so none of these can corrupt
               // state -- they mean the strategy ran out of capacity at this
               // contention level. Counted separately from correctness failures.
               timeouts++;
               const label = failureLabel(err);
               errorStates[label] = (errorStates[label] ?? 0) + 1;
            } else {
               unexpectedErrors++;
               const label = failureLabel(err);
               errorStates[label] = (errorStates[label] ?? 0) + 1;
            }
         } finally {
            latencies.push(performance.now() - started);
         }
      }),
   );

   const startedAt = performance.now();
   gate.resolve();
   await Promise.all(tasks);
   const wallMs = performance.now() - startedAt;

   const table = strategy === "naive" ? "naive_reservations" : "reservations";
   const conflicts = await withTransaction((tx) => repo.findDoubleBookings(tx, table));

   return {
      scenario: scenario.name,
      strategy,
      attempts: scenario.attempts,
      units: scenario.units,
      winners,
      soldOut,
      exhausted,
      timeouts,
      unexpectedErrors,
      errorStates,
      doubleBookings: conflicts.length,
      exclusionConflicts,
      serializationFailures,
      transactions,
      wallMs,
      throughput: (scenario.attempts / wallMs) * 1000,
      latency: summarize(latencies),
   };
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

const n = (v: number) => v.toLocaleString("en-US");
const ms = (v: number) => `${v.toFixed(1)}`;

function resultsTable(results: Result[]): string {
   const header =
      "| Scenario | Strategy | Attempts | Units | Booked | Sold out | Retries | Shed | **Double-bookings** | p50 | p95 | p99 | Wall |\n" +
      "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |";

   const rows = results.map((r) => {
      const retries = r.transactions > 0 ? r.transactions - r.winners : 0;
      const flag = r.doubleBookings === 0 ? "**0**" : `**${n(r.doubleBookings)}** ⚠️`;
      return (
         `| ${r.scenario} | \`${r.strategy}\` | ${n(r.attempts)} | ${n(r.units)} | ` +
         `${n(r.winners)} | ${n(r.soldOut)} | ${n(retries + r.exclusionConflicts + r.serializationFailures)} | ` +
         `${r.timeouts > 0 ? `${n(r.timeouts)} ⚠️` : "0"} | ${flag} | ` +
         `${ms(r.latency.p50)} | ${ms(r.latency.p95)} | ${ms(r.latency.p99)} | ` +
         `${(r.wallMs / 1000).toFixed(2)}s |`
      );
   });

   return [header, ...rows].join("\n");
}

function verdict(results: Result[]): string {
   const safe = results.filter((r) => r.strategy !== "naive");
   const naive = results.filter((r) => r.strategy === "naive");

   const totalAttempts = safe.reduce((a, r) => a + r.attempts, 0);
   const totalDoubles = safe.reduce((a, r) => a + r.doubleBookings, 0);
   const naiveDoubles = naive.reduce((a, r) => a + r.doubleBookings, 0);
   const naiveAttempts = naive.reduce((a, r) => a + r.attempts, 0);

   const lines = [
      `**${n(totalDoubles)} double-bookings across ${n(totalAttempts)} concurrent reservation attempts** ` +
         `on the guarded schema.`,
   ];

   if (naive.length > 0) {
      lines.push(
         "",
         `Control group: the same harness, run against a table with the exclusion constraint removed, ` +
            `produced **${n(naiveDoubles)} double-bookings in ${n(naiveAttempts)} attempts** — ` +
            `confirming the load actually contends and the zero above is a result, not an absence of pressure.`,
      );
   }

   return lines.join("\n");
}

function correctnessIssues(results: Result[], defs: Scenario[]): string[] {
   const issues: string[] = [];
   for (const r of results) {
      if (r.strategy === "naive") continue;
      if (r.doubleBookings > 0) {
         issues.push(`${r.scenario}/${r.strategy}: ${r.doubleBookings} double-bookings`);
      }
      if (r.unexpectedErrors > 0) {
         issues.push(
            `${r.scenario}/${r.strategy}: ${r.unexpectedErrors} unexpected errors ` +
               `(${JSON.stringify(r.errorStates)})`,
         );
      }
      const expected = defs.find((s) => s.name === r.scenario)?.expectedWinners ?? null;
      // Overselling is always a failure. Under-booking is only a failure if
      // every request got a real answer -- if some were shed by timeout or
      // exhausted retries, unsold inventory is the expected consequence.
      const shed = r.timeouts + r.exhausted;
      if (expected !== null && r.winners > expected) {
         issues.push(`${r.scenario}/${r.strategy}: oversold ${r.winners} on ${expected} units`);
      } else if (expected !== null && r.winners < expected && shed === 0) {
         issues.push(
            `${r.scenario}/${r.strategy}: booked ${r.winners} of ${expected} units ` +
               `with no requests shed (lost inventory)`,
         );
      }
      if (expected === null && r.winners > r.units) {
         issues.push(`${r.scenario}/${r.strategy}: booked ${r.winners} on ${r.units} units`);
      }
   }
   return issues;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const ALL_STRATEGIES: AllocationStrategy[] = [
   "optimistic",
   "pessimistic",
   "serializable",
   "naive",
];

function selectedStrategies(): AllocationStrategy[] {
   const raw = values.strategies;
   if (!raw) return ALL_STRATEGIES;

   const requested = raw
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean) as AllocationStrategy[];

   const unknown = requested.filter((s) => !ALL_STRATEGIES.includes(s));
   if (unknown.length > 0) {
      throw new Error(
         `Unknown strategy: ${unknown.join(", ")}. ` +
            `Valid values are ${ALL_STRATEGIES.join(", ")}.`,
      );
   }
   return requested;
}

const STRATEGIES = selectedStrategies();

await migrate(() => {});

console.log(`\nreservation-service load test`);
console.log(`${"=".repeat(72)}\n`);

if (!values["skip-seed"]) {
   const seedResources = Number.parseInt(values["seed-resources"]!, 10);
   process.stdout.write(`seeding ${n(seedResources)} filler resources... `);
   const stats = await seedFiller({
      resources: seedResources,
      unitsPerResource: 20,
      reservationsPerUnit: 2,
   });
   console.log(
      `${n(stats.reservations)} reservations / ${n(stats.units)} units ` +
         `(${stats.tableSize}) in ${stats.seconds.toFixed(1)}s`,
   );
}

const defs = scenarios();
const results: Result[] = [];

for (const scenario of defs) {
   console.log(`\n${scenario.name} — ${scenario.description}`);
   for (const strategy of STRATEGIES) {
      process.stdout.write(`  ${strategy.padEnd(13)} `);
      const result = await runScenario(scenario, strategy);
      results.push(result);
      console.log(
         `booked ${String(result.winners).padStart(4)}/${String(result.units).padEnd(4)} ` +
            `double-bookings=${result.doubleBookings} ` +
            `p99=${ms(result.latency.p99)}ms ` +
            `wall=${(result.wallMs / 1000).toFixed(2)}s`,
      );
   }
}

// Sanity check that the seeding actually moved the planner off a seq scan.
const { rows: anyResource } = await pool.query<{ id: string }>(
   "select id from resources where slug like 'filler-%' limit 1",
);
const plan = anyResource[0] ? await candidatePlan(anyResource[0].id) : "(no filler data)";

console.log(`\n${"=".repeat(72)}`);
const issues = correctnessIssues(results, defs);
if (issues.length === 0) {
   console.log("PASS — zero double-bookings, no lost inventory, no unexpected errors.");
} else {
   console.log("FAIL");
   for (const issue of issues) console.log(`  - ${issue}`);
}
console.log(`${"=".repeat(72)}\n`);
console.log(resultsTable(results));
console.log();

if (values["write-report"]) {
   const report = [
      "<!-- Generated by `npm run report`. Do not edit by hand. -->",
      `# Load test results`,
      "",
      `Generated ${new Date().toISOString()} · PostgreSQL 17 · pool max ${pool.options.max} · ` +
         `Node ${process.version}`,
      "",
      verdict(results),
      "",
      "## Results",
      "",
      resultsTable(results),
      "",
      "## Candidate-selection plan under load",
      "",
      "Table size decides the plan, and the plan decides how SERIALIZABLE behaves:",
      "a seq scan takes a relation-level predicate lock, so every reader conflicts",
      "with every writer. The harness seeds the table until the planner uses an index.",
      "",
      "```",
      plan,
      "```",
      "",
      "## Verification",
      "",
      "Every figure in the **Double-bookings** column comes from `find_double_bookings()`,",
      "a SQL self-join over the table itself:",
      "",
      "```sql",
      "SELECT a.unit_id, a.id, b.id, a.period * b.period",
      "FROM reservations a JOIN reservations b",
      "  ON a.unit_id = b.unit_id AND a.id < b.id AND a.period && b.period",
      "WHERE a.state IN ('held','confirmed') AND b.state IN ('held','confirmed');",
      "```",
      "",
      issues.length === 0
         ? "Result: **PASS** — zero double-bookings, no lost inventory, no unexpected errors."
         : `Result: **FAIL**\n\n${issues.map((i) => `- ${i}`).join("\n")}`,
      "",
   ].join("\n");

   const reportPath = join(HERE, "RESULTS.md");
   await writeFile(reportPath, report, "utf8");
   console.log(`wrote ${reportPath}`);

   // Splice the table into the README between marker comments so the headline
   // numbers there are always the ones the harness actually produced.
   const readmePath = join(HERE, "..", "README.md");
   try {
      const readme = await readFile(readmePath, "utf8");
      const start = "<!-- BENCH:START -->";
      const end = "<!-- BENCH:END -->";
      if (readme.includes(start) && readme.includes(end)) {
         const before = readme.slice(0, readme.indexOf(start) + start.length);
         const after = readme.slice(readme.indexOf(end));
         const block = ["", verdict(results), "", resultsTable(results), ""].join("\n");
         await writeFile(readmePath, `${before}${block}${after}`, "utf8");
         console.log(`updated ${readmePath}`);
      }
   } catch {
      // README not written yet; RESULTS.md still stands on its own.
   }
}

await closePool();
process.exitCode = issues.length === 0 ? 0 : 1;
