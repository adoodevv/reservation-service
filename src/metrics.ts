/**
 * In-process counters and latency percentiles.
 *
 * Deliberately dependency-free: the interesting numbers here (conflict rate,
 * retries per success) are cheap to count and the point is that they are
 * *visible*, not that they ship to a TSDB.
 */

const counters = new Map<string, number>();
const samples = new Map<string, number[]>();

/** Cap per-series retention so a long-running process cannot grow unbounded. */
const MAX_SAMPLES = 10_000;

export function increment(name: string, by = 1): void {
   counters.set(name, (counters.get(name) ?? 0) + by);
}

export function observe(name: string, valueMs: number): void {
   let series = samples.get(name);
   if (!series) {
      series = [];
      samples.set(name, series);
   }
   if (series.length >= MAX_SAMPLES) {
      // Reservoir-free approximation: drop the oldest half and keep going. Good
      // enough for a load-test readout, and bounded.
      series.splice(0, MAX_SAMPLES / 2);
   }
   series.push(valueMs);
}

export function percentile(values: number[], p: number): number {
   if (values.length === 0) return 0;
   const sorted = [...values].sort((a, b) => a - b);
   const rank = Math.ceil((p / 100) * sorted.length) - 1;
   const index = Math.min(sorted.length - 1, Math.max(0, rank));
   return sorted[index]!;
}

export interface LatencySummary {
   count: number;
   min: number;
   p50: number;
   p95: number;
   p99: number;
   max: number;
   mean: number;
}

export function summarize(values: number[]): LatencySummary {
   if (values.length === 0) {
      return { count: 0, min: 0, p50: 0, p95: 0, p99: 0, max: 0, mean: 0 };
   }
   const sorted = [...values].sort((a, b) => a - b);
   const sum = sorted.reduce((a, b) => a + b, 0);
   return {
      count: sorted.length,
      min: sorted[0]!,
      p50: percentile(sorted, 50),
      p95: percentile(sorted, 95),
      p99: percentile(sorted, 99),
      max: sorted[sorted.length - 1]!,
      mean: sum / sorted.length,
   };
}

export function snapshot(): {
   counters: Record<string, number>;
   latencies: Record<string, LatencySummary>;
} {
   return {
      counters: Object.fromEntries(counters),
      latencies: Object.fromEntries(
         [...samples].map(([name, values]) => [name, summarize(values)]),
      ),
   };
}

export function reset(): void {
   counters.clear();
   samples.clear();
}
