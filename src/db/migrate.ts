/**
 * Minimal forward-only migration runner.
 *
 * Each file in db/migrations runs exactly once, in filename order, inside its
 * own transaction, and is recorded in schema_migrations. A checksum is stored
 * so an edited-after-the-fact migration is caught rather than silently skipped.
 */
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { closePool, pool } from "./pool.ts";

const MIGRATIONS_DIR = join(dirname(fileURLToPath(import.meta.url)), "../../db/migrations");

async function ensureMigrationsTable(): Promise<void> {
   await pool.query(`
      create table if not exists schema_migrations (
         filename    text primary key,
         checksum    text not null,
         applied_at  timestamptz not null default now()
      )
   `);
}

function checksum(sql: string): string {
   return createHash("sha256").update(sql).digest("hex").slice(0, 16);
}

export async function migrate(log: (msg: string) => void = console.log): Promise<void> {
   await ensureMigrationsTable();

   const files = (await readdir(MIGRATIONS_DIR)).filter((f) => f.endsWith(".sql")).sort();

   const { rows } = await pool.query<{ filename: string; checksum: string }>(
      "select filename, checksum from schema_migrations",
   );
   const applied = new Map(rows.map((r) => [r.filename, r.checksum]));

   for (const filename of files) {
      const sql = await readFile(join(MIGRATIONS_DIR, filename), "utf8");
      const sum = checksum(sql);
      const previous = applied.get(filename);

      if (previous !== undefined) {
         if (previous !== sum) {
            throw new Error(
               `Migration ${filename} changed after it was applied ` +
                  `(recorded ${previous}, now ${sum}). Write a new migration instead.`,
            );
         }
         continue;
      }

      const client = await pool.connect();
      try {
         await client.query("begin");
         await client.query(sql);
         await client.query(
            "insert into schema_migrations (filename, checksum) values ($1, $2)",
            [filename, sum],
         );
         await client.query("commit");
         log(`applied ${filename}`);
      } catch (err) {
         await client.query("rollback").catch(() => {});
         throw new Error(`Migration ${filename} failed: ${(err as Error).message}`, {
            cause: err,
         });
      } finally {
         client.release();
      }
   }

   log(`schema up to date (${files.length} migration(s))`);
}

// Only run when invoked directly, so tests can import migrate() as a library.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
   try {
      await migrate();
   } catch (err) {
      console.error((err as Error).message);
      process.exitCode = 1;
   } finally {
      await closePool();
   }
}
