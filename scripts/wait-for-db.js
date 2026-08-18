// Polls the database until it accepts queries, so `npm run db:up` is safe to
// chain straight into `db:migrate` in scripts and CI.
import { Client } from "pg";

const url = process.env.DATABASE_URL;
if (!url) {
   console.error("DATABASE_URL is not set (did you copy .env.example to .env?)");
   process.exit(1);
}

const deadline = Date.now() + 60_000;

while (Date.now() < deadline) {
   const client = new Client({ connectionString: url, connectionTimeoutMillis: 2000 });
   try {
      await client.connect();
      await client.query("select 1");
      await client.end();
      console.log("postgres is ready");
      process.exit(0);
   } catch {
      await client.end().catch(() => {});
      await new Promise((r) => setTimeout(r, 500));
   }
}

console.error("timed out waiting for postgres after 60s");
process.exit(1);
