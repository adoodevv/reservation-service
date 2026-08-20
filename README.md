# reservation-service

[![CI](https://github.com/adoodevv/reservation-service/actions/workflows/ci.yml/badge.svg)](https://github.com/adoodevv/reservation-service/actions/workflows/ci.yml)

A standalone booking service for a finite resource over date ranges, built so that
**two users hitting "book" on the last room at the same millisecond cannot both win** —
and built to prove it rather than assert it.

The interesting part is not the CRUD. It is the answer to *what happens at the
moment of collision*, and the evidence that the answer holds under thousands of
simultaneous attempts.

**0 double-bookings across 27,000 concurrent reservation attempts** on the guarded schema.

Control group: the same harness, run against a table with the exclusion constraint removed, produced **35,966 double-bookings in 9,000 attempts** — confirming the load actually contends and the zero above is a result, not an absence of pressure.

| Scenario | Strategy | Attempts | Units | Booked | Sold out | Retries | Shed | **Double-bookings** | p50 | p95 | p99 | Wall |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| last-room | `optimistic` | 3,000 | 1 | 1 | 2,999 | 0 | 0 | **0** | 2623.2 | 4468.0 | 4635.6 | 4.71s |
| last-room | `pessimistic` | 3,000 | 1 | 1 | 2,999 | 0 | 0 | **0** | 7827.9 | 15336.6 | 15962.5 | 16.11s |
| last-room | `serializable` | 3,000 | 1 | 1 | 2,999 | 0 | 0 | **0** | 5437.2 | 9188.6 | 9680.4 | 9.78s |
| last-room | `naive` | 3,000 | 1 | 2 | 2,998 | 0 | 0 | **1** ⚠️ | 1464.4 | 2151.8 | 2210.4 | 2.24s |
| full-house | `optimistic` | 3,000 | 200 | 200 | 2,800 | 0 | 0 | **0** | 12056.6 | 14395.2 | 14611.3 | 14.63s |
| full-house | `pessimistic` | 3,000 | 200 | 200 | 2,800 | 0 | 0 | **0** | 6546.6 | 12540.5 | 13022.7 | 13.18s |
| full-house | `serializable` | 3,000 | 200 | 200 | 2,691 | 0 | 109 ⚠️ | **0** | 7771.2 | 10281.1 | 10585.4 | 10.63s |
| full-house | `naive` | 3,000 | 200 | 2,505 | 495 | 0 | 0 | **17,126** ⚠️ | 1789.3 | 3575.8 | 3721.3 | 3.77s |
| sliding-windows | `optimistic` | 3,000 | 200 | 200 | 2,800 | 0 | 0 | **0** | 5172.5 | 7681.9 | 7915.7 | 7.94s |
| sliding-windows | `pessimistic` | 3,000 | 200 | 200 | 2,800 | 0 | 0 | **0** | 5442.1 | 10347.5 | 10789.5 | 10.92s |
| sliding-windows | `serializable` | 3,000 | 200 | 200 | 2,797 | 18 | 3 ⚠️ | **0** | 83664.7 | 85986.4 | 86185.7 | 86.22s |
| sliding-windows | `naive` | 3,000 | 200 | 2,724 | 276 | 0 | 0 | **18,839** ⚠️ | 2960.0 | 6032.4 | 6205.2 | 6.25s |

<sub>Curated by hand from one canonical run on a single developer machine
(Postgres 17 in Docker, 32-connection pool) — see
[`bench/RESULTS.md`](bench/RESULTS.md) for that run's full output, including the
query plan it executed under. Reproduce it with `npm run report`, which writes
`bench/RESULTS.md` and leaves this table alone. Absolute latencies are
hardware-bound and will differ on yours; the double-booking column will
not.</sub>

<sub>⚠️ The `serializable` rows above were measured before
`004_trustworthy_verifier.sql`, with a checker that could not see the failure
mode `serializable` actually has, and their zeros should not be read as evidence
of safety — see [what the measurements actually
showed](#what-the-measurements-actually-showed). The `optimistic`, `pessimistic`
and `naive` rows are unaffected: their behaviour does not depend on the index the
old checker was blind to, and re-running reproduces them.</sub>

---

## How it works, in one picture

Every path to a booking runs through a single database constraint. That is where
safety lives — not in any of the application logic above it.

```
       POST /v1/holds
              │
              ▼
    ┌──────────────────────┐
    │   pick a free unit   │◄──────────────┐
    └──────────────────────┘               │
              │                            │
              ▼                            │
    ┌──────────────────────┐               │
    │   INSERT the hold    │               │
    └──────────────────────┘               │
              │                            │
              ▼                            │
    ┌──────────────────────┐    rejected   │
    │  EXCLUDE constraint  ├───────────────┘
    └──────────────────────┘     (23P01)
              │
           accepted
              │
              ▼
        201 reservation
```

Losing that race is expected rather than exceptional — the loser simply retries
against a different unit. Two things end the loop instead: nothing free at all,
which is a terminal `409 no_inventory`, and a spent retry budget with inventory
still on the shelf, which is a `503 exhausted_retries`. Keeping those answers
distinct is most of what the allocator is for.

---

## The problem

A reservation system's core invariant is one sentence:

> No two active reservations may overlap in time on the same unit of inventory.

Almost every first implementation enforces it like this:

```ts
const free = await findFreeUnit(resourceId, from, to);   // check
if (!free) throw new SoldOut();
await insertReservation(free.id, from, to);              // ...and insert
```

Under READ COMMITTED, concurrent transactions all read the same pre-insert
snapshot. They all see the same unit as free. They all take it. Nothing in that
code — or in any amount of application-level care around it — prevents the
overlap, because the check and the insert are not atomic with respect to each
other.

This service ships that broken version too, as `ALLOCATION_STRATEGY=naive`, and
runs it under the identical load harness. Its numbers are in the table above,
next to zero.

---

## The answer

### 1. The invariant is a database constraint, not application logic

```sql
ALTER TABLE reservations
   ADD CONSTRAINT reservations_no_overlap
   EXCLUDE USING gist (unit_id WITH =, period WITH &&)
   WHERE (state IN ('held', 'confirmed'));
```

`EXCLUDE USING gist` is the elegant answer to overlap. Postgres evaluates it
inside the index while holding a lock on the candidate key range, so two
concurrent transactions inserting the same night cannot both commit — the second
blocks until the first commits and is then rejected with SQLSTATE `23P01`.

Three details in that statement carry real weight:

- **`btree_gist`.** gist understands range overlap (`&&`) but not scalar
  equality (`=`). The extension supplies the btree operator class that lets one
  index mix both.
- **`period` is a `tstzrange` with `[)` bounds.** Half-open is what makes
  back-to-back stays legal: a stay ending on the 5th and one starting on the 5th
  do not overlap. With inclusive bounds, every turnover day would silently cost
  a night of inventory.
- **The `WHERE` clause.** Only `held` and `confirmed` occupy inventory.
  Cancelled and expired rows drop out of the partial index and stop blocking,
  while staying on the table as history.

Because it is a constraint, it holds for callers that never touch this codebase:
a future endpoint, a migration, an engineer at a `psql` prompt. `test/constraint.test.ts`
writes directly to the table, bypassing the service entirely, to verify exactly that.

### 2. Expiry has to be *written*, not inferred

An index predicate must be `IMMUTABLE`, so the constraint cannot say
`AND hold_expires_at > now()`. A row does not leave the index when its deadline
passes — it leaves when something writes `'expired'` to it.

That is a design consequence, not an oversight, and it forces two mechanisms:

- **Inline, in the allocator.** Before it will ever answer "sold out", a request
  reclaims expired holds blocking its own window. An abandoned checkout is
  released for the very next customer, not on the next timer tick.
- **The background reaper.** A batched sweep so availability reads are honest
  without someone first *attempting* a booking.

The inline reclaim runs only on the slow path — when the candidate query already
came back empty. That ordering is load-bearing, not a micro-optimisation:
running it eagerly widened every transaction's read/write footprint, and under
SERIALIZABLE that footprint is exactly what SSI takes predicate locks on. Moving
it turned a 249s workload into a 1.3s one.

### 3. Optimistic locking guards the *row*, not the range

The exclusion constraint says nothing about two requests transitioning the same
reservation at once — confirm racing cancel, or a double-submitted confirm. Every
transition asserts the version it read:

```sql
UPDATE reservations SET state = $4, version = version + 1
WHERE id = $1 AND version = $2 AND state = ANY($3)
```

Zero rows matched means someone else got there first; the service re-reads to
distinguish "your version is stale" (409, with the current version attached)
from "no such reservation" (404). `test/optimistic-lock.test.ts` fires 50
concurrent confirms of one hold and asserts exactly one wins.

### 4. Allocation is a retry loop around a race it expects to lose

Picking *which* free unit to take is a genuine race: the candidate query is a
hint, and under READ COMMITTED it cannot see other transactions' uncommitted
rows. The allocator picks a **random** free unit, inserts, and treats `23P01` as
routine — retrying with full-jitter backoff against a fresh candidate list.

Random selection is the single biggest lever on throughput. Ordered selection
makes every concurrent request pick the same lowest-labelled unit and collide;
random fans them out across free inventory and the conflict rate collapses.

---

## Four strategies, measured against each other

All of this is configurable, because the comparison *is* the point:

| Strategy | Isolation | Mechanism |
| --- | --- | --- |
| `optimistic` (default) | READ COMMITTED | Pick, insert, let the constraint referee. Retry on `23P01`. |
| `pessimistic` | READ COMMITTED | Per-resource advisory lock serialises allocation. Zero conflicts by construction. |
| `serializable` | SERIALIZABLE | Same body; SSI also guarantees the read was consistent. Retry on `40001`. **Unsafe here — see below.** |
| `naive` | READ COMMITTED | Check-then-insert against a table with no constraint. **Intentionally broken.** |

`pessimistic` uses `pg_advisory_xact_lock` rather than `SELECT ... FOR UPDATE` on
the resource row: it is a mutex over *the act of allocating*, not a claim on
resource metadata, so it does not block an unrelated rename, and it releases
automatically on commit or rollback so a crashed backend cannot strand it. The
`_xact_` half matters for deployment too: a transaction-scoped advisory lock is
released with the transaction and so survives transaction-mode pooling
(PgBouncer), whereas the session-scoped `pg_advisory_lock` would outlive the
transaction and be handed to whichever client got that pooled session next.

### What the measurements actually showed

Findings that were not obvious before running the harness, each of which changed
the implementation:

**SERIALIZABLE adds no safety here, costs availability — and then takes safety
away.** The exclusion constraint already provides the guarantee; SSI is layered
on top of it. Under high contention its aborts multiply the population of
in-flight transactions on the same key range, each INSERT then waits out every
conflicting uncommitted transaction it meets, and statements accumulate past
`statement_timeout`. At 120 requests against 10 units: 97 of 120 died on SQLSTATE
`57014`, and the run took 129s against 0.7s for `optimistic`. At 3,000 requests it
also sheds requests to predicate-lock shared-memory exhaustion (`53200`), and on
the sliding-window scenario it runs 86s against 7.9s for `optimistic`. That row
is worse than "slow" makes it sound: a p50 of 83.7s against an 86.2s wall means
the median request was in flight for 97% of the entire run, so the requests were
not sharing the database so much as queuing through it one at a time. That is
near-total serialization — the isolation level's name taken literally.

That was the whole story until the suite produced two `held` rows on one unit for
the same nights. **`serializable` double-books.** Cancelling a statement while a
SERIALIZABLE transaction is inserting into the GiST exclusion index can leave a
committed heap row with no matching index entry, and the constraint is enforced
*by an index scan* — so that row is invisible to it and the next overlapping
insert is admitted. `REINDEX` then refuses to rebuild the index at all, which is
what a heap holding rows the index never recorded looks like.

Any cancellation source triggers it: `lock_timeout`, `statement_timeout`, a
dropped client, `pg_cancel_backend`. Reproduced on stock `postgres:17-alpine`
(17.11) in pure SQL with none of `src/` involved — 60 concurrent SERIALIZABLE
transactions over 10 units fail within ~10 rounds at a 150ms `lock_timeout`, and
still fail with `lock_timeout` disabled and a 200ms `statement_timeout`. Only
with no cancellation source whatsoever did 160 rounds survive, and that is not a
configuration anything can run in. `optimistic` and `pessimistic`, which set the
same timeouts, stayed clean across 320 rounds.

`serializable` is therefore no longer in the tested-safe set. Little is lost:
safety never came from the isolation level, it came from the constraint. What
SERIALIZABLE bought was a *truthful* "sold out" rather than a merely current
one, and the allocator now gets that from an authoritative recount at the end of
its retry loop, far more cheaply.

**A verifier must not share a failure mode with the thing it verifies.** This one
did, and it is the most uncomfortable finding here. `find_double_bookings()`
resolved `period && period` through `reservations_no_overlap` — the exclusion
index itself. So the check for "did the index fail?" was answered *by the index*.
On a table holding four genuine violations it reported two; forcing a sequential
scan over the same rows returned all four. The function now pins the planner off
index scans (`004_trustworthy_verifier.sql`), which is the only reason the
numbers below can be read as evidence at all.

**Benchmarking on a small table measures the wrong thing.** On a 20-row
`reservations` table the planner seq-scans, and under SERIALIZABLE a seq scan
takes a *relation-level* predicate lock, so every reader conflicts with every
writer. The harness seeds the table until the planner switches to index scans;
without that, the comparison is an artefact of table size. `bench/RESULTS.md`
prints the plan it actually ran under.

**The client was the bottleneck before the database was.** Sampling
`pg_stat_activity` mid-run showed backends sitting `idle in transaction` on
`ClientRead` with only ~2 queries ever `active` — Postgres waiting on
single-threaded Node, not the reverse. Retry amplification is therefore a
client-side cost as much as a database one, which is why the hold insert and its
audit event now write in one statement, and `BEGIN` carries the lock timeout with
it.

A fourth finding came from a failing test rather than the bench: `lock_timeout`
was set to 750ms on the theory that waiting out a commit is cheap. It is not.
Under READ COMMITTED the candidate query cannot see uncommitted winners, so
during a burst thousands of requests queue behind transactions taking the units
they just picked — spending their entire retry budget waiting instead of
re-picking. Dropping it to 150ms took the 3,000-request / 200-unit scenario from 272s —
with only 1 of 3,000 requests getting a clean answer and 2,533 dying on pool
timeouts — to 14.6s with all 3,000 answered.

---

## Verification

The concurrency tests never assert that *the service reported* no conflicts.
They assert that no conflicting rows exist, using a SQL self-join that knows
nothing about the code that wrote them:

```sql
CREATE FUNCTION find_double_bookings() RETURNS TABLE (...) AS $$
   SELECT a.unit_id, a.id, b.id, a.period * b.period
   FROM reservations a JOIN reservations b
     ON a.unit_id = b.unit_id AND a.id < b.id AND a.period && b.period
   WHERE a.state IN ('held','confirmed') AND b.state IN ('held','confirmed');
$$ LANGUAGE sql STABLE;
```

Two properties are checked every time, and the second is the one that gets
forgotten:

- **Safety** — no two inventory-occupying reservations overlap on a unit.
  *A service that rejects everything is trivially safe.*
- **Liveness** — the number of winners is exactly the number of units. Not
  fewer. A system that books 47 of 50 rooms under load is also broken, just
  quietly.

And the control group: the same harness against the same schema minus the
constraint. If `naive` ever stops producing double-bookings, the harness has
stopped generating real contention and every zero elsewhere is vacuous. It is
the smoke detector's test button.

That button is worth pressing honestly. On `last-room`, `naive` produced a
single double-booking in 3,000 attempts — one unit gives a narrow window in
which two check-then-inserts can interleave, so a near-zero there is a property
of the scenario, not evidence of pressure. The control-group argument therefore
rests on `full-house` (17,126) and `sliding-windows` (18,839), where the window
is wide enough for the absence of a constraint to be unmistakable; read
`last-room`'s control number as the weakest of the three, because it is.

---

## Running it

### Everything in one command

```bash
git clone https://github.com/adoodevv/reservation-service.git
cd reservation-service
docker compose up --build      # or: npm run up
```

That builds the service, starts Postgres 17, waits until it is genuinely
healthy, runs the migrations, and serves on <http://localhost:3000>. No Node
installation, no `.env`, nothing else to configure:

```bash
curl localhost:3000/health
# {"status":"ok","strategy":"optimistic"}
```

Postgres is still published on `:5433` for host tools, and the service reaches
it over the compose network as `postgres:5432`.

### Developing on the host

Requires **Node >= 22.18** and Docker. (The service runs TypeScript directly —
Node strips the types, so there is no build step. Type stripping is only
unflagged from 22.18 onward.)

```bash
cp .env.example .env   # or export DATABASE_URL yourself -- see note below
npm install
npm run db:up          # Postgres 17 in Docker on :5433, waits until it answers
npm run db:migrate
npm start              # http://localhost:3000
```

`npm run db:up` starts *only* Postgres, so the test and bench workflows below
never wait on a container build.

Nothing listens on 5432, so this will not collide with a Postgres you already
run locally. `npm run db:reset` tears the volume down and rebuilds from scratch.

The npm scripts load `.env` *if it exists* (`--env-file-if-exists`) and
otherwise fall back to the ambient environment, so the same commands work
locally with a file and in CI or a container with plain environment variables.

```bash
npm test               # full suite, including the concurrency stampedes (~15s)
npm run bench          # load test, printed
npm run report         # load test, written to bench/RESULTS.md
```

The harness takes `--attempts`, `--units`, `--seed-resources`, `--max-retries`
and `--strategies`, so the numbers above can be reproduced, narrowed, or pushed
harder:

```bash
# Reproduce the table above (several minutes -- `serializable` dominates it).
node --env-file-if-exists=.env bench/loadtest.ts --attempts 3000 --units 200

# The headline comparison on its own, in a few seconds.
node --env-file-if-exists=.env bench/loadtest.ts --strategies optimistic,naive
```

It exits non-zero if any double-booking, oversell, or unexplained error shows
up, which is why CI runs it too.

Two things worth knowing before you run it:

- **Postgres data is stored in `tmpfs`** (see `docker-compose.yml`), so it is
  deliberately wiped when the container stops. Benchmarks should measure lock
  contention, not laptop disk latency. Remove the `tmpfs:` block if you want
  data to survive a restart.
- **`npm run report` writes `bench/RESULTS.md` and nothing else.** The table at
  the top of this README is curated by hand from one canonical run, so your
  numbers land in `bench/RESULTS.md` without dirtying the README or inviting a
  commit of machine-specific latencies.

> If `npm run db:up` fails to pull the image with a `gpg`/credential error, that
> is the host's Docker credential helper, not this project. `docker pull
> postgres:17-alpine` with a clean `DOCKER_CONFIG` works around it.

---

## API

| Method | Path | Notes |
| --- | --- | --- |
| `POST` | `/v1/resources` | Create a resource and its units. |
| `GET` | `/v1/resources/:id/availability?from&to` | Per-unit free/taken for a window. |
| `POST` | `/v1/holds` | Place a hold. Honours `Idempotency-Key`. |
| `POST` | `/v1/holds/:id/confirm` | Body `{version}`. 409 on a stale version. |
| `POST` | `/v1/reservations/:id/cancel` | Body `{version}`. Releases inventory. |
| `GET` | `/v1/reservations/:id` | Read one reservation. |
| `GET` | `/internal/double-bookings` | The invariant verifier, over HTTP. |
| `POST` | `/internal/reap` | Force an expiry sweep. |
| `GET` | `/metrics` | Counters and latency percentiles. |
| `GET` | `/health` | Liveness plus active strategy. |

Status codes distinguish the two failures that look alike:

- **409 `no_inventory`** — genuinely sold out. Terminal; retrying will not help.
- **503 `exhausted_retries`** (with `Retry-After`) — inventory may exist, we
  kept losing races. Retrying is the correct client behaviour.

Holds are idempotent under `Idempotency-Key`. The key is claimed in its own
committed transaction *before* allocation runs, so a duplicate arriving while
the first is still allocating collides and is told to wait, rather than sailing
past and consuming a second unit. A failed allocation releases the key, so a
sold-out first attempt cannot permanently poison it.

---

## Layout

```
db/migrations/     schema; 001_init.sql holds the correctness argument
src/db/            pool, transaction helper, migration runner
src/repo/          all SQL; every function takes an explicit transaction
src/services/      allocator (4 strategies), reaper, idempotency
src/http/          Fastify routes, error→status mapping, validation
test/              constraint, concurrency, optimistic-lock, expiry, api
bench/             load harness and generated RESULTS.md
```

`src/repo` never opens its own transaction. Choosing the transaction and its
isolation level is the allocator's entire job, and it cannot do that if the
layer beneath it opens one first.

---

## License

MIT — see [LICENSE](LICENSE).
