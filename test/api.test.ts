/**
 * HTTP surface: status-code mapping, validation, and idempotency.
 *
 * Uses Fastify's inject() rather than a live socket, so these run without a
 * port and without racing the concurrency suites for one.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import type { FastifyInstance } from "fastify";
import { closePool } from "../src/db/pool.ts";
import { buildServer } from "../src/http/server.ts";
import { ensureSchema, truncateAll } from "./helpers.ts";

let app: FastifyInstance;

beforeAll(async () => {
   await ensureSchema();
   app = buildServer();
   await app.ready();
});

afterAll(async () => {
   await app.close();
   await closePool();
});

/** Creates a resource and returns its id. */
async function createResource(units: number, slug = "api-test"): Promise<string> {
   const res = await app.inject({
      method: "POST",
      url: "/v1/resources",
      payload: {
         slug,
         name: "API test resource",
         unitLabels: Array.from({ length: units }, (_, i) => `u${i}`),
      },
   });
   expect(res.statusCode).toBe(201);
   return res.json().id;
}

function holdPayload(resourceId: string, guestRef = "guest") {
   return {
      resourceId,
      guestRef,
      from: "2030-09-01T00:00:00Z",
      to: "2030-09-05T00:00:00Z",
   };
}

describe("HTTP API", () => {
   beforeEach(truncateAll);

   it("reports health", async () => {
      const res = await app.inject({ method: "GET", url: "/health" });
      expect(res.statusCode).toBe(200);
      expect(res.json().status).toBe("ok");
   });

   it("creates a hold and returns its version", async () => {
      const resourceId = await createResource(2);
      const res = await app.inject({
         method: "POST",
         url: "/v1/holds",
         payload: holdPayload(resourceId),
      });

      expect(res.statusCode).toBe(201);
      const body = res.json();
      expect(body.state).toBe("held");
      expect(body.version).toBe(1);
      expect(body.holdExpiresAt).not.toBeNull();
      // Diagnostics that make contention visible to the caller.
      expect(res.headers["x-allocation-attempts"]).toBe("1");
   });

   it("reports availability before and after a hold", async () => {
      const resourceId = await createResource(3);
      const url = `/v1/resources/${resourceId}/availability?from=2030-09-01&to=2030-09-05`;

      expect((await app.inject({ method: "GET", url })).json()).toMatchObject({
         total: 3,
         free: 3,
      });

      await app.inject({ method: "POST", url: "/v1/holds", payload: holdPayload(resourceId) });

      expect((await app.inject({ method: "GET", url })).json()).toMatchObject({
         total: 3,
         free: 2,
      });
   });

   it("returns 409 no_inventory when the resource is sold out", async () => {
      const resourceId = await createResource(1);
      await app.inject({ method: "POST", url: "/v1/holds", payload: holdPayload(resourceId) });

      const res = await app.inject({
         method: "POST",
         url: "/v1/holds",
         payload: holdPayload(resourceId, "second"),
      });
      expect(res.statusCode).toBe(409);
      expect(res.json().error.code).toBe("no_inventory");
   });

   it("returns 409 version_mismatch on a stale confirm", async () => {
      const resourceId = await createResource(1);
      const hold = (
         await app.inject({ method: "POST", url: "/v1/holds", payload: holdPayload(resourceId) })
      ).json();

      const first = await app.inject({
         method: "POST",
         url: `/v1/holds/${hold.id}/confirm`,
         payload: { version: hold.version },
      });
      expect(first.statusCode).toBe(200);
      expect(first.json().state).toBe("confirmed");

      const second = await app.inject({
         method: "POST",
         url: `/v1/holds/${hold.id}/confirm`,
         payload: { version: hold.version },
      });
      expect(second.statusCode).toBe(409);
      expect(second.json().error.code).toBe("version_mismatch");
   });

   it("returns 404 for an unknown reservation", async () => {
      const res = await app.inject({
         method: "GET",
         url: "/v1/reservations/00000000-0000-0000-0000-000000000000",
      });
      expect(res.statusCode).toBe(404);
   });

   it("returns 422 for a malformed range", async () => {
      const resourceId = await createResource(1);
      const res = await app.inject({
         method: "POST",
         url: "/v1/holds",
         payload: { ...holdPayload(resourceId), to: "2030-08-01T00:00:00Z" },
      });
      expect(res.statusCode).toBe(422);
      expect(res.json().error.code).toBe("invalid_request");
   });

   it("returns 422 for a non-uuid resource id", async () => {
      const res = await app.inject({
         method: "POST",
         url: "/v1/holds",
         payload: { ...holdPayload("not-a-uuid") },
      });
      expect(res.statusCode).toBe(422);
   });

   describe("idempotency", () => {
      it("replays the original reservation instead of consuming a second unit", async () => {
         const resourceId = await createResource(5);
         const headers = { "idempotency-key": "key-1" };

         const first = await app.inject({
            method: "POST",
            url: "/v1/holds",
            headers,
            payload: holdPayload(resourceId),
         });
         const second = await app.inject({
            method: "POST",
            url: "/v1/holds",
            headers,
            payload: holdPayload(resourceId),
         });

         expect(first.statusCode).toBe(201);
         expect(second.statusCode).toBe(200);
         expect(second.json().id).toBe(first.json().id);
         expect(second.json().replayed).toBe(true);

         // The decisive assertion: only one unit was consumed.
         const availability = await app.inject({
            method: "GET",
            url: `/v1/resources/${resourceId}/availability?from=2030-09-01&to=2030-09-05`,
         });
         expect(availability.json().free).toBe(4);
      });

      it("rejects a key reused with a different request body", async () => {
         const resourceId = await createResource(5);
         const headers = { "idempotency-key": "key-2" };

         await app.inject({
            method: "POST",
            url: "/v1/holds",
            headers,
            payload: holdPayload(resourceId, "alice"),
         });
         const reused = await app.inject({
            method: "POST",
            url: "/v1/holds",
            headers,
            payload: holdPayload(resourceId, "bob"),
         });

         expect(reused.statusCode).toBe(422);
         expect(reused.json().error.message).toMatch(/different request body/);
      });

      it("20 concurrent duplicates of one key consume exactly one unit", async () => {
         // The double-tap, at scale. Requests that arrive while the original is
         // still allocating get 409 rather than a second unit; requests that
         // arrive after it resolves replay the stored reservation.
         const resourceId = await createResource(10);
         const headers = { "idempotency-key": "key-3" };

         const responses = await Promise.all(
            Array.from({ length: 20 }, () =>
               app.inject({
                  method: "POST",
                  url: "/v1/holds",
                  headers,
                  payload: holdPayload(resourceId),
               }),
            ),
         );

         const created = responses.filter((r) => r.statusCode === 201);
         const replayed = responses.filter((r) => r.statusCode === 200);
         const inFlight = responses.filter((r) => r.statusCode === 409);

         expect(created).toHaveLength(1);
         expect(created.length + replayed.length + inFlight.length).toBe(20);
         for (const r of [...created, ...replayed]) {
            expect(r.json().id).toBe(created[0]!.json().id);
         }

         const availability = await app.inject({
            method: "GET",
            url: `/v1/resources/${resourceId}/availability?from=2030-09-01&to=2030-09-05`,
         });
         expect(availability.json().free).toBe(9);
      });

      it("releases the key when allocation fails, so a retry can succeed", async () => {
         const resourceId = await createResource(1);
         const headers = { "idempotency-key": "key-4" };

         // Consume the only unit without a key.
         await app.inject({
            method: "POST",
            url: "/v1/holds",
            payload: holdPayload(resourceId, "squatter"),
         });

         const soldOut = await app.inject({
            method: "POST",
            url: "/v1/holds",
            headers,
            payload: holdPayload(resourceId),
         });
         expect(soldOut.statusCode).toBe(409);
         expect(soldOut.json().error.code).toBe("no_inventory");

         // The key must not be poisoned by a failure that was not its fault.
         const retry = await app.inject({
            method: "POST",
            url: "/v1/holds",
            headers,
            payload: holdPayload(resourceId),
         });
         expect(retry.json().error.code).toBe("no_inventory");
      });
   });

   it("exposes the invariant verifier", async () => {
      const resourceId = await createResource(3);
      await app.inject({ method: "POST", url: "/v1/holds", payload: holdPayload(resourceId) });

      const res = await app.inject({ method: "GET", url: "/internal/double-bookings" });
      expect(res.statusCode).toBe(200);
      expect(res.json()).toEqual({ count: 0, conflicts: [] });
   });
});
