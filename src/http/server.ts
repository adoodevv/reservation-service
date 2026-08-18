/** HTTP surface. Thin: parse, delegate, map errors, serialise. */
import Fastify from "fastify";
import type { FastifyInstance } from "fastify";
import { ZodError } from "zod";
import { config } from "../config.ts";
import { pool, withTransaction } from "../db/pool.ts";
import { ServiceError } from "../domain/errors.ts";
import type { Reservation } from "../domain/types.ts";
import { increment, snapshot } from "../metrics.ts";
import * as repo from "../repo/reservations.ts";
import * as bookings from "../services/bookings.ts";
import { reapOnce } from "../services/reaper.ts";
import { sendError } from "./errors.ts";
import {
   availabilityQuerySchema,
   createHoldSchema,
   createResourceSchema,
   uuidParamSchema,
   versionSchema,
} from "./schemas.ts";

/** Wire representation: dates as ISO strings, no internal fields. */
function serialize(reservation: Reservation) {
   return {
      id: reservation.id,
      resourceId: reservation.resourceId,
      unitId: reservation.unitId,
      unitLabel: reservation.unitLabel,
      guestRef: reservation.guestRef,
      state: reservation.state,
      from: reservation.from.toISOString(),
      to: reservation.to.toISOString(),
      holdExpiresAt: reservation.holdExpiresAt?.toISOString() ?? null,
      // Echoed back because the client must send it on the next transition.
      version: reservation.version,
      createdAt: reservation.createdAt.toISOString(),
   };
}

export function buildServer(): FastifyInstance {
   const app = Fastify({
      logger: { level: process.env.LOG_LEVEL ?? "info" },
      // The load harness fires everything at once; the default 30s is fine but
      // being explicit documents the intent.
      requestTimeout: 30_000,
   });

   // Zod failures become 422 with the field paths intact, rather than 500s.
   app.setErrorHandler((err, _req, reply) => {
      if (err instanceof ZodError) {
         return reply.status(422).send({
            error: {
               code: "invalid_request",
               message: "Request failed validation",
               detail: { issues: err.issues },
            },
         });
      }
      if (err instanceof ServiceError) return sendError(reply, err);
      app.log.error(err);
      return sendError(reply, err);
   });

   // -----------------------------------------------------------------------
   // Operations
   // -----------------------------------------------------------------------

   app.get("/health", async (_req, reply) => {
      try {
         await pool.query("select 1");
         return reply.send({ status: "ok", strategy: config.allocationStrategy });
      } catch {
         return reply.status(503).send({ status: "degraded", reason: "database unreachable" });
      }
   });

   /** Counters and latency percentiles for the current process. */
   app.get("/metrics", async (_req, reply) => reply.send(snapshot()));

   /**
    * Verifier endpoint. Returns any pair of inventory-occupying reservations
    * that overlap on the same unit -- which, given the exclusion constraint,
    * must always be empty. Exposed so the invariant can be checked from outside
    * the test suite.
    */
   app.get("/internal/double-bookings", async (_req, reply) => {
      const conflicts = await withTransaction((tx) => repo.findDoubleBookings(tx));
      return reply.send({ count: conflicts.length, conflicts });
   });

   app.post("/internal/reap", async (_req, reply) => {
      const expired = await reapOnce();
      return reply.send({ expired });
   });

   // -----------------------------------------------------------------------
   // Inventory
   // -----------------------------------------------------------------------

   app.post("/v1/resources", async (req, reply) => {
      const body = createResourceSchema.parse(req.body);
      const resource = await withTransaction((tx) => repo.createResource(tx, body));
      return reply.status(201).send(resource);
   });

   app.get("/v1/resources/:id/availability", async (req, reply) => {
      const { id } = uuidParamSchema.parse(req.params);
      const { from, to } = availabilityQuerySchema.parse(req.query);
      const units = await bookings.getAvailability(id, { from, to });
      return reply.send({
         resourceId: id,
         from: from.toISOString(),
         to: to.toISOString(),
         total: units.length,
         free: units.filter((u) => u.isFree).length,
         units,
      });
   });

   // -----------------------------------------------------------------------
   // Holds and reservations
   // -----------------------------------------------------------------------

   app.post("/v1/holds", async (req, reply) => {
      const body = createHoldSchema.parse(req.body);
      const idempotencyKey = req.headers["idempotency-key"];

      increment("http.holds.requested");

      try {
         const { outcome, replayed } = await bookings.createHold(
            {
               resourceId: body.resourceId,
               guestRef: body.guestRef,
               period: { from: body.from, to: body.to },
               ...(body.ttlSeconds !== undefined ? { ttlSeconds: body.ttlSeconds } : {}),
               ...(body.strategy !== undefined ? { strategy: body.strategy } : {}),
            },
            typeof idempotencyKey === "string" ? idempotencyKey : undefined,
         );

         return reply
            .status(replayed ? 200 : 201)
            .header("x-allocation-attempts", String(outcome.attempts))
            .header("x-allocation-conflicts", String(outcome.exclusionConflicts))
            .send({ ...serialize(outcome.reservation), replayed });
      } catch (err) {
         return sendError(reply, err);
      }
   });

   app.post("/v1/holds/:id/confirm", async (req, reply) => {
      const { id } = uuidParamSchema.parse(req.params);
      const { version } = versionSchema.parse(req.body);
      try {
         const reservation = await bookings.confirmHold(id, version);
         return reply.send(serialize(reservation));
      } catch (err) {
         return sendError(reply, err);
      }
   });

   app.post("/v1/reservations/:id/cancel", async (req, reply) => {
      const { id } = uuidParamSchema.parse(req.params);
      const { version } = versionSchema.parse(req.body);
      try {
         const reservation = await bookings.cancelReservation(id, version);
         return reply.send(serialize(reservation));
      } catch (err) {
         return sendError(reply, err);
      }
   });

   app.get("/v1/reservations/:id", async (req, reply) => {
      const { id } = uuidParamSchema.parse(req.params);
      try {
         return reply.send(serialize(await bookings.getReservation(id)));
      } catch (err) {
         return sendError(reply, err);
      }
   });

   return app;
}
