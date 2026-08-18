/** Request validation. Rejects at the edge so no bad range reaches the database. */
import { z } from "zod";

const isoDate = z.coerce.date();

/**
 * A stay is [from, to) and must be non-empty. Both halves of that rule are also
 * enforced by CHECK constraints in 001_init.sql; validating here too turns a
 * constraint violation into a readable 422 instead of a 500.
 */
export const periodSchema = z
   .object({ from: isoDate, to: isoDate })
   .refine((p) => p.to > p.from, {
      message: "`to` must be strictly after `from` (stays are half-open [from, to))",
   });

export const createResourceSchema = z.object({
   slug: z
      .string()
      .min(1)
      .max(64)
      .regex(/^[a-z0-9][a-z0-9-]*$/, "slug must be lowercase alphanumeric with dashes"),
   name: z.string().min(1).max(200),
   unitLabels: z.array(z.string().min(1).max(64)).min(1).max(500),
});

export const createHoldSchema = z.object({
   resourceId: z.string().uuid(),
   guestRef: z.string().min(1).max(200),
   from: isoDate,
   to: isoDate,
   ttlSeconds: z.number().int().min(1).max(86_400).optional(),
   strategy: z.enum(["optimistic", "serializable", "pessimistic", "naive"]).optional(),
}).refine((v) => v.to > v.from, {
   message: "`to` must be strictly after `from`",
   path: ["to"],
});

export const versionSchema = z.object({
   version: z.number().int().min(1),
});

export const availabilityQuerySchema = z.object({
   from: isoDate,
   to: isoDate,
}).refine((v) => v.to > v.from, { message: "`to` must be strictly after `from`", path: ["to"] });

export const uuidParamSchema = z.object({ id: z.string().uuid() });
