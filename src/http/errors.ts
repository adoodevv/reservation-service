/** Maps domain errors onto HTTP status codes and a stable error envelope. */
import type { FastifyReply } from "fastify";
import { ServiceError } from "../domain/errors.ts";
import type { ErrorCode } from "../domain/errors.ts";
import { increment } from "../metrics.ts";

const STATUS_BY_CODE: Record<ErrorCode, number> = {
   invalid_request: 422,
   not_found: 404,
   // Terminal: the resource really is sold out for that window. Retrying the
   // identical request will not help.
   no_inventory: 409,
   // Someone else transitioned the row first. The client must re-read and decide.
   version_mismatch: 409,
   conflict: 409,
   // Not terminal: inventory may exist, we just kept losing races. Retrying is
   // the correct client behaviour, hence 503 + Retry-After rather than 409.
   exhausted_retries: 503,
};

export function sendError(reply: FastifyReply, err: unknown): FastifyReply {
   if (err instanceof ServiceError) {
      const status = STATUS_BY_CODE[err.code];
      increment(`http.error.${err.code}`);
      if (err.code === "exhausted_retries") reply.header("retry-after", "1");
      return reply.status(status).send({
         error: { code: err.code, message: err.message, detail: err.detail },
      });
   }

   increment("http.error.internal");
   return reply.status(500).send({
      error: { code: "internal_error", message: "Unexpected server error" },
   });
}
