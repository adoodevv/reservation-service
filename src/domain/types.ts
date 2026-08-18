/** Shapes shared between the repository, services and HTTP layers. */

export type ReservationState = "held" | "confirmed" | "cancelled" | "expired";

export interface Period {
   /** Inclusive start (check-in). */
   from: Date;
   /** Exclusive end (check-out). Half-open, so back-to-back stays don't clash. */
   to: Date;
}

export interface Resource {
   id: string;
   slug: string;
   name: string;
   unitCount: number;
}

export interface ResourceUnit {
   id: string;
   resourceId: string;
   label: string;
}

export interface Reservation {
   id: string;
   unitId: string;
   unitLabel?: string;
   resourceId: string;
   guestRef: string;
   state: ReservationState;
   from: Date;
   to: Date;
   holdExpiresAt: Date | null;
   version: number;
   createdAt: Date;
   updatedAt: Date;
}

export interface AvailabilityRow {
   unitId: string;
   label: string;
   isFree: boolean;
   takenBy: string | null;
}

/** Diagnostics returned alongside a successful allocation. */
export interface AllocationOutcome {
   reservation: Reservation;
   /** How many transactions it took. 1 means the first pick was uncontended. */
   attempts: number;
   /** Attempts lost to another transaction taking the unit first (SQLSTATE 23P01). */
   exclusionConflicts: number;
   /** Attempts lost to a serialization failure or deadlock (40001 / 40P01). */
   serializationFailures: number;
   /** Expired holds this request reclaimed on its way in. */
   reclaimedHolds: number;
}
