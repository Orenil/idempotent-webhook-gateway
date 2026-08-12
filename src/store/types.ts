/**
 * The outbox store is the single source of truth for exactly-once semantics.
 *
 * Design: every inbound webhook is written to a durable table with a UNIQUE
 * constraint on (provider, delivery_id) *before* the HTTP response is sent.
 * If the same delivery is retried by the provider, the insert violates the
 * unique constraint and the row is simply not created a second time — the
 * database enforces the dedup guarantee, not application code. Async
 * processing then claims rows off this same table, so the row that a
 * duplicate delivery could never create is also a row that can never be
 * double-processed.
 */

export type EventStatus = "pending" | "processing" | "completed" | "failed";

export interface WebhookEventInput {
  provider: string;
  deliveryId: string;
  eventType: string;
  payload: string; // raw JSON string, stored verbatim for replay fidelity
  headers: string; // JSON-serialized subset of request headers
  receivedAt: number; // epoch ms
}

export interface WebhookEventRecord extends WebhookEventInput {
  id: number;
  status: EventStatus;
  attempts: number;
  claimedAt: number | null;
  claimedBy: string | null;
  processedAt: number | null;
  lastError: string | null;
}

export interface InsertResult {
  /** true if this call created the row (first delivery of this ID) */
  inserted: boolean;
  /** the row, whether newly created or the pre-existing duplicate */
  event: WebhookEventRecord;
}

/**
 * Handlers run *inside* the same transaction that marks an event completed.
 * If the handler throws, the whole transaction (handler side effects +
 * status update) rolls back, so a crash or error mid-handler can never
 * leave behind a partially-applied side effect paired with a "completed"
 * status. On restart the event is still "processing" (or reclaimed back to
 * pending) and will be retried from scratch.
 */
export type EventHandler<TxCtx> = (
  event: WebhookEventRecord,
  tx: TxCtx,
) => void | Promise<void>;

export interface EventStore<TxCtx = unknown> {
  /**
   * Durably store an inbound webhook. Must be safe to call concurrently
   * with the same deliveryId from many callers; exactly one call may
   * return inserted: true.
   */
  insertEvent(input: WebhookEventInput): Promise<InsertResult>;

  /**
   * Atomically claim the oldest actionable event: status = 'pending', or
   * status = 'processing' whose claim has gone stale (worker died mid-work).
   * Returns null if nothing is claimable right now.
   */
  claimNext(workerId: string, staleClaimMs: number): Promise<WebhookEventRecord | null>;

  /**
   * Run `handler` inside a transaction, then mark the event completed, all
   * atomically. On handler failure the transaction rolls back, attempts is
   * incremented, and the event reverts to 'pending' (or 'failed' once the
   * retry budget is exhausted).
   */
  processClaimed(
    event: WebhookEventRecord,
    handler: EventHandler<TxCtx>,
    maxAttempts: number,
  ): Promise<{ ok: true } | { ok: false; error: string }>;

  getById(id: number): Promise<WebhookEventRecord | null>;
  getByDeliveryId(provider: string, deliveryId: string): Promise<WebhookEventRecord | null>;
  listRecent(limit: number): Promise<WebhookEventRecord[]>;

  /** Reset a completed/failed event back to pending so it can be replayed. */
  requeue(id: number): Promise<WebhookEventRecord | null>;

  close(): Promise<void>;
}
