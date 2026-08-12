import pg from "pg";
import type {
  EventHandler,
  EventStatus,
  EventStore,
  InsertResult,
  WebhookEventInput,
  WebhookEventRecord,
} from "./types.js";

const { Pool } = pg;
type PoolClient = pg.PoolClient;

/**
 * Real Postgres adapter implementing the same EventStore contract as the
 * SQLite adapter. This is genuine, runnable-against-a-real-database code —
 * it is simply not exercised by the test suite in this environment, which
 * has no live Postgres instance. The SQLite adapter proves the same SQL
 * strategy (UNIQUE constraint dedup, atomic claim, transactional handler)
 * against a real embedded database instead.
 *
 * Production swap notes:
 *  - claimNext uses `SELECT ... FOR UPDATE SKIP LOCKED`, the standard
 *    Postgres pattern for a multi-worker queue claim — it lets N worker
 *    processes poll the same table concurrently without ever handing the
 *    same row to two workers, and without blocking on each other's locks.
 *  - The poll loop in worker.ts can be augmented (or replaced) with
 *    `LISTEN webhook_events_channel` / `NOTIFY webhook_events_channel` (fired
 *    from an AFTER INSERT trigger on this table) so workers wake immediately
 *    on new rows instead of waiting out the poll interval. Polling is kept
 *    as a fallback because NOTIFY delivery is best-effort — a worker that
 *    was disconnected when NOTIFY fired must still fall back to polling to
 *    pick up what it missed.
 *  - At higher throughput, swap the outbox+poll worker for BullMQ (Redis)
 *    fed by the same outbox insert (transactional outbox -> queue publish),
 *    keeping Postgres as the durability source of truth and Redis purely
 *    as the dispatch layer.
 */

export const POSTGRES_SCHEMA = `
CREATE TABLE IF NOT EXISTS webhook_events (
  id BIGSERIAL PRIMARY KEY,
  provider TEXT NOT NULL,
  delivery_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  headers TEXT NOT NULL,
  received_at BIGINT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  claimed_at BIGINT,
  claimed_by TEXT,
  processed_at BIGINT,
  last_error TEXT,
  CONSTRAINT webhook_events_provider_delivery_id_key UNIQUE (provider, delivery_id)
);
CREATE INDEX IF NOT EXISTS idx_webhook_events_status ON webhook_events (status, received_at);

-- Optional LISTEN/NOTIFY wake-up, see class doc comment above.
CREATE OR REPLACE FUNCTION notify_webhook_event_inserted() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('webhook_events_channel', NEW.id::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS webhook_events_notify ON webhook_events;
CREATE TRIGGER webhook_events_notify
  AFTER INSERT ON webhook_events
  FOR EACH ROW EXECUTE FUNCTION notify_webhook_event_inserted();
`;

interface Row {
  id: string;
  provider: string;
  delivery_id: string;
  event_type: string;
  payload: string;
  headers: string;
  received_at: string;
  status: EventStatus;
  attempts: number;
  claimed_at: string | null;
  claimed_by: string | null;
  processed_at: string | null;
  last_error: string | null;
}

function toRecord(row: Row): WebhookEventRecord {
  return {
    id: Number(row.id),
    provider: row.provider,
    deliveryId: row.delivery_id,
    eventType: row.event_type,
    payload: row.payload,
    headers: row.headers,
    receivedAt: Number(row.received_at),
    status: row.status,
    attempts: row.attempts,
    claimedAt: row.claimed_at === null ? null : Number(row.claimed_at),
    claimedBy: row.claimed_by,
    processedAt: row.processed_at === null ? null : Number(row.processed_at),
    lastError: row.last_error,
  };
}

/** Postgres unique_violation error code, see https://www.postgresql.org/docs/current/errcodes-appendix.html */
const UNIQUE_VIOLATION = "23505";

export class PostgresEventStore implements EventStore<PoolClient> {
  private pool: pg.Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({ connectionString });
  }

  async init(): Promise<void> {
    await this.pool.query(POSTGRES_SCHEMA);
  }

  async insertEvent(input: WebhookEventInput): Promise<InsertResult> {
    const insert = await this.pool.query<Row>(
      `INSERT INTO webhook_events (provider, delivery_id, event_type, payload, headers, received_at, status, attempts)
       VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0)
       ON CONFLICT ON CONSTRAINT webhook_events_provider_delivery_id_key DO NOTHING
       RETURNING *`,
      [input.provider, input.deliveryId, input.eventType, input.payload, input.headers, input.receivedAt],
    );
    if (insert.rows.length === 1) {
      return { inserted: true, event: toRecord(insert.rows[0]) };
    }
    // ON CONFLICT DO NOTHING means the unique constraint already held this
    // deliveryId; fetch the row that won so callers can still inspect it.
    const existing = await this.getByDeliveryId(input.provider, input.deliveryId);
    if (existing) return { inserted: false, event: existing };
    // Extremely unlikely race: conflicting row was deleted between the
    // INSERT and this SELECT. Surface the real constraint violation instead
    // of silently lying about dedup.
    throw new Error(`insert conflicted for ${input.provider}/${input.deliveryId} but no row found (code ${UNIQUE_VIOLATION})`);
  }

  async claimNext(workerId: string, staleClaimMs: number): Promise<WebhookEventRecord | null> {
    const staleThreshold = Date.now() - staleClaimMs;
    const result = await this.pool.query<Row>(
      `UPDATE webhook_events
       SET status = 'processing', claimed_at = $1, claimed_by = $2
       WHERE id = (
         SELECT id FROM webhook_events
         WHERE status = 'pending' OR (status = 'processing' AND claimed_at < $3)
         ORDER BY received_at ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       )
       RETURNING *`,
      [Date.now(), workerId, staleThreshold],
    );
    return result.rows[0] ? toRecord(result.rows[0]) : null;
  }

  async processClaimed(
    event: WebhookEventRecord,
    handler: EventHandler<PoolClient>,
    maxAttempts: number,
  ): Promise<{ ok: true } | { ok: false; error: string }> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await handler(event, client);
      await client.query(`UPDATE webhook_events SET status = 'completed', processed_at = $1 WHERE id = $2`, [
        Date.now(),
        event.id,
      ]);
      await client.query("COMMIT");
      return { ok: true };
    } catch (err) {
      await client.query("ROLLBACK");
      const message = err instanceof Error ? err.message : String(err);
      const attempts = event.attempts + 1;
      const status: EventStatus = attempts >= maxAttempts ? "failed" : "pending";
      await client.query(
        `UPDATE webhook_events
         SET status = $1, attempts = $2, last_error = $3, claimed_at = NULL, claimed_by = NULL
         WHERE id = $4`,
        [status, attempts, message, event.id],
      );
      return { ok: false, error: message };
    } finally {
      client.release();
    }
  }

  async getById(id: number): Promise<WebhookEventRecord | null> {
    const result = await this.pool.query<Row>(`SELECT * FROM webhook_events WHERE id = $1`, [id]);
    return result.rows[0] ? toRecord(result.rows[0]) : null;
  }

  async getByDeliveryId(provider: string, deliveryId: string): Promise<WebhookEventRecord | null> {
    const result = await this.pool.query<Row>(
      `SELECT * FROM webhook_events WHERE provider = $1 AND delivery_id = $2`,
      [provider, deliveryId],
    );
    return result.rows[0] ? toRecord(result.rows[0]) : null;
  }

  async listRecent(limit: number): Promise<WebhookEventRecord[]> {
    const result = await this.pool.query<Row>(`SELECT * FROM webhook_events ORDER BY received_at DESC LIMIT $1`, [
      limit,
    ]);
    return result.rows.map(toRecord);
  }

  async requeue(id: number): Promise<WebhookEventRecord | null> {
    await this.pool.query(
      `UPDATE webhook_events
       SET status = 'pending', claimed_at = NULL, claimed_by = NULL, last_error = NULL
       WHERE id = $1`,
      [id],
    );
    return this.getById(id);
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
