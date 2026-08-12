import Database from "better-sqlite3";
import type {
  EventHandler,
  EventStatus,
  EventStore,
  InsertResult,
  WebhookEventInput,
  WebhookEventRecord,
} from "./types.js";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS webhook_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL,
  delivery_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  headers TEXT NOT NULL,
  received_at INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  claimed_at INTEGER,
  claimed_by TEXT,
  processed_at INTEGER,
  last_error TEXT,
  UNIQUE (provider, delivery_id)
);
CREATE INDEX IF NOT EXISTS idx_webhook_events_status ON webhook_events (status, received_at);
`;

interface Row {
  id: number;
  provider: string;
  delivery_id: string;
  event_type: string;
  payload: string;
  headers: string;
  received_at: number;
  status: EventStatus;
  attempts: number;
  claimed_at: number | null;
  claimed_by: string | null;
  processed_at: number | null;
  last_error: string | null;
}

function toRecord(row: Row): WebhookEventRecord {
  return {
    id: row.id,
    provider: row.provider,
    deliveryId: row.delivery_id,
    eventType: row.event_type,
    payload: row.payload,
    headers: row.headers,
    receivedAt: row.received_at,
    status: row.status,
    attempts: row.attempts,
    claimedAt: row.claimed_at,
    claimedBy: row.claimed_by,
    processedAt: row.processed_at,
    lastError: row.last_error,
  };
}

/** Matches SQLite's UNIQUE constraint violation, however node-better-sqlite3 surfaces it. */
function isUniqueViolation(err: unknown): boolean {
  const code = (err as { code?: string } | null)?.code;
  if (code === "SQLITE_CONSTRAINT_UNIQUE" || code === "SQLITE_CONSTRAINT_PRIMARYKEY") return true;
  const message = err instanceof Error ? err.message : String(err);
  return message.includes("UNIQUE constraint failed");
}

/**
 * Real SQLite-backed adapter used throughout the test suite to prove the
 * dedup mechanism. better-sqlite3 gives synchronous, genuinely transactional
 * access with real UNIQUE constraints and real SQLITE_BUSY-based lock
 * contention when multiple connections (e.g. separate worker_threads, as
 * used in the concurrency test) hit the same file at once.
 */
export class SqliteEventStore implements EventStore<Database.Database> {
  private db: Database.Database;

  constructor(filename: string) {
    this.db = new Database(filename);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("busy_timeout = 5000");
    this.db.pragma("foreign_keys = ON");
    this.db.exec(SCHEMA);
  }

  async insertEvent(input: WebhookEventInput): Promise<InsertResult> {
    try {
      const info = this.db
        .prepare(
          `INSERT INTO webhook_events
             (provider, delivery_id, event_type, payload, headers, received_at, status, attempts)
           VALUES (?, ?, ?, ?, ?, ?, 'pending', 0)`,
        )
        .run(input.provider, input.deliveryId, input.eventType, input.payload, input.headers, input.receivedAt);
      const event = await this.getById(Number(info.lastInsertRowid));
      if (!event) throw new Error("insert succeeded but row could not be re-read");
      return { inserted: true, event };
    } catch (err) {
      if (isUniqueViolation(err)) {
        const existing = await this.getByDeliveryId(input.provider, input.deliveryId);
        if (existing) return { inserted: false, event: existing };
      }
      throw err;
    }
  }

  async claimNext(workerId: string, staleClaimMs: number): Promise<WebhookEventRecord | null> {
    const now = Date.now();
    const staleThreshold = now - staleClaimMs;

    const claim = this.db.transaction((): number | null => {
      const row = this.db
        .prepare(
          `SELECT id FROM webhook_events
           WHERE status = 'pending' OR (status = 'processing' AND claimed_at < ?)
           ORDER BY received_at ASC
           LIMIT 1`,
        )
        .get(staleThreshold) as { id: number } | undefined;
      if (!row) return null;

      const info = this.db
        .prepare(
          `UPDATE webhook_events
           SET status = 'processing', claimed_at = ?, claimed_by = ?
           WHERE id = ? AND (status = 'pending' OR (status = 'processing' AND claimed_at < ?))`,
        )
        .run(now, workerId, row.id, staleThreshold);
      return info.changes === 1 ? row.id : null;
    });

    const id = claim();
    if (id === null) return null;
    return this.getById(id);
  }

  async processClaimed(
    event: WebhookEventRecord,
    handler: EventHandler<Database.Database>,
    maxAttempts: number,
  ): Promise<{ ok: true } | { ok: false; error: string }> {
    // Manual BEGIN/COMMIT (rather than db.transaction(), which requires a
    // synchronous callback) so the handler can be a real async function —
    // mirroring how the Postgres adapter awaits a client inside BEGIN/COMMIT.
    this.db.exec("BEGIN IMMEDIATE");
    try {
      await handler(event, this.db);
      this.db
        .prepare(`UPDATE webhook_events SET status = 'completed', processed_at = ? WHERE id = ?`)
        .run(Date.now(), event.id);
      this.db.exec("COMMIT");
      return { ok: true };
    } catch (err) {
      this.db.exec("ROLLBACK");
      const message = err instanceof Error ? err.message : String(err);
      const attempts = event.attempts + 1;
      const status: EventStatus = attempts >= maxAttempts ? "failed" : "pending";
      this.db
        .prepare(
          `UPDATE webhook_events
           SET status = ?, attempts = ?, last_error = ?, claimed_at = NULL, claimed_by = NULL
           WHERE id = ?`,
        )
        .run(status, attempts, message, event.id);
      return { ok: false, error: message };
    }
  }

  async getById(id: number): Promise<WebhookEventRecord | null> {
    const row = this.db.prepare(`SELECT * FROM webhook_events WHERE id = ?`).get(id) as Row | undefined;
    return row ? toRecord(row) : null;
  }

  async getByDeliveryId(provider: string, deliveryId: string): Promise<WebhookEventRecord | null> {
    const row = this.db
      .prepare(`SELECT * FROM webhook_events WHERE provider = ? AND delivery_id = ?`)
      .get(provider, deliveryId) as Row | undefined;
    return row ? toRecord(row) : null;
  }

  async listRecent(limit: number): Promise<WebhookEventRecord[]> {
    const rows = this.db
      .prepare(`SELECT * FROM webhook_events ORDER BY received_at DESC LIMIT ?`)
      .all(limit) as Row[];
    return rows.map(toRecord);
  }

  async requeue(id: number): Promise<WebhookEventRecord | null> {
    this.db
      .prepare(
        `UPDATE webhook_events
         SET status = 'pending', claimed_at = NULL, claimed_by = NULL, last_error = NULL
         WHERE id = ?`,
      )
      .run(id);
    return this.getById(id);
  }

  async close(): Promise<void> {
    this.db.close();
  }

  /** Escape hatch for callers (processor.ts, tests) that need to extend the schema or run handler-side inserts in the same database file. */
  get raw(): Database.Database {
    return this.db;
  }
}
