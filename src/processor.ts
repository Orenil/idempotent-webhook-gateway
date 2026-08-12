import type Database from "better-sqlite3";
import type { EventHandler, WebhookEventRecord } from "./store/types.js";

const EFFECTS_SCHEMA = `
CREATE TABLE IF NOT EXISTS processed_effects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,
  provider TEXT NOT NULL,
  delivery_id TEXT NOT NULL,
  detail TEXT NOT NULL,
  applied_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_processed_effects_delivery ON processed_effects (provider, delivery_id);
`;

export function ensureEffectsSchema(db: Database.Database): void {
  db.exec(EFFECTS_SCHEMA);
}

/**
 * The "downstream side effect" every test in this repo measures: a real DB
 * write standing in for whatever a payment processor / telephony provider /
 * CRM webhook would trigger in production — crediting a ledger, logging a
 * call outcome, syncing a contact. It executes inside the *same* SQLite
 * transaction that marks the outbox row completed (see
 * SqliteEventStore#processClaimed), so "effect applied but not marked
 * completed" is not a reachable state: either the transaction commits both,
 * or it rolls back both and the event is retried from a clean slate.
 */
export function sqliteDownstreamHandler(simulateWorkMs = 0): EventHandler<Database.Database> {
  return async (event: WebhookEventRecord, db: Database.Database) => {
    if (simulateWorkMs > 0) await new Promise((resolve) => setTimeout(resolve, simulateWorkMs));
    const payload = JSON.parse(event.payload) as Record<string, unknown>;
    const detail = describeEffect(event.provider, event.eventType, payload);
    db.prepare(
      `INSERT INTO processed_effects (event_id, provider, delivery_id, detail, applied_at) VALUES (?, ?, ?, ?, ?)`,
    ).run(event.id, event.provider, event.deliveryId, detail, Date.now());
  };
}

function describeEffect(provider: string, eventType: string, payload: Record<string, unknown>): string {
  switch (provider) {
    case "stripe":
      return `credited ledger for ${eventType} amount=${String(payload.amount ?? "?")}`;
    case "telephony":
      return `logged call outcome ${eventType} call=${String(payload.call_sid ?? "?")}`;
    case "crm":
      return `synced ${String(payload.object_type ?? "object")} id=${String(payload.object_id ?? "?")}`;
    default:
      return `processed ${eventType}`;
  }
}

/** Counts how many times the downstream effect actually fired for a delivery — the number every dedup test asserts on. */
export function countEffects(db: Database.Database, provider: string, deliveryId: string): number {
  const row = db
    .prepare(`SELECT COUNT(*) as n FROM processed_effects WHERE provider = ? AND delivery_id = ?`)
    .get(provider, deliveryId) as { n: number };
  return row.n;
}
