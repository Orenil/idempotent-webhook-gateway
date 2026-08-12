// Plain JS (not TS) worker_thread entry point: each thread opens its OWN
// better-sqlite3 connection to the SAME database file and races every other
// thread to insert an identical (provider, delivery_id) pair. This is what
// makes the dedup test in dedup.test.ts a genuine test of the SQL UNIQUE
// constraint under real concurrent access — every thread is a fully
// independent OS-level connection, not a callback sharing one JS call stack.
import Database from "better-sqlite3";
import { parentPort, workerData } from "node:worker_threads";

const { dbPath, provider, deliveryId, eventType, payload, headers, receivedAt, workerIndex } = workerData;

const db = new Database(dbPath);
db.pragma("journal_mode = WAL");
db.pragma("busy_timeout = 10000");

try {
  const info = db
    .prepare(
      `INSERT INTO webhook_events
         (provider, delivery_id, event_type, payload, headers, received_at, status, attempts)
       VALUES (?, ?, ?, ?, ?, ?, 'pending', 0)`,
    )
    .run(provider, deliveryId, eventType, payload, headers, receivedAt);
  parentPort.postMessage({ workerIndex, inserted: true, rowId: Number(info.lastInsertRowid) });
} catch (err) {
  const code = err && typeof err === "object" ? err.code : undefined;
  const message = err instanceof Error ? err.message : String(err);
  const isUnique = code === "SQLITE_CONSTRAINT_UNIQUE" || message.includes("UNIQUE constraint failed");
  if (!isUnique) {
    parentPort.postMessage({ workerIndex, inserted: false, error: message });
  } else {
    parentPort.postMessage({ workerIndex, inserted: false });
  }
} finally {
  db.close();
}
