// Run as a genuinely separate OS process (see worker-recovery.test.ts,
// which spawns this via tsx and SIGKILLs it mid-handler). This is the
// "chaos test killing the gateway mid-processing" from the spec: it claims
// one real event, writes the downstream effect row, then sleeps — giving
// the parent test a window to send SIGKILL before the transaction commits.
import type Database from "better-sqlite3";
import { ensureEffectsSchema } from "../src/processor.js";
import { SqliteEventStore } from "../src/store/sqlite-store.js";
import type { EventHandler } from "../src/store/types.js";

async function main(): Promise<void> {
  const [, , dbPath, workMsRaw] = process.argv;
  const workMs = Number(workMsRaw);
  const store = new SqliteEventStore(dbPath);
  ensureEffectsSchema(store.raw);

  const claimed = await store.claimNext(`chaos-${process.pid}`, 30_000);
  if (!claimed) {
    console.error("chaos worker: nothing claimable");
    process.exit(2);
  }

  const handler: EventHandler<Database.Database> = async (event, db) => {
    // Write the effect FIRST, then stall — proves that even though the
    // INSERT has physically happened inside this uncommitted transaction,
    // a SIGKILL before COMMIT means it never becomes visible to anyone
    // reopening the database afterward.
    db.prepare(
      `INSERT INTO processed_effects (event_id, provider, delivery_id, detail, applied_at) VALUES (?, ?, ?, ?, ?)`,
    ).run(event.id, event.provider, event.deliveryId, "chaos-effect-pre-commit", Date.now());
    console.log("chaos worker: effect written inside uncommitted tx, now stalling");
    await new Promise((resolve) => setTimeout(resolve, workMs));
  };

  await store.processClaimed(claimed, handler, 5);
  // Reaching this line means the parent failed to kill us in time.
  console.log("chaos worker: finished without being killed");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
