#!/usr/bin/env node
/**
 * Replay CLI — reprocesses a stored webhook directly against the SQLite
 * file, without needing the HTTP gateway running. Intended for incident
 * recovery: `npm run replay -- replay 42` after shipping a fix for a
 * handler bug that silently ate an event.
 */
import { loadConfig } from "./config.js";
import { ensureEffectsSchema, sqliteDownstreamHandler } from "./processor.js";
import { SqliteEventStore } from "./store/sqlite-store.js";

async function main(): Promise<void> {
  const [, , command, arg] = process.argv;
  const config = loadConfig();
  const store = new SqliteEventStore(config.sqlitePath);
  ensureEffectsSchema(store.raw);
  const handler = sqliteDownstreamHandler();

  try {
    if (command === "list") {
      const events = await store.listRecent(arg ? Number(arg) : 20);
      if (events.length === 0) console.log("(no events stored)");
      for (const e of events) {
        console.log(
          `[${e.id}] ${e.provider}/${e.deliveryId} type=${e.eventType} status=${e.status} attempts=${e.attempts}`,
        );
      }
      return;
    }

    if (command === "replay") {
      if (!arg) throw new Error("usage: webhook-replay replay <eventId>");
      const eventId = Number(arg);
      const event = await store.getById(eventId);
      if (!event) throw new Error(`event ${eventId} not found`);

      await store.requeue(eventId);
      const claimed = await store.claimNext(`cli-replay-${process.pid}`, 0);
      if (!claimed || claimed.id !== eventId) {
        console.log(`event ${eventId} was requeued but claimed by another worker before this CLI could grab it`);
        return;
      }

      const result = await store.processClaimed(claimed, handler, config.workerMaxAttempts);
      if (result.ok) {
        console.log(`replayed event ${eventId} (${event.provider}/${event.deliveryId}) successfully`);
      } else {
        console.error(`replay of event ${eventId} failed: ${result.error}`);
        process.exitCode = 1;
      }
      return;
    }

    console.log("usage: webhook-replay <list [limit] | replay <eventId>>");
    process.exitCode = 1;
  } finally {
    await store.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
