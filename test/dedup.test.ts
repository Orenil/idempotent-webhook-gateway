import { fileURLToPath } from "node:url";
import { Worker as ThreadWorker } from "node:worker_threads";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { countEffects, ensureEffectsSchema, sqliteDownstreamHandler } from "../src/processor.js";
import { SqliteEventStore } from "../src/store/sqlite-store.js";
import { Worker } from "../src/worker.js";
import { stripePayload, tempDbPath } from "./helpers.js";

const WORKER_SCRIPT = fileURLToPath(new URL("./concurrent-insert-worker.mjs", import.meta.url));

interface ThreadResult {
  workerIndex: number;
  inserted: boolean;
  rowId?: number;
  error?: string;
}

function runConcurrentInsert(
  dbPath: string,
  n: number,
  fields: { provider: string; deliveryId: string; eventType: string; payload: string; headers: string; receivedAt: number },
): Promise<ThreadResult[]> {
  const promises: Promise<ThreadResult>[] = [];
  for (let i = 0; i < n; i++) {
    promises.push(
      new Promise((resolve, reject) => {
        const thread = new ThreadWorker(WORKER_SCRIPT, {
          workerData: { dbPath, workerIndex: i, ...fields },
        });
        thread.once("message", (msg: ThreadResult) => resolve(msg));
        thread.once("error", reject);
      }),
    );
  }
  // Promise.all starts every thread's constructor synchronously in this
  // loop before any of them can finish, so their inserts genuinely race
  // each other rather than running one-at-a-time.
  return Promise.all(promises);
}

describe("dedup via unique constraint — concurrent duplicate delivery", () => {
  let dbPath: string;
  let store: SqliteEventStore;

  beforeEach(() => {
    dbPath = tempDbPath("dedup-test");
    store = new SqliteEventStore(dbPath);
    ensureEffectsSchema(store.raw);
  });

  afterEach(async () => {
    await store.close();
  });

  it("lets exactly one of N truly concurrent identical deliveries create the row", async () => {
    const N = 25;
    const deliveryId = "evt_concurrent_race";
    const fields = {
      provider: "stripe",
      deliveryId,
      eventType: "payment_intent.succeeded",
      payload: stripePayload(deliveryId, 5000),
      headers: "{}",
      receivedAt: Date.now(),
    };

    const results = await runConcurrentInsert(dbPath, N, fields);

    const winners = results.filter((r) => r.inserted);
    const losers = results.filter((r) => !r.inserted);
    expect(winners).toHaveLength(1);
    expect(losers).toHaveLength(N - 1);
    // every loser lost to the UNIQUE constraint specifically, not some other error
    for (const loser of losers) expect(loser.error).toBeUndefined();

    // exactly one row exists for this delivery id, no matter how many threads raced to create it
    const stored = await store.getByDeliveryId("stripe", deliveryId);
    expect(stored).not.toBeNull();

    // Now drain the async worker: even though 25 HTTP-equivalent deliveries
    // arrived, only one outbox row ever existed, so the downstream side
    // effect can only ever fire once.
    const handler = sqliteDownstreamHandler();
    const worker = new Worker(store, handler, {
      workerId: "test-worker",
      pollIntervalMs: 10,
      staleClaimMs: 30_000,
      maxAttempts: 3,
    });
    const processed = await worker.drain();
    expect(processed).toBe(1);
    expect(countEffects(store.raw, "stripe", deliveryId)).toBe(1);

    // A late-arriving retry from the provider (the classic "already
    // processed but they retry anyway because our ACK was slow") still
    // cannot create a second row or trigger a second effect.
    const retry = await store.insertEvent(fields);
    expect(retry.inserted).toBe(false);
    const processedAgain = await worker.drain();
    expect(processedAgain).toBe(0);
    expect(countEffects(store.raw, "stripe", deliveryId)).toBe(1);
  }, 20_000);

  it("keeps independent delivery IDs fully independent", async () => {
    const handler = sqliteDownstreamHandler();
    for (const id of ["evt_a", "evt_b", "evt_c"]) {
      await store.insertEvent({
        provider: "stripe",
        deliveryId: id,
        eventType: "payment_intent.succeeded",
        payload: stripePayload(id),
        headers: "{}",
        receivedAt: Date.now(),
      });
    }
    const worker = new Worker(store, handler, {
      workerId: "w",
      pollIntervalMs: 10,
      staleClaimMs: 30_000,
      maxAttempts: 3,
    });
    const processed = await worker.drain();
    expect(processed).toBe(3);
    expect(countEffects(store.raw, "stripe", "evt_a")).toBe(1);
    expect(countEffects(store.raw, "stripe", "evt_b")).toBe(1);
    expect(countEffects(store.raw, "stripe", "evt_c")).toBe(1);
  });
});
