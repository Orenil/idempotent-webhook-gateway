import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { countEffects, ensureEffectsSchema, sqliteDownstreamHandler } from "../src/processor.js";
import { SqliteEventStore } from "../src/store/sqlite-store.js";
import type { EventHandler } from "../src/store/types.js";
import { Worker } from "../src/worker.js";
import { stripePayload, tempDbPath } from "./helpers.js";

const TSX_BIN = fileURLToPath(new URL("../node_modules/.bin/tsx", import.meta.url));
const CHAOS_SCRIPT = fileURLToPath(new URL("./chaos-crash-worker.ts", import.meta.url));

function waitForStdout(child: ReturnType<typeof spawn>, pattern: RegExp, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for ${pattern}`)), timeoutMs);
    child.stdout?.on("data", (chunk: Buffer) => {
      if (pattern.test(chunk.toString())) {
        clearTimeout(timer);
        resolve();
      }
    });
    child.once("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

function waitForExit(child: ReturnType<typeof spawn>): Promise<number | null> {
  return new Promise((resolve) => child.once("exit", (code) => resolve(code)));
}

describe("crash / restart recovery", () => {
  let dbPath: string;
  let store: SqliteEventStore;

  beforeEach(() => {
    dbPath = tempDbPath("recovery-test");
    store = new SqliteEventStore(dbPath);
    ensureEffectsSchema(store.raw);
  });

  afterEach(async () => {
    await store.close();
  });

  it(
    "SIGKILLing the gateway mid-handler loses the uncommitted effect, and restart reprocesses exactly once",
    async () => {
      const deliveryId = "evt_chaos_kill";
      await store.insertEvent({
        provider: "stripe",
        deliveryId,
        eventType: "payment_intent.succeeded",
        payload: stripePayload(deliveryId, 7777),
        headers: "{}",
        receivedAt: Date.now(),
      });

      // `detached: true` makes this child the leader of its own process
      // group. That matters because `tsx` itself re-execs a grandchild node
      // process to actually run the script — SIGKILLing just the direct
      // child leaves that grandchild as an orphan that keeps running to
      // completion, silently defeating the whole point of this test. Killing
      // the negated pid kills the whole group in one shot.
      const child = spawn(TSX_BIN, [CHAOS_SCRIPT, dbPath, "5000"], {
        stdio: ["ignore", "pipe", "pipe"],
        detached: true,
      });
      let stderr = "";
      child.stderr?.on("data", (c: Buffer) => (stderr += c.toString()));

      // Wait until the child has genuinely written the effect row inside its
      // still-open, uncommitted transaction, then kill it hard — no chance
      // for its own catch/rollback code to run.
      await waitForStdout(child, /stalling/, 5000);
      process.kill(-child.pid!, "SIGKILL");
      const exitCode = await waitForExit(child);
      expect(exitCode).not.toBe(0); // confirms it actually died, not exited cleanly
      expect(stderr).not.toMatch(/nothing claimable/);
      await new Promise((resolve) => setTimeout(resolve, 200)); // let the OS fully tear down the killed process's fds

      // Reopen the database as a brand new connection — simulating the
      // gateway process restarting — and inspect the damage directly.
      const postCrashStore = new SqliteEventStore(dbPath);
      ensureEffectsSchema(postCrashStore.raw);

      const eventAfterCrash = await postCrashStore.getByDeliveryId("stripe", deliveryId);
      expect(eventAfterCrash).not.toBeNull();
      // The claim (a separate, already-committed autocommit statement) survived.
      expect(eventAfterCrash!.status).toBe("processing");
      // But the effect insert, made inside the transaction that was never
      // committed, did NOT survive — proving the crash could not leave a
      // partially-applied side effect behind.
      expect(countEffects(postCrashStore.raw, "stripe", deliveryId)).toBe(0);

      // Recovery: a worker with a short stale-claim window reclaims the
      // orphaned "processing" row and reprocesses it through the real handler.
      const handler = sqliteDownstreamHandler();
      const worker = new Worker(postCrashStore, handler, {
        workerId: "recovered-worker",
        pollIntervalMs: 10,
        staleClaimMs: 5, // the crashed claim is already far older than 5ms
        maxAttempts: 3,
      });
      const processed = await worker.drain();
      expect(processed).toBe(1);

      const eventAfterRecovery = await postCrashStore.getByDeliveryId("stripe", deliveryId);
      expect(eventAfterRecovery!.status).toBe("completed");
      // Exactly one effect: no data loss (it did get processed) and no
      // duplicate processing (the lost pre-crash write doesn't count twice).
      expect(countEffects(postCrashStore.raw, "stripe", deliveryId)).toBe(1);

      await postCrashStore.close();
    },
    15_000,
  );

  it("rolls back a handler that throws, retries, and eventually succeeds exactly once", async () => {
    const deliveryId = "evt_flaky_handler";
    await store.insertEvent({
      provider: "crm",
      deliveryId,
      eventType: "contact.updated",
      payload: JSON.stringify({ event_id: deliveryId, object_type: "contact", object_id: "c_1" }),
      headers: "{}",
      receivedAt: Date.now(),
    });

    let attemptCount = 0;
    const flakyHandler: EventHandler<import("better-sqlite3").Database> = async (event, db) => {
      attemptCount++;
      db.prepare(
        `INSERT INTO processed_effects (event_id, provider, delivery_id, detail, applied_at) VALUES (?, ?, ?, ?, ?)`,
      ).run(event.id, event.provider, event.deliveryId, "attempt", Date.now());
      if (attemptCount < 3) throw new Error(`simulated downstream failure #${attemptCount}`);
    };

    // Drive claim/process directly (rather than Worker#drain, which loops
    // until nothing is claimable — a retried event becomes claimable again
    // immediately, so drain() would race through all 3 attempts in one
    // call). Stepping through by hand lets us assert the state in between.
    for (let attempt = 1; attempt <= 2; attempt++) {
      const claimed = await store.claimNext("flaky-worker", 30_000);
      expect(claimed).not.toBeNull();
      const result = await store.processClaimed(claimed!, flakyHandler, 5);
      expect(result.ok).toBe(false);
      // Each failed attempt rolls its INSERT back along with the failure —
      // the effect table never accumulates a row for a failed attempt.
      expect(countEffects(store.raw, "crm", deliveryId)).toBe(0);
      const afterFailure = await store.getByDeliveryId("crm", deliveryId);
      expect(afterFailure!.status).toBe("pending");
      expect(afterFailure!.attempts).toBe(attempt);
    }

    const finalClaim = await store.claimNext("flaky-worker", 30_000);
    const finalResult = await store.processClaimed(finalClaim!, flakyHandler, 5);
    expect(finalResult.ok).toBe(true);

    const final = await store.getByDeliveryId("crm", deliveryId);
    expect(final!.status).toBe("completed");
    expect(countEffects(store.raw, "crm", deliveryId)).toBe(1);
    expect(attemptCount).toBe(3);
  });

  it("marks an event failed after exhausting max attempts, without ever recording a partial effect", async () => {
    const deliveryId = "evt_always_fails";
    await store.insertEvent({
      provider: "telephony",
      deliveryId,
      eventType: "call.completed",
      payload: JSON.stringify({ event_id: deliveryId, call_sid: "CA1" }),
      headers: "{}",
      receivedAt: Date.now(),
    });

    const alwaysFails: EventHandler<import("better-sqlite3").Database> = async () => {
      throw new Error("downstream permanently unavailable");
    };
    const worker = new Worker(store, alwaysFails, {
      workerId: "doomed-worker",
      pollIntervalMs: 5,
      staleClaimMs: 30_000,
      maxAttempts: 2,
    });

    await worker.drain();
    await worker.drain();

    const final = await store.getByDeliveryId("telephony", deliveryId);
    expect(final!.status).toBe("failed");
    expect(final!.attempts).toBe(2);
    expect(countEffects(store.raw, "telephony", deliveryId)).toBe(0);
  });
});
