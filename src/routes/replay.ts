import type { FastifyInstance } from "fastify";
import type { AppConfig } from "../config.js";
import type { EventHandler, EventStore } from "../store/types.js";

/**
 * Replay tooling is first-class: any stored webhook — successfully
 * processed or not — can be manually re-run through the exact same
 * claim -> handler -> complete path live traffic uses. This is what an
 * on-call engineer reaches for after fixing a downstream bug that ate an
 * event, without needing the provider to redeliver anything.
 */
export function registerReplayRoutes<TxCtx>(
  app: FastifyInstance,
  store: EventStore<TxCtx>,
  handler: EventHandler<TxCtx>,
  config: AppConfig,
): void {
  app.post("/replay/:id", async (request, reply) => {
    const { id } = request.params as { id: string };
    const eventId = Number(id);
    if (!Number.isInteger(eventId)) {
      reply.code(400);
      return { error: "id must be an integer" };
    }

    const existing = await store.getById(eventId);
    if (!existing) {
      reply.code(404);
      return { error: "event not found" };
    }

    await store.requeue(eventId);
    const claimed = await store.claimNext(`replay-${eventId}-${Date.now()}`, 0);
    if (!claimed || claimed.id !== eventId) {
      // Lost the claim race to the live poll worker — that's fine, it will
      // still be processed exactly once, just not synchronously with this
      // request.
      return { status: "requeued", eventId };
    }

    const result = await store.processClaimed(claimed, handler, config.workerMaxAttempts);
    return {
      status: result.ok ? "replayed" : "replay_failed",
      eventId,
      error: result.ok ? undefined : result.error,
    };
  });

  app.get("/events", async (request) => {
    const { limit } = request.query as { limit?: string };
    const events = await store.listRecent(limit ? Number(limit) : 50);
    return { events };
  });
}
