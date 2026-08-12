import type { FastifyInstance } from "fastify";
import type { AppConfig } from "../config.js";
import { getProvider } from "../providers.js";
import type { EventStore } from "../store/types.js";

/**
 * Signature verification and payload normalization happen here, at the
 * edge, before anything touches the store. Once verified, the raw webhook
 * is durably inserted and the HTTP response is sent immediately — the
 * gateway never blocks the response on downstream processing.
 */
export function registerWebhookRoutes<TxCtx>(
  app: FastifyInstance,
  store: EventStore<TxCtx>,
  config: AppConfig,
): void {
  app.post("/webhooks/:provider", async (request, reply) => {
    const { provider: slug } = request.params as { provider: string };
    const provider = getProvider(slug);
    if (!provider) {
      reply.code(404);
      return { error: `unknown provider: ${slug}` };
    }

    // Registered via a custom content-type parser (see server.ts) so we
    // verify the signature over the exact bytes that were sent, before any
    // JSON parsing/normalization can change them.
    const rawBody = request.body as string;
    const headerValue = request.headers[provider.signatureHeader] as string | undefined;
    const secret = config.secrets[provider.slug];

    const verification = provider.verify(rawBody, headerValue, secret);
    if (!verification.valid) {
      reply.code(401);
      return { error: "signature verification failed", reason: verification.reason };
    }

    let payload: Record<string, unknown>;
    try {
      payload = JSON.parse(rawBody) as Record<string, unknown>;
    } catch {
      reply.code(400);
      return { error: "invalid JSON payload" };
    }

    const deliveryId = provider.extractDeliveryId(payload);
    if (!deliveryId) {
      reply.code(400);
      return { error: "payload missing a delivery id" };
    }

    const eventType = provider.extractEventType(payload);
    const headersSubset = JSON.stringify({
      [provider.signatureHeader]: headerValue,
      "content-type": request.headers["content-type"],
    });

    // The unique constraint on (provider, delivery_id) is the entire dedup
    // mechanism: `inserted` is true only for the delivery that actually won
    // the race to create the row. Every retry of the same delivery ID lands
    // here and gets `inserted: false` against the exact same row.
    const { inserted, event } = await store.insertEvent({
      provider: provider.slug,
      deliveryId,
      eventType,
      payload: rawBody,
      headers: headersSubset,
      receivedAt: Date.now(),
    });

    reply.code(inserted ? 202 : 200);
    return {
      status: inserted ? "accepted" : "duplicate",
      eventId: event.id,
      deliveryId: event.deliveryId,
    };
  });
}
