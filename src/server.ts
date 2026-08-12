import Fastify, { type FastifyInstance } from "fastify";
import type { AppConfig } from "./config.js";
import { loadConfig } from "./config.js";
import { ensureEffectsSchema, sqliteDownstreamHandler } from "./processor.js";
import { registerReplayRoutes } from "./routes/replay.js";
import { registerWebhookRoutes } from "./routes/webhook.js";
import { SqliteEventStore } from "./store/sqlite-store.js";
import type { EventHandler, EventStore } from "./store/types.js";
import { Worker } from "./worker.js";

/** Assembles the HTTP surface only — no worker, no listening socket — so tests can drive it directly against an in-memory store. */
export function buildApp<TxCtx>(
  store: EventStore<TxCtx>,
  config: AppConfig,
  handler: EventHandler<TxCtx>,
): FastifyInstance {
  const app = Fastify({ logger: false });

  // Keep the exact raw bytes of the request body so signature verification
  // runs against what the provider actually sent, not a re-serialized copy.
  app.addContentTypeParser("application/json", { parseAs: "string" }, (_req, body, done) => {
    done(null, body);
  });

  registerWebhookRoutes(app, store, config);
  registerReplayRoutes(app, store, handler, config);
  app.get("/health", async () => ({ status: "ok" }));

  return app;
}

async function main(): Promise<void> {
  const config = loadConfig();
  const store = new SqliteEventStore(config.sqlitePath);
  ensureEffectsSchema(store.raw);
  const handler = sqliteDownstreamHandler();

  const app = buildApp(store, config, handler);
  const worker = new Worker(store, handler, {
    workerId: `gateway-${process.pid}`,
    pollIntervalMs: config.workerPollIntervalMs,
    staleClaimMs: config.workerStaleClaimMs,
    maxAttempts: config.workerMaxAttempts,
  });
  worker.start();

  await app.listen({ port: config.port, host: "0.0.0.0" });
  console.log(`idempotent-webhook-gateway listening on :${config.port} (sqlite: ${config.sqlitePath})`);

  const shutdown = async (signal: string) => {
    console.log(`received ${signal}, shutting down`);
    worker.stop();
    await app.close();
    await store.close();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

const isMain = process.argv[1] && import.meta.url === `file://${process.argv[1]}`;
if (isMain) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
