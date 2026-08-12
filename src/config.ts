export interface AppConfig {
  port: number;
  sqlitePath: string;
  workerPollIntervalMs: number;
  workerStaleClaimMs: number;
  workerMaxAttempts: number;
  secrets: Record<string, string>;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  return {
    port: Number(env.PORT ?? 3000),
    sqlitePath: env.SQLITE_PATH ?? "./data/gateway.db",
    workerPollIntervalMs: Number(env.WORKER_POLL_INTERVAL_MS ?? 250),
    workerStaleClaimMs: Number(env.WORKER_STALE_CLAIM_MS ?? 30_000),
    workerMaxAttempts: Number(env.WORKER_MAX_ATTEMPTS ?? 5),
    secrets: {
      stripe: env.STRIPE_WEBHOOK_SECRET ?? "dev_stripe_secret",
      telephony: env.TELEPHONY_WEBHOOK_SECRET ?? "dev_telephony_secret",
      crm: env.CRM_WEBHOOK_SECRET ?? "dev_crm_secret",
    },
  };
}
