import { createHmac } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export function tempDbPath(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), `${prefix}-`));
  return join(dir, "gateway.db");
}

export const TEST_SECRETS = {
  stripe: "test_stripe_secret",
  telephony: "test_telephony_secret",
  crm: "test_crm_secret",
};

export function signStripe(rawBody: string, secret: string, tSeconds = Math.floor(Date.now() / 1000)): string {
  const v1 = createHmac("sha256", secret).update(`${tSeconds}.${rawBody}`).digest("hex");
  return `t=${tSeconds},v1=${v1}`;
}

export function signTelephony(rawBody: string, secret: string): string {
  return createHmac("sha1", secret).update(rawBody).digest("base64");
}

export function signCrm(rawBody: string, secret: string): string {
  return `sha256=${createHmac("sha256", secret).update(rawBody).digest("hex")}`;
}

export function stripePayload(id: string, amount = 4200) {
  return JSON.stringify({
    id,
    type: "payment_intent.succeeded",
    amount,
    data: { object: { id: `pi_${id}` } },
  });
}
