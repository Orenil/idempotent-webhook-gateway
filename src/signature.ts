import { createHmac, timingSafeEqual } from "node:crypto";

/**
 * Constant-time comparison of two hex/base64 strings. Bails out to a length
 * check first (timingSafeEqual throws on mismatched lengths) — the length
 * check itself leaks only the length, never the content, which is the
 * accepted tradeoff every HMAC-comparison implementation makes.
 */
function safeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return timingSafeEqual(bufA, bufB);
}

export interface VerifyResult {
  valid: boolean;
  reason?: string;
}

/**
 * Stripe's real scheme: header `t=<unix seconds>,v1=<hex hmac-sha256>` where
 * the signed payload is `${t}.${rawBody}`. Binding the timestamp into the
 * signed content (rather than just the body) lets us reject old signatures
 * even if the body+secret+signature were captured and replayed later.
 */
export function verifyStripeStyle(
  rawBody: string,
  headerValue: string | undefined,
  secret: string,
  toleranceSeconds = 300,
  now: number = Date.now(),
): VerifyResult {
  if (!headerValue) return { valid: false, reason: "missing signature header" };

  const parts = Object.fromEntries(
    headerValue.split(",").map((kv) => {
      const [k, v] = kv.split("=");
      return [k, v];
    }),
  );
  const t = parts.t;
  const v1 = parts.v1;
  if (!t || !v1) return { valid: false, reason: "malformed signature header" };

  const tSeconds = Number(t);
  if (!Number.isFinite(tSeconds)) return { valid: false, reason: "malformed timestamp" };
  const ageSeconds = Math.abs(now / 1000 - tSeconds);
  if (ageSeconds > toleranceSeconds) return { valid: false, reason: "timestamp outside tolerance" };

  const expected = createHmac("sha256", secret).update(`${t}.${rawBody}`).digest("hex");
  if (!safeEqual(expected, v1)) return { valid: false, reason: "signature mismatch" };
  return { valid: true };
}

/**
 * Telephony-style scheme: base64(hmac-sha1(rawBody, secret)) in a single
 * header, no timestamp binding. Deliberately a different algorithm family
 * (SHA-1/base64 vs SHA-256/hex) from the other providers so the gateway
 * demonstrably supports distinct per-provider verification, not one scheme
 * with the header name swapped.
 */
export function verifyTelephonyStyle(rawBody: string, headerValue: string | undefined, secret: string): VerifyResult {
  if (!headerValue) return { valid: false, reason: "missing signature header" };
  const expected = createHmac("sha1", secret).update(rawBody).digest("base64");
  if (!safeEqual(expected, headerValue)) return { valid: false, reason: "signature mismatch" };
  return { valid: true };
}

/**
 * GitHub/HubSpot-style scheme: `sha256=<hex hmac-sha256(rawBody)>`.
 */
export function verifyCrmStyle(rawBody: string, headerValue: string | undefined, secret: string): VerifyResult {
  if (!headerValue) return { valid: false, reason: "missing signature header" };
  const prefix = "sha256=";
  if (!headerValue.startsWith(prefix)) return { valid: false, reason: "unsupported signature scheme" };
  const provided = headerValue.slice(prefix.length);
  const expected = createHmac("sha256", secret).update(rawBody).digest("hex");
  if (!safeEqual(expected, provided)) return { valid: false, reason: "signature mismatch" };
  return { valid: true };
}
