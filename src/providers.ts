import { verifyCrmStyle, verifyStripeStyle, verifyTelephonyStyle, type VerifyResult } from "./signature.js";

export interface ProviderConfig {
  /** URL slug: POST /webhooks/:provider */
  slug: string;
  /** Human label for logs/README, e.g. "payment processor" */
  kind: string;
  /** Header carrying the HMAC signature */
  signatureHeader: string;
  verify(rawBody: string, headerValue: string | undefined, secret: string): VerifyResult;
  /** Pull the provider's own delivery/event id out of the parsed JSON body. */
  extractDeliveryId(payload: Record<string, unknown>): string | null;
  extractEventType(payload: Record<string, unknown>): string;
  secretEnvVar: string;
}

function stringField(payload: Record<string, unknown>, key: string): string | null {
  const value = payload[key];
  return typeof value === "string" && value.length > 0 ? value : null;
}

export const PROVIDERS: Record<string, ProviderConfig> = {
  stripe: {
    slug: "stripe",
    kind: "payment processor",
    signatureHeader: "stripe-signature",
    verify: verifyStripeStyle,
    extractDeliveryId: (payload) => stringField(payload, "id"),
    extractEventType: (payload) => stringField(payload, "type") ?? "unknown",
    secretEnvVar: "STRIPE_WEBHOOK_SECRET",
  },
  telephony: {
    slug: "telephony",
    kind: "telephony provider",
    signatureHeader: "x-telephony-signature",
    verify: verifyTelephonyStyle,
    extractDeliveryId: (payload) => stringField(payload, "event_id"),
    extractEventType: (payload) => stringField(payload, "status") ?? "unknown",
    secretEnvVar: "TELEPHONY_WEBHOOK_SECRET",
  },
  crm: {
    slug: "crm",
    kind: "CRM",
    signatureHeader: "x-webhook-signature",
    verify: verifyCrmStyle,
    extractDeliveryId: (payload) => stringField(payload, "event_id"),
    extractEventType: (payload) => stringField(payload, "object_type") ?? "unknown",
    secretEnvVar: "CRM_WEBHOOK_SECRET",
  },
};

export function getProvider(slug: string): ProviderConfig | undefined {
  return PROVIDERS[slug];
}
