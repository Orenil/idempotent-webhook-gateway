import { describe, expect, it } from "vitest";
import { verifyCrmStyle, verifyStripeStyle, verifyTelephonyStyle } from "../src/signature.js";
import { signCrm, signStripe, signTelephony } from "./helpers.js";

const SECRET = "shhh";
const BODY = JSON.stringify({ id: "evt_1", type: "thing.happened" });

describe("verifyStripeStyle", () => {
  it("accepts a correctly signed, fresh payload", () => {
    const header = signStripe(BODY, SECRET);
    expect(verifyStripeStyle(BODY, header, SECRET)).toEqual({ valid: true });
  });

  it("rejects a missing header", () => {
    const result = verifyStripeStyle(BODY, undefined, SECRET);
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/missing/);
  });

  it("rejects when the secret does not match", () => {
    const header = signStripe(BODY, "wrong-secret");
    const result = verifyStripeStyle(BODY, header, SECRET);
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/mismatch/);
  });

  it("rejects a tampered body even with a validly-formed signature", () => {
    const header = signStripe(BODY, SECRET);
    const tampered = JSON.stringify({ id: "evt_1", type: "thing.happened", amount: 999999 });
    const result = verifyStripeStyle(tampered, header, SECRET);
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/mismatch/);
  });

  it("rejects a malformed header", () => {
    const result = verifyStripeStyle(BODY, "not-a-real-header", SECRET);
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/malformed/);
  });

  it("rejects a signature whose timestamp is outside the replay tolerance", () => {
    const staleTimestamp = Math.floor(Date.now() / 1000) - 3600; // one hour old
    const header = signStripe(BODY, SECRET, staleTimestamp);
    const result = verifyStripeStyle(BODY, header, SECRET, 300);
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/tolerance/);
  });

  it("accepts a signature at the edge of the tolerance window", () => {
    const timestamp = Math.floor(Date.now() / 1000) - 200;
    const header = signStripe(BODY, SECRET, timestamp);
    const result = verifyStripeStyle(BODY, header, SECRET, 300);
    expect(result.valid).toBe(true);
  });
});

describe("verifyTelephonyStyle", () => {
  it("accepts a correctly signed payload", () => {
    const header = signTelephony(BODY, SECRET);
    expect(verifyTelephonyStyle(BODY, header, SECRET)).toEqual({ valid: true });
  });

  it("rejects an incorrect signature", () => {
    const result = verifyTelephonyStyle(BODY, "bm90LWEtcmVhbC1zaWc=", SECRET);
    expect(result.valid).toBe(false);
  });

  it("rejects a missing header", () => {
    const result = verifyTelephonyStyle(BODY, undefined, SECRET);
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/missing/);
  });
});

describe("verifyCrmStyle", () => {
  it("accepts a correctly signed payload", () => {
    const header = signCrm(BODY, SECRET);
    expect(verifyCrmStyle(BODY, header, SECRET)).toEqual({ valid: true });
  });

  it("rejects a header missing the sha256= prefix", () => {
    const badHeader = createHmacHex(BODY, SECRET);
    const result = verifyCrmStyle(BODY, badHeader, SECRET);
    expect(result.valid).toBe(false);
    expect(result.reason).toMatch(/unsupported/);
  });

  it("rejects a tampered body", () => {
    const header = signCrm(BODY, SECRET);
    const result = verifyCrmStyle(BODY + " ", header, SECRET);
    expect(result.valid).toBe(false);
  });
});

function createHmacHex(body: string, secret: string): string {
  // deliberately missing the "sha256=" prefix verifyCrmStyle requires
  return signCrm(body, secret).replace("sha256=", "");
}
