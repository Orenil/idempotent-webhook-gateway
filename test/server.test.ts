import type { FastifyInstance } from "fastify";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ensureEffectsSchema, sqliteDownstreamHandler, countEffects } from "../src/processor.js";
import { buildApp } from "../src/server.js";
import { SqliteEventStore } from "../src/store/sqlite-store.js";
import { Worker } from "../src/worker.js";
import { TEST_SECRETS, signCrm, signStripe, signTelephony, stripePayload, tempDbPath } from "./helpers.js";

describe("gateway HTTP surface", () => {
  let store: SqliteEventStore;
  let app: FastifyInstance;
  let worker: Worker<import("better-sqlite3").Database>;

  beforeEach(() => {
    const dbPath = tempDbPath("server-test");
    store = new SqliteEventStore(dbPath);
    ensureEffectsSchema(store.raw);
    const handler = sqliteDownstreamHandler();
    app = buildApp(store, { port: 0, sqlitePath: dbPath, workerPollIntervalMs: 10, workerStaleClaimMs: 30_000, workerMaxAttempts: 3, secrets: TEST_SECRETS }, handler);
    worker = new Worker(store, handler, { workerId: "http-test", pollIntervalMs: 10, staleClaimMs: 30_000, maxAttempts: 3 });
  });

  afterEach(async () => {
    await app.close();
    await store.close();
  });

  it("accepts a validly signed webhook fast, then processes it async", async () => {
    const body = stripePayload("evt_http_1", 1500);
    const signature = signStripe(body, TEST_SECRETS.stripe);

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/stripe",
      headers: { "content-type": "application/json", "stripe-signature": signature },
      payload: body,
    });

    expect(response.statusCode).toBe(202);
    const json = response.json();
    expect(json.status).toBe("accepted");

    // Not yet processed — the ACK happened before any downstream work ran.
    expect(countEffects(store.raw, "stripe", "evt_http_1")).toBe(0);

    await worker.drain();
    expect(countEffects(store.raw, "stripe", "evt_http_1")).toBe(1);
  });

  it("returns 202 once and 200 duplicate for every retry of the same delivery id, with only one effect", async () => {
    const body = stripePayload("evt_http_retry", 2500);
    const signature = signStripe(body, TEST_SECRETS.stripe);
    const req = () =>
      app.inject({
        method: "POST",
        url: "/webhooks/stripe",
        headers: { "content-type": "application/json", "stripe-signature": signature },
        payload: body,
      });

    const first = await req();
    const second = await req();
    const third = await req();

    expect(first.statusCode).toBe(202);
    expect(first.json().status).toBe("accepted");
    expect(second.statusCode).toBe(200);
    expect(second.json().status).toBe("duplicate");
    expect(third.statusCode).toBe(200);
    expect(third.json().status).toBe("duplicate");
    // all three responses point at the same underlying row
    expect(second.json().eventId).toBe(first.json().eventId);
    expect(third.json().eventId).toBe(first.json().eventId);

    await worker.drain();
    expect(countEffects(store.raw, "stripe", "evt_http_retry")).toBe(1);
  });

  it("rejects an incorrectly signed webhook with 401 and never stores it", async () => {
    const body = stripePayload("evt_bad_sig", 100);
    const response = await app.inject({
      method: "POST",
      url: "/webhooks/stripe",
      headers: { "content-type": "application/json", "stripe-signature": "t=1,v1=deadbeef" },
      payload: body,
    });
    expect(response.statusCode).toBe(401);
    const stored = await store.getByDeliveryId("stripe", "evt_bad_sig");
    expect(stored).toBeNull();
  });

  it("rejects a request with no signature header at all", async () => {
    const body = stripePayload("evt_no_sig", 100);
    const response = await app.inject({
      method: "POST",
      url: "/webhooks/stripe",
      headers: { "content-type": "application/json" },
      payload: body,
    });
    expect(response.statusCode).toBe(401);
  });

  it("accepts telephony and crm providers with their own distinct signature schemes", async () => {
    const telephonyBody = JSON.stringify({ event_id: "ev_tel_1", status: "completed", call_sid: "CA1" });
    const telephonyResponse = await app.inject({
      method: "POST",
      url: "/webhooks/telephony",
      headers: {
        "content-type": "application/json",
        "x-telephony-signature": signTelephony(telephonyBody, TEST_SECRETS.telephony),
      },
      payload: telephonyBody,
    });
    expect(telephonyResponse.statusCode).toBe(202);

    const crmBody = JSON.stringify({ event_id: "ev_crm_1", object_type: "contact", object_id: "c1" });
    const crmResponse = await app.inject({
      method: "POST",
      url: "/webhooks/crm",
      headers: { "content-type": "application/json", "x-webhook-signature": signCrm(crmBody, TEST_SECRETS.crm) },
      payload: crmBody,
    });
    expect(crmResponse.statusCode).toBe(202);

    // cross-provider signatures must not validate against the wrong scheme
    const crossed = await app.inject({
      method: "POST",
      url: "/webhooks/crm",
      headers: {
        "content-type": "application/json",
        "x-webhook-signature": signTelephony(crmBody, TEST_SECRETS.crm), // wrong scheme entirely
      },
      payload: crmBody,
    });
    expect(crossed.statusCode).toBe(401);
  });

  it("rejects an unknown provider slug", async () => {
    const response = await app.inject({
      method: "POST",
      url: "/webhooks/nonexistent",
      headers: { "content-type": "application/json" },
      payload: "{}",
    });
    expect(response.statusCode).toBe(404);
  });

  it("rejects a validly-signed payload missing a delivery id", async () => {
    const body = JSON.stringify({ type: "payment_intent.succeeded", amount: 100 }); // no `id`
    const signature = signStripe(body, TEST_SECRETS.stripe);
    const response = await app.inject({
      method: "POST",
      url: "/webhooks/stripe",
      headers: { "content-type": "application/json", "stripe-signature": signature },
      payload: body,
    });
    expect(response.statusCode).toBe(400);
  });

  it("supports replaying a completed event through /replay/:id", async () => {
    const body = stripePayload("evt_replay_me", 999);
    const signature = signStripe(body, TEST_SECRETS.stripe);
    const ingest = await app.inject({
      method: "POST",
      url: "/webhooks/stripe",
      headers: { "content-type": "application/json", "stripe-signature": signature },
      payload: body,
    });
    const eventId = ingest.json().eventId;

    await worker.drain();
    expect(countEffects(store.raw, "stripe", "evt_replay_me")).toBe(1);

    const replay = await app.inject({ method: "POST", url: `/replay/${eventId}` });
    expect(replay.statusCode).toBe(200);
    expect(replay.json().status).toBe("replayed");

    // Replay re-runs the handler; the effect table now has two rows because
    // replay is an intentional, explicit operator action — distinct from
    // an unwanted duplicate delivery, which is prevented at insert time.
    expect(countEffects(store.raw, "stripe", "evt_replay_me")).toBe(2);
  });

  it("404s replay for an event id that does not exist", async () => {
    const response = await app.inject({ method: "POST", url: "/replay/999999" });
    expect(response.statusCode).toBe(404);
  });

  it("lists recent events", async () => {
    const body = stripePayload("evt_list_me", 42);
    const signature = signStripe(body, TEST_SECRETS.stripe);
    await app.inject({
      method: "POST",
      url: "/webhooks/stripe",
      headers: { "content-type": "application/json", "stripe-signature": signature },
      payload: body,
    });
    const response = await app.inject({ method: "GET", url: "/events" });
    expect(response.statusCode).toBe(200);
    const { events } = response.json();
    expect(events.some((e: { deliveryId: string }) => e.deliveryId === "evt_list_me")).toBe(true);
  });

  it("reports health", async () => {
    const response = await app.inject({ method: "GET", url: "/health" });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ status: "ok" });
  });
});
