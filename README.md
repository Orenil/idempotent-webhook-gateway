# idempotent-webhook-gateway

A webhook ingestion gateway that guarantees **exactly-once downstream processing** for providers that only guarantee **at-least-once delivery** — payment processors, telephony providers, CRMs, and basically every other webhook sender in existence.

## The problem

Every external webhook provider retries on anything other than a fast 2xx: timeouts, 5xx, connection resets, even a slow response. That's the correct design on their end — it's what makes delivery reliable in the face of network failures. But it pushes a hard requirement onto the receiver: **the same delivery can, and will, arrive more than once**, and naive handling turns that into double-charging a payment, double-logging a call outcome, or double-syncing a CRM contact.

The fix people reach for first — an in-memory `Set` of seen IDs, or a Redis `SETNX` — works until the process restarts, at which point the set is empty and the next retry sails through as if it were new. Real durability requires the "have I seen this before" check to survive the process dying, and to be atomic with the decision to actually process the event.

## Architecture

```
                                 ┌─────────────────────────┐
  provider POST  ──────────────▶│  edge: verify signature  │
  (at-least-once,                │  parse + extract         │
   retries on non-2xx)           │  delivery id              │
                                 └─────────────┬─────────────┘
                                               │
                                               ▼
                                 ┌─────────────────────────┐
                                 │ INSERT ... ON CONFLICT   │──▶ 202 (accepted) or
                                 │ UNIQUE(provider,         │    200 (duplicate)
                                 │ delivery_id) DO NOTHING  │    — response sent here,
                                 └─────────────┬─────────────┘    before any processing
                                               │  durably stored
                                               ▼
                                 ┌─────────────────────────┐
                                 │  outbox table (SQLite /  │
                                 │  Postgres) — status:     │
                                 │  pending → processing →  │
                                 │  completed | failed      │
                                 └─────────────┬─────────────┘
                                               │  polled
                                               ▼
                                 ┌─────────────────────────┐
                                 │  worker: claim → run     │
                                 │  handler + mark complete │
                                 │  in ONE transaction      │
                                 └─────────────────────────┘
                                               ▲
                                 replay CLI / POST /replay/:id
                                 re-enters at "claim", same path
```

### Design decisions

**Idempotency via a durable outbox with a UNIQUE constraint, not Redis dedup.** The inbound webhook is written to a table with `UNIQUE(provider, delivery_id)` inside the same insert that will eventually trigger downstream processing. If the provider retries, the second insert violates the constraint and is turned into a no-op response — the database enforces the guarantee, not application code racing an in-memory check against a write. This survives gateway restarts (a Redis-only dedup set does not, unless you also make Redis itself durable and consistent with your processing state, at which point you've just rebuilt this) and requires no separate coordination service.

**ACK fast, process async.** The HTTP response is sent as soon as the event is durably stored — before any downstream side effect runs. This is what makes providers stop retrying (they see a fast 2xx) without coupling the response latency to however long the actual business logic takes. A worker polls the same table and claims rows to process independently of the HTTP request/response cycle.

**The handler runs inside the same transaction as marking the event `completed`.** This is the second half of the exactly-once guarantee: it's not enough to dedup on ingestion if the *processing step itself* can partially apply and then crash before recording that it did. By running the handler's side effect and the `completed` status update in one transaction, a crash between them is not a reachable state — either both commit or neither does, and a restart safely reprocesses from scratch. See the chaos test below for a real proof of this, not just an argument.

**Claim via an atomic conditional UPDATE (SQLite) / `SELECT ... FOR UPDATE SKIP LOCKED` (Postgres), not an app-level mutex.** Two workers can safely poll the same table; only one will win the claim for a given row, and a worker that dies mid-processing leaves the row visibly `processing` with a `claimed_at` timestamp that a stale-claim sweep later reclaims.

**Signature verification and payload normalization happen at the edge, before storage.** An unverified or unparseable payload never reaches the outbox at all — it's rejected with 401/400 before touching the database. Each provider gets its own verification scheme (see `src/signature.ts`): Stripe-style HMAC-SHA256 with a timestamp bound into the signed content (defeats replay of an old, valid signature+body pair), a SHA-1/base64 telephony-style scheme, and a GitHub/HubSpot-style `sha256=<hex>` CRM scheme — genuinely different algorithms, not one scheme with the header name swapped.

**Replay is first-class, not a debugging afterthought.** Any stored webhook — processed or not — can be re-run through `POST /replay/:id` or the `webhook-replay` CLI, using the exact same claim → handler → complete path live traffic uses. This is what you reach for after shipping a fix for a handler bug that silently dropped an event, without needing the provider to redeliver anything.

### Rejected tradeoffs

- **Redis `SETNX` for dedup.** Rejected because it must be durable across gateway restarts and transactionally consistent with the decision to process — a separate cache invalidation/TTL policy is one more thing to get wrong, and a cache miss after a restart is exactly the failure mode this project exists to prevent. Postgres already gives us durable, transactional uniqueness for free.
- **Deduping only in application code (`if (seen.has(id)) return`).** Rejected because it's inherently racy under concurrent delivery of the same ID (see the concurrency test) unless backed by a real lock, and a lock you build yourself is strictly worse than a UNIQUE constraint the database already enforces correctly.
- **Synchronous downstream processing inside the webhook handler.** Rejected because it couples response latency (and therefore the provider's retry behavior) to however long the business logic takes, and a slow or flaky downstream call becomes a slow or flaky webhook ACK, which providers interpret as a delivery failure and retry — creating more duplicate load exactly when the system is already struggling.
- **BullMQ/Redis-backed queue for this project's scope.** Rejected for now in favor of a Postgres/SQLite-native polling worker, since the outbox table is already the durability source of truth — see "Production notes" below for exactly where BullMQ or `LISTEN`/`NOTIFY` would slot in at higher throughput.

### Production notes (documented, not built here)

- The `EventStore` interface (`src/store/types.ts`) is implemented twice: `SqliteEventStore` (real, exercised by the entire test suite) and `PostgresEventStore` (real `pg` client code with the actual `UNIQUE`/`ON CONFLICT`/`FOR UPDATE SKIP LOCKED` SQL, correct but not run in this environment — there's no live Postgres here). Swapping the gateway to Postgres is a one-line change in `server.ts`.
- The worker (`src/worker.ts`) polls. `postgres-store.ts` includes a `LISTEN`/`NOTIFY` trigger definition in its schema comment for waking workers immediately instead of waiting out the poll interval — polling stays as a fallback because `NOTIFY` delivery is best-effort and a disconnected worker must still catch up by polling.
- At meaningfully higher throughput, replace the poll worker with a BullMQ worker fed by the same outbox insert (transactional outbox → queue publish), keeping Postgres as the durability source of truth and Redis purely as the dispatch layer.

## Setup

Requires Node 20+.

```bash
git clone https://github.com/Orenil/idempotent-webhook-gateway.git
cd idempotent-webhook-gateway
npm install
npm test        # runs the full suite against a real SQLite backend
npm run build
npm start        # starts the gateway on :3000 against ./data/gateway.db
```

Environment variables (all optional, sane defaults for local use):

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `3000` | HTTP port |
| `SQLITE_PATH` | `./data/gateway.db` | outbox database file |
| `WORKER_POLL_INTERVAL_MS` | `250` | idle poll interval |
| `WORKER_STALE_CLAIM_MS` | `30000` | how long a `processing` row can go unclaimed-by-progress before another worker reclaims it |
| `WORKER_MAX_ATTEMPTS` | `5` | retries before an event is marked `failed` |
| `STRIPE_WEBHOOK_SECRET` / `TELEPHONY_WEBHOOK_SECRET` / `CRM_WEBHOOK_SECRET` | dev defaults | per-provider HMAC secrets |

## Usage

Real output from actually running the built server against a fresh SQLite file, sending a signed Stripe-style webhook and then retrying the identical delivery twice:

```
$ node dist/server.js
idempotent-webhook-gateway listening on :3987 (sqlite: /tmp/gw-demo/gateway.db)

$ curl -X POST http://localhost:3987/webhooks/stripe \
    -H "content-type: application/json" \
    -H "stripe-signature: t=1786568309,v1=a03addb6d1537ecf05b36b5c3d9266163e967f3a93a197c1afa061e5797781d6" \
    -d '{"id":"evt_demo_001","type":"payment_intent.succeeded","amount":4999,"data":{"object":{"id":"pi_demo_001"}}}'
{"status":"accepted","eventId":1,"deliveryId":"evt_demo_001"}
HTTP 202

# provider retries the exact same delivery — no downstream side effect fires again
$ curl -X POST http://localhost:3987/webhooks/stripe  ... (identical body + signature)
{"status":"duplicate","eventId":1,"deliveryId":"evt_demo_001"}
HTTP 200

$ curl -X POST http://localhost:3987/webhooks/stripe  ... (identical body + signature, again)
{"status":"duplicate","eventId":1,"deliveryId":"evt_demo_001"}
HTTP 200

$ curl http://localhost:3987/events
{
  "events": [
    {
      "id": 1, "provider": "stripe", "deliveryId": "evt_demo_001",
      "eventType": "payment_intent.succeeded", "status": "completed",
      "attempts": 0, "claimedAt": 1786568309164, "processedAt": 1786568309164,
      "lastError": null
    }
  ]
}
```

Signature failures are rejected before anything is stored:

```
$ curl -X POST http://localhost:3987/webhooks/stripe \
    -H "content-type: application/json" -H "stripe-signature: t=1,v1=deadbeef" \
    -d '{"id":"evt_bad","type":"x"}'
{"error":"signature verification failed","reason":"timestamp outside tolerance"}
HTTP 401
```

Replaying a stored event via the CLI (works directly against the SQLite file, no running HTTP server required):

```
$ SQLITE_PATH=/tmp/gw-demo/gateway.db node dist/cli.js list
[1] stripe/evt_demo_001 type=payment_intent.succeeded status=completed attempts=0

$ SQLITE_PATH=/tmp/gw-demo/gateway.db node dist/cli.js replay 1
replayed event 1 (stripe/evt_demo_001) successfully
```

Or via HTTP: `POST /replay/:id`.

## Testing

```bash
npm test
```

Real output from this repository's test suite (SQLite-backed throughout — the same adapter the server uses by default):

```
 ✓ test/signature.test.ts (13 tests) 5ms
 ✓ test/dedup.test.ts (2 tests) 270ms
 ✓ test/server.test.ts (11 tests) 146ms
 ✓ test/worker-recovery.test.ts (3 tests) 539ms
   ✓ crash / restart recovery > SIGKILLing the gateway mid-handler loses the uncommitted effect, and restart reprocesses exactly once  532ms

 Test Files  4 passed (4)
      Tests  29 passed (29)
   Start at  21:58:48
   Duration  825ms (transform 153ms, setup 0ms, collect 424ms, tests 960ms, environment 0ms, prepare 204ms)
```

Full test names (`npx vitest run --reporter=verbose`):

```
 ✓ test/signature.test.ts > verifyStripeStyle > accepts a correctly signed, fresh payload
 ✓ test/signature.test.ts > verifyStripeStyle > rejects a missing header
 ✓ test/signature.test.ts > verifyStripeStyle > rejects when the secret does not match
 ✓ test/signature.test.ts > verifyStripeStyle > rejects a tampered body even with a validly-formed signature
 ✓ test/signature.test.ts > verifyStripeStyle > rejects a malformed header
 ✓ test/signature.test.ts > verifyStripeStyle > rejects a signature whose timestamp is outside the replay tolerance
 ✓ test/signature.test.ts > verifyStripeStyle > accepts a signature at the edge of the tolerance window
 ✓ test/signature.test.ts > verifyTelephonyStyle > accepts a correctly signed payload
 ✓ test/signature.test.ts > verifyTelephonyStyle > rejects an incorrect signature
 ✓ test/signature.test.ts > verifyTelephonyStyle > rejects a missing header
 ✓ test/signature.test.ts > verifyCrmStyle > accepts a correctly signed payload
 ✓ test/signature.test.ts > verifyCrmStyle > rejects a header missing the sha256= prefix
 ✓ test/signature.test.ts > verifyCrmStyle > rejects a tampered body
 ✓ test/dedup.test.ts > dedup via unique constraint — concurrent duplicate delivery > lets exactly one of N truly concurrent identical deliveries create the row
 ✓ test/dedup.test.ts > dedup via unique constraint — concurrent duplicate delivery > keeps independent delivery IDs fully independent
 ✓ test/server.test.ts > gateway HTTP surface > accepts a validly signed webhook fast, then processes it async
 ✓ test/server.test.ts > gateway HTTP surface > returns 202 once and 200 duplicate for every retry of the same delivery id, with only one effect
 ✓ test/server.test.ts > gateway HTTP surface > rejects an incorrectly signed webhook with 401 and never stores it
 ✓ test/server.test.ts > gateway HTTP surface > rejects a request with no signature header at all
 ✓ test/server.test.ts > gateway HTTP surface > accepts telephony and crm providers with their own distinct signature schemes
 ✓ test/server.test.ts > gateway HTTP surface > rejects an unknown provider slug
 ✓ test/server.test.ts > gateway HTTP surface > rejects a validly-signed payload missing a delivery id
 ✓ test/server.test.ts > gateway HTTP surface > supports replaying a completed event through /replay/:id
 ✓ test/server.test.ts > gateway HTTP surface > 404s replay for an event id that does not exist
 ✓ test/server.test.ts > gateway HTTP surface > lists recent events
 ✓ test/server.test.ts > gateway HTTP surface > reports health
 ✓ test/worker-recovery.test.ts > crash / restart recovery > SIGKILLing the gateway mid-handler loses the uncommitted effect, and restart reprocesses exactly once
 ✓ test/worker-recovery.test.ts > crash / restart recovery > rolls back a handler that throws, retries, and eventually succeeds exactly once
 ✓ test/worker-recovery.test.ts > crash / restart recovery > marks an event failed after exhausting max attempts, without ever recording a partial effect

 Test Files  4 passed (4)
      Tests  29 passed (29)
```

### What each test actually proves

- **`test/dedup.test.ts` — the concurrent-duplicate-delivery test.** Spawns 25 real `worker_threads`, each opening its **own independent SQLite connection** to the same database file, all racing to `INSERT` the identical `(provider, delivery_id)` pair at once. This is deliberately not "25 async calls in one JS callstack" — each thread is a genuinely separate OS-level connection, so the only thing that can prevent a double-insert is the database's own `UNIQUE` constraint, not any in-process lock or `Set`. The test asserts exactly 1 of the 25 threads gets `inserted: true` and the other 24 fail on the constraint specifically (not some other error), then drains the async worker and asserts the downstream `processed_effects` row count is exactly 1 — not 25, not 0. A late retry of the same delivery afterward still can't create a second row or a second effect.
- **`test/worker-recovery.test.ts` — the chaos test.** Spawns the gateway's claim/process logic in a genuinely separate OS process, waits until it has claimed an event, opened a transaction, and written the downstream effect row *inside that uncommitted transaction*, then sends real `SIGKILL` to the whole process group (killing `tsx`'s wrapper process alone isn't enough — its actual worker subprocess would survive as an orphan and finish anyway, which is exactly the kind of gap this test is designed to catch). It then reopens the database from a fresh connection — simulating a gateway restart — and confirms the pre-crash effect write is **not visible** (the transaction was never committed) while the claim itself (a separate, already-committed statement) survived. A recovery worker with a short stale-claim window then reclaims and reprocesses the orphaned row, and the test asserts the effect count ends at exactly 1: no data loss (it does get processed) and no duplicate processing (the lost pre-crash write doesn't count).
- **`test/signature.test.ts`** — negative tests for all three provider schemes: missing header, wrong secret, tampered body, malformed header, and a signature whose timestamp has aged out of the replay-tolerance window.

## Project layout

```
src/
  store/
    types.ts            EventStore interface — the contract both adapters implement
    sqlite-store.ts      real better-sqlite3 adapter, exercised by every test
    postgres-store.ts    real pg adapter (schema, UNIQUE/ON CONFLICT, FOR UPDATE SKIP LOCKED)
  signature.ts           per-provider HMAC verification
  providers.ts            provider registry (stripe / telephony / crm)
  processor.ts            the downstream "side effect" + effect-counting helper used by tests
  worker.ts                polling worker loop
  routes/webhook.ts        POST /webhooks/:provider
  routes/replay.ts          POST /replay/:id, GET /events
  server.ts                 Fastify app assembly + process entrypoint
  cli.ts                    webhook-replay CLI
test/
  signature.test.ts
  dedup.test.ts             the concurrent-duplicate-delivery test
  worker-recovery.test.ts   the chaos / crash-recovery tests
  server.test.ts            HTTP-level integration tests
  chaos-crash-worker.ts     child-process entrypoint used by the chaos test
  concurrent-insert-worker.mjs  worker_thread entrypoint used by the dedup test
```

## License

MIT — see [LICENSE](./LICENSE).
