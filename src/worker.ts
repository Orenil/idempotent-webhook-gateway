import type { EventHandler, EventStore } from "./store/types.js";

export interface WorkerOptions {
  workerId: string;
  pollIntervalMs: number;
  staleClaimMs: number;
  maxAttempts: number;
}

/**
 * Polling worker loop. This is the "LISTEN/NOTIFY-emulated" async processor
 * called out in the spec: rather than reacting to a Postgres NOTIFY, it
 * repeatedly asks the store "is there anything claimable?" and backs off to
 * `pollIntervalMs` between checks when idle, but loops immediately (no
 * delay) after a successful claim so a burst of inbound webhooks drains
 * without waiting out the poll interval between each one.
 *
 * Production swap: point this at Postgres and either keep polling (fine up
 * to moderate throughput) or add a `LISTEN webhook_events_channel` listener
 * that calls `drain()` on notification instead of waiting for the next
 * timer tick — see the trigger defined in postgres-store.ts. At higher
 * throughput, replace this whole class with a BullMQ worker fed by the same
 * outbox insert.
 */
export class Worker<TxCtx> {
  private timer: NodeJS.Timeout | null = null;
  private stopped = true;
  private ticking = false;

  constructor(
    private readonly store: EventStore<TxCtx>,
    private readonly handler: EventHandler<TxCtx>,
    private readonly options: WorkerOptions,
  ) {}

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.scheduleNext(0);
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }

  /**
   * Claims and processes every currently-claimable event, one at a time,
   * until the store reports nothing left. Used directly by the poll loop,
   * and by tests that want deterministic processing without waiting on
   * timers.
   */
  async drain(): Promise<number> {
    let processed = 0;
    for (;;) {
      const event = await this.store.claimNext(this.options.workerId, this.options.staleClaimMs);
      if (!event) break;
      await this.store.processClaimed(event, this.handler, this.options.maxAttempts);
      processed++;
    }
    return processed;
  }

  private scheduleNext(delayMs: number): void {
    if (this.stopped) return;
    this.timer = setTimeout(() => void this.tick(), delayMs);
    if (typeof this.timer.unref === "function") this.timer.unref();
  }

  private async tick(): Promise<void> {
    if (this.ticking) return;
    this.ticking = true;
    try {
      const processed = await this.drain();
      this.scheduleNext(processed > 0 ? 0 : this.options.pollIntervalMs);
    } catch (err) {
      console.error("worker tick failed:", err);
      this.scheduleNext(this.options.pollIntervalMs);
    } finally {
      this.ticking = false;
    }
  }
}
