import * as SqliteClient from "@effect/sql-sqlite-do/SqliteClient";
import type * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";

import * as HookInboxStore from "./HookInboxStore.ts";
import { sendUpstream, TUNNEL_OFFLINE_STATUS } from "./upstream.ts";

/** Statuses that mean the environment is not there; the whole inbox waits and backs off. */
const UNREACHABLE_STATUSES = new Set([502, 503, 504, TUNNEL_OFFLINE_STATUS]);
/**
 * Statuses that mean the environment is there but did not take this request
 * yet: its task's queue is full (429) or it failed while handling it (500).
 * The environment drops a delivery id it has already run, so retrying is safe.
 */
const BUSY_STATUSES = new Set([429, 500]);
/** When a run itself fails, the next one is tried after this long. */
const RUN_FAILURE_RETRY_MS = 60_000;

type Call<A> = Effect.Effect<A, never, Alchemy.RuntimeContext>;

export interface HookInboxObjectShape {
  /** Holds a request for `baseUrl`; false when the inbox is full and nothing was stored. */
  readonly hold: (hook: HookInboxStore.HeldHook, baseUrl: string) => Call<boolean>;
  /** The environment is back at `baseUrl`: deliver what is waiting now. */
  readonly wake: (baseUrl: string) => Call<boolean>;
  readonly clear: () => Call<void>;
}

/**
 * One per managed endpoint, addressed by endpoint key. Holds webhook requests
 * the environment could not take, in SQLite, and pushes them back through
 * its tunnel from the alarm, oldest first, backing off while it stays away.
 */
export class HookInboxObject extends Cloudflare.DurableObject<
  HookInboxObject,
  HookInboxObjectShape
>()("HookInboxObject") {}

const deliver = (baseUrl: string, hook: HookInboxStore.HeldHook) =>
  sendUpstream(baseUrl, hook).pipe(
    Effect.result,
    Effect.map((result) => {
      // Unreachable or timed out: a timeout may still have run it, and the
      // environment drops a delivery id it has already seen.
      if (Result.isFailure(result)) {
        return { outcome: "unreachable" as const, reason: result.failure._tag };
      }
      if (Option.isNone(result.success))
        return { outcome: "unreachable" as const, reason: "timeout" };
      const status = result.success.value.status;
      const outcome: HookInboxStore.DeliveryOutcome = UNREACHABLE_STATUSES.has(status)
        ? "unreachable"
        : BUSY_STATUSES.has(status)
          ? "busy"
          : "delivered";
      return { outcome, reason: `status ${status}` };
    }),
    Effect.tap(({ outcome, reason }) =>
      outcome === "delivered"
        ? Effect.void
        : Effect.logInfo("Held webhook not delivered yet", {
            outcome,
            reason,
            deliveryId: hook.id,
          }),
    ),
    Effect.map(({ outcome }) => outcome),
    Effect.provide(FetchHttpClient.layer),
  );

export const HookInboxObjectLive = HookInboxObject.make(
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    // The init phase returns the per-instance Effect, which alchemy runs once
    // per object; only that inner Effect may touch storage.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const sql = SqliteClient.layer({ storage: state.raw.storage });
      const run = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(Effect.provide(sql), Effect.orDie);
      yield* run(HookInboxStore.migrate);

      /** Moves the alarm to `at`, unless one is already due sooner. */
      const scheduleBy = (at: number) =>
        Effect.gen(function* () {
          const current = yield* state.storage.getAlarm();
          if (current === null || current > at) yield* state.storage.setAlarm(at);
        });

      return {
        hold: (hook: HookInboxStore.HeldHook, baseUrl: string) =>
          Effect.gen(function* () {
            const dueAt = yield* run(HookInboxStore.hold(hook, baseUrl));
            if (dueAt === null) return false;
            yield* scheduleBy(dueAt);
            return true;
          }),
        wake: (baseUrl: string) =>
          Effect.gen(function* () {
            const pending = yield* run(HookInboxStore.wake(baseUrl));
            if (pending) yield* state.storage.setAlarm(yield* Clock.currentTimeMillis);
            return pending;
          }),
        clear: () =>
          Effect.gen(function* () {
            yield* run(HookInboxStore.clear);
            yield* state.storage.deleteAlarm();
          }),
        alarm: () =>
          run(HookInboxStore.deliverDue(deliver)).pipe(
            Effect.flatMap((nextAt) =>
              nextAt === null ? Effect.void : state.storage.setAlarm(nextAt),
            ),
            Effect.catchCause((cause) =>
              Effect.logWarning("Held webhook delivery run failed", { cause }).pipe(
                Effect.andThen(Clock.currentTimeMillis),
                Effect.flatMap((now) => state.storage.setAlarm(now + RUN_FAILURE_RETRY_MS)),
              ),
            ),
          ),
      };
    });
  }),
);
