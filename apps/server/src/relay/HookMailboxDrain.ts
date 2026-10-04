import type { RelayPendingHook } from "@t3tools/contracts/relay";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { readHoldWebhooksWhileOffline, readRelayConnection } from "../cloud/config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import { WEBHOOK_MAX_BODY_BYTES } from "../scheduledTasks/webhookRoute.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { makeRelayEnvironmentClient } from "./relayEnvironmentClient.ts";

const DRAIN_INTERVAL_MS = 30_000;
const MAX_BACKOFF_MS = 5 * 60_000;
const PULL_LIMIT = 10;

export class HookMailboxRelayError extends Schema.TaggedError<HookMailboxRelayError>()(
  "HookMailboxRelayError",
  { operation: Schema.Literals(["pending", "ack"]), cause: Schema.Defect() },
) {}

/**
 * Talks to the relay's held-webhook endpoints. A service so tests can stand in
 * for the relay without a network.
 */
export class HookMailboxRelay extends Context.Service<
  HookMailboxRelay,
  {
    /** Null when this environment is not linked to T3 Connect. */
    readonly pending: Effect.Effect<ReadonlyArray<RelayPendingHook> | null, HookMailboxRelayError>;
    readonly ack: (ids: ReadonlyArray<string>) => Effect.Effect<void, HookMailboxRelayError>;
  }
>()("t3/relay/HookMailboxDrain/HookMailboxRelay") {}

export const relayLayer = Layer.effect(
  HookMailboxRelay,
  Effect.gen(function* () {
    const secrets = yield* ServerSecretStore.ServerSecretStore;
    const environment = yield* ServerEnvironment.ServerEnvironment;
    const client = Effect.gen(function* () {
      const connection = yield* readRelayConnection(secrets);
      if (connection === null) return null;
      return {
        api: yield* makeRelayEnvironmentClient(connection),
        environmentId: yield* environment.getEnvironmentId,
      };
    });
    return HookMailboxRelay.of({
      pending: Effect.gen(function* () {
        const relay = yield* client;
        if (relay === null) return null;
        const { deliveries } = yield* relay.api.server.listPendingHooks({
          params: { environmentId: relay.environmentId },
          query: { limit: PULL_LIMIT },
        });
        return deliveries;
      }).pipe(
        Effect.mapError((cause) => new HookMailboxRelayError({ operation: "pending", cause })),
      ),
      ack: (ids) =>
        Effect.gen(function* () {
          const relay = yield* client;
          if (relay === null || ids.length === 0) return;
          yield* relay.api.server.ackPendingHooks({
            params: { environmentId: relay.environmentId },
            payload: { ids: [...ids] },
          });
        }).pipe(Effect.mapError((cause) => new HookMailboxRelayError({ operation: "ack", cause }))),
    });
  }),
);

/** Turns a held request back into what the webhook route would have built. */
export function toTriggerRequest(
  hook: RelayPendingHook,
): ScheduledTaskService.WebhookTriggerRequest | null {
  let hookId: string;
  let token: string;
  try {
    hookId = decodeURIComponent(hook.rawHookId);
    token = decodeURIComponent(hook.rawToken);
  } catch {
    return null;
  }
  const body = new Uint8Array(Buffer.from(hook.bodyBase64, "base64"));
  if (body.byteLength > WEBHOOK_MAX_BODY_BYTES) return null;
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(hook.headers)) headers[name.toLowerCase()] = value;
  return {
    hookId,
    token,
    method: hook.method,
    path: `${ScheduledTaskService.WEBHOOK_ROUTE_PREFIX}/${encodeURIComponent(hookId)}`,
    query: hook.query,
    headers,
    body,
    bodyText: new TextDecoder().decode(body),
    relayDeliveryId: hook.id,
    receivedAt: hook.receivedAt,
  };
}

/**
 * Delivers webhook requests T3 Connect held while this environment was
 * offline. Runs once at startup and then every 30 seconds while the opt-in is
 * on. Rate-limited requests stay held for the next pass; every other outcome,
 * including a request that cannot be decoded, is acked.
 */
export const drainOnce = Effect.fn("HookMailboxDrain.drainOnce")(function* () {
  const relay = yield* HookMailboxRelay;
  const scheduledTasks = yield* ScheduledTaskService.ScheduledTaskService;
  let delivered = 0;
  // Bounded so one pass cannot run forever against a relay that keeps returning work.
  for (let page = 0; page < 50; page++) {
    const pending = yield* relay.pending;
    if (pending === null || pending.length === 0) return delivered;
    const ack: Array<string> = [];
    let rateLimited = false;
    for (const hook of pending) {
      const request = toTriggerRequest(hook);
      if (request === null) {
        ack.push(hook.id);
        continue;
      }
      const result = yield* scheduledTasks.triggerWebhook(request);
      if (result._tag === "rate_limited") {
        rateLimited = true;
        continue;
      }
      ack.push(hook.id);
      delivered++;
    }
    yield* relay.ack(ack);
    // Rate-limited requests are still held; stop until the next pass.
    if (rateLimited || ack.length === 0) return delivered;
  }
  return delivered;
});

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const scheduler = yield* Scheduler.Scheduler;
    const secrets = yield* ServerSecretStore.ServerSecretStore;
    const relay = yield* HookMailboxRelay;
    const scheduledTasks = yield* ScheduledTaskService.ScheduledTaskService;
    const state = yield* Ref.make({ nextAt: 0, failures: 0, wasEnabled: false });

    const tick = Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const current = yield* Ref.get(state);
      if (now < current.nextAt) return;
      const enabled = yield* readHoldWebhooksWhileOffline(secrets);
      // One last pass after the opt-in is turned off, so nothing already held
      // is stranded; after that the loop stays idle.
      if (!enabled && !current.wasEnabled) return;
      const outcome = yield* drainOnce().pipe(
        Effect.provideService(HookMailboxRelay, relay),
        Effect.provideService(ScheduledTaskService.ScheduledTaskService, scheduledTasks),
        Effect.result,
      );
      const failures = outcome._tag === "Failure" ? current.failures + 1 : 0;
      if (outcome._tag === "Failure") {
        yield* Effect.logWarning("Could not deliver held webhook requests", {
          cause: outcome.failure,
        });
      }
      const delay =
        failures === 0
          ? DRAIN_INTERVAL_MS
          : Math.min(DRAIN_INTERVAL_MS * 2 ** failures, MAX_BACKOFF_MS);
      yield* Ref.set(state, { nextAt: now + delay, failures, wasEnabled: enabled });
    });

    yield* scheduler.register("relay-hook-mailbox", tick);
  }),
);
