import { assert, it } from "@effect/vitest";
import type { RelayPendingHook } from "@t3tools/contracts/relay";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import {
  ScheduledTaskService,
  type WebhookTriggerRequest,
  type WebhookTriggerResult,
} from "../scheduledTasks/ScheduledTaskService.ts";
import { drainOnce, HookMailboxRelay, toTriggerRequest } from "./HookMailboxDrain.ts";

const held = (id: string, overrides: Partial<RelayPendingHook> = {}): RelayPendingHook => ({
  id,
  receivedAt: "2026-10-04T10:00:00.000Z",
  method: "POST",
  rawHookId: "scheduled-task%3Ahook",
  rawToken: "tok%2Fen",
  query: "a=1",
  headers: { "Content-Type": "application/json" },
  bodyBase64: Buffer.from('{"x":1}').toString("base64"),
  ...overrides,
});

/** Runs one drain pass against a relay holding `pages` and a service answering `outcome`. */
const drain = (
  pages: ReadonlyArray<ReadonlyArray<RelayPendingHook>> | null,
  outcome: (request: WebhookTriggerRequest) => WebhookTriggerResult["_tag"],
) =>
  Effect.gen(function* () {
    const acked: Array<string> = [];
    const triggered: Array<WebhookTriggerRequest> = [];
    let page = 0;
    const relay = Layer.succeed(HookMailboxRelay, {
      pending: Effect.sync(() => (pages === null ? null : (pages[page++] ?? []))),
      ack: (ids) => Effect.sync(() => void acked.push(...ids)),
    });
    const service = Layer.mock(ScheduledTaskService)({
      triggerWebhook: (request) =>
        Effect.sync(() => {
          triggered.push(request);
          return { _tag: outcome(request) } as WebhookTriggerResult;
        }),
    });
    const delivered = yield* drainOnce().pipe(Effect.provide(Layer.merge(relay, service)));
    return { acked, triggered, delivered };
  });

it("rebuilds the request the webhook route would have built", () => {
  const request = toTriggerRequest(held("d1"));
  assert.equal(request?.hookId, "scheduled-task:hook");
  assert.equal(request?.token, "tok/en");
  assert.equal(request?.headers["content-type"], "application/json");
  assert.equal(request?.bodyText, '{"x":1}');
  assert.equal(request?.relayDeliveryId, "d1");
  assert.equal(request?.receivedAt, "2026-10-04T10:00:00.000Z");
});

it.effect("delivers held requests and acks every final outcome", () =>
  Effect.gen(function* () {
    const result = yield* drain([[held("ok"), held("gone"), held("bad-sig")]], (request) =>
      request.relayDeliveryId === "ok"
        ? "accepted"
        : request.relayDeliveryId === "gone"
          ? "not_found"
          : "rejected_signature",
    );
    assert.deepEqual(result.acked, ["ok", "gone", "bad-sig"]);
  }),
);

it.effect("leaves rate-limited requests held for the next pass", () =>
  Effect.gen(function* () {
    const result = yield* drain(
      [[held("first"), held("limited")], [held("never-pulled")]],
      (request) => (request.relayDeliveryId === "limited" ? "rate_limited" : "accepted"),
    );
    assert.deepEqual(result.acked, ["first"]);
    // The pass stops instead of pulling the next page.
    assert.isFalse(result.triggered.some((request) => request.relayDeliveryId === "never-pulled"));
  }),
);

it.effect("acks and drops a request whose path cannot be decoded", () =>
  Effect.gen(function* () {
    const result = yield* drain([[held("broken", { rawToken: "%E0" })]], () => "accepted");
    assert.deepEqual(result.acked, ["broken"]);
    assert.equal(result.triggered.length, 0);
  }),
);

it.effect("does nothing when the environment is not linked", () =>
  Effect.gen(function* () {
    const result = yield* drain(null, () => "accepted");
    assert.equal(result.delivered, 0);
    assert.equal(result.triggered.length, 0);
  }),
);
