import { describe, expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";

import * as HookInboxStore from "./HookInboxStore.ts";

const BASE_URL = "https://env.example.test/";

const hook = (id: string, overrides: Partial<HookInboxStore.HeldHook> = {}) =>
  Effect.map(Clock.currentTimeMillis, (now): HookInboxStore.HeldHook => ({
    id,
    receivedAt: DateTime.formatIso(DateTime.makeUnsafe(now)),
    method: "POST",
    rawHookId: "hook-1",
    hookKey: "hook-1",
    rawToken: "tok%2Fen",
    query: "a=1",
    headers: { "content-type": "application/json", "x-sig": "s" },
    body: new Uint8Array([0, 255, 10]),
    ...overrides,
  }));

/** Delivers with `outcome` per request and records what the environment was sent. */
const deliverer = (outcome: (hook: HookInboxStore.HeldHook) => HookInboxStore.DeliveryOutcome) => {
  const sent: Array<{ readonly baseUrl: string; readonly hook: HookInboxStore.HeldHook }> = [];
  const send = (baseUrl: string, held: HookInboxStore.HeldHook) =>
    Effect.sync(() => {
      sent.push({ baseUrl, hook: held });
      return outcome(held);
    });
  return { sent, send };
};

const withInbox = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  HookInboxStore.migrate.pipe(
    Effect.andThen(effect),
    Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })),
  );

describe("HookInboxStore", () => {
  it.effect("delivers held requests oldest first, byte for byte", () =>
    withInbox(
      Effect.gen(function* () {
        const first = yield* hook("first");
        expect(yield* HookInboxStore.hold(first, BASE_URL)).not.toBeNull();
        yield* HookInboxStore.hold(yield* hook("second"), BASE_URL);
        const { sent, send } = deliverer(() => "delivered");
        expect(yield* HookInboxStore.deliverDue(send)).toBeNull();
        expect(sent.map((entry) => entry.hook.id)).toEqual(["first", "second"]);
        expect(sent[0]).toEqual({ baseUrl: BASE_URL, hook: first });
        // Delivered requests are gone.
        expect(yield* HookInboxStore.deliverDue(send)).toBeNull();
        expect(sent).toHaveLength(2);
      }),
    ),
  );

  it.effect("keeps a request the environment did not take and backs off", () =>
    withInbox(
      Effect.gen(function* () {
        yield* HookInboxStore.hold(yield* hook("first"), BASE_URL);
        yield* HookInboxStore.hold(yield* hook("second"), BASE_URL);
        const offline = deliverer(() => "retry");
        const now = yield* Clock.currentTimeMillis;
        // Stops at the first failure, so order is kept.
        expect(yield* HookInboxStore.deliverDue(offline.send)).toBe(now + 30_000);
        expect(offline.sent.map((entry) => entry.hook.id)).toEqual(["first"]);
        expect(yield* HookInboxStore.deliverDue(offline.send)).toBe(now + 60_000);
        expect(yield* HookInboxStore.deliverDue(offline.send)).toBe(now + 120_000);

        const online = deliverer(() => "delivered");
        expect(yield* HookInboxStore.deliverDue(online.send)).toBeNull();
        expect(online.sent.map((entry) => entry.hook.id)).toEqual(["first", "second"]);
      }),
    ),
  );

  it("caps the wait between attempts at 10 minutes", () => {
    expect(HookInboxStore.retryDelayMs(1)).toBe(30_000);
    expect(HookInboxStore.retryDelayMs(20)).toBe(10 * 60_000);
  });

  it.effect("waking resets the backoff and reports whether anything waits", () =>
    withInbox(
      Effect.gen(function* () {
        expect(yield* HookInboxStore.wake(BASE_URL)).toBe(false);
        yield* HookInboxStore.hold(yield* hook("first"), "https://old.example.test/");
        const offline = deliverer(() => "retry");
        yield* HookInboxStore.deliverDue(offline.send);
        yield* HookInboxStore.deliverDue(offline.send);

        expect(yield* HookInboxStore.wake(BASE_URL)).toBe(true);
        const now = yield* Clock.currentTimeMillis;
        // Back to the first step, and sent to where the environment is now.
        expect(yield* HookInboxStore.deliverDue(offline.send)).toBe(now + 30_000);
        expect(offline.sent.at(-1)?.baseUrl).toBe(BASE_URL);
      }),
    ),
  );

  it.effect("drops requests older than 24 hours", () =>
    withInbox(
      Effect.gen(function* () {
        yield* HookInboxStore.hold(yield* hook("old"), BASE_URL);
        yield* TestClock.adjust(Duration.hours(1));
        yield* HookInboxStore.hold(yield* hook("new"), BASE_URL);
        yield* TestClock.adjust(Duration.minutes(23 * 60 + 1));
        const { sent, send } = deliverer(() => "delivered");
        yield* HookInboxStore.deliverDue(send);
        expect(sent.map((entry) => entry.hook.id)).toEqual(["new"]);
      }),
    ),
  );

  it.effect("refuses requests past the per-hook cap", () =>
    withInbox(
      Effect.gen(function* () {
        for (let index = 0; index < HookInboxStore.HOOK_INBOX_MAX_PER_HOOK; index++) {
          expect(yield* HookInboxStore.hold(yield* hook(`a-${index}`), BASE_URL)).not.toBeNull();
        }
        expect(yield* HookInboxStore.hold(yield* hook("one-too-many"), BASE_URL)).toBeNull();
        // Another hook still has room.
        const other = yield* hook("other", { rawHookId: "hook-2", hookKey: "hook-2" });
        expect(yield* HookInboxStore.hold(other, BASE_URL)).not.toBeNull();
      }),
    ),
  );

  it.effect("refuses a request that would pass the byte cap", () =>
    withInbox(
      Effect.gen(function* () {
        const big = new Uint8Array(HookInboxStore.HOOK_INBOX_MAX_BYTES - 10);
        expect(
          yield* HookInboxStore.hold(yield* hook("big", { body: big }), BASE_URL),
        ).not.toBeNull();
        const small = new Uint8Array(11);
        expect(
          yield* HookInboxStore.hold(
            yield* hook("small", { rawHookId: "x", hookKey: "x", body: small }),
            BASE_URL,
          ),
        ).toBeNull();
      }),
    ),
  );

  it.effect("stores a request id once", () =>
    withInbox(
      Effect.gen(function* () {
        const first = yield* hook("same");
        expect(yield* HookInboxStore.hold(first, BASE_URL)).not.toBeNull();
        expect(yield* HookInboxStore.hold(first, BASE_URL)).toBeNull();
      }),
    ),
  );

  it.effect("clear drops everything held", () =>
    withInbox(
      Effect.gen(function* () {
        yield* HookInboxStore.hold(yield* hook("first"), BASE_URL);
        yield* HookInboxStore.clear;
        const { sent, send } = deliverer(() => "delivered");
        expect(yield* HookInboxStore.deliverDue(send)).toBeNull();
        expect(sent).toHaveLength(0);
      }),
    ),
  );
});
