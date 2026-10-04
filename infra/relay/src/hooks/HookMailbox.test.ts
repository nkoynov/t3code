import { describe, expect, it } from "@effect/vitest";
import { PgDialect } from "drizzle-orm/pg-core";
import type { SQL } from "drizzle-orm";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as RelayDb from "../db.ts";
import { relayHookMailbox } from "../persistence/schema.ts";
import * as HookMailbox from "./HookMailbox.ts";

const dialect = new PgDialect();
const render = (condition: unknown) => dialect.sqlToQuery(condition as SQL);

const layerWithDb = (db: RelayDb.RelayDb["Service"]) =>
  HookMailbox.layer.pipe(Layer.provide(Layer.succeed(RelayDb.RelayDb, db)));

const hook: HookMailbox.HeldHook = {
  id: "delivery-1",
  environmentId: "environment-1",
  receivedAt: "2026-10-04T10:00:00.000Z",
  method: "POST",
  rawHookId: "task",
  rawToken: "token",
  query: "",
  headers: { "content-type": "application/json" },
  body: new Uint8Array([1, 2, 3]),
};

describe("HookMailbox", () => {
  it.effect("stores a request with a 24 hour expiry while there is room", () => {
    const inserted: Array<Record<string, unknown>> = [];
    const db = {
      select: () => ({
        from: () => ({ where: () => Effect.succeed([{ value: 3 }]) }),
      }),
      insert: (table: unknown) => {
        expect(table).toBe(relayHookMailbox);
        return {
          values: (values: Record<string, unknown>) => {
            inserted.push(values);
            return { onConflictDoNothing: () => Effect.void };
          },
        };
      },
    } as unknown as RelayDb.RelayDb["Service"];

    return Effect.gen(function* () {
      const mailbox = yield* HookMailbox.HookMailbox;
      expect(yield* mailbox.enqueue(hook)).toBe(true);
      expect(inserted[0]?.expiresAt).toBe("2026-10-05T10:00:00.000Z");
      expect([...(inserted[0]?.body as Uint8Array)]).toEqual([1, 2, 3]);
    }).pipe(Effect.provide(layerWithDb(db)));
  });

  it.effect("refuses a request once the environment's mailbox is full", () => {
    let insertedAny = false;
    const db = {
      select: () => ({
        from: () => ({
          where: () => Effect.succeed([{ value: HookMailbox.HOOK_MAILBOX_MAX_PER_ENVIRONMENT }]),
        }),
      }),
      insert: () => {
        insertedAny = true;
        return { values: () => ({ onConflictDoNothing: () => Effect.void }) };
      },
    } as unknown as RelayDb.RelayDb["Service"];

    return Effect.gen(function* () {
      const mailbox = yield* HookMailbox.HookMailbox;
      expect(yield* mailbox.enqueue(hook)).toBe(false);
      expect(insertedAny).toBe(false);
    }).pipe(Effect.provide(layerWithDb(db)));
  });

  it.effect("lists and acks only the calling environment's requests", () => {
    const conditions: Array<unknown> = [];
    let limit = 0;
    const db = {
      select: () => ({
        from: () => ({
          where: (condition: unknown) => {
            conditions.push(condition);
            return {
              orderBy: () => ({
                limit: (value: number) => {
                  limit = value;
                  return Effect.succeed([]);
                },
              }),
            };
          },
        }),
      }),
      delete: () => ({
        where: (condition: unknown) => {
          conditions.push(condition);
          return { returning: () => Effect.succeed([{ id: "delivery-1" }]) };
        },
      }),
    } as unknown as RelayDb.RelayDb["Service"];

    return Effect.gen(function* () {
      const mailbox = yield* HookMailbox.HookMailbox;
      yield* mailbox.listPending({ environmentId: "environment-1", limit: 1_000 });
      expect(limit).toBe(HookMailbox.HOOK_MAILBOX_MAX_PULL);
      expect(yield* mailbox.ack({ environmentId: "environment-1", ids: ["delivery-1"] })).toBe(1);
      for (const condition of conditions) {
        const query = render(condition);
        expect(query.sql).toContain('"relay_hook_mailbox"."environment_id" = $1');
        expect(query.params[0]).toBe("environment-1");
      }
    }).pipe(Effect.provide(layerWithDb(db)));
  });

  it.effect("acks nothing for an empty id list", () => {
    const db = {
      delete: () => {
        throw new Error("no delete expected");
      },
    } as unknown as RelayDb.RelayDb["Service"];

    return Effect.gen(function* () {
      const mailbox = yield* HookMailbox.HookMailbox;
      expect(yield* mailbox.ack({ environmentId: "environment-1", ids: [] })).toBe(0);
    }).pipe(Effect.provide(layerWithDb(db)));
  });
});
