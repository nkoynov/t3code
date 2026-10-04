import { and, asc, count, eq, inArray, lt } from "drizzle-orm";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as RelayDb from "../db.ts";
import { relayHookMailbox } from "../persistence/schema.ts";

/** How long a held request waits for its environment. */
export const HOOK_MAILBOX_TTL_MS = 24 * 60 * 60 * 1000;
/** Requests one environment may have waiting at once. */
export const HOOK_MAILBOX_MAX_PER_ENVIRONMENT = 500;
/** Requests returned per pull; bodies are up to 1 MiB each. */
export const HOOK_MAILBOX_MAX_PULL = 10;

export class HookMailboxPersistenceError extends Schema.TaggedError<HookMailboxPersistenceError>()(
  "HookMailboxPersistenceError",
  {
    operation: Schema.Literals(["enqueue", "list", "ack", "prune", "clear"]),
    environmentId: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Hook mailbox '${this.operation}' failed${this.environmentId ? ` for environment '${this.environmentId}'` : ""}`;
  }
}

export interface HeldHook {
  readonly id: string;
  readonly environmentId: string;
  readonly receivedAt: string;
  readonly method: string;
  readonly rawHookId: string;
  readonly rawToken: string;
  readonly query: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: Uint8Array;
}

export class HookMailbox extends Context.Service<
  HookMailbox,
  {
    /** Stores a request; returns false when the environment's mailbox is full. */
    readonly enqueue: (hook: HeldHook) => Effect.Effect<boolean, HookMailboxPersistenceError>;
    /** Oldest first, never another environment's requests. */
    readonly listPending: (input: {
      readonly environmentId: string;
      readonly limit: number;
    }) => Effect.Effect<ReadonlyArray<HeldHook>, HookMailboxPersistenceError>;
    readonly ack: (input: {
      readonly environmentId: string;
      readonly ids: ReadonlyArray<string>;
    }) => Effect.Effect<number, HookMailboxPersistenceError>;
    readonly pruneExpired: (input: {
      readonly now: string;
    }) => Effect.Effect<void, HookMailboxPersistenceError>;
    /** Deletes everything held for an environment, used when it is unlinked. */
    readonly clearEnvironment: (input: {
      readonly environmentId: string;
    }) => Effect.Effect<void, HookMailboxPersistenceError>;
  }
>()("t3code-relay/hooks/HookMailbox") {}

export const make = Effect.gen(function* () {
  const db = yield* RelayDb.RelayDb;
  const fail =
    (operation: HookMailboxPersistenceError["operation"], environmentId?: string) =>
    (cause: unknown) =>
      new HookMailboxPersistenceError({
        operation,
        ...(environmentId === undefined ? {} : { environmentId }),
        cause,
      });

  return HookMailbox.of({
    enqueue: Effect.fn("relay.hook_mailbox.enqueue")(function* (hook) {
      yield* Effect.annotateCurrentSpan({ "relay.environment_id": hook.environmentId });
      const [held] = yield* db
        .select({ value: count() })
        .from(relayHookMailbox)
        .where(eq(relayHookMailbox.environmentId, hook.environmentId))
        .pipe(Effect.mapError(fail("enqueue", hook.environmentId)));
      if ((held?.value ?? 0) >= HOOK_MAILBOX_MAX_PER_ENVIRONMENT) return false;
      const expiresAt = DateTime.formatIso(
        DateTime.add(DateTime.makeUnsafe(hook.receivedAt), { milliseconds: HOOK_MAILBOX_TTL_MS }),
      );
      yield* db
        .insert(relayHookMailbox)
        .values({
          id: hook.id,
          environmentId: hook.environmentId,
          receivedAt: hook.receivedAt,
          expiresAt,
          method: hook.method,
          rawHookId: hook.rawHookId,
          rawToken: hook.rawToken,
          query: hook.query,
          headers: { ...hook.headers },
          body: Buffer.from(hook.body),
        })
        .onConflictDoNothing()
        .pipe(Effect.mapError(fail("enqueue", hook.environmentId)));
      return true;
    }),

    listPending: Effect.fn("relay.hook_mailbox.list_pending")(function* (input) {
      yield* Effect.annotateCurrentSpan({ "relay.environment_id": input.environmentId });
      const rows = yield* db
        .select()
        .from(relayHookMailbox)
        .where(eq(relayHookMailbox.environmentId, input.environmentId))
        .orderBy(asc(relayHookMailbox.receivedAt), asc(relayHookMailbox.id))
        .limit(Math.max(1, Math.min(input.limit, HOOK_MAILBOX_MAX_PULL)))
        .pipe(Effect.mapError(fail("list", input.environmentId)));
      return rows.map((row) => ({
        id: row.id,
        environmentId: row.environmentId,
        receivedAt: row.receivedAt,
        method: row.method,
        rawHookId: row.rawHookId,
        rawToken: row.rawToken,
        query: row.query,
        headers: row.headers,
        body: new Uint8Array(row.body),
      }));
    }),

    ack: Effect.fn("relay.hook_mailbox.ack")(function* (input) {
      yield* Effect.annotateCurrentSpan({ "relay.environment_id": input.environmentId });
      if (input.ids.length === 0) return 0;
      const deleted = yield* db
        .delete(relayHookMailbox)
        .where(
          and(
            eq(relayHookMailbox.environmentId, input.environmentId),
            inArray(relayHookMailbox.id, [...input.ids]),
          ),
        )
        .returning({ id: relayHookMailbox.id })
        .pipe(Effect.mapError(fail("ack", input.environmentId)));
      return deleted.length;
    }),

    pruneExpired: Effect.fn("relay.hook_mailbox.prune_expired")(function* (input) {
      yield* db
        .delete(relayHookMailbox)
        .where(lt(relayHookMailbox.expiresAt, input.now))
        .pipe(Effect.mapError(fail("prune")));
    }),

    clearEnvironment: Effect.fn("relay.hook_mailbox.clear_environment")(function* (input) {
      yield* db
        .delete(relayHookMailbox)
        .where(eq(relayHookMailbox.environmentId, input.environmentId))
        .pipe(Effect.mapError(fail("clear", input.environmentId)));
    }),
  });
});

export const layer = Layer.effect(HookMailbox, make);
