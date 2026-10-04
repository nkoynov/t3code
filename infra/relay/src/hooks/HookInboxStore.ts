import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * The storage and delivery rules of one environment's held webhook requests.
 * Runs inside the environment's `HookInboxObject` Durable Object against its SQLite
 * storage; written against `SqlClient` so tests can run it on any SQLite.
 */

/** How long a held request waits for its environment. */
const HOOK_INBOX_TTL_MS = 24 * 60 * 60 * 1000;
/** Requests one environment may have waiting at once. */
const HOOK_INBOX_MAX_REQUESTS = 1_000;
/** Body bytes one environment may have waiting at once. */
export const HOOK_INBOX_MAX_BYTES = 50 * 1_048_576;
/**
 * Requests one hook may have waiting at once. The relay cannot check tokens,
 * so this keeps junk sent to one hook id from crowding out the others.
 */
export const HOOK_INBOX_MAX_PER_HOOK = 100;
/** Requests pushed per alarm run; the next run starts right away while more wait. */
const DELIVERIES_PER_RUN = 20;
const FIRST_RETRY_MS = 30_000;
const MAX_RETRY_MS = 10 * 60_000;

export interface HeldHook {
  readonly id: string;
  readonly receivedAt: string;
  readonly method: string;
  /** Path segments exactly as the sender sent them; the environment decodes them. */
  readonly rawHookId: string;
  readonly rawToken: string;
  /** Without the leading `?`. */
  readonly query: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: Uint8Array;
}

/** `retry` keeps the request and backs off; `delivered` deletes it. */
export type DeliveryOutcome = "delivered" | "retry";

/** 30 s, 1 min, 2 min, ... up to 10 min between attempts while the environment stays away. */
export const retryDelayMs = (failures: number) =>
  Math.min(FIRST_RETRY_MS * 2 ** Math.max(0, failures - 1), MAX_RETRY_MS);

const HeadersJson = Schema.fromJsonString(Schema.Record(Schema.String, Schema.String));
const encodeHeaders = Schema.encodeSync(HeadersJson);
const decodeHeaders = Schema.decodeUnknownSync(HeadersJson);

interface HeldHookRow {
  readonly id: string;
  readonly received_at: string;
  readonly method: string;
  readonly raw_hook_id: string;
  readonly raw_token: string;
  readonly query: string;
  readonly headers: string;
  readonly body: Uint8Array;
}

interface TargetRow {
  readonly base_url: string;
  readonly failures: number;
}

const iso = (epochMillis: number) => DateTime.formatIso(DateTime.makeUnsafe(epochMillis));

export const migrate = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS held_hooks (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      received_at TEXT NOT NULL,
      method TEXT NOT NULL,
      raw_hook_id TEXT NOT NULL,
      raw_token TEXT NOT NULL,
      query TEXT NOT NULL,
      headers TEXT NOT NULL,
      body BLOB NOT NULL
    )
  `;
  yield* sql`CREATE INDEX IF NOT EXISTS held_hooks_hook ON held_hooks (raw_hook_id)`;
  // Where to push, and how many attempts in a row have failed. One row.
  yield* sql`
    CREATE TABLE IF NOT EXISTS held_hooks_target (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      base_url TEXT NOT NULL,
      failures INTEGER NOT NULL DEFAULT 0
    )
  `;
});

const setTarget = (baseUrl: string, options: { readonly resetFailures: boolean }) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* options.resetFailures
      ? sql`
          INSERT INTO held_hooks_target (id, base_url) VALUES (1, ${baseUrl})
          ON CONFLICT (id) DO UPDATE SET base_url = excluded.base_url, failures = 0
        `
      : sql`
          INSERT INTO held_hooks_target (id, base_url) VALUES (1, ${baseUrl})
          ON CONFLICT (id) DO UPDATE SET base_url = excluded.base_url
        `;
  });

const readTarget = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<TargetRow>`SELECT base_url, failures FROM held_hooks_target WHERE id = 1`;
  return rows[0] ?? null;
});

const setFailures = (failures: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE held_hooks_target SET failures = ${failures} WHERE id = 1`;
  });

const hasPending = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly id: string }>`SELECT id FROM held_hooks LIMIT 1`;
  return rows.length > 0;
});

/**
 * Stores a request for later delivery to `baseUrl`. Returns the time the
 * first attempt is due, or null when the inbox is full and nothing was stored.
 */
export const hold = Effect.fn("HookInboxStore.hold")(function* (hook: HeldHook, baseUrl: string) {
  const sql = yield* SqlClient.SqlClient;
  // One statement, so the caps hold however many requests arrive at once.
  const inserted = yield* sql<{ readonly id: string }>`
    INSERT INTO held_hooks (id, received_at, method, raw_hook_id, raw_token, query, headers, body)
    SELECT ${hook.id}, ${hook.receivedAt}, ${hook.method}, ${hook.rawHookId}, ${hook.rawToken},
      ${hook.query}, ${encodeHeaders(hook.headers)}, ${hook.body}
    WHERE (SELECT count(*) FROM held_hooks) < ${HOOK_INBOX_MAX_REQUESTS}
      AND (SELECT count(*) FROM held_hooks WHERE raw_hook_id = ${hook.rawHookId})
        < ${HOOK_INBOX_MAX_PER_HOOK}
      AND (SELECT coalesce(sum(length(body)), 0) FROM held_hooks) + ${hook.body.byteLength}
        <= ${HOOK_INBOX_MAX_BYTES}
    ON CONFLICT (id) DO NOTHING
    RETURNING id
  `;
  if (inserted.length === 0) return null;
  yield* setTarget(baseUrl, { resetFailures: false });
  const target = yield* readTarget;
  return (yield* Clock.currentTimeMillis) + retryDelayMs(Math.max(1, target?.failures ?? 0));
});

/**
 * The environment is reachable again at `baseUrl`. Returns whether anything
 * is waiting, so the caller can deliver right away.
 */
export const wake = Effect.fn("HookInboxStore.wake")(function* (baseUrl: string) {
  if (!(yield* hasPending)) return false;
  yield* setTarget(baseUrl, { resetFailures: true });
  return true;
});

export const clear = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`DELETE FROM held_hooks`;
  yield* sql`DELETE FROM held_hooks_target`;
});

/**
 * Pushes the oldest held requests to the environment, one at a time, and
 * stops at the first one that has to be retried. Drops requests older than
 * the TTL. Returns when the next run is due, or null when nothing is left.
 */
export const deliverDue = Effect.fn("HookInboxStore.deliverDue")(function* <R>(
  send: (baseUrl: string, hook: HeldHook) => Effect.Effect<DeliveryOutcome, never, R>,
) {
  const sql = yield* SqlClient.SqlClient;
  const startedAt = yield* Clock.currentTimeMillis;
  yield* sql`DELETE FROM held_hooks WHERE received_at < ${iso(startedAt - HOOK_INBOX_TTL_MS)}`;
  const target = yield* readTarget;
  const batch = yield* sql<HeldHookRow>`
    SELECT id, received_at, method, raw_hook_id, raw_token, query, headers, body
    FROM held_hooks ORDER BY seq LIMIT ${DELIVERIES_PER_RUN}
  `;
  if (batch.length === 0 || target === null) {
    if (target === null) yield* sql`DELETE FROM held_hooks`;
    return null;
  }

  let failures = target.failures;
  for (const row of batch) {
    const outcome = yield* send(target.base_url, {
      id: row.id,
      receivedAt: row.received_at,
      method: row.method,
      rawHookId: row.raw_hook_id,
      rawToken: row.raw_token,
      query: row.query,
      headers: decodeHeaders(row.headers),
      body: row.body,
    });
    if (outcome === "retry") {
      failures += 1;
      yield* setFailures(failures);
      return (yield* Clock.currentTimeMillis) + retryDelayMs(failures);
    }
    yield* sql`DELETE FROM held_hooks WHERE id = ${row.id}`;
    if (failures !== 0) {
      failures = 0;
      yield* setFailures(0);
    }
  }
  return (yield* hasPending) ? yield* Clock.currentTimeMillis : null;
});
