import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";

import { withoutRedirects } from "../environments/EnvironmentConnector.ts";

/** Set by the relay on every forward; the environment uses it as the delivery id. */
const RELAY_DELIVERY_ID_HEADER = "x-t3-relay-delivery-id";
export const RELAY_HOOK_UPSTREAM_TIMEOUT_MS = 8_000;
// Cloudflare answers 530 when the tunnel for a hostname has no connected origin.
export const TUNNEL_OFFLINE_STATUS = 530;

/** A webhook request as the relay sends it on to the environment. */
export interface UpstreamHook {
  readonly id: string;
  readonly method: string;
  /** Path segments exactly as the sender sent them; the environment decodes them. */
  readonly rawHookId: string;
  readonly rawToken: string;
  /** Without the leading `?`. */
  readonly query: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: Uint8Array;
}

export interface UpstreamResponse {
  readonly status: number;
  readonly contentType: string | undefined;
  readonly body: Uint8Array;
}

/**
 * Sends a webhook request through the environment's tunnel. Fails when the
 * environment cannot be reached, and succeeds with None on timeout.
 */
export const sendUpstream = (baseUrl: string, hook: UpstreamHook) =>
  Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    const base = baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
    const headers: Record<string, string> = {
      ...hook.headers,
      [RELAY_DELIVERY_ID_HEADER]: hook.id,
    };
    let request = HttpClientRequest.make(hook.method as "GET" | "POST" | "PUT" | "PATCH")(
      `${base}api/hooks/${hook.rawHookId}/${hook.rawToken}${hook.query ? `?${hook.query}` : ""}`,
      { headers },
    );
    if (hook.method !== "GET") {
      request = HttpClientRequest.bodyUint8Array(request, hook.body, headers["content-type"]);
    }
    return yield* httpClient.execute(request).pipe(
      Effect.flatMap((response) =>
        response.arrayBuffer.pipe(
          Effect.map((bytes): UpstreamResponse => ({
            status: response.status,
            contentType: response.headers["content-type"],
            body: new Uint8Array(bytes),
          })),
        ),
      ),
      withoutRedirects,
      // The client span would record url.full, which carries the token.
      Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
      Effect.timeoutOption(Duration.millis(RELAY_HOOK_UPSTREAM_TIMEOUT_MS)),
    );
  });
