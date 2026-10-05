# Relay observability

> For maintainers. Using T3 Code? See [docs/user](../user/).

The relay Alchemy stack owns a shared Axiom trace setup:

- `t3-code-relay-traces-prod`, the OpenTelemetry trace dataset shared by the Worker, mobile app, and
  first-party relay clients
- `t3-code-relay-otel-ingest-prod`, the dataset-scoped Worker ingest token
- `t3-code-mobile-otel-ingest-prod`, the dataset-scoped mobile ingest token
- `t3-code-relay-client-otel-ingest-prod`, the dataset-scoped first-party relay-client ingest token
- `t3-code-relay-recent-spans-prod`, a view of recent request and endpoint spans

Alchemy stages append their sanitized stage name to isolate resources, for example
`t3-code-relay-traces-dev-julius` for a personal stage.

Deploy from `infra/relay` with the normal Alchemy workflow:

```sh
vp run deploy
```

Alchemy resolves account-level Axiom deployment credentials through its provider. At runtime, the
Worker receives only its scoped ingest token. Mobile and relay clients use their own separately
provisioned scoped ingest tokens.

The Worker emits Effect's built-in HTTP server spans plus endpoint and database child spans.
Effect's OpenTelemetry exporter stores semantic HTTP attributes below the `attributes.` prefix.
For example:

```apl
['t3-code-relay-traces-prod']
| where name startswith 'http.server'
| extend endpoint = column_ifexists('attributes.http.route', ''),
    customAttributes = column_ifexists('attributes.custom', dynamic({}))
| project _time, name, trace_id, duration,
    ['attributes.http.request.method'],
    ['attributes.url.path'],
    ['attributes.http.response.status_code'],
    endpoint,
    relayOperation = customAttributes['relay']['operation']
| order by _time desc
| limit 200
```

The provisioned view also reads the endpoint from `attributes.http.route`. Relay-specific span
annotations are stored under `attributes.custom`; `relay.operation` is one of the emitted custom
attributes.

Agents should prefer the provisioned view or APL queries for completed incidents instead of
tailing the Cloudflare Worker. The stack does not provision a separate query token. Responders who
need scripted query access use the authorized account-level `AXIOM_TOKEN` together with
`AXIOM_ORG_ID`; scoped ingest tokens remain write-only credentials for their producers.

DPoP proof failures include the stable `relay.dpop.failure_code` span attribute. A `time_window`
failure means that a signed proof was too old or too far in the future for the relay's allowed
window. It can point to a date or time problem on either device, but it can also result from a
delayed request. The client uses this category, and the absence of a category from an older relay,
to decide whether clock skew is confirmed or only one possible cause.

## Webhooks

A public webhook request is one `relay.hooks.forward` span. Its `relay.hook.outcome` says what
happened: `forwarded`, `held`, `rate_limited`, `inbox_full`, `not_found`, `payload_too_large`,
`environment_unavailable`, or `environment_timeout`. `relay.hook.endpoint_key` identifies the
managed endpoint, and with it the environment. On a forward, `relay.hook.upstream_status` or
`relay.hook.upstream_error` records the environment's answer. `relay.hook.rate_limit` says which
budget ran out: `endpoint` or `hook`. `relay.hook.rate_limiter_failed_open` is set when the
Cloudflare rate limiter was unavailable and the request went through unlimited.

Held requests are handled in the endpoint's `HookInboxObject`, and each call into it is its own
trace, not a child of the forward span: `relay.inbox.hold`, `relay.inbox.wake`, and
`relay.inbox.deliver` for each alarm run. Join them to forwards on the inbox, which is named by
endpoint key. A hold the inbox refused carries `relay.inbox.refused` (`max_per_hook`,
`max_requests`, `max_bytes`, or `already_held`). A delivery run carries `relay.inbox.run_result`
(`drained`, `more_pending`, `busy`, `unreachable`, or `failed`), how many requests it sent and
delivered, the consecutive failures behind the retry delay, the longest time a delivered request
waited, and the backlog left.

Questions these answer:

```apl
// Webhook outcomes per hour
['t3-code-relay-traces-prod']
| where name == 'relay.hooks.forward'
| summarize count() by bin(_time, 1h), outcome = tostring(['attributes.custom']['relay.hook.outcome'])

// Inboxes stuck behind an unreachable environment
['t3-code-relay-traces-prod']
| where name == 'relay.inbox.deliver'
| extend c = ['attributes.custom']
| where tostring(c['relay.inbox.run_result']) == 'unreachable'
| summarize runs = count(), failures = max(toint(c['relay.inbox.consecutive_failures'])),
    backlog = max(toint(c['relay.inbox.held_count'])) by inbox = tostring(c['relay.inbox.id'])
```
