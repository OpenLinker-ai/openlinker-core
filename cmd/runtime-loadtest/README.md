# Runtime load test

`runtime-loadtest` drives the same reliable Runtime state machine through
two transports:

- WebSocket is the primary transport. It performs an authenticated upgrade,
  sends `runtime.hello`, waits for `runtime.ready`, and receives server-pushed
  assignments and cancellation commands.
- HTTP Pull is the long-poll fallback. It creates the same durable Session and
  uses the same offer, Attempt, lease, fence, ACK, cancellation, and resume
  identities.

The command uses the published Go SDK at the exact module version pinned in
`go.mod`. Its report reads the contract ID, digest, protocol version, and
required features from that dependency. It has no compatibility execution
path: missing Node credentials, a contract mismatch, or an unavailable forced
transport fails the run.

## Before running

The account Auth API, Core API, and dedicated Runtime listener may be different
origins. The Auth API creates disposable users. Core creates Agents, Agent
Tokens, and Runs. Runtime traffic must use the mTLS listener. In a single-service
deployment, `OPENLINKER_AUTH_API_ROOT` may be omitted and defaults to
`OPENLINKER_API_ROOT`.

Issue one load-generator Node whose capacity covers the number of worker
Sessions. The client CA key stays on the provisioning host:

```bash
DATABASE_URL='postgres://...' ./bin/api runtime-node issue \
  --ca-cert /secure/runtime-client-ca.crt \
  --ca-key /secure/runtime-client-ca.key \
  --display-name 'Runtime load generator' \
  --capacity 100 \
  --cert-out ./node-pki/loadtest.crt \
  --key-out ./node-pki/loadtest.key
```

Keep the `node_id` from the JSON output. The Runtime server CA below is the CA
that signed the Core mTLS listener certificate, not the client CA used to sign
the Node certificate.

```bash
export OPENLINKER_NODE_ID='00000000-0000-4000-8000-000000000001'
export OPENLINKER_API_ROOT='http://127.0.0.1:8080/api/v1'
# Set this when Cloud owns account registration and Core has a separate origin.
export OPENLINKER_AUTH_API_ROOT='http://127.0.0.1:8080/api/v1'
export OPENLINKER_RUNTIME_URL='https://127.0.0.1:8443'
export OPENLINKER_RUNTIME_MTLS_CERT_FILE="$PWD/node-pki/loadtest.crt"
export OPENLINKER_RUNTIME_MTLS_KEY_FILE="$PWD/node-pki/loadtest.key"
export OPENLINKER_RUNTIME_MTLS_CA_FILE="$PWD/node-pki/runtime-server-ca.crt"
```

The private key, certificate, CA, Node ID, and HTTPS Runtime origin are
required. Validation happens before any disposable account or Agent is
created. Each worker also fsyncs its Attempt identity, Event sequence, pending
Result ID, and ACK state to `-state-dir` (mode `0700`, files `0600`) before
advancing the wire protocol. Tokens and invocation capabilities are not stored.

## Baseline transports

The default `auto` mode connects with WebSocket first, falls back to Runtime
Pull after a transport failure, probes WebSocket recovery, then resumes the
same in-flight Attempts before switching back.

```bash
go run ./cmd/runtime-loadtest \
  -api http://127.0.0.1:8080/api/v1 \
  -auth-api http://127.0.0.1:8080/api/v1 \
  -runtime-url https://127.0.0.1:8443 \
  -transport auto \
  -agents 10 -workers-per-agent 1 -node-capacity 10 \
  -runs 100
```

Use an explicit scenario when a test must not change transports:

```bash
# WebSocket only
go run ./cmd/runtime-loadtest \
  -transport ws -scenarios ws-only

# HTTP long-poll only
go run ./cmd/runtime-loadtest \
  -transport pull -scenarios pull-only
```

The exported environment variables above supply the API origin, Runtime
origin, Node ID, and three mTLS files. Run
`go run ./cmd/runtime-loadtest -help` for the complete list.

## Recovery and safety scenarios

Run scenarios independently so their prerequisites and assertions remain
clear.

```bash
# Planned WebSocket → Pull → WebSocket with active Attempt resume
go run ./cmd/runtime-loadtest \
  -transport auto -scenarios ws-pull-ws \
  -switch-after 3s -switch-back-after 8s -result-delay 12s

# Core A → Core B attachment and resume; both origins must share the DB/contract
go run ./cmd/runtime-loadtest \
  -transport ws -scenarios core-a-b-resume \
  -runtime-url-secondary https://core-b.example.test:8443 \
  -switch-after 3s -result-delay 8s

# Core commits each Pull request, but the client loses one response per ACK type
go run ./cmd/runtime-loadtest \
  -transport pull -scenarios ack-response-loss \
  -drop-ack-responses assignment,event,result,cancel

# Duplicate delivery must not start a second execution; stale fences must fail
go run ./cmd/runtime-loadtest \
  -transport auto -scenarios duplicate-assignment,stale-fence \
  -duplicate-assignments 2 -stale-fence-probes 1
```

The 1000-way cancellation scenario is a real client load, not an in-memory
race test. It requires at least one executing worker Session per cancellation;
the example uses 100 Agents with ten Agent Tokens each. Cancellation requests
are scheduled against the Result deadline so `Cancel/Result` and
`Cancel/ACK` contend in Core.

```bash
go run ./cmd/runtime-loadtest \
  -transport auto -scenarios cancel-race \
  -agents 100 -workers-per-agent 10 -node-capacity 1000 \
  -runs 1000 -run-concurrency 250 \
  -result-delay 10s -cancel-delay 10s \
  -cancel-count 1000 -cancel-concurrency 250 \
  -timeout 10m
```

For Redis signal outage, stop or isolate the Runtime Redis dependency outside
the load-test process while leaving the chosen Core address directly
reachable. The command refuses to start measured Runs until `/readyz` proves
`signal_dependency_unavailable`. It then completes Pull assignments through
the database polling path.

```bash
go run ./cmd/runtime-loadtest \
  -transport pull -scenarios redis-signal-outage \
  -redis-outage-observe 60s -runs 100
```

## Report contract

The JSON report separates `runtime.transports.ws`,
`runtime.transports.pull`, and their aggregate. Each section includes:

- hello/ready connection latency;
- offer-to-confirm and assignment latency;
- lease renew, Event ACK, Result ACK, and cancellation latency;
- replayed Event/Result ACKs and recovered assignment ACK response loss;
- stable error-code counts.

`runtime.switches` records endpoint and transport transitions;
`runtime.resume` records decisions and latency. The safety section must show
`duplicate_execution: 0` and `stale_fence_accepts: 0`. The Redis scenario also
requires `redis_signal_outage_observed: true` and a positive
`db_polling_fallback_completions` count.

Reports never contain Agent Tokens, private-key material, or certificate
contents.

## Slow connection capacity probe

`-connection-capacity` retains every accepted mTLS WebSocket while adding the
next batch. It checks Core health/readiness and requires at least 99% of the
target workers to remain connected. The default profile adds 25 workers per
stage at two connections per second, observes each stage for 30 seconds, runs a
small functional workload, and confirms the highest candidate for five
minutes.

Choose enough Agents to keep each Agent at or below the ten-token limit. The
overall timeout must cover the complete ramp and hold periods; validation
prints the minimum value for the selected target.

```bash
go run ./cmd/runtime-loadtest \
  -api http://127.0.0.1:8080/api/v1 \
  -auth-api https://cloud.example.test/api/v1 \
  -runtime-url https://127.0.0.1:8443 \
  -transport ws -scenarios ws-only \
  -connection-capacity \
  -agents 60 -workers-per-agent 10 -node-capacity 600 \
  -connection-step-size 25 -connection-step-hold 30s \
  -connect-stagger 500ms \
  -runs 1 -run-concurrency 1 \
  -hold-after-completion 5m -timeout 30m
```

The JSON `connection_capacity_report` contains every accepted or rejected
stage, the first rejected target, the five-minute confirmed stable value, and
the recommended value after retaining 20% operating headroom. Reaching the
configured target without a rejected stage is a lower bound, not proof that
the host cannot accept more connections.
