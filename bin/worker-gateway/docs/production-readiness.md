# Worker gateway production-readiness review

Date: 2026-10-07.
Base commit: `02fe2a1e` (main).
Tracking issue: #1584.

## Verdict

At `02fe2a1e` the worker gateway is not ready to sit in front of a validator's worker as a public endpoint.
Its core is sound: requests have header-read and whole-request deadlines, responses are streamed, readiness fails closed, the transaction screen cannot wrongly reject, and the container runs non-root on a read-only filesystem.
The problems are in what it lets through and what it shares.
One gateway can fill the worker's 500-request permit pool, which every gateway shares, so slow reads through one gateway stop submissions through all of them (WG-01, Critical).
One request with a large JSON `id` OOMs a gateway (WG-02), the global rate budget is spent before the per-IP check (WG-08), a batch costs one token however long it is (WG-03), one host can hold every connection slot (WG-06), the health probes queue behind clients (WG-07), and reads and submissions share every limit (WG-09).
The reference manifest never becomes ready (WG-19), every method including `tn_*` reaches the worker (WG-11), and the gateway-to-worker hop is plaintext only (WG-04).
This PR fixes the manifest's readiness URL, adds a `preStop` hook, logs the cause of upstream transport errors with the URL redacted, lowers the default request body cap to 1 MiB, and adds `--redirect-queries`, which takes every non-submission call off the worker and refuses upstream HTTP redirects.
With `--redirect-queries` set, WG-11 is closed, the gateway keeps serving reads while the worker is down (WG-14, as long as nothing in front of it acts on `/ready`), and slow reads no longer reach the worker (WG-01).
The post-change review found that this moves the shared failure point rather than removing it: a public RPC that stalls holds every gateway's connection slots and starves submissions on all of them until the follow-up for WG-01 and WG-09 lands, and a resolver that hangs on the public RPC's name can make readiness flap (see [Post-change review](#post-change-review)).
Every other finding of Medium or higher has a follow-up issue or operator guidance in the plan below.
WG-02 and WG-08 are small fixes and are recommended before any public deployment.
The remaining High findings should be closed before general availability.

## Scope and method

Reviewed:

- All of `bin/worker-gateway/**`: `src/*.rs`, `Cargo.toml`, `Dockerfile`, `README.md`, `deploy/README.md`, `deploy/k8s/*.yaml` and `grafana-worker-gateway.json`.
- The node-side contract the gateway depends on: `crates/node/src/health.rs` (the `/health/workers` listener), `crates/tn-reth/src/env/rpc.rs:327-330` (the two submission methods the node's fee-cap guard replaces) and `crates/execution/tn-rpc/src/rpc_ext.rs` (the `tn` namespace and its note that validators should not expose it).

Method:

- A review in separate passes, each given the same brief (the deployment model, the threat scenarios T1-T9, the severity scale and a checklist of 28 candidate findings that each pass had to confirm or refute from the code): denial of service with measurements, STRIDE threat classification, DREAD risk scoring, business logic and coupled state, dependencies and build, runtime state machines, cryptographic handling, correctness, docs and tests, and hardening sweeps (panics, blocking work, tracing, error propagation, resource bounds, task supervision).
  Consensus safety, smart contracts and determinism were checked and are out of scope: the gateway holds no consensus state and executes nothing.
- The passes' findings were merged by root cause and file:line into 53 findings.
- Every finding was then verified independently: it needed a file:line that resolves at `02fe2a1e`, a reproduction (a test, a command with observed output, or arithmetic plus a measurement), and a check that the impact holds in the deployment model below.
  Reproductions ran a debug build of the gateway at `02fe2a1e` on loopback against small mock upstreams (a mock worker RPC and a mock health listener); memory figures are peak RSS (`VmHWM`).
  No finding was refuted as a whole; the parts of claims that were refuted are in the appendix.
- One calibration pass re-scored all verified findings together against the scale, so the passes' scores do not drift apart.

Evidence is pinned to `02fe2a1e`.
Paths are relative to `bin/worker-gateway/` unless they name a crate.
Line numbers move with this PR's own commits; read them against the base commit.

Not reviewed: a real worker under load (WG-01 is confirmed from code, arithmetic and a mock, not against a running node), a real Kubernetes cluster (probe and HPA timings are computed from the manifests), the container image build, the observer-side transaction forwarding, and the behaviour of any particular CDN or DNS provider.

## Deployment model

Governance wants public RPC endpoints, and a validator must accept transactions from the public without exposing its worker RPC and without spending validator capacity on reads.

- Clients reach one or more public DNS names, served by round-robin A records, anycast, or a CDN or DDoS front.
- The names lead to N gateways on separate hosts (N of at least 2, often 3-10).
  Each gateway is stateless; every budget it enforces is per process.
- Behind a firewall sit the validator's private worker RPC (one `rpc_url` per worker) and the node's healthcheck listener (`--healthcheck <PORT>`, `GET /health/workers`).
- With `--redirect-queries <URL>` (added by this PR), every call except `eth_sendRawTransaction` and `eth_sendRawTransactionSync` goes to a public RPC node instead of the worker.

Trust boundaries:

1. Internet to gateway: unauthenticated, untrusted clients; one host or a botnet of about 100 IPs is the adversary the scale is written for.
2. Gateway to worker RPC: a private network, or the public internet to the validator host; plaintext `http` only at the base commit.
3. Gateway to node health listener: the same path as 2, unauthenticated.
4. Gateway to public RPC: a third party, `http` or `https`; its answers are what clients read.
5. DNS and the network paths between all of these, which an attacker may control in part.

## Threat scenarios

- **T1, volumetric L3/L4 against one gateway IP.**
  Gateway state is per process, so one gateway failing does not take the others down, with two exceptions: the worker's shared 500-request permit pool (WG-01) and the node's serial health listener (WG-13).
  One gateway's own capacity falls to WG-05, WG-06 and WG-07.
  Volume at L3/L4 has to be absorbed in front of the gateway; see Operator guidance.
- **T2, L7 floods.**
  A single request OOMs a gateway (WG-02), concurrent large bodies do the same (WG-05), one host holds every connection (WG-06), probes queue behind it (WG-07), one source drains the global budget (WG-08), and reads crowd out submissions (WG-09).
  Batches carry many calls for one token (WG-03), and expensive reads including `tn_*` reach the worker (WG-11) and drive WG-01.
  Slow-loris is bounded by the 10 s header-read timeout and the 40 s request deadline, and slow readers by `TCP_USER_TIMEOUT` and the 10-minute connection lifetime.
- **T3, amplification toward the validator.**
  One token per batch (WG-03), N replicas times the global budget (WG-17), the HPA growing N with upstream latency (WG-16), the shared permit pool (WG-01), and N gateways polling a listener that serves one connection at a time (WG-13).
  The gateway never retries a forward; HTTP redirect-following was the only replay path (WG-10).
- **T4, getting around the shield.**
  At the base commit a `307`/`308` from the query upstream would replay a read onto the worker (WG-10); this PR's proxy client follows no redirects.
  The classifier added by this PR matches the two submission methods exactly and case-sensitively, and sends everything it cannot read as submissions to the query upstream: mixed batches, case variants, unicode-escaped names, a method name inside `params`, trailing bytes, non-object batch elements and empty batches.
  The post-change review checks this again on the final diff.
- **T5, split-routing confusion.**
  Pending-state and fee reads served by the public RPC do not see the worker's pool (WG-23), and nothing checks that the redirect points at the same chain (WG-24).
  A submission inside a mixed batch enters the network through the public RPC, not this validator.
- **T6, limits across replicas.**
  Every budget is per process (WG-17), the HPA multiplies it (WG-16), and the per-process connection cap does not protect the worker's shared pool (WG-01).
- **T7, DNS attacks on the advertised names.**
  Round-robin records keep sending clients to a dead gateway (WG-21).
  No error body, response header or metric leaks the worker's address.
  A TCP-terminating front hides client identity (WG-18), so operators must choose between per-client limits and an origin reachable only through the front.
  Registrar or DNS hijack, provider DDoS and origin discovery have no code finding; they are covered in Operator guidance.
- **T8, trusting the upstream.**
  A plaintext worker hop lets an on-path attacker drop submissions behind valid-looking hashes and forge reads and readiness (WG-04).
  A malicious health endpoint can stream an unbounded body (WG-28).
  With an `https` redirect the public RPC's certificate is checked against the system roots; plain `http` to a public host logs a warning at startup.
- **T9, information leaks.**
  The transport error cause was dropped, and logging it naively would leak the URL's path and query (WG-25, fixed with the URL redacted).
  Upstream URLs with credentials appear in debug readiness logs and startup errors (WG-35).
  No client-facing error body carries an upstream URL.

## Severity scale

- **Critical:** a remote, unauthenticated attacker can stop transaction intake on all of a validator's gateways at once; or get past the gateway to crash or overload the private worker or node badly enough to affect consensus participation; or make the gateway silently drop, alter or misreport an accepted submission.
- **High:** one host, or a botnet of about 100 IPs or fewer, can take one gateway down (OOM, slot exhaustion, starving submissions); or push 10× or more of the configured budget onto the validator's worker; or make the reference deployment defeat the read shield.
- **Medium:** availability or correctness gets worse under realistic conditions, such as misleading readiness, errors during restarts, rate budgets multiplied across replicas, failures that cannot be diagnosed, or split-routing inconsistency.
  It must be fixed or documented with a mitigation before general availability; a tracked issue is acceptable.
- **Low:** hardening, small protocol deviations, or observability gaps that have a workaround.
- **Info:** design notes, things left out of scope on purpose, and things already done well.

Calibration rules: a mechanism that works per replica stays High even when it can be repeated against all N gateways; Critical needs a shared dependency, consensus impact or a silent misreport; findings that need an on-path, DNS or upstream-compromise position are capped at High; findings that only exist with `--redirect-queries` are scored as if the option were set.

## Summary

Status refers to the rows of the [plan](#plan-to-address-medium-and-above) (P1-P27), which name the commit in this PR or the follow-up issue.

| ID | Title | Severity | Status | Effort |
| --- | --- | --- | --- | --- |
| WG-01 | One gateway can fill the worker's shared 500-request permit pool | Critical | mostly closed in this PR when `--redirect-queries` is set (P1); #1594 (P2) | M |
| WG-02 | Reject paths build an attacker-sized `id` as a `Value`; one request OOMs a gateway | High | #1595 (P3), recommended before any public deployment | S |
| WG-03 | A batch costs one token, skips the screen and has no length cap | High | #1600 (P10) | M |
| WG-04 | The gateway-to-worker hop is plaintext only | High | #1601 (P11) | M-L |
| WG-05 | Request bodies up to 25 MiB are buffered whole on up to 500 connections | High | partly fixed in this PR (P6); #1597 (P7) | S + M |
| WG-06 | No per-IP connection cap; one host holds every slot | High | #1599 (P9) | M |
| WG-07 | Health probes share the client connection permits | High | #1598 (P8) | M |
| WG-08 | The global token is spent before the per-IP check | High | #1596 (P4), recommended before any public deployment | S |
| WG-09 | Reads and submissions share every limit | High | #1594 (P2) | M |
| WG-10 | Upstream clients follow HTTP redirects and honour proxy variables | High | fixed for the proxy client in this PR (P5); rest in #1612 (P25) | S |
| WG-11 | Every method reaches the worker, including `tn_*` | Medium | closed in this PR when `--redirect-queries` is set (P1); #1602 (P15) | S |
| WG-12 | Readiness has no hysteresis | Medium | #1605 (P18) | S |
| WG-13 | The node health listener is serial and unauthenticated | Medium | documented in this PR (P14) | S |
| WG-14 | Readiness gates reads as well as submissions | Medium | closed for reads at the gateway in this PR when `--redirect-queries` is set (P1); fronts that act on `/ready` in #1613 (P26) | S |
| WG-15 | Readiness is measured on the health listener, not on the RPC path | Medium | #1605 (P18) | M |
| WG-16 | The reference HPA multiplies the worker-facing budget | Medium | documented in this PR (P14) | S |
| WG-17 | Every budget is per process | Medium | documented in this PR (P14) | S |
| WG-18 | Client identity is the TCP peer | Medium | #1607 (P20) | M |
| WG-19 | The reference manifest polls the wrong readiness URL | Medium | fixed in this PR (P12) | S |
| WG-20 | Shutdown does not drain | Medium | partly fixed in this PR (P12); #1606 (P19) | S + M |
| WG-21 | Round-robin DNS has no health feedback | Medium | documented in this PR (P14) | S |
| WG-22 | Non-JSON upstream errors are relayed and counted as forwarded | Medium | #1603 (P16) | S |
| WG-23 | Pending-state reads miss the worker's pool under the redirect | Medium | documented in this PR (P14); #1608 (P21) | M |
| WG-24 | Redirected reads leave from one egress IP with a spoofable XFF | Medium | documented in this PR (P14); #1609 (P22) | M |
| WG-25 | The upstream transport error cause is dropped | Medium | fixed in this PR (P13) | S |
| WG-26 | No access log or client dimension | Medium | #1610 (P23) | M |
| WG-27 | CORS cannot work | Medium | #1604 (P17) | S |
| WG-28 | The readiness poller polls serially and reads the body unbounded | Low | #1612 (P25) | S |
| WG-29 | The per-IP table fails open when full; IPv6 /64 keys | Low | #1612 (P25) | S |
| WG-30 | The 408 rewrite misreports | Low | #1612 (P25) | S |
| WG-31 | The lifetime cap cuts an in-flight request | Low | #1612 (P25) | S |
| WG-32 | The screen skips the Sync method, escaped names and other param shapes | Low | #1612 (P25) | S |
| WG-33 | A mid-stream upstream failure gives a truncated `200` | Low | #1612 (P25) | S |
| WG-34 | The binary hop marker breaks gateway chaining and is client-settable | Low | redirect hop addressed in this PR (P1); rest in #1612 (P25) | S |
| WG-35 | Upstream URLs with credentials in debug logs and startup errors | Low | #1612 (P25) | S |
| WG-36 | The shipped feature set and image are never built in CI | Low | #1612 (P25) | S |
| WG-37 | Image build not `--locked`, unpinned base images, `:latest` tag | Low | #1612 (P25) | S |
| WG-38 | Large transitive dependency graph | Low | #1612 (P25) | M |
| WG-39 | TLS pitfalls: an empty root store builds silently | Low | #1612 (P25) | S |
| WG-40 | CLI validation gaps (zero durations, duplicate ids, mapped IPv6) | Low | #1612 (P25) | S |
| WG-41 | The screen's rejected types match the pool by convention only | Low | #1612 (P25) | S |
| WG-42 | Not every error is a JSON-RPC envelope; every HTTP method is forwarded | Low | #1612 (P25) | S |
| WG-43 | Errors use non-200 statuses | Low | #1612 (P25) | S |
| WG-44 | Batch errors come back as one object with `id: null` | Low | #1612 (P25) | S |
| WG-45 | XFF appended to the client's value; XFP hard-coded | Low | #1612 (P25) | S |
| WG-46 | `upstream_ready{worker_id}` collides; in-flight gauge misses streaming | Low | #1612 (P25) | S |
| WG-47 | Unthrottled warns on some rejects; none on others | Low | #1612 (P25) | S |
| WG-48 | A never-ready gateway logs no reason at info | Low | #1612 (P25) | S |
| WG-49 | Docs contradict the code in several places | Low | #1612 (P25) | S |
| WG-50 | ANSI escapes in non-TTY logs; no structured format | Low | #1612 (P25) | S |
| WG-51 | The Grafana datasource uid is hard-coded | Low | #1612 (P25) | S |
| WG-52 | An EIP-7702 rejection is reported as "EIP-4844 blob" | Low | #1612 (P25) | S |
| WG-53 | No WebSocket, HTTP/2, TLS termination or auth, by design | Info | no action | - |

## Findings

### WG-01 One gateway can fill the worker's shared 500-request permit pool — Critical

- **Evidence:** `src/cli.rs:97-100` defaults `--max-connections` to 500, and `src/app.rs:70-73` builds the proxy client with no cap on concurrent upstream requests.
  On the worker, `crates/tn-reth/src/rpc_server_args.rs:67-68` sets the RPC connection limit, and jsonrpsee-server 0.26.0 holds one permit per in-flight request (`server.rs:1028-1029`, `1122-1133`) and answers `429` text/plain "Too many connections" over the limit (`transport/http.rs:212-214`).
  ```rust
  // crates/tn-reth/src/rpc_server_args.rs:67-68
  /// Default number of incoming connections.
  pub(crate) const RPC_DEFAULT_MAX_CONNECTIONS: u32 = 500;
  ```
- **Reproduction:** arithmetic plus a mock.
  Each open inbound request holds one upstream request for as long as the worker takes to answer, so one gateway at its default 500 connections can hold all 500 worker permits.
  A mock worker answering `429` text/plain "Too many connections" was relayed to the client verbatim (see WG-22).
  Not run against a real worker.
- **Impact:** the permit pool is shared by every gateway in front of the worker, so slow calls through one gateway make the worker refuse submissions arriving through all the others: intake stops on every gateway at once.
  Permits are released when the worker finishes a call, not when the client reads the response, so the attacker needs calls that are slow on the worker (`eth_getLogs`, `eth_call`, `tn_*`).
  With `--redirect-queries` reads no longer reach the worker, which removes that path; 500 concurrent `eth_sendRawTransactionSync` calls, each held up to 30 s, can still fill the pool.
- **Recommendation:** cap concurrent upstream requests per route, keep the fleet's total below the worker's `--rpc.max-connections`, and fail fast with a `503` envelope over the cap.
  Until then, size the fleet as described in Operator guidance.
- **Effort:** M.
- **Status:** mostly closed in this PR by `feat(worker-gateway): serve non-submission calls from --redirect-queries` when `--redirect-queries` is set (P1); the cap is #1594 (P2).

### WG-02 Reject paths build an attacker-sized `id` as a `Value`; one request OOMs a gateway — High

- **Evidence:** `src/error.rs:263-279` recovers the `id` member as a full `serde_json::Value`, with no size or type limit.
  It runs on every gateway-side rejection: the hop marker (`src/proxy.rs:85-91`), the screen (`100`), no ready upstream (`105`) and upstream errors (`115`).
  ```rust
  // src/error.rs:271-272
  member.and_then(|member| match member.as_ref() {
      "id" => members.next_value::<Value>().map(Some),
  ```
- **Reproduction:** one request whose `id` was a 25,165,823-byte JSON array of zeros, sent with `X-TN-Gateway: 1`, got `508` and drove peak RSS to 844,360 kB.
  Without the header and with no ready upstream it got `503` and 844,516 kB.
  The same body forwarded normally peaked at 33,740 kB.
- **Impact:** one request reaches about 3.2× the reference 256Mi limit, so one host OOM-kills a gateway with one request, repeatably; any client can force the reject path by sending the hop header.
  The redirect does not help, because the hop-marker check runs before routing.
  At this PR's 1 MiB default a request costs about 34 MiB while its `id` is built (the measured ratio is about 34× the `id` size), and as many requests build at once as the runtime has worker threads: 50 concurrent requests peaked at 613 MB with 17 threads and at 89 MB on one CPU.
- **Recommendation:** recover the `id` with a visitor that accepts only `null`, numbers and strings up to a small length, and echoes `null` otherwise, without building a `Value`.
- **Effort:** S.
- **Status:** #1595 (P3); recommended before any public deployment.

### WG-03 A batch costs one token, skips the screen and has no length cap — High

- **Evidence:** the rate-limit middleware calls `check` once per HTTP request (`src/ratelimit.rs:481-499`).
  The screen only reads single-call objects, so a batch is forwarded unscreened (`src/proxy.rs:235-238`).
  Neither the gateway nor the worker limits batch length (reth rpc-builder `config.rs:169-175` sets none, so jsonrpsee's default is unlimited).
  ```rust
  // src/proxy.rs:235-236
  // Single-call objects only; a batch (a JSON array) fails the map visitor and
  // is forwarded, left to the worker to validate per element.
  ```
- **Reproduction:** with per-IP 1/s and burst 2, one 200-element batch got `200` and the mock saw one POST carrying 200 calls.
  A type-3 transaction inside a batch was forwarded; the same call alone got `400`.
  The worker's 15 MiB request cap holds about 135k `eth_getBalance` calls.
- **Impact:** one token carries up to about 135k unscreened calls to the worker, far over 10× `--rate-limit-global`.
  `--redirect-queries` sends read and mixed batches to the public RPC, but an all-submission batch still reaches the worker for one token; at the 1 MiB default that is still thousands of submissions.
- **Recommendation:** charge one token per element, cap batch length with `413` / `-32003`, and screen every element of an all-submission batch.
- **Effort:** M.
- **Status:** #1600 (P10).

### WG-04 The gateway-to-worker hop is plaintext only — High

- **Evidence:** `src/cli.rs:374-385` rejects any non-`http` upstream, because the workspace `reqwest` has no TLS backend (root `Cargo.toml:249-252`); readiness uses the same transport (`src/readiness.rs:197-225`).
  ```rust
  // src/cli.rs:379-383
  fn ensure_http_scheme(url: &Url) -> eyre::Result<()> {
      eyre::ensure!(
          url.scheme() == "http",
          "unsupported URL scheme `{}` in `{url}`: the worker gateway is HTTP-only (no TLS)",
  ```
- **Reproduction:** an `https` upstream URL fails at startup with "unsupported URL scheme `https`" (exit 1).
  A man-in-the-middle mock forged readiness and answered a submission with the transaction's keccak hash and a fake block number; the gateway passed all of it through.
- **Impact:** where the hop crosses a network the operator does not control, an on-path attacker can drop submissions while returning their computable hash, so clients believe they were accepted, and can forge reads and readiness.
  Signed transactions cannot be altered.
  An `https` redirect protects redirected reads only.
  Capped at High because it needs an on-path position.
- **Recommendation:** accept `https` worker URLs (optionally with a client certificate), or require a tunnel when the hop leaves a private network.
- **Effort:** M for TLS, L for mTLS.
- **Status:** #1601 (P11); until then see Operator guidance.

### WG-05 Request bodies up to 25 MiB are buffered whole on up to 500 connections — High

- **Evidence:** `src/proxy.rs:44` sets the default cap, `src/server.rs:129` applies it as the body limit, `src/cli.rs:97-100` allows 500 connections, and `deploy/k8s/deployment.yaml:70-72` limits the pod to 256Mi.
  The worker's own request cap is 15 MiB (`crates/tn-reth/src/rpc_server_args.rs:59-60`).
  ```rust
  // src/proxy.rs:44
  pub(crate) const MAX_REQUEST_BYTES: usize = 25 * 1024 * 1024;
  ```
- **Reproduction:** with the mock upstream delayed 25 s, K bodies held one byte short of the cap gave 236, 262 and 287 MiB peak RSS at K = 9, 10 and 11.
  Complete 26,214,381-byte bodies gave 253-311 MiB at K = 9 and 274 MiB at K = 10.
- **Impact:** about 10 connections from one IP, within its per-IP burst, exceed the reference memory limit and OOM the pod, and the attacker can repeat it.
  The gateway accepts bodies the worker would refuse anyway.
- **Recommendation:** default to 1 MiB, document the sizing rule, size the reference manifest to match, and budget total in-flight request bytes.
- **Effort:** S for the default, M for the byte budget.
- **Status:** partly fixed in this PR by `fix(worker-gateway): lower the default request body cap to 1 MiB` (P6): the default is 1 MiB, the README states the sizing rule, and the reference memory limit is 1Gi; the byte budget is #1597 (P7).

### WG-06 No per-IP connection cap; one host holds every slot — High

- **Evidence:** `src/server.rs:207-228`: the only connection limit is one global semaphore, acquired before `accept`, so excess connections wait in the kernel backlog.
  ```rust
  // src/server.rs:216-219
  let permit = tokio::select! {
      () = &shutdown => break,
      permit = Arc::clone(&limiter).acquire_owned() => permit,
  };
  ```
- **Reproduction:** with `--max-connections 8`, eight keep-alive connections from 127.0.0.2 sending 1.6 requests/s left 127.0.0.3 without a response for 30 s, and `curl -m 1 /health` timed out.
  Trickled bodies hold a slot for the 40 s request deadline.
  An idle keep-alive connection closed at 10.00 s, so the holder has to keep sending; at the default 500 slots that is about 56 requests/s from one host.
- **Impact:** one host starves every other client of one gateway, submissions included.
  It works per replica and can be repeated against all N; the redirect does not change it.
- **Recommendation:** cap concurrent connections per client prefix at accept time.
- **Effort:** M.
- **Status:** #1599 (P9).

### WG-07 Health probes share the client connection permits — High

- **Evidence:** `/health` and `/ready` are routes on the same server (`src/server.rs:125-128`) behind the same connection semaphore (`211-228`).
  The reference probes use port `rpc` with Kubernetes' default 1 s timeout and failure threshold 3 (`deploy/k8s/deployment.yaml:53-65`).
- **Reproduction:** with `--max-connections 4` and four keep-alive connections each sending `GET /health`, `curl -m 1` to `/health` and to `/ready` timed out in 4 of 4 tries; with trickled bodies, 16 of 16.
  With the manifest's probe settings the kubelet restarts the container after about 21-31 s.
- **Impact:** the probes are exempt from rate limiting but not from the connection cap, so a connection flood (WG-06) becomes liveness restarts and readiness flaps.
  This holds when gateways are exposed at L4 or by DNS; a front that terminates TCP breaks the chain.
- **Recommendation:** serve the probes on a separate listener or from reserved permits, and point the manifest's probes there.
- **Effort:** M.
- **Status:** #1598 (P8).

### WG-08 The global token is spent before the per-IP check — High

- **Evidence:** `src/ratelimit.rs:421-437` takes a global token first and never refunds it when the per-IP bucket then refuses the request.
  ```rust
  // src/ratelimit.rs:427-435
  let global_ok = self.global.as_ref().is_none_or(|global| {
      lock(&global.bucket).try_admit(
          now,
          global.limit.tokens_per_sec(),
          global.limit.capacity(),
      )
  });
  let allowed = global_ok
      && self.per_ip.as_ref().zip(peer).is_none_or(|(per_ip, ip)| per_ip.admit(now, ip));
  ```
- **Reproduction:** with global 1/s burst 3 and per-IP 1/s burst 1, 127.0.0.2 sent three requests and got `200`, `429`, `429`; the first request from 127.0.0.3 then got `429`, and only got `200` after 3.5 s.
  At the defaults, one source at about 52k requests/s left a second client with 9 `200`s and 98 `429`s.
  No test covers the ordering.
- **Impact:** the per-IP limit does not protect other clients: one source above the global rate (3000/s by default) drains the bucket for everyone, submissions included.
  Exceeding its own per-IP limit alone is not enough.
- **Recommendation:** run the per-IP check first and spend a global token only for requests it admits.
- **Effort:** S.
- **Status:** #1596 (P4); recommended before any public deployment.

### WG-09 Reads and submissions share every limit — High

- **Evidence:** one connection semaphore (`src/server.rs:211-212`), one global bucket (`src/ratelimit.rs:425-437`, `481-499`) and one upstream client with a 30 s timeout (`src/app.rs:70-73`) serve every call.
- **Reproduction:** with global 2/s and burst 2, reads from 127.0.0.2 got `200`, `200`, `429`, and then a submission from 127.0.0.3 got `429` / `-32006`.
  With `--max-connections 2` and two slow reads in flight, submission latency rose from 0.001 s to 4.54 s.
- **Impact:** a read flood or slow reads starve submissions on that gateway.
  With the redirect, the semaphore and the bucket are still shared, and a slow public RPC holds slots for up to the 30 s upstream timeout.
- **Recommendation:** per-route in-flight caps and a budget reserved for submissions (the same change as WG-01).
- **Effort:** M.
- **Status:** #1594 (P2).

### WG-10 Upstream clients follow HTTP redirects and honour proxy variables — High

- **Evidence:** `src/app.rs:70-74` builds both clients without `.redirect(..)` or `.no_proxy()`; reqwest 0.12.28 defaults to following up to ten redirects and to the system proxy (`async_impl/client.rs:308-310`, `419-421`).
- **Reproduction:** a stub upstream answering `307` to `http://127.0.0.1:20911/` made the gateway re-POST the client's body there, and the client got that host's answer.
  `308` behaves the same, and a self-redirect made 11 POSTs before a `502`.
  `HTTP_PROXY` and `ALL_PROXY` captured both forwarded POSTs and readiness GETs, with no loopback exemption.
- **Impact:** reth never sends a `3xx`, so at the base commit this needs a man-in-the-middle or operator environment variables.
  With `--redirect-queries` the query upstream is a third party, and one `307` from it, malicious or intercepted, replays reads onto the private worker or any internal host: it defeats the read shield and is a server-side request forgery.
- **Recommendation:** `redirect(Policy::none())` and `no_proxy()` on both clients.
- **Effort:** S.
- **Status:** fixed for the proxy client in this PR by `feat(worker-gateway): serve non-submission calls from --redirect-queries` (P5); the readiness client and `no_proxy` are in #1612 (P25).

### WG-11 Every method reaches the worker, including `tn_*` — Medium

- **Evidence:** the only method check in `proxy()` is the screen (`src/proxy.rs:66-121`, `224-262`), so every method is forwarded.
  The node's own docs say the `tn` namespace should not be public (`crates/execution/tn-rpc/src/rpc_ext.rs:164-169`).
  ```rust
  // crates/execution/tn-rpc/src/rpc_ext.rs:164
  /// Validators should not expose the `tn` namespace publicly. A request for a block from a
  ```
- **Reproduction:** `tn_*`, `debug_*`, `trace_*`, `admin_*` and `txpool_*` calls, a batch and `POST /foo/bar` all reached the mock upstream.
  On a real worker the default HTTP API is `eth`, `net` and `web3` plus `tn`, and tn-reth strips `admin` and `txpool` (`crates/tn-reth/src/env/rpc.rs:320-323`, test `admin_and_txpool_are_still_stripped`).
- **Impact:** `tn_*` reaches the validator by default, and `debug_*` and `trace_*` do if the operator enables them.
  Public `tn_*` calls can churn the consensus-pack cache the node shares with state sync, under a tight bound; no consensus impact was shown.
- **Recommendation:** set `--redirect-queries` on validators; without it, refuse `tn_*`, `debug_*`, `trace_*` and `admin_*` by default.
- **Effort:** S.
- **Status:** closed in this PR by `feat(worker-gateway): serve non-submission calls from --redirect-queries` when `--redirect-queries` is set (P1); the allowlist is #1602 (P15).

### WG-12 Readiness has no hysteresis — Medium

- **Evidence:** each poll result replaces the upstream's state directly (`src/readiness.rs:144-151`), and the cause is logged only at debug (`183-196`).
- **Reproduction:** one slow (3 s) poll made `/ready` and proxied calls return `503` for 3 s; the info log said only "became not-ready".
- **Impact:** all gateways poll the same listener, so one slow poll drops all of them together, with no cause at the default log level.
- **Recommendation:** flip to not-ready after several consecutive failures and back after several successes, and log the cause at info on each transition.
- **Effort:** S.
- **Status:** #1605 (P18).

### WG-13 The node health listener is serial and unauthenticated — Medium

- **Evidence:** `crates/node/src/health.rs:42` sets a 2 s read timeout, `165-166` binds `0.0.0.0` with the comment "use firewall to protect this endpoint", and `205-243` serves one connection at a time.
  The gateway's poll timeout is also 2 s (`src/cli.rs:59-67`).
- **Reproduction:** the base gateway polled a model of the node's serial listener with a 2 s read timeout; four idle connections to the listener turned the gateway's `/ready` from `200` to `503` for 14 s.
- **Impact:** anyone who reaches the port can make every gateway unready at once.
  The node source says to firewall it; the gateway docs did not.
- **Recommendation:** firewall the port to the gateway hosts; on the node side, consider serving it concurrently.
- **Effort:** S (docs).
- **Status:** documented in this PR by `docs(worker-gateway): document query redirect topology and record post-change review` (P14).

### WG-14 Readiness gates reads as well as submissions — Medium

- **Evidence:** every proxied call is refused when no worker is ready (`src/proxy.rs:103-106`), and `/ready` follows the same state (`src/server.rs:156-162`).
  ```rust
  // src/proxy.rs:103-106
  let Some(rpc_url) = state.readiness.first_ready_rpc_url() else {
      warn!(target: "gateway::proxy", "no upstream worker ready; rejecting request");
      return error_response(&GatewayError::NoUpstreamReady, body.as_ref());
  };
  ```
- **Reproduction:** tests `server::tests::ready_reflects_upstream_state` and `rejects_with_jsonrpc_error_when_no_upstream_ready`; with a not-ready health mock, reads, batches and `GET /` all got `503` / `-32000`.
- **Impact:** a short worker outage fails every read on all N gateways together.
- **Recommendation:** serve reads from a public RPC that does not depend on this worker's readiness.
- **Effort:** S.
- **Status:** closed for reads at the gateway in this PR by `feat(worker-gateway): serve non-submission calls from --redirect-queries` when `--redirect-queries` is set (P1).
  `/ready` keeps meaning "can take submissions" by design, so a front that acts on `/ready` (the reference `readinessProbe`, a health-checked DNS record) still drops every gateway, and their reads, while the shared worker is down; the deploy README documents probing `/health` instead (P14), and a probe that reflects both routes is #1613 (P26).

### WG-15 Readiness is measured on the health listener, not on the RPC path — Medium

- **Evidence:** an upstream's `rpc_url` and `readiness_url` are independent (`src/config.rs:15-20`), forwarding always picks the first ready upstream (`src/readiness.rs:91-96`), and a forwarding failure never feeds back into readiness (`src/proxy.rs:103-117`).
- **Reproduction:** with a healthy health mock and a closed RPC port, `/ready` returned `200` and every POST `502`.
  With two upstreams and the first one's RPC dead, three POSTs got `502` and the second upstream was never used (test `unreachable_upstream_maps_to_bad_gateway` shows the single-upstream case).
- **Impact:** readiness says the gateway can take submissions while every submission fails; a health-checked front keeps routing to it.
- **Recommendation:** mark an upstream not ready after consecutive connection failures on the RPC path, and try the next ready upstream only when the request was never sent.
- **Effort:** M.
- **Status:** #1605 (P18).

### WG-16 The reference HPA multiplies the worker-facing budget — Medium

- **Evidence:** `deploy/k8s/hpa.yaml:20-29` scales between 2 and 10 replicas on average in-flight requests, which rise with upstream latency (`src/proxy.rs:75`, `153-162`); each replica has its own global budget (`src/ratelimit.rs:151-155`).
  ```yaml
  # deploy/k8s/hpa.yaml:20-21
  minReplicas: 2
  maxReplicas: 10
  ```
- **Reproduction:** with the mock delayed 3 s and 80 concurrent requests, the in-flight gauge read 80.
  Two gateways at global 20/s and burst 20 let 40 requests through to the mock.
  At the default 3000/s per replica the worker-facing ceiling goes from 6,000/s at 2 replicas to 30,000/s at 10.
- **Impact:** slowing the worker scales the fleet and raises the worker-facing ceiling fivefold; reaching 10 replicas needs a fleet-wide in-flight above 450, which about 100 IPs can sustain.
  The HPA needs a prometheus-adapter rule that is not shipped.
- **Recommendation:** size `--rate-limit-global` for `maxReplicas`, not `minReplicas`.
- **Effort:** S (docs).
- **Status:** documented in this PR by `docs(worker-gateway): document query redirect topology and record post-change review` (P14).

### WG-17 Every budget is per process — Medium

- **Evidence:** the buckets live in each process (`src/ratelimit.rs:151-154`, `404-414`, `src/app.rs:53-58`), and the README describes the global limit without a replica rule (`README.md:159-164`).
- **Reproduction:** two gateways at global 1/s and burst 10, 30 POSTs each: each answered 10 `200`s and 20 `429`s, and the mock received 20.
- **Impact:** N gateways pass N × `--rate-limit-global` to the worker.
- **Recommendation:** document the rule: the worker-facing budget is the per-gateway global limit times the largest number of gateways that can run.
- **Effort:** S (docs).
- **Status:** documented in this PR by `docs(worker-gateway): document query redirect topology and record post-change review` (P14).

### WG-18 Client identity is the TCP peer — Medium

- **Evidence:** the rate-limit key is the immediate peer address (`src/ratelimit.rs:21-26`, `479-492`, `src/server.rs:229`, `251-254`); the README says to run the gateway edge-facing (`README.md:200`), but the reference Service is a ClusterIP (`deploy/k8s/service.yaml:12`), which needs a front.
- **Reproduction:** with per-IP 1/s and burst 1, 127.0.0.2 sent `X-Forwarded-For: 1.1.1.1` and then `2.2.2.2` and got `200` then `429` (the header is ignored), while 127.0.0.3 got `200`.
  A PROXY protocol v1 preamble got `400`.
- **Impact:** behind a TCP-terminating front every client shares the front's buckets, so operators choose between per-client limits with a reachable origin and a front that merges every client into a few keys.
- **Recommendation:** take the client address from `X-Forwarded-For` or PROXY protocol v2 only when the peer is in a configured set of trusted CIDRs.
- **Effort:** M.
- **Status:** #1607 (P20).

### WG-19 The reference manifest polls the wrong readiness URL — Medium

- **Evidence:** `deploy/k8s/deployment.yaml:44-47` points readiness at the worker RPC's `:8545/ready`; the poller expects the node's per-worker envelope (`src/readiness.rs:199-224`), which only the healthcheck listener serves at `/health/workers` (`crates/node/src/health.rs:28`).
  ```yaml
  # deploy/k8s/deployment.yaml:46-47
  - name: WORKER_GATEWAY_UPSTREAM_READINESS_URL
    value: "http://tn-worker.telcoin-network.svc.cluster.local:8545/ready"
  ```
- **Reproduction:** a readiness mock answering `405` as jsonrpsee does gave `/ready` `503` and POST `503`; the same gateway polling a `/health/workers` mock gave `200` and `200`.
- **Impact:** deployed as shipped, every replica stays unready; it fails closed, so the mistake is visible.
- **Recommendation:** poll `http://<node>:<healthcheck port>/health/workers`.
- **Effort:** S.
- **Status:** fixed in this PR by `fix(worker-gateway): poll the node healthcheck in the reference manifest` (P12).

### WG-20 Shutdown does not drain — Medium

- **Evidence:** on SIGTERM the listener is dropped at once while in-flight requests finish (`src/server.rs:290-293`); `/ready` never reports draining, and the reference Deployment has no `lifecycle.preStop`.
  ```rust
  // src/server.rs:290-292
  // Stop accepting (drop the listener), then drain in-flight connections
  // until they finish or the graceful deadline elapses.
  drop(listener);
  ```
- **Reproduction:** after SIGTERM, new `/ready` and POST connections were refused from +0.1 s to +6 s and idle keep-alive connections were closed, while the in-flight POST still got `200` at +7 s.
- **Impact:** rollouts and scale-downs refuse new connections while endpoints and load balancers still route to the pod; DNS clients are refused for the record's TTL.
- **Recommendation:** a `preStop` sleep now; later, a shutdown delay during which `/ready` returns `503` while the listener keeps accepting.
- **Effort:** S for `preStop`, M for drain-aware shutdown.
- **Status:** partly fixed in this PR by `fix(worker-gateway): poll the node healthcheck in the reference manifest` (P12: `preStop` sleeps 5 s, inside the 40 s grace period); drain-aware shutdown is #1606 (P19).

### WG-21 Round-robin DNS has no health feedback — Medium

- **Evidence:** only the kubelet consumes `/ready` (`deploy/k8s/deployment.yaml:60-65`, `deploy/k8s/service.yaml:12`); a search of the READMEs and manifests for DNS, anycast, CDN, load balancing and health checks found three hits, none about an external front.
- **Reproduction:** the search above.
- **Impact:** with plain round-robin A records, a dead gateway keeps receiving 1/N of DNS answers until someone edits the zone.
- **Recommendation:** publish the gateways behind health-checked DNS or a load balancer that probes `/ready`.
- **Effort:** S (docs).
- **Status:** documented in this PR by `docs(worker-gateway): document query redirect topology and record post-change review` (P14).

### WG-22 Non-JSON upstream errors are relayed and counted as forwarded — Medium

- **Evidence:** every upstream response, whatever its status and content type, is streamed back and counted as `outcome="forwarded"` (`src/proxy.rs:108-112`, `153-182`); the worker's jsonrpsee answers 403, 415 and 429 with text/plain bodies (`transport/http.rs:88-111`, `202-214`).
- **Reproduction:** mock `429`, `403` and `415` text/plain responses were relayed verbatim, counted as forwarded and not logged.
- **Impact:** clients get non-JSON bodies, and worker saturation (WG-01) looks like success on the dashboard.
- **Recommendation:** wrap non-JSON error responses in a JSON-RPC envelope with the request's `id`, and count them under their own outcome.
- **Effort:** S.
- **Status:** #1603 (P16).

### WG-23 Pending-state reads miss the worker's pool under the redirect — Medium

New with `--redirect-queries`.

- **Evidence:** pool transactions are created with `propagate: false` (`crates/tn-reth/src/txn_pool.rs:20-22`, `161-172`), observers forward submissions only to the owning validator (`crates/tn-reth/src/forward.rs:1-8`), and fee quotes are per worker and per epoch (`crates/tn-reth/src/env/rpc.rs:237-271`).
- **Reproduction:** code trace; the option did not exist at the base commit.
- **Impact:** `eth_getTransactionCount(.., "pending")`, `eth_getTransactionByHash`, receipts right after a submission and calls at `pending` are answered by a node that has not seen the worker's pool, so clients that send several transactions quickly reuse nonces; fee quotes from the public node may lag an epoch boundary.
- **Recommendation:** document it; later, route pending-tag and by-hash reads to the worker, or keep them consistent another way.
- **Effort:** M.
- **Status:** documented in this PR by `docs(worker-gateway): document query redirect topology and record post-change review` (P14); #1608 (P21).

### WG-24 Redirected reads leave from one egress IP with a spoofable XFF — Medium

New with `--redirect-queries`.

- **Evidence:** all forwards use one client from the gateway's address, and `X-Forwarded-For` is appended to whatever the client sent (`src/proxy.rs:153-160`, `185-195`).
  ```rust
  // src/proxy.rs:189-193
  let chain = headers
      .get(X_FORWARDED_FOR)
      .and_then(|previous| previous.to_str().ok())
      .map(|previous| format!("{previous}, {peer_ip}"))
      .unwrap_or(peer_ip);
  ```
- **Reproduction:** a request from 127.0.0.5 with `X-Forwarded-For: 1.2.3.4` reached the mock as `1.2.3.4, 127.0.0.5`, from a single egress peer.
- **Impact:** the public RPC's per-IP limits throttle everyone behind one gateway, and one client can burn that quota on purpose; the public RPC cannot trust the forwarded client address.
  Nothing checks that the redirect serves the same chain as the worker.
  Reads only.
- **Recommendation:** agree an authentication header with the public RPC operator so it can exempt or meter gateways, and warn at startup when the redirect's `eth_chainId` differs from the worker's.
- **Effort:** M.
- **Status:** documented in this PR by `docs(worker-gateway): document query redirect topology and record post-change review` (P14); #1609 (P22).

### WG-25 The upstream transport error cause is dropped — Medium

- **Evidence:** `classify_error` keeps only `is_timeout()` (`src/proxy.rs:197-204`), and the warn at `114` logs only the variant.
  ```rust
  // src/proxy.rs:198-203
  fn classify_error(err: reqwest::Error) -> GatewayError {
      if err.is_timeout() {
          GatewayError::UpstreamTimeout
      } else {
          GatewayError::UpstreamUnreachable
      }
  ```
- **Reproduction:** a closed port and a resetting mock both produced `502` and the log line `err=UpstreamUnreachable` with no cause.
  reqwest's `Display` for the same error includes the URL's path and query (a debug readiness log printed `?key=SECRET_R`).
- **Impact:** `502` and `504` cannot be diagnosed from the logs, and a naive fix would leak any key in the URL.
- **Recommendation:** log `without_url()` with its `source()` chain, and the upstream's origin only.
- **Effort:** S.
- **Status:** fixed in this PR by `fix(worker-gateway): log the upstream transport error cause` (P13).

### WG-26 No access log or client dimension — Medium

- **Evidence:** metric labels are only `le`, `outcome`, `reason`, `version` and `worker_id` (`src/telemetry.rs:31-45`, `74-89`), and `429`s are not logged (`src/ratelimit.rs:483-498`).
- **Reproduction:** six `429`s produced no log lines.
- **Impact:** operators cannot attribute the floods in WG-05 to WG-09 to their sources; a CDN front, if any, may keep its own logs.
- **Recommendation:** an optional sampled access log (client prefix, method class, route, status, latency) and a bounded per-prefix rejection view.
- **Effort:** M.
- **Status:** #1610 (P23).

### WG-27 CORS cannot work — Medium

- **Evidence:** no `CorsLayer` is installed (`src/server.rs:126-131`), and only `Content-Type` is copied back from the upstream (`src/proxy.rs:179-181`).
- **Reproduction:** a preflight got `204` with only a `date` header, and a POST with `Origin` got no `Access-Control-Allow-Origin` although the mock sent `*`; the forwarded `OPTIONS` lost its `Origin` header.
- **Impact:** browser dApps cannot use the advertised endpoint unless a front adds CORS.
- **Recommendation:** an optional allowed-origins flag, with preflights answered by the gateway.
- **Effort:** S.
- **Status:** #1604 (P17).

### WG-28 The readiness poller polls serially and reads the body unbounded — Low

- **Evidence:** `src/readiness.rs:139-175` polls upstreams one after another; `183-202` reads the whole response body, bounded only by time.
- **Reproduction:** a health mock streaming a 1e13-byte body pushed peak RSS to 6,599,424 KiB in 16 s; with four upstreams and three hanging, the fourth was polled only every 6 s.
- **Impact:** needs a malicious health endpoint or a man-in-the-middle, which already has WG-04.
- **Recommendation:** cap the body size and poll upstreams concurrently.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-29 The per-IP table fails open when full; IPv6 /64 keys — Low

- **Evidence:** a full table admits new clients untracked (`src/ratelimit.rs:56`, `351-368`).
- **Reproduction:** test `ratelimit::tests::full_table_admits_untracked_new_ip`; one /48 holds 65,536 /64 keys.
- **Impact:** amplifies WG-08 and WG-09; the global bucket still caps load.
- **Recommendation:** refuse or share a bucket for untracked clients when the table is full, and document it.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-30 The 408 rewrite misreports — Low

- **Evidence:** every `408` is rewritten into the gateway's envelope, including a real upstream `408`, and the deadline also covers the upstream wait (`src/server.rs:140-149`, `src/app.rs:132-139`).
- **Reproduction:** a mock `408` reached the client as `408` / `-32005` with `id: null` and was counted as both forwarded and rejected; with a 3 s + 2 s deadline, a trickled valid transaction reached the mock but the client got `408`.
- **Impact:** a delivered transaction can be reported as "did not complete"; reth never sends `408`, and the case needs a slow upload plus a slow worker.
- **Recommendation:** rewrite only the timeout layer's own `408`, and say in the error that delivery is unknown.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-31 The lifetime cap cuts an in-flight request — Low

- **Evidence:** `src/server.rs:257-289` closes the connection at the lifetime cap even mid-request (`src/cli.rs:116-134`).
- **Reproduction:** with a 5 s cap, two requests got `200` at 2.01 s and 4.05 s, and the third got EOF at 5.01 s after it had reached the mock.
- **Impact:** a busy pooled connection can lose a response for a delivered call; documented in `--help`.
- **Recommendation:** at the cap, stop accepting new requests on the connection and let the current one finish.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-32 The screen skips the Sync method, escaped names and other param shapes — Low

- **Evidence:** the screen matches `eth_sendRawTransaction` literally and reads only a positional hex string (`src/proxy.rs:46-48`, `224-242`, `263-267`, `359-364`).
- **Reproduction:** a type-3 transaction as a plain call got `400`; the same transaction via `eth_sendRawTransactionSync`, a unicode-escaped method name, named params or a `u8` array was forwarded, and the worker decodes all of them.
  reth's 30 s sync timeout equals the gateway's upstream timeout.
- **Impact:** the pool still refuses EIP-4844 and EIP-7702, so the cost is a worker round trip; the Sync call can race the gateway's timeout.
- **Recommendation:** screen the Sync method and the other param shapes, and set the gateway's timeout above reth's sync timeout.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-33 A mid-stream upstream failure gives a truncated `200` — Low

- **Evidence:** the status is sent before the body streams (`src/proxy.rs:108-112`, `177-182`).
- **Reproduction:** a mock sent `200` with `Content-Length: 1000`, 36 bytes, then closed; the client got a chunked `200` with no final chunk (curl exit 18), counted as forwarded, with no warning.
- **Impact:** the client sees a framing error; metrics overcount success.
- **Recommendation:** log and count body-stream failures; preserve the upstream's `Content-Length`.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-34 The binary hop marker breaks gateway chaining and is client-settable — Low

- **Evidence:** any inbound `X-TN-Gateway` gets `508`, and every forward sets it (`src/proxy.rs:50-54`, `82-92`, `156`).
- **Reproduction:** a request with the header got `508` / `-32004`, one warning and +1 on `loop_detected`; gateway B in front of gateway A answered 3 of 3 requests with `508`.
- **Impact:** a public RPC fronted by a gateway would reject redirected reads if the redirect carried the marker; client-set markers only reject the client's own request, with log noise capped by the rate limits.
- **Recommendation:** use a distinct marker on the redirect hop; consider a hop count instead of a flag.
- **Effort:** S.
- **Status:** the redirect hop is addressed in this PR by `feat(worker-gateway): serve non-submission calls from --redirect-queries` (P1: the query route sends `X-TN-Gateway-Redirect` and never `X-TN-Gateway`); the rest is #1612 (P25).

### WG-35 Upstream URLs with credentials in debug logs and startup errors — Low

- **Evidence:** `src/readiness.rs:186`, `191` log the readiness URL at debug; `src/cli.rs:379-386`, `405-409` print the full URL in startup errors.
- **Reproduction:** at debug level every poll logged the URL's userinfo, path and query; startup errors printed `user:secret`.
- **Impact:** needs debug level or a bad URL.
- **Recommendation:** print origins only.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-36 The shipped feature set and image are never built in CI — Low

- **Evidence:** the image builds `-p tn-worker-gateway` alone (`Dockerfile:42`), but CI builds only the workspace (`etc/ci-lanes.sh:89-98`, `118-121`), where feature unification can hide a missing feature.
- **Reproduction:** no CI workflow runs `-p tn-worker-gateway` or an image build.
- **Impact:** the shipped dependency graph is untested; this PR adds a compile-time guard for the TLS backend.
- **Recommendation:** a CI step that builds the gateway alone with `--locked`, and optionally the image.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-37 Image build not `--locked`, unpinned base images, `:latest` tag — Low

- **Evidence:** `Dockerfile:10`, `42`, `46`; `deploy/k8s/deployment.yaml:32`.
- **Reproduction:** no `--locked`, no `@sha256` digests, and a `:latest` image tag.
- **Impact:** replicas can run different builds.
- **Recommendation:** `--locked`, pinned digests and a versioned tag.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-38 Large transitive dependency graph — Low

- **Evidence:** `Cargo.toml:50-52` pulls in three workspace crates.
- **Reproduction:** the lockfile closure is about 805 packages, against about 275 without those crates; an advisory scan finds 5 advisories in the closure.
- **Impact:** about 3× the build and audit surface; whether the advisories are reachable at runtime was not checked.
- **Recommendation:** depend on narrower crates for the few types the gateway uses.
- **Effort:** M.
- **Status:** #1612 (P25).

### WG-39 TLS pitfalls: an empty root store builds silently — Low

- **Evidence:** reqwest 0.12.28 builds a client with an empty native root store (`async_impl/client.rs:697-731`, `763-771`).
- **Reproduction:** no rustls symbols in the base binary; reqwest selects one provider, so two providers in the lockfile do not panic.
- **Impact:** with this PR's TLS support, a host without CA certificates fails every `https` call at runtime, not at startup.
- **Recommendation:** check at startup that the root store is not empty when an `https` URL is configured.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-40 CLI validation gaps — Low

- **Evidence:** `src/cli.rs:50-85`, `394-411`; `src/readiness.rs:123`; `src/config.rs:29-34`.
- **Reproduction:** a 0 s poll interval panics and exits 1; a 0 s request timeout gives `504` on every call; a duplicate `worker_id` is accepted; a `[::ffff:127.0.0.1]` self-URL starts and then answers `508`.
- **Impact:** operator misconfiguration only.
- **Recommendation:** reject zero durations and duplicate ids; normalise mapped IPv6 in the self check.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-41 The screen's rejected types match the pool by convention only — Low

- **Evidence:** `src/proxy.rs:250-258` shares its predicate with the batch builder (`crates/types/src/lib.rs:78-86`), not with the pool (`crates/tn-reth/src/txn_pool.rs:318-334`).
- **Reproduction:** no test compares the two sets.
- **Impact:** the sets match today, and drift fails closed.
- **Recommendation:** a test that pins the two sets together.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-42 Not every error is a JSON-RPC envelope; every HTTP method is forwarded — Low

- **Evidence:** axum's `405` on the probe routes and hyper's `400`/`431` on parse errors are not enveloped (`src/server.rs:125-131`); any HTTP method reaches `proxy()` and is forwarded to the `rpc_url` path (`src/proxy.rs:66-72`, `138-162`).
- **Reproduction:** a non-GET request to a probe route got axum's bare `405`, and malformed requests got hyper's bare `400` or `431`; requests with every HTTP method reached the mock at the `rpc_url` path.
- **Impact:** the README's "always a JSON-RPC error" promise does not hold, and the worker answers forwarded non-POST requests with text/plain `405` or `415`; the request's own path and query are dropped, so nothing reaches other worker paths.
- **Recommendation:** answer non-POST requests locally with an envelope, and correct the README.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-43 Errors use non-200 statuses — Low

- **Evidence:** `src/error.rs:82-94`, `164-189`.
- **Reproduction:** curl shows `503`, `502`, `508`, `413`, `429`, `400`, `504` and `408`, each with a JSON-RPC body.
- **Impact:** some clients treat these as transport failures and never read the body.
- **Recommendation:** document it, or offer HTTP `200` for JSON-RPC errors.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-44 Batch errors come back as one object with `id: null` — Low

- **Evidence:** `src/error.rs:164-189`, `260-263`.
- **Reproduction:** a batch with no ready upstream, with the hop header, or with a dead upstream got one error object with `id: null`.
- **Impact:** clients expecting an array mis-parse the answer.
- **Recommendation:** return an array with one error per element, or document the single object.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-45 XFF appended to the client's value; XFP hard-coded — Low

- **Evidence:** `src/proxy.rs:153-158`, `185-195`.
- **Reproduction:** the mock saw `6.6.6.6, 7.7.7.7, 127.0.0.1` and `X-Forwarded-Proto: http`; nothing in the node, reth or jsonrpsee reads `X-Forwarded-For`.
- **Impact:** latent today; matters to any upstream that trusts the header (see WG-24).
- **Recommendation:** replace a client-supplied value unless the peer is trusted (with WG-18).
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-46 `upstream_ready{worker_id}` collides; the in-flight gauge misses streaming — Low

- **Evidence:** `src/telemetry.rs:87-89`; `src/proxy.rs:75`, `177`.
- **Reproduction:** two upstreams sharing an id: `/ready` `200` while `upstream_ready{worker_id="0"}` read 0; during a 10 s trickled response the in-flight gauge stayed 0 and the latency histogram recorded 1.2 ms.
- **Impact:** dashboards and the HPA cannot see streaming load.
- **Recommendation:** label by upstream index or origin; hold the in-flight guard until the body finishes.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-47 Unthrottled warns on some rejects; none on others — Low

- **Evidence:** `src/proxy.rs:86-90`, `99`, `104`, `114`, `126`.
- **Reproduction:** 150 rejections of each logged kind gave 150 warn lines; 429s, 408s and aborted bodies logged nothing; 1000 hop-header requests from one IP gave 232 warn lines and 721 silent `429`s.
- **Impact:** log volume capped by the rate limits at about 0.5 MB/s.
- **Recommendation:** rate-limit the warns and count every reject kind.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-48 A never-ready gateway logs no reason at info — Low

- **Evidence:** `src/readiness.rs:84`, `151-171`.
- **Reproduction:** with a closed readiness port, the info log never gave a reason; debug shows it.
- **Impact:** a never-ready gateway cannot be diagnosed at the default level.
- **Recommendation:** log the first failure's cause at info.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-49 Docs contradict the code in several places — Low

- **Evidence:** `README.md:25-27` (worker 0 vs first ready), `161-162` (per-IP caps share, contradicted by WG-08), `200-203` (edge-facing vs a ClusterIP Service), `243-245` (always an envelope, see WG-42), `286`, `289` (gauge and histogram scope), `deploy/README.md:115-117`, `src/proxy.rs:6-8`.
- **Reproduction:** 9 of 13 checked items contradict the code, 3 are half true and 1 is stale.
- **Impact:** misleads sizing; the underlying issues have their own ids.
- **Recommendation:** correct each item.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-50 ANSI escapes in non-TTY logs; no structured format — Low

- **Evidence:** `src/main.rs:47-52`.
- **Reproduction:** with stdout redirected to a file, 9 of 17 lines contained `\x1b[`; `NO_COLOR=1` gave none.
- **Impact:** log hygiene; `NO_COLOR=1` works around it.
- **Recommendation:** disable colour off a TTY and add a JSON log format.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-51 The Grafana datasource uid is hard-coded — Low

- **Evidence:** `grafana-worker-gateway.json:26-29`, `390-392`.
- **Reproduction:** 18 hard-coded uids, an empty `templating.list` and no `__inputs`.
- **Impact:** the dashboard needs editing to import; operability only.
- **Recommendation:** a datasource template variable.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-52 An EIP-7702 rejection is reported as "EIP-4844 blob" — Low

- **Evidence:** `src/proxy.rs:250-259`; `src/error.rs:125-127`.
- **Reproduction:** test `decodable_but_disallowed_tx_type_is_rejected_with_its_id`; a signed type-4 transaction got "EIP-4844 blob transactions are not accepted".
- **Impact:** cosmetic.
- **Recommendation:** name the rejected type in the message.
- **Effort:** S.
- **Status:** #1612 (P25).

### WG-53 No WebSocket, HTTP/2, TLS termination or auth, by design — Info

- **Evidence:** `README.md` Scope; `src/server.rs:207` (HTTP/1 only).
- **Impact:** subscriptions and TLS termination belong to a front; readiness can be stale by the poll interval plus up to 2 s per upstream polled before it.
- **Status:** no action.

## What is already good

- The hand-rolled accept loop gives a header-read deadline and a whole-request deadline (`src/server.rs:207-208`, `130`; 30 s + 10 s, `src/app.rs:139`).
- Slow readers are bounded by `TCP_USER_TIMEOUT` and the connection lifetime cap (`src/server.rs:245-249`, `263-266`).
- Responses are streamed, never buffered whole (`src/proxy.rs:177`).
- Request members other than `id` are skipped in place, without building a `Value` tree (`src/error.rs:263-279`); the `id` itself is the exception (WG-02).
- The transaction screen cannot wrongly reject: it never recovers the signer, and its decode is looser than the node's (`src/proxy.rs:252` against `rpc_fee_cap.rs:57`).
- Readiness fails closed (`src/readiness.rs:84`, `195`, `223`).
- A hop marker and a startup check catch a gateway pointed at itself (`src/proxy.rs:85-92`, `src/cli.rs:394-411`).
- Rate limiting keys on network prefix, with a bounded map, garbage collection, and exempt health probes.
- Long-running tasks are spawned as critical tasks under `TaskManager` (`src/app.rs:121`, `149`, `155`).
- The container runs non-root, with a read-only root filesystem and every capability dropped (`Dockerfile:59-62`, `deploy/k8s/deployment.yaml:73-83`).
- The gateway never retries a forward at the application level (redirect-following aside, WG-10).
- Client-facing errors never carry an upstream URL, and no response header or metric leaks the worker's address.

## Test-coverage gaps

| Behaviour | Covered at `02fe2a1e`? | Proposed test |
| --- | --- | --- |
| A request refused per IP does not spend a global token | no | `per_ip_rejection_does_not_spend_global_token` (WG-08) |
| An oversized or structured `id` echoes `null` without a `Value` tree | no | `oversized_id_echoes_null` (WG-02) |
| Poller end to end: fetch, timeout, non-200, transitions, gauge | no (only `parse_ready` units) | `poller_marks_ready_then_not_ready_on_timeout` against a mock `/health/workers` |
| Node and gateway agree on the readiness shape | partly (the same JSON literal in `health.rs` and `readiness.rs` tests) | serialize the node's readiness type and parse it with `parse_ready` |
| Upstream status and content type pass through (429, 500, 415) | partly (`proxies_to_ready_upstream`, 200 only) | `upstream_error_status_and_content_type_pass_through` (WG-22) |
| An upstream `408` is not rewritten or double-counted | no | `upstream_408_is_passed_through` (WG-30) |
| Shape of a gateway error answering a batch | no | `batch_rejection_shape` (WG-44) |
| Upstream body fails mid-stream | no | `upstream_body_error_truncates_response` (WG-33) |
| Graceful shutdown drains in-flight requests | no | `shutdown_drains_in_flight_request` (WG-20) |
| Idle keep-alive closed at the header-read timeout | no | `idle_keepalive_closed_after_header_timeout` (WG-06) |
| Redirects are not followed | no | added in this PR: `query_upstream_redirects_are_not_followed` (WG-10) |
| `eth_sendRawTransactionSync` is screened | no | `send_raw_sync_is_screened` (WG-32) |
| Non-POST methods get an envelope | no | `non_post_methods_get_an_envelope` (WG-42) |
| A client-supplied XFF is not trusted | no | `client_supplied_xff_is_not_trusted` (WG-45) |
| Counters add up and the in-flight gauge returns to 0 | no (labels only) | a recorder test: forwarded + rejected = requests, gauge 0 afterwards |
| Probes during connection-slot exhaustion | no | `ready_probe_under_connection_exhaustion` (WG-07) |
| The reference manifest uses the node's readiness URL | no | a manifest check that the readiness path is `/health/workers` (WG-19) |
| Reads survive a worker outage | no | added in this PR: `worker_down_serves_queries_and_refuses_submissions` (WG-14) |
| A failing query upstream never falls back to the worker | n/a | added in this PR: `query_upstream_down_never_falls_back_to_the_worker`, `slow_query_upstream_times_out_without_falling_back` |
| Transport error cause logged without the URL | no | added in this PR: `forwarding_failure_log_fields_hide_the_url` (WG-25) |
| Redirect startup errors name the URL by origin only | n/a | added in this PR: `redirect_errors_name_the_url_by_origin_only` (post-rev-3) |

## Plan to address Medium and above

Rows are ordered by severity, then by the size of the change; P26 and P27 come from the [post-change review](#post-change-review).
WG-02 and WG-08 (P3, P4) are small and are recommended before any public deployment.

| # | WG-IDs | Change | Acceptance criteria (testable) | This PR (commit) or follow-up (issue) |
| --- | --- | --- | --- | --- |
| P1 | WG-01, WG-11, WG-14 | `--redirect-queries <URL>`: the two submission methods go to the first ready worker, every other call and every mixed batch to the URL, with no readiness gate and no fallback to the worker | `redirect_sends_each_method_to_its_upstream`, `redirect_sends_only_all_submission_batches_to_the_worker`, `worker_down_serves_queries_and_refuses_submissions` and `query_upstream_down_never_falls_back_to_the_worker` pass; with the option set, a `tn_*` call never reaches the worker | this PR: `feat(worker-gateway): serve non-submission calls from --redirect-queries` |
| P2 | WG-01, WG-09 | Per-route in-flight caps that fail fast with a `503` envelope, a global rate budget reserved for submissions, and a cap on concurrent upstream requests to the worker below its `--rpc.max-connections` | in a test, submissions succeed while the query route is saturated by a slow mock; the worker mock never sees more concurrent requests than the configured cap | follow-up: worker-gateway: isolate submission capacity from query traffic — #1594 |
| P3 | WG-02 | Recover the `id` as `null`, a number or a short string only, without a `Value` tree | a request with a 1 MiB array `id` and the hop header gets `508` with `id: null`; 500 such concurrent requests keep peak RSS under the reference limit | follow-up: worker-gateway: bound the recovered request id on reject paths — #1595; recommended before any public deployment |
| P4 | WG-08 | Spend a global token only for requests the per-IP check admits | with global 1/s burst 3 and per-IP 1/s burst 1, after one IP sends 3 requests (1 admitted, 2 refused) a second IP's first request is admitted | follow-up: worker-gateway: charge the global rate budget only after the per-IP check — #1596; recommended before any public deployment |
| P5 | WG-10 | The proxy client follows no HTTP redirects | `query_upstream_redirects_are_not_followed` (307 and 308) passes, and fails with the redirect policy removed | this PR: `feat(worker-gateway): serve non-submission calls from --redirect-queries` |
| P6 | WG-05 | Default `--max-request-bytes` 1 MiB, the sizing rule in the README, and a reference memory limit of 1Gi | `edge_protection_defaults` pins 1,048,576; the manifest's memory limit is at least 1.5 × `--max-connections` × `--max-request-bytes` + 64 MiB at the defaults (500 held 1 MiB bodies peaked at about 712 MiB) | this PR: `fix(worker-gateway): lower the default request body cap to 1 MiB` |
| P7 | WG-05 | A global budget on in-flight request bytes | peak RSS stays under the budget plus a constant for N parallel maximum-size bodies | follow-up: worker-gateway: global in-flight request-byte budget — #1597 |
| P8 | WG-07 | Serve `/health` and `/ready` outside the client connection cap | with `--max-connections 4` and four held connections, `curl -m 1` to `/health` and `/ready` returns `200` | follow-up: worker-gateway: serve health probes outside the client connection cap — #1598 |
| P9 | WG-06 | A per-IP concurrent connection cap | one IP holding the cap's worth of idle sockets does not stop a second IP from getting `200` | follow-up: worker-gateway: per-IP concurrent connection cap — #1599 |
| P10 | WG-03 | Charge one token per batch element, cap batch length, screen every element of an all-submission batch | an over-length batch gets `413` / `-32003`; a batch of K costs K tokens; a bad element inside an all-submission batch is refused | follow-up: worker-gateway: per-element batch charging, max batch length, per-element submission screening — #1600 |
| P11 | WG-04 | TLS (optionally mTLS) for worker upstreams, or a documented tunnel | an `https` worker URL is accepted, and forwarding and readiness work against a test server with a custom CA | follow-up: worker-gateway: TLS/mTLS or tunnel for worker upstreams — #1601 |
| P12 | WG-19, WG-20 | The reference manifest polls `/health/workers` on the node's healthcheck port and gains a 5 s `preStop` sleep | the manifest's readiness URL path is `/health/workers`; the unedited port placeholder fails at startup; `preStop` + drain + join margin (36 s) fits the 40 s grace period | this PR: `fix(worker-gateway): poll the node healthcheck in the reference manifest` |
| P13 | WG-25 | Log the transport error cause and the upstream origin, never the URL | `upstream_origin_drops_userinfo_path_and_query`, `error_chain_joins_every_source` and `forwarding_failure_log_fields_hide_the_url` pass | this PR: `fix(worker-gateway): log the upstream transport error cause` |
| P14 | WG-13, WG-16, WG-17, WG-21, WG-23, WG-24 | Operator guidance: firewall the health listener, size budgets for N gateways and the HPA maximum, use health-checked DNS, state the split-routing caveats | the README states each rule with the numbers to plug in | this PR: `docs(worker-gateway): document query redirect topology and record post-change review` |
| P15 | WG-11 | Without `--redirect-queries`, refuse `tn_*`, `debug_*`, `trace_*` and `admin_*` by default | by default those calls get a JSON-RPC method-not-allowed error and never reach the worker mock | follow-up: worker-gateway: method allowlist when no redirect is configured — #1602 |
| P16 | WG-22 | Wrap non-JSON upstream errors in an envelope and count them separately | a mock `429` text/plain reaches the client as a JSON-RPC error with the request's `id`, counted under its own outcome | follow-up: worker-gateway: wrap non-JSON upstream errors and count them — #1603 |
| P17 | WG-27 | Optional CORS | the preflight is answered by the gateway, and `Access-Control-Allow-Origin` comes back on POST | follow-up: worker-gateway: optional CORS — #1604 |
| P18 | WG-12, WG-15 | Readiness hysteresis, and passive health from forwarding errors | one slow poll does not flip `/ready`; consecutive connection failures on the RPC path mark the upstream not ready and the next ready one is used; the cause is logged at info | follow-up: worker-gateway: readiness hysteresis and passive upstream health — #1605 |
| P19 | WG-20 | Drain-aware shutdown | after SIGTERM, `/ready` returns `503` for `--shutdown-delay` while the listener still accepts; tested | follow-up: worker-gateway: drain-aware shutdown — #1606 |
| P20 | WG-18 | Trusted proxy CIDRs and PROXY protocol for client identity | the per-IP key uses the forwarded client address only when the peer is in a configured CIDR | follow-up: worker-gateway: trusted proxy / PROXY protocol for client identity — #1607 |
| P21 | WG-23 | Consistent pending-state reads under the redirect | `eth_getTransactionCount(.., "pending")` through the gateway counts a submission made through it immediately before | follow-up: worker-gateway: pending-state reads under redirect — #1608 |
| P22 | WG-24 | An authentication header for the query upstream and a chain-id consistency check | the header reaches the query mock and never the worker mock; startup warns when the redirect's `eth_chainId` differs from the worker's | follow-up: worker-gateway: public RPC auth header + chain-id consistency warning — #1609 |
| P23 | WG-26 | An optional sampled access log | with the option on, a sampled request logs one structured line with client prefix, method class, route, status and latency | follow-up: worker-gateway: sampled access log — #1610 |
| P24 | (design) | Split mixed batches between the two upstreams and merge the answers in order | a mixed batch's submissions reach the worker, its reads the query mock, and the client gets one array in request order | follow-up: worker-gateway: split-and-merge mixed batches — #1611; only if `tn_worker_gateway_mixed_batches_total` shows real use |
| P25 | WG-28 to WG-52 | The Low findings, as one checklist | each item's recommendation above | follow-up: worker-gateway: low-severity hardening — #1612 |
| P26 | WG-14, post-rev-1 | A probe that keeps a redirecting gateway in rotation while its worker is down | with the worker down and the query upstream up, the new probe returns `200` while `/ready` returns `503`; the reference `readinessProbe` uses it | follow-up: worker-gateway: probe that keeps redirecting gateways in rotation while the worker is down — #1613 |
| P27 | post-dos-2 | Cache upstream addresses and cap concurrent DNS lookups | with a resolver that delays the query host by 10 s and 100 reads per second, `/ready` and submissions stay `200` | follow-up: worker-gateway: bound and cache DNS lookups for upstream hosts — #1614 |

## Operator guidance

**Always set `--redirect-queries` on a validator's gateways.**
Without it every read, including `tn_*`, reaches the worker (WG-11), and slow reads through one gateway can fill the worker's permit pool for every gateway (WG-01).
Point it at an `https` public RPC that serves the same chain.

**DNS.**

- Publish the gateways behind health-checked DNS or a load balancer that probes each gateway's `/ready`; plain round-robin A records keep sending clients to a dead gateway (WG-21).
- `/ready` means "this gateway can take submissions"; a gateway whose worker is down returns `503` even though it still serves reads.
- Lock the domain at the registrar, enable DNSSEC where the provider supports it, and use a DNS provider with its own DDoS protection; a hijacked name serves forged state to every client.
- Keep TTLs short enough to drain a gateway in minutes, but not so short that resolver load becomes the attack surface.
- The gateway resolves upstream names through the system resolver on the runtime's blocking threads.
  A lookup that fails fast gives reads a `502` and leaves submissions alone, but a resolver that hangs on the public RPC's name ties up those threads and can make readiness polls time out on every gateway (post-dos-2, #1614 (P27)).
  Use a resolver close to the gateways, and an IP literal for the readiness URL where you can.
- `/ready` means "this gateway can take submissions", and every gateway shares the worker, so a front that acts on `/ready` drops all of them, reads included, while the worker is down; probe `/health` instead if reads must survive a worker outage (WG-14, #1613 (P26)).

**DDoS front.**

- Absorb L3/L4 volume in front of the gateways (T1); the gateway has no defence against packet floods.
- A front that terminates TCP makes every client share the front's rate-limit buckets until P20 lands (WG-18).
  Either use an L4 front that preserves the client address, or accept front-level limits and set the gateway's per-IP limit for the front's addresses.
- If a front is used, firewall the gateways so only the front's address ranges reach them; an origin reachable directly bypasses the front.

**Firewalling.**

- The worker RPC port accepts connections from the gateway hosts only.
- The node's `--healthcheck` port accepts connections from the gateway hosts only: it is unauthenticated and serves one connection at a time, so four idle connections from anyone make every gateway unready (WG-13).
- The gateway's metrics port is unauthenticated; keep it inside the monitoring network.
- When the gateway-to-worker hop leaves a network you control, run it through a tunnel until worker TLS lands (WG-04, P11).

**Sizing for N gateways.**

- Budgets are per process (WG-17): the worker receives up to N × `--rate-limit-global`.
  Size `--rate-limit-global` as the worker's budget divided by the largest N that can run, which is the HPA's `maxReplicas` if the HPA is installed (WG-16).
- Connections: until P2 lands, N × `--max-connections` can exceed the worker's `--rpc.max-connections` (500 by default).
  With `--redirect-queries` only submissions reach the worker, and they are fast except `eth_sendRawTransactionSync`, which can hold a worker permit for up to 30 s; raise the worker's limit above N × `--max-connections` or accept that a flood of Sync calls through one gateway can make the worker refuse submissions from the others.
- Memory: each held body costs about 1.4 × its size, because the connection's read buffer stays allocated while the request is in flight (500 held 1 MiB bodies peaked at about 712 MiB).
  Budget 1.5 × `--max-connections` × `--max-request-bytes` + 64 MiB, about 814 MiB at the defaults, so the reference manifest sets a 1Gi limit.
  The limit does not cover WG-02, whose amplification is about 34× the request size; that needs P3.
- Query upstream: until P2 lands, a public RPC that accepts requests and never answers holds a gateway's connection slots for up to `--upstream-request-timeout` (30 s) each, and about 17 reads per second fill the default 500 slots, starving submissions on every gateway that uses it (post-dos-1).
  Alert on `tn_worker_gateway_routed_requests_total{route="query",result="timeout"}` and agree on availability with the public RPC's operator.

**Split routing (with `--redirect-queries`).**

- Pending-state reads are answered by the public RPC, which has not seen this validator's pool: `eth_getTransactionCount(.., "pending")`, `eth_getTransactionByHash` and receipts right after a submission can lag (WG-23).
  Clients that send several transactions in a row should track their own nonces.
- A submission inside a mixed batch goes to the public RPC with the rest of the batch and enters the network there.
- Every redirected read reaches the public RPC from the gateway's address, so its per-IP limits apply to all of the gateway's clients together (WG-24); agree limits with its operator.
- Each redirected call carries the client's address in `X-Forwarded-For`, so the public RPC's operator sees your clients' addresses.

## Appendix: unconfirmed findings

No finding was refuted as a whole, and none was left unverifiable.
These parts of the original claims were refuted or could not be checked, and are not part of the findings above:

- WG-08: going over the per-IP limit is not enough to block other clients; the source has to exceed the global rate (3000/s by default).
- WG-06: an idle keep-alive connection gives up its slot after the 10 s header-read timeout, not after the 10-minute lifetime cap.
- WG-11: a TN worker never serves `admin_*` or `txpool_*`; `debug_*` and `trace_*` are served only if the operator enables them.
- WG-25: reqwest's error `Display` leaks the URL's path and query, but not its userinfo, which moves into a Basic auth header.
- WG-47: `429`s, `408`s and aborted bodies are not logged at all, rather than logged per request.
- WG-38: whether the 5 advisories in the dependency closure are reachable at runtime was not checked.
- WG-39: the two rustls providers in the lockfile are not both linked into the binary, and reqwest would not panic.
- WG-42: the request's path and query are not forwarded, so non-POST requests cannot reach other worker paths.
- WG-01: the worker's permit is held per in-flight request, so a client reading the response slowly does not hold it; the attacker needs calls that are slow on the worker.
  WG-01 was confirmed from code, arithmetic and a mock, not against a running worker.
- WG-48: a failure to bind the metrics port exits the process at startup, so that half of the claim (a silent metrics failure) was refuted.

## Post-change review

After the fixes and the feature landed, the branch diff (`02fe2a1e...HEAD -- bin/worker-gateway`) had two more passes: a correctness, docs and tests review, and a denial-of-service review with measurements on a debug build.
Their findings are numbered post-rev-N and post-dos-N; line numbers in them are at the branch head, not at `02fe2a1e`.
Every Medium finding was fixed in this PR or given an issue, and the Low findings joined the Low checklist.

What came out clean:

- No Critical or High finding, and no way around the shield.
  The classifier was checked against the worker's own JSON-RPC parser (jsonrpsee 0.26): case variants, escaped names, a byte-order mark, trailing bytes, nested arrays, non-object elements and empty batches all go to the query route.
  A duplicated `method` key is routed by its last occurrence, but the worker's parser refuses duplicated fields, so such a call can only be rejected there as an invalid request; nothing runs.
- The query route follows no redirects and relays no `Location` header; the hop markers, the `508` rules, the readiness and fallback matrix and the new metrics behave as documented.
- The rustls compile guard holds when the gateway is built alone, and `Cargo.lock` is unchanged.
- Metric labels are bounded, the upstream connection pool and the TLS session cache are bounded, and the classifier's cost is linear in the body with no recursion path.

| ID | Title | Severity | Resolution |
| --- | --- | --- | --- |
| post-rev-1 | With the reference probes, reads still stop while the worker is down, though the report called WG-14 closed | Medium | report corrected (WG-14 and the verdict); the README and the deploy README's "Readiness and reads" section document probing `/health`; issue #1613 (P26) |
| post-rev-2 | The reference manifest never set `--redirect-queries` | Medium | fixed in `feat(worker-gateway): serve non-submission calls from --redirect-queries`: the manifest sets `WORKER_GATEWAY_REDIRECT_QUERIES` to a placeholder that stops the gateway at startup until it is replaced, and the deploy README lists it |
| post-dos-1 | A stalled query upstream fills every connection slot and starves submissions on every gateway | Medium | issue #1594 (P2: a cap on in-flight query forwards and a shorter query timeout); operator guidance |
| post-dos-2 | A resolver that hangs on the redirect host uses up the blocking threads, so readiness flaps and submissions get `503` | Medium | operator guidance corrected; issue #1614 (P27) |
| post-dos-3 | The first sizing rule undercounted each held body by about 42% | Medium | fixed in `fix(worker-gateway): lower the default request body cap to 1 MiB`: the reference limit is 1Gi and the rule uses the measured cost; bounding the read buffer is in issue #1597 (P7) |
| post-rev-3 | A `--redirect-queries` startup error printed the full URL, credentials included | Low | fixed in `feat(worker-gateway): serve non-submission calls from --redirect-queries`: startup errors name upstream URLs by origin only (test `redirect_errors_name_the_url_by_origin_only`); clap's echo of an unparseable value is in issue #1612 |
| post-rev-7 | README and doc comments still said every call goes to the worker | Low | fixed: doc comments in `feat(worker-gateway): serve non-submission calls from --redirect-queries`, README in `docs(worker-gateway): document query redirect topology and record post-change review` |
| post-rev-8 | The README's Query redirect section omitted the split-routing caveats | Low | fixed in `docs(worker-gateway): document query redirect topology and record post-change review` |
| post-rev-4 | The redaction test does not exercise the log line itself | Low | issue #1612 |
| post-rev-5 | No test checks the classifier against the worker's parser | Low | issue #1612 |
| post-rev-6 | The two new metrics are untested | Low | issue #1612 |
| post-rev-9 | The query upstream chooses the `Content-Type` served on the validator's origin | Low | issue #1612 |
| post-dos-4 | The classifier repeats the screen's parse and allocates per key and per element | Low | issue #1612 |
| post-dos-5 | Every failed query forward logs an unthrottled warning | Low | issue #1612 |
| post-rev-10 | The redirect's worker-origin check compares host literals only | Info | issue #1612 |
| post-rev-11 | `tn_worker_gateway_mixed_batches_total` misses a submission batched with a non-object element | Info | issue #1612 |

### post-rev-1 Reads still stop with the worker behind the reference probes — Medium

- **Evidence:** `/ready` reports whether a worker is ready (`src/server.rs`, `fn readiness`), and the reference Deployment's `readinessProbe` uses it; the Service does not publish not-ready addresses.
- **Reproduction:** `server::tests::worker_down_serves_queries_and_refuses_submissions` shows the gateway serving `eth_chainId` from the query upstream while `/ready` returns `503`; every replica polls the same worker, so with the manifest's probe (period 5 s, failure threshold 3) all of them leave the Service about 15 s into a worker outage.
- **Impact:** in the reference deployment, and behind any front that acts on `/ready`, a worker restart still fails every read on every gateway.

### post-rev-2 The reference manifest never set `--redirect-queries` — Medium

- **Evidence:** at the feature commit, nothing under `deploy/` mentioned the redirect, while the operator guidance says to always set it on a validator's gateways.
- **Impact:** an operator following the deploy README got no read shield.

### post-dos-1 A stalled query upstream starves submissions on every gateway — Medium

- **Evidence:** the query route shares the connection semaphore, the client and the 30 s request timeout with submissions, and every gateway uses the same query URL.
- **Reproduction:** with a query upstream that read each request and never answered, 500 concurrent `eth_getBalance` calls held every slot; a submission then took 27.04 s (it took 0.00 s before the load), and the queries ended as `504` after 30-31 s.
  At 30 s per stalled request, about 17 reads per second fill 500 slots.
- **Impact:** one slowdown at the public RPC, attacker-driven or not, stops submissions on every gateway at once; WG-09 describes the mechanism for one gateway, and the shared query URL extends it to the fleet.

### post-dos-2 A hanging resolver for the redirect host makes readiness flap — Medium

- **Evidence:** reqwest resolves names through hyper-util's `GaiResolver`, which runs each lookup with `spawn_blocking`; tokio caps the blocking pool at 512 threads, a started lookup keeps its thread past the 2 s connect timeout, and the readiness client shares the pool and needs a lookup on every poll when its URL is a hostname.
- **Reproduction:** with a resolver shim that answered names containing "slow" after 10 s, `--redirect-queries http://rpc.slow.test:23003/` and 100 reads per second, the thread count rose from 18 to 529 in 6 s; `/ready` and submissions returned `503` twice in 25 s, and the gateway recovered within 5 s after the load stopped.
- **Impact:** a slow resolver for one third-party name takes submissions down on every gateway that uses it; the first version of this report said submissions were unaffected.

### post-dos-3 The first sizing rule undercounted held bodies — Medium

- **Evidence:** hyper's per-connection read buffer can grow to 408 KiB (`DEFAULT_MAX_BUFFER_SIZE`), and the gateway does not lower it.
- **Reproduction:** 500 complete 1 MiB queries held by a stalled upstream peaked at 728,904 kB (about 712 MiB; the idle process uses about 11 MB), about 1.42 MiB per connection, against the 500 MiB the first rule assumed.
- **Impact:** at the defaults the first proposed limit of 768Mi would have left about 56 MiB of headroom instead of 268 MiB.
