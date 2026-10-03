# 18 — Load qualification

Status: not started

Depends on: [17](17-compatibility-ci.md).

## One-turn outcome

Measure whether the one-table design meets its declared latency target at a
fixed four-node workload while retaining correctness and bounded resources.

## Frozen baseline recipe

The values below are a reproducible qualification workload, not a claimed
production capacity. Freeze its manifest before running implementation tests;
record any proposed change here with a reason and keep prior results comparable.

| Parameter | Baseline |
| --- | --- |
| Sources | 4 stock HAProxy processes |
| Transport | Mutual TLS |
| Initial live keys | 100,000 per source |
| Overlap | 50,000 shared by all sources; 50,000 unique per source |
| Key type | IPv6, including a deterministic mix of IPv4-mapped keys |
| Rate period | 60 seconds |
| Source expiry | 10 minutes for this retention/load test |
| Source table capacity | 300,000 entries per source |
| Offered traffic | 10,000 HTTP requests/second/source; report achieved rate |
| Key selection | Seeded distribution: 80% to a 10% hot set, 20% to remaining keys |
| Warm-up | 60 seconds after initial population and synchronization |
| Steady measurement | 120 seconds |
| Churn measurement | 120 seconds, replacing 1,000 active keys/second/source |
| Latency objective | End-to-end p99 at most 250 ms while authoritative |

During churn, the active set stays at 100,000 keys/source. Retired keys remain
until their normal expiry, so resident state can grow to about 220,000/source
within this short run. Report both counts. Aggregator caps must accommodate the
declared workload, including snapshot replacement overhead; phase 14 separately
tests exhaustion. The ordinary 10/60-second expiry/decay tests remain required
and are not replaced by this longer-retention benchmark.

## Work

- Implement the deterministic workload/report driver using the existing lab.
  Freeze the seed, source-key mapping, request schedule, and manifest before the
  first scored run. Fail on generator shortfall instead of lowering load silently.
- Measure accepted HTTP rate, wire update rate, output rate, CPU, peak/steady
  RSS, queue depth, publication lag, recovery time, and retained entries.
- Measure propagation with isolated sampled keys whose expected output change
  is known, from the triggering request through observation on each HAProxy.
  Record timer resolution; Go processing time alone is not end-to-end latency.
- Include zeros/decay in observations, verify sampled totals with an independent
  oracle, and count timeouts or authority loss rather than dropping slow samples.
- Record hardware, OS, CPU allocation, toolchains, hashes, TLS settings, and
  whether the generator shares CPU with the system being measured.
- After the baseline, perform a short explicitly labeled rate sweep to find the
  measured saturation boundary. Do not generalize results to arbitrary hardware.

## Acceptance

- [ ] Both steady and churn runs achieve the frozen offered workload, or report
      qualification failure with generator/system bottlenecks distinguished.
- [ ] Correctness samples and resource bounds pass at 100,000 active keys/source.
- [ ] Healthy end-to-end p99 is at most 250 ms; authority loss/timeouts are reported
      and cannot be excluded to manufacture a passing percentile.
- [ ] Peak memory includes source copies, output state, queues, and resync overhead.
- [ ] An aggregator restart at populated cardinality reports recovery duration
      and obeys fallback/readiness rules without replay inflation.
- [ ] The report distinguishes HAProxy input sampling delay, Go work, publication,
      and observation where measurable; uncertainty is stated.
- [ ] The supported workload and remaining headroom are evidence, not estimates
      borrowed from unrelated Go or Bun benchmarks.

## Stop and handoff

Do not spend the turn on unbounded performance tuning. If the baseline fails,
capture a profile and identify the dominant limit with a bounded next step.
Changing the workload or latency promise requires an explicit decision, not a
quiet benchmark adjustment. This phase does not justify production enforcement.

## Execution record

- Commands, hardware, seed, and versions: not run.
- Acceptance evidence / report paths: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 17.
