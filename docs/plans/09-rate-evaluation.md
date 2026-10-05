# 09 — Rate evaluation

Status: complete

Depends on: [08](08-counter-aggregation.md).

## One-turn outcome

Evaluate each source's HAProxy rate estimate at a common time and sum the
results, including different window phases and idle decay.

## Work

- Write down the estimator's units, rounding, low-rate behavior, age conversion,
  rollover, and empty-window behavior from pinned upstream references. Implement
  original Go code from that behavioral specification and fresh observations.
- Convert received wire age to a local monotonic basis and advance it as time
  passes. State the transport-delay approximation; do not promise an exact
  reconstruction of event arrival times that the wire does not contain.
- Define the sum as the sum of individually evaluated native integer estimates
  at the same time. Account explicitly for any upstream version differences.
- Inject a clock into the evaluator. Arrange sources with equal periods but
  different starting phases and histories; adding their raw counters is invalid.
- Schedule the next required reevaluation even without incoming updates. Define
  a bounded publication cadence/error budget for changing integer rates.
- Compare the independent specification and source-wise local HAProxy readings
  at controlled times. Use timestamped requests to quantify estimation error,
  not to assert equality to an exact sliding-window log.
- Carried in from phase 08: read source entries through
  `snapshot.Store.KeyView`, which returns the roster and the key's entries
  under one lock, and sum only sources the roster reports Ready, as
  `aggregate.Count` does. A rate built from a syncing, degraded, or
  disconnected source must not be presented as complete.

## Acceptance

- [x] Equal and deliberately offset windows produce the specified aggregate.
- [x] One-event, burst, steady, empty, and multi-period-idle cases match the oracle.
- [x] Integer rounding and native low-rate correction are explicitly tested.
- [x] Rates decay to zero without new peer updates and without resurrecting keys.
- [x] 10-second and 60-second periods pass the same semantic cases.
- [x] Delayed messages and clock advancement have documented error behavior.
- [x] Mismatched periods fail schema validation; no silent conversion occurs.
- [x] The evaluated result fits the chosen output field or invalidates authority.

## Stop and handoff

Do not synthesize native `http_req_rate` output fields. If upstream estimators
differ, preserve a reproducible comparison and settle an explicit supported
behavior before publication. Treat rounding and delay as part of the contract.

## Execution record

- Commands and versions (2026-10-05, Debian 13 amd64): Go `go1.27.1`
  (repo-pinned); stock HAProxy `3.4.6-56332c5` (binary SHA-256
  `81df335a…d1c4`, source `791e1815…368b`) and `3.2.25-70469d3`
  (`5f89a725…8869`, source `d59a68d0…866e`) from phase 01's
  `make haproxy` (not rebuilt).
  - `make check`: pass (0 lint issues, race tests ok, no
    vulnerabilities; final tree, log `artifacts/check-09-final.log`).
  - `make lab-test` (both versions, race detector on): pass, including
    the new `TestLiveRates` (10 s: ~46 s, 60 s: ~79 s, in parallel) and
    every earlier live test (log `artifacts/lab-test-09.log`). That run
    started before two later edits: a doc-comment rewording in
    `internal/rate/rate.go` and the new test-only
    `internal/aggregate/rate_internal_test.go`, both covered by the final
    `make check`.
  - Upstream comparison, reproducible from the pinned tarballs:
    `for v in 3.4.6 3.2.25; do mkdir -p $D/$v && tar -xzf
    artifacts/haproxy/dl/haproxy-$v.tar.gz -C $D/$v --strip-components=1
    haproxy-$v/src/freq_ctr.c haproxy-$v/include/haproxy/freq_ctr.h
    haproxy-$v/include/haproxy/freq_ctr-t.h; done; diff -r $D/3.4.6
    $D/3.2.25`, plus the `STD_T_FRQP` encode/decode in `src/peers.c` and
    `smp_fetch_http_req_rate`/the `show table` dump in
    `src/stick_table.c`. Result: the read path
    (`_freq_ctr_total_from_values` with the `pend < 0` correction,
    `read_freq_ctr_period`'s truncating `div64_32`), the rotation rule,
    and the wire age/tick conversion are identical. The only differences
    are that 3.4.6 reads the global clock through a pointer and clamps
    `freq_ctr_overshoot_period` (unused by `http_req_rate` reads) and
    adds `swrate_add_peak_local`; trace messages differ in `peers.c`. No
    decision between estimators was needed; the live comparison below
    passes unchanged on both builds.
- Behavioral specification (package documentation of `internal/rate`,
  original Go code from that description; nothing translated):
  - Units: estimated events per configured period P (not per second),
    an unsigned integer.
  - Reading at e ms after the current period's start: `Curr +
    floor(Prev*(P-e)/P)` for e ≤ P; `floor(Curr*(2P-e)/P)` for
    P < e ≤ 2P; 0 beyond 2P (multi-period idle, empty window).
    Rounding is truncation of the exact quotient.
  - Low-rate correction: when the newer non-rotated count is 0 and the
    older is 0 or 1, the reading is that count exactly (one event reads
    1 until 2P after its period started, then 0, without flapping).
  - Empty counter (`Curr = Prev = 0`) reads 0; its wire age is the
    sender's raw clock (tick 0) and is ignored.
  - Rollover: the sender's age is `now_ms - tick` mod 2^32, correct
    across the 32-bit clock wrap. Locally the age is
    `Age + floor((t - Received)/1ms)` on the monotonic clock, never
    reduced mod 2^32, so idle counters stay at 0. Upstream computes the
    rest of the period, P − age, as a signed 32-bit value, which is exact
    for ages up to P + 2^31 ms; beyond that it wraps positive and a
    long-idle counter reads as current again. A non-empty counter
    received with `Age > P + 2^31` (`rate.MaxAge(P)`) is evaluated
    exactly (0, since it is older than 2P) and flagged
    `AgeOutOfRange`/`Uncertain`; `TestAgeRange` checks both sides of the
    bound for P from 1 ms to 2^31−1 ms against a model of the wrapped
    arithmetic. (Review R1: the first version flagged ages above
    2^31−1 regardless of P, which marked valid counters uncertain.)
  - Periods are limited to 1..2^31-1 ms (`rate.MaxPeriod`; config now
    refuses longer periods, where upstream's reading is undefined).
  - Readings can exceed 32 bits (upstream truncates its quotient); the
    exact 64-bit value is kept.
  - Transport delay: the wire has no send time, so the delay δ between
    the source encoding and the local session reading an update is taken
    as 0. The evaluated reading at local t is the source's at t − δ
    (minus ≤ 1 ms truncation): with no new events it can only overstate
    the source's current reading, by at most
    `rate.DecayBound(δ+1ms) = floor((Curr+Prev)·ceil(δ/1ms)/P) + 1`.
    Events counted after encoding are unseen until their update arrives
    (bounded by propagation, phases 06/10). No exact event-time
    reconstruction is claimed.
- Acceptance evidence. Unit tests inject the clock through
  `snapshot.Options.Now` (the evaluator reads the store's view and
  evaluates at its `At`). Expectations come from `internal/rate/ratetest`
  (a simulated HAProxy counter on a wrapping 32-bit source clock and a
  `big.Rat` reference reading) and the extended trace oracle
  (`aggregatetest.Oracle.WantRate`), neither of which calls package
  `rate` or `aggregate.Rate`. The live test is the external oracle.
  - Equal and offset windows: `TestRateOffsetWindows` (two sources,
    windows 0.37P apart, different histories, unrelated clocks one of
    which wraps; at 600 common times the sum equals the sum of each
    simulated source's own reading and the oracle; adding raw
    `Curr`/`Prev` fields gives a different, invalid value at 363 of
    them); `TestRateEqualWindows` (same phase: 4 + 4 = 8, not
    floor(9)). Live `TestLiveRates`: nodes a and b start their windows
    0.37P (10 s) / 0.04P (60 s) apart with different traffic; every
    per-source reading agrees with HAProxy's own `show table` reading
    within the asserted delay bounds (and was equal at the query midpoint
    in every logged sample), and the sum matches the oracle.
  - One-event, burst, steady, empty, multi-period idle: `TestScenarios`
    (each at 10 s and 60 s, on a source clock far from and across its
    32-bit wrap; the evaluated reading equals the source's at every check
    and is 0 with nothing further scheduled once decayed);
    `TestEstimateMatchesReference` (exhaustive small periods and 200k
    random inputs against the reference).
  - Rounding and low-rate correction: `TestRoundingAndLowRate` (both
    periods: 7.9 → 7; two previous events read 1 at exactly P/2 and 0
    after; one previous event reads 1 across the whole period; one
    current event reads 1 to 2P inclusive, then 0; a current event
    disables the correction; exact 64-bit beyond 2^32); sum of
    individually truncated estimates in `TestRateEqualWindows`.
    Mutations caught: dropping the correction, moving the e = P
    boundary.
  - Decay without peer updates and without resurrection: `TestRateDecay`
    (both periods; reevaluated only at `Cadence.Due` with no input, the
    rate reaches 0 and nothing is due; the entries outlive the decay;
    after expiry a timed replay with no lifetime left does not bring the
    key back), `TestRateNextIncludesExpiry`, `TestDelayAndClock` (clock
    jumps of 3P, 2^32−1 ms, 2^32 ms, 400 days read 0, no wrap). Live
    (10 s): the readings fall to 0 on both sides with each entry's
    `Received` unchanged since the last request (no peer update), the
    decayed rate has no `Next`, then the entries expire on HAProxy and
    in the store and the key stays absent with a complete rate of 0.
  - 10 s and 60 s: every unit case above runs at both periods. Live:
    10 s runs through rotation, proportional decay, idle decay to 0,
    and expiry (~46 s); 60 s is a targeted comparison through the first
    rotation and proportional decay (~79 s, in parallel); its full
    2-period idle decay would take over two minutes and is covered by
    the clock-injected cases.
  - Delayed messages and clock advancement: `TestDelayAndClock`
    (delays 0–2 s: the reading equals the source's at the send time plus
    the local elapsed time, never below the source's current reading and
    at most `DecayBound(δ+1ms)` above it; a time before reception reads
    as at reception; sub-millisecond advances change nothing; large
    jumps decay to 0). `TestDecayBound` checks the bound over 200k random
    intervals. Live: per-source agreement within
    [ours(q1 + 50 ms), ours(q0 − 5 ms)] around each `show table` query
    (the asserted property). Exact equality at the query midpoint is
    logged, not asserted: in the final runs every per-source reading was
    equal on both versions (10 s: 124/124, 60 s: 128/128). An earlier run sampling on the send schedule's grid saw
    off-by-one readings within the bounds where a reading steps on the
    sampled millisecond; samples are now offset from that grid.
  - Mismatched periods: `snapshot` refuses a definition with another
    period (`ErrSchema`: phase 07's `TestSchemaMismatch` "period" case,
    and `TestRatePeriodMismatch`: the source degrades, its counter is
    never stored, converted, or counted, the rate is incomplete); the
    evaluator itself refuses an entry whose period differs from the
    table's with `rate.ErrPeriod` (`TestSumRatesRefusesOtherPeriod`) and
    an unsupported table period (`rate.CheckPeriod`, `TestRanges`);
    config refuses periods over 2^31−1 ms (`period over 2^31-1 ms`).
  - Output fit: `RateTotal.Uint32` narrows to the 32-bit gpt rate slot
    and `RateTotal.Authoritative` returns `ErrOverflow` beyond
    `math.MaxUint32` (or `ErrIncomplete` unless every source is Ready);
    `TestRateOutputFit` (MaxUint32 fits, MaxUint32+1 and a single
    source at 2·MaxUint32 are refused, never wrapped or saturated),
    `TestRateCompleteness`.
  - Randomized: `TestRandomTraces` now also sends random rate counters
    (empty, low, 32-bit limits, ages around 2^31) and checks
    `aggregate.Rate` against `WantRate` (sum, completeness, uncertainty,
    each counted estimate) at every read. Mutations caught: counting
    non-Ready sources, ignoring the age range, no local aging.
  - Estimation error (quantified, not asserted equal): sum vs the exact
    sliding-window count of timestamped requests, on both versions:
    10 s max |error| 11, mean 1.89 over 66 samples; 60 s max 21, mean
    3.92 over 65 (the estimate assumes uniform traffic, so a burst keeps
    reading high after it leaves the exact window). The rate
    never exceeded the requests of the last two periods.
- Decisions or deviations:
  - New package `internal/rate`: `Estimate`, `Counter` (`Elapsed`, `At`,
    `Next`, `AgeInRange`, `Empty`), `DecayBound`, `CheckPeriod`,
    `MaxPeriod`, `MaxAge(period)`, `ErrPeriod`.
  - `aggregate.Rate(store, table, key)` reads `snapshot.Store.KeyView`
    (roster and entries under one lock) and sums only Ready sources'
    estimates, as `aggregate.Count` does; `RateTotal` (`Sum`, `Period`,
    `At`, `Complete`, `Uncertain`, `Next`, per-source
    `RateContribution`), `SumAt`, `DecayBound`, `Uint32`,
    `Authoritative`, `ErrIncomplete`. `Uncertain` for rates means a
    held-over entry or an out-of-range age; a count decrease does not
    make a rate uncertain (the counter is the source's current state).
  - Clock injection is the store's clock: one clock stamps sessions and
    judges state, deadlines, and evaluation, so the view and the
    evaluation share one instant.
  - Reevaluation and cadence: `RateTotal.Next` is the earliest time a
    counted estimate falls (found by binary search over the monotone
    reading) or a counted non-zero entry expires. `Cadence{MinInterval}`
    (default 100 ms) schedules the next evaluation at
    `max(Next, At + MinInterval)` and nothing once the rate is constant.
    Error budget: between evaluations the published value overstates
    the decay-only rate by at most `DecayBound(MinInterval)` (plus an
    entry expiring within it) and never understates it through decay;
    e.g. 1000 requests per 10 s period on two sources gives at most
    2·(floor(2000·100/10000)+1) = 42. Source state changes (health
    timeouts, sessions) are not in `Next`; phase 10 reevaluates on them.
  - `snapshot.Store.Table(name)` exposes the configured table (period).
  - Config: `tables[].period` is now limited to 2^31−1 ms (was
    2^32−1 ms), because upstream's reading is undefined beyond it.
  - Not done (out of scope): no native `http_req_rate` output field, no
    publication/enforcement wiring (phase 10), no recovery
    reconciliation (phase 11). htad is unchanged.
- Remaining work / next action: none for this phase's acceptance.
  Phase 10 should publish `RateTotal.Authoritative()` values only
  (an error, including overflow, means no certified value; note
  `output.RateValue` still saturates and must not be used to certify an
  overflowing rate), reevaluate keys at `Cadence.Due` and on source
  state changes, and keep `Uncertain` visible in diagnostics.
