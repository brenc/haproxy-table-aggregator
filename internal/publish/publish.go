// Package publish connects the source snapshots to the published output.
// A Publisher evaluates the aggregate rate of every key of each input
// table (package aggregate), writes the authoritative values into that
// input's aggregate output table (package output), and drives the output
// store's readiness lease from roster readiness, using the phase 06
// freshness scheme. HAProxy then enforces locally, with no per-request
// call to the aggregator.
//
// # Values
//
// A key is reevaluated when a source's update for it is applied (Apply),
// when its published rate is due to change without input
// (aggregate.Cadence.Due: decay and expiry), and after any change of
// source state (every key then). Only a value that
// aggregate.RateTotal.Authoritative certifies is written: one evaluated
// while every configured source was Ready, that fits the 32-bit rate slot.
// The output store keeps only the latest value per key, so superseded
// values are coalesced before any session sends them, and a value that is
// unchanged is not sent again.
//
// A rate that does not fit the slot (aggregate.ErrOverflow) is withheld:
// the key is retired from the output, so HAProxy's copy keeps an older
// session generation that no later marker certifies, and the key reads as
// local protection until its rate fits again. A value is never saturated
// to fit.
//
// A missing aggregate entry is never read as a zero rate: under the
// frozen authority rule a missing key selects local protection, which
// decides exactly as a zero aggregate would, because the aggregate limit
// can only add denials to the local limit, which is always active.
// Uncertain rates (a held-over entry from an earlier session, or an
// out-of-range counter age) are published and counted in Stats.
//
// # Lease
//
// Every Renew (at most a quarter of the lease, as phase 06 requires), and
// as soon as the roster becomes ready, a pass reads the wall clock (W),
// feeds session liveness into the snapshot store, and requires the roster
// to be Ready; it then reevaluates every key that is dirty or due and
// calls output.Store.SetLease(W + Lease). So the lease certifies values
// current at W: every input applied before W has been evaluated, and an
// unchanged key would evaluate the same at W within the cadence's decay
// budget. While any configured source is not Ready (missing, syncing,
// unhealthy, degraded) or a key's view is incomplete, the publisher
// writes no value and revokes the lease once, which sessions deliver as a
// revocation marker; it renews nothing until the roster is Ready again
// and every key has been reevaluated.
//
// Destination progress is per session (package peersession): each
// session writes a marker only after the values it certifies, so a slow
// destination's markers wait behind its own data and expire there, while
// healthy destinations keep receiving theirs.
//
// # Retirement
//
// A key whose published rate is 0 and that needs no further evaluation
// (it has decayed, or its entries have expired) is retired from the
// output in batches, every RetireInterval: each batch costs every session
// one full re-send under a new generation (output.Store.Retire).
package publish

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/aggregate"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// Defaults for omitted Options.
const (
	// DefaultLease is the longest lease phase 06 allows.
	DefaultLease = output.MaxLeaseLength
	// DefaultRetireInterval is how often zero-rate keys are retired.
	DefaultRetireInterval = 10 * time.Second
	// DefaultCoalesce is the shortest time between two passes that new
	// input triggers.
	DefaultCoalesce = 5 * time.Millisecond
)

// ErrInvalid is wrapped by New's errors.
var ErrInvalid = errors.New("publish: invalid options")

// Route publishes the aggregate rate of one input table into one
// aggregate output table.
type Route struct {
	// Input is the input table's name in the snapshot store.
	Input string
	// Output is the aggregate output table's name in the output store.
	Output string
}

// Options configures New.
type Options struct {
	// Snapshot holds the sources' snapshots. Required.
	Snapshot *snapshot.Store
	// Output is the store every session teaches. Required.
	Output *output.Store
	// Routes pair each input table with its aggregate output table.
	Routes []Route
	// Status, if non-nil, returns every source's session status; each
	// pass feeds it to Snapshot.ObserveStatus first, so heartbeats count
	// for source health at the moment readiness is judged.
	Status func() []sources.SourceStatus
	// Now is the snapshot store's clock (snapshot.Options.Now), against
	// which due times are compared; nil means time.Now. The lease always
	// uses the real clock, as output.Store.SetLease does.
	Now func() time.Time
	// Lease is the lease length: zero means DefaultLease; at most
	// output.MaxLeaseLength.
	Lease time.Duration
	// Renew is how often the lease is renewed while the roster is
	// ready: zero means Lease/4, the most phase 06 allows.
	Renew time.Duration
	// Cadence bounds how often a changing rate is reevaluated.
	Cadence aggregate.Cadence
	// RetireInterval is how often zero-rate keys are retired: zero means
	// DefaultRetireInterval.
	RetireInterval time.Duration
	// Coalesce is the shortest time between passes that input triggers:
	// zero means DefaultCoalesce.
	Coalesce time.Duration
	// Logger receives authority transitions and withheld values; nil
	// discards them.
	Logger *slog.Logger
}

// Stats is a snapshot of a Publisher's diagnostics.
type Stats struct {
	// Ready reports roster readiness at the last pass, and NotReady the
	// sources that were not Ready then, with their reasons.
	Ready    bool
	NotReady []string
	// Leased reports that the last lease decision granted authority.
	Leased bool
	// Leases counts SetLease calls, Revocations Revoke calls, and
	// LeaseErrors leases SetLease refused (a pass that outlasted its
	// lease).
	Leases, Revocations, LeaseErrors uint64
	// LastLease is the wall-clock time the last lease's data was current
	// at; its deadline is LastLease plus the lease length.
	LastLease time.Time
	// Passes counts evaluation passes; Evaluations counts key
	// evaluations; Incomplete counts passes abandoned because a key's
	// view was not complete.
	Passes, Evaluations, Incomplete uint64
	// Published is the number of keys currently in the output, and
	// Uncertain how many of them were Uncertain when last evaluated.
	Published, Uncertain int
	// Withheld is the number of keys whose rate does not fit the output
	// slot and is withheld; Overflows counts withholdings.
	Withheld  int
	Overflows uint64
	// Retired counts keys retired from the output.
	Retired uint64
	// Errors counts keys that could not be evaluated (a table or period
	// error), and LastError is the last such error.
	Errors    uint64
	LastError string
}

// keyState is what the publisher last decided for one key.
type keyState struct {
	value     uint32
	published bool // value is in the output store
	withheld  bool // the rate overflowed the slot
	uncertain bool
	due       time.Time // next evaluation without input; zero if none
}

// Publisher evaluates and publishes aggregate rates. Create it with New,
// pass its Apply to sources.Options.Apply, and call Run once.
type Publisher struct {
	snap   *snapshot.Store
	out    *output.Store
	routes []Route
	byIn   map[string]int
	status func() []sources.SourceStatus
	now    func() time.Time
	lease  time.Duration
	renew  time.Duration
	cad    aggregate.Cadence
	retire time.Duration
	coal   time.Duration
	log    *slog.Logger

	mu    sync.Mutex
	dirty []map[peermsg.Key]bool // by route
	state bool                   // a source event other than an update arrived
	wake  chan struct{}

	// Owned by the pass loop.
	keys       []map[peermsg.Key]*keyState // by route
	wasReady   bool
	sig        string
	leased     bool
	lastLease  time.Time
	lastRetire time.Time

	statsMu sync.Mutex
	stats   Stats
}

// New returns a Publisher for opts.
func New(opts Options) (*Publisher, error) {
	if opts.Snapshot == nil || opts.Output == nil {
		return nil, fmt.Errorf("%w: need a snapshot store and an output store", ErrInvalid)
	}
	p := &Publisher{
		snap: opts.Snapshot, out: opts.Output, byIn: map[string]int{}, status: opts.Status, now: opts.Now,
		lease: opts.Lease, renew: opts.Renew, cad: opts.Cadence, retire: opts.RetireInterval,
		coal: opts.Coalesce, log: opts.Logger, wake: make(chan struct{}, 1),
	}
	if p.now == nil {
		p.now = time.Now
	}
	if p.lease == 0 {
		p.lease = DefaultLease
	}
	if p.lease < 0 || p.lease > output.MaxLeaseLength {
		return nil, fmt.Errorf("%w: lease %v outside (0, %v]", ErrInvalid, p.lease, output.MaxLeaseLength)
	}
	if p.renew == 0 {
		p.renew = p.lease / 4
	}
	if p.renew < 0 || p.renew > p.lease/4 {
		return nil, fmt.Errorf("%w: renewal every %v, want at most a quarter of the %v lease", ErrInvalid, p.renew,
			p.lease)
	}
	if p.retire <= 0 {
		p.retire = DefaultRetireInterval
	}
	if p.coal <= 0 {
		p.coal = DefaultCoalesce
	}
	if p.log == nil {
		p.log = slog.New(slog.DiscardHandler)
	}
	for _, r := range opts.Routes {
		if _, ok := p.snap.Table(r.Input); !ok {
			return nil, fmt.Errorf("%w: route from unknown input table %q", ErrInvalid, r.Input)
		}
		if k, ok := p.out.Kind(r.Output); !ok || k != output.KindAggregate {
			return nil, fmt.Errorf("%w: route to %q, which is not an aggregate output table", ErrInvalid, r.Output)
		}
		if _, dup := p.byIn[r.Input]; dup {
			return nil, fmt.Errorf("%w: input table %q routed twice", ErrInvalid, r.Input)
		}
		p.byIn[r.Input] = len(p.routes)
		p.routes = append(p.routes, r)
		p.dirty = append(p.dirty, map[peermsg.Key]bool{})
		p.keys = append(p.keys, map[peermsg.Key]*keyState{})
	}
	return p, nil
}

// Apply applies a session event to the snapshot store and marks what it
// affects for the next pass: the key of an accepted update, or every key
// after any other event (a session or table change can change source
// state). It has the signature of sources.Options.Apply and returns the
// store's result.
func (p *Publisher) Apply(ev sources.Event) error {
	err := p.snap.Apply(ev)
	p.mu.Lock()
	if u, ok := ev.Body.(peersession.EntryUpdated); ok {
		if i, routed := p.byIn[u.Table]; routed && err == nil {
			p.dirty[i][u.Update.Key] = true
		}
	} else {
		p.state = true
	}
	p.mu.Unlock()
	p.signal()
	return err
}

func (p *Publisher) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Stats returns the current diagnostics.
func (p *Publisher) Stats() Stats {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	s := p.stats
	s.NotReady = append([]string(nil), s.NotReady...)
	return s
}

func (p *Publisher) update(f func(*Stats)) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	f(&p.stats)
}

// Run passes until done is closed: after input (no sooner than Coalesce
// after the previous pass), when a key is due, and every Renew. It
// revokes the lease when it returns, if one is held, counting the
// revocation in Stats.Revocations.
func (p *Publisher) Run(done <-chan struct{}) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	defer func() {
		if p.leased {
			p.out.Revoke()
			p.leased = false
			p.update(func(s *Stats) { s.Revocations++; s.Leased = false })
		}
	}()
	for {
		start := time.Now()
		timer.Reset(max(time.Until(p.Pass()), 0))
		select {
		case <-done:
			return
		case <-timer.C:
			continue
		case <-p.wake:
		}
		// Input: wait out the rest of Coalesce, so that a burst of
		// updates shares one pass.
		if d := time.Until(start.Add(p.coal)); d > 0 {
			timer.Reset(d)
			select {
			case <-done:
				return
			case <-timer.C:
			}
		}
	}
}

// Pass runs one evaluation pass now and returns when the next one is
// needed without new input: the next renewal, due key, or retirement. Run
// calls it; tests may call it directly instead of Run.
func (p *Publisher) Pass() time.Time {
	wall := time.Now()
	if p.status != nil {
		p.snap.ObserveStatus(p.status())
	}
	roster := p.snap.Roster()
	now := roster.At
	p.update(func(s *Stats) {
		s.Passes++
		s.Ready, s.NotReady = roster.Ready, nil
		for _, src := range roster.NotReady() {
			s.NotReady = append(s.NotReady, fmt.Sprintf("%s: %s (%s)", src.Name, src.State, src.Reason))
		}
	})
	sig := signature(roster)
	p.mu.Lock()
	all := p.state || sig != p.sig || !p.wasReady
	p.state = false
	dirty := p.dirty
	p.dirty = make([]map[peermsg.Key]bool, len(p.routes))
	for i := range p.dirty {
		p.dirty[i] = map[peermsg.Key]bool{}
	}
	p.mu.Unlock()
	p.sig = sig

	nextRenew := wall.Add(p.renew)
	if !roster.Ready {
		p.notReady(fmt.Sprint(len(roster.NotReady()), " source(s) not ready"))
		return nextRenew
	}
	for i, r := range p.routes {
		todo := dirty[i]
		if all {
			for _, k := range p.snap.Keys(r.Input) {
				todo[k] = true
			}
			for k := range p.keys[i] {
				todo[k] = true
			}
		}
		for k, st := range p.keys[i] {
			if !st.due.IsZero() && !st.due.After(now) {
				todo[k] = true
			}
		}
		for k := range todo {
			if !p.evaluate(i, k) {
				p.requeue(dirty)
				p.notReady("a key's view was incomplete")
				p.update(func(s *Stats) { s.Incomplete++ })
				return nextRenew
			}
		}
	}
	p.wasReady = true
	if !p.leased || !wall.Before(p.lastLease.Add(p.renew)) {
		p.setLease(wall)
	}
	if wall.Sub(p.lastRetire) >= p.retire {
		p.retireIdle()
		p.lastRetire = wall
	}
	p.publishStats()
	next := p.lastLease.Add(p.renew)
	if r := p.lastRetire.Add(p.retire); r.Before(next) {
		next = r
	}
	if d, ok := p.nextDue(); ok {
		// Due times are on the snapshot clock; convert by offset.
		if w := wall.Add(d.Sub(now)); w.Before(next) {
			next = w
		}
	}
	return next
}

// evaluate reevaluates key k of route i and publishes the result. It
// returns false if the key's view was incomplete: the roster stopped
// being ready during the pass.
func (p *Publisher) evaluate(i int, k peermsg.Key) bool {
	r := p.routes[i]
	rt, err := aggregate.Rate(p.snap, r.Input, k)
	p.update(func(s *Stats) { s.Evaluations++ })
	st := p.keys[i][k]
	if st == nil {
		st = &keyState{}
		p.keys[i][k] = st
	}
	if err != nil {
		// A table or period error: no value can be certified.
		p.withdraw(r, k, st)
		st.due = time.Time{}
		p.update(func(s *Stats) { s.Errors++; s.LastError = err.Error() })
		p.log.Warn("aggregate rate not evaluated; key withdrawn", "table", r.Input, "key", k, "err", err)
		return true
	}
	st.due = time.Time{}
	if d, ok := p.cad.Due(rt); ok {
		st.due = d
	}
	v, err := rt.Authoritative()
	switch {
	case errors.Is(err, aggregate.ErrIncomplete):
		return false
	case err != nil:
		if !st.withheld {
			p.update(func(s *Stats) { s.Overflows++ })
			p.log.Warn("aggregate rate withheld: it does not fit the output slot", "table", r.Input,
				"key", k, "rate", rt.Sum, "err", err)
		}
		p.withdraw(r, k, st)
		st.withheld = true
		return true
	}
	if serr := p.out.Set(r.Output, k, output.AggregateValues(v)); serr != nil {
		// Unreachable: New checked the route and the values are valid.
		p.withdraw(r, k, st)
		p.update(func(s *Stats) { s.Errors++; s.LastError = serr.Error() })
		return true
	}
	st.value, st.published, st.withheld, st.uncertain = v, true, false, rt.Uncertain
	return true
}

// withdraw removes k from the output at once, if published.
func (p *Publisher) withdraw(r Route, k peermsg.Key, st *keyState) {
	if !st.published {
		return
	}
	if err := p.out.Retire(r.Output, k); err == nil {
		p.update(func(s *Stats) { s.Retired++ })
	}
	st.published = false
}

// retireIdle retires every published key whose rate is 0 and needs no
// further evaluation, and forgets keys that are neither published nor
// pending.
func (p *Publisher) retireIdle() {
	batch := map[string][]peermsg.Key{}
	for i, r := range p.routes {
		for k, st := range p.keys[i] {
			if !st.due.IsZero() || st.withheld {
				continue
			}
			if st.published && st.value != 0 {
				continue
			}
			if st.published {
				batch[r.Output] = append(batch[r.Output], k)
			}
			delete(p.keys[i], k)
		}
	}
	if len(batch) == 0 {
		return
	}
	// One store change for the whole batch: each session rotates and
	// re-sends once (output.Store.RetireBatch).
	if err := p.out.RetireBatch(batch); err == nil {
		var n uint64
		for _, ks := range batch {
			n += uint64(len(ks))
		}
		p.update(func(s *Stats) { s.Retired += n })
	}
}

func (p *Publisher) nextDue() (time.Time, bool) {
	var next time.Time
	found := false
	for i := range p.routes {
		for _, st := range p.keys[i] {
			if !st.due.IsZero() && (!found || st.due.Before(next)) {
				next, found = st.due, true
			}
		}
	}
	return next, found
}

// requeue returns the pass's dirty keys to the next pass.
func (p *Publisher) requeue(dirty []map[peermsg.Key]bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, m := range dirty {
		for k := range m {
			p.dirty[i][k] = true
		}
	}
}

// setLease certifies every value written so far as current at wall.
func (p *Publisher) setLease(wall time.Time) {
	if err := p.out.SetLease(wall.Add(p.lease)); err != nil {
		p.update(func(s *Stats) { s.LeaseErrors++; s.LastError = err.Error() })
		p.log.Warn("lease not renewed", "err", err)
		return
	}
	if !p.leased {
		p.log.Info("aggregate authority granted", "lease", p.lease)
	}
	p.leased, p.lastLease = true, wall
	p.update(func(s *Stats) { s.Leases++; s.Leased = true; s.LastLease = wall })
}

// notReady stops certifying: it revokes a held lease once and makes the
// next ready pass reevaluate every key.
func (p *Publisher) notReady(why string) {
	p.wasReady = false
	if p.leased {
		p.out.Revoke()
		p.leased = false
		p.log.Info("aggregate authority revoked", "reason", why)
		p.update(func(s *Stats) { s.Revocations++; s.Leased = false })
	}
	p.publishStats()
}

func (p *Publisher) publishStats() {
	published, uncertain, withheld := 0, 0, 0
	for i := range p.routes {
		for _, st := range p.keys[i] {
			if st.published {
				published++
				if st.uncertain {
					uncertain++
				}
			}
			if st.withheld {
				withheld++
			}
		}
	}
	p.update(func(s *Stats) { s.Published, s.Uncertain, s.Withheld = published, uncertain, withheld })
}

// signature identifies every source's state and session, so a pass can
// tell that one changed.
func signature(r snapshot.Roster) string {
	b := make([]byte, 0, 32*len(r.Sources))
	for _, s := range r.Sources {
		b = fmt.Appendf(b, "%s/%d/%d;", s.Name, s.State, s.Session)
	}
	return string(b)
}
