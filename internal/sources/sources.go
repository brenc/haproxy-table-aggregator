// Package sources maintains one peers session per configured source: it
// accepts the sessions sources open, dials the sources that have an
// address, chooses which connection represents each source, and hands the
// sessions' validated events to the application through one bounded
// queue.
//
// # One logical source
//
// A source is identified by its configured name, never by a connection or
// process ID. At most one session per source is current at a time, and its
// events carry the source name and a session number that increases with
// every session the source gets. For one source, every event of a session,
// from SessionUp to SessionDown, is queued before the next session's
// SessionUp.
//
// Colliding connections are resolved so that both ends agree on the
// survivor. Stock HAProxy keeps one session per peer: when it accepts a
// hello it closes whatever session it had for that peer, including its own
// connection attempt still in progress, and it dials a peer only while it
// has none. Therefore:
//
//   - An outbound session whose hello HAProxy accepted (status 200) always
//     becomes current, replacing any other session of the source: HAProxy
//     has just closed its side of that other session.
//   - An inbound session becomes current, replacing a previous inbound
//     one, unless an outbound session is current. HAProxy dials only once
//     it has no session, so the previous inbound session is dead on its
//     side.
//   - An inbound connection that arrives while an outbound session is
//     current is refused without a status line (ErrOutboundCurrent). It
//     comes either from an attempt HAProxy abandoned when it accepted the
//     outbound hello (its hello can still arrive later, once a retried TCP
//     connect completes) or from HAProxy having lost the outbound session,
//     in which case this daemon sees that session close or go idle within
//     the idle timeout, and HAProxy dials again.
//
// Outbound attempts to a source wait while it has a current session and
// back off exponentially with jitter, between the configured bounds,
// after every attempt or session end.
//
// # Event queue
//
// Table events (definitions, updates, resync replies) wait at most the
// configured event timeout for room in the queue; on timeout the session
// fails without acknowledging the update (see package peersession).
// SessionUp and SessionDown wait for room until the manager closes (and
// then at most the event timeout), so the application learns of every
// session end unless it stops draining the queue.
//
// # Output
//
// With output tables configured, the manager owns one output.Store that
// every session teaches to its source (see package peersession): each
// table once the source has announced a matching definition of it, the
// whole store whenever the source requests a resync, then each change as
// Publish makes it, and the lease (SetLease, Revoke) after the values it
// certifies. Sessions read the store independently, so a slow source
// delays only its own session. Output is never an input: the sources'
// copies of output tables are acknowledged but never queued as events,
// and no session ever announces an input table.
package sources

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"sync"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
)

// Event is one event of one source's session.
type Event struct {
	// Source is the configured source name.
	Source string
	// Session numbers the source's sessions from 1, in the order they
	// became current.
	Session uint64
	// Body is a peersession.SessionUp, SessionDown, TableDefined,
	// EntryUpdated, or SyncFinished.
	Body peersession.Event
}

// ErrQueueFull is the cause a session reports when the event queue stayed
// full for the whole event timeout.
var ErrQueueFull = errors.New("sources: event queue full")

// ErrOutboundCurrent is why an inbound connection is refused: the source
// already has a current outbound session (see the package documentation).
var ErrOutboundCurrent = errors.New("sources: the source's outbound session is current")

// errClosing refuses new sessions during shutdown.
var errClosing = errors.New("sources: manager closing")

// maxPendingInbound bounds connections accepted but not yet established.
const maxPendingInbound = 64

// Options configures Start.
type Options struct {
	// Config is the validated configuration.
	Config config.Config
	// Listener, if non-nil, is used instead of listening on
	// Config.Listen. The manager closes it.
	Listener net.Listener
	// Dial, if non-nil, replaces the TCP dialer for outbound sessions.
	Dial func(ctx context.Context, address string) (net.Conn, error)
	// Logger receives operational logs. Nil discards them.
	Logger *slog.Logger
	// Output, if non-nil, is the output store to teach instead of a new
	// empty one, so that entries published before Start are taught when
	// the first sessions start. Its tables must be exactly the
	// configuration's outputs, in order.
	Output *output.Store
	// OutputRefresh overrides how often each session re-sends the whole
	// output (peersession.Options.Refresh): zero keeps the default, a
	// third of the shortest aggregate table expire; negative disables
	// it, which only tests should do.
	OutputRefresh time.Duration
}

// Manager runs the sessions of every configured source.
type Manager struct {
	cfg      config.Config
	sessOpts peersession.Options
	out      *output.Store
	log      *slog.Logger
	dial     func(ctx context.Context, address string) (net.Conn, error)
	ln       net.Listener
	events   chan Event
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	pending  chan struct{}

	mu      sync.Mutex
	slots   map[string]*slot
	closing bool

	closeOnce sync.Once
}

// slot is one source's session state, guarded by Manager.mu.
type slot struct {
	name    string
	gen     uint64
	cur     *active // current session, nil when none
	last    *active // most recently claimed session, current or ending
	changed chan struct{}
	lastErr error // why the last attempt or session failed
}

// active is one claimed session.
type active struct {
	gen    uint64
	dir    peersession.Direction
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	conn   *peersession.Conn
	up     bool
}

// Start listens (when configured), starts dialing the sources that have
// addresses, and returns. Close stops everything. It fails, wrapping
// config.ErrInvalid, unless the configuration passes
// config.Config.CheckPlaintextGate and a given Listener is on loopback.
func Start(ctx context.Context, opts Options) (*Manager, error) {
	cfg := opts.Config
	// Until mutual TLS exists every session is plaintext: refuse a
	// Config that bypassed Validate and would reach off loopback.
	if err := cfg.CheckPlaintextGate(); err != nil {
		return nil, fmt.Errorf("sources: %w", err)
	}
	if opts.Listener != nil {
		if err := config.CheckLoopbackAddr(opts.Listener.Addr().String(), true); err != nil {
			return nil, fmt.Errorf("sources: listener: %w: %w", config.ErrInvalid, err)
		}
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	tables := map[string]peersession.TableSpec{}
	for _, t := range cfg.Tables {
		ms := t.Period.Milliseconds()
		if ms <= 0 || ms > math.MaxUint32 {
			return nil, fmt.Errorf("sources: table %s: period %v out of range", t.Name, t.Period)
		}
		tables[t.Name] = peersession.TableSpec{Period: peermsg.Millis(ms)}
	}
	out := opts.Output
	if out != nil {
		if err := sameOutputs(out, cfg.OutputTables()); err != nil {
			return nil, err
		}
	}
	if len(cfg.Outputs) > 0 {
		if !cfg.RequestResync {
			return nil, fmt.Errorf("sources: %w: output tables need RequestResync", config.ErrInvalid)
		}
		if out == nil {
			var err error
			if out, err = output.NewStore(cfg.OutputTables(), output.StoreOptions{}); err != nil {
				return nil, fmt.Errorf("sources: %w", err)
			}
		}
		for _, t := range cfg.Outputs {
			if _, ok := tables[t.Name]; ok {
				return nil, fmt.Errorf("sources: output table %s is also an input table", t.Name)
			}
		}
	}
	m := &Manager{
		cfg: cfg,
		out: out,
		sessOpts: peersession.Options{
			LocalPeer:        cfg.LocalPeer,
			Tables:           tables,
			Heartbeat:        cfg.Heartbeat,
			IdleTimeout:      cfg.IdleTimeout,
			HandshakeTimeout: cfg.HandshakeTimeout,
			MaxTables:        cfg.MaxSessionTables,
			RequestResync:    cfg.RequestResync,
			Output:           out,
			Refresh:          opts.OutputRefresh,
		},
		log:     log,
		dial:    opts.Dial,
		ln:      opts.Listener,
		events:  make(chan Event, cfg.EventQueue),
		pending: make(chan struct{}, maxPendingInbound),
		slots:   map[string]*slot{},
	}
	if m.dial == nil {
		m.dial = func(ctx context.Context, address string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", address)
		}
	}
	for _, s := range cfg.Sources {
		m.slots[s.Name] = &slot{name: s.Name, changed: make(chan struct{})}
	}
	if m.ln == nil && cfg.Listen != "" {
		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, "tcp", cfg.Listen)
		if err != nil {
			return nil, fmt.Errorf("sources: listen: %w", err)
		}
		m.ln = ln
	}
	m.ctx, m.cancel = context.WithCancel(context.WithoutCancel(ctx))
	if m.ln != nil {
		m.wg.Go(m.acceptLoop)
	}
	for _, s := range cfg.Sources {
		if s.Address != "" {
			m.wg.Go(func() { m.dialLoop(m.slots[s.Name], s.Address) })
		}
	}
	return m, nil
}

// sameOutputs requires a store's tables to be the configured outputs.
func sameOutputs(s *output.Store, want []output.Table) error {
	got := s.Tables()
	if len(got) != len(want) {
		return fmt.Errorf("sources: output store has %d tables, configuration %d", len(got), len(want))
	}
	for i, w := range want {
		d := output.Definition(w.Name, w.Expiry)
		k, _ := s.Kind(got[i].Name)
		if g := got[i]; g.Name != d.Name || g.Expiry != d.Expiry || k != w.Kind {
			return fmt.Errorf("sources: output store table %d is %v table %s expiring after %v, configuration "+
				"has %v table %s after %v", i, k, g.Name, g.Expiry, w.Kind, d.Name, d.Expiry)
		}
	}
	return nil
}

// Addr returns the listener's address, or nil without a listener.
func (m *Manager) Addr() net.Addr {
	if m.ln == nil {
		return nil
	}
	return m.ln.Addr()
}

// Publish sets the values of key in the output table named table, for
// every current and future session to teach. Publishing unchanged values
// sends nothing. It fails, wrapping output.ErrInvalid, for a table that is
// not a configured output or values its kind does not allow.
func (m *Manager) Publish(table string, key peermsg.Key, values output.Values) error {
	if m.out == nil {
		return fmt.Errorf("%w: no output tables configured", output.ErrInvalid)
	}
	return m.out.Set(table, key, values)
}

// SetLease declares every value published so far authoritative until
// until (see output.Store.SetLease); each session writes the lease into
// its source's metadata tables after those values. It fails, wrapping
// output.ErrInvalid, without output tables or for a lease that has
// already ended or runs longer than output.MaxLeaseLength.
func (m *Manager) SetLease(until time.Time) error {
	if m.out == nil {
		return fmt.Errorf("%w: no output tables configured", output.ErrInvalid)
	}
	return m.out.SetLease(until)
}

// Retire stops publishing key in the aggregate table named table (see
// output.Store.Retire): no session sends it again, each switches
// generation so that its sources' copies are never certified again, and
// HAProxy expires them. It fails, wrapping output.ErrInvalid, without
// output tables or for a table that is not an aggregate output.
func (m *Manager) Retire(table string, key peermsg.Key) error {
	if m.out == nil {
		return fmt.Errorf("%w: no output tables configured", output.ErrInvalid)
	}
	return m.out.Retire(table, key)
}

// Revoke withdraws authority at once in every source (see
// output.Store.Revoke). It does nothing without output tables.
func (m *Manager) Revoke() {
	if m.out != nil {
		m.out.Revoke()
	}
}

// Events returns the event queue. It is closed after Close once every
// session has ended.
func (m *Manager) Events() <-chan Event { return m.events }

// Close stops accepting and dialing, ends every session, waits for every
// goroutine the manager started, and closes the event queue. Events still
// queued stay readable. It is safe to call more than once.
func (m *Manager) Close() error {
	var err error
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closing = true
		m.mu.Unlock()
		m.cancel()
		if m.ln != nil {
			if cerr := m.ln.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
				err = cerr
			}
		}
		m.wg.Wait()
		close(m.events)
	})
	return err
}

// SourceStatus describes one source.
type SourceStatus struct {
	// Name is the source name.
	Name string
	// Up reports whether a session is current and established.
	Up bool
	// Session is the current (or last) session number; 0 if none yet.
	Session uint64
	// Direction is the current session's direction, if Up.
	Direction peersession.Direction
	// Stats are the current session's counters, if Up.
	Stats peersession.Stats
	// LastErr is why the source's last connection attempt or session
	// failed, or nil.
	LastErr error
}

// Status returns every source's status, in configuration order.
func (m *Manager) Status() []SourceStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SourceStatus, 0, len(m.cfg.Sources))
	for _, s := range m.cfg.Sources {
		sl := m.slots[s.Name]
		st := SourceStatus{Name: s.Name, Session: sl.gen, LastErr: sl.lastErr}
		if a := sl.cur; a != nil && a.up {
			st.Up, st.Direction, st.Stats = true, a.dir, a.conn.Stats()
		}
		out = append(out, st)
	}
	return out
}

// Disconnect ends the source's current session, as if its connection had
// dropped, and reports whether there was one. Reconnection then proceeds
// as after any other session end.
func (m *Manager) Disconnect(source string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	sl := m.slots[source]
	if sl == nil || sl.cur == nil {
		return false
	}
	sl.cur.cancel()
	return true
}

func (m *Manager) known(name string) bool {
	_, ok := m.slots[name]
	return ok
}

// claim makes a newly handshaken connection the source's current session,
// cancelling the previous one, and waits until the previous session has
// queued its last event. An inbound connection never replaces a current
// outbound session; see the package documentation.
func (m *Manager) claim(sl *slot, dir peersession.Direction, pc *peersession.Conn) (*active, error) {
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return nil, errClosing
	}
	if cur := sl.cur; dir == peersession.Inbound && cur != nil && cur.dir == peersession.Outbound {
		m.mu.Unlock()
		return nil, ErrOutboundCurrent
	}
	prev := sl.last
	if sl.cur != nil {
		sl.cur.cancel()
	}
	sl.gen++
	ctx, cancel := context.WithCancel(m.ctx)
	a := &active{
		gen: sl.gen, dir: dir, ctx: ctx, cancel: cancel,
		done: make(chan struct{}), conn: pc,
	}
	sl.cur, sl.last = a, a
	m.notify(sl)
	m.mu.Unlock()
	if prev != nil {
		<-prev.done
	}
	return a, nil
}

// release ends a claimed session's tenure.
func (m *Manager) release(sl *slot, a *active) {
	m.mu.Lock()
	if sl.cur == a {
		sl.cur = nil
		m.notify(sl)
	}
	m.mu.Unlock()
	a.cancel()
	close(a.done)
}

// nudgeDelays are when a session sends extra heartbeats (see
// peersession.Conn.Nudge) after it is established and after a connection
// from its source was refused: both are moments when HAProxy may just
// have released another connection to this daemon, clearing its liveness
// flag, and after a collision HAProxy checks liveness again within 50 ms
// to 2 s. A heartbeat counts only if it arrives after the release, so one
// comes almost at once and one later in case the first overtook it.
var nudgeDelays = []time.Duration{10 * time.Millisecond, 250 * time.Millisecond}

// nudge makes the source's current session send heartbeats shortly.
func (m *Manager) nudge(sl *slot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if a := sl.cur; a != nil && a.up {
		nudgeConn(a.conn)
	}
}

func nudgeConn(c *peersession.Conn) {
	for _, d := range nudgeDelays {
		time.AfterFunc(d, func() { c.Nudge(0) })
	}
}

// fault records why a source's attempt or session failed.
func (m *Manager) fault(sl *slot, err error) {
	m.mu.Lock()
	sl.lastErr = err
	m.mu.Unlock()
}

// notify wakes waiters on sl's state. Callers hold m.mu.
func (m *Manager) notify(sl *slot) {
	close(sl.changed)
	sl.changed = make(chan struct{})
}

// run emits SessionUp, runs the session, and emits SessionDown.
func (m *Manager) run(sl *slot, a *active, up peersession.SessionUp) {
	m.mu.Lock()
	a.up = true
	m.mu.Unlock()
	if !m.emitLifecycle(sl, a, up) {
		return
	}
	m.log.Info("session up", "source", sl.name, "session", a.gen, "direction", a.dir.String(),
		"remote", up.RemoteAddr)
	nudgeConn(a.conn)
	err := a.conn.Run(a.ctx, func(ctx context.Context, ev peersession.Event) error {
		return m.emit(ctx, sl, a, ev)
	})
	// Close before queueing SessionDown, which may wait for room.
	_ = a.conn.Close()
	if errors.Is(err, context.Canceled) && m.ctx.Err() == nil {
		err = fmt.Errorf("%w: replaced or disconnected", err)
	}
	m.log.Info("session down", "source", sl.name, "session", a.gen, "err", err)
	m.fault(sl, err)
	m.emitLifecycle(sl, a, peersession.SessionDown{Err: err, At: time.Now()})
}

// emit queues a table event, waiting at most the event timeout.
func (m *Manager) emit(ctx context.Context, sl *slot, a *active, ev peersession.Event) error {
	e := Event{Source: sl.name, Session: a.gen, Body: ev}
	select {
	case m.events <- e:
		return nil
	default:
	}
	t := time.NewTimer(m.cfg.EventTimeout)
	defer t.Stop()
	select {
	case m.events <- e:
		return nil
	case <-t.C:
		return fmt.Errorf("%w for %v", ErrQueueFull, m.cfg.EventTimeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// emitLifecycle queues SessionUp or SessionDown. It waits for room until
// the manager closes, and then at most the event timeout, so that a
// consumer still draining the queue during Close learns of every session
// end. It reports whether the event was queued.
func (m *Manager) emitLifecycle(sl *slot, a *active, ev peersession.Event) bool {
	e := Event{Source: sl.name, Session: a.gen, Body: ev}
	select {
	case m.events <- e:
		return true
	case <-m.ctx.Done():
	}
	t := time.NewTimer(m.cfg.EventTimeout)
	defer t.Stop()
	select {
	case m.events <- e:
		return true
	case <-t.C:
		return false
	}
}
