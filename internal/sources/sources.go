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
}

// Manager runs the sessions of every configured source.
type Manager struct {
	cfg      config.Config
	sessOpts peersession.Options
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
	m := &Manager{
		cfg: cfg,
		sessOpts: peersession.Options{
			LocalPeer:        cfg.LocalPeer,
			Tables:           tables,
			Heartbeat:        cfg.Heartbeat,
			IdleTimeout:      cfg.IdleTimeout,
			HandshakeTimeout: cfg.HandshakeTimeout,
			MaxTables:        cfg.MaxSessionTables,
			RequestResync:    cfg.RequestResync,
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

// Addr returns the listener's address, or nil without a listener.
func (m *Manager) Addr() net.Addr {
	if m.ln == nil {
		return nil
	}
	return m.ln.Addr()
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
