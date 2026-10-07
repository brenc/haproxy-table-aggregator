package peersession

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// Options configures one session. Durations must be positive.
type Options struct {
	// LocalPeer is this side's peer name.
	LocalPeer string
	// Tables are the input tables by HAProxy table name. Every table the
	// source announces that is neither an input nor an output table is
	// ignored.
	Tables map[string]TableSpec
	// Output, if non-nil, is the published output this session teaches
	// to the source: its tables are announced under local IDs 1, 2, ...
	// in Output.Tables order. A table is taught (definition and every
	// entry) once the source announces a matching definition of it, and
	// again whenever the source requests a resync, and is updated live
	// as the store changes. Nothing is sent for a table the source has
	// not announced. Output table names must differ from input names,
	// and RequestResync must be set: stock HAProxy announces a table it
	// never updates itself, as an output table, only while teaching.
	//
	// The store's lease is written into every metadata table as a timed
	// update, and only once every output table has been taught in this
	// session and every value the lease certifies has been written
	// ahead of it on the connection: a marker never overtakes the
	// output it certifies, and a teach never replays an old marker.
	// Every entry the session writes, values and markers, carries the
	// session's Generation, so a marker certifies only its own session's
	// values.
	Output *output.Store
	// Generation is the session's output generation (see package
	// output). Zero picks a random non-zero one, as every session should:
	// a generation must differ from every earlier session's to the
	// same source. Tests set it to read fixed output. The session picks
	// a new random generation, and re-sends the whole output under it,
	// whenever Store.Retire removes keys.
	Generation uint32
	// Refresh is how often a session whose output tables are all taught
	// re-sends every output entry under its generation (definitions and
	// values, then the lease again), so that an entry another writer
	// overwrote in HAProxy (an old process's teach on a soft reload)
	// regains aggregate authority within Refresh, and an unchanged entry
	// never expires in HAProxy while published. Zero means a third of the
	// shortest aggregate table expiry; negative disables it (tests).
	Refresh time.Duration
	// Heartbeat is the longest this side stays silent once established.
	Heartbeat time.Duration
	// IdleTimeout ends an established session after this long without a
	// complete message from the source, measured from when the last one
	// was processed (so a slow sink never makes a source look idle).
	IdleTimeout time.Duration
	// HandshakeTimeout bounds the hello exchange, measured from NewConn.
	HandshakeTimeout time.Duration
	// MaxTables bounds the table IDs the source may bind in this session
	// (see peermsg.NewInbound).
	MaxTables int
	// RequestResync sends a resync request once established.
	RequestResync bool
	// ResyncRetry, when positive, makes the session request another
	// resync this long after the source answered one with "partial" (it
	// is not itself synchronized yet, so what it taught may be
	// incomplete). The session keeps retrying until a "finished" reply;
	// zero or negative never retries.
	ResyncRetry time.Duration
	// PID is the process ID announced in this side's hello. Zero means
	// the current process ID.
	PID uint32
	// Limits are the framing limits; the zero value means
	// peerwire.DefaultLimits.
	Limits peerwire.Limits
}

// TableSpec is the schema an input table must announce, beyond the fixed
// IPv6 key and the field set http_req_cnt, http_req_rate(Period).
type TableSpec struct {
	// Period is the required http_req_rate period.
	Period peermsg.Millis
}

// Sink receives validated events from Run, in wire order. It must return
// promptly; a non-nil error ends the session without acknowledging the
// update the event carried. The context is the session's.
type Sink func(ctx context.Context, ev Event) error

// Phase is a connection's lifecycle phase.
type Phase uint32

// Phases, in order. A Conn never moves backwards.
const (
	// PhaseHello: accepted connection, reading the source's hello.
	PhaseHello Phase = iota + 1
	// PhaseStatus: dialed connection, hello sent, awaiting the status.
	PhaseStatus
	// PhaseEstablished: handshake succeeded; binary messages flow.
	PhaseEstablished
	// PhaseClosed: the handshake or the session ended.
	PhaseClosed
)

// String names the phase.
func (p Phase) String() string {
	switch p {
	case PhaseHello:
		return "hello"
	case PhaseStatus:
		return "status"
	case PhaseEstablished:
		return "established"
	case PhaseClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// LearnState tracks this side's own resync request.
type LearnState uint32

// Learn states.
const (
	// LearnNone: no resync requested.
	LearnNone LearnState = iota
	// LearnRequested: request sent, awaiting "finished" or "partial".
	LearnRequested
	// LearnFinished: the source replied "finished"; confirmed.
	LearnFinished
	// LearnPartial: the source replied "partial"; confirmed. With
	// Options.ResyncRetry set, the session requests again once it has
	// passed (back to LearnRequested); otherwise the state is final.
	LearnPartial
)

// String names the state.
func (s LearnState) String() string {
	switch s {
	case LearnNone:
		return "none"
	case LearnRequested:
		return "requested"
	case LearnFinished:
		return "finished"
	case LearnPartial:
		return "partial"
	default:
		return "unknown"
	}
}

// Stats is a snapshot of a session's counters. It is safe to read while
// the session runs.
type Stats struct {
	Phase           Phase
	Learn           LearnState
	PendingConfirms uint32
	RxMessages      uint64
	RxHeartbeats    uint64
	TxHeartbeats    uint64
	Updates         uint64
	SkippedUpdates  uint64
	AcksSent        uint64
	// ResyncRequests counts the resync requests sent (see
	// Options.ResyncRetry).
	ResyncRequests uint64
	// LastRx is when the last processed message was read; zero before
	// the first. It carries a monotonic clock reading, so its age is
	// immune to wall-clock steps.
	LastRx time.Time
	// EchoedUpdates counts updates of output tables received from the
	// source (its copy of what this side published, replayed when it
	// teaches): acknowledged, never delivered as events.
	EchoedUpdates uint64
	// Teaches counts teaches of the output sent: one per output table
	// when the source first announces it, and one (of every announced
	// table) per resync request from the source.
	Teaches uint64
	// Refreshes counts periodic re-sends of the whole output (see
	// Options.Refresh); their updates count as TaughtUpdates.
	Refreshes uint64
	// OutputUpdates counts aggregate entry updates sent; TaughtUpdates
	// counts those sent by teaches, the rest were live changes. Lease
	// markers are counted in Markers only.
	OutputUpdates uint64
	TaughtUpdates uint64
	// AcksReceived counts acknowledgements of output updates.
	AcksReceived uint64
	// Markers counts lease markers written to the metadata tables
	// (revocations included); Revocations counts the revocations.
	// LastMarker is when the last one was written, and LastDeadline
	// its deadline slot. Written means handed to the connection, not
	// delivered: the deadline, not this time, bounds its authority.
	Markers      uint64
	Revocations  uint64
	LastMarker   time.Time
	LastDeadline uint32
	// LiveMarker reports that a live lease marker of this session may be
	// outstanding at the source: a publish has read a live lease it may
	// write, or one was queued and no revocation queued after it has been
	// entirely written to the connection yet. False before the first live marker. A source whose
	// LiveMarker is false holds no authority from this session.
	LiveMarker bool
	// Generation is the session's current output generation.
	Generation uint32
	// Rotations counts generation changes after Store.Retire, each with
	// a full re-send of the output.
	Rotations uint64
	// OutQueued is how many bytes wait in the session's send queue, and
	// MaxOutQueued the most that ever waited in this session.
	OutQueued    int64
	MaxOutQueued int64
	// OutDeferred counts the backlogs during which output changes were
	// deferred because more than SoftBacklog bytes waited.
	OutDeferred uint64
	// Outputs describes each output table, in local ID order.
	Outputs []OutputStats
}

// OutputStats is one output table's state in a session.
type OutputStats struct {
	// Table is the output table name.
	Table string
	// LocalID is the ID this side announced the table under.
	LocalID peermsg.LocalTableID
	// SourceID is the ID under which the source announced its own
	// definition of the table, which matched; 0 if it has not. Nothing
	// is sent for a table before the source announces it, so a source
	// that never does (it does not share the table with this peer)
	// receives no output for it.
	SourceID peermsg.RemoteTableID
	// Sent counts updates sent for the table in this session; LastSent
	// is the last one's ID.
	Sent     uint64
	LastSent peermsg.UpdateID
	// Acked reports whether the source acknowledged any update of the
	// table; LastAcked is the latest acknowledged ID. Acknowledgement is
	// transport progress only: it is not proof that the values are fresh.
	Acked     bool
	LastAcked peermsg.UpdateID
}

// Conn is one peers connection. Create it with NewConn, complete the
// handshake with ReadHello and AcceptHello (acceptor) or Greet
// (initiator), then call Run once. A Conn closes the net.Conn only in
// Close; the caller closes it after Run or a failed handshake returns.
// Stats, Nudge, Close, and RemoteAddr may be called from any goroutine;
// the other methods must be called from one goroutine.
type Conn struct {
	nc       net.Conn
	opts     Options
	dec      *peerwire.Decoder
	buf      []byte
	deadline time.Time // handshake deadline
	eof      bool

	in       *peermsg.Inbound
	tables   map[peermsg.RemoteTableID]*tableState
	local    *peermsg.LocalTables
	outs     []*outTable // by local ID - 1
	outNames map[string]*outTable
	outSeq   uint64               // store sequence sent so far
	outSel   peermsg.LocalTableID // table the last definition sent selected
	obuf     []byte
	confirms uint32
	learn    LearnState
	lastRx   time.Time
	lastTx   time.Time
	rxTime   time.Time // when the last read returned data
	wbuf     []byte

	phase          atomic.Uint32
	learnA         atomic.Uint32
	confirmsA      atomic.Uint32
	rxMessages     atomic.Uint64
	rxHeartbeats   atomic.Uint64
	txHeartbeats   atomic.Uint64
	updates        atomic.Uint64
	skipped        atomic.Uint64
	acksSent       atomic.Uint64
	lastRxMono     atomic.Int64 // LastRx as time since monoBase, or 0
	resyncAt       time.Time    // when to repeat a resync request, or zero
	resyncRequests atomic.Uint64
	nudgeAt        atomic.Int64 // UnixNano of a requested early heartbeat, or 0
	echoed         atomic.Uint64
	teaches        atomic.Uint64
	outUpdates     atomic.Uint64
	taught         atomic.Uint64
	acksRecv       atomic.Uint64
	outWake        atomic.Bool // the output store changed since the last send
	markers        atomic.Uint64
	revocations    atomic.Uint64
	markerNano     atomic.Int64
	markerDL       atomic.Uint32
	liveGen        atomic.Uint64 // live markers queued (or being queued)
	revWritten     atomic.Uint64 // liveGen covered by a fully written revocation
	markerPending  atomic.Bool   // publish read a live lease and may write it

	// leaseSent is the lease generation last written (0: none), and
	// markerDue forces the next publish to write the lease again, after
	// a teach.
	leaseSent uint64
	markerDue bool
	// refreshAt is when the next periodic refresh is due; zero until
	// every output table is taught.
	refreshAt time.Time
	refreshes atomic.Uint64

	// wq is the established session's send queue (nil during the
	// handshake, whose writes are synchronous). outBlocked asks its
	// writer to wake Run once a backlog drains, and outPending remembers
	// an output change deferred meanwhile.
	wq         *sendQueue
	wqA        atomic.Pointer[sendQueue] // wq, for Stats
	outBlocked atomic.Bool
	outPending bool
	deferred   atomic.Uint64

	// gen is the session's current output generation, changed only when
	// a Retire is seen (retiredSeen is the store's count then).
	gen         uint32
	genA        atomic.Uint32
	retiredSeen uint64
	rotations   atomic.Uint64
}

// tableKind is how a session treats a table the source announced.
type tableKind uint8

const (
	kindIgnored tableKind = iota // not shared with this side's purpose
	kindInput                    // a configured input table
	kindOutput                   // the source's copy of an output table
)

// outTable is one output table this session announces.
type outTable struct {
	id   peermsg.LocalTableID
	def  peermsg.Definition
	kind output.Kind
	last peermsg.UpdateID // last update ID sent
	sent uint64
	// ready is set once the source announced a matching definition of
	// the table; nothing is sent for it before.
	ready bool
	// taughtSeq is the store sequence the table's last teach was
	// current to; publish skips entries no newer.
	taughtSeq uint64

	sourceID  atomic.Uint32
	sentA     atomic.Uint64
	lastSentA atomic.Uint32
	acked     atomic.Bool
	lastAcked atomic.Uint32
}

// tableState is this session's view of one table ID the source bound.
type tableState struct {
	name    string
	kind    tableKind
	def     peermsg.Definition
	pending peermsg.UpdateID // last update accepted by the sink
	dirty   bool             // pending not yet acknowledged
}

// readSize is the largest single read; the decoder's free space bounds it
// further.
const readSize = 16 << 10

// errorTimeout bounds the best-effort error status or message written
// before a failed session ends.
const errorTimeout = time.Second

// monoBase anchors times shared across goroutines as monotonic offsets
// (see Stats.LastRx).
var monoBase = time.Now()

// aLongTimeAgo is a deadline in the past, used to interrupt blocked I/O.
var aLongTimeAgo = time.Unix(1, 0)

// errDeadline is the internal result of a read whose deadline passed.
var errDeadline = errors.New("read deadline")

// NewConn wraps nc, accepted or dialed just now. The handshake deadline
// starts now.
func NewConn(nc net.Conn, opts Options) (*Conn, error) {
	if opts.LocalPeer == "" || opts.Heartbeat <= 0 || opts.IdleTimeout <= 0 || opts.HandshakeTimeout <= 0 {
		return nil, errors.New("peersession: incomplete options")
	}
	if opts.Limits == (peerwire.Limits{}) {
		opts.Limits = peerwire.DefaultLimits()
	}
	dec, err := peerwire.NewDecoder(opts.Limits)
	if err != nil {
		return nil, err
	}
	if opts.PID == 0 {
		opts.PID = currentPID()
	}
	c := &Conn{
		nc:       nc,
		opts:     opts,
		dec:      dec,
		buf:      make([]byte, readSize),
		deadline: time.Now().Add(opts.HandshakeTimeout),
		in:       peermsg.NewInbound(opts.MaxTables),
		tables:   map[peermsg.RemoteTableID]*tableState{},
		outNames: map[string]*outTable{},
	}
	if opts.Output != nil {
		if opts.Generation == 0 {
			g, err := output.NewGeneration()
			if err != nil {
				return nil, fmt.Errorf("peersession: %w", err)
			}
			c.opts.Generation = g
		}
		c.gen = c.opts.Generation
		c.genA.Store(c.gen)
		c.retiredSeen = opts.Output.Retired()
		if opts.Refresh == 0 {
			for _, def := range opts.Output.Tables() {
				if k, _ := opts.Output.Kind(def.Name); k != output.KindAggregate {
					continue
				}
				if r := def.Expiry.Duration() / 3; c.opts.Refresh == 0 || r < c.opts.Refresh {
					c.opts.Refresh = r
				}
			}
		}
		if !opts.RequestResync {
			return nil, errors.New("peersession: output needs RequestResync: the source announces output tables only when it teaches")
		}
		defs := opts.Output.Tables()
		c.local = peermsg.NewLocalTables(len(defs))
		for _, def := range defs {
			if _, ok := opts.Tables[def.Name]; ok {
				return nil, fmt.Errorf("peersession: output table %s is also an input table", def.Name)
			}
			id, err := c.local.Register(def)
			if err != nil {
				return nil, fmt.Errorf("peersession: output table: %w", err)
			}
			kind, _ := opts.Output.Kind(def.Name)
			t := &outTable{id: id, def: def, kind: kind}
			c.outs = append(c.outs, t)
			c.outNames[def.Name] = t
		}
	}
	return c, nil
}

func currentPID() uint32 {
	pid := os.Getpid()
	if pid <= 0 || uint64(pid) > math.MaxUint32 {
		return 1
	}
	return uint32(pid)
}

// Stats returns a snapshot of the counters. It may be called from any
// goroutine.
func (c *Conn) Stats() Stats {
	s := Stats{
		Phase:           Phase(c.phase.Load()),
		Learn:           LearnState(c.learnA.Load()),
		PendingConfirms: c.confirmsA.Load(),
		RxMessages:      c.rxMessages.Load(),
		RxHeartbeats:    c.rxHeartbeats.Load(),
		TxHeartbeats:    c.txHeartbeats.Load(),
		Updates:         c.updates.Load(),
		SkippedUpdates:  c.skipped.Load(),
		AcksSent:        c.acksSent.Load(),
		ResyncRequests:  c.resyncRequests.Load(),
		EchoedUpdates:   c.echoed.Load(),
		Teaches:         c.teaches.Load(),
		OutputUpdates:   c.outUpdates.Load(),
		TaughtUpdates:   c.taught.Load(),
		AcksReceived:    c.acksRecv.Load(),
		Markers:         c.markers.Load(),
		Revocations:     c.revocations.Load(),
		LastDeadline:    c.markerDL.Load(),
		LiveMarker:      c.markerPending.Load() || c.liveGen.Load() > c.revWritten.Load(),
		Generation:      c.genA.Load(),
		Rotations:       c.rotations.Load(),
		Refreshes:       c.refreshes.Load(),
		OutDeferred:     c.deferred.Load(),
	}
	if q := c.wqA.Load(); q != nil {
		s.OutQueued, s.MaxOutQueued = q.queued.Load(), q.maxSeen.Load()
	}
	if n := c.lastRxMono.Load(); n != 0 {
		s.LastRx = monoBase.Add(time.Duration(n))
	}
	if n := c.markerNano.Load(); n != 0 {
		s.LastMarker = time.Unix(0, n)
	}
	for _, t := range c.outs {
		s.Outputs = append(s.Outputs, OutputStats{
			Table:     t.def.Name,
			LocalID:   t.id,
			SourceID:  peermsg.RemoteTableID(t.sourceID.Load()),
			Sent:      t.sentA.Load(),
			LastSent:  peermsg.UpdateID(t.lastSentA.Load()),
			Acked:     t.acked.Load(),
			LastAcked: peermsg.UpdateID(t.lastAcked.Load()),
		})
	}
	return s
}

// Close closes the underlying connection. The caller may call it as soon
// as Run returns, before reporting the session's end, so that the source
// sees the close at once.
func (c *Conn) Close() error { return c.nc.Close() }

// Nudge asks an established session to send a heartbeat within d, even
// if it sent something more recently than the heartbeat interval. It may
// be called from any goroutine.
//
// Stock HAProxy clears a peer's liveness flag whenever any of its
// connections to that peer is released, including an abandoned connection
// attempt, and closes the peer's current session as dead if no message
// arrives before its next liveness check. The caller nudges the session
// right after closing such an attempt, so that a message follows the
// release.
func (c *Conn) Nudge(d time.Duration) {
	at := time.Now().Add(d).UnixNano()
	for {
		old := c.nudgeAt.Load()
		if old != 0 && old <= at {
			break
		}
		if c.nudgeAt.CompareAndSwap(old, at) {
			break
		}
	}
	time.AfterFunc(d, func() { _ = c.nc.SetReadDeadline(aLongTimeAgo) })
}

// RemoteAddr returns the connection's remote address.
func (c *Conn) RemoteAddr() string { return c.nc.RemoteAddr().String() }

// watch interrupts blocked I/O when ctx ends. Every read and write sets
// its deadline first and checks ctx afterwards, so an interruption is
// never overwritten unnoticed.
func (c *Conn) watch(ctx context.Context) func() bool {
	return context.AfterFunc(ctx, func() { _ = c.nc.SetDeadline(aLongTimeAgo) })
}

// write sends b: during the handshake directly, with deadline; once
// established through the send queue, whose writer applies the idle
// timeout to each write.
func (c *Conn) write(ctx context.Context, b []byte, deadline time.Time) error {
	if c.wq != nil {
		return c.enqueue(b)
	}
	if err := c.nc.SetWriteDeadline(deadline); err != nil {
		return wrapCause(ErrIO, err, "set write deadline")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := c.nc.Write(b); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return wrapCause(ErrIO, err, "write")
	}
	c.lastTx = time.Now()
	return nil
}

// fill performs one read into the decoder. It returns errDeadline when
// the deadline passed, and nil at end of input (setting c.eof).
func (c *Conn) fill(ctx context.Context, deadline time.Time) error {
	if err := c.nc.SetReadDeadline(deadline); err != nil {
		return wrapCause(ErrIO, err, "set read deadline")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if at := c.nudgeAt.Load(); at != 0 && at <= time.Now().UnixNano() {
		return errDeadline // a Nudge fired before this deadline was set
	}
	if c.outWake.Load() {
		return errDeadline // the output changed before this deadline was set
	}
	if c.wq != nil && c.wq.failed.Load() {
		return errDeadline // the writer failed before this deadline was set
	}
	n, err := c.nc.Read(c.buf[:min(len(c.buf), c.dec.Free())])
	if n > 0 {
		c.rxTime = time.Now()
		if ferr := c.dec.Feed(c.buf[:n]); ferr != nil {
			return wrapCause(ErrIO, ferr, "buffer input")
		}
	}
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.Is(err, os.ErrDeadlineExceeded):
		return errDeadline
	case errors.Is(err, io.EOF):
		c.eof = true
		c.dec.CloseInput()
		return nil
	default:
		return wrapCause(ErrIO, err, "read")
	}
}

// line returns a copy of the next handshake line.
func (c *Conn) line(ctx context.Context, what string) ([]byte, error) {
	for {
		l, err := c.dec.NextLine()
		switch {
		case err == nil:
			return bytes.Clone(l), nil
		case errors.Is(err, peerwire.ErrNeedMore):
		case errors.Is(err, io.EOF), errors.Is(err, peerwire.ErrTruncated):
			return nil, &HandshakeError{Reason: "connection closed while reading the " + what}
		case errors.Is(err, peerwire.ErrHandshakeTooLarge):
			return nil, c.reject(ctx, peerwire.StatusProtocolError, "handshake too large", err)
		default:
			return nil, &HandshakeError{Reason: "reading the " + what, Err: err}
		}
		if err := c.fill(ctx, c.deadline); err != nil {
			if errors.Is(err, errDeadline) {
				return nil, &HandshakeError{Reason: "timed out reading the " + what}
			}
			if ctx.Err() != nil {
				return nil, err
			}
			return nil, &HandshakeError{Reason: "reading the " + what, Err: err}
		}
	}
}

// reject sends a status line (best effort) and returns the error.
func (c *Conn) reject(ctx context.Context, code peerwire.StatusCode, reason string, cause error) error {
	c.phase.Store(uint32(PhaseClosed))
	if b, err := peerwire.AppendStatus(nil, code); err == nil {
		_ = c.write(ctx, b, minTime(c.deadline, time.Now().Add(errorTimeout)))
	}
	return &HandshakeError{Code: code, Sent: true, Reason: reason, Err: cause}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// ReadHello reads and validates the hello of an accepted connection.
// Like HAProxy, it judges each line as it arrives and answers the first
// problem with its status code: 501 for a malformed protocol line or
// sender line, 502 for any version but 2.1, 503 when the hello is not
// addressed to LocalPeer, and 504 when known rejects the sender's name.
// It does not send a status on success; call AcceptHello, or close the
// connection to refuse it silently.
//
// Version 2.0, which HAProxy accepts as a downgrade without timed
// updates, is refused: the lifetime semantics this service relies on need
// them, and stock HAProxy announces 2.1.
func (c *Conn) ReadHello(ctx context.Context, known func(name string) bool) (peerwire.Hello, error) {
	defer c.watch(ctx)()
	c.phase.Store(uint32(PhaseHello))
	first, err := c.line(ctx, "protocol line")
	if err != nil {
		return peerwire.Hello{}, err
	}
	if !bytes.HasPrefix(first, []byte(peerwire.ProtocolName+" ")) {
		return peerwire.Hello{}, c.reject(ctx, peerwire.StatusProtocolError, "not a peers protocol hello", nil)
	}
	v, err := peerwire.ParseVersionLine(first)
	if err != nil {
		return peerwire.Hello{}, c.reject(ctx, peerwire.StatusBadVersion, "unparsable protocol version", err)
	}
	if v != peerwire.ProtocolVersion {
		return peerwire.Hello{}, c.reject(ctx, peerwire.StatusBadVersion,
			fmt.Sprintf("unsupported protocol version %v, want %v", v, peerwire.ProtocolVersion), nil)
	}
	second, err := c.line(ctx, "local peer line")
	if err != nil {
		return peerwire.Hello{}, err
	}
	if string(second) != c.opts.LocalPeer {
		return peerwire.Hello{}, c.reject(ctx, peerwire.StatusHostMismatch,
			fmt.Sprintf("hello addressed to %q, not %q", boundedName(second), c.opts.LocalPeer), nil)
	}
	third, err := c.line(ctx, "sender line")
	if err != nil {
		return peerwire.Hello{}, err
	}
	name, pid, rel, err := peerwire.ParseSenderLine(third)
	if err != nil {
		return peerwire.Hello{}, c.reject(ctx, peerwire.StatusProtocolError, "malformed sender line", err)
	}
	if !known(name) {
		return peerwire.Hello{}, c.reject(ctx, peerwire.StatusUnknownPeer,
			fmt.Sprintf("sender %q is not a configured source", boundedName([]byte(name))), nil)
	}
	return peerwire.Hello{
		Version: v, RemotePeer: c.opts.LocalPeer, LocalPeer: name, PID: pid, RelativePID: rel,
	}, nil
}

// boundedName quotes at most 64 bytes of a peer-supplied name.
func boundedName(b []byte) string {
	const limit = 64
	if len(b) > limit {
		return string(b[:limit]) + "..."
	}
	return string(b)
}

// AcceptHello sends the success status after ReadHello, establishing the
// session.
func (c *Conn) AcceptHello(ctx context.Context) error {
	defer c.watch(ctx)()
	b, err := peerwire.AppendStatus(nil, peerwire.StatusSucceeded)
	if err != nil {
		return err
	}
	if err := c.write(ctx, b, c.deadline); err != nil {
		c.phase.Store(uint32(PhaseClosed))
		if ctx.Err() != nil {
			return err
		}
		return &HandshakeError{Reason: "sending the success status", Err: err}
	}
	c.phase.Store(uint32(PhaseEstablished))
	return nil
}

// Greet sends this side's hello to the source named remotePeer on a
// dialed connection and reads the status. Any status but 200 fails with a
// *HandshakeError carrying the code.
func (c *Conn) Greet(ctx context.Context, remotePeer string) error {
	defer c.watch(ctx)()
	c.phase.Store(uint32(PhaseStatus))
	hello, err := peerwire.AppendHello(nil, peerwire.Hello{
		Version: peerwire.ProtocolVersion, RemotePeer: remotePeer, LocalPeer: c.opts.LocalPeer,
		PID: c.opts.PID, RelativePID: 1,
	})
	if err != nil {
		c.phase.Store(uint32(PhaseClosed))
		return &HandshakeError{Reason: "encoding the hello", Err: err}
	}
	if werr := c.write(ctx, hello, c.deadline); werr != nil {
		c.phase.Store(uint32(PhaseClosed))
		if ctx.Err() != nil {
			return werr
		}
		return &HandshakeError{Reason: "sending the hello", Err: werr}
	}
	line, err := c.line(ctx, "status line")
	if err != nil {
		c.phase.Store(uint32(PhaseClosed))
		return err
	}
	code, err := peerwire.ParseStatusLine(line)
	if err != nil {
		c.phase.Store(uint32(PhaseClosed))
		return &HandshakeError{Reason: "malformed status line", Err: err}
	}
	if code != peerwire.StatusSucceeded {
		c.phase.Store(uint32(PhaseClosed))
		return &HandshakeError{Code: code, Reason: "source refused the hello"}
	}
	c.phase.Store(uint32(PhaseEstablished))
	return nil
}
