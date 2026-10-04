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

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// Options configures one session. Durations must be positive.
type Options struct {
	// LocalPeer is this side's peer name.
	LocalPeer string
	// Tables are the input tables by HAProxy table name. Every other
	// table the source announces is ignored.
	Tables map[string]TableSpec
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
	// LearnPartial: the source replied "partial"; confirmed.
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
	LastRx          time.Time
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
	confirms uint32
	learn    LearnState
	lastRx   time.Time
	lastTx   time.Time
	rxTime   time.Time // when the last read returned data
	wbuf     []byte

	phase        atomic.Uint32
	learnA       atomic.Uint32
	confirmsA    atomic.Uint32
	rxMessages   atomic.Uint64
	rxHeartbeats atomic.Uint64
	txHeartbeats atomic.Uint64
	updates      atomic.Uint64
	skipped      atomic.Uint64
	acksSent     atomic.Uint64
	lastRxNano   atomic.Int64
	nudgeAt      atomic.Int64 // UnixNano of a requested early heartbeat, or 0
}

// tableState is this session's view of one table ID the source bound.
type tableState struct {
	name    string
	input   bool
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
	return &Conn{
		nc:       nc,
		opts:     opts,
		dec:      dec,
		buf:      make([]byte, readSize),
		deadline: time.Now().Add(opts.HandshakeTimeout),
		in:       peermsg.NewInbound(opts.MaxTables),
		tables:   map[peermsg.RemoteTableID]*tableState{},
	}, nil
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
	}
	if n := c.lastRxNano.Load(); n != 0 {
		s.LastRx = time.Unix(0, n)
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

func (c *Conn) write(ctx context.Context, b []byte, deadline time.Time) error {
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
