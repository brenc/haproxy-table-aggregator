package peertest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// IOTimeout bounds every read, write, dial, and runtime command.
const IOTimeout = 5 * time.Second

// NewCapture returns a capture for one connection to h, with the case
// name, description, and h's identity as metadata.
func NewCapture(name, description string, h *lab.Custom) *Capture {
	c := &Capture{}
	c.Set("format", Format)
	c.Set("case", name)
	c.Set("description", description)
	c.Set("haproxy-version", h.Record.HAProxyVersion)
	c.Set("haproxy-sha256", h.Record.HAProxySHA256)
	c.Set("haproxy-pid", strconv.Itoa(h.Node.PID()))
	c.Set("go-version", h.Record.GoVersion)
	c.Set("captured-at", h.Record.StartedAt.Format(time.RFC3339))
	return c
}

// Comments returns a fixture's comment lines: header, then the HAProxy
// configuration indented by two spaces.
func Comments(header []string, config string) []string {
	out := append([]string(nil), header...)
	for _, line := range strings.Split(strings.TrimRight(config, "\n"), "\n") {
		out = append(out, "  "+line)
	}
	return out
}

// Conn drives one recorded connection through a peerwire.Decoder: every
// byte written or read is added to the capture.
type Conn struct {
	tb    testing.TB
	conn  net.Conn
	c     *Capture
	d     *peerwire.Decoder
	lines int
	buf   []byte
	eof   bool
}

// Wrap records conn into c.
func Wrap(tb testing.TB, conn net.Conn, c *Capture) *Conn {
	tb.Helper()
	d, err := peerwire.NewDecoder(peerwire.DefaultLimits())
	if err != nil {
		tb.Fatal(err)
	}
	return &Conn{tb: tb, conn: conn, c: c, d: d, buf: make([]byte, 4096)}
}

// Dial connects to addr and records the connection into c.
func Dial(tb testing.TB, addr string, c *Capture) *Conn {
	tb.Helper()
	d := net.Dialer{Timeout: IOTimeout}
	conn, err := d.DialContext(context.Background(), "tcp4", addr)
	if err != nil {
		tb.Fatalf("dial %s: %v", addr, err)
	}
	return Wrap(tb, conn, c)
}

// Capture returns the capture being recorded.
func (l *Conn) Capture() *Capture { return l.c }

// Send writes b and records it.
func (l *Conn) Send(b []byte) {
	l.tb.Helper()
	l.c.Add(KindTx, b)
	if err := l.conn.SetWriteDeadline(time.Now().Add(IOTimeout)); err != nil {
		l.tb.Fatal(err)
	}
	if _, err := l.conn.Write(b); err != nil {
		l.tb.Fatalf("write: %v", err)
	}
}

// fill performs one read into the decoder, closing its input at EOF. It
// returns a read error other than EOF, such as a deadline expiry.
func (l *Conn) fill(deadline time.Time) error {
	l.tb.Helper()
	if err := l.conn.SetReadDeadline(deadline); err != nil {
		l.tb.Fatal(err)
	}
	n, err := l.conn.Read(l.buf[:min(len(l.buf), l.d.Free())])
	l.c.Add(KindRx, l.buf[:n])
	if ferr := l.d.Feed(l.buf[:n]); ferr != nil {
		l.tb.Fatalf("feed: %v", ferr)
	}
	switch {
	case errors.Is(err, io.EOF):
		l.c.Note("HAProxy closed the connection")
		l.eof = true
		l.d.CloseInput()
	case err != nil:
		return fmt.Errorf("read: %w", err)
	}
	return nil
}

// Line returns the next handshake line.
func (l *Conn) Line() []byte {
	l.tb.Helper()
	deadline := time.Now().Add(IOTimeout)
	for {
		line, err := l.d.NextLine()
		if err == nil {
			l.lines++
			return line
		}
		if !errors.Is(err, peerwire.ErrNeedMore) {
			l.tb.Fatalf("line %d: %v", l.lines+1, err)
		}
		if err := l.fill(deadline); err != nil {
			l.tb.Fatalf("line %d: %v", l.lines+1, err)
		}
	}
}

// Frame returns the next frame, or io.EOF when HAProxy closed cleanly.
// The frame aliases the decoder's buffer until the next read.
func (l *Conn) Frame() (peerwire.Frame, error) {
	l.tb.Helper()
	return l.FrameWithin(IOTimeout)
}

// FrameWithin is Frame with an explicit read timeout. A read failure,
// including the timeout expiring, is returned as an error.
func (l *Conn) FrameWithin(timeout time.Duration) (peerwire.Frame, error) {
	l.tb.Helper()
	return l.FrameBy(time.Now().Add(timeout))
}

// FrameBy is Frame with an absolute read deadline.
func (l *Conn) FrameBy(deadline time.Time) (peerwire.Frame, error) {
	l.tb.Helper()
	for {
		f, err := l.d.NextFrame()
		if !errors.Is(err, peerwire.ErrNeedMore) {
			return f, err
		}
		if err := l.fill(deadline); err != nil {
			return peerwire.Frame{}, err
		}
	}
}

// UntilControl reads frames until a control message of type typ.
func (l *Conn) UntilControl(typ peerwire.MessageType) {
	l.tb.Helper()
	for {
		f, err := l.Frame()
		if err != nil {
			l.tb.Fatalf("waiting for control message %d: %v", typ, err)
		}
		if f.Class == peerwire.ClassControl && f.Type == typ {
			return
		}
	}
}

// ExpectEOF reads frames until HAProxy closes the connection.
func (l *Conn) ExpectEOF() {
	l.tb.Helper()
	for {
		_, err := l.Frame()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			l.tb.Fatalf("waiting for close: %v", err)
		}
	}
}

// Finish closes the connection and drops received bytes past the last
// consumed frame.
func (l *Conn) Finish() *Capture {
	if !l.eof {
		l.c.Note("test peer closed the connection")
	}
	_ = l.conn.Close()
	if n := l.c.TrimRx(l.d.Offset()); n > 0 {
		l.c.Set("rx-trimmed", strconv.Itoa(n))
	}
	return l.c
}

// Runtime runs a runtime API command on h, records it as a note, and
// returns the reply.
func Runtime(tb testing.TB, h *lab.Custom, c *Capture, cmd string) string {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), IOTimeout)
	defer cancel()
	c.Note("runtime: %s", cmd)
	reply, err := h.Node.Runtime(ctx, cmd)
	if err != nil {
		tb.Fatalf("runtime %q: %v", cmd, err)
	}
	return reply
}

// RuntimeSilent runs a command that must reply with nothing but blank
// lines.
func RuntimeSilent(tb testing.TB, h *lab.Custom, c *Capture, cmd string) {
	tb.Helper()
	if reply := Runtime(tb, h, c, cmd); strings.TrimSpace(reply) != "" {
		tb.Fatalf("runtime %q: %q", cmd, reply)
	}
}
