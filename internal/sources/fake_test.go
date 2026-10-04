package sources_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// fakePeer is a scripted peers endpoint standing in for HAProxy.
type fakePeer struct {
	t    *testing.T
	conn net.Conn
	dec  *peerwire.Decoder
	buf  []byte
	eof  bool
}

const fakeTimeout = 5 * time.Second

func newFake(t *testing.T, conn net.Conn) *fakePeer {
	t.Helper()
	d, err := peerwire.NewDecoder(peerwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &fakePeer{t: t, conn: conn, dec: d, buf: make([]byte, 4096)}
}

// dialFake connects to addr without sending anything.
func dialFake(t *testing.T, addr string) *fakePeer {
	t.Helper()
	d := net.Dialer{Timeout: fakeTimeout}
	conn, err := d.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return newFake(t, conn)
}

// connectFake dials addr and completes a hello as peer name, addressed to
// "agg", failing unless the daemon answers 200.
func connectFake(t *testing.T, addr, name string) *fakePeer {
	t.Helper()
	f := dialFake(t, addr)
	f.hello(peerwire.ProtocolVersion, "agg", name)
	if code := f.status(); code != peerwire.StatusSucceeded {
		t.Fatalf("hello as %s: status %d", name, code)
	}
	return f
}

func (f *fakePeer) hello(v peerwire.Version, remote, name string) {
	f.t.Helper()
	b, err := peerwire.AppendHello(nil, peerwire.Hello{Version: v, RemotePeer: remote, LocalPeer: name, PID: 42, RelativePID: 1})
	if err != nil {
		f.t.Fatal(err)
	}
	f.send(b)
}

func (f *fakePeer) send(b []byte) {
	f.t.Helper()
	_ = f.conn.SetWriteDeadline(time.Now().Add(fakeTimeout))
	if _, err := f.conn.Write(b); err != nil {
		f.t.Fatalf("fake write: %v", err)
	}
}

func (f *fakePeer) control(typ peerwire.MessageType) {
	f.t.Helper()
	b, err := peerwire.AppendFrame(nil, peerwire.ClassControl, typ, nil)
	if err != nil {
		f.t.Fatal(err)
	}
	f.send(b)
}

func (f *fakePeer) fill(deadline time.Time) error {
	_ = f.conn.SetReadDeadline(deadline)
	n, err := f.conn.Read(f.buf[:min(len(f.buf), f.dec.Free())])
	if ferr := f.dec.Feed(f.buf[:n]); ferr != nil {
		f.t.Fatal(ferr)
	}
	if errors.Is(err, io.EOF) || (err != nil && !errors.Is(err, os.ErrDeadlineExceeded) && n == 0) {
		f.eof = true
		f.dec.CloseInput()
		return nil
	}
	return err
}

// line reads one handshake line; ok is false at end of stream.
func (f *fakePeer) line() ([]byte, bool) {
	f.t.Helper()
	deadline := time.Now().Add(fakeTimeout)
	for {
		l, err := f.dec.NextLine()
		switch {
		case err == nil:
			return bytes.Clone(l), true
		case errors.Is(err, io.EOF), errors.Is(err, peerwire.ErrTruncated):
			return nil, false
		case !errors.Is(err, peerwire.ErrNeedMore):
			f.t.Fatalf("fake line: %v", err)
		}
		if err := f.fill(deadline); err != nil {
			f.t.Fatalf("fake read: %v", err)
		}
	}
}

// status reads the daemon's status line.
func (f *fakePeer) status() peerwire.StatusCode {
	f.t.Helper()
	l, ok := f.line()
	if !ok {
		f.t.Fatal("connection closed before a status line")
	}
	code, err := peerwire.ParseStatusLine(l)
	if err != nil {
		f.t.Fatal(err)
	}
	return code
}

// frame returns the next frame, or io.EOF, or a deadline error.
func (f *fakePeer) frame(timeout time.Duration) (peerwire.Frame, error) {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		fr, err := f.dec.NextFrame()
		if !errors.Is(err, peerwire.ErrNeedMore) {
			if err == nil {
				fr.Body = bytes.Clone(fr.Body)
			}
			return fr, err
		}
		if err := f.fill(deadline); err != nil {
			return peerwire.Frame{}, err
		}
	}
}

// nonHeartbeat returns the next frame that is not a heartbeat.
func (f *fakePeer) nonHeartbeat(timeout time.Duration) (peerwire.Frame, error) {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		fr, err := f.frame(time.Until(deadline))
		if err != nil || fr.Class != peerwire.ClassControl || fr.Type != peerwire.ControlHeartbeat {
			return fr, err
		}
	}
}

// expect requires the next non-heartbeat frame to have class and type.
func (f *fakePeer) expect(class peerwire.MessageClass, typ peerwire.MessageType) peerwire.Frame {
	f.t.Helper()
	fr, err := f.nonHeartbeat(fakeTimeout)
	if err != nil {
		f.t.Fatalf("waiting for message %d/%#02x: %v", class, uint8(typ), err)
	}
	if fr.Class != class || fr.Type != typ {
		f.t.Fatalf("got %v, want class %d type %#02x", fr, class, uint8(typ))
	}
	return fr
}

// expectClose reads until the daemon closes and returns the
// non-heartbeat frames it sent first.
func (f *fakePeer) expectClose() []peerwire.Frame {
	f.t.Helper()
	var out []peerwire.Frame
	for {
		fr, err := f.nonHeartbeat(fakeTimeout)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			f.t.Fatalf("waiting for close: %v", err)
		}
		out = append(out, fr)
	}
}

// established completes the daemon's opening: it expects the daemon's
// resync request.
func (f *fakePeer) established() {
	f.t.Helper()
	f.expect(peerwire.ClassControl, peerwire.ControlResyncRequest)
}

var inDef = peermsg.Definition{
	Name: "t_in", KeyType: peermsg.KeyTypeIPv6, Expiry: 30000,
	Fields: []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: 10000}},
}

func (f *fakePeer) define(id peermsg.LocalTableID, def peermsg.Definition) {
	f.t.Helper()
	b, err := peermsg.AppendDefinition(nil, id, def)
	if err != nil {
		f.t.Fatal(err)
	}
	f.send(b)
}

func (f *fakePeer) update(def peermsg.Definition, id peermsg.UpdateID, key string, cnt uint32) {
	f.t.Helper()
	k, err := peermsg.KeyFromAddr(netip.MustParseAddr(key))
	if err != nil {
		f.t.Fatal(err)
	}
	var vals []peermsg.Value
	for _, fl := range def.Fields {
		switch fl.Type {
		case peermsg.DataHTTPReqCnt:
			vals = append(vals, peermsg.Value{Type: fl.Type, Uint: cnt})
		case peermsg.DataHTTPReqRate:
			vals = append(vals, peermsg.Value{Type: fl.Type, Freq: peermsg.FreqCounter{Age: 5, Curr: cnt}})
		case peermsg.DataGPT:
			vals = append(vals, peermsg.Value{Type: fl.Type, Array: make([]uint32, fl.ArrayLen)})
		default:
			f.t.Fatalf("no value for %v", fl.Type)
		}
	}
	b, err := peermsg.AppendUpdate(nil, def, peermsg.Update{ID: id, ExplicitID: true, Key: k, Values: vals})
	if err != nil {
		f.t.Fatal(err)
	}
	f.send(b)
}

// fakeServer accepts outbound sessions as the named HAProxy peer would.
type fakeServer struct {
	ln    net.Listener
	conns chan *fakePeer
}

func startFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	s := &fakeServer{ln: listen(t), conns: make(chan *fakePeer, 16)}
	go func() {
		for {
			c, err := s.ln.Accept()
			if err != nil {
				return
			}
			s.conns <- newFake(t, c)
		}
	}()
	return s
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

func (s *fakeServer) next(t *testing.T) *fakePeer {
	t.Helper()
	select {
	case f := <-s.conns:
		return f
	case <-time.After(fakeTimeout):
		t.Fatal("no outbound connection")
		return nil
	}
}

// accept reads the daemon's hello and answers code.
func (f *fakePeer) accept(name string, code peerwire.StatusCode) peerwire.Hello {
	f.t.Helper()
	var lines [3][]byte
	for i := range lines {
		l, ok := f.line()
		if !ok {
			f.t.Fatal("closed during hello")
		}
		lines[i] = l
	}
	h, err := peerwire.ParseHello(lines[0], lines[1], lines[2])
	if err != nil {
		f.t.Fatal(err)
	}
	if h.RemotePeer != name {
		f.t.Errorf("hello addressed to %q, want %q", h.RemotePeer, name)
	}
	b, err := peerwire.AppendStatus(nil, code)
	if err != nil {
		f.t.Fatal(err)
	}
	f.send(b)
	return h
}
