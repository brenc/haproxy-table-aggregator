package sources_test

import (
	"context"
	"errors"
	"net"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// inboundManager starts a manager listening for the named inbound sources.
func inboundManager(t *testing.T, names ...string) (*sources.Manager, *recorder, string) {
	t.Helper()
	var srcs []config.Source
	for _, n := range names {
		srcs = append(srcs, config.Source{Name: n})
	}
	ln := listen(t)
	m, r := startManager(t, sources.Options{Config: fastConfig(srcs...), Listener: ln})
	return m, r, ln.Addr().String()
}

func TestHandshakeRejections(t *testing.T) {
	_, r, addr := inboundManager(t, "a")
	cases := []struct {
		name  string
		lines string
		want  peerwire.StatusCode
	}{
		{"not peers", "GET / HTTP/1.1\n", peerwire.StatusProtocolError},
		{"version 2.0", "HAProxyS 2.0\nagg\na 1 1\n", peerwire.StatusBadVersion},
		{"version 2.2", "HAProxyS 2.2\nagg\na 1 1\n", peerwire.StatusBadVersion},
		{"version 3.1", "HAProxyS 3.1\nagg\na 1 1\n", peerwire.StatusBadVersion},
		{"unparsable version", "HAProxyS two\nagg\na 1 1\n", peerwire.StatusBadVersion},
		{"wrong local peer", "HAProxyS 2.1\nother\na 1 1\n", peerwire.StatusHostMismatch},
		{"unknown sender", "HAProxyS 2.1\nagg\nmallory 1 1\n", peerwire.StatusUnknownPeer},
		{"local name as sender", "HAProxyS 2.1\nagg\nagg 1 1\n", peerwire.StatusUnknownPeer},
		{"malformed sender", "HAProxyS 2.1\nagg\na\n", peerwire.StatusProtocolError},
		{"oversized line", strings.Repeat("x", 5000) + "\n", peerwire.StatusProtocolError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := dialFake(t, addr)
			f.t = t
			f.send([]byte(tc.lines))
			if code := f.status(); code != tc.want {
				t.Fatalf("status %d, want %d", code, tc.want)
			}
			if fr := f.expectClose(); len(fr) != 0 {
				t.Fatalf("frames after a refused hello: %v", fr)
			}
		})
	}
	// The status follows the first bad line at once, as in HAProxy.
	f := dialFake(t, addr)
	f.send([]byte("HAProxyS 9.9\n"))
	if code := f.status(); code != peerwire.StatusBadVersion {
		t.Fatalf("status %d after a lone bad version line", code)
	}
	if evs := r.snapshot(); len(evs) != 0 {
		t.Fatalf("refused hellos produced events:\n%s", describe(evs))
	}
	// A valid hello still works afterwards.
	connectFake(t, addr, "a").established()
	r.waitFor("session up", fakeTimeout, upCount("a", 1))
}

func TestHandshakeTimeout(t *testing.T) {
	_, _, addr := inboundManager(t, "a")
	f := dialFake(t, addr)
	f.send([]byte("HAProxyS 2.1\n"))
	start := time.Now()
	if _, ok := f.line(); ok {
		t.Fatal("got a status line for an incomplete hello")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("incomplete hello held for %v", d)
	}
}

func TestOutboundStatus(t *testing.T) {
	srv := startFakeServer(t)
	cfg := fastConfig(config.Source{Name: "a", Address: srv.addr()})
	m, r := startManager(t, sources.Options{Config: cfg})

	// HAProxy refusing the hello (503: wrong peer name on its side).
	f := srv.next(t)
	h := f.accept("a", peerwire.StatusHostMismatch)
	if h.LocalPeer != "agg" || h.Version != peerwire.ProtocolVersion {
		t.Fatalf("daemon hello %+v", h)
	}
	deadline := time.Now().Add(fakeTimeout)
	for {
		var he *peersession.HandshakeError
		if err := status(t, m, "a").LastErr; errors.As(err, &he) && he.Code == peerwire.StatusHostMismatch && !he.Sent {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("last error %v, want received status 503", status(t, m, "a").LastErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The daemon retries with backoff; the next attempt succeeds.
	f = srv.next(t)
	f.accept("a", peerwire.StatusSucceeded)
	f.established()
	evs := r.waitFor("outbound session up", fakeTimeout, upCount("a", 1))
	if d := direction(ups(evs, "a")[0]); d != peersession.Outbound {
		t.Fatalf("direction %v", d)
	}
}

// TestProtocolFailures checks that unknown controls, unknown messages,
// malformed frames, and state violations end only the affected session,
// after the matching error message.
func TestProtocolFailures(t *testing.T) {
	frame := func(class peerwire.MessageClass, typ peerwire.MessageType, body []byte) []byte {
		b, err := peerwire.AppendFrame(nil, class, typ, body)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	defBody := func() []byte {
		b, err := peermsg.AppendDefinition(nil, 1, inDef)
		if err != nil || int(b[2]) != len(b)-3 {
			t.Fatalf("definition frame %x: %v", b, err)
		}
		return b[3:]
	}()
	cases := []struct {
		name    string
		send    []byte
		errType peerwire.MessageType
	}{
		{"unknown control", frame(peerwire.ClassControl, 7, nil), peerwire.ErrorTypeProtocol},
		{"unknown class", frame(5, 1, nil), peerwire.ErrorTypeProtocol},
		{"unknown table message", frame(peerwire.ClassStickTable, 0x87, []byte{1}), peerwire.ErrorTypeProtocol},
		{"reserved class", []byte{255, 0}, peerwire.ErrorTypeProtocol},
		{"long length encoding", []byte{10, 0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, peerwire.ErrorTypeProtocol},
		{"oversized message", append([]byte{10, 0x82}, peerwire.AppendUint(nil, 20000)...), peerwire.ErrorTypeSizeLimit},
		{"malformed definition", frame(peerwire.ClassStickTable, peerwire.StickTableDefine, []byte{1, 200}), peerwire.ErrorTypeProtocol},
		{"trailing definition bytes", frame(peerwire.ClassStickTable, peerwire.StickTableDefine, append(defBody, 0)), peerwire.ErrorTypeProtocol},
		{"update without table", frame(peerwire.ClassStickTable, peerwire.StickTableUpdate, []byte{0, 0, 0, 1}), peerwire.ErrorTypeProtocol},
		{"switch to unknown table", frame(peerwire.ClassStickTable, peerwire.StickTableSwitch, []byte{9}), peerwire.ErrorTypeProtocol},
		{"ack for unannounced table", frame(peerwire.ClassStickTable, peerwire.StickTableAck, []byte{1, 0, 0, 0, 1}), peerwire.ErrorTypeProtocol},
		{"unsolicited confirm", frame(peerwire.ClassControl, peerwire.ControlResyncConfirm, nil), peerwire.ErrorTypeProtocol},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, r, addr := inboundManager(t, "a", "b")
			bystander := connectFake(t, addr, "b")
			bystander.t = t
			bystander.established()
			f := connectFake(t, addr, "a")
			f.t = t
			f.established()
			f.send(tc.send)
			got := f.expectClose()
			if len(got) != 1 || got[0].Class != peerwire.ClassError || got[0].Type != tc.errType {
				t.Fatalf("daemon sent %v before closing, want one error message type %d", got, tc.errType)
			}
			r.waitFor("session a down", fakeTimeout, func(evs []sources.Event) bool { return len(downs(evs, "a")) == 1 })
			if err := downs(r.snapshot(), "a")[0].Err; !errors.Is(err, peersession.ErrProtocol) {
				t.Fatalf("session ended with %v, want ErrProtocol", err)
			}
			// The other source's session is untouched.
			bystander.control(peerwire.ControlHeartbeat)
			time.Sleep(100 * time.Millisecond)
			if n := len(downs(r.snapshot(), "b")); n != 0 {
				t.Fatalf("bystander session ended")
			}
		})
	}
}

func TestPeerErrorMessage(t *testing.T) {
	_, r, addr := inboundManager(t, "a")
	f := connectFake(t, addr, "a")
	f.established()
	b, _ := peerwire.AppendFrame(nil, peerwire.ClassError, peerwire.ErrorTypeProtocol, nil)
	f.send(b)
	if got := f.expectClose(); len(got) != 0 {
		t.Fatalf("daemon answered a peer error with %v", got)
	}
	evs := r.waitFor("down", fakeTimeout, func(evs []sources.Event) bool { return len(downs(evs, "a")) == 1 })
	if err := downs(evs, "a")[0].Err; !errors.Is(err, peersession.ErrPeerError) {
		t.Fatalf("ended with %v", err)
	}
}

// TestUpdatesAndAcks checks input events, acknowledgements, and that
// unconfigured tables (of any schema) are ignored and never acknowledged.
func TestUpdatesAndAcks(t *testing.T) {
	m, r, addr := inboundManager(t, "a")
	f := connectFake(t, addr, "a")
	f.established()

	outDef := peermsg.Definition{
		Name: "t_out", KeyType: peermsg.KeyTypeIPv6, Expiry: 30000,
		Fields: []peermsg.Field{{Type: peermsg.DataGPT, ArrayLen: 3}},
	}
	f.define(2, outDef)
	f.update(outDef, 7, "2001:db8::", 0)
	// An unsupported schema (integer keys) on an unconfigured table.
	body := peerwire.AppendUint(nil, 3)
	body = peerwire.AppendUint(body, 5)
	body = append(body, "t_int"...)
	for _, v := range []uint64{2, 4, 0, 30000} {
		body = peerwire.AppendUint(body, v)
	}
	b, _ := peerwire.AppendFrame(nil, peerwire.ClassStickTable, peerwire.StickTableDefine, body)
	f.send(b)
	f.send([]byte{10, 0x80, 4, 0, 0, 0, 1}) // unparsed: the table is ignored

	f.define(1, inDef)
	f.update(inDef, 100, "2001:db8:1::", 3)
	f.update(inDef, 101, "2001:db8:2::", 4)
	// Acknowledgements are cumulative and sent after each read, so the
	// two updates may be acknowledged together or one by one.
	ackUntil := func(want peermsg.UpdateID, allowed ...peermsg.UpdateID) {
		t.Helper()
		for {
			ack := f.expect(peerwire.ClassStickTable, peerwire.StickTableAck)
			id, upd, err := peermsg.DecodeAck(ack.Body)
			if err != nil || id != 1 || (upd != want && !slices.Contains(allowed, upd)) {
				t.Fatalf("ack %d/%d %v, want table 1 update %d", id, upd, err, want)
			}
			if upd == want {
				return
			}
		}
	}
	ackUntil(101, 100)
	// Switching away and back by redefinition keeps the binding.
	f.define(2, outDef)
	f.define(1, inDef)
	f.update(inDef, 102, "2001:db8:1::", 5)
	ackUntil(102)
	evs := r.waitFor("three updates", fakeTimeout, func(evs []sources.Event) bool {
		return count[peersession.EntryUpdated](evs, "a") == 3
	})
	if n := count[peersession.TableDefined](evs, "a"); n != 1 {
		t.Fatalf("%d definitions reported, want 1 (t_in once)\n%s", n, describe(evs))
	}
	for _, ev := range evs {
		if u, ok := ev.Body.(peersession.EntryUpdated); ok && (u.Table != "t_in" || u.ID != 1) {
			t.Fatalf("update event for %s/%d", u.Table, u.ID)
		}
	}
	if s := status(t, m, "a").Stats; s.SkippedUpdates != 2 || s.Updates != 3 || s.AcksSent < 2 || s.AcksSent > 3 {
		t.Fatalf("stats %+v", s)
	}
}

func TestSchemaMismatch(t *testing.T) {
	cases := []struct {
		name string
		def  peermsg.Definition
	}{
		{"period", peermsg.Definition{
			Name: "t_in", KeyType: peermsg.KeyTypeIPv6, Expiry: 30000,
			Fields: []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: 5000}},
		}},
		{"missing count", peermsg.Definition{
			Name: "t_in", KeyType: peermsg.KeyTypeIPv6, Expiry: 30000,
			Fields: []peermsg.Field{{Type: peermsg.DataHTTPReqRate, Period: 10000}},
		}},
		{"output form", peermsg.Definition{
			Name: "t_in", KeyType: peermsg.KeyTypeIPv6, Expiry: 30000,
			Fields: []peermsg.Field{{Type: peermsg.DataGPT, ArrayLen: 1}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, r, addr := inboundManager(t, "a")
			f := connectFake(t, addr, "a")
			f.t = t
			f.established()
			f.define(1, tc.def)
			if got := f.expectClose(); len(got) != 0 {
				t.Fatalf("daemon sent %v", got)
			}
			evs := r.waitFor("down", fakeTimeout, func(evs []sources.Event) bool { return len(downs(evs, "a")) == 1 })
			if err := downs(evs, "a")[0].Err; !errors.Is(err, peersession.ErrSchema) {
				t.Fatalf("ended with %v", err)
			}
		})
	}
	t.Run("unsupported", func(t *testing.T) {
		_, r, addr := inboundManager(t, "a")
		f := connectFake(t, addr, "a")
		f.t = t
		f.established()
		body := peerwire.AppendUint(nil, 1)
		body = peerwire.AppendUint(body, 4)
		body = append(body, "t_in"...)
		for _, v := range []uint64{2, 4, 0, 30000} {
			body = peerwire.AppendUint(body, v)
		}
		b, _ := peerwire.AppendFrame(nil, peerwire.ClassStickTable, peerwire.StickTableDefine, body)
		f.send(b)
		f.expectClose()
		evs := r.waitFor("down", fakeTimeout, func(evs []sources.Event) bool { return len(downs(evs, "a")) == 1 })
		if err := downs(evs, "a")[0].Err; !errors.Is(err, peersession.ErrSchema) || !errors.Is(err, peermsg.ErrKeyType) {
			t.Fatalf("ended with %v", err)
		}
	})
}

// TestQueueFullNoAck holds the application so that the event queue fills:
// the update that cannot be queued is not acknowledged, and the session
// ends.
func TestQueueFullNoAck(t *testing.T) {
	cfg := fastConfig(config.Source{Name: "a"})
	cfg.EventQueue = 1
	ln := listen(t)
	m, err := sources.Start(context.Background(), sources.Options{Config: cfg, Listener: ln})
	if err != nil {
		t.Fatal(err)
	}
	r := &recorder{t: t, changed: make(chan struct{}), done: make(chan struct{}), hold: make(chan struct{})}
	hold := r.hold
	go func() {
		defer close(r.done)
		for ev := range m.Events() {
			r.mu.Lock()
			h := r.hold
			r.mu.Unlock()
			if h != nil {
				<-h
			}
			r.mu.Lock()
			r.evs = append(r.evs, ev)
			close(r.changed)
			r.changed = make(chan struct{})
			r.mu.Unlock()
		}
	}()
	var released sync.Once
	release := func() {
		released.Do(func() {
			r.mu.Lock()
			r.hold = nil
			r.mu.Unlock()
			close(hold)
		})
	}
	defer func() {
		release()
		_ = m.Close()
		<-r.done
	}()

	f := connectFake(t, ln.Addr().String(), "a")
	f.established()
	// SessionUp is held by the application, TableDefined fills the
	// queue, and the update times out.
	f.define(1, inDef)
	f.update(inDef, 1, "2001:db8::", 1)
	if got := f.expectClose(); len(got) != 0 {
		t.Fatalf("daemon sent %v (an ack?) for an update the queue refused", got)
	}
	release()
	evs := r.waitFor("down", fakeTimeout, func(evs []sources.Event) bool { return len(downs(evs, "a")) == 1 })
	err = downs(evs, "a")[0].Err
	if !errors.Is(err, peersession.ErrEventRejected) || !errors.Is(err, sources.ErrQueueFull) {
		t.Fatalf("ended with %v", err)
	}
	if n := count[peersession.EntryUpdated](evs, "a"); n != 0 {
		t.Fatalf("refused update was delivered")
	}
	checkLifecycle(t, evs)
}

func TestResyncControls(t *testing.T) {
	_, r, addr := inboundManager(t, "a")
	f := connectFake(t, addr, "a")
	f.established()
	// The source asks for a resync: nothing to teach, so "finished".
	f.control(peerwire.ControlResyncRequest)
	f.expect(peerwire.ClassControl, peerwire.ControlResyncFinished)
	f.control(peerwire.ControlResyncConfirm)
	// The source answers the daemon's request.
	f.control(peerwire.ControlResyncPartial)
	f.expect(peerwire.ClassControl, peerwire.ControlResyncConfirm)
	evs := r.waitFor("sync event", fakeTimeout, func(evs []sources.Event) bool {
		return count[peersession.SyncFinished](evs, "a") == 1
	})
	for _, ev := range evs {
		if s, ok := ev.Body.(peersession.SyncFinished); ok && !s.Partial {
			t.Fatal("partial reply reported as finished")
		}
	}
	// A second reply was never requested.
	f.control(peerwire.ControlResyncFinished)
	got := f.expectClose()
	if len(got) != 1 || got[0].Class != peerwire.ClassError {
		t.Fatalf("daemon sent %v", got)
	}
}

func TestHeartbeatsAndIdle(t *testing.T) {
	cfg := fastConfig(config.Source{Name: "a"}, config.Source{Name: "b"})
	ln := listen(t)
	m, r := startManager(t, sources.Options{Config: cfg, Listener: ln})
	quiet := connectFake(t, ln.Addr().String(), "a")
	quiet.established()
	live := connectFake(t, ln.Addr().String(), "b")
	live.established()

	// Source b sends only heartbeats and stays up well past the idle
	// timeout; the daemon heartbeats to it.
	stop := time.Now().Add(4 * cfg.IdleTimeout)
	beats := 0
	for time.Now().Before(stop) {
		live.control(peerwire.ControlHeartbeat)
		for {
			fr, err := live.frame(cfg.Heartbeat / 2)
			if err != nil {
				break
			}
			if fr.Class == peerwire.ClassControl && fr.Type == peerwire.ControlHeartbeat {
				beats++
			}
		}
	}
	if s := status(t, m, "b"); !s.Up || s.Session != 1 || s.Stats.RxHeartbeats == 0 {
		t.Fatalf("heartbeating source: %+v", s)
	}
	if beats < int(4*cfg.IdleTimeout/(2*cfg.Heartbeat)) {
		t.Fatalf("daemon sent %d heartbeats over %v", beats, 4*cfg.IdleTimeout)
	}
	// Source a sent nothing and timed out.
	evs := r.waitFor("a idle", fakeTimeout, downWith(peersession.ErrIdle))
	if len(downs(evs, "b")) != 0 {
		t.Fatal("heartbeating source went down")
	}
}

// TestReplacement checks that a new session from a source replaces the
// current one, keeping one logical source with ordered events.
func TestReplacement(t *testing.T) {
	_, r, addr := inboundManager(t, "a")
	first := connectFake(t, addr, "a")
	first.established()
	r.waitFor("first", fakeTimeout, upCount("a", 1))
	second := connectFake(t, addr, "a")
	second.established()
	first.expectClose()
	evs := r.waitFor("second", fakeTimeout, upCount("a", 2))
	u := ups(evs, "a")
	if u[0].Session != 1 || u[1].Session != 2 || len(downs(evs, "a")) != 1 {
		t.Fatalf("events:\n%s", describe(evs))
	}
	if !errors.Is(downs(evs, "a")[0].Err, context.Canceled) {
		t.Fatalf("replaced session ended with %v", downs(evs, "a")[0].Err)
	}
}

// TestStaleInbound checks the collision rule directly: an inbound
// connection, whether accepted before or after the outbound session was
// established, is refused without a status line while that session is
// current, and accepted once it is gone.
func TestStaleInbound(t *testing.T) {
	srv := startFakeServer(t)
	gate := newGate(listen(t))
	cfg := fastConfig(config.Source{Name: "a", Address: srv.addr()})
	in := dialFake(t, gate.Addr().String())
	in.hello(peerwire.ProtocolVersion, "agg", "a")
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		<-gate.accepted
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
	_, r := startManager(t, sources.Options{Config: cfg, Listener: gate, Dial: dial})
	out := srv.next(t)
	out.accept("a", peerwire.StatusSucceeded)
	out.established()
	r.waitFor("outbound up", fakeTimeout, upCount("a", 1))
	close(gate.open)
	if l, ok := in.line(); ok {
		t.Fatalf("stale inbound connection got %q", l)
	}
	time.Sleep(100 * time.Millisecond)
	evs := r.snapshot()
	if len(ups(evs, "a")) != 1 || len(downs(evs, "a")) != 0 {
		t.Fatalf("events:\n%s", describe(evs))
	}
	late := dialFake(t, gate.Addr().String())
	late.hello(peerwire.ProtocolVersion, "agg", "a")
	if l, ok := late.line(); ok {
		t.Fatalf("inbound connection during an outbound session got %q", l)
	}
	// Once the outbound session ends, an inbound one is accepted. The
	// fake server stops answering, so the daemon cannot redial first.
	_ = srv.ln.Close()
	_ = out.conn.Close()
	r.waitFor("outbound down", fakeTimeout, func(evs []sources.Event) bool { return len(downs(evs, "a")) == 1 })
	connectFake(t, gate.Addr().String(), "a").established()
	evs = r.waitFor("inbound up", fakeTimeout, upCount("a", 2))
	if d := direction(ups(evs, "a")[1]); d != peersession.Inbound {
		t.Fatalf("second session %v", d)
	}
}

// TestCloseReleasesEverything closes a manager with established, pending,
// and outbound connections, and checks that every goroutine ends and every
// connection and the listener close.
func TestCloseReleasesEverything(t *testing.T) {
	base := runtime.NumGoroutine()
	srv := startFakeServer(t)
	ln := listen(t)
	addr := ln.Addr().String()
	cfg := fastConfig(config.Source{Name: "a"}, config.Source{Name: "b", Address: srv.addr()})
	cfg.IdleTimeout = time.Minute // nothing may end on its own
	m, err := sources.Start(context.Background(), sources.Options{Config: cfg, Listener: ln})
	if err != nil {
		t.Fatal(err)
	}
	r := record(t, m)
	inbound := connectFake(t, addr, "a")
	inbound.established()
	out := srv.next(t)
	out.accept("b", peerwire.StatusSucceeded)
	out.established()
	pending := dialFake(t, addr)
	pending.send([]byte("HAProxyS 2.1\n"))
	r.waitFor("both up", fakeTimeout, func(evs []sources.Event) bool {
		return len(ups(evs, "a")) == 1 && len(ups(evs, "b")) == 1
	})

	start := time.Now()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Close took %v", d)
	} else {
		t.Logf("Close took %v", d)
	}
	<-r.done
	for name, f := range map[string]*fakePeer{"inbound": inbound, "outbound": out, "pending": pending} {
		f.t = t
		if name == "pending" {
			if _, ok := f.line(); ok {
				t.Fatalf("pending connection got a status")
			}
			continue
		}
		f.expectClose()
	}
	var d net.Dialer
	if c, err := d.DialContext(context.Background(), "tcp", addr); err == nil {
		_ = c.Close()
		t.Fatal("listener still accepting after Close")
	}
	checkLifecycle(t, r.snapshot())
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > base+2 { // the fake server's accept loop and test cleanup
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("goroutines: %d, baseline %d\n%s", runtime.NumGoroutine(), base, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// offHostListener is a listener that claims a non-loopback address.
type offHostListener struct{ net.Listener }

func (offHostListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1} }

// TestStartRefusesOffLoopback checks that Start re-applies the plaintext
// loopback gate to a Config that bypassed validation: nothing is bound or
// dialed.
func TestStartRefusesOffLoopback(t *testing.T) {
	dialed := false
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		dialed = true
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
	cases := map[string]func(*config.Config, *sources.Options){
		"switch unset":    func(c *config.Config, _ *sources.Options) { c.InsecurePlaintextLoopbackLab = false },
		"wildcard listen": func(c *config.Config, _ *sources.Options) { c.Listen = "0.0.0.0:0" },
		"off-host source": func(c *config.Config, _ *sources.Options) {
			c.Sources = []config.Source{{Name: "a", Address: "192.0.2.1:1"}}
		},
		"off-host listener": func(_ *config.Config, o *sources.Options) {
			o.Listener = offHostListener{listen(t)}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := fastConfig(config.Source{Name: "a", Address: "127.0.0.1:1"})
			opts := sources.Options{Dial: dial}
			mutate(&cfg, &opts)
			opts.Config = cfg
			m, err := sources.Start(context.Background(), opts)
			if err == nil {
				_ = m.Close()
				t.Fatal("started")
			}
			if !errors.Is(err, config.ErrInvalid) {
				t.Fatalf("error %v, want ErrInvalid", err)
			}
		})
	}
	if dialed {
		t.Fatal("a refused manager dialed")
	}
}
