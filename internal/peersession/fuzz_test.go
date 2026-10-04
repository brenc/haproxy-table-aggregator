package peersession_test

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

var fuzzDef = peermsg.Definition{
	Name: "t_in", KeyType: peermsg.KeyTypeIPv6, Expiry: 30000,
	Fields: []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: 10000}},
}

func fuzzSeeds(tb testing.TB) [][]byte {
	tb.Helper()
	ctl := func(t peerwire.MessageType) []byte {
		b, _ := peerwire.AppendFrame(nil, peerwire.ClassControl, t, nil)
		return b
	}
	def, err := peermsg.AppendDefinition(nil, 1, fuzzDef)
	if err != nil {
		tb.Fatal(err)
	}
	key, _ := peermsg.KeyFromAddr(netip.MustParseAddr("2001:db8::"))
	upd, err := peermsg.AppendUpdate(nil, fuzzDef, peermsg.Update{ID: 5, ExplicitID: true, Key: key, Values: []peermsg.Value{
		{Type: peermsg.DataHTTPReqCnt, Uint: 1}, {Type: peermsg.DataHTTPReqRate, Freq: peermsg.FreqCounter{Curr: 1}},
	}})
	if err != nil {
		tb.Fatal(err)
	}
	inc, err := peermsg.AppendUpdate(nil, fuzzDef, peermsg.Update{Key: key, Values: []peermsg.Value{
		{Type: peermsg.DataHTTPReqCnt, Uint: 2}, {Type: peermsg.DataHTTPReqRate, Freq: peermsg.FreqCounter{Curr: 2}},
	}})
	if err != nil {
		tb.Fatal(err)
	}
	outDef := output.Definition("t_out", 30000)
	odef, err := peermsg.AppendDefinition(nil, 2, outDef)
	if err != nil {
		tb.Fatal(err)
	}
	echo, err := peermsg.AppendUpdate(nil, outDef, peermsg.Update{ID: 9, ExplicitID: true, Key: key, Values: []peermsg.Value{
		{Type: peermsg.DataGPT, Array: []uint32{1, 2, 0, 0}},
	}})
	if err != nil {
		tb.Fatal(err)
	}
	ack, err := peermsg.AppendAck(nil, 1, 1)
	if err != nil {
		tb.Fatal(err)
	}
	cat := func(parts ...[]byte) []byte {
		var out []byte
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	return [][]byte{
		nil,
		ctl(peerwire.ControlHeartbeat),
		cat(ctl(peerwire.ControlResyncRequest), ctl(peerwire.ControlResyncConfirm), ctl(peerwire.ControlResyncFinished)),
		cat(def, upd, inc, ctl(peerwire.ControlResyncPartial)),
		cat(def, upd, def, inc),
		{0x00, 0x07},
		{0x0a, 0x82, 0x01, 0x01},
		{0xff, 0x00},
		cat(ctl(peerwire.ControlResyncConfirm)),
		cat(def, upd, odef, echo, ack, ctl(peerwire.ControlResyncRequest), ctl(peerwire.ControlResyncConfirm)),
		cat(odef, echo, def, inc),
	}
}

// FuzzRun feeds arbitrary bytes to an established session that also
// publishes one output table. Run must end, without panicking, with an
// error of one of the package's classes once the input ends, and it may
// acknowledge only updates the sink accepted or updates of the source's
// copy of the output table, which never reach the sink.
func FuzzRun(f *testing.F) {
	for _, s := range fuzzSeeds(f) {
		f.Add(s, false)
		f.Add(s, true)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		f.Fatal(err)
	}
	store, err := output.NewStore([]output.Table{{Name: "t_out", Kind: output.KindAggregate, Expiry: 30000}},
		output.StoreOptions{})
	if err != nil {
		f.Fatal(err)
	}
	okey, _ := peermsg.KeyFromAddr(netip.MustParseAddr("2001:db8::"))
	if err := store.Set("t_out", okey, output.AggregateValues(1)); err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = ln.Close() })
	f.Fuzz(func(t *testing.T, input []byte, rejectUpdates bool) {
		var d net.Dialer
		cc, err := d.DialContext(context.Background(), "tcp4", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		client := cc.(*net.TCPConn) //nolint:forcetypeassert // tcp4 dials return *net.TCPConn.
		defer func() { _ = client.Close() }()
		server, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = server.Close() }()
		pc, err := peersession.NewConn(server, peersession.Options{
			LocalPeer: "agg", Tables: map[string]peersession.TableSpec{"t_in": {Period: 10000}},
			Heartbeat: time.Second, IdleTimeout: 5 * time.Second, HandshakeTimeout: time.Second,
			MaxTables: 4, RequestResync: true, Output: store,
		})
		if err != nil {
			t.Fatal(err)
		}
		accepted := echoes(input)
		sink := func(_ context.Context, ev peersession.Event) error {
			if u, ok := ev.(peersession.EntryUpdated); ok {
				if rejectUpdates {
					return errors.New("rejected")
				}
				accepted[ackKey{u.ID, u.Update.ID}] = true
			}
			return nil
		}
		hello := make(chan error, 1)
		go func() {
			b, _ := peerwire.AppendHello(nil, peerwire.Hello{
				Version: peerwire.ProtocolVersion, RemotePeer: "agg", LocalPeer: "a", PID: 1, RelativePID: 1,
			})
			if _, err := client.Write(b); err != nil {
				hello <- err
				return
			}
			buf := make([]byte, 4)
			_, err := client.Read(buf)
			hello <- err
		}()
		ctx := context.Background()
		if _, err := pc.ReadHello(ctx, func(string) bool { return true }); err != nil {
			t.Fatal(err)
		}
		if err := pc.AcceptHello(ctx); err != nil {
			t.Fatal(err)
		}
		if err := <-hello; err != nil {
			t.Fatal(err)
		}
		// The peer side: write the input, then close; collect what the
		// session sends meanwhile.
		sent := make(chan []byte, 1)
		go func() {
			var out []byte
			buf := make([]byte, 4096)
			for {
				n, err := client.Read(buf)
				out = append(out, buf[:n]...)
				if err != nil {
					sent <- out
					return
				}
			}
		}()
		go func() {
			_, _ = client.Write(input)
			_ = client.CloseWrite() // end of input
		}()
		done := make(chan error, 1)
		go func() { done <- pc.Run(ctx, sink) }()
		var runErr error
		select {
		case runErr = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not end")
		}
		_ = server.Close()
		out := <-sent
		classes := []error{
			peersession.ErrProtocol, peersession.ErrSchema, peersession.ErrLimit, peersession.ErrIdle,
			peersession.ErrClosed, peersession.ErrPeerError, peersession.ErrEventRejected, peersession.ErrIO,
		}
		known := false
		for _, c := range classes {
			known = known || errors.Is(runErr, c)
		}
		if !known && runErr != nil {
			t.Fatalf("Run ended with unclassified error %v", runErr)
		}
		checkAcks(t, out, accepted)
	})
}

// ackKey names one update of one table: the table ID is the source's,
// which an acknowledgement carries back.
type ackKey struct {
	table  peermsg.RemoteTableID
	update peermsg.UpdateID
}

// echoes returns every update of the output table t_out that input
// carries, decoded as a fresh session would: the session acknowledges
// those without delivering them. It over-approximates (it continues past
// errors that would end the session), which only widens what checkAcks
// allows for output-table updates.
func echoes(input []byte) map[ackKey]bool {
	out := map[ackKey]bool{}
	d, err := peerwire.NewDecoder(peerwire.DefaultLimits())
	if err != nil || d.Feed(input[:min(len(input), d.Free())]) != nil {
		return out
	}
	d.CloseInput()
	in := peermsg.NewInbound(4)
	for {
		fr, err := d.NextFrame()
		if err != nil {
			return out
		}
		if msg, err := in.Decode(fr, time.Time{}); err == nil {
			if u, ok := msg.(peermsg.UpdateMessage); ok && u.Table == "t_out" {
				out[ackKey{u.ID, u.Update.ID}] = true
			}
		}
	}
}

// checkAcks decodes what the session sent and requires every
// acknowledgement to name a table and update the sink accepted together.
func checkAcks(t *testing.T, out []byte, accepted map[ackKey]bool) {
	t.Helper()
	d, err := peerwire.NewDecoder(peerwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Feed(out[min(len(out), 4):]); err != nil { // skip "200\n"
		t.Fatal(err)
	}
	d.CloseInput()
	for {
		fr, err := d.NextFrame()
		if err != nil {
			return
		}
		if fr.Class == peerwire.ClassStickTable && fr.Type == peerwire.StickTableAck {
			id, upd, err := peermsg.DecodeAck(fr.Body)
			if err != nil {
				t.Fatalf("session sent a malformed ack: %v", err)
			}
			// The ack's table ID is the source's own (LocalTableID from
			// the source's point of view, RemoteTableID from ours).
			if !accepted[ackKey{peermsg.RemoteTableID(id), upd}] {
				t.Fatalf("session acknowledged table %d update %d, which the sink did not accept", id, upd)
			}
		}
	}
}
