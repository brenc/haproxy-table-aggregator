package peersession_test

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// TestSlowSinkIsNotIdle delivers a burst of updates in one write to a sink
// that takes most of the idle timeout in total several times over, while
// the source keeps heartbeating: the session must not end as idle. Stats
// LastRx meanwhile keeps the time the burst was read, so a readiness check
// on its age sees the backlog that the idle timer deliberately ignores.
func TestSlowSinkIsNotIdle(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	client, err := (&net.Dialer{}).DialContext(context.Background(), "tcp4", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	go func() { // drain the session's output
		buf := make([]byte, 4096)
		for {
			if _, rerr := client.Read(buf); rerr != nil {
				return
			}
		}
	}()

	pc, err := peersession.NewConn(server, peersession.Options{
		LocalPeer: "agg", Tables: map[string]peersession.TableSpec{"t_in": {Period: 10000}},
		Heartbeat: 300 * time.Millisecond, IdleTimeout: time.Second, HandshakeTimeout: time.Second,
		MaxTables: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	hello, _ := peerwire.AppendHello(nil, peerwire.Hello{
		Version: peerwire.ProtocolVersion, RemotePeer: "agg", LocalPeer: "a", PID: 1, RelativePID: 1,
	})
	if _, err = client.Write(hello); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err = pc.ReadHello(ctx, func(string) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if err = pc.AcceptHello(ctx); err != nil {
		t.Fatal(err)
	}

	const n = 10
	burst, err := peermsg.AppendDefinition(nil, 1, fuzzDef)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := peermsg.KeyFromAddr(netip.MustParseAddr("2001:db8::"))
	for i := range n {
		burst, err = peermsg.AppendUpdate(burst, fuzzDef, peermsg.Update{
			ID: peermsg.UpdateID(i), ExplicitID: true,
			Key: key, Values: []peermsg.Value{
				{Type: peermsg.DataHTTPReqCnt, Uint: 1}, {Type: peermsg.DataHTTPReqRate, Freq: peermsg.FreqCounter{Curr: 1}},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.Write(burst); err != nil {
		t.Fatal(err)
	}
	hb, _ := peerwire.AppendFrame(nil, peerwire.ClassControl, peerwire.ControlHeartbeat, nil)
	go func() {
		for ctx.Err() == nil {
			if _, err := client.Write(hb); err != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	written := time.Now()
	accepted := make(chan struct{}, n)
	lags := make(chan time.Duration, n)
	done := make(chan error, 1)
	go func() {
		done <- pc.Run(ctx, func(_ context.Context, ev peersession.Event) error {
			if _, ok := ev.(peersession.EntryUpdated); ok {
				lags <- time.Since(pc.Stats().LastRx)
				time.Sleep(300 * time.Millisecond) // 10 x 300 ms = 3 idle timeouts
				accepted <- struct{}{}
			}
			return nil
		})
	}()
	deadline := time.After(10 * time.Second)
	for i := range n {
		select {
		case <-accepted:
		case err := <-done:
			t.Fatalf("Run ended after %d updates: %v", i, err)
		case <-deadline:
			t.Fatal("updates not delivered")
		}
	}
	var lag time.Duration
	for range n {
		lag = <-lags
	}
	if want := time.Duration(n-1) * 300 * time.Millisecond; lag < want-50*time.Millisecond ||
		lag > time.Since(written) {
		t.Fatalf("LastRx was %v old at the last update of the burst, want about %v", lag, want)
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run ended with %v", err)
	}
}
