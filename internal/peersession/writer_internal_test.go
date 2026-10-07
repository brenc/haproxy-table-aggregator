package peersession

import (
	"context"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
)

// TestDrainWakeNotLost replays, step by step, the interleaving in which
// the writer drains a backlog between Run's backlog check and its consuming
// of the output wake: output must resume at once, with the deferred
// change pending, rather than wait for the next read.
func TestDrainWakeNotLost(t *testing.T) {
	c := &Conn{wq: &sendQueue{}}
	c.wq.queued.Store(SoftBacklog + 1)
	if !c.backlogged() || !c.outBlocked.Load() {
		t.Fatal("backlog not detected and armed")
	}
	// The writer drains below SoftBacklog and wakes Run.
	c.wq.queued.Store(0)
	if !c.outBlocked.CompareAndSwap(true, false) {
		t.Fatal("writer could not disarm")
	}
	c.outWake.Store(true)
	if c.deferOutput(true) {
		t.Fatal("output still deferred after the writer drained and consumed wake was lost")
	}
	if !c.outPending || c.outWake.Load() {
		t.Fatalf("pending %v, wake %v: the deferred change must be published now", c.outPending, c.outWake.Load())
	}

	// Without a drain, output stays deferred and the change pending.
	d := &Conn{wq: &sendQueue{}}
	d.wq.queued.Store(SoftBacklog + 1)
	d.outWake.Store(true)
	if !d.deferOutput(d.backlogged()) || !d.outPending || d.deferred.Load() != 1 {
		t.Fatalf("backlogged: pending %v, deferred %d", d.outPending, d.deferred.Load())
	}
	if d.deferOutput(false) {
		t.Fatal("deferred without a backlog")
	}
}

// TestLiveMarkerAccounting checks that a live marker counts as
// outstanding from before it is queued until a revocation queued after
// it has been entirely written, not merely queued.
func TestLiveMarkerAccounting(t *testing.T) {
	c := &Conn{wq: &sendQueue{}}
	live := func() bool { return c.Stats().LiveMarker }
	if live() {
		t.Fatal("live before any marker")
	}
	queue := func(n int64) {
		c.wq.mu.Lock()
		c.wq.total += n
		c.wq.mu.Unlock()
	}
	c.liveGen.Add(1)
	if !live() {
		t.Fatal("marker being queued is not counted")
	}
	queue(100) // the live marker and data before it
	c.noteWritten(c.wq, 100)
	if !live() {
		t.Fatal("a written live marker is no longer counted")
	}
	queue(40) // the revocation
	c.noteRevocation()
	if !live() {
		t.Fatal("a queued revocation withdrew the marker before it was written")
	}
	c.noteWritten(c.wq, 39)
	if !live() {
		t.Fatal("a partly written revocation withdrew the marker")
	}
	c.noteWritten(c.wq, 1)
	if live() {
		t.Fatal("a written revocation did not withdraw the marker")
	}
	// A newer live marker is outstanding again until the next revocation.
	c.liveGen.Add(1)
	queue(50)
	c.noteWritten(c.wq, 50)
	if !live() {
		t.Fatal("new live marker not counted")
	}
}

// TestRevocationWrittenFirst checks the order in which the writer can
// finish a revocation before its boundary is recorded: the live marker
// must still be withdrawn, not left outstanding.
func TestRevocationWrittenFirst(t *testing.T) {
	c := &Conn{wq: &sendQueue{}}
	c.liveGen.Add(1)
	c.wq.total = 140 // a live marker, then a revocation
	c.noteWritten(c.wq, 140)
	if !c.Stats().LiveMarker {
		t.Fatal("withdrawn before the revocation was recorded")
	}
	c.noteRevocation()
	if c.Stats().LiveMarker {
		t.Fatal("a revocation written before its boundary was recorded left the marker outstanding")
	}
}

// TestLiveMarkerPendingDuringPublish checks that once publish has read a
// live lease, the session reports a possibly outstanding live marker
// while it still queues the values ahead of the marker, so shutdown
// cannot see "no live marker" in between.
func TestLiveMarkerPendingDuringPublish(t *testing.T) {
	store, err := output.NewStore([]output.Table{
		{Name: "t_out", Kind: output.KindAggregate, Expiry: 30000},
		{Name: "t_meta", Kind: output.KindMetadata, Expiry: 2000},
	}, output.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	go func() { _, _ = io.Copy(io.Discard, b) }()
	c, err := NewConn(a, Options{
		LocalPeer: "agg", Heartbeat: time.Second, IdleTimeout: 5 * time.Second, HandshakeTimeout: time.Second,
		MaxTables: 8, RequestResync: true, Output: store, Generation: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range c.outs {
		o.ready = true
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.startWriter(ctx)
	defer c.stopWriter(0)
	k, err := peermsg.KeyFromAddr(netip.MustParseAddr("2001:db8::"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set("t_out", k, output.AggregateValues(5)); err != nil {
		t.Fatal(err)
	}
	if err := store.SetLease(time.Now().Add(output.MaxLeaseLength)); err != nil {
		t.Fatal(err)
	}
	var during *bool
	beforeLeaseHook = func(h *Conn) {
		if h == c {
			v := c.Stats().LiveMarker
			during = &v
		}
	}
	defer func() { beforeLeaseHook = nil }()
	if err := c.publish(ctx); err != nil {
		t.Fatal(err)
	}
	if during == nil || !*during {
		t.Fatalf("LiveMarker before the marker was queued: %v, want true", during)
	}
	if st := c.Stats(); !st.LiveMarker || st.Markers != 1 {
		t.Fatalf("after publish: %+v", st)
	}
}
