package output_test

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
)

const gen = 0x5eed

var tables = []output.Table{
	{Name: "out", Kind: output.KindAggregate, Expiry: 30000},
	{Name: "meta", Kind: output.KindMetadata, Expiry: 2000},
}

func key(t *testing.T, s string) peermsg.Key {
	t.Helper()
	k, err := peermsg.KeyFromAddr(netip.MustParseAddr(s))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func newStore(t *testing.T, opts output.StoreOptions) *output.Store {
	t.Helper()
	s, err := output.NewStore(tables, opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// stamped is an aggregate entry as the store keeps it: without a
// generation, which each session writes.
func stamped(rate uint32) output.Values { return output.AggregateValues(rate) }

func TestSchema(t *testing.T) {
	def := output.Definition("out", 30000)
	if err := def.Validate(); err != nil {
		t.Fatal(err)
	}
	if want := []peermsg.Field{{Type: peermsg.DataGPT, ArrayLen: 4}}; !slices.Equal(def.Fields, want) ||
		def.KeyType != peermsg.KeyTypeIPv6 || def.Expiry != 30000 {
		t.Fatalf("definition %+v", def)
	}
	if v := output.AggregateValues(1234); v != (output.Values{2, 1234, 0, 0}) {
		t.Fatalf("aggregate values %v", v)
	}
	if v := output.MarkerValues(77, 9); v != (output.Values{2, 77, 9, 0}) {
		t.Fatalf("marker values %v", v)
	}
	if v := output.RevocationValues(); v != (output.Values{2, 0, 0, 0}) {
		t.Fatalf("revocation values %v", v)
	}
	if output.MetadataKey.String() != "::" {
		t.Fatalf("metadata key %v", output.MetadataKey)
	}
	for _, k := range []output.Kind{output.KindAggregate, output.KindMetadata} {
		if got, err := output.ParseKind(k.String()); err != nil || got != k {
			t.Errorf("ParseKind(%v) = %v, %v", k, got, err)
		}
	}
	if _, err := output.ParseKind("rate"); err == nil {
		t.Error("ParseKind accepted an unknown kind")
	}
	if output.LeaseWindowMillis != output.MaxLease.Milliseconds() {
		t.Fatalf("lease window %d ms, MaxLease %v", output.LeaseWindowMillis, output.MaxLease)
	}
}

// TestDeadlineArithmetic checks the deadline encoding against what the
// HAProxy expressions compute: the low 32 bits of the Unix time in
// milliseconds, compared modulo 2^32, so a lease stays correct across the
// 49.7-day wrap of the 32-bit value.
func TestDeadlineArithmetic(t *testing.T) {
	base := time.UnixMilli(1791140245262)
	if got := output.DeadlineValue(base); got != uint32(1791140245262%(1<<32)) {
		t.Fatalf("DeadlineValue = %d", got)
	}
	wrap := time.UnixMilli(3 << 32) // low 32 bits are 0 here
	for _, now := range []time.Time{base, wrap.Add(-time.Second), wrap, wrap.Add(-time.Millisecond)} {
		for _, tc := range []struct {
			offset time.Duration
			live   bool
		}{
			{1500 * time.Millisecond, true},
			{output.MaxLease, true},
			{output.MaxLease + time.Millisecond, false}, // too far ahead
			{0, true},                  // the deadline's own millisecond
			{-time.Millisecond, false}, // expired
			{-time.Hour, false},
		} {
			left := output.LeaseLeft(output.DeadlineValue(now.Add(tc.offset)), now)
			if live := left <= output.LeaseWindowMillis; live != tc.live {
				t.Errorf("now %d offset %v: left %d, live %v, want %v", now.UnixMilli(), tc.offset, left, live, tc.live)
			}
		}
	}
	if output.DeadlineValue(time.UnixMilli(-1)) != math.MaxUint32 {
		t.Fatal("pre-1970 time does not wrap like HAProxy's and()")
	}
}

func TestStoreSinceAndChanged(t *testing.T) {
	s := newStore(t, output.StoreOptions{})
	if defs := s.Tables(); len(defs) != 2 || defs[0].Name != "out" || defs[1].Name != "meta" {
		t.Fatalf("tables %+v", defs)
	}
	if e, seq := s.Since(0); len(e) != 0 || seq != 0 {
		t.Fatalf("empty store: %v %d", e, seq)
	}
	ch := s.Changed()
	k1, k2 := key(t, "2001:db8::"), key(t, "::ffff:192.0.2.1")
	set := func(k peermsg.Key, v output.Values) {
		t.Helper()
		if err := s.Set("out", k, v); err != nil {
			t.Fatal(err)
		}
	}
	set(k2, output.AggregateValues(2))
	select {
	case <-ch:
	default:
		t.Fatal("Set did not close the change channel")
	}
	ch = s.Changed()
	set(k1, output.AggregateValues(1))
	select {
	case <-ch:
	default:
		t.Fatal("Set did not close the next change channel")
	}
	all, seq := s.Since(0)
	want := []output.Entry{
		{Table: "out", Key: k2, Values: stamped(2), Seq: 1},
		{Table: "out", Key: k1, Values: stamped(1), Seq: 2},
	}
	if !slices.Equal(all, want) || seq != 2 {
		t.Fatalf("Since(0) = %v, %d; want %v, 2", all, seq, want)
	}
	// Unchanged values are not a change.
	ch = s.Changed()
	set(k1, output.AggregateValues(1))
	set(k1, stamped(1))
	select {
	case <-ch:
		t.Fatal("unchanged Set closed the change channel")
	default:
	}
	set(k2, output.AggregateValues(7))
	if got, cur := s.Since(seq); !slices.Equal(got, []output.Entry{{Table: "out", Key: k2, Values: stamped(7), Seq: 3}}) ||
		cur != 3 {
		t.Fatalf("Since(%d) = %v, %d", seq, got, cur)
	}
	if v, ok := s.Get("out", k2); !ok || v != stamped(7) {
		t.Fatalf("Get = %v, %v", v, ok)
	}
	if _, ok := s.Get("out", key(t, "::1")); ok {
		t.Fatal("Get found an unpublished key")
	}
}

func TestNewGeneration(t *testing.T) {
	seen := map[uint32]bool{}
	for range 8 {
		g, err := output.NewGeneration()
		if err != nil {
			t.Fatal(err)
		}
		if g == 0 {
			t.Fatal("generation 0")
		}
		seen[g] = true
	}
	if len(seen) < 2 {
		t.Fatalf("eight generations alike: %v", seen)
	}
}

// TestLeaseDeadlineIsFixed checks that a lease's deadline is read from
// the wall clock once, when it is set: a marker written after the wall
// clock moved on without the monotonic clock (a suspend, or a step
// forward) carries the original deadline, or is not written at all once
// that deadline has passed; a step back neither extends nor moves it.
func TestLeaseDeadlineIsFixed(t *testing.T) {
	var jump atomic.Int64
	s := newStore(t, output.StoreOptions{WallClock: func() time.Time { return time.Now().Add(time.Duration(jump.Load())) }})
	if err := s.SetLease(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	l := s.Lease()
	before, _, ok := s.Marker(l, gen)
	if !ok {
		t.Fatal("no marker for a live lease")
	}
	jump.Store(int64(time.Hour))
	if v, rem, live := s.Marker(l, gen); live {
		t.Fatalf("an hour later on the wall clock the lease still writes %v with %v left", v, rem)
	}
	jump.Store(-int64(time.Hour))
	after, rem, ok := s.Marker(l, gen)
	if !ok || after != before || rem > 1000 {
		t.Fatalf("after a step back: %v (%v, %v), want %v within the lease", after, rem, ok, before)
	}
}

func TestLease(t *testing.T) {
	skew := 1500 * time.Millisecond
	s := newStore(t, output.StoreOptions{WallClock: func() time.Time { return time.Now().Add(skew) }})
	if l := s.Lease(); l.Gen != 0 {
		t.Fatalf("new store lease %+v", l)
	}
	if _, _, ok := s.Marker(s.Lease(), gen); ok {
		t.Fatal("marker without a lease")
	}
	if err := s.Set("out", key(t, "2001:db8::"), output.AggregateValues(5)); err != nil {
		t.Fatal(err)
	}
	ch := s.Changed()
	until := time.Now().Add(time.Second)
	if err := s.SetLease(until); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	default:
		t.Fatal("SetLease did not close the change channel")
	}
	l := s.Lease()
	if l.Gen != 1 || l.Seq != 1 || !l.Valid || !l.Until.Equal(until) {
		t.Fatalf("lease %+v", l)
	}
	v, rem, ok := s.Marker(l, gen)
	wall := time.Now().Add(skew)
	if !ok || rem < 900 || rem > 1000 || v[0] != output.SchemaVersion || v[output.SlotGeneration] != gen || v[3] != 0 {
		t.Fatalf("marker %v, %v, %v", v, rem, ok)
	}
	// The deadline is on the (skewed) wall clock, as far ahead of it as
	// the lifetime the marker is sent with.
	if got := output.LeaseLeft(v[output.SlotDeadline], wall); got > uint32(rem)+1 || got+50 < uint32(rem) {
		t.Fatalf("marker deadline %d leaves %d ms on the skewed wall clock, lifetime %d ms", v[output.SlotDeadline],
			got, rem)
	}
	if got := output.LeaseLeft(v[output.SlotDeadline], time.Now()); got <= output.LeaseWindowMillis {
		t.Fatalf("deadline 1.5 s ahead on the skewed clock is live (%d ms left) on an unskewed one", got)
	}
	short := time.Now().Add(20 * time.Millisecond)
	if err := s.SetLease(short); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(short))
	if _, _, ok := s.Marker(s.Lease(), gen); ok {
		t.Fatal("marker for a lease that has run out")
	}

	ch = s.Changed()
	s.Revoke()
	select {
	case <-ch:
	default:
		t.Fatal("Revoke did not close the change channel")
	}
	l = s.Lease()
	if l.Gen != 3 || l.Valid {
		t.Fatalf("revoked lease %+v", l)
	}
	if v, rem, ok := s.Marker(l, gen); !ok || v != output.RevocationValues() || rem != 2000 {
		t.Fatalf("revocation %v, %v, %v", v, rem, ok)
	}

	for name, until := range map[string]time.Time{
		"past":     time.Now().Add(-time.Millisecond),
		"zero":     {},
		"too long": time.Now().Add(output.MaxLeaseLength + 50*time.Millisecond),
	} {
		if err := s.SetLease(until); !errors.Is(err, output.ErrInvalid) {
			t.Errorf("SetLease %s: %v", name, err)
		}
	}
	if l := s.Lease(); l.Gen != 3 {
		t.Fatalf("refused leases changed the lease: %+v", l)
	}
}

func TestStoreErrors(t *testing.T) {
	for name, tbls := range map[string][]output.Table{
		"duplicate":     {tables[0], tables[0]},
		"no expiry":     {{Name: "out", Kind: output.KindAggregate}},
		"bad name":      {{Name: "o ut", Kind: output.KindAggregate, Expiry: 1}},
		"unknown kind":  {{Name: "out", Expiry: 1}},
		"long metadata": {{Name: "meta", Kind: output.KindMetadata, Expiry: 2001}},
	} {
		if _, err := output.NewStore(tbls, output.StoreOptions{}); !errors.Is(err, output.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	s := newStore(t, output.StoreOptions{})
	k := key(t, "2001:db8::")
	for name, set := range map[string]func() error{
		"unknown table":  func() error { return s.Set("in", k, output.AggregateValues(1)) },
		"version 0":      func() error { return s.Set("out", k, output.Values{0, 1}) },
		"version 1":      func() error { return s.Set("out", k, output.Values{1, 1}) },
		"generation set": func() error { return s.Set("out", k, output.Values{2, 1, gen, 0}) },
		"reserved set":   func() error { return s.Set("out", k, output.Values{2, 1, 0, 1}) },
		"metadata table": func() error { return s.Set("meta", output.MetadataKey, output.MarkerValues(1, gen)) },
	} {
		if err := set(); !errors.Is(err, output.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if e, _ := s.Since(0); len(e) != 0 {
		t.Fatalf("rejected values were stored: %v", e)
	}
}

func TestRetire(t *testing.T) {
	s := newStore(t, output.StoreOptions{})
	k1, k2 := key(t, "2001:db8:1::"), key(t, "2001:db8:2::")
	for _, k := range []peermsg.Key{k1, k2} {
		if err := s.Set("out", k, output.AggregateValues(5)); err != nil {
			t.Fatal(err)
		}
	}
	ch := s.Changed()
	if err := s.Retire("out", k2); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	default:
		t.Fatal("Retire did not close the change channel")
	}
	if all, _ := s.Since(0); len(all) != 1 || all[0].Key != k1 || s.Retired() != 1 {
		t.Fatalf("after Retire: %v, retired %d", all, s.Retired())
	}
	if _, ok := s.Get("out", k2); ok {
		t.Fatal("retired key still published")
	}
	ch = s.Changed()
	if err := s.Retire("out", k2); err != nil || s.Retired() != 1 {
		t.Fatalf("retiring an absent key: %v, retired %d", err, s.Retired())
	}
	select {
	case <-ch:
		t.Fatal("retiring an absent key woke sessions")
	default:
	}
	for _, table := range []string{"meta", "in"} {
		if err := s.Retire(table, k1); !errors.Is(err, output.ErrInvalid) {
			t.Errorf("Retire on %s: %v", table, err)
		}
	}
	if output.MaxLeaseLength != time.Second {
		t.Fatalf("lease limit %v", output.MaxLeaseLength)
	}
}

// TestRetireBatch checks that a batch removes every listed key as one
// change: the change channel closes once, and nothing is removed if any
// table is invalid.
func TestRetireBatch(t *testing.T) {
	s := newStore(t, output.StoreOptions{})
	var ks []peermsg.Key
	for i := 1; i <= 5; i++ {
		k := key(t, fmt.Sprintf("2001:db8:%d::", i))
		ks = append(ks, k)
		if err := s.Set("out", k, output.AggregateValues(5)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RetireBatch(map[string][]peermsg.Key{"out": ks[:2], "meta": ks[2:]}); !errors.Is(err, output.ErrInvalid) {
		t.Fatalf("batch with a metadata table: %v", err)
	}
	if all, _ := s.Since(0); len(all) != 5 || s.Retired() != 0 {
		t.Fatalf("invalid batch removed keys: %d left, retired %d", len(all), s.Retired())
	}
	ch := s.Changed()
	if err := s.RetireBatch(map[string][]peermsg.Key{"out": ks[:4]}); err != nil {
		t.Fatal(err)
	}
	after := s.Changed()
	select {
	case <-ch:
	default:
		t.Fatal("RetireBatch did not wake sessions")
	}
	select {
	case <-after:
		t.Fatal("RetireBatch notified more than once")
	default:
	}
	if all, _ := s.Since(0); len(all) != 1 || all[0].Key != ks[4] || s.Retired() != 4 {
		t.Fatalf("after RetireBatch: %v, retired %d", all, s.Retired())
	}
	if err := s.RetireBatch(map[string][]peermsg.Key{"out": ks[:4]}); err != nil || s.Retired() != 4 {
		t.Fatalf("retiring absent keys: %v, retired %d", err, s.Retired())
	}
	select {
	case <-after:
		t.Fatal("retiring absent keys woke sessions")
	default:
	}
}
