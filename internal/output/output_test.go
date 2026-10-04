package output_test

import (
	"errors"
	"math"
	"net/netip"
	"slices"
	"testing"

	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
)

var tables = []output.Table{
	{Name: "out", Kind: output.KindAggregate, Expiry: 30000},
	{Name: "meta", Kind: output.KindMetadata, Expiry: 30000},
}

func key(t *testing.T, s string) peermsg.Key {
	t.Helper()
	k, err := peermsg.KeyFromAddr(netip.MustParseAddr(s))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestSchema(t *testing.T) {
	def := output.Definition("out", 30000)
	if err := def.Validate(); err != nil {
		t.Fatal(err)
	}
	if want := []peermsg.Field{{Type: peermsg.DataGPT, ArrayLen: 4}}; !slices.Equal(def.Fields, want) ||
		def.KeyType != peermsg.KeyTypeIPv6 || def.Expiry != 30000 {
		t.Fatalf("definition %+v", def)
	}
	if v := output.AggregateValues(1234); v != (output.Values{1, 1234, 0, 0}) {
		t.Fatalf("aggregate values %v", v)
	}
	if v := output.MetadataValues(); v != (output.Values{1, 0, 0, 0}) {
		t.Fatalf("metadata values %v", v)
	}
	for in, want := range map[uint64]uint32{
		0: 0, 1: 1, math.MaxUint32 - 1: math.MaxUint32 - 1, math.MaxUint32: math.MaxUint32,
		math.MaxUint32 + 1: math.MaxUint32, math.MaxUint64: math.MaxUint32,
	} {
		if got := output.RateValue(in); got != want {
			t.Errorf("RateValue(%d) = %d, want %d", in, got, want)
		}
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
}

func TestStoreSinceAndChanged(t *testing.T) {
	s, err := output.NewStore(tables)
	if err != nil {
		t.Fatal(err)
	}
	if defs := s.Tables(); len(defs) != 2 || defs[0].Name != "out" || defs[1].Name != "meta" {
		t.Fatalf("tables %+v", defs)
	}
	if e, seq := s.Since(0); len(e) != 0 || seq != 0 {
		t.Fatalf("empty store: %v %d", e, seq)
	}
	ch := s.Changed()
	k1, k2 := key(t, "2001:db8::"), key(t, "::ffff:192.0.2.1")
	set := func(table string, k peermsg.Key, v output.Values) {
		t.Helper()
		if err := s.Set(table, k, v); err != nil {
			t.Fatal(err)
		}
	}
	set("meta", output.MetadataKey, output.MetadataValues())
	select {
	case <-ch:
	default:
		t.Fatal("Set did not close the change channel")
	}
	ch = s.Changed()
	set("out", k2, output.AggregateValues(2))
	set("out", k1, output.AggregateValues(1))
	select {
	case <-ch:
	default:
		t.Fatal("Set did not close the next change channel")
	}
	all, seq := s.Since(0)
	want := []output.Entry{
		{Table: "out", Key: k2, Values: output.AggregateValues(2), Seq: 2},
		{Table: "out", Key: k1, Values: output.AggregateValues(1), Seq: 3},
		{Table: "meta", Key: output.MetadataKey, Values: output.MetadataValues(), Seq: 1},
	}
	if !slices.Equal(all, want) || seq != 3 {
		t.Fatalf("Since(0) = %v, %d; want %v, 3", all, seq, want)
	}
	// Unchanged values are not a change.
	ch = s.Changed()
	set("out", k1, output.AggregateValues(1))
	select {
	case <-ch:
		t.Fatal("unchanged Set closed the change channel")
	default:
	}
	set("out", k2, output.AggregateValues(7))
	if got, cur := s.Since(seq); !slices.Equal(got, []output.Entry{{Table: "out", Key: k2, Values: output.AggregateValues(7), Seq: 4}}) ||
		cur != 4 {
		t.Fatalf("Since(%d) = %v, %d", seq, got, cur)
	}
	if v, ok := s.Get("out", k2); !ok || v != output.AggregateValues(7) {
		t.Fatalf("Get = %v, %v", v, ok)
	}
	if _, ok := s.Get("out", key(t, "::1")); ok {
		t.Fatal("Get found an unpublished key")
	}
}

func TestStoreErrors(t *testing.T) {
	for name, tbls := range map[string][]output.Table{
		"duplicate":    {tables[0], tables[0]},
		"no expiry":    {{Name: "out", Kind: output.KindAggregate}},
		"bad name":     {{Name: "o ut", Kind: output.KindAggregate, Expiry: 1}},
		"unknown kind": {{Name: "out", Expiry: 1}},
	} {
		if _, err := output.NewStore(tbls); !errors.Is(err, output.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	s, err := output.NewStore(tables)
	if err != nil {
		t.Fatal(err)
	}
	k := key(t, "2001:db8::")
	for name, set := range map[string]func() error{
		"unknown table":      func() error { return s.Set("in", k, output.AggregateValues(1)) },
		"version 0":          func() error { return s.Set("out", k, output.Values{0, 1}) },
		"version 2":          func() error { return s.Set("out", k, output.Values{2, 1}) },
		"generation set":     func() error { return s.Set("out", k, output.Values{1, 1, 1, 0}) },
		"reserved set":       func() error { return s.Set("out", k, output.Values{1, 1, 0, 1}) },
		"metadata other key": func() error { return s.Set("meta", k, output.MetadataValues()) },
		"metadata slot 1":    func() error { return s.Set("meta", output.MetadataKey, output.Values{1, 1}) },
	} {
		if err := set(); !errors.Is(err, output.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if e, _ := s.Since(0); len(e) != 0 {
		t.Fatalf("rejected values were stored: %v", e)
	}
}
