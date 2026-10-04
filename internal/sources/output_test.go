package sources_test

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// The fake configuration's output tables.
var (
	outCfg = []config.Output{
		{Name: "t_out", Kind: output.KindAggregate, Expire: 30 * time.Second},
		{Name: "t_meta", Kind: output.KindMetadata, Expire: 2 * time.Second},
	}
	aggDef  = output.Definition("t_out", 30000)
	metaDef = output.Definition("t_meta", 2000)
)

// outputManager starts a manager for inbound source "a" with the output
// tables and store, which may hold entries already.
func outputManager(t *testing.T, store *output.Store) (*sources.Manager, *recorder, string) {
	t.Helper()
	return refreshingManager(t, store, 0)
}

// refreshingManager is outputManager with sources.Options.OutputRefresh.
func refreshingManager(t *testing.T, store *output.Store, refresh time.Duration) (*sources.Manager, *recorder, string) {
	t.Helper()
	cfg := fastConfig(config.Source{Name: "a"})
	cfg.Outputs = outCfg
	ln := listen(t)
	m, r := startManager(t, sources.Options{Config: cfg, Listener: ln, Output: store, OutputRefresh: refresh})
	return m, r, ln.Addr().String()
}

func newStore(t *testing.T) *output.Store {
	t.Helper()
	cfg := config.Config{Outputs: outCfg}
	s, err := output.NewStore(cfg.OutputTables(), output.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// daemonMsg is one decoded message from the daemon, as the source's
// side of the session sees it: the daemon's table IDs are remote IDs.
type daemonMsg struct {
	frame peerwire.Frame
	msg   peermsg.Message
}

// readDaemon decodes the daemon's next non-heartbeat messages until stop
// holds for the last one.
func readDaemon(f *fakePeer, in *peermsg.Inbound, stop func(daemonMsg) bool) []daemonMsg {
	f.t.Helper()
	var out []daemonMsg
	for {
		fr, err := f.nonHeartbeat(fakeTimeout)
		if err != nil {
			f.t.Fatalf("reading the daemon after %d messages: %v", len(out), err)
		}
		msg, err := in.Decode(fr, time.Now())
		if err != nil {
			f.t.Fatalf("daemon message %v: %v", fr, err)
		}
		dm := daemonMsg{fr, msg}
		out = append(out, dm)
		if stop(dm) {
			return out
		}
	}
}

// summarize renders definitions as "def NAME/ID", aggregate updates as
// "upd NAME ID KEY VALUES", lease markers as "mark NAME ID EPOCH" (after
// checking that the deadline is live and the lifetime within the lease
// bound), revocations as "revoke NAME ID", and other messages by type.
func summarize(t *testing.T, msgs []daemonMsg) []string {
	t.Helper()
	var out []string
	for _, m := range msgs {
		switch v := m.msg.(type) {
		case peermsg.DefinitionMessage:
			if !slices.Equal(v.Definition.Fields, aggDef.Fields) || v.Definition.KeyType != peermsg.KeyTypeIPv6 {
				t.Errorf("daemon announced %v", v.Definition)
			}
			out = append(out, fmt.Sprintf("def %s/%d", v.Definition.Name, v.ID))
		case peermsg.UpdateMessage:
			a := v.Update.Values[0].Array
			if v.Table == metaDef.Name {
				out = append(out, summarizeMarker(t, v))
				continue
			}
			if !v.Update.ExplicitID || v.Update.Timed {
				t.Errorf("update %+v: want explicit and untimed", v.Update)
			}
			out = append(out, fmt.Sprintf("upd %s %d %v %v", v.Table, v.Update.ID, v.Update.Key, a))
		case peermsg.Control:
			out = append(out, fmt.Sprintf("ctl %d", v.Type))
		default:
			out = append(out, "other")
		}
	}
	return out
}

func summarizeMarker(t *testing.T, v peermsg.UpdateMessage) string {
	t.Helper()
	u, a := v.Update, v.Update.Values[0].Array
	if !u.ExplicitID || !u.Timed || u.Key != output.MetadataKey || u.Remaining > 2000 || u.Remaining == 0 ||
		a[0] != output.SchemaVersion || a[3] != 0 {
		t.Errorf("marker %+v: want an explicit timed update of :: within the lease bound", u)
	}
	if a[output.SlotGeneration] == 0 {
		if a[output.SlotDeadline] != 0 {
			t.Errorf("revocation %v carries a deadline", a)
		}
		return fmt.Sprintf("revoke %s %d", v.Table, u.ID)
	}
	if left := output.LeaseLeft(a[output.SlotDeadline], time.Now()); left > output.LeaseWindowMillis {
		t.Errorf("marker %v is not live: %d ms left", a, left)
	}
	return fmt.Sprintf("mark %s %d %d", v.Table, u.ID, a[output.SlotGeneration])
}

// TestOutputTeachAndLive checks what the daemon sends a source: nothing
// for an output table until the source announces a matching definition
// of it, then that table's definition and every entry; live changes with a
// definition only on table switches; a full teach per resync request;
// never an input table; explicit untimed aggregate update IDs; lease
// markers as explicit timed updates, only once every output table is
// taught and always after the values they certify, never taught from the
// store; and acknowledgements routed by the daemon's own table IDs even
// where the source's IDs collide with them.
func TestOutputTeachAndLive(t *testing.T) {
	store := newStore(t)
	k1, k2 := mustKey(t, "2001:db8:1::"), mustKey(t, "::ffff:192.0.2.1")
	if err := store.Set("t_out", k1, output.AggregateValues(5)); err != nil {
		t.Fatal(err)
	}
	m, r, addr := outputManager(t, store)
	f := connectFake(t, addr, "a")
	f.established()
	in := peermsg.NewInbound(8)
	isUpdate := func(m daemonMsg) bool { _, ok := m.msg.(peermsg.UpdateMessage); return ok }
	isMeta := func(m daemonMsg) bool { u, ok := m.msg.(peermsg.UpdateMessage); return ok && u.Table == "t_meta" }
	lease := func() {
		t.Helper()
		if err := m.SetLease(time.Now().Add(output.MaxLeaseLength)); err != nil {
			t.Fatal(err)
		}
	}
	// Every entry the session writes carries its generation.
	g := status(t, m, "a").Stats.Generation
	if g == 0 {
		t.Fatal("session without a generation")
	}
	mark := func(id int) string { return fmt.Sprintf("mark t_meta %d %d", id, g) }
	upd := func(id int, key string, rate int) string {
		return fmt.Sprintf("upd t_out %d %s [2 %d %d 0]", id, key, rate, g)
	}

	// Nothing is sent before the source announces a table, however much
	// is published or leased.
	if err := m.Publish("t_out", k2, output.AggregateValues(9)); err != nil {
		t.Fatal(err)
	}
	lease()
	// (Shorter than the fake configuration's 300 ms idle timeout.)
	if fr, err := f.nonHeartbeat(150 * time.Millisecond); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("daemon sent %v (%v) before the source announced any output table", fr, err)
	}
	f.control(peerwire.ControlHeartbeat)

	// The source announces its t_meta (its ID 3) first: the daemon
	// announces it back but writes no marker while t_out is untaught.
	f.define(3, metaDef)
	got := summarize(t, readDaemon(f, in, func(daemonMsg) bool { return true }))
	if want := []string{"def t_meta/2"}; !slices.Equal(got, want) {
		t.Fatalf("t_meta announced: sent %q, want %q", got, want)
	}
	if fr, err := f.nonHeartbeat(100 * time.Millisecond); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("daemon sent %v (%v) with t_out still untaught", fr, err)
	}
	f.control(peerwire.ControlHeartbeat)

	// The source announces its t_out (its ID 2): the daemon teaches that
	// table, then writes the lease after the values.
	lease()
	f.define(2, aggDef)
	got = summarize(t, readDaemon(f, in, isMeta))
	want := []string{
		"def t_out/1", upd(1, "2001:db8:1::", 5), upd(2, "::ffff:192.0.2.1", 9),
		"def t_meta/2", mark(1),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("t_out announced: sent %q, want %q", got, want)
	}
	// Re-announcing (as HAProxy does to switch tables) teaches nothing.
	f.define(2, aggDef)

	// Live: t_meta is selected, so switching to t_out needs a definition;
	// unchanged values send nothing, and a value alone writes no marker.
	if err := m.Publish("t_out", k1, output.AggregateValues(6)); err != nil {
		t.Fatal(err)
	}
	if err := m.Publish("t_out", k1, output.AggregateValues(6)); err != nil {
		t.Fatal(err)
	}
	got = summarize(t, readDaemon(f, in, isUpdate))
	if wantLive := []string{"def t_out/1", upd(3, "2001:db8:1::", 6)}; !slices.Equal(got, wantLive) {
		t.Fatalf("live switch sent %q, want %q", got, wantLive)
	}
	// A value and the lease that certifies it: the marker follows it.
	if err := m.Publish("t_out", k2, output.AggregateValues(10)); err != nil {
		t.Fatal(err)
	}
	lease()
	got = summarize(t, readDaemon(f, in, isMeta))
	want = []string{upd(4, "::ffff:192.0.2.1", 10), "def t_meta/2", mark(2)}
	if !slices.Equal(got, want) {
		t.Fatalf("value and lease sent %q, want %q", got, want)
	}

	// A resync request gets a full teach (in change order) with no
	// marker from the store, then "finished", and only then the lease
	// again.
	f.control(peerwire.ControlResyncRequest)
	got = summarize(t, readDaemon(f, in, isMeta))
	want = []string{
		"def t_out/1", upd(5, "2001:db8:1::", 6), upd(6, "::ffff:192.0.2.1", 10),
		"def t_meta/2", fmt.Sprintf("ctl %d", peerwire.ControlResyncFinished), mark(3),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("teach sent %q, want %q", got, want)
	}
	f.control(peerwire.ControlResyncConfirm)

	// Revocation is written at once.
	m.Revoke()
	got = summarize(t, readDaemon(f, in, isMeta))
	if want := []string{"revoke t_meta 4"}; !slices.Equal(got, want) {
		t.Fatalf("revocation sent %q, want %q", got, want)
	}

	// The source's IDs collide with the daemon's: its input t_in is its
	// ID 1 (the daemon's t_out), its copy of t_out its ID 2 (the daemon's
	// t_meta).
	f.define(1, inDef)
	f.update(inDef, 50, "2001:db8:9::", 7)
	ack := f.expect(peerwire.ClassStickTable, peerwire.StickTableAck)
	if id, upd, err := peermsg.DecodeAck(ack.Body); err != nil || id != 1 || upd != 50 {
		t.Fatalf("ack of the input update: table %d update %d %v", id, upd, err)
	}
	f.define(2, aggDef)
	f.update(aggDef, 900, "2001:db8:1::", 0) // the source's replayed copy
	ack = f.expect(peerwire.ClassStickTable, peerwire.StickTableAck)
	if id, upd, err := peermsg.DecodeAck(ack.Body); err != nil || id != 2 || upd != 900 {
		t.Fatalf("ack of the echoed output: table %d update %d %v", id, upd, err)
	}
	// The source acknowledges the daemon's t_out (its ID 1) and t_meta.
	sendAck := func(id peermsg.RemoteTableID, upd peermsg.UpdateID) {
		b, err := peermsg.AppendAck(nil, id, upd)
		if err != nil {
			t.Fatal(err)
		}
		f.send(b)
	}
	sendAck(1, 6)
	sendAck(2, 4)
	f.control(peerwire.ControlHeartbeat)
	deadline := time.Now().Add(fakeTimeout)
	for status(t, m, "a").Stats.AcksReceived < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("acks not processed: %+v", status(t, m, "a").Stats)
		}
		time.Sleep(5 * time.Millisecond)
	}
	st := status(t, m, "a").Stats
	wantOut := []peersession.OutputStats{
		{Table: "t_out", LocalID: 1, SourceID: 2, Sent: 6, LastSent: 6, Acked: true, LastAcked: 6},
		{Table: "t_meta", LocalID: 2, SourceID: 3, Sent: 4, LastSent: 4, Acked: true, LastAcked: 4},
	}
	if !slices.Equal(st.Outputs, wantOut) || st.EchoedUpdates != 1 || st.Teaches != 3 || st.TaughtUpdates != 4 ||
		st.OutputUpdates != 6 || st.Updates != 1 || st.Markers != 4 || st.Revocations != 1 ||
		st.LastDeadline != 0 || st.LastMarker.IsZero() {
		t.Fatalf("stats %+v", st)
	}
	evs := r.waitFor("input update", fakeTimeout, func(evs []sources.Event) bool {
		return count[peersession.EntryUpdated](evs, "a") == 1
	})
	for _, ev := range evs {
		switch b := ev.Body.(type) {
		case peersession.TableDefined:
			if b.Definition.Name != "t_in" || b.ID != 1 {
				t.Errorf("definition event %s/%d", b.Definition.Name, b.ID)
			}
		case peersession.EntryUpdated:
			if b.Table != "t_in" || b.ID != 1 || reqCnt(b) != 7 {
				t.Errorf("update event %s/%d %d", b.Table, b.ID, reqCnt(b))
			}
		}
	}
}

// TestOutputFailures covers acknowledgements and output definitions the
// daemon must refuse. A source whose table of an output's name stores
// anything else (an input schema, another gpt length, another expiry) gets
// no definition or entry of it before the session ends: stock HAProxy
// would apply them to that table.
func TestOutputFailures(t *testing.T) {
	ack := func(id peermsg.RemoteTableID, upd peermsg.UpdateID) []byte {
		b, err := peermsg.AppendAck(nil, id, upd)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	def := func(d peermsg.Definition) []byte {
		b, err := peermsg.AppendDefinition(nil, 3, d)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	short := output.Definition("t_out", 30000)
	short.Fields = []peermsg.Field{{Type: peermsg.DataGPT, ArrayLen: 3}}
	cases := []struct {
		name     string
		announce bool // the source announces t_out first and reads its teach
		send     []byte
		want     error
	}{
		{"ack for an unannounced table", true, ack(3, 1), peersession.ErrProtocol},
		{"ack for an update never sent", true, ack(1, 2), peersession.ErrProtocol},
		{"ack for a table with nothing sent", true, ack(2, 1), peersession.ErrProtocol},
		{"ack before any teach", false, ack(1, 1), peersession.ErrProtocol},
		{"output table with fewer slots", false, def(short), peersession.ErrSchema},
		{"output table with another expiry", false, def(output.Definition("t_out", 60000)), peersession.ErrSchema},
		{"output name on an input-schema table", false, def(peermsg.Definition{
			Name: "t_out", KeyType: peermsg.KeyTypeIPv6, Expiry: 30000, Fields: inDef.Fields,
		}), peersession.ErrSchema},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore(t)
			if err := store.Set("t_out", mustKey(t, "2001:db8::"), output.AggregateValues(1)); err != nil {
				t.Fatal(err)
			}
			_, r, addr := outputManager(t, store)
			f := connectFake(t, addr, "a")
			f.t = t
			f.established()
			if tc.announce {
				f.define(2, aggDef)
				f.expect(peerwire.ClassStickTable, peerwire.StickTableDefine)
				f.expect(peerwire.ClassStickTable, peerwire.StickTableUpdate)
			}
			f.send(tc.send)
			for _, fr := range f.expectClose() {
				if !tc.announce && fr.Class == peerwire.ClassStickTable {
					t.Errorf("daemon sent stick-table message %#02x for a table the source never matched", uint8(fr.Type))
				}
			}
			evs := r.waitFor("down", fakeTimeout, func(evs []sources.Event) bool { return len(downs(evs, "a")) == 1 })
			if err := downs(evs, "a")[0].Err; !errors.Is(err, tc.want) {
				t.Fatalf("ended with %v, want %v", err, tc.want)
			}
		})
	}
}

func TestPublishErrors(t *testing.T) {
	m, _, _ := inboundManager(t, "a")
	if err := m.Publish("t_out", mustKey(t, "::1"), output.AggregateValues(1)); !errors.Is(err, output.ErrInvalid) {
		t.Fatalf("publish without outputs: %v", err)
	}
	if err := m.SetLease(time.Now().Add(time.Second)); !errors.Is(err, output.ErrInvalid) {
		t.Fatalf("lease without outputs: %v", err)
	}
	m.Revoke()
	m, _, _ = outputManager(t, nil)
	k := mustKey(t, "2001:db8::")
	for name, publish := range map[string]func() error{
		"unknown table":  func() error { return m.Publish("t_in", k, output.AggregateValues(1)) },
		"wrong version":  func() error { return m.Publish("t_out", k, output.Values{1, 1}) },
		"reserved slot":  func() error { return m.Publish("t_out", k, output.Values{2, 1, 0, 1}) },
		"metadata table": func() error { return m.Publish("t_meta", output.MetadataKey, output.MarkerValues(1, 1)) },
		"past lease":     func() error { return m.SetLease(time.Now().Add(-time.Second)) },
		"long lease":     func() error { return m.SetLease(time.Now().Add(output.MaxLeaseLength + 50*time.Millisecond)) },
	} {
		if err := publish(); !errors.Is(err, output.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestStartRejectsMismatchedStore(t *testing.T) {
	cfg := fastConfig(config.Source{Name: "a"})
	cfg.Outputs = outCfg
	for name, tables := range map[string][]output.Table{
		"fewer":  {{Name: "t_out", Kind: output.KindAggregate, Expiry: 30000}},
		"expiry": {{Name: "t_out", Kind: output.KindAggregate, Expiry: 1}, {Name: "t_meta", Kind: output.KindMetadata, Expiry: 2000}},
		"kind":   {{Name: "t_out", Kind: output.KindMetadata, Expiry: 2000}, {Name: "t_meta", Kind: output.KindMetadata, Expiry: 2000}},
		"order":  {{Name: "t_meta", Kind: output.KindMetadata, Expiry: 2000}, {Name: "t_out", Kind: output.KindAggregate, Expiry: 30000}},
	} {
		s, err := output.NewStore(tables, output.StoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if m, err := sources.Start(t.Context(), sources.Options{Config: cfg, Listener: listen(t), Output: s}); err == nil {
			_ = m.Close()
			t.Errorf("%s: accepted", name)
		}
	}
	cfg.RequestResync = false
	if m, err := sources.Start(t.Context(), sources.Options{Config: cfg, Listener: listen(t)}); err == nil {
		_ = m.Close()
		t.Error("outputs without RequestResync: accepted")
	}
	cfg.RequestResync = true
	cfg.Outputs = []config.Output{{Name: "t_in", Kind: output.KindAggregate, Expire: time.Second}}
	if m, err := sources.Start(t.Context(), sources.Options{Config: cfg, Listener: listen(t)}); err == nil {
		_ = m.Close()
		t.Error("output named like an input table: accepted")
	}
}

// TestOutputRefresh checks the periodic refresh: once every output table
// is taught, the session re-sends every entry, unchanged ones included,
// under its generation at the refresh interval, and writes the lease
// again only after them; tables are switched with definitions as in a
// teach.
func TestOutputRefresh(t *testing.T) {
	const refresh = 200 * time.Millisecond
	store := newStore(t)
	k1 := mustKey(t, "2001:db8:1::")
	if err := store.Set("t_out", k1, output.AggregateValues(5)); err != nil {
		t.Fatal(err)
	}
	m, _, addr := refreshingManager(t, store, refresh)
	f := connectFake(t, addr, "a")
	f.established()
	in := peermsg.NewInbound(8)
	isMeta := func(m daemonMsg) bool { u, ok := m.msg.(peermsg.UpdateMessage); return ok && u.Table == "t_meta" }
	g := status(t, m, "a").Stats.Generation
	upd := fmt.Sprintf("upd t_out %%d 2001:db8:1:: [2 5 %d 0]", g)
	// Long enough to outlast the test without renewal.
	if err := m.SetLease(time.Now().Add(output.MaxLeaseLength)); err != nil {
		t.Fatal(err)
	}
	f.define(2, aggDef)
	f.define(3, metaDef)
	got := summarize(t, readDaemon(f, in, isMeta))
	mark := func(id int) string { return fmt.Sprintf("mark t_meta %d %d", id, g) }
	if want := []string{"def t_out/1", fmt.Sprintf(upd, 1), "def t_meta/2", mark(1)}; !slices.Equal(got, want) {
		t.Fatalf("teach sent %q, want %q", got, want)
	}
	taught := time.Now()
	for i := 2; i <= 3; i++ {
		// (Within the fake configuration's 300 ms idle timeout.)
		f.control(peerwire.ControlHeartbeat)
		got = summarize(t, readDaemon(f, in, isMeta))
		if want := []string{"def t_out/1", fmt.Sprintf(upd, i), "def t_meta/2", mark(i)}; !slices.Equal(got, want) {
			t.Fatalf("refresh %d sent %q, want %q", i-1, got, want)
		}
	}
	if took := time.Since(taught); took < 2*refresh-50*time.Millisecond || took > 2*refresh+time.Second {
		t.Fatalf("two refreshes took %v, want about %v", took, 2*refresh)
	}
	if st := status(t, m, "a").Stats; st.Refreshes < 2 || st.Teaches != 2 {
		t.Fatalf("stats %+v", st)
	}
}

// TestOutputRetire checks that retiring a key makes the session switch
// to a new generation and re-send the remaining output under it before the
// next marker, never sending the retired key again.
func TestOutputRetire(t *testing.T) {
	store := newStore(t)
	k1, k2 := mustKey(t, "2001:db8:1::"), mustKey(t, "2001:db8:2::")
	for _, k := range []peermsg.Key{k1, k2} {
		if err := store.Set("t_out", k, output.AggregateValues(5)); err != nil {
			t.Fatal(err)
		}
	}
	m, _, addr := outputManager(t, store)
	f := connectFake(t, addr, "a")
	f.established()
	in := peermsg.NewInbound(8)
	isMeta := func(m daemonMsg) bool { u, ok := m.msg.(peermsg.UpdateMessage); return ok && u.Table == "t_meta" }
	if err := m.SetLease(time.Now().Add(output.MaxLeaseLength)); err != nil {
		t.Fatal(err)
	}
	g := status(t, m, "a").Stats.Generation
	f.define(2, aggDef)
	f.define(3, metaDef)
	got := summarize(t, readDaemon(f, in, isMeta))
	want := []string{
		"def t_out/1", fmt.Sprintf("upd t_out 1 2001:db8:1:: [2 5 %d 0]", g),
		fmt.Sprintf("upd t_out 2 2001:db8:2:: [2 5 %d 0]", g), "def t_meta/2", fmt.Sprintf("mark t_meta 1 %d", g),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("teach sent %q, want %q", got, want)
	}
	if err := m.Retire("t_out", k2); err != nil {
		t.Fatal(err)
	}
	got = summarize(t, readDaemon(f, in, isMeta))
	st := status(t, m, "a").Stats
	g2 := st.Generation
	want = []string{
		"def t_out/1", fmt.Sprintf("upd t_out 3 2001:db8:1:: [2 5 %d 0]", g2), "def t_meta/2",
		fmt.Sprintf("mark t_meta 2 %d", g2),
	}
	if g2 == g || st.Rotations != 1 || !slices.Equal(got, want) {
		t.Fatalf("after Retire (generation %d -> %d, %d rotations) sent %q, want %q", g, g2, st.Rotations, got, want)
	}
}
