package peermsg_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire/peertest"
)

// Capture fixtures (format: package peertest) are written only by
// TestLiveTableCapture and are never edited by hand.
const captureDir = "testdata/captures"

// Versions with committed fixtures. Each must have every case.
var captureVersions = []string{"3.4.6", "3.2.25"}

// Fixture case names.
const (
	casePush       = "push"
	caseCollisionA = "collision-a"
	caseCollisionB = "collision-b"
)

// Tables of the capture configuration.
const (
	tableIn  = "t_in"
	tableOut = "t_out"
)

// replyPrefix marks a note holding one line of a runtime command's reply.
const replyPrefix = "reply: "

// The tables' configured schemas, as HAProxy must announce them.
var (
	defIn = peermsg.Definition{
		Name: tableIn, KeyType: peermsg.KeyTypeIPv6, Expiry: 30000,
		Fields: []peermsg.Field{
			{Type: peermsg.DataHTTPReqCnt},
			{Type: peermsg.DataHTTPReqRate, Period: 10000},
		},
	}
	defOut = peermsg.Definition{
		Name: tableOut, KeyType: peermsg.KeyTypeIPv6, Expiry: 30000,
		Fields: []peermsg.Field{{Type: peermsg.DataGPT, ArrayLen: 3}},
	}
)

// Traffic of the push case. The lab frontend keys IPv6 clients by /64 and
// IPv4 clients by address, stored IPv4-mapped.
var (
	pushTraffic = []struct {
		ip    string
		count int
	}{
		{"2001:db8:1:1::5", 3},
		{"2001:db8:1:1:ffff::9", 2},
		{"192.0.2.1", 4},
		{otherClient, 1},
	}
	nativeClient = "2001:db8:1:1::5"
	otherClient  = "2001:db8:1:2::1"
	keyNative    = mustKey("2001:db8:1:1::")
	keyMapped    = mustKey("::ffff:192.0.2.1")
	keyOther     = mustKey("2001:db8:1:2::")
	pushCounts   = map[peermsg.Key]uint32{keyNative: 5, keyMapped: 4, keyOther: 1}
	presetGPT    = []uint32{7, 0, 4294967295}
)

// collisionSources gives both sources table ID 1, for different tables.
var collisionSources = []struct {
	node, caseName string
	tables         []string
	count          int
}{
	{"srca", caseCollisionA, []string{tableIn, tableOut}, 10},
	{"srcb", caseCollisionB, []string{tableOut, tableIn}, 20},
}

// Output updates the push case sends to HAProxy's t_out, announced under
// local table ID 1 (the number HAProxy uses for t_in in the other
// direction). The update IDs cross the 32-bit wrap; the last two are
// implicit (0 and then 1). keyCompat is IPv4-compatible and keyMappedOut
// IPv4-mapped with the same embedded address: distinct keys.
var (
	keyAA        = mustKey("2001:db8:aa::")
	keyMappedOut = mustKey("::ffff:198.51.100.7")
	keyBB        = mustKey("2001:db8:bb::")
	keyCompat    = mustKey("::198.51.100.7")
	outUpdates   = []peermsg.Update{
		{ID: 0xfffffffe, ExplicitID: true, Key: keyAA, Values: gpt(1, 2, 3)},
		{ID: 0xffffffff, ExplicitID: true, Timed: true, Remaining: 4000, Key: keyMappedOut, Values: gpt(4294967295, 0, 7)},
		{ID: 0, Key: keyBB, Values: gpt(9, 9, 9)},
		{ID: 1, Key: keyCompat, Values: gpt(5, 6, 8)},
	}
)

const outLocalID peermsg.LocalTableID = 1

type outputStep struct {
	what string
	msg  []byte
	ack  bool        // an update: HAProxy acks it
	key  peermsg.Key // the updated key, when ack is set
}

// outEntryRead is the runtime command that reads one t_out entry right
// after HAProxy acknowledges its update.
func outEntryRead(k peermsg.Key) string { return "show table " + tableOut + " key " + k.String() }

func outputSteps(tb testing.TB) []outputStep {
	tb.Helper()
	def, err := peermsg.AppendDefinition(nil, outLocalID, defOut)
	if err != nil {
		tb.Fatal(err)
	}
	steps := []outputStep{{what: "definition of t_out as local table 1", msg: def}}
	for _, u := range outUpdates {
		b, err := peermsg.AppendUpdate(nil, defOut, u)
		if err != nil {
			tb.Fatal(err)
		}
		steps = append(steps, outputStep{
			what: fmt.Sprintf("update %#x (explicit %v, timed %v) for %v", uint32(u.ID), u.ExplicitID, u.Timed, u.Key),
			msg:  b, ack: true, key: u.Key,
		})
	}
	return steps
}

func gpt(v ...uint32) []peermsg.Value {
	return []peermsg.Value{{Type: peermsg.DataGPT, Array: v}}
}

func mustKey(s string) peermsg.Key {
	k, err := peermsg.KeyFromAddr(netip.MustParseAddr(s))
	if err != nil {
		panic(err)
	}
	return k
}

// wantAcks is HAProxy's acknowledgement of each output update when it
// runs on a host of the given byte order. HAProxy 3.4.6 derives every
// implicit ID as last_get + 1 and stores it with htonl, so on a
// little-endian host every implicit ID is recorded and acknowledged
// byte-swapped. Here the first implicit ID is 0, which reads the same in
// both byte orders; the second (1) is acknowledged as 0x01000000. On a
// big-endian host htonl is the identity and it acknowledges 1. 3.2.25
// increments last_get and acknowledges 1 either way.
func wantAcks(version string, littleEndian bool) []peermsg.UpdateID {
	last := peermsg.UpdateID(1)
	if strings.HasPrefix(version, "3.4.") && littleEndian {
		last = 0x01000000
	}
	return []peermsg.UpdateID{0xfffffffe, 0xffffffff, 0, last}
}

// fixturesLittleEndian records that the committed fixtures were captured
// with HAProxy on a little-endian (amd64) host, whatever host replays them.
const fixturesLittleEndian = true

// hostLittleEndian reports the byte order of the host running the live
// capture, which is also HAProxy's.
func hostLittleEndian() bool { return binary.NativeEndian.Uint16([]byte{1, 0}) == 1 }

// decoded is one frame of a capture stream and what Inbound made of it.
type decoded struct {
	frame    peerwire.Frame
	msg      peermsg.Message
	err      error
	received time.Time
}

// receivedBase is the synthetic reception time of a replayed stream's
// first frame; each later frame is one millisecond later.
var receivedBase = time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)

// decodeStream frames a capture stream after its handshake lines and
// decodes every frame through a fresh Inbound.
func decodeStream(t *testing.T, stream []byte, lines int) ([]decoded, *peermsg.Inbound) {
	t.Helper()
	d, err := peerwire.NewDecoder(peerwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Feed(stream); err != nil {
		t.Fatal(err)
	}
	d.CloseInput()
	for i := range lines {
		if _, err := d.NextLine(); err != nil {
			t.Fatalf("handshake line %d: %v", i+1, err)
		}
	}
	in := peermsg.NewInbound(8)
	var out []decoded
	for i := 0; ; i++ {
		f, err := d.NextFrame()
		if errors.Is(err, io.EOF) {
			return out, in
		}
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		f.Body = append([]byte(nil), f.Body...)
		f.Raw = append([]byte(nil), f.Raw...)
		rx := receivedBase.Add(time.Duration(i) * time.Millisecond)
		m, err := in.Decode(f, rx)
		out = append(out, decoded{frame: f, msg: m, err: err, received: rx})
	}
}

// runtimeReplies returns the reply lines of every runtime command noted
// in c that is exactly cmd, in order.
func runtimeReplies(c *peertest.Capture, cmd string) [][]string {
	var out [][]string
	var cur *[]string
	for _, n := range c.Notes() {
		switch {
		case strings.HasPrefix(n, "runtime: "):
			cur = nil
			if strings.TrimPrefix(n, "runtime: ") == cmd {
				out = append(out, nil)
				cur = &out[len(out)-1]
			}
		case strings.HasPrefix(n, replyPrefix) && cur != nil:
			*cur = append(*cur, strings.TrimPrefix(n, replyPrefix))
		}
	}
	return out
}

// showTables parses every noted "show table <name>" reply, in order.
func showTables(t *testing.T, c *peertest.Capture, name string) []lab.Table {
	t.Helper()
	return showReads(t, c, "show table "+name)
}

// showReads parses every noted reply to the show table command cmd.
func showReads(t *testing.T, c *peertest.Capture, cmd string) []lab.Table {
	t.Helper()
	var out []lab.Table
	for _, r := range runtimeReplies(c, cmd) {
		tab, err := lab.ParseTable(strings.Join(r, "\n"))
		if err != nil {
			t.Fatalf("%s: %v", cmd, err)
		}
		out = append(out, tab)
	}
	return out
}

func entry(t *testing.T, tab lab.Table, k peermsg.Key) lab.Entry {
	t.Helper()
	for _, e := range tab.Entries {
		a, err := netip.ParseAddr(e.Key)
		if err != nil {
			t.Fatalf("show table %s: key %q: %v", tab.Name, e.Key, err)
		}
		if a.As16() == k {
			return e
		}
	}
	t.Fatalf("show table %s has no entry for %v: %v", tab.Name, k, tab.Keys())
	return lab.Entry{}
}

func TestTableCaptureFixtures(t *testing.T) {
	for _, v := range captureVersions {
		load := func(t *testing.T, name string) *peertest.Capture {
			t.Helper()
			f, err := os.Open(filepath.Join(captureDir, v, name+".txt"))
			if err != nil {
				t.Fatalf("fixture missing; regenerate with `make peerwire-captures`: %v", err)
			}
			defer func() { _ = f.Close() }()
			c, err := peertest.Parse(f)
			if err != nil {
				t.Fatal(err)
			}
			if got := c.Get("haproxy-version"); !strings.HasPrefix(got, v+"-") && got != v {
				t.Fatalf("fixture records haproxy %q", got)
			}
			return c
		}
		t.Run(v+"/"+casePush, func(t *testing.T) {
			c := load(t, casePush)
			verifyPush(t, c, wantAcks(c.Get("haproxy-version"), fixturesLittleEndian))
		})
		t.Run(v+"/collision", func(t *testing.T) {
			verifyCollision(t, load(t, caseCollisionA), load(t, caseCollisionB))
		})
	}
}

// checkReencode requires this package's encoder to reproduce every
// definition and update HAProxy sent, byte for byte.
func checkReencode(t *testing.T, msgs []decoded) {
	t.Helper()
	defs := map[peermsg.RemoteTableID]peermsg.Definition{}
	for i, d := range msgs {
		var b []byte
		var err error
		switch m := d.msg.(type) {
		case peermsg.DefinitionMessage:
			defs[m.ID] = m.Definition
			// The ID is HAProxy's local ID; re-encoding as if it were ours
			// must give HAProxy's bytes.
			b, err = peermsg.AppendDefinition(nil, peermsg.LocalTableID(m.ID), m.Definition)
		case peermsg.UpdateMessage:
			b, err = peermsg.AppendUpdate(nil, defs[m.ID], m.Update)
		case peermsg.AckMessage:
			b, err = peermsg.AppendAck(nil, peermsg.RemoteTableID(m.ID), m.Update)
		default:
			continue
		}
		if err != nil || string(b) != string(d.frame.Raw) {
			t.Fatalf("frame %d: re-encoded as %x (%v), HAProxy sent %x", i, b, err, d.frame.Raw)
		}
	}
}

// checkLastGet reads HAProxy's own record of the last update ID it
// received for t_out, from "show peers": the shared-table line for the
// table bound to the test peer's ID 1 is followed by its cursors.
func checkLastGet(t *testing.T, c *peertest.Capture, want []peermsg.UpdateID) {
	t.Helper()
	replies := runtimeReplies(c, "show peers")
	if len(replies) != 1 {
		t.Fatalf("%d show peers replies", len(replies))
	}
	lines := replies[0]
	for i, line := range lines {
		if !strings.Contains(line, "local_id=2 remote_id=1 ") || i+1 == len(lines) {
			continue
		}
		for field := range strings.FieldsSeq(lines[i+1]) {
			if v, ok := strings.CutPrefix(field, "last_get="); ok {
				if v != strconv.FormatUint(uint64(want[len(want)-1]), 10) {
					t.Fatalf("show peers last_get=%s, want %d", v, want[len(want)-1])
				}
				return
			}
		}
	}
	t.Fatalf("no last_get for t_out in show peers: %q", lines)
}

// pushPhases splits the push capture's updates: the initial push (before
// the first timed update), the timed resync, and live updates after it.
type pushPhases struct {
	push, resync, live []peermsg.UpdateMessage
	acks               []peermsg.AckMessage
	defs               []peermsg.DefinitionMessage
}

func splitPush(t *testing.T, msgs []decoded) pushPhases {
	t.Helper()
	var p pushPhases
	phase := 0
	for i, d := range msgs {
		if d.err != nil {
			t.Fatalf("frame %d %v: %v", i, d.frame, d.err)
		}
		switch m := d.msg.(type) {
		case peermsg.DefinitionMessage:
			p.defs = append(p.defs, m)
		case peermsg.UpdateMessage:
			if !m.Update.Received.Equal(d.received) {
				t.Fatalf("update received %v, want %v", m.Update.Received, d.received)
			}
			switch {
			case m.Update.Timed:
				if phase == 2 {
					t.Fatalf("timed update after the live phase: %+v", m)
				}
				phase = 1
				p.resync = append(p.resync, m)
			case phase == 0:
				p.push = append(p.push, m)
			default:
				phase = 2
				p.live = append(p.live, m)
			}
		case peermsg.AckMessage:
			p.acks = append(p.acks, m)
		case peermsg.Control, peermsg.ErrorMessage, peermsg.SwitchMessage:
		}
	}
	return p
}

func byKey(t *testing.T, us []peermsg.UpdateMessage, table string) map[peermsg.Key]peermsg.UpdateMessage {
	t.Helper()
	out := map[peermsg.Key]peermsg.UpdateMessage{}
	for _, u := range us {
		if u.Table != table {
			continue
		}
		if _, dup := out[u.Update.Key]; dup {
			t.Fatalf("%s: two updates for %v in one phase", table, u.Update.Key)
		}
		out[u.Update.Key] = u
	}
	return out
}

func checkDefinition(t *testing.T, got peermsg.DefinitionMessage, id peermsg.RemoteTableID, want peermsg.Definition) {
	t.Helper()
	if got.ID != id || fmt.Sprint(got.Definition) != fmt.Sprint(want) {
		t.Fatalf("definition %d %+v, want %d %+v", got.ID, got.Definition, id, want)
	}
}

// checkInEntry compares a decoded t_in update with the runtime read e of
// the same entry, which saw count requests.
//
// With firstPeriod, e was taken right after the traffic and before the
// update was sent, inside the counter's first period: all events are then
// in Curr, and HAProxy's printed estimate equals it. Otherwise e was taken
// at an unknown point after the update, possibly periods later, so only
// what holds at any time is checked: the events are split between Curr
// and Prev, and the decaying estimate never exceeds them. The wire state
// itself does not rotate without events; HAProxy applies the counter's
// age when it reads it.
func checkInEntry(t *testing.T, u peermsg.Update, e lab.Entry, count uint32, firstPeriod bool) {
	t.Helper()
	cnt, _ := u.Value(peermsg.DataHTTPReqCnt)
	rate, _ := u.Value(peermsg.DataHTTPReqRate)
	if cnt.Uint != count || int64(cnt.Uint) != e.Data["http_req_cnt"] {
		t.Fatalf("%v: http_req_cnt %d, runtime %d, want %d", u.Key, cnt.Uint, e.Data["http_req_cnt"], count)
	}
	shown := e.Data["http_req_rate(10000)"]
	f := rate.Freq
	if firstPeriod {
		if f.Prev != 0 || f.Curr != count || int64(f.Curr) != shown {
			t.Fatalf("%v: http_req_rate %+v, runtime %d in the first period", u.Key, f, shown)
		}
		return
	}
	if uint64(f.Curr)+uint64(f.Prev) != uint64(count) || shown < 0 || shown > int64(count) {
		t.Fatalf("%v: http_req_rate %+v for %d events, runtime estimate %d", u.Key, f, count, shown)
	}
}

// checkLaterSend checks that later is the same unchanged t_in entry as
// earlier, sent again later: identical counts, and a counter age that
// did not shrink, since both come from the sender's one clock.
func checkLaterSend(t *testing.T, earlier, later peermsg.Update) {
	t.Helper()
	if !sameCounts(earlier, later) {
		t.Fatalf("%v: resend changed counts: %+v then %+v", earlier.Key, earlier.Values, later.Values)
	}
	ea, _ := earlier.Value(peermsg.DataHTTPReqRate)
	la, _ := later.Value(peermsg.DataHTTPReqRate)
	if la.Freq.Age < ea.Freq.Age {
		t.Fatalf("%v: counter age went from %v back to %v", earlier.Key, ea.Freq.Age, la.Freq.Age)
	}
}

// verifyPush checks the push capture; acks is HAProxy's expected
// acknowledgement of each output update (see wantAcks).
func verifyPush(t *testing.T, c *peertest.Capture, acks []peermsg.UpdateID) {
	t.Helper()
	rx, _ := decodeStream(t, c.Stream(peertest.KindRx), 1)
	checkReencode(t, rx)
	p := splitPush(t, rx)
	if len(p.defs) < 2 {
		t.Fatalf("%d definitions", len(p.defs))
	}
	for _, d := range p.defs {
		switch d.Definition.Name {
		case tableIn:
			checkDefinition(t, d, 1, defIn)
		case tableOut:
			checkDefinition(t, d, 2, defOut)
		default:
			t.Fatalf("definition of unexpected table %q", d.Definition.Name)
		}
	}
	shownIn := showTables(t, c, tableIn)
	shownOut := showTables(t, c, tableOut)
	if len(shownIn) != 3 || len(shownOut) != 1 {
		t.Fatalf("%d t_in and %d t_out runtime reads", len(shownIn), len(shownOut))
	}

	push, resync := byKey(t, p.push, tableIn), byKey(t, p.resync, tableIn)
	implicit := 0
	for k, count := range pushCounts {
		pu, ok := push[k]
		if !ok {
			t.Fatalf("no pushed update for %v", k)
		}
		checkInEntry(t, pu.Update, entry(t, shownIn[0], k), count, true)
		if pu.Update.Lifetime(defIn.Expiry) != defIn.Expiry {
			t.Fatalf("ordinary update lifetime %v", pu.Update.Lifetime(defIn.Expiry))
		}
		ru, ok := resync[k]
		if !ok {
			t.Fatalf("no timed update for %v", k)
		}
		// The resync replays the unchanged entry: same counts, an age
		// that kept growing, and the same update ID, whether explicit or
		// derived; and it agrees with the read taken after it.
		checkLaterSend(t, pu.Update, ru.Update)
		checkInEntry(t, ru.Update, entry(t, shownIn[1], k), count, false)
		if ru.Update.ID != pu.Update.ID {
			t.Fatalf("%v: resync update ID %#x, push %#x", k, uint32(ru.Update.ID), uint32(pu.Update.ID))
		}
		// HAProxy sent the remaining lifetime before the later runtime
		// read, so it cannot be shorter than what the read shows.
		exp := entry(t, shownIn[1], k).Exp
		if ru.Update.Remaining > defIn.Expiry || ru.Update.Remaining.Duration() < exp {
			t.Fatalf("%v: remaining %v, table expiry %v, later runtime exp %v", k, ru.Update.Remaining, defIn.Expiry, exp)
		}
		if ru.Update.Lifetime(defIn.Expiry) != ru.Update.Remaining {
			t.Fatalf("timed update lifetime %v, remaining %v", ru.Update.Lifetime(defIn.Expiry), ru.Update.Remaining)
		}
		for _, u := range []peermsg.Update{pu.Update, ru.Update} {
			if !u.ExplicitID {
				implicit++
			}
		}
	}
	if implicit == 0 {
		t.Fatal("no implicit update ID was exercised")
	}
	checkPushOutput(t, byKey(t, p.push, tableOut), shownOut[0])
	checkLive(t, p, push, shownIn[2])
	checkOutput(t, c, p.acks, acks)
	checkLastGet(t, c, acks)
}

func checkPushOutput(t *testing.T, push map[peermsg.Key]peermsg.UpdateMessage, shown lab.Table) {
	t.Helper()
	u, ok := push[keyNative]
	if !ok {
		t.Fatal("no pushed t_out update")
	}
	v, _ := u.Update.Value(peermsg.DataGPT)
	e := entry(t, shown, keyNative)
	if fmt.Sprint(v.Array) != fmt.Sprint(presetGPT) {
		t.Fatalf("t_out gpt %v, want %v", v.Array, presetGPT)
	}
	for i, want := range presetGPT {
		if got := e.Data[gptField(i)]; got != int64(want) {
			t.Fatalf("runtime gpt[%d] %d, want %d (%v)", i, got, want, e.Data)
		}
	}
}

// gptField is how "show table" names element i of a gpt array.
func gptField(i int) string { return "gpt" + strconv.Itoa(i) }

// sameCounts reports whether two t_in updates carry the same counts; the
// counter age legitimately grows between them.
func sameCounts(a, b peermsg.Update) bool {
	ac, _ := a.Value(peermsg.DataHTTPReqCnt)
	bc, _ := b.Value(peermsg.DataHTTPReqCnt)
	ar, _ := a.Value(peermsg.DataHTTPReqRate)
	br, _ := b.Value(peermsg.DataHTTPReqRate)
	return ac.Uint == bc.Uint && ar.Freq.Curr == br.Freq.Curr && ar.Freq.Prev == br.Freq.Prev
}

// checkLive checks the ordinary updates after the resync. Confirming the
// resync makes HAProxy re-push its tables from the resync's origin, so
// unchanged entries are replayed with their original update IDs; only the
// entry the extra request changed gets a new ID and count.
func checkLive(t *testing.T, p pushPhases, push map[peermsg.Key]peermsg.UpdateMessage, shown lab.Table) {
	t.Helper()
	var changed *peermsg.Update
	for _, u := range p.live {
		if u.Table != tableIn {
			continue
		}
		if u.Update.Timed || u.Update.Lifetime(defIn.Expiry) != defIn.Expiry {
			t.Fatalf("live update is timed or has lifetime %v", u.Update.Lifetime(defIn.Expiry))
		}
		pu, ok := push[u.Update.Key]
		if !ok {
			t.Fatalf("live update for unknown key %v", u.Update.Key)
		}
		if u.Update.ID == pu.Update.ID {
			checkLaterSend(t, pu.Update, u.Update)
			continue
		}
		if u.Update.Key != keyOther || changed != nil {
			t.Fatalf("unexpected new update %#x for %v", uint32(u.Update.ID), u.Update.Key)
		}
		changed = &u.Update
	}
	if changed == nil {
		t.Fatal("no live update for the changed entry")
	}
	checkInEntry(t, *changed, entry(t, shown, keyOther), 2, false)
}

// checkOutput checks the output messages this package encoded and
// HAProxy's handling of them: its acknowledgements and table contents.
func checkOutput(t *testing.T, c *peertest.Capture, acks []peermsg.AckMessage, want []peermsg.UpdateID) {
	t.Helper()
	var sent []byte
	for _, s := range outputSteps(t) {
		sent = append(sent, s.msg...)
	}
	if !strings.Contains(string(c.Stream(peertest.KindTx)), string(sent)) {
		t.Fatal("tx stream does not hold the current encoding of the output messages")
	}
	tx, _ := decodeStream(t, c.Stream(peertest.KindTx), 3)
	var echoed []peermsg.Update
	for i, d := range tx {
		if d.err != nil {
			t.Fatalf("tx frame %d: %v", i, d.err)
		}
		switch m := d.msg.(type) {
		case peermsg.DefinitionMessage:
			checkDefinition(t, m, peermsg.RemoteTableID(outLocalID), defOut)
		case peermsg.UpdateMessage:
			echoed = append(echoed, m.Update)
		case peermsg.Control, peermsg.ErrorMessage, peermsg.SwitchMessage, peermsg.AckMessage:
		}
	}
	if len(echoed) != len(outUpdates) {
		t.Fatalf("%d output updates decoded, want %d", len(echoed), len(outUpdates))
	}
	for i, u := range echoed {
		w := outUpdates[i]
		if u.ID != w.ID || u.ExplicitID != w.ExplicitID || u.Timed != w.Timed || u.Remaining != w.Remaining ||
			u.Key != w.Key || fmt.Sprint(u.Values) != fmt.Sprint(w.Values) {
			t.Fatalf("output update %d decodes as %+v, want %+v", i, u, w)
		}
	}

	if len(acks) != len(want) {
		t.Fatalf("acks %+v, want %v", acks, want)
	}
	for i, a := range acks {
		if a.ID != outLocalID || a.Update != want[i] {
			t.Fatalf("ack %d: table %d update %#x, want table %d update %#x", i, a.ID, uint32(a.Update), outLocalID, uint32(want[i]))
		}
	}

	// Each entry was read right after HAProxy acknowledged it. The
	// IPv4-mapped and IPv4-compatible keys read back as separate entries
	// with their own values.
	for _, u := range outUpdates {
		reads := showReads(t, c, outEntryRead(u.Key))
		if len(reads) != 1 {
			t.Fatalf("%v: %d runtime reads", u.Key, len(reads))
		}
		e := entry(t, reads[0], u.Key)
		for i, want := range u.Values[0].Array {
			if got := e.Data[gptField(i)]; got != int64(want) {
				t.Fatalf("%v: runtime gpt[%d] %d, want %d", u.Key, i, got, want)
			}
		}
		// HAProxy restarts the full table expiry for an ordinary update
		// and uses the sent remaining lifetime for a timed one.
		limit := defOut.Expiry.Duration()
		if u.Timed {
			limit = u.Remaining.Duration()
		}
		if e.Exp > limit || (!u.Timed && e.Exp <= outUpdates[1].Remaining.Duration()) {
			t.Fatalf("%v: runtime exp %v, timed %v, limit %v", u.Key, e.Exp, u.Timed, limit)
		}
	}
}

// verifyCollision checks two sources whose table ID 1 names different
// tables (t_in on A, t_out on B) and whose t_in counts for one key differ.
// Each stream is decoded in its own namespace, and every update must be
// attributed to the table its own source bound the ID to.
func verifyCollision(t *testing.T, a, b *peertest.Capture) {
	t.Helper()
	srcs := []struct {
		c           *peertest.Capture
		inID, outID peermsg.RemoteTableID
		count       uint32
	}{{a, 1, 2, 10}, {b, 2, 1, 20}}
	for _, s := range srcs {
		name := s.c.Get("case")
		msgs, in := decodeStream(t, s.c.Stream(peertest.KindRx), 1)
		checkReencode(t, msgs)
		shown := showTables(t, s.c, tableIn)
		if len(shown) != 1 {
			t.Fatalf("%s: %d runtime reads", name, len(shown))
		}
		var gotIn, gotOut bool
		for i, d := range msgs {
			if d.err != nil {
				t.Fatalf("%s frame %d: %v", name, i, d.err)
			}
			u, ok := d.msg.(peermsg.UpdateMessage)
			if !ok || u.Update.Key != keyNative {
				continue
			}
			switch {
			case u.ID == s.inID && u.Table == tableIn:
				checkInEntry(t, u.Update, entry(t, shown[0], keyNative), s.count, true)
				gotIn = true
			case u.ID == s.outID && u.Table == tableOut:
				v, _ := u.Update.Value(peermsg.DataGPT)
				if fmt.Sprint(v.Array) != fmt.Sprint([]uint32{0, s.count, 0}) {
					t.Fatalf("%s: t_out gpt %v", name, v.Array)
				}
				gotOut = true
			default:
				t.Fatalf("%s: update attributed to table %d (%s)", name, u.ID, u.Table)
			}
		}
		if !gotIn || !gotOut {
			t.Fatalf("%s: t_in update %v, t_out update %v", name, gotIn, gotOut)
		}
		for id, want := range map[peermsg.RemoteTableID]string{s.inID: tableIn, s.outID: tableOut} {
			if tab, ok := in.Table(id); !ok || tab.Definition.Name != want {
				t.Fatalf("%s: table ID %d is %+v, want %s", name, id, tab, want)
			}
		}
	}
}
