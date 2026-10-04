package peerwire_test

import (
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire/peertest"
)

// Capture fixtures (format: package peertest) are written only by
// TestLiveCapture (see capture_live_test.go) and are never edited by hand.
const captureDir = "testdata/captures"

// Versions with committed fixtures. Each must have every case.
var captureVersions = []string{"3.4.6", "3.2.25"}

// Boundary values the live capture stores through the runtime API, keyed
// by integer stick-table key (index + 1). Expected bytes come from
// specEncodings, never from this package's encoder.
var (
	boundaryU32 = boundaryValues(func(v uint64) bool { return v <= math.MaxUint32 })
	boundaryU64 = boundaryValues(func(v uint64) bool { return v > math.MaxUint32 })
)

func boundaryValues(keep func(uint64) bool) []uint64 {
	var out []uint64
	for _, e := range specEncodings {
		if keep(e.v) {
			out = append(out, e.v)
		}
	}
	return out
}

func specHex(tb testing.TB, v uint64) []byte {
	tb.Helper()
	for _, e := range specEncodings {
		if e.v == v {
			return mustHex(tb, e.hex)
		}
	}
	tb.Fatalf("%#x is not in specEncodings", v)
	return nil
}

// Table names used by the capture configuration and fixture checks.
const (
	tableReq = "t_req"
	tableU32 = "t_u32"
	tableU64 = "t_u64"
)

// captureCase says how to frame and check one fixture.
type captureCase struct {
	// rxLines and txLines are the handshake lines that open each stream.
	rxLines, txLines int
	// status is the status line expected in whichever stream carries it.
	status peerwire.StatusCode
	// rxTail is the class/type pair of the last frame HAProxy sent, if it
	// ended the session with an error message. Otherwise HAProxy must
	// have sent no error message at all.
	rxTail []byte
	// txErr is what this package's decoder must report on the test
	// peer's stream: io.EOF if it is well-formed.
	txErr error
}

var captureCases = map[string]captureCase{
	"initiator":          {rxLines: 3, txLines: 1, status: peerwire.StatusSucceeded, txErr: io.EOF},
	"acceptor":           {rxLines: 1, txLines: 3, status: peerwire.StatusSucceeded, txErr: io.EOF},
	"status-501":         {rxLines: 1, txLines: 3, status: peerwire.StatusProtocolError, txErr: io.EOF},
	"status-502":         {rxLines: 1, txLines: 3, status: peerwire.StatusBadVersion, txErr: io.EOF},
	"status-503":         {rxLines: 1, txLines: 3, status: peerwire.StatusHostMismatch, txErr: io.EOF},
	"status-504":         {rxLines: 1, txLines: 3, status: peerwire.StatusUnknownPeer, txErr: io.EOF},
	"frame-at-bufsize":   {rxLines: 1, txLines: 3, status: peerwire.StatusSucceeded, rxTail: []byte{1, 0}, txErr: peerwire.ErrReservedClass},
	"frame-over-bufsize": {rxLines: 1, txLines: 3, status: peerwire.StatusSucceeded, txErr: peerwire.ErrReservedClass},
	"size-limit":         {rxLines: 1, txLines: 3, status: peerwire.StatusSucceeded, rxTail: []byte{1, 1}, txErr: peerwire.ErrMessageTooLarge},
	"unknown-messages":   {rxLines: 1, txLines: 3, status: peerwire.StatusSucceeded, rxTail: []byte{1, 0}, txErr: peerwire.ErrReservedClass},
	"reserved-class":     {rxLines: 1, txLines: 3, status: peerwire.StatusSucceeded, rxTail: []byte{1, 0}, txErr: peerwire.ErrReservedClass},
	"length-encoding":    {rxLines: 1, txLines: 3, status: peerwire.StatusSucceeded, rxTail: []byte{1, 0}, txErr: peerwire.ErrLengthEncoding},
}

func TestCaptureFixtures(t *testing.T) {
	for _, v := range captureVersions {
		for name := range captureCases {
			t.Run(v+"/"+name, func(t *testing.T) {
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
				verifyCapture(t, c)
			})
		}
	}
}

// verifyCapture frames both streams of c under every chunking and checks
// the case's expectations. It is shared by fixture replay and live runs.
func verifyCapture(t *testing.T, c *peertest.Capture) {
	t.Helper()
	name := c.Get("case")
	cc, ok := captureCases[name]
	if !ok {
		t.Fatalf("unknown case %q", name)
	}
	rx := frameEverySplit(t, "rx", c.Stream(peertest.KindRx), cc.rxLines)
	tx := frameEverySplit(t, "tx", c.Stream(peertest.KindTx), cc.txLines)
	if !errors.Is(rx.err, io.EOF) {
		t.Fatalf("rx stream: %v, want a clean end", rx.err)
	}
	if !errors.Is(tx.err, cc.txErr) {
		t.Fatalf("tx stream: %v, want %v", tx.err, cc.txErr)
	}

	statusStream, statusAt := rx, cc.rxLines-1
	if cc.rxLines == 3 {
		statusStream, statusAt = tx, 0
	}
	got, err := peerwire.ParseStatusLine(statusStream.events[statusAt].data)
	if err != nil || got != cc.status {
		t.Fatalf("status line %q: %d, %v; want %d", statusStream.events[statusAt].data, got, err, cc.status)
	}
	if cc.txLines == 3 && cc.status == peerwire.StatusSucceeded {
		checkHello(t, tx.events, "hap", "agg", 0)
	}
	if cc.rxLines == 3 {
		pid, err := strconv.ParseUint(c.Get("haproxy-pid"), 10, 32)
		if err != nil {
			t.Fatalf("haproxy-pid: %v", err)
		}
		checkHello(t, rx.events, "agg", "hap", uint32(pid))
	}

	frames := rx.events[cc.rxLines:]
	if cc.status != peerwire.StatusSucceeded && len(frames) != 0 {
		t.Fatalf("HAProxy sent %d frames after status %d", len(frames), cc.status)
	}
	before := frames
	if cc.rxTail != nil {
		if len(frames) == 0 || !bytes.Equal(frames[len(frames)-1].data, cc.rxTail) {
			t.Fatalf("last rx frame is not %x: %v", cc.rxTail, frames)
		}
		before = frames[:len(frames)-1]
	}
	if hasClass(before, peerwire.ClassError) {
		t.Fatalf("unexpected error message from HAProxy: %v", before)
	}
	switch name {
	case "initiator":
		checkInitiatorFrames(t, frames)
	case "acceptor":
		if len(frames) == 0 || frames[0].start != statusStream.events[0].end {
			t.Fatalf("no binary message directly after the status line")
		}
		if !hasFrame(frames, peerwire.ClassControl, peerwire.ControlHeartbeat) {
			t.Errorf("no heartbeat")
		}
		checkBoundaryUpdates(t, frames)
	case "frame-at-bufsize", "frame-over-bufsize":
		// HAProxy's default tune.bufsize. A message is processed only
		// once all of it fits in that buffer: the protocol error in
		// frame-at-bufsize proves the preceding 16384-byte message was
		// consumed, while one byte more stalls the session until HAProxy
		// times it out, without an error message.
		const bufsize = 16384
		want := bufsize
		if name == "frame-over-bufsize" {
			want++
		}
		if !slices.ContainsFunc(tx.events[cc.txLines:], func(e event) bool { return len(e.data) == want }) {
			t.Fatalf("tx stream has no %d-byte message", want)
		}
	}
}

// frameEverySplit frames a stream whole, byte by byte, and split in two at
// every offset, and requires identical results that preserve every byte.
func frameEverySplit(t *testing.T, label string, input []byte, lines int) outcome {
	t.Helper()
	lim := peerwire.DefaultLimits()
	var want outcome
	for i, chunks := range splits(input) {
		got, err := decodeChunks(lim, chunks, lines)
		if err != nil {
			t.Fatalf("%s: %d chunks: %v", label, len(chunks), err)
		}
		if i == 0 {
			want = got
			if err := checkPreserved(input, want); err != nil {
				t.Fatalf("%s: %v", label, err)
			}
			if len(want.events) < lines {
				t.Fatalf("%s: %d handshake lines, want %d (%v)", label, len(want.events), lines, want.err)
			}
			continue
		}
		if !got.equal(want) {
			t.Fatalf("%s: %d chunks (first %d bytes) differ: %v / %v vs %v / %v",
				label, len(chunks), len(chunks[0]), got.events, got.err, want.events, want.err)
		}
	}
	return want
}

func checkHello(t *testing.T, events []event, remote, local string, pid uint32) {
	t.Helper()
	h, err := peerwire.ParseHello(events[0].data, events[1].data, events[2].data)
	if err != nil {
		t.Fatalf("hello: %v", err)
	}
	if h.Version != peerwire.ProtocolVersion || h.RemotePeer != remote || h.LocalPeer != local || h.RelativePID != 1 ||
		(pid != 0 && h.PID != pid) {
		t.Fatalf("hello %+v, want version %v, %s -> %s, pid %d, relative 1", h, peerwire.ProtocolVersion, local, remote, pid)
	}
}

func hasClass(frames []event, class peerwire.MessageClass) bool {
	return slices.ContainsFunc(frames, func(e event) bool { return e.class == class })
}

func hasFrame(frames []event, class peerwire.MessageClass, typ peerwire.MessageType) bool {
	return slices.ContainsFunc(frames, func(e event) bool { return e.class == class && e.typ == typ })
}

// checkInitiatorFrames checks the binary part of the initiator capture:
// the control exchange and the boundary updates.
func checkInitiatorFrames(t *testing.T, frames []event) {
	t.Helper()
	for _, want := range []peerwire.MessageType{peerwire.ControlResyncRequest, peerwire.ControlResyncConfirm, peerwire.ControlHeartbeat} {
		if !hasFrame(frames, peerwire.ClassControl, want) {
			t.Errorf("no control message %d", want)
		}
	}
	checkBoundaryUpdates(t, frames)
}

// checkBoundaryUpdates checks that every capture table is defined and
// that HAProxy's own encoding of every boundary value matches
// specEncodings. Locating the
// value inside an update relies on the capture's fixed schema (integer
// key, one data type), not on a general entry decoder: an update body is
// [4-byte update ID if 0x80/0x85][4-byte expiry if 0x85/0x86][4-byte key]
// [encoded value]. The key bytes are checked too, so a misread layout
// fails rather than passes.
func checkBoundaryUpdates(t *testing.T, frames []event) {
	t.Helper()
	defined := map[string]bool{}
	seen := map[string]map[uint32]bool{tableU32: {}, tableU64: {}}
	values := map[string][]uint64{tableU32: boundaryU32, tableU64: boundaryU64}
	var table string
	for _, f := range frames {
		if f.class != peerwire.ClassStickTable {
			continue
		}
		c := peerwire.NewCursor(f.body)
		switch f.typ {
		case peerwire.StickTableDefine:
			if _, err := c.Uint32(); err != nil {
				t.Fatalf("definition %v: table ID: %v", f, err)
			}
			n, err := c.Uint()
			if err != nil {
				t.Fatalf("definition %v: name length: %v", f, err)
			}
			name, err := c.Bytes(n)
			if err != nil {
				t.Fatalf("definition %v: name: %v", f, err)
			}
			table = string(name)
			defined[table] = true
		case peerwire.StickTableUpdate, peerwire.StickTableIncrementalUpdate,
			peerwire.StickTableTimedUpdate, peerwire.StickTableIncrementalTimedUpdate:
			vals, ok := values[table]
			if !ok {
				continue
			}
			if f.typ == peerwire.StickTableUpdate || f.typ == peerwire.StickTableTimedUpdate {
				if _, err := c.Fixed32(); err != nil {
					t.Fatalf("%v: update ID: %v", f, err)
				}
			}
			if f.typ == peerwire.StickTableTimedUpdate || f.typ == peerwire.StickTableIncrementalTimedUpdate {
				if _, err := c.Fixed32(); err != nil {
					t.Fatalf("%v: expiry: %v", f, err)
				}
			}
			key, err := c.Fixed32()
			if err != nil || key == 0 || int(key) > len(vals) {
				t.Fatalf("%s update %v: key %d, %v", table, f, key, err)
			}
			want := vals[key-1]
			raw := c.Rest()
			got, n, err := peerwire.DecodeUint(raw)
			if err != nil || n != len(raw) || got != want || !bytes.Equal(raw, specHex(t, want)) {
				t.Fatalf("%s key %d: HAProxy sent %x (%#x, %d/%d bytes, %v); spec encoding of %#x is %x",
					table, key, raw, got, n, len(raw), err, want, specHex(t, want))
			}
			seen[table][key] = true
		default:
		}
	}
	for _, name := range []string{tableReq, tableU32, tableU64} {
		if !defined[name] {
			t.Errorf("no definition for %s", name)
		}
	}
	for table, keys := range seen {
		if len(keys) != len(values[table]) {
			t.Errorf("%s: saw %d of %d boundary keys", table, len(keys), len(values[table]))
		}
	}
}
