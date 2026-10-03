package peerwire_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func frame(tb testing.TB, class peerwire.MessageClass, typ peerwire.MessageType, body []byte) []byte {
	tb.Helper()
	b, err := peerwire.AppendFrame(nil, class, typ, body)
	if err != nil {
		tb.Fatalf("AppendFrame: %v", err)
	}
	return b
}

// synthetic is a hand-built hello followed, with no gap, by several
// frames: fixed control and error messages, an empty variable body, a
// body whose length needs two bytes, an unknown class, and an unknown
// variable type.
func synthetic(tb testing.TB) []byte {
	tb.Helper()
	return cat(
		[]byte("HAProxyS 2.1\r\nhap\nagg 4242 1\n"),
		[]byte{0x00, 0x00},
		frame(tb, peerwire.ClassStickTable, peerwire.StickTableDefine, []byte{0x01, 0x05, 't', '_', 'r', 'e', 'q'}),
		frame(tb, peerwire.ClassStickTable, peerwire.StickTableAck, nil),
		frame(tb, peerwire.ClassStickTable, peerwire.StickTableUpdate, bytes.Repeat([]byte{0xa5}, 0x8ef)),
		[]byte{0x2a, 0x07},
		frame(tb, 0x2a, 0xc3, []byte("x")),
		[]byte{0x01, 0x01},
	)
}

func decodeAll(tb testing.TB, lim peerwire.Limits, input []byte, lines int) outcome {
	tb.Helper()
	out, err := decodeChunks(lim, [][]byte{input}, lines)
	if err != nil {
		tb.Fatalf("decode: %v", err)
	}
	return out
}

func TestDecoderChunkBoundaries(t *testing.T) {
	input := synthetic(t)
	lim := peerwire.DefaultLimits()
	want := decodeAll(t, lim, input, peerwire.HelloLines)
	if !errors.Is(want.err, io.EOF) {
		t.Fatalf("terminal %v, want io.EOF", want.err)
	}
	if err := checkPreserved(input, want); err != nil {
		t.Fatal(err)
	}
	if len(want.events) != 10 {
		t.Fatalf("got %d events, want 3 lines and 7 frames: %v", len(want.events), want.events)
	}
	if string(want.events[0].data) != "HAProxyS 2.1" {
		t.Errorf("CR not stripped: %q", want.events[0].data)
	}
	ack := want.events[5]
	if ack.typ != peerwire.StickTableAck || !ack.isBody || len(ack.body) != 0 {
		t.Errorf("empty variable body: %v body=%v", ack, ack.body)
	}
	if want.events[3].isBody {
		t.Errorf("fixed frame has a body: %v", want.events[3])
	}
	for _, chunks := range splits(input) {
		got, err := decodeChunks(lim, chunks, peerwire.HelloLines)
		if err != nil {
			t.Fatalf("%d chunks: %v", len(chunks), err)
		}
		if !got.equal(want) {
			t.Fatalf("%d chunks (first %d bytes): got %v / %v, want %v / %v",
				len(chunks), len(chunks[0]), got.events, got.err, want.events, want.err)
		}
	}
}

func TestDecoderMultipleFramesOneFeed(t *testing.T) {
	d, err := peerwire.NewDecoder(peerwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	input := cat(
		[]byte("200\n"),
		[]byte{0x00, 0x00},
		frame(t, peerwire.ClassStickTable, peerwire.StickTableDefine, []byte("first")),
		frame(t, peerwire.ClassStickTable, peerwire.StickTableDefine, []byte("second")),
		[]byte{0x00, 0x04, 0x0a}, // heartbeat, then one byte of the next frame
	)
	if ferr := d.Feed(input); ferr != nil {
		t.Fatal(ferr)
	}
	line, err := d.NextLine()
	if err != nil || string(line) != "200" {
		t.Fatalf("NextLine = %q, %v", line, err)
	}
	var frames []peerwire.Frame
	for {
		f, ferr := d.NextFrame()
		if errors.Is(ferr, peerwire.ErrNeedMore) {
			break
		}
		if ferr != nil {
			t.Fatal(ferr)
		}
		frames = append(frames, f)
	}
	if len(frames) != 4 || d.Buffered() != 1 {
		t.Fatalf("got %d frames, %d buffered; want 4, 1", len(frames), d.Buffered())
	}
	// Earlier frames stay intact while later ones are consumed.
	if string(frames[1].Body) != "first" || string(frames[2].Body) != "second" {
		t.Fatalf("bodies %q %q", frames[1].Body, frames[2].Body)
	}
	if _, lerr := d.NextLine(); !errors.Is(lerr, peerwire.ErrHandshakeDone) {
		t.Fatalf("NextLine after NextFrame: %v", lerr)
	}
	if ferr := d.Feed([]byte{0x80, 0x00}); ferr != nil {
		t.Fatal(ferr)
	}
	f, err := d.NextFrame()
	if err != nil || f.Class != peerwire.ClassStickTable || f.Type != 0x80 || f.Offset != uint64(len(input)-1) {
		t.Fatalf("frame across feeds = %v, %v", f, err)
	}
	d.CloseInput()
	if _, err := d.NextFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("clean end: %v", err)
	}
	if err := d.Feed([]byte{0}); !errors.Is(err, peerwire.ErrInputClosed) {
		t.Fatalf("Feed after CloseInput: %v", err)
	}
}

// TestDecoderTruncation closes input after every proper prefix of a valid
// stream: at a line or frame boundary the result is io.EOF, anywhere else
// a bounded ErrTruncated naming the offset where the partial item starts.
func TestDecoderTruncation(t *testing.T) {
	input := synthetic(t)
	lim := peerwire.DefaultLimits()
	full := decodeAll(t, lim, input, peerwire.HelloLines)
	boundary := map[uint64]bool{0: true}
	for _, e := range full.events {
		boundary[e.end] = true
	}
	for n := range input {
		got := decodeAll(t, lim, input[:n], peerwire.HelloLines)
		if err := checkPreserved(input[:n], got); err != nil {
			t.Fatalf("prefix %d: %v", n, err)
		}
		if boundary[uint64(n)] {
			if !errors.Is(got.err, io.EOF) {
				t.Fatalf("prefix %d at a boundary: %v", n, got.err)
			}
			continue
		}
		var pe *peerwire.Error
		if !errors.As(got.err, &pe) || !errors.Is(got.err, peerwire.ErrTruncated) {
			t.Fatalf("prefix %d: %v, want ErrTruncated", n, got.err)
		}
		last := uint64(0)
		if k := len(got.events); k > 0 {
			last = got.events[k-1].end
		}
		if pe.Offset != last || len(pe.Error()) > 300 {
			t.Fatalf("prefix %d: error %q, want offset %d", n, pe, last)
		}
	}
}

func TestDecoderLimits(t *testing.T) {
	small := peerwire.Limits{MaxHandshake: 16, MaxMessage: 300, MaxBuffered: 320}
	if err := small.Validate(); err != nil {
		t.Fatal(err)
	}
	body := func(n int) []byte { return bytes.Repeat([]byte{'b'}, n) }
	cases := []struct {
		name  string
		input []byte
		lines int
		want  error
		at    uint64
	}{
		{"body at limit", cat([]byte{0x0a, 0x80, 0xfc, 0x03}, body(300)), 0, io.EOF, 0},
		{"body over limit, header only", []byte{0x0a, 0x80, 0xfd, 0x03}, 0, peerwire.ErrMessageTooLarge, 0},
		{"over limit after a frame", []byte{0x00, 0x04, 0x0a, 0xff, 0xfd, 0x03}, 0, peerwire.ErrMessageTooLarge, 2},
		{"five-byte length", []byte{0x0a, 0x80, 0xf0, 0x80, 0x80, 0x80, 0x00}, 0, peerwire.ErrMessageTooLarge, 0},
		{"six-byte length", []byte{0x0a, 0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, 0, peerwire.ErrLengthEncoding, 0},
		{"six-byte length, partial", []byte{0x0a, 0x80, 0xff, 0xff, 0xff, 0xff, 0xff}, 0, peerwire.ErrLengthEncoding, 0},
		{"reserved class", []byte{0x00, 0x04, 0xff, 0x00}, 0, peerwire.ErrReservedClass, 2},
		{"reserved class, variable", []byte{0xff, 0x80}, 0, peerwire.ErrReservedClass, 0},
		{"handshake at limit", []byte("HAProxyS 2.1\nab\n"), 2, io.EOF, 0},
		{"handshake over limit", []byte("HAProxyS 2.1\nabc\n"), 2, peerwire.ErrHandshakeTooLarge, 13},
		{"unterminated line over limit", []byte("HAProxyS 2.1 with no end"), 1, peerwire.ErrHandshakeTooLarge, 0},
		{"unterminated line at limit", []byte("HAProxyS 2.1 abc"), 1, peerwire.ErrHandshakeTooLarge, 0},
		{"truncated length", []byte{0x0a, 0x80, 0xf0}, 0, peerwire.ErrTruncated, 0},
		{"truncated header", []byte{0x0a}, 0, peerwire.ErrTruncated, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, chunks := range splits(tc.input) {
				out, err := decodeChunks(small, chunks, tc.lines)
				if err != nil {
					t.Fatal(err)
				}
				if !errors.Is(out.err, tc.want) {
					t.Fatalf("%d chunks: got %v, want %v", len(chunks), out.err, tc.want)
				}
				var pe *peerwire.Error
				if errors.As(out.err, &pe) && pe.Offset != tc.at {
					t.Fatalf("%d chunks: offset %d, want %d", len(chunks), pe.Offset, tc.at)
				}
				if err := checkPreserved(tc.input, out); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestDecoderHandshakeFillsBuffer uses a handshake budget equal to the
// buffer size: an unterminated line that fills the buffer must fail
// rather than leave the reader with nothing to feed.
func TestDecoderHandshakeFillsBuffer(t *testing.T) {
	lim := peerwire.Limits{MaxHandshake: 16, MaxMessage: 9, MaxBuffered: 16}
	input := []byte("0123456789abcdef and more")
	for _, chunks := range splits(input) {
		out, err := decodeChunks(lim, chunks, 1)
		if err != nil {
			t.Fatalf("%d chunks: %v", len(chunks), err)
		}
		if !errors.Is(out.err, peerwire.ErrHandshakeTooLarge) {
			t.Fatalf("%d chunks: %v", len(chunks), out.err)
		}
	}
}

// TestDecoderHandshakeBudgetSpent checks that consuming exactly
// MaxHandshake bytes of lines is not itself a failure: with nothing
// buffered, NextLine waits for input or reports a clean end.
func TestDecoderHandshakeBudgetSpent(t *testing.T) {
	lim := peerwire.Limits{MaxHandshake: 4, MaxMessage: 8, MaxBuffered: 16}
	d, err := peerwire.NewDecoder(lim)
	if err != nil {
		t.Fatal(err)
	}
	if ferr := d.Feed([]byte("200\n")); ferr != nil {
		t.Fatal(ferr)
	}
	if line, lerr := d.NextLine(); lerr != nil || string(line) != "200" {
		t.Fatalf("NextLine = %q, %v", line, lerr)
	}
	if _, lerr := d.NextLine(); !errors.Is(lerr, peerwire.ErrNeedMore) {
		t.Fatalf("open input: %v, want ErrNeedMore", lerr)
	}
	d.CloseInput()
	if _, lerr := d.NextLine(); !errors.Is(lerr, io.EOF) {
		t.Fatalf("closed input: %v, want io.EOF", lerr)
	}
	// A further byte, by contrast, can never fit the handshake.
	for _, chunks := range splits([]byte("200\nx")) {
		out, cerr := decodeChunks(lim, chunks, 2)
		if cerr != nil {
			t.Fatal(cerr)
		}
		var pe *peerwire.Error
		if !errors.As(out.err, &pe) || !errors.Is(out.err, peerwire.ErrHandshakeTooLarge) || pe.Offset != 4 {
			t.Fatalf("%d chunks: %v, want ErrHandshakeTooLarge at 4", len(chunks), out.err)
		}
	}
}

func TestDecoderFeedLimit(t *testing.T) {
	lim := peerwire.Limits{MaxHandshake: 8, MaxMessage: 8, MaxBuffered: 16}
	d, err := peerwire.NewDecoder(lim)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Feed(make([]byte, 17)); !errors.Is(err, peerwire.ErrInputLimit) {
		t.Fatalf("oversized Feed: %v", err)
	}
	if d.Buffered() != 0 || d.Free() != 16 {
		t.Fatalf("rejected Feed buffered %d bytes", d.Buffered())
	}
	if err := d.Feed([]byte{0x00, 0x04}); err != nil {
		t.Fatalf("decoder unusable after ErrInputLimit: %v", err)
	}
	if f, err := d.NextFrame(); err != nil || f.Type != peerwire.ControlHeartbeat {
		t.Fatalf("NextFrame = %v, %v", f, err)
	}
}

func TestDecoderStickyFailure(t *testing.T) {
	d, err := peerwire.NewDecoder(peerwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Feed([]byte{0xff, 0x00, 0x00, 0x04}); err != nil {
		t.Fatal(err)
	}
	_, first := d.NextFrame()
	if !errors.Is(first, peerwire.ErrReservedClass) {
		t.Fatalf("got %v", first)
	}
	if _, err := d.NextFrame(); !errors.Is(err, first) {
		t.Fatalf("second NextFrame %v, want the same error", err)
	}
	if err := d.Feed([]byte{0}); !errors.Is(err, first) {
		t.Fatalf("Feed after failure %v, want the same error", err)
	}
	if !strings.Contains(first.Error(), "offset 0") {
		t.Fatalf("error lacks offset: %q", first)
	}
}

func TestLimitsValidate(t *testing.T) {
	bad := []peerwire.Limits{
		{},
		{MaxHandshake: 1, MaxMessage: -1, MaxBuffered: 100},
		{MaxHandshake: 100, MaxMessage: 10, MaxBuffered: 99},
		{MaxHandshake: 1, MaxMessage: 10, MaxBuffered: 16},
		{MaxHandshake: 1, MaxMessage: 1 << 33, MaxBuffered: 1 << 34},
	}
	for _, l := range bad {
		if err := l.Validate(); !errors.Is(err, peerwire.ErrInvalidLimits) {
			t.Errorf("Validate(%+v) = %v", l, err)
		}
		if _, err := peerwire.NewDecoder(l); err == nil {
			t.Errorf("NewDecoder(%+v) succeeded", l)
		}
	}
	ok := peerwire.Limits{MaxHandshake: 1, MaxMessage: 10, MaxBuffered: 17}
	if err := ok.Validate(); err != nil {
		t.Errorf("Validate(%+v) = %v", ok, err)
	}
	if err := peerwire.DefaultLimits().Validate(); err != nil {
		t.Errorf("default limits: %v", err)
	}
}

func TestAppendFrame(t *testing.T) {
	if _, err := peerwire.AppendFrame(nil, peerwire.ClassControl, peerwire.ControlHeartbeat, []byte{1}); !errors.Is(err, peerwire.ErrFixedBody) {
		t.Errorf("fixed with body: %v", err)
	}
	if _, err := peerwire.AppendFrame(nil, peerwire.ClassReserved, 0, nil); !errors.Is(err, peerwire.ErrReservedClass) {
		t.Errorf("reserved: %v", err)
	}
	cases := []struct {
		class peerwire.MessageClass
		typ   peerwire.MessageType
		body  []byte
		want  string
	}{
		{peerwire.ClassControl, peerwire.ControlResyncFinished, nil, "0001"},
		{peerwire.ClassError, peerwire.ErrorTypeSizeLimit, nil, "0101"},
		{peerwire.ClassStickTable, peerwire.StickTableAck, nil, "0a8400"},
		{peerwire.ClassStickTable, peerwire.StickTableAck, []byte{1, 0, 0, 0, 7}, "0a84050100000007"},
		{peerwire.ClassStickTable, peerwire.StickTableUpdate, make([]byte, 0xf0), "0a80f000" + strings.Repeat("00", 0xf0)},
	}
	for _, tc := range cases {
		got := frame(t, tc.class, tc.typ, tc.body)
		if want := mustHex(t, tc.want); !bytes.Equal(got, want) {
			t.Errorf("AppendFrame(%d, %#x, %d bytes) = %x, want %s", tc.class, tc.typ, len(tc.body), got, tc.want)
		}
	}
}
