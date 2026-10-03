package peerwire_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

var maxUint64 = new(big.Int).SetUint64(^uint64(0))

// FuzzDecodeUint checks DecodeUint against the specification's decode
// rule evaluated with arbitrary precision, and that every accepted
// encoding is exactly what AppendUint produces for its value.
func FuzzDecodeUint(f *testing.F) {
	for _, e := range specEncodings {
		f.Add(mustHex(f, e.hex))
	}
	for _, s := range []string{"", "f0", "ff8080", "fff0fefefefefefefe10", "fff0fefefefefefefe0f", "ffffffffffffffffffff00"} {
		f.Add(mustHex(f, s))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		v, n, err := peerwire.DecodeUint(b)
		// Sum the bytes up to and including the first terminator.
		sum := new(big.Int)
		terminated := false
		used := 0
		for i, c := range b {
			used = i + 1
			if i == 0 {
				sum.SetUint64(uint64(c))
				if c < 0xf0 {
					terminated = true
					break
				}
				continue
			}
			sum.Add(sum, new(big.Int).Lsh(big.NewInt(int64(c)), uint(4+7*(i-1))))
			// Further bytes only add, so stop once overflow is certain;
			// this bounds the work to about MaxUintLen terms.
			if sum.Cmp(maxUint64) > 0 {
				break
			}
			if c < 0x80 {
				terminated = true
				break
			}
		}
		switch {
		case sum.Cmp(maxUint64) > 0:
			// More bytes only add, so no completion can fit.
			if !errors.Is(err, peerwire.ErrIntOverflow) {
				t.Fatalf("DecodeUint(%x) = %d, %d, %v; spec sum %v overflows", b, v, n, err, sum)
			}
		case !terminated:
			if !errors.Is(err, peerwire.ErrIntTruncated) {
				t.Fatalf("DecodeUint(%x) = %d, %d, %v; want truncated", b, v, n, err)
			}
		default:
			if err != nil || v != sum.Uint64() || n != used {
				t.Fatalf("DecodeUint(%x) = %d, %d, %v; spec %v over %d bytes", b, v, n, err, sum, used)
			}
			if enc := peerwire.AppendUint(nil, v); !bytes.Equal(enc, b[:n]) || peerwire.EncodedLen(v) != n {
				t.Fatalf("value %#x decoded from %x re-encodes as %x", v, b[:n], enc)
			}
		}
		if err != nil && (v != 0 || n != 0) {
			t.Fatalf("DecodeUint(%x) error %v with value %d, n %d", b, err, v, n)
		}
	})
}

// FuzzUintRoundTrip checks encode-then-decode for arbitrary values.
func FuzzUintRoundTrip(f *testing.F) {
	for _, e := range specEncodings {
		f.Add(e.v)
	}
	f.Fuzz(func(t *testing.T, v uint64) {
		enc := peerwire.AppendUint([]byte{0xee}, v)[1:]
		if len(enc) > peerwire.MaxUintLen || len(enc) != peerwire.EncodedLen(v) {
			t.Fatalf("%#x encodes in %d bytes (EncodedLen %d)", v, len(enc), peerwire.EncodedLen(v))
		}
		got, n, err := peerwire.DecodeUint(enc)
		if err != nil || got != v || n != len(enc) {
			t.Fatalf("%#x -> %x -> %#x, %d, %v", v, enc, got, n, err)
		}
	})
}

// fuzzLimits are small enough for the fuzzer to reach every limit.
var fuzzLimits = peerwire.Limits{MaxHandshake: 40, MaxMessage: 64, MaxBuffered: 80}

// FuzzDecoder feeds arbitrary input, opened by 0-3 handshake lines, in one
// chunk, byte by byte, and split at a fuzzed offset, under small and
// default limits. Every run must keep the decoder's invariants (bounded
// buffer, progress, no consumption on failure, sticky errors), preserve
// every byte it frames, and agree across chunkings.
func FuzzDecoder(f *testing.F) {
	f.Add(synthetic(f), uint8(3), uint16(17), false)
	f.Add([]byte("200\n\x00\x00\x0a\x84\x00"), uint8(1), uint16(5), true)
	f.Add([]byte{0x0a, 0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, uint8(0), uint16(3), true)
	f.Add([]byte{0x0a, 0x80, 0xf0, 0x80, 0x80, 0x80, 0x00}, uint8(0), uint16(2), false)
	f.Add([]byte{0x00, 0x04, 0xff, 0x00}, uint8(0), uint16(1), false)
	f.Add([]byte{0x0a, 0x80, 0x45, 0x01}, uint8(0), uint16(2), true)
	f.Add([]byte("HAProxyS 2.1 and a line far longer than the handshake budget\n"), uint8(1), uint16(9), true)
	for _, p := range captureStreams(f) {
		f.Add(p.data, uint8(p.lines), uint16(len(p.data)/2), false)
	}
	f.Fuzz(func(t *testing.T, input []byte, lines uint8, split uint16, small bool) {
		lim := peerwire.DefaultLimits()
		if small {
			lim = fuzzLimits
		}
		n := int(lines % 4)
		want, err := decodeChunks(lim, [][]byte{input}, n)
		if err != nil {
			t.Fatalf("whole: %v", err)
		}
		if err := checkPreserved(input, want); err != nil {
			t.Fatalf("whole: %v", err)
		}
		k := int(split) % (len(input) + 1)
		chunkings := [][][]byte{{input[:k], input[k:]}}
		if len(input) <= 4096 {
			bytewise := make([][]byte, len(input))
			for i := range input {
				bytewise[i] = input[i : i+1]
			}
			chunkings = append(chunkings, bytewise)
		}
		for _, chunks := range chunkings {
			got, err := decodeChunks(lim, chunks, n)
			if err != nil {
				t.Fatalf("%d chunks: %v", len(chunks), err)
			}
			if !got.equal(want) {
				t.Fatalf("%d chunks: %v / %v, whole: %v / %v", len(chunks), got.events, got.err, want.events, want.err)
			}
		}
	})
}

// FuzzHandshake checks that the line parsers never panic and that what
// they accept survives an encode/parse round trip.
func FuzzHandshake(f *testing.F) {
	f.Add([]byte("HAProxyS 2.1"), []byte("hap"), []byte("agg 4242 1"), []byte("200"))
	f.Add([]byte("HAProxyS 3.0"), []byte("nothap"), []byte("nobody 1"), []byte("50x"))
	f.Add([]byte("HAProxyX 2.1"), []byte(""), []byte("a  1 1"), []byte(""))
	f.Fuzz(func(t *testing.T, version, remote, sender, status []byte) {
		if code, err := peerwire.ParseStatusLine(status); err == nil {
			b, err := peerwire.AppendStatus(nil, code)
			if err != nil || !bytes.Equal(b, append(bytes.Clone(status), '\n')) {
				t.Fatalf("status %q -> %d -> %q, %v", status, code, b, err)
			}
		}
		h, err := peerwire.ParseHello(version, remote, sender)
		if err != nil {
			if !errors.Is(err, peerwire.ErrBadHandshake) {
				t.Fatalf("ParseHello error %v does not wrap ErrBadHandshake", err)
			}
			return
		}
		b, err := peerwire.AppendHello(nil, h)
		if err != nil {
			t.Fatalf("AppendHello(%+v): %v", h, err)
		}
		lines := bytes.Split(bytes.TrimSuffix(b, []byte{'\n'}), []byte{'\n'})
		if len(lines) != peerwire.HelloLines {
			t.Fatalf("AppendHello(%+v) = %q", h, b)
		}
		again, err := peerwire.ParseHello(lines[0], lines[1], lines[2])
		if err != nil || again != h {
			t.Fatalf("round trip %+v -> %q -> %+v, %v", h, b, again, err)
		}
	})
}

type seedStream struct {
	data  []byte
	lines int
}

// captureStreams returns both streams of every committed capture fixture
// as fuzz seeds.
func captureStreams(tb testing.TB) []seedStream {
	tb.Helper()
	paths, err := filepath.Glob(filepath.Join(captureDir, "*", "*.txt"))
	if err != nil {
		tb.Fatal(err)
	}
	var out []seedStream
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			tb.Fatal(err)
		}
		c, err := parseCapture(f)
		_ = f.Close()
		if err != nil {
			tb.Fatalf("%s: %v", p, err)
		}
		cc := captureCases[c.get("case")]
		out = append(out, seedStream{c.stream("rx"), cc.rxLines}, seedStream{c.stream("tx"), cc.txLines})
	}
	return out
}

// Cursor operations selected by FuzzCursor's op bytes (op % cursorOps).
const (
	opUint = iota
	opUint32
	opFixed32
	opBytes
	opRest
	cursorOps
	// opBytesHuge selects Bytes with a length near 2^64: its top bit is set
	// and opBytesHuge % cursorOps == opBytes.
	opBytesHuge = 0x80
)

// FuzzCursor drives arbitrary sequences of Cursor reads over a fuzzed
// body. Each op byte selects a read; for Bytes the next op byte is the
// length, or, when the op byte's top bit is set, a length near 2^64. Every
// read must stay within the body, consume exactly the bytes it returns or
// nothing on failure, fail only with its documented sentinels as a
// positional *Error, and never allocate in proportion to a requested
// length.
func FuzzCursor(f *testing.F) {
	var spec []byte
	for _, e := range specEncodings {
		spec = append(spec, mustHex(f, e.hex)...)
	}
	f.Add(spec, []byte{opUint, opUint32, opUint, opFixed32, opBytes, 3, opRest})
	f.Add(mustHex(f, "fff0fefefefefefefe10"), []byte{opUint, opUint32})
	f.Add(mustHex(f, "f0f1fefe7e00000001616263f0"), []byte{opUint32, opUint32, opFixed32, opBytes, 3, opUint})
	if opBytesHuge%cursorOps != opBytes {
		f.Fatalf("opBytesHuge %#x does not select opBytes", opBytesHuge)
	}
	f.Add([]byte{1, 2, 3}, []byte{opBytesHuge, 0xff, opBytesHuge, 0, opFixed32, opBytes, 4, opBytes, 3})
	for _, p := range captureStreams(f) {
		out, err := decodeChunks(peerwire.DefaultLimits(), [][]byte{p.data}, p.lines)
		if err != nil {
			f.Fatal(err)
		}
		for _, e := range out.events {
			if len(e.body) > 0 {
				// Definition and update layouts: ID, name, then encoded
				// fields; update ID, key, then encoded values.
				f.Add(e.body, []byte{opUint32, opUint, opBytes, 5, opUint, opUint, opUint, opUint, opRest})
				f.Add(e.body, []byte{opFixed32, opFixed32, opUint, opUint, opUint, opUint, opRest})
			}
		}
	}
	f.Fuzz(func(t *testing.T, body, ops []byte) {
		c := peerwire.NewCursor(body)
		for i := 0; i < len(ops); i++ {
			op := ops[i]
			before := c.Offset()
			var (
				got     []byte // bytes the read claims to have consumed
				err     error
				allowed = []error{peerwire.ErrShortBody}
			)
			switch op % cursorOps {
			case opUint:
				var v uint64
				v, err = c.Uint()
				got = peerwire.AppendUint(nil, v)
				allowed = append(allowed, peerwire.ErrIntOverflow)
			case opUint32:
				var v uint32
				v, err = c.Uint32()
				got = peerwire.AppendUint(nil, uint64(v))
				allowed = append(allowed, peerwire.ErrIntOverflow, peerwire.ErrIntRange)
			case opFixed32:
				var v uint32
				v, err = c.Fixed32()
				got = binary.BigEndian.AppendUint32(nil, v)
			case opBytes:
				n := uint64(0)
				if i+1 < len(ops) {
					i++
					n = uint64(ops[i])
				}
				if op&0x80 != 0 {
					n = math.MaxUint64 - n
				}
				var ms0, ms1 runtime.MemStats
				runtime.ReadMemStats(&ms0)
				got, err = c.Bytes(n)
				runtime.ReadMemStats(&ms1)
				if d := ms1.TotalAlloc - ms0.TotalAlloc; d > 1<<16 {
					t.Fatalf("Bytes(%d) allocated %d bytes", n, d)
				}
				if err == nil && (uint64(len(got)) != n || cap(got) != len(got)) {
					t.Fatalf("Bytes(%d) returned %d bytes, cap %d", n, len(got), cap(got))
				}
			case opRest:
				if rest := c.Rest(); !bytes.Equal(rest, body[before:]) || cap(rest) != len(rest) {
					t.Fatalf("Rest at %d = %x, cap %d", before, rest, cap(rest))
				}
				// Rest does not consume.
				if c.Offset() != before {
					t.Fatalf("Rest moved the cursor %d -> %d", before, c.Offset())
				}
				continue
			}
			after := c.Offset()
			if after < before || after > len(body) || c.Remaining() != len(body)-after {
				t.Fatalf("op %d: offset %d -> %d, remaining %d, body %d", op, before, after, c.Remaining(), len(body))
			}
			if err != nil {
				var pe *peerwire.Error
				if after != before || !errors.As(err, &pe) || pe.Offset != uint64(before) ||
					!slices.ContainsFunc(allowed, func(s error) bool { return errors.Is(err, s) }) {
					t.Fatalf("op %d at %d: error %v, offset now %d", op, before, err, after)
				}
				continue
			}
			if !bytes.Equal(got, body[before:after]) {
				t.Fatalf("op %d at %d: consumed %x but read %x", op, before, body[before:after], got)
			}
		}
	})
}
