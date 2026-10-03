package peerwire_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"math/big"
	"testing"

	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// specEncodings are boundary values and their expected encodings. Origin:
// worked by hand from the "encode"/"decode" pseudo-code in doc/peers.txt
// (3.4.6/3.2.25, SHA-256 1378d18d...ba71), not produced by this package:
//
//   - 0x1234 is the specification's own worked example.
//   - The smallest n-byte value is f0, n-2 bytes of 80, then 00; the largest
//     is ff, n-2 bytes of ff, then 7f. Decoding is a plain sum of each byte
//     shifted left by 0, 4, 11, 18, ... bits, so each row can be checked by
//     adding the terms.
//   - The 32- and 64-bit limits follow from the same sum.
//
// TestCaptureFixtures independently checks every value from 0xef upward
// against bytes that stock HAProxy 3.4.6 and 3.2.25 put on the wire.
var specEncodings = []struct {
	v   uint64
	hex string
}{
	{0x0, "00"},
	{0x1, "01"},
	{0xef, "ef"},
	{0xf0, "f000"},
	{0x8ef, "ff7f"},
	{0x8f0, "f08000"},
	{0x1234, "f49401"},
	{0x408ef, "ffff7f"},
	{0x408f0, "f0808000"},
	{0x20408ef, "ffffff7f"},
	{0x20408f0, "f080808000"},
	{0x7fffffff, "fff0fefe3e"},
	{0x80000000, "f0f1fefe3e"},
	{0xffffffff, "fff0fefe7e"},
	{0x100000000, "f0f1fefe7e"},
	{0x1020408ef, "ffffffff7f"},
	{0x1020408f0, "f08080808000"},
	{0x81020408ef, "ffffffffff7f"},
	{0x81020408f0, "f0808080808000"},
	{0x4081020408ef, "ffffffffffff7f"},
	{0x4081020408f0, "f080808080808000"},
	{0x204081020408ef, "ffffffffffffff7f"},
	{0x204081020408f0, "f08080808080808000"},
	{0x10204081020408ef, "ffffffffffffffff7f"},
	{0x10204081020408f0, "f0808080808080808000"},
	{0x7fffffffffffffff, "fff0fefefefefefefe06"},
	{0x8000000000000000, "f0f1fefefefefefefe06"},
	{0xffffffffffffffff, "fff0fefefefefefefe0e"},
}

func mustHex(tb testing.TB, s string) []byte {
	tb.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		tb.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

// specSum evaluates the specification's decode rule with arbitrary
// precision: the first byte, plus every following byte shifted by
// 4+7*i, until a byte below 0x80. It returns nil if b ends first.
func specSum(b []byte) (*big.Int, int) {
	if len(b) == 0 {
		return nil, 0
	}
	v := big.NewInt(int64(b[0]))
	if b[0] < 0xf0 {
		return v, 1
	}
	for i, c := range b[1:] {
		v.Add(v, new(big.Int).Lsh(big.NewInt(int64(c)), uint(4+7*i)))
		if c < 0x80 {
			return v, i + 2
		}
	}
	return nil, 0
}

func TestSpecEncodings(t *testing.T) {
	for _, tc := range specEncodings {
		want := mustHex(t, tc.hex)
		if sum, n := specSum(want); sum == nil || n != len(want) || !sum.IsUint64() || sum.Uint64() != tc.v {
			t.Fatalf("table row %#x: %s does not sum to the value under the spec rule", tc.v, tc.hex)
		}
		if got := peerwire.AppendUint(nil, tc.v); !bytes.Equal(got, want) {
			t.Errorf("AppendUint(%#x) = %x, want %s", tc.v, got, tc.hex)
		}
		if got := peerwire.EncodedLen(tc.v); got != len(want) {
			t.Errorf("EncodedLen(%#x) = %d, want %d", tc.v, got, len(want))
		}
		// Trailing bytes must not be consumed.
		v, n, err := peerwire.DecodeUint(append(append([]byte{}, want...), 0xaa, 0xf0))
		if err != nil || v != tc.v || n != len(want) {
			t.Errorf("DecodeUint(%s..) = %#x, %d, %v; want %#x, %d", tc.hex, v, n, err, tc.v, len(want))
		}
		v32, n32, err := peerwire.DecodeUint32(want)
		switch {
		case tc.v <= math.MaxUint32 && (err != nil || uint64(v32) != tc.v || n32 != len(want)):
			t.Errorf("DecodeUint32(%s) = %#x, %d, %v", tc.hex, v32, n32, err)
		case tc.v > math.MaxUint32 && !errors.Is(err, peerwire.ErrIntRange):
			t.Errorf("DecodeUint32(%s) error = %v, want ErrIntRange", tc.hex, err)
		}
		// Every proper prefix is truncated, never a shorter value.
		for i := range want {
			if _, _, err := peerwire.DecodeUint(want[:i]); !errors.Is(err, peerwire.ErrIntTruncated) {
				t.Errorf("DecodeUint(%x) error = %v, want ErrIntTruncated", want[:i], err)
			}
		}
	}
}

func TestDecodeUintMalformed(t *testing.T) {
	cases := []struct {
		name, hex string
		want      error
	}{
		{"empty", "", peerwire.ErrIntTruncated},
		{"lone first byte", "f0", peerwire.ErrIntTruncated},
		{"open continuation", "ff8080", peerwire.ErrIntTruncated},
		{"tenth byte too large", "fff0fefefefefefefe10", peerwire.ErrIntOverflow},
		{"tenth byte continues", "f08080808080808080808000", peerwire.ErrIntOverflow},
		{"carry past 64 bits", "fff0fefefefefefefe0f", peerwire.ErrIntOverflow},
		{"max plus one", "f0f1fefefefefefefe0e", peerwire.ErrIntOverflow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := mustHex(t, tc.hex)
			v, n, err := peerwire.DecodeUint(b)
			sum, _ := specSum(b)
			if !errors.Is(err, tc.want) || v != 0 || n != 0 {
				t.Fatalf("DecodeUint(%s) = %#x, %d, %v; want error %v", tc.hex, v, n, err, tc.want)
			}
			if errors.Is(tc.want, peerwire.ErrIntOverflow) && sum != nil && sum.IsUint64() {
				t.Fatalf("case %s is not an overflow under the spec rule (sum %v)", tc.name, sum)
			}
		})
	}
}
