package peerwire

import (
	"math"
	"math/bits"
)

// HAProxy's integer encoding (doc/peers.txt, "Encoding"). It is not
// LEB128: values below 0xf0 take one byte; otherwise the first byte holds
// the low four bits OR 0xf0 and each following byte contributes its full
// value shifted left by 4, 11, 18, ... bits, with bit 7 meaning "another
// byte follows". The encoder subtracts each byte's marker bits before
// shifting, which makes every byte sequence decode to a distinct value:
// there are no alternative (overlong) encodings to reject, only values
// beyond 64 bits.
const (
	encTwoByteMin  = 0xf0 // smallest value needing a second byte
	encFirstBits   = 4    // value bits carried by a multi-byte first byte
	encStopBit     = 0x80 // set on every byte but the last of a multi-byte value
	encNextBits    = 7    // shift added per continuation byte
	lastByteShift  = encFirstBits + encNextBits*(MaxUintLen-2)
	maxLastByteVal = math.MaxUint8 >> (lastByteShift + 8 - 64)
)

// MaxUintLen is the longest encoding of a 64-bit value, in bytes.
const MaxUintLen = 10

// EncodedLen returns the number of bytes AppendUint uses for v.
func EncodedLen(v uint64) int {
	if v < encTwoByteMin {
		return 1
	}
	n := 2
	v = (v - encTwoByteMin) >> encFirstBits
	for v >= encStopBit {
		v = (v - encStopBit) >> encNextBits
		n++
	}
	return n
}

// AppendUint appends HAProxy's encoding of v to dst and returns the
// extended slice. It always succeeds and uses at most MaxUintLen bytes.
func AppendUint(dst []byte, v uint64) []byte {
	if v < encTwoByteMin {
		return append(dst, byte(v))
	}
	dst = append(dst, byte(v&0xff)|encTwoByteMin)
	v = (v - encTwoByteMin) >> encFirstBits
	for v >= encStopBit {
		dst = append(dst, byte(v&0xff)|encStopBit)
		v = (v - encStopBit) >> encNextBits
	}
	return append(dst, byte(v))
}

// DecodeUint decodes one integer from the start of b. It returns the value
// and the number of bytes consumed.
//
// It returns ErrIntTruncated if b ends before the encoding does, and
// ErrIntOverflow if the encoding's value exceeds 64 bits; in both cases n
// is 0. It reads at most MaxUintLen bytes of b and never retains b.
func DecodeUint(b []byte) (v uint64, n int, err error) {
	if len(b) == 0 {
		return 0, 0, ErrIntTruncated
	}
	v = uint64(b[0])
	if v < encTwoByteMin {
		return v, 1, nil
	}
	shift := uint(encFirstBits)
	for i := 1; ; i++ {
		if i >= len(b) {
			return 0, 0, ErrIntTruncated
		}
		c := uint64(b[i])
		// The tenth byte is shifted by 60, so only its low four bits fit;
		// any larger byte (including one announcing an eleventh byte)
		// overflows here, which also bounds the loop at MaxUintLen bytes.
		if shift == lastByteShift && c > maxLastByteVal {
			return 0, 0, ErrIntOverflow
		}
		var carry uint64
		v, carry = bits.Add64(v, c<<shift, 0)
		if carry != 0 {
			return 0, 0, ErrIntOverflow
		}
		if c < encStopBit {
			return v, i + 1, nil
		}
		shift += encNextBits
	}
}

// DecodeUint32 is DecodeUint for fields that must fit in 32 bits. A
// well-formed encoding of a larger value yields ErrIntRange.
func DecodeUint32(b []byte) (uint32, int, error) {
	v, n, err := DecodeUint(b)
	if err != nil {
		return 0, 0, err
	}
	if v > math.MaxUint32 {
		return 0, 0, ErrIntRange
	}
	return uint32(v), n, nil
}
