package peerwire_test

import (
	"errors"
	"math"
	"testing"

	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

func TestCursor(t *testing.T) {
	// Fixed32 update ID 1, encoded 0xffffffff, encoded 2^32, three name
	// bytes, then a truncated encoding.
	body := mustHex(t, "00000001"+"fff0fefe7e"+"f0f1fefe7e"+"616263"+"f0")
	c := peerwire.NewCursor(body)
	if v, err := c.Fixed32(); err != nil || v != 1 {
		t.Fatalf("Fixed32 = %d, %v", v, err)
	}
	if v, err := c.Uint32(); err != nil || v != math.MaxUint32 {
		t.Fatalf("Uint32 = %#x, %v", v, err)
	}
	at := c.Offset()
	if _, err := c.Uint32(); !errors.Is(err, peerwire.ErrIntRange) || c.Offset() != at {
		t.Fatalf("Uint32 over range: %v, offset %d -> %d", err, at, c.Offset())
	}
	if v, err := c.Uint(); err != nil || v != 1<<32 {
		t.Fatalf("Uint = %#x, %v", v, err)
	}
	if b, err := c.Bytes(3); err != nil || string(b) != "abc" || cap(b) != 3 {
		t.Fatalf("Bytes = %q (cap %d), %v", b, cap(b), err)
	}
	var pe *peerwire.Error
	if _, err := c.Uint(); !errors.Is(err, peerwire.ErrShortBody) || !errors.As(err, &pe) || pe.Offset != 17 {
		t.Fatalf("truncated Uint: %v", err)
	}
	if _, err := c.Fixed32(); !errors.Is(err, peerwire.ErrShortBody) {
		t.Fatalf("short Fixed32: %v", err)
	}
	if _, err := c.Bytes(math.MaxUint64); !errors.Is(err, peerwire.ErrShortBody) {
		t.Fatalf("huge Bytes: %v", err)
	}
	if c.Remaining() != 1 || len(c.Rest()) != 1 || c.Offset() != 17 {
		t.Fatalf("failed reads moved the cursor: remaining %d offset %d", c.Remaining(), c.Offset())
	}
	overflow := peerwire.NewCursor(mustHex(t, "fff0fefefefefefefe10"))
	if _, err := overflow.Uint(); !errors.Is(err, peerwire.ErrIntOverflow) {
		t.Fatalf("overflow: %v", err)
	}
}
