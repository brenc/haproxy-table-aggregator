package aggregate

import (
	"errors"
	"math"
	"testing"
)

// TestAddChecked: the wide sum detects a carry instead of wrapping.
func TestAddChecked(t *testing.T) {
	for _, c := range []struct {
		a, b, want uint64
		overflow   bool
	}{
		{1, 2, 3, false},
		{math.MaxUint32, math.MaxUint32, 2 * math.MaxUint32, false},
		{math.MaxUint64 - math.MaxUint32, math.MaxUint32, math.MaxUint64, false},
		{math.MaxUint64, 1, 0, true},
		{math.MaxUint64 - 5, math.MaxUint32, 0, true},
	} {
		got, err := add(c.a, c.b)
		if c.overflow != errors.Is(err, ErrOverflow) || (!c.overflow && got != c.want) {
			t.Errorf("add(%d, %d) = %d, %v", c.a, c.b, got, err)
		}
	}
}
