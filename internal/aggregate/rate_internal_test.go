package aggregate

import (
	"errors"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/rate"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
)

// TestSumRatesRefusesOtherPeriod: an entry whose period differs from the
// table's is an error, never converted. The store's schema check makes
// this unreachable through Rate; the evaluator does not rely on it.
func TestSumRatesRefusesOtherPeriod(t *testing.T) {
	now := time.Unix(10, 0)
	roster := snapshot.Roster{At: now, Ready: true, Sources: []snapshot.SourceReport{{Name: "a", State: snapshot.Ready}}}
	e := snapshot.Entry{
		Received: now, Deadline: now.Add(time.Minute), Period: 60000,
		Rate: peermsg.FreqCounter{Curr: 6},
	}
	_, err := sumRates("t_in", peermsg.Key{}, 10000, roster, []snapshot.Contribution{{Source: "a", Entry: e}})
	if !errors.Is(err, rate.ErrPeriod) {
		t.Fatalf("entry period 60s in a 10s table: %v, want ErrPeriod", err)
	}
	e.Period = 10000
	r, err := sumRates("t_in", peermsg.Key{}, 10000, roster, []snapshot.Contribution{{Source: "a", Entry: e}})
	if err != nil || r.Sum != 6 {
		t.Fatalf("matching period: %+v, %v", r, err)
	}
}
