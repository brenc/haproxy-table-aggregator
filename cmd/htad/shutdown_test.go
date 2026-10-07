package main

import (
	"testing"

	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// TestRevocationWait checks which sessions shutdown waits for: only
// established sessions that may still leave their source holding a live
// marker (never one without a metadata table, one that wrote no marker,
// or one whose revocation is written), and not ended sessions.
func TestRevocationWait(t *testing.T) {
	st := func(up, live bool) sources.SourceStatus {
		return sources.SourceStatus{Name: "a", Up: up, Stats: peersession.Stats{LiveMarker: live}}
	}
	for name, tc := range map[string]struct {
		statuses []sources.SourceStatus
		done     bool
	}{
		"no sessions":                       {nil, true},
		"no live marker (e.g. no metadata)": {[]sources.SourceStatus{st(true, false)}, true},
		"live marker outstanding":           {[]sources.SourceStatus{st(true, true)}, false},
		"one of two outstanding":            {[]sources.SourceStatus{st(true, false), st(true, true)}, false},
		"session ended":                     {[]sources.SourceStatus{st(false, true)}, true},
	} {
		if got := revocationsWritten(tc.statuses); got != tc.done {
			t.Errorf("%s: written %v, want %v", name, got, tc.done)
		}
	}
}
