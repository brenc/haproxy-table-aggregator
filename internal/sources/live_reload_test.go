//go:build linux

package sources_test

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/lab"
)

// TestLiveFreshnessReloadOverwrite soft-reloads a healthy node while the
// aggregator keeps publishing, and makes the old process's own teach of
// the output reach the new process after the aggregator's newer teach
// (the old process is stopped across the reload, as a slow teach of a
// large table would be). HAProxy then holds the old process's copy of the
// key, written under the old session's generation. The new session's
// markers never certify it; the old process's marker, taught the same
// way, certifies at most the old session's values until its own deadline.
// The session's periodic refresh re-sends the key under its own
// generation, so the key regains authority within the refresh interval
// even when its value never changed.
func TestLiveFreshnessReloadOverwrite(t *testing.T) {
	for _, tc := range []struct {
		name string
		// resumeAfter is when the old process resumes after it was
		// stopped; past authorityBound its marker has expired.
		resumeAfter time.Duration
		// changed publishes 1500 -> 50 while the old process is stopped,
		// so its copy (1500) is also an older value; otherwise the store
		// keeps 1500 and only the generation tells the copies apart.
		changed bool
	}{
		{"old marker expired", authorityBound + 500*time.Millisecond, true},
		{"old marker live", 0, true},
		{"unchanged key", authorityBound + 500*time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFresh(t, freshOptions{reloadable: true})
			refresh := lab.ExpiryFactor * f.l.Period / 3 // the default
			f.publish(overClient, 1500)
			defer renewLease(t, f.store)()
			f.waitProbe(overClient, "authority", authoritative(1500))
			oldPID, oldGen := f.n.PID(), f.gen()
			if err := syscall.Kill(oldPID, syscall.SIGSTOP); err != nil {
				t.Fatal(err)
			}
			stopped := time.Now()
			resumed := false
			resume := func() {
				if !resumed {
					resumed = true
					if err := syscall.Kill(oldPID, syscall.SIGCONT); err != nil {
						t.Error(err)
					}
				}
			}
			defer resume()
			want := int64(1500)
			if tc.changed {
				want = 50
				f.publish(overClient, 50)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			reloaded := make(chan error, 1)
			go func() { reloaded <- f.n.Reload(ctx) }()
			p := f.waitProbe(overClient, "the new process authoritative on the store's value", func(p lab.ProbeResponse) bool {
				return p.PID != int64(oldPID) && p.Gen != oldGen && authoritative(want)(p)
			})
			newGen := p.Gen
			t.Logf("new process before the old one resumes: %+v", p)
			time.Sleep(time.Until(stopped.Add(tc.resumeAfter)))
			w := f.watch(overClient)
			resume()
			resumedAt := time.Now()
			if err := <-reloaded; err != nil {
				t.Fatal(err)
			}
			regained := f.waitProbe(overClient, "the key re-sent under the new generation", func(p lab.ProbeResponse) bool {
				return p.Gen == newGen && authoritative(want)(p)
			})
			regainedAt := time.Now()
			time.Sleep(500 * time.Millisecond)
			ds := w.end()
			overwritten, oldAuthority := 0, 0
			var lastOverwritten time.Time
			for _, d := range ds {
				switch d.p.Gen {
				case oldGen:
					if d.p.Rate != 1500 {
						t.Fatalf("the old session's copy reads %d, not 1500: %+v", d.p.Rate, d.p)
					}
					overwritten++
					lastOverwritten = d.sent
					if d.p.Aggregate() {
						oldAuthority++
						if d.p.MetaGen != oldGen {
							t.Errorf("the old copy certified by another session's marker: %+v", d.p)
						}
						if since := d.sent.Sub(stopped); since > authorityBound {
							t.Errorf("the old copy aggregate %v after the old process's last marker: %+v", since, d.p)
						}
					}
				case newGen:
					if d.p.Rate != want {
						t.Errorf("the new session's entry reads %d, want %d: %+v", d.p.Rate, want, d.p)
					}
				default:
					t.Errorf("entry of an unknown generation: %+v", d.p)
				}
			}
			if overwritten == 0 {
				t.Fatalf("the old process's teach never overwrote the key (%d decisions); the ordering was not reproduced", len(ds))
			}
			if took := regainedAt.Sub(resumedAt); took > refresh+time.Second {
				t.Errorf("the key regained authority %v after the overwrite, beyond the %v refresh", took, refresh)
			}
			t.Logf("old process resumed %v after it stopped: %d of %d decisions saw its copy (1500, generation %d), %d aggregate under its own marker; the last %v after it resumed; authority regained %v after it resumed (refresh %v): %+v",
				tc.resumeAfter, overwritten, len(ds), oldGen, oldAuthority, lastOverwritten.Sub(resumedAt).Round(time.Millisecond),
				regainedAt.Sub(resumedAt).Round(time.Millisecond), refresh, regained)
		})
	}
}
