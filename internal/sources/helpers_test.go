package sources_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// recorder drains a manager's event queue into a log the test can wait on.
type recorder struct {
	t       *testing.T
	mu      sync.Mutex
	evs     []sources.Event
	changed chan struct{}
	done    chan struct{}
	// hold, while non-nil, stops draining until it is closed.
	hold chan struct{}
}

func record(t *testing.T, m *sources.Manager) *recorder {
	t.Helper()
	r := &recorder{t: t, changed: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(r.done)
		for ev := range m.Events() {
			r.mu.Lock()
			hold := r.hold
			r.mu.Unlock()
			if hold != nil {
				<-hold
			}
			r.mu.Lock()
			r.evs = append(r.evs, ev)
			close(r.changed)
			r.changed = make(chan struct{})
			r.mu.Unlock()
		}
	}()
	return r
}

func (r *recorder) snapshot() []sources.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sources.Event(nil), r.evs...)
}

// waitFor waits until pred holds for the log, failing after timeout.
func (r *recorder) waitFor(what string, timeout time.Duration, pred func([]sources.Event) bool) []sources.Event {
	r.t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		r.mu.Lock()
		evs, changed := append([]sources.Event(nil), r.evs...), r.changed
		r.mu.Unlock()
		if pred(evs) {
			return evs
		}
		select {
		case <-changed:
		case <-r.done:
			if pred(r.snapshot()) {
				return r.snapshot()
			}
			r.t.Fatalf("event queue closed while waiting for %s\n%s", what, describe(evs))
		case <-deadline.C:
			r.t.Fatalf("timed out after %v waiting for %s\n%s", timeout, what, describe(evs))
		}
	}
}

func describe(evs []sources.Event) string {
	var b strings.Builder
	for _, ev := range evs {
		fmt.Fprintf(&b, "  %s#%d %T %s\n", ev.Source, ev.Session, ev.Body, detail(ev.Body))
	}
	return b.String()
}

func detail(ev peersession.Event) string {
	switch b := ev.(type) {
	case peersession.SessionUp:
		return b.Direction.String() + " " + b.RemoteAddr
	case peersession.SessionDown:
		return b.Err.Error()
	case peersession.TableDefined:
		return b.Definition.Name
	case peersession.EntryUpdated:
		return fmt.Sprintf("%s id=%d key=%v values=%v", b.Table, b.Update.ID, b.Update.Key, b.Update.Values)
	case peersession.SyncFinished:
		return fmt.Sprintf("partial=%v", b.Partial)
	default:
		return ""
	}
}

// count returns how many events of type T the source has.
func count[T peersession.Event](evs []sources.Event, source string) int {
	n := 0
	for _, ev := range evs {
		if _, ok := ev.Body.(T); ok && ev.Source == source {
			n++
		}
	}
	return n
}

// ups returns the source's SessionUp events in order.
func ups(evs []sources.Event, source string) []sources.Event {
	var out []sources.Event
	for _, ev := range evs {
		if _, ok := ev.Body.(peersession.SessionUp); ok && ev.Source == source {
			out = append(out, ev)
		}
	}
	return out
}

// downs returns the source's SessionDown events in order.
func downs(evs []sources.Event, source string) []peersession.SessionDown {
	var out []peersession.SessionDown
	for _, ev := range evs {
		if d, ok := ev.Body.(peersession.SessionDown); ok && ev.Source == source {
			out = append(out, d)
		}
	}
	return out
}

// checkLifecycle asserts the per-source ordering contract: session numbers
// never decrease, each session starts with SessionUp, nothing follows its
// SessionDown, and a session's events all precede the next session's.
// So at no point are two sessions of one source up.
func checkLifecycle(t *testing.T, evs []sources.Event) {
	t.Helper()
	type state struct {
		session uint64
		up      bool
	}
	states := map[string]*state{}
	for i, ev := range evs {
		s := states[ev.Source]
		if s == nil {
			s = &state{}
			states[ev.Source] = s
		}
		switch ev.Body.(type) {
		case peersession.SessionUp:
			if s.up {
				t.Fatalf("event %d: %s session %d up while session %d is still up\n%s",
					i, ev.Source, ev.Session, s.session, describe(evs))
			}
			if ev.Session <= s.session {
				t.Fatalf("event %d: %s session number %d after %d\n%s", i, ev.Source, ev.Session, s.session, describe(evs))
			}
			s.session, s.up = ev.Session, true
		case peersession.SessionDown:
			if !s.up || ev.Session != s.session {
				t.Fatalf("event %d: %s SessionDown for session %d, current %d up=%v\n%s",
					i, ev.Source, ev.Session, s.session, s.up, describe(evs))
			}
			s.up = false
		default:
			if !s.up || ev.Session != s.session {
				t.Fatalf("event %d: %s %T for session %d, current %d up=%v\n%s",
					i, ev.Source, ev.Body, ev.Session, s.session, s.up, describe(evs))
			}
		}
	}
}

// listen opens a loopback listener closed at cleanup.
func listen(t *testing.T) net.Listener {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// closedAddr returns a loopback address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	return closedAddrOn(t, "127.0.0.1")
}

// closedAddrOn returns an address on ip nothing listens on.
func closedAddrOn(t *testing.T, ip string) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", ip+":0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// startManager starts a manager closed at cleanup, with its recorder.
func startManager(t *testing.T, opts sources.Options) (*sources.Manager, *recorder) {
	t.Helper()
	m, err := sources.Start(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	r := record(t, m)
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Errorf("close manager: %v", err)
		}
		<-r.done
		checkLifecycle(t, r.snapshot())
	})
	return m, r
}

// status returns one source's status.
func status(t *testing.T, m *sources.Manager, source string) sources.SourceStatus {
	t.Helper()
	for _, s := range m.Status() {
		if s.Name == source {
			return s
		}
	}
	t.Fatalf("no source %q", source)
	return sources.SourceStatus{}
}

// fastConfig is a configuration with short timings for fake-peer tests.
// It bypasses config validation, whose minimums suit HAProxy.
func fastConfig(srcs ...config.Source) config.Config {
	return config.Config{
		InsecurePlaintextLoopbackLab: true,
		LocalPeer:                    "agg",
		Sources:                      srcs,
		Tables:                       []config.Table{{Name: "t_in", Period: 10 * time.Second}},
		Heartbeat:                    50 * time.Millisecond,
		IdleTimeout:                  300 * time.Millisecond,
		HandshakeTimeout:             time.Second,
		ReconnectMin:                 10 * time.Millisecond,
		ReconnectMax:                 50 * time.Millisecond,
		EventQueue:                   64,
		EventTimeout:                 20 * time.Millisecond,
		MaxSessionTables:             8,
		RequestResync:                true,
	}
}

// downWith holds once any session ended with an error matching target.
func downWith(target error) func([]sources.Event) bool {
	return func(evs []sources.Event) bool {
		for _, ev := range evs {
			if d, ok := ev.Body.(peersession.SessionDown); ok && errors.Is(d.Err, target) {
				return true
			}
		}
		return false
	}
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// direction returns a SessionUp event's direction, or 0 for other events.
func direction(ev sources.Event) peersession.Direction {
	up, _ := ev.Body.(peersession.SessionUp)
	return up.Direction
}
