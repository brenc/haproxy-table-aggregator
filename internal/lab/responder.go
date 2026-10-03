package lab

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// Observation identifies the requests the responder saw for one node,
// listener, and HAProxy-tracked key.
type Observation struct {
	Node     string
	Listener string
	Key      string
}

// Responder is the local HTTP origin behind every lab node. It is the
// ground truth for traffic: a request counts only once the responder has
// received it, and each harness-assigned request ID must arrive once.
type Responder struct {
	addr string
	srv  *http.Server
	done chan error

	mu         sync.Mutex
	seen       map[string]Observation
	counts     map[Observation]int
	duplicates []string
	missingID  int
}

// StartResponder listens on an ephemeral loopback port and serves until
// Close.
func StartResponder() (*Responder, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("responder listen: %w", err)
	}
	r := &Responder{
		addr:   ln.Addr().String(),
		done:   make(chan error, 1),
		seen:   map[string]Observation{},
		counts: map[Observation]int{},
	}
	r.srv = &http.Server{
		Handler:           http.HandlerFunc(r.serveHTTP),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { r.done <- r.srv.Serve(ln) }()
	return r, nil
}

// Addr returns the responder's host:port.
func (r *Responder) Addr() string { return r.addr }

func (r *Responder) serveHTTP(w http.ResponseWriter, req *http.Request) {
	id := req.Header.Get(RequestIDHeader)
	obs := Observation{
		Node:     req.Header.Get(NodeHeader),
		Listener: req.Header.Get(ListenerHeader),
		Key:      req.Header.Get(KeyHeader),
	}
	r.mu.Lock()
	switch {
	case id == "":
		r.missingID++
	case r.seenID(id):
		r.duplicates = append(r.duplicates, id)
	default:
		r.seen[id] = obs
		r.counts[obs]++
	}
	r.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("ok\n"))
}

func (r *Responder) seenID(id string) bool {
	_, ok := r.seen[id]
	return ok
}

// Count returns how many distinct request IDs were observed for obs.
func (r *Responder) Count(obs Observation) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[obs]
}

// Counts returns a copy of all per-observation counts.
func (r *Responder) Counts() map[Observation]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[Observation]int, len(r.counts))
	for k, v := range r.counts {
		out[k] = v
	}
	return out
}

// Seen reports whether the responder received the request with this ID.
func (r *Responder) Seen(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seenID(id)
}

// Anomalies returns request IDs received more than once and the number of
// requests that arrived without an ID. Both must be empty in a sound run.
func (r *Responder) Anomalies() (duplicates []string, missingID int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.duplicates...), r.missingID
}

// Close stops the responder and waits for its serve loop to exit.
func (r *Responder) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := r.srv.Shutdown(ctx)
	if serr := <-r.done; serr != nil && !errors.Is(serr, http.ErrServerClosed) {
		err = errors.Join(err, serr)
	}
	return err
}
