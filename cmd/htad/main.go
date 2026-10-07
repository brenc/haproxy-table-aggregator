// Command htad is the aggregator daemon. It keeps one peers-protocol
// session per configured HAProxy source, keeps each source's validated,
// expiring snapshot in memory (package snapshot), and publishes the
// aggregate rate of each input table into its aggregate output table on
// every source, with the readiness lease that makes the output
// authoritative while every source is ready (package publish). HAProxy
// enforces locally from those tables; nothing calls the daemon per
// request. It also writes each validated session and table event to
// stdout as one JSON object per line, logs source state changes, roster
// readiness, and authority changes, and keeps no state across restarts.
//
//	htad -config htad.json [-log-level info]
//
// Logs go to stderr. SIGINT or SIGTERM revokes the lease, waits at most
// 500 ms for every session to write the revocation, closes every session,
// and exits 0. The configuration format is documented in
// internal/config; plaintext sessions require its explicit isolated
// loopback lab switch.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/publish"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// After the first signal, restore the default disposition so that a
	// second one terminates the process even if shutdown is stuck.
	context.AfterFunc(ctx, stop)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "htad:", err)
		os.Exit(1)
	}
}

// run starts the daemon and returns after ctx ends and every session has
// closed, or on a startup error. A failure to write events also stops the
// daemon. Updates are acknowledged when the event queue accepts them, before
// they are written, so events queued when a write fails may already be
// acknowledged; this phase stores nothing, so they are only lost from the
// output. If stdout stops accepting writes, shutdown waits at most
// outputDrainTimeout for queued events to be written and then returns an
// error.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runWith(ctx, args, stdout, stderr, hooks{})
}

// maintainInterval is how often the daemon feeds session liveness into the
// snapshot store, expires entries, and reports state changes. Reads judge
// health and expiry at their own time, so it only bounds how late a
// change is logged and expired memory is released.
const maintainInterval = 250 * time.Millisecond

// maintain runs the snapshot store's housekeeping until ctx ends: it
// records each session's last message read (heartbeats included, which
// produce no events), removes expired entries, and logs every source
// state change and every change of roster readiness.
func maintain(ctx context.Context, m *sources.Manager, store *snapshot.Store, log *slog.Logger) {
	tick := time.NewTicker(maintainInterval)
	defer tick.Stop()
	states := map[string]snapshot.State{}
	ready := false
	for {
		store.ObserveStatus(m.Status())
		store.Expire()
		r := store.Roster()
		for _, src := range r.Sources {
			if states[src.Name] != src.State {
				states[src.Name] = src.State
				log.Info("source state", "source", src.Name, "state", src.State.String(), "session", src.Session,
					"entries", src.Entries, "reason", src.Reason)
			}
		}
		if r.Ready != ready {
			ready = r.Ready
			if ready {
				log.Info("roster ready", "sources", len(r.Sources))
			} else {
				log.Info("roster not ready", "not_ready", len(r.NotReady()))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// revocationDrain bounds how long shutdown waits for every session to
// write the revocation marker. A source the revocation does not reach in
// time falls back to local protection when its last marker runs out (at
// most output.MaxLeaseLength later).
const revocationDrain = 500 * time.Millisecond

// revocationsWritten reports whether no established session may still
// leave its source holding a live lease marker (peersession.Stats
// LiveMarker): each wrote no live marker, or a revocation queued after its
// last one has been entirely written to the connection. Sessions that
// ended are not waited for.
func revocationsWritten(statuses []sources.SourceStatus) bool {
	for _, st := range statuses {
		if st.Up && st.Stats.LiveMarker {
			return false
		}
	}
	return true
}

// awaitRevocation waits at most bound for revocationsWritten, reading the
// sessions afresh each time, and reports whether it held.
func awaitRevocation(m *sources.Manager, bound time.Duration) bool {
	deadline := time.Now().Add(bound)
	for !revocationsWritten(m.Status()) {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// start builds the stores and the publisher and starts the sessions.
func start(ctx context.Context, cfg config.Config, log *slog.Logger, h hooks) (*daemon, error) {
	snap, err := snapshot.New(snapshot.OptionsFrom(cfg))
	if err != nil {
		return nil, err
	}
	out, err := output.NewStore(cfg.OutputTables(), output.StoreOptions{})
	if err != nil {
		return nil, err
	}
	d := &daemon{snap: snap, out: out}
	apply := snap.Apply
	var routes []publish.Route
	for _, o := range cfg.Outputs {
		if o.Kind == output.KindAggregate {
			routes = append(routes, publish.Route{Input: o.Input, Output: o.Name})
		}
	}
	if len(routes) > 0 {
		opts := publish.Options{
			Snapshot: snap, Output: out, Routes: routes, Logger: log,
			// The manager exists before the publisher runs.
			Status: func() []sources.SourceStatus { return d.m.Status() },
		}
		if h.publish != nil {
			h.publish(&opts)
		}
		if d.pub, err = publish.New(opts); err != nil {
			return nil, err
		}
		apply = d.pub.Apply
	}
	var ln net.Listener
	if h.listener != nil && cfg.Listen != "" {
		var lc net.ListenConfig
		raw, lerr := lc.Listen(ctx, "tcp", cfg.Listen)
		if lerr != nil {
			return nil, fmt.Errorf("listen: %w", lerr)
		}
		ln = h.listener(raw)
	}
	sopts := sources.Options{Config: cfg, Logger: log, Apply: apply, Dial: h.dial, Listener: ln}
	if len(cfg.Outputs) > 0 {
		sopts.Output = out
	}
	d.m, err = sources.Start(ctx, sopts)
	if err != nil {
		if ln != nil {
			_ = ln.Close()
		}
		return nil, err
	}
	return d, nil
}

// outputDrainTimeout bounds how long shutdown waits for the event writer.
var outputDrainTimeout = 5 * time.Second

// daemon is the running daemon's state, as tests see it.
type daemon struct {
	m    *sources.Manager
	snap *snapshot.Store
	out  *output.Store
	pub  *publish.Publisher // nil without aggregate outputs
}

// hooks lets tests observe and steer the daemon.
type hooks struct {
	// ready receives the daemon once it is running.
	ready func(*daemon)
	// dial replaces the TCP dialer for outbound sessions.
	dial func(ctx context.Context, address string) (net.Conn, error)
	// listener wraps the listener accepting inbound sessions.
	listener func(net.Listener) net.Listener
	// publish adjusts the publisher's options.
	publish func(*publish.Options)
}

// runWith is run with test hooks.
func runWith(ctx context.Context, args []string, stdout, stderr io.Writer, h hooks) (err error) {
	fs := flag.NewFlagSet("htad", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("config", "", "configuration file (JSON, see internal/config)")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn, or error")
	if err = fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %q", fs.Args())
	}
	if *cfgPath == "" {
		return errors.New("no configuration: pass -config")
	}
	var level slog.Level
	if err = level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("-log-level: %w", err)
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil // signalled during startup: nothing to shut down yet
	}
	log := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	d, err := start(ctx, cfg, log, h)
	if err != nil {
		return err
	}
	m, store := d.m, d.snap
	maintained := make(chan struct{})
	go func() {
		defer close(maintained)
		maintain(ctx, m, store, log)
	}()
	defer func() {
		cancel()
		<-maintained
	}()
	published := make(chan struct{})
	stopPublishing := make(chan struct{})
	go func() {
		defer close(published)
		if d.pub != nil {
			d.pub.Run(stopPublishing)
		}
	}()
	// On shutdown the publisher revokes its lease first, and the sessions
	// get a bounded time to write the revocation before they close.
	stopPublisher := sync.OnceFunc(func() {
		close(stopPublishing)
		<-published
		if d.pub != nil && !awaitRevocation(m, revocationDrain) {
			log.Warn("revocation not written to every source before shutdown; their markers expire on their own",
				"bound", revocationDrain)
		}
	})
	defer stopPublisher()
	if addr := m.Addr(); addr != nil {
		log.Info("listening", "address", addr.String(), "local_peer", cfg.LocalPeer)
	}
	if h.ready != nil {
		h.ready(d)
	}

	out := bufio.NewWriter(stdout)
	writeErr := make(chan error, 1)
	go func() {
		var werr error
		for ev := range m.Events() {
			if werr != nil {
				continue // keep draining so sessions can finish
			}
			if werr = writeEvent(out, ev); werr == nil && len(m.Events()) == 0 {
				werr = out.Flush()
			}
			if werr != nil {
				log.Error("writing events failed; shutting down", "err", werr)
				cancel()
			}
		}
		if werr == nil {
			werr = out.Flush()
		}
		writeErr <- werr
	}()

	<-ctx.Done()
	log.Info("shutting down")
	stopPublisher()
	err = m.Close()
	drain := time.NewTimer(outputDrainTimeout)
	defer drain.Stop()
	select {
	case werr := <-writeErr:
		if werr != nil {
			err = errors.Join(err, fmt.Errorf("writing events: %w", werr))
		}
	case <-drain.C:
		// The writer is blocked in stdout; it cannot be interrupted, and
		// the process exits on this error.
		err = errors.Join(err, fmt.Errorf("event output blocked for %v; events lost", outputDrainTimeout))
	}
	if err == nil {
		log.Info("stopped")
	}
	return err
}
