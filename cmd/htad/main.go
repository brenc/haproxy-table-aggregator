// Command htad is the aggregator daemon. In this phase it keeps one
// peers-protocol session per configured HAProxy source, writes each
// validated session and table event to stdout as one JSON object per line,
// and keeps each source's validated, expiring snapshot in memory (package
// snapshot), logging every source state change and roster readiness. It
// aggregates nothing, publishes nothing, and keeps no state across
// restarts.
//
//	htad -config htad.json [-log-level info]
//
// Logs go to stderr. SIGINT or SIGTERM closes every session and exits 0.
// The configuration format is documented in internal/config; plaintext
// sessions require its explicit isolated loopback lab switch.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
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
	return runWith(ctx, args, stdout, stderr, nil)
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

// outputDrainTimeout bounds how long shutdown waits for the event writer.
var outputDrainTimeout = 5 * time.Second

// runWith is run with a hook that receives the manager once it is running.
func runWith(ctx context.Context, args []string, stdout, stderr io.Writer, ready func(*sources.Manager)) (err error) {
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
	store, err := snapshot.New(snapshot.OptionsFrom(cfg))
	if err != nil {
		return err
	}
	m, err := sources.Start(ctx, sources.Options{Config: cfg, Logger: log, Apply: store.Apply})
	if err != nil {
		return err
	}
	maintained := make(chan struct{})
	go func() {
		defer close(maintained)
		maintain(ctx, m, store, log)
	}()
	defer func() {
		cancel()
		<-maintained
	}()
	if addr := m.Addr(); addr != nil {
		log.Info("listening", "address", addr.String(), "local_peer", cfg.LocalPeer)
	}
	if ready != nil {
		ready(m)
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
