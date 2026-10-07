package peersession

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Output backlog bounds. Once established, a session hands every message
// to its own writer goroutine through a byte queue, so a source that is
// slow to read never stops the session from reading what the source
// sends: its input stays current, and its heartbeats and resync controls
// are answered. While more than SoftBacklog bytes wait, the session
// defers publishing output changes, refreshes, and lease markers; the
// output store coalesces the deferred changes per key, so the backlog of
// a slow source grows by controls and acknowledgements only, never by
// superseded values. A teach (answering a resync request, or a table the
// source has just announced) is queued whole; HardBacklog bounds what
// one session may queue, and exceeding it ends the session with ErrLimit.
const (
	SoftBacklog = 256 << 10
	HardBacklog = 64 << 20
)

// maxWriteChunk bounds one write call, so the per-write deadline (the
// idle timeout) bounds how long the writer waits for a source to take a
// limited amount of data, as before the queue existed.
const maxWriteChunk = 64 << 10

// sendQueue is the established session's outgoing byte queue, drained in
// order by one writer goroutine.
type sendQueue struct {
	mu      sync.Mutex
	buf     []byte // queued bytes from off on
	off     int
	spare   []byte
	closing bool
	wake    chan struct{}
	done    chan struct{}

	// total counts bytes ever queued (under mu), and written bytes ever
	// written; revEnd is the total at the end of the last revocation
	// queued and revGen the live-marker count it covers (under mu).
	total   int64
	written atomic.Int64
	revEnd  int64
	revGen  uint64

	queued  atomic.Int64 // bytes queued or being written
	maxSeen atomic.Int64
	failed  atomic.Bool
	errMu   sync.Mutex
	err     error
	stopped atomic.Bool
}

// startWriter starts the writer goroutine; every later write is queued.
func (c *Conn) startWriter(ctx context.Context) {
	q := &sendQueue{wake: make(chan struct{}, 1), done: make(chan struct{})}
	c.wq = q
	c.wqA.Store(q)
	go c.writeLoop(ctx, q)
}

// enqueue queues b, which the caller may reuse afterwards. It fails with
// the writer's error once a write failed, or ErrLimit beyond HardBacklog.
func (c *Conn) enqueue(b []byte) error {
	q := c.wq
	if err := q.error(); err != nil {
		return err
	}
	n := q.queued.Add(int64(len(b)))
	if n > HardBacklog {
		return wrap(ErrLimit, "output backlog of %d bytes exceeds %d: the source is not reading", n, HardBacklog)
	}
	if n > q.maxSeen.Load() {
		q.maxSeen.Store(n)
	}
	q.mu.Lock()
	q.buf = append(q.buf, b...)
	q.total += int64(len(b))
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	c.lastTx = time.Now()
	return nil
}

func (q *sendQueue) error() error {
	if !q.failed.Load() {
		return nil
	}
	q.errMu.Lock()
	defer q.errMu.Unlock()
	return q.err
}

// writeLoop writes the queue in order until it is closed and empty, the
// context ends, or a write fails. A failure is recorded for Run, which the
// writer interrupts by expiring the read deadline.
func (c *Conn) writeLoop(ctx context.Context, q *sendQueue) {
	defer close(q.done)
	for {
		q.mu.Lock()
		for q.off == len(q.buf) {
			if q.closing {
				q.mu.Unlock()
				return
			}
			q.mu.Unlock()
			select {
			case <-q.wake:
			case <-ctx.Done():
				return
			}
			q.mu.Lock()
		}
		n := min(len(q.buf)-q.off, maxWriteChunk)
		q.spare = append(q.spare[:0], q.buf[q.off:q.off+n]...)
		chunk := q.spare
		q.off += n
		switch {
		case q.off == len(q.buf):
			q.buf, q.off = q.buf[:0], 0
		case q.off > len(q.buf)/2:
			q.buf, q.off = append(q.buf[:0], q.buf[q.off:]...), 0
		}
		q.mu.Unlock()
		err := c.writeChunk(ctx, q, chunk)
		left := q.queued.Add(-int64(len(chunk)))
		if err == nil {
			c.noteWritten(q, int64(len(chunk)))
		}
		if err != nil {
			q.errMu.Lock()
			q.err = err
			q.errMu.Unlock()
			q.failed.Store(true)
			_ = c.nc.SetReadDeadline(aLongTimeAgo)
			return
		}
		if left <= SoftBacklog && c.outBlocked.CompareAndSwap(true, false) {
			c.outWake.Store(true)
			_ = c.nc.SetReadDeadline(aLongTimeAgo)
		}
	}
}

func (c *Conn) writeChunk(ctx context.Context, q *sendQueue, b []byte) error {
	if err := c.nc.SetWriteDeadline(time.Now().Add(c.opts.IdleTimeout)); err != nil {
		return wrapCause(ErrIO, err, "set write deadline")
	}
	if q.stopped.Load() {
		return wrap(ErrIO, "writer stopped")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := c.nc.Write(b); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return wrapCause(ErrIO, err, "write")
	}
	return nil
}

// noteRevocation records that a revocation covering every live marker
// counted so far has just been queued (its last byte is the queue's
// total).
func (c *Conn) noteRevocation() {
	q := c.wq
	q.mu.Lock()
	end, gen := q.total, c.liveGen.Load()
	q.revEnd, q.revGen = end, gen
	q.mu.Unlock()
	// The writer may already have written those bytes before the
	// boundary was recorded; then it will not check again.
	if q.written.Load() >= end {
		c.markWithdrawn(gen)
	}
}

// markWithdrawn records that every live marker up to gen is withdrawn.
func (c *Conn) markWithdrawn(gen uint64) {
	for {
		old := c.revWritten.Load()
		if gen <= old || c.revWritten.CompareAndSwap(old, gen) {
			return
		}
	}
}

// noteWritten records n more bytes written, and once the last queued
// revocation is entirely written, that the live markers it covers are
// withdrawn.
func (c *Conn) noteWritten(q *sendQueue, n int64) {
	w := q.written.Add(n)
	q.mu.Lock()
	end, gen := q.revEnd, q.revGen
	q.mu.Unlock()
	if end > 0 && w >= end {
		c.markWithdrawn(gen)
	}
}

// deferOutput applies a backlog: while blocked, it consumes the output
// wake into outPending (the store keeps coalescing changes, which are sent
// once the writer has drained the backlog and woken Run). It returns
// whether output must still wait. The writer may have drained the queue,
// cleared outBlocked, and set its wake after backlogged armed it; that
// wake may have just been consumed here, so output resumes at once rather
// than waiting for the next read.
func (c *Conn) deferOutput(blocked bool) bool {
	if !blocked {
		return false
	}
	if c.outWake.Swap(false) && !c.outPending {
		c.outPending = true
		c.deferred.Add(1)
	}
	return c.outBlocked.Load()
}

// stopWriter lets the writer flush what is queued for at most drain, then
// interrupts it and waits for it to exit.
func (c *Conn) stopWriter(drain time.Duration) {
	q := c.wq
	if q == nil {
		return
	}
	q.mu.Lock()
	q.closing = true
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	t := time.NewTimer(drain)
	defer t.Stop()
	select {
	case <-q.done:
		return
	case <-t.C:
	}
	q.stopped.Store(true)
	_ = c.nc.SetWriteDeadline(aLongTimeAgo)
	<-q.done
}

// backlogged reports whether output must wait for the writer: more than
// SoftBacklog bytes are queued. It arms the writer to wake Run once the
// queue drains to SoftBacklog, re-checking after arming so that a drain
// in between is not missed.
func (c *Conn) backlogged() bool {
	if c.wq == nil || c.wq.queued.Load() <= SoftBacklog {
		return false
	}
	c.outBlocked.Store(true)
	if c.wq.queued.Load() <= SoftBacklog {
		c.outBlocked.Store(false)
		return false
	}
	return true
}
