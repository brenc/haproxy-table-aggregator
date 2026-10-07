package peersession

import (
	"context"
	"fmt"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
)

// outFlushSize is the buffered output size that triggers a write while
// encoding a batch of output updates.
const outFlushSize = 32 << 10

// watchOutput wakes Run whenever the output store changes, until the
// returned stop function is called. Run calls it before it reads the
// store: the watcher takes the store's change channel before any read,
// and takes the next one before signalling, so every change after a read
// is followed by a wake-up.
func (c *Conn) watchOutput(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	ch := c.opts.Output.Changed()
	go func() {
		defer close(done)
		for {
			select {
			case <-ch:
			case <-ctx.Done():
				return
			}
			ch = c.opts.Output.Changed()
			c.outWake.Store(true)
			_ = c.nc.SetReadDeadline(aLongTimeAgo)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// teach sends, for every output table the source has announced with a
// matching definition, its definition followed by all of its entries, then
// records the store sequence it is current to. It answers each resync
// request from the source. Tables the source has not announced are left
// for teachTable.
func (c *Conn) teach(ctx context.Context) error {
	if err := c.resend(ctx); err != nil {
		return err
	}
	c.teaches.Add(1)
	return nil
}

// refresh re-sends the whole output when it is due (see
// Options.Refresh): every entry of every taught table under this
// session's generation, then the lease again. Like a teach, it writes the
// values before the marker, so a marker still certifies only values this
// session wrote ahead of it.
func (c *Conn) refresh(ctx context.Context, now time.Time) error {
	if c.opts.Output == nil || c.opts.Refresh <= 0 {
		return nil
	}
	for _, t := range c.outs {
		if !t.ready {
			return nil
		}
	}
	if c.refreshAt.IsZero() {
		c.refreshAt = now.Add(c.opts.Refresh)
		return nil
	}
	if now.Before(c.refreshAt) {
		return nil
	}
	if err := c.resend(ctx); err != nil {
		return err
	}
	c.refreshes.Add(1)
	return nil
}

// rotate switches the session to a new generation after Store.Retire and
// re-sends the remaining output under it; the lease follows in the same
// publish. HAProxy's copies of retired keys keep the old generation, which
// no later marker of this session matches, and expire there because no
// session sends them again.
func (c *Conn) rotate(ctx context.Context) error {
	for {
		g, err := output.NewGeneration()
		if err != nil {
			return fmt.Errorf("peersession: %w", err)
		}
		if g != c.gen {
			c.gen = g
			break
		}
	}
	c.genA.Store(c.gen)
	c.rotations.Add(1)
	return c.resend(ctx)
}

// resend sends every ready table's definition and entries, records the
// store sequence it is current to, and makes the next publish write the
// lease again.
func (c *Conn) resend(ctx context.Context) error {
	entries, seq := c.opts.Output.Since(0)
	next := 0
	for _, t := range c.outs {
		if t.ready {
			if err := c.appendDefinition(t); err != nil {
				return err
			}
			t.taughtSeq = seq
		}
		for ; next < len(entries) && entries[next].Table == t.def.Name; next++ {
			if !t.ready {
				continue
			}
			if err := c.appendOutUpdate(ctx, t, entries[next], true); err != nil {
				return err
			}
		}
	}
	if err := c.flushOut(ctx); err != nil {
		return err
	}
	c.outSeq = seq
	c.leaseAfterTeach()
	if !c.refreshAt.IsZero() {
		c.refreshAt = time.Now().Add(c.opts.Refresh)
	}
	return nil
}

// teachTable sends t's definition and every entry it holds. It runs once
// per session, when the source first announces a definition of t that
// matches, which is what makes it safe to write: until then the session
// sends nothing for t, because stock HAProxy applies an update to any
// table of that name whose key type matches, whatever it stores.
func (c *Conn) teachTable(ctx context.Context, t *outTable) error {
	entries, seq := c.opts.Output.Since(0)
	t.taughtSeq = seq
	if err := c.appendDefinition(t); err != nil {
		return err
	}
	for _, e := range entries {
		if e.Table != t.def.Name {
			continue
		}
		if err := c.appendOutUpdate(ctx, t, e, true); err != nil {
			return err
		}
	}
	if err := c.flushOut(ctx); err != nil {
		return err
	}
	c.teaches.Add(1)
	c.leaseAfterTeach()
	return nil
}

// leaseAfterTeach makes the next publish write the lease again: a teach
// may have completed the output, and the marker is written only after the
// values, never taught from the store.
func (c *Conn) leaseAfterTeach() {
	c.markerDue = true
	c.outWake.Store(true)
}

// beforeLeaseHook, when set by a test, runs in publish after the values
// are queued and before the lease marker is written.
var beforeLeaseHook func(*Conn)

// publish sends every entry changed since the last teach or publish,
// switching tables with a definition as HAProxy does. Entries of tables
// the source has not announced yet are skipped; teachTable sends them
// once it does.
func (c *Conn) publish(ctx context.Context) error {
	// The lease is read before the values, so every value it certifies
	// (up to lease.Seq) is among those written below or earlier.
	lease := c.opts.Output.Lease()
	if lease.Valid {
		// A live marker of this lease may follow the values; report it
		// as possibly outstanding from now on, until writeLease has
		// counted it (or decided not to write it).
		c.markerPending.Store(true)
		defer c.markerPending.Store(false)
	}
	if r := c.opts.Output.Retired(); r != c.retiredSeen {
		c.retiredSeen = r
		if err := c.rotate(ctx); err != nil {
			return err
		}
	}
	entries, seq := c.opts.Output.Since(c.outSeq)
	for _, e := range entries {
		t := c.outNames[e.Table]
		if t == nil {
			return fmt.Errorf("peersession: output store entry for unannounced table %s", e.Table)
		}
		if !t.ready || e.Seq <= t.taughtSeq {
			continue // not announced yet, or already taught
		}
		if c.outSel != t.id {
			if err := c.appendDefinition(t); err != nil {
				return err
			}
		}
		if err := c.appendOutUpdate(ctx, t, e, false); err != nil {
			return err
		}
	}
	if err := c.flushOut(ctx); err != nil {
		return err
	}
	c.outSeq = seq
	if beforeLeaseHook != nil {
		beforeLeaseHook(c)
	}
	return c.writeLease(ctx, lease)
}

// writeLease writes lease l into every metadata table as a timed update,
// unless this session already wrote it or has not yet taught every
// output table (then the marker would certify values the source may not
// have). The caller has written every value l certifies. A lease that has
// run out is not written: HAProxy already drops the previous marker on
// its own.
func (c *Conn) writeLease(ctx context.Context, l output.Lease) error {
	if l.Gen == c.leaseSent && !c.markerDue {
		return nil
	}
	for _, t := range c.outs {
		if !t.ready {
			return nil
		}
	}
	values, remaining, ok := c.opts.Output.Marker(l, c.gen)
	c.leaseSent, c.markerDue = l.Gen, false
	if !ok {
		return nil
	}
	n := 0
	for _, t := range c.outs {
		if t.kind != output.KindMetadata {
			continue
		}
		if l.Valid && n == 0 {
			// Counted before the marker is queued, so that from now on
			// Stats.LiveMarker reports it may be outstanding.
			c.liveGen.Add(1)
		}
		if c.outSel != t.id {
			if err := c.appendDefinition(t); err != nil {
				return err
			}
		}
		e := output.Entry{Table: t.def.Name, Key: output.MetadataKey, Values: values}
		if err := c.appendUpdate(ctx, t, e, true, remaining); err != nil {
			return err
		}
		n++
	}
	if err := c.flushOut(ctx); err != nil {
		return err
	}
	if n > 0 {
		c.markers.Add(1)
		if !l.Valid {
			c.revocations.Add(1)
			c.noteRevocation()
		}
		c.markerDL.Store(values[output.SlotDeadline])
		c.markerNano.Store(time.Now().UnixNano())
	}
	return nil
}

// appendDefinition buffers t's definition, which also selects t for the
// updates that follow.
func (c *Conn) appendDefinition(t *outTable) error {
	b, err := peermsg.AppendDefinition(c.obuf, t.id, t.def)
	if err != nil {
		return fmt.Errorf("peersession: encode definition of %s: %w", t.def.Name, err)
	}
	c.obuf = b
	c.outSel = t.id
	return nil
}

// appendOutUpdate buffers one aggregate entry update, stamped with the
// session's generation. Aggregate updates are ordinary, not timed, so
// HAProxy gives each entry its own table's full expire from reception; a
// renewed lifetime grants no authority, which only the lease marker
// does.
func (c *Conn) appendOutUpdate(ctx context.Context, t *outTable, e output.Entry, teaching bool) error {
	e.Values[output.SlotGeneration] = c.gen
	if err := c.appendUpdate(ctx, t, e, false, 0); err != nil {
		return err
	}
	c.outUpdates.Add(1)
	if teaching {
		c.taught.Add(1)
	}
	return nil
}

// appendUpdate buffers one entry update with the table's next update ID,
// timed with remaining if timed. Every update carries its ID explicitly:
// HAProxy 3.4.6 records implicit IDs byte-swapped (see the phase 03
// record).
func (c *Conn) appendUpdate(ctx context.Context, t *outTable, e output.Entry, timed bool, remaining peermsg.Millis) error {
	id := t.last.Next()
	b, err := peermsg.AppendUpdate(c.obuf, t.def, peermsg.Update{
		ID: id, ExplicitID: true, Timed: timed, Remaining: remaining, Key: e.Key,
		Values: []peermsg.Value{{Type: peermsg.DataGPT, Array: e.Values[:]}},
	})
	if err != nil {
		return fmt.Errorf("peersession: encode update of %s: %w", t.def.Name, err)
	}
	c.obuf = b
	t.last = id
	t.sent++
	t.sentA.Store(t.sent)
	t.lastSentA.Store(uint32(id))
	if len(c.obuf) >= outFlushSize {
		return c.flushOut(ctx)
	}
	return nil
}

func (c *Conn) flushOut(ctx context.Context) error {
	if len(c.obuf) == 0 {
		return nil
	}
	err := c.write(ctx, c.obuf, time.Now().Add(c.opts.IdleTimeout))
	c.obuf = c.obuf[:0]
	return err
}

// ack records the source's acknowledgement of an output table. Its table
// ID is in this side's namespace, never the source's, and it must name
// an update this session sent for that table.
func (c *Conn) ack(m peermsg.AckMessage) error {
	if c.local == nil {
		return protocol("acknowledgement for local table %d, but this session announced no tables", m.ID)
	}
	if _, err := c.local.Definition(m.ID); err != nil {
		return protocolCause(err, "acknowledgement for local table %d", m.ID)
	}
	t := c.outs[m.ID-1]
	// The IDs this session sent for t are the t.sent consecutive serial
	// numbers ending at t.last.
	if t.sent == 0 || uint64(uint32(t.last-m.Update)) >= t.sent {
		return protocol("acknowledgement of update %d of %s, which this session did not send (last sent %d, %d sent)",
			m.Update, t.def.Name, t.last, t.sent)
	}
	t.lastAcked.Store(uint32(m.Update))
	t.acked.Store(true)
	c.acksRecv.Add(1)
	return nil
}
