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
	c.teaches.Add(1)
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
	return nil
}

// publish sends every entry changed since the last teach or publish,
// switching tables with a definition as HAProxy does. Entries of tables
// the source has not announced yet are skipped; teachTable sends them
// once it does.
func (c *Conn) publish(ctx context.Context) error {
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

// appendOutUpdate buffers one entry update with the table's next update
// ID. Every update carries its ID explicitly: HAProxy 3.4.6 records
// implicit IDs byte-swapped (see the phase 03 record). Updates are
// ordinary, not timed, so HAProxy gives each entry its own table's full
// expire from reception.
func (c *Conn) appendOutUpdate(ctx context.Context, t *outTable, e output.Entry, teaching bool) error {
	id := t.last.Next()
	b, err := peermsg.AppendUpdate(c.obuf, t.def, peermsg.Update{
		ID: id, ExplicitID: true, Key: e.Key,
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
	c.outUpdates.Add(1)
	if teaching {
		c.taught.Add(1)
	}
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
