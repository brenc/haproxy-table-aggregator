package peersession

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// protocolFailure is a protocol violation by the source, answered with
// the error message typ before the session ends.
type protocolFailure struct {
	typ peerwire.MessageType
	err error
}

func (p *protocolFailure) Error() string { return p.err.Error() }
func (p *protocolFailure) Unwrap() error { return p.err }

func protocol(format string, args ...any) error {
	return &protocolFailure{typ: peerwire.ErrorTypeProtocol, err: wrap(ErrProtocol, format, args...)}
}

func protocolCause(cause error, format string, args ...any) error {
	return &protocolFailure{typ: peerwire.ErrorTypeProtocol, err: wrapCause(ErrProtocol, cause, format, args...)}
}

// Run exchanges binary messages on an established session until ctx ends
// (returning ctx's error) or the session fails. It delivers validated
// events to sink in wire order and returns an error wrapping one of the
// package's failure classes. It does not close the connection.
func (c *Conn) Run(ctx context.Context, sink Sink) (err error) {
	if Phase(c.phase.Load()) != PhaseEstablished {
		return errors.New("peersession: Run before a successful handshake")
	}
	defer c.phase.Store(uint32(PhaseClosed))
	defer func() {
		// Cancellation interrupts I/O and the sink; report it as such
		// rather than as whatever failure the interruption caused.
		if cerr := ctx.Err(); cerr != nil {
			err = cerr
		}
	}()
	defer c.watch(ctx)()
	c.startWriter(ctx)
	defer func() {
		// Let a final error message out unless the session was
		// cancelled.
		drain := errorTimeout
		if ctx.Err() != nil {
			drain = 0
		}
		c.stopWriter(drain)
	}()
	now := time.Now()
	c.lastRx, c.rxTime = now, now
	if c.lastTx.IsZero() {
		c.lastTx = now
	}
	if c.opts.RequestResync {
		if err := c.requestResync(ctx); err != nil {
			return err
		}
	}
	if c.opts.Output != nil {
		defer c.watchOutput(ctx)()
	}
	for {
		if perr := c.process(ctx, sink); perr != nil {
			return c.fail(ctx, perr)
		}
		if c.eof {
			if c.dec.Buffered() > 0 {
				return wrap(ErrClosed, "connection closed inside a message")
			}
			return wrap(ErrClosed, "end of stream")
		}
		if err := c.wq.error(); err != nil {
			return err
		}
		if err := c.flushAcks(ctx); err != nil {
			return err
		}
		blocked := c.deferOutput(c.backlogged())
		if !blocked {
			if err := c.refresh(ctx, time.Now()); err != nil {
				return err
			}
			if c.outWake.Swap(false) || c.outPending {
				c.outPending = false
				if err := c.publish(ctx); err != nil {
					return err
				}
			}
		}
		if err := c.tick(ctx, time.Now()); err != nil {
			return err
		}
		if !c.resyncAt.IsZero() && !time.Now().Before(c.resyncAt) {
			if err := c.requestResync(ctx); err != nil {
				return err
			}
		}
		next := minTime(c.lastRx.Add(c.opts.IdleTimeout), c.lastTx.Add(c.opts.Heartbeat))
		if !c.refreshAt.IsZero() && !blocked {
			next = minTime(next, c.refreshAt)
		}
		if !c.resyncAt.IsZero() {
			next = minTime(next, c.resyncAt)
		}
		if err := c.fill(ctx, next); err != nil && !errors.Is(err, errDeadline) {
			return err
		}
	}
}

// requestResync asks the source to teach every table it shares.
func (c *Conn) requestResync(ctx context.Context) error {
	c.resyncAt = time.Time{}
	if err := c.sendControl(ctx, peerwire.ControlResyncRequest); err != nil {
		return err
	}
	c.setLearn(LearnRequested)
	c.resyncRequests.Add(1)
	return nil
}

// fail sends the error message a protocol failure calls for, best effort,
// and returns err.
func (c *Conn) fail(ctx context.Context, err error) error {
	var pf *protocolFailure
	if errors.As(err, &pf) && ctx.Err() == nil {
		if b, aerr := peerwire.AppendFrame(nil, peerwire.ClassError, pf.typ, nil); aerr == nil {
			_ = c.write(ctx, b, time.Now().Add(errorTimeout))
		}
	}
	return err
}

// tick enforces the idle timeout and sends a heartbeat when this side has
// been silent for the heartbeat interval.
func (c *Conn) tick(ctx context.Context, now time.Time) error {
	if idle := now.Sub(c.lastRx); idle >= c.opts.IdleTimeout {
		return wrap(ErrIdle, "no message for %v", idle.Round(time.Millisecond))
	}
	nudged := false
	if at := c.nudgeAt.Load(); at != 0 && at <= now.UnixNano() {
		nudged = c.nudgeAt.CompareAndSwap(at, 0)
	}
	if nudged || now.Sub(c.lastTx) >= c.opts.Heartbeat {
		if err := c.sendControl(ctx, peerwire.ControlHeartbeat); err != nil {
			return err
		}
		c.txHeartbeats.Add(1)
	}
	return nil
}

func (c *Conn) sendControl(ctx context.Context, typ peerwire.MessageType) error {
	b, err := peerwire.AppendFrame(c.wbuf[:0], peerwire.ClassControl, typ, nil)
	if err != nil {
		return err
	}
	c.wbuf = b
	return c.write(ctx, b, time.Now().Add(c.opts.IdleTimeout))
}

// process handles every complete frame buffered.
func (c *Conn) process(ctx context.Context, sink Sink) error {
	for {
		f, err := c.dec.NextFrame()
		switch {
		case err == nil:
		case errors.Is(err, peerwire.ErrNeedMore), errors.Is(err, io.EOF):
			return nil
		case errors.Is(err, peerwire.ErrTruncated):
			return nil // reported by Run as a close inside a message
		case errors.Is(err, peerwire.ErrMessageTooLarge):
			return &protocolFailure{typ: peerwire.ErrorTypeSizeLimit, err: wrapCause(ErrProtocol, err, "framing")}
		default:
			return protocolCause(err, "framing")
		}
		// Idleness counts from when a message is taken in, not from the
		// read that delivered it: a burst buffered behind a slow sink must
		// not make a live source look idle.
		c.lastRx = time.Now()
		c.lastRxMono.Store(int64(c.rxTime.Sub(monoBase)))
		c.rxMessages.Add(1)
		if err := c.handle(ctx, f, sink); err != nil {
			return err
		}
		if err := c.tick(ctx, time.Now()); err != nil {
			return err
		}
	}
}

func (c *Conn) handle(ctx context.Context, f peerwire.Frame, sink Sink) error {
	if f.Class == peerwire.ClassStickTable && peermsg.IsUpdateType(f.Type) {
		if id, ok := c.in.Selected(); ok {
			if t := c.tables[id]; t != nil && t.kind == kindIgnored {
				c.skipped.Add(1)
				return nil
			}
		}
	}
	msg, err := c.in.Decode(f, c.rxTime)
	if err != nil {
		return c.decodeError(ctx, f, err, sink)
	}
	switch m := msg.(type) {
	case peermsg.Control:
		return c.control(ctx, m.Type, sink)
	case peermsg.ErrorMessage:
		return wrap(ErrPeerError, "error message type %d", uint8(m.Type))
	case peermsg.DefinitionMessage:
		return c.define(ctx, m.ID, m.Definition, nil, sink)
	case peermsg.SwitchMessage:
		return nil
	case peermsg.UpdateMessage:
		return c.update(ctx, m, sink)
	case peermsg.AckMessage:
		return c.ack(m)
	default:
		return protocol("unexpected message %T", msg)
	}
}

func (c *Conn) decodeError(ctx context.Context, f peerwire.Frame, err error, sink Sink) error {
	switch {
	case errors.Is(err, peermsg.ErrSchema):
		// Inbound bound the rejected definition and selected it.
		id, _ := c.in.Selected()
		t, _ := c.in.Table(id)
		return c.define(ctx, id, t.Definition, err, sink)
	case errors.Is(err, peermsg.ErrTooManyTables):
		return wrapCause(ErrLimit, err, "table definition")
	case errors.Is(err, peermsg.ErrUnknownMessage):
		return protocolCause(err, "message class %d type %#02x", uint8(f.Class), uint8(f.Type))
	default:
		return protocolCause(err, "message class %d type %#02x", uint8(f.Class), uint8(f.Type))
	}
}

// define records a definition the source sent, accepted (rejected == nil)
// or rejected by peermsg. HAProxy re-sends a table's definition to switch
// back to it, so an identical redefinition changes nothing and is not
// reported again.
//
// An input table must match its configured spec, and the source's copy of
// an output table must match the definition this side announces for it
// (same key, expiry, and gpt length), so that a misconfigured output table
// is found instead of silently dropping slots; either mismatch ends the
// session with ErrSchema. Every other table is ignored.
func (c *Conn) define(ctx context.Context, id peermsg.RemoteTableID, def peermsg.Definition, rejected error,
	sink Sink,
) error {
	kind := kindIgnored
	if spec, ok := c.opts.Tables[def.Name]; ok {
		kind = kindInput
		var err error
		if rejected != nil {
			err = wrapCause(ErrSchema, rejected, "input table %s", def.Name)
		} else {
			err = checkSpec(def, spec)
		}
		if err != nil {
			// Best effort: the session ends with err whether or not
			// the sink takes the report.
			_ = sink(ctx, TableRejected{ID: id, Table: def.Name, Err: err, Received: c.rxTime})
			return err
		}
	} else if out := c.outNames[def.Name]; out != nil {
		kind = kindOutput
		if rejected != nil {
			return wrapCause(ErrSchema, rejected, "output table %s", def.Name)
		}
		if !sameDefinition(def, out.def) {
			return wrap(ErrSchema, "output table %s: source stores %v expiring after %v, this side publishes %v expiring after %v",
				def.Name, def.Fields, def.Expiry, out.def.Fields, out.def.Expiry)
		}
	}
	if old := c.tables[id]; old != nil && old.name == def.Name && old.kind == kind &&
		(kind == kindIgnored || sameDefinition(old.def, def)) {
		return nil
	}
	if err := c.unbind(ctx, id, def.Name); err != nil {
		return err
	}
	c.tables[id] = &tableState{name: def.Name, kind: kind, def: def}
	switch kind {
	case kindInput:
		if err := sink(ctx, TableDefined{ID: id, Definition: def, Received: c.rxTime}); err != nil {
			return wrapCause(ErrEventRejected, err, "definition of %s", def.Name)
		}
	case kindOutput:
		out := c.outNames[def.Name]
		out.sourceID.Store(uint32(id))
		if !out.ready {
			out.ready = true
			return c.teachTable(ctx, out)
		}
	case kindIgnored:
	}
	return nil
}

// unbind drops the bindings that a new definition of name under id
// replaces, as peermsg.Inbound does, acknowledging their accepted updates
// first so that none is left unacknowledged.
func (c *Conn) unbind(ctx context.Context, id peermsg.RemoteTableID, name string) error {
	for oldID, t := range c.tables {
		if oldID != id && t.name != name {
			continue
		}
		if t.dirty {
			if err := c.sendAck(ctx, oldID, t); err != nil {
				return err
			}
		}
		if t.kind == kindOutput {
			c.outNames[t.name].sourceID.CompareAndSwap(uint32(oldID), 0)
		}
		delete(c.tables, oldID)
	}
	return nil
}

func sameDefinition(a, b peermsg.Definition) bool {
	if a.Name != b.Name || a.KeyType != b.KeyType || a.Expiry != b.Expiry || len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i] != b.Fields[i] {
			return false
		}
	}
	return true
}

// checkSpec requires an input table to store exactly http_req_cnt and
// http_req_rate with the configured period.
func checkSpec(def peermsg.Definition, spec TableSpec) error {
	want := []peermsg.Field{
		{Type: peermsg.DataHTTPReqCnt},
		{Type: peermsg.DataHTTPReqRate, Period: spec.Period},
	}
	if len(def.Fields) != len(want) || def.Fields[0] != want[0] || def.Fields[1] != want[1] {
		return wrap(ErrSchema, "input table %s stores %v, want %v", def.Name, def.Fields, want)
	}
	return nil
}

func (c *Conn) update(ctx context.Context, m peermsg.UpdateMessage, sink Sink) error {
	t := c.tables[m.ID]
	if t == nil || t.kind == kindIgnored {
		// Inbound only decodes updates for a selected, accepted table,
		// and handle skips ignored tables, so this cannot happen.
		return protocol("update for unbound table %d", m.ID)
	}
	if t.kind == kindOutput {
		// The source's copy of what this side published, typically
		// replayed when it teaches: acknowledged so that its cursor
		// advances, but never an input contribution.
		t.pending, t.dirty = m.Update.ID, true
		c.echoed.Add(1)
		return nil
	}
	ev := EntryUpdated{ID: m.ID, Table: t.name, Expiry: t.def.Expiry, Update: m.Update}
	if err := sink(ctx, ev); err != nil {
		return wrapCause(ErrEventRejected, err, "update %d of %s", m.Update.ID, t.name)
	}
	t.pending, t.dirty = m.Update.ID, true
	c.updates.Add(1)
	return nil
}

func (c *Conn) control(ctx context.Context, typ peerwire.MessageType, sink Sink) error {
	switch typ {
	case peerwire.ControlHeartbeat:
		c.rxHeartbeats.Add(1)
		return nil
	case peerwire.ControlResyncRequest:
		// Teach the whole output (nothing without one), then finish.
		if c.opts.Output != nil {
			if err := c.teach(ctx); err != nil {
				return err
			}
		}
		if err := c.sendControl(ctx, peerwire.ControlResyncFinished); err != nil {
			return err
		}
		c.confirms++
		c.confirmsA.Store(c.confirms)
		return nil
	case peerwire.ControlResyncConfirm:
		if c.confirms == 0 {
			return protocol("resync confirm without a finished resync to confirm")
		}
		c.confirms--
		c.confirmsA.Store(c.confirms)
		return nil
	case peerwire.ControlResyncFinished, peerwire.ControlResyncPartial:
		if c.learn != LearnRequested {
			return protocol("resync reply (control %d) in learn state %v", uint8(typ), c.learn)
		}
		partial := typ == peerwire.ControlResyncPartial
		if partial {
			c.setLearn(LearnPartial)
		} else {
			c.setLearn(LearnFinished)
		}
		if err := c.sendControl(ctx, peerwire.ControlResyncConfirm); err != nil {
			return err
		}
		if err := sink(ctx, SyncFinished{Partial: partial, Received: c.rxTime}); err != nil {
			return wrapCause(ErrEventRejected, err, "resync reply")
		}
		if partial && c.opts.ResyncRetry > 0 {
			c.resyncAt = time.Now().Add(c.opts.ResyncRetry)
		}
		return nil
	default:
		return protocol("unknown control type %d", uint8(typ))
	}
}

func (c *Conn) setLearn(s LearnState) {
	c.learn = s
	c.learnA.Store(uint32(s))
}

// flushAcks acknowledges every table with updates accepted since its
// last acknowledgement.
func (c *Conn) flushAcks(ctx context.Context) error {
	for id, t := range c.tables {
		if !t.dirty {
			continue
		}
		if err := c.sendAck(ctx, id, t); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) sendAck(ctx context.Context, id peermsg.RemoteTableID, t *tableState) error {
	b, err := peermsg.AppendAck(c.wbuf[:0], id, t.pending)
	if err != nil {
		return fmt.Errorf("encode ack: %w", err)
	}
	c.wbuf = b
	if err := c.write(ctx, b, time.Now().Add(c.opts.IdleTimeout)); err != nil {
		return err
	}
	t.dirty = false
	c.acksSent.Add(1)
	return nil
}
