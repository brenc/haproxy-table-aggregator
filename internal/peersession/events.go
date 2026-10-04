package peersession

import (
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
)

// Event is a validated session event: one of TableDefined,
// TableRejected, EntryUpdated, or SyncFinished, delivered by Run, or
// SessionUp or SessionDown, which package sources emits around Run.
type Event interface{ event() }

// Direction says which side opened the connection.
type Direction uint8

// Connection directions.
const (
	// Inbound: the source dialed this daemon.
	Inbound Direction = iota + 1
	// Outbound: this daemon dialed the source.
	Outbound
)

// String returns "inbound" or "outbound".
func (d Direction) String() string {
	switch d {
	case Inbound:
		return "inbound"
	case Outbound:
		return "outbound"
	default:
		return "unknown"
	}
}

// SessionUp reports a session that completed its handshake and now
// represents its source.
type SessionUp struct {
	// Direction is who dialed.
	Direction Direction
	// RemotePID and RemoteRelativePID are what the source's hello
	// announced. Only an inbound session receives a hello, so both are
	// zero for an outbound one.
	RemotePID         uint32
	RemoteRelativePID uint32
	// RemoteAddr is the connection's remote address.
	RemoteAddr string
	// At is when the handshake completed.
	At time.Time
}

// SessionDown reports the end of a session that was up. No event of that
// session follows it.
type SessionDown struct {
	// Err is why the session ended; it is never nil.
	Err error
	// At is when the session ended.
	At time.Time
}

// TableDefined reports an accepted definition of an input table. Updates
// for the table follow until the source redefines its ID or the session
// ends.
type TableDefined struct {
	// ID is the source's table ID, meaningful only within this session.
	ID peermsg.RemoteTableID
	// Definition is the announced schema, which matched the configured
	// input table.
	Definition peermsg.Definition
	// Received is when the definition was read.
	Received time.Time
}

// TableRejected reports an input table whose announced definition is
// outside the supported subset or differs from its configured schema. The
// session ends with ErrSchema right after it, so no update of the table
// is ever delivered or acknowledged; the event lets the application
// record which logical table failed and why.
type TableRejected struct {
	// ID is the source's table ID, meaningful only within this session.
	ID peermsg.RemoteTableID
	// Table is the table name.
	Table string
	// Err is the schema error the session ends with; it wraps ErrSchema.
	Err error
	// Received is when the definition was read.
	Received time.Time
}

// EntryUpdated is one decoded update of an input table.
type EntryUpdated struct {
	// ID is the source's table ID, meaningful only within this session.
	ID peermsg.RemoteTableID
	// Table is the table name.
	Table string
	// Expiry is the table's announced entry lifetime, for
	// Update.Lifetime.
	Expiry peermsg.Millis
	// Update is the decoded update, with its reception time.
	Update peermsg.Update
}

// SyncFinished reports the source's reply to one of this session's
// resync requests: Partial is false for "finished" and true for "partial"
// (the source is not itself fully synchronized yet). Every update the
// source taught in answer precedes it. After a "partial" reply the
// session requests another resync once Options.ResyncRetry has passed, so
// a session may report several partial replies before a finished one; it
// requests nothing more after "finished".
type SyncFinished struct {
	Partial  bool
	Received time.Time
}

func (SessionUp) event()     {}
func (SessionDown) event()   {}
func (TableDefined) event()  {}
func (TableRejected) event() {}
func (EntryUpdated) event()  {}
func (SyncFinished) event()  {}
