package peermsg

import (
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// Message is a decoded peers message: one of Control, ErrorMessage,
// DefinitionMessage, SwitchMessage, UpdateMessage, or AckMessage.
type Message interface{ message() }

// Control is a control-class message (resync request, finished, partial,
// confirm, or heartbeat).
type Control struct{ Type peerwire.MessageType }

// ErrorMessage is an error-class message. HAProxy sends one and closes the
// session.
type ErrorMessage struct{ Type peerwire.MessageType }

// DefinitionMessage reports an accepted table definition, which also
// selects the table for the updates that follow.
type DefinitionMessage struct {
	ID         RemoteTableID
	Definition Definition
}

// SwitchMessage reports that a switch message selected a defined table.
type SwitchMessage struct {
	ID    RemoteTableID
	Table string
}

// UpdateMessage is a decoded entry update for the selected table.
type UpdateMessage struct {
	ID     RemoteTableID
	Table  string
	Update Update
}

// AckMessage is the peer's acknowledgement of updates this side sent for
// its local table ID.
type AckMessage struct {
	ID     LocalTableID
	Update UpdateID
}

func (Control) message()           {}
func (ErrorMessage) message()      {}
func (DefinitionMessage) message() {}
func (SwitchMessage) message()     {}
func (UpdateMessage) message()     {}
func (AckMessage) message()        {}

// RemoteTable describes a table ID bound in an Inbound namespace.
type RemoteTable struct {
	// ID is the peer's table ID.
	ID RemoteTableID
	// Definition is the accepted definition. For a rejected table only
	// Name is set.
	Definition Definition
	// Rejected is the ErrSchema error that rejected the definition, or
	// nil if it was accepted.
	Rejected error
	// LastUpdate is the ID of the last update decoded for this binding,
	// valid when HaveUpdate is set.
	LastUpdate UpdateID
	// HaveUpdate records whether any update was decoded since the
	// definition.
	HaveUpdate bool
}

// Inbound holds the table namespace of one peer session's inbound
// direction: which table each RemoteTableID names, which table is
// selected, and the last update ID per table. It assigns meaning to the
// peer's numeric IDs only within this session; use one Inbound per
// session, so that sources announcing the same ID for different tables
// never interfere. It is not safe for concurrent use.
//
// Binding of accepted definitions follows stock HAProxy (src/peers.c,
// peer_treat_definemsg): a definition binds its ID to its table name,
// first unbinding whatever the ID and the name were bound to, and selects
// the table. Rejected definitions deviate deliberately, in two ways:
//
//   - HAProxy ignores such a definition and selects nothing, so the
//     updates that follow are dropped silently; Inbound selects the
//     rejected table, so they fail with ErrRejectedTable instead.
//   - HAProxy leaves the named table bound to its old ID; Inbound unbinds
//     the name and binds it, as rejected, under the new ID, so no update
//     is attributed to the old definition after its peer redefined it.
//
// Stock senders trigger neither: they define each table once per session
// with one schema. A definition starts a fresh binding with no previous
// update ID; HAProxy always sends an explicit ID in the first update after
// one.
type Inbound struct {
	maxTables int
	byID      map[RemoteTableID]*RemoteTable
	byName    map[string]RemoteTableID
	selected  RemoteTableID
}

// MaxTables is the largest table limit a namespace accepts; larger limits
// are reduced to it.
const MaxTables = 1 << 16

func clampTables(n int) int { return min(max(n, 1), MaxTables) }

// NewInbound returns an empty namespace that binds at most maxTables IDs
// at once (clamped to 1..MaxTables).
func NewInbound(maxTables int) *Inbound {
	return &Inbound{
		maxTables: clampTables(maxTables),
		byID:      map[RemoteTableID]*RemoteTable{},
		byName:    map[string]RemoteTableID{},
	}
}

// Table returns the binding of id, if any. The Definition shares nothing
// with the namespace.
func (in *Inbound) Table(id RemoteTableID) (RemoteTable, bool) {
	t, ok := in.byID[id]
	if !ok {
		return RemoteTable{}, false
	}
	out := *t
	out.Definition.Fields = slices.Clone(t.Definition.Fields)
	return out, true
}

// Selected returns the currently selected table ID, if any.
func (in *Inbound) Selected() (RemoteTableID, bool) { return in.selected, in.selected != 0 }

// Decode interprets one frame received from the peer at time received
// and applies it to the namespace.
//
// It returns ErrUnknownMessage for a class or type it does not interpret
// (stock HAProxy skips those). Definitions outside the supported subset
// return their ErrSchema error after binding the table as rejected.
// Malformed messages return an ErrMalformed error and clear the selection.
// A switch to an unbound ID (ErrUnknownTable) and a definition beyond the
// table limit (ErrTooManyTables, class ErrState even when the definition
// was also rejected) also clear the selection and bind nothing; other
// ErrState errors change nothing. Only a nil error comes with a non-nil
// Message.
func (in *Inbound) Decode(f peerwire.Frame, received time.Time) (Message, error) {
	switch f.Class {
	case peerwire.ClassControl:
		if f.Body != nil || !knownControl(f.Type) {
			return nil, newErr(ErrUnknownMessage, nil, nil, "control type %d", uint8(f.Type))
		}
		return Control{Type: f.Type}, nil
	case peerwire.ClassError:
		if f.Body != nil || !knownError(f.Type) {
			return nil, newErr(ErrUnknownMessage, nil, nil, "error type %d", uint8(f.Type))
		}
		return ErrorMessage{Type: f.Type}, nil
	case peerwire.ClassStickTable:
		return in.decodeTable(f, received)
	case peerwire.ClassReserved:
	}
	return nil, newErr(ErrUnknownMessage, nil, nil, "class %d type %#02x", uint8(f.Class), uint8(f.Type))
}

func (in *Inbound) decodeTable(f peerwire.Frame, received time.Time) (Message, error) {
	switch {
	case f.Type == peerwire.StickTableDefine:
		return in.define(f.Body)
	case f.Type == peerwire.StickTableSwitch:
		id, err := DecodeSwitch(f.Body)
		if err != nil {
			in.selected = 0
			return nil, err
		}
		t, ok := in.byID[id]
		if !ok {
			in.selected = 0
			return nil, stateErr(ErrUnknownTable, "switch to remote table %d", id)
		}
		in.selected = id
		return SwitchMessage{ID: id, Table: t.Definition.Name}, nil
	case IsUpdateType(f.Type):
		return in.update(f, received)
	case f.Type == peerwire.StickTableAck:
		id, upd, err := DecodeAck(f.Body)
		if err != nil {
			return nil, err
		}
		return AckMessage{ID: id, Update: upd}, nil
	default:
		return nil, newErr(ErrUnknownMessage, nil, nil, "stick-table type %#02x", uint8(f.Type))
	}
}

func (in *Inbound) define(body []byte) (Message, error) {
	id, def, err := DecodeDefinition(body)
	if err != nil && !errors.Is(err, ErrSchema) {
		in.selected = 0
		return nil, err
	}
	if old, ok := in.byID[id]; ok {
		delete(in.byName, old.Definition.Name)
		delete(in.byID, id)
	}
	if oldID, ok := in.byName[def.Name]; ok {
		delete(in.byID, oldID)
		delete(in.byName, def.Name)
	}
	in.selected = 0
	if len(in.byID) >= in.maxTables {
		// Nothing is bound, so the class stays ErrState; a schema
		// rejection contributes only its specific reason and its text.
		rejected := ""
		if err != nil {
			rejected = "; definition also rejected: " + strings.TrimPrefix(err.Error(), ErrSchema.Error()+": ")
		}
		return nil, newErr(ErrState, ErrTooManyTables, schemaReason(err),
			"binding remote table %d (%s) exceeds %d tables%s", id, def.Name, in.maxTables, rejected)
	}
	t := &RemoteTable{ID: id, Rejected: err}
	if err != nil {
		t.Definition = Definition{Name: def.Name}
	} else {
		t.Definition = def
	}
	in.byID[id] = t
	in.byName[def.Name] = id
	in.selected = id
	if err != nil {
		return nil, err
	}
	return DefinitionMessage{ID: id, Definition: Definition{
		Name: def.Name, KeyType: def.KeyType, Expiry: def.Expiry, Fields: slices.Clone(def.Fields),
	}}, nil
}

func (in *Inbound) update(f peerwire.Frame, received time.Time) (Message, error) {
	if in.selected == 0 {
		return nil, stateErr(ErrNoTable, "update type %#02x", uint8(f.Type))
	}
	t := in.byID[in.selected]
	if t.Rejected != nil {
		return nil, stateErr(ErrRejectedTable, "update for remote table %d (%s)", t.ID, t.Definition.Name)
	}
	u, err := DecodeUpdate(f.Type, f.Body, t.Definition, t.LastUpdate, t.HaveUpdate, received)
	if err != nil {
		if errors.Is(err, ErrMalformed) {
			in.selected = 0
		}
		return nil, err
	}
	t.LastUpdate, t.HaveUpdate = u.ID, true
	return UpdateMessage{ID: t.ID, Table: t.Definition.Name, Update: u}, nil
}

// LocalTables is the namespace of table IDs this side announces in one
// session. IDs are assigned from 1 in registration order, as HAProxy
// assigns its own. It is not safe for concurrent use.
type LocalTables struct {
	maxTables int
	defs      []Definition // defs[i] has ID i+1
	byName    map[string]LocalTableID
}

// NewLocalTables returns an empty namespace for at most maxTables tables
// (clamped to 1..MaxTables).
func NewLocalTables(maxTables int) *LocalTables {
	return &LocalTables{maxTables: clampTables(maxTables), byName: map[string]LocalTableID{}}
}

// Register validates def and assigns it the next local ID. It fails with
// def's ErrSchema error, ErrDuplicateTable for a name already registered,
// or ErrTooManyTables.
func (l *LocalTables) Register(def Definition) (LocalTableID, error) {
	if err := def.Validate(); err != nil {
		return 0, err
	}
	if _, ok := l.byName[def.Name]; ok {
		return 0, stateErr(ErrDuplicateTable, "local table %s", def.Name)
	}
	if len(l.defs) >= l.maxTables {
		return 0, stateErr(ErrTooManyTables, "local table %s exceeds %d tables", def.Name, l.maxTables)
	}
	def.Fields = slices.Clone(def.Fields)
	l.defs = append(l.defs, def)
	id := LocalTableID(len(l.defs)) //nolint:gosec // G115: len(defs) <= maxTables <= MaxTables.
	l.byName[def.Name] = id
	return id, nil
}

// Definition returns the definition registered under id. It fails with
// ErrUnknownTable for an ID this namespace never assigned, such as one in
// a peer's acknowledgement.
func (l *LocalTables) Definition(id LocalTableID) (Definition, error) {
	if id == 0 || uint64(id) > uint64(len(l.defs)) {
		return Definition{}, stateErr(ErrUnknownTable, "local table %d", id)
	}
	d := l.defs[id-1]
	d.Fields = slices.Clone(d.Fields)
	return d, nil
}

// ID returns the local ID registered for a table name.
func (l *LocalTables) ID(name string) (LocalTableID, bool) {
	id, ok := l.byName[name]
	return id, ok
}
