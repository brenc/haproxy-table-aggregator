package peermsg

import (
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// DecodeSwitch decodes the body of a table switch message
// (peerwire.StickTableSwitch): one encoded table ID, which selects a table
// the peer defined earlier. Stock HAProxy accepts switch messages but
// never sends one; it re-sends a definition to change tables.
func DecodeSwitch(body []byte) (RemoteTableID, error) {
	c := peerwire.NewCursor(body)
	id, err := c.Uint32()
	if err != nil {
		return 0, malformed(err, "switch table ID")
	}
	if id == 0 {
		return 0, newErr(ErrMalformed, ErrTableID, nil, "switch to table ID 0")
	}
	if c.Remaining() != 0 {
		return 0, newErr(ErrMalformed, ErrTrailingData, nil, "%d bytes after switch table ID", c.Remaining())
	}
	return RemoteTableID(id), nil
}

// AppendSwitch appends a table switch message selecting the local table
// id, previously announced with AppendDefinition.
func AppendSwitch(dst []byte, id LocalTableID) ([]byte, error) {
	if id == 0 {
		return dst, newErr(ErrMalformed, ErrTableID, nil, "switch to local table ID 0")
	}
	return appendTableFrame(dst, peerwire.StickTableSwitch, peerwire.AppendUint(nil, uint64(id)))
}

// DecodeAck decodes the body of an update acknowledgement
// (peerwire.StickTableAck): an encoded table ID and a 4-byte big-endian
// update ID. The table ID is one this side announced, so it is a
// LocalTableID; the update ID is the last update the peer received for
// that table.
func DecodeAck(body []byte) (LocalTableID, UpdateID, error) {
	c := peerwire.NewCursor(body)
	id, err := c.Uint32()
	if err != nil {
		return 0, 0, malformed(err, "ack table ID")
	}
	if id == 0 {
		return 0, 0, newErr(ErrMalformed, ErrTableID, nil, "ack for table ID 0")
	}
	upd, err := c.Fixed32()
	if err != nil {
		return 0, 0, malformed(err, "ack update ID")
	}
	if c.Remaining() != 0 {
		return 0, 0, newErr(ErrMalformed, ErrTrailingData, nil, "%d bytes after ack", c.Remaining())
	}
	return LocalTableID(id), UpdateID(upd), nil
}

// AppendAck appends an acknowledgement of update upd of the peer's table
// id.
func AppendAck(dst []byte, id RemoteTableID, upd UpdateID) ([]byte, error) {
	if id == 0 {
		return dst, newErr(ErrMalformed, ErrTableID, nil, "ack for remote table ID 0")
	}
	body := peerwire.AppendUint(nil, uint64(id))
	return appendTableFrame(dst, peerwire.StickTableAck, appendFixed32(body, uint32(upd)))
}

// Known control and error message types. Both classes are fixed-size and
// carry no body; encode them with peerwire.AppendFrame.
func knownControl(t peerwire.MessageType) bool {
	switch t {
	case peerwire.ControlResyncRequest, peerwire.ControlResyncFinished, peerwire.ControlResyncPartial,
		peerwire.ControlResyncConfirm, peerwire.ControlHeartbeat:
		return true
	default:
		return false
	}
}

func knownError(t peerwire.MessageType) bool {
	return t == peerwire.ErrorTypeProtocol || t == peerwire.ErrorTypeSizeLimit
}
