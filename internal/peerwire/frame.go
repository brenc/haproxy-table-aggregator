package peerwire

import (
	"fmt"
	"math"
)

// MessageClass is the first byte of every binary message.
type MessageClass uint8

// Message classes. Only ClassReserved is rejected by framing; other
// unknown classes are framed and left to the caller, as HAProxy ignores
// them.
const (
	ClassControl    MessageClass = 0
	ClassError      MessageClass = 1
	ClassStickTable MessageClass = 10
	ClassReserved   MessageClass = 255
)

// MessageType is the second byte of every binary message. Its meaning
// depends on the class; bit 7 alone decides the framing (see Variable).
type MessageType uint8

// Control class (ClassControl) message types. All are fixed-size.
const (
	ControlResyncRequest  MessageType = 0
	ControlResyncFinished MessageType = 1
	ControlResyncPartial  MessageType = 2
	ControlResyncConfirm  MessageType = 3
	ControlHeartbeat      MessageType = 4
)

// Error class (ClassError) message types. All are fixed-size. HAProxy
// sends one and then closes the session.
const (
	ErrorTypeProtocol  MessageType = 0
	ErrorTypeSizeLimit MessageType = 1
)

// Stick-table class (ClassStickTable) message types, as observed from the
// pinned releases. All are variable-length. doc/peers.txt numbers the
// acknowledgement 133 and omits the timed updates; stock HAProxy 3.2.25
// and 3.4.6 send acknowledgements as 0x84.
const (
	StickTableUpdate                 MessageType = 0x80
	StickTableIncrementalUpdate      MessageType = 0x81
	StickTableDefine                 MessageType = 0x82
	StickTableSwitch                 MessageType = 0x83
	StickTableAck                    MessageType = 0x84
	StickTableTimedUpdate            MessageType = 0x85
	StickTableIncrementalTimedUpdate MessageType = 0x86
)

// variableTypeBit marks a variable-length message type.
const variableTypeBit = 0x80

// Variable reports whether messages of this type carry an encoded length
// and a body. HAProxy decides this from bit 7 of the type byte alone,
// regardless of class.
func (t MessageType) Variable() bool { return t&variableTypeBit != 0 }

// HeaderLen is the size of the class and type bytes that start every
// message.
const HeaderLen = 2

// MaxLengthLen is the longest message-length encoding HAProxy accepts.
const MaxLengthLen = 5

// Frame is one binary message.
//
// Body and Raw alias the Decoder's buffer when the frame comes from
// NextFrame; see Decoder for how long they stay valid. Both are capped
// (len == cap) so appending to them never overwrites buffered input.
type Frame struct {
	// Class is the message class byte.
	Class MessageClass
	// Type is the message type byte.
	Type MessageType
	// Body is the bytes after the encoded length; nil for a fixed-size
	// type, and empty but non-nil for a variable-length type with a zero
	// length.
	Body []byte
	// Raw is the complete message as it appeared on the wire: header,
	// encoded length, and body.
	Raw []byte
	// Offset is the stream offset of the first header byte.
	Offset uint64
}

// String describes the frame without its body content.
func (f Frame) String() string {
	if !f.Type.Variable() {
		return fmt.Sprintf("frame{class=%d type=%#02x offset=%d}", f.Class, uint8(f.Type), f.Offset)
	}
	return fmt.Sprintf("frame{class=%d type=%#02x body=%d offset=%d}", f.Class, uint8(f.Type), len(f.Body), f.Offset)
}

// AppendFrame appends one encoded message to dst and returns the extended
// slice.
//
// A fixed-size type must have an empty body (ErrFixedBody). The reserved
// class is refused (ErrReservedClass). A body longer than math.MaxUint32 is
// refused (ErrMessageTooLarge) because HAProxy reads lengths as 32-bit
// values. AppendFrame does not know the receiver's buffer size: stock
// HAProxy processes a message only if all of it, header and length
// included, fits in its tune.bufsize, so senders must check the encoded
// size. On error dst is returned unchanged.
func AppendFrame(dst []byte, class MessageClass, typ MessageType, body []byte) ([]byte, error) {
	if class == ClassReserved {
		return dst, ErrReservedClass
	}
	if !typ.Variable() {
		if len(body) != 0 {
			return dst, ErrFixedBody
		}
		return append(dst, byte(class), byte(typ)), nil
	}
	if uint64(len(body)) > math.MaxUint32 {
		return dst, ErrMessageTooLarge
	}
	dst = append(dst, byte(class), byte(typ))
	dst = AppendUint(dst, uint64(len(body)))
	return append(dst, body...), nil
}
