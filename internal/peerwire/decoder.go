package peerwire

import (
	"bytes"
	"errors"
	"io"
	"math"
)

// Limits bounds what a Decoder accepts and buffers. All sizes are in bytes.
type Limits struct {
	// MaxHandshake bounds the total size of all handshake lines,
	// terminators included. A hello is three lines and a status one.
	MaxHandshake int
	// MaxMessage bounds a message body: the bytes after the class, type,
	// and encoded length. HAProxy's own bound is its tune.bufsize.
	MaxMessage int
	// MaxBuffered bounds the unconsumed input the decoder holds, and so
	// its buffer capacity. It must hold the largest frame
	// (HeaderLen+MaxLengthLen+MaxMessage) and the whole handshake.
	MaxBuffered int
}

// Default limits. DefaultMaxMessage is stock HAProxy's default
// tune.bufsize: the pinned releases never send a larger message and refuse
// a larger declared body. A deployment that raises tune.bufsize must raise
// MaxMessage to match.
const (
	DefaultMaxHandshake = 4096
	DefaultMaxMessage   = 16384
	DefaultMaxBuffered  = 2 * (HeaderLen + MaxLengthLen + DefaultMaxMessage)
)

// DefaultLimits returns the default limits.
func DefaultLimits() Limits {
	return Limits{
		MaxHandshake: DefaultMaxHandshake,
		MaxMessage:   DefaultMaxMessage,
		MaxBuffered:  DefaultMaxBuffered,
	}
}

// Validate reports whether l can frame every line and message it admits.
// The error wraps ErrInvalidLimits.
func (l Limits) Validate() error {
	switch {
	case l.MaxHandshake < 1:
		return detailError(ErrInvalidLimits, "MaxHandshake %d must be positive", l.MaxHandshake)
	case l.MaxMessage < 0 || uint64(l.MaxMessage) > math.MaxUint32:
		return detailError(ErrInvalidLimits, "MaxMessage %d must be within 0..%d", l.MaxMessage, uint32(math.MaxUint32))
	case l.MaxBuffered < l.MaxHandshake:
		return detailError(ErrInvalidLimits, "MaxBuffered %d is below MaxHandshake %d", l.MaxBuffered, l.MaxHandshake)
	case l.MaxBuffered-HeaderLen-MaxLengthLen < l.MaxMessage:
		return detailError(ErrInvalidLimits, "MaxBuffered %d cannot hold a %d-byte body with its header",
			l.MaxBuffered, l.MaxMessage)
	}
	return nil
}

// Decoder incrementally frames one direction of a peers connection: first
// zero or more handshake lines (NextLine), then binary messages
// (NextFrame). It performs no I/O and is not safe for concurrent use.
//
// Buffer ownership: Feed copies its argument, so the caller may reuse it
// at once. Slices returned by NextLine and in Frames from NextFrame alias
// the decoder's buffer. They stay valid until the next Feed, which may
// compact the buffer; consuming further lines or frames does not disturb
// them. Callers that keep data across a Feed must copy it.
//
// Side effects: a successful NextLine or NextFrame consumes exactly the
// returned line or frame and nothing else. ErrNeedMore consumes nothing.
// The first NextFrame call permanently switches the decoder to binary
// framing; bytes already buffered after the last line are framed as
// binary. A framing failure is sticky: the failing call and every later
// call return the same *Error, and Feed refuses input. Input past the
// failure point is never examined.
//
// Flow control: after NextLine or NextFrame returns ErrNeedMore, Free is
// always positive, so a caller that alternates "read at most Free bytes,
// Feed, drain until ErrNeedMore" always makes progress within Limits.
type Decoder struct {
	lim       Limits
	maxBody   uint64 // lim.MaxMessage
	buf       []byte // buf[r:] is unconsumed input
	r         int
	off       uint64 // stream offset of buf[r]
	handshake int    // handshake bytes consumed so far
	binary    bool
	eof       bool
	err       error // sticky framing failure
}

// NewDecoder returns a decoder enforcing lim. It returns an error wrapping
// ErrInvalidLimits if lim fails Validate.
func NewDecoder(lim Limits) (*Decoder, error) {
	if err := lim.Validate(); err != nil {
		return nil, err
	}
	return &Decoder{lim: lim, maxBody: uint64(lim.MaxMessage)}, nil //nolint:gosec // G115: Validate bounds MaxMessage to 0..MaxUint32.
}

// Limits returns the limits the decoder enforces.
func (d *Decoder) Limits() Limits { return d.lim }

// Buffered returns the number of input bytes fed but not yet consumed.
func (d *Decoder) Buffered() int { return len(d.buf) - d.r }

// Free returns how many more bytes Feed accepts now.
func (d *Decoder) Free() int { return d.lim.MaxBuffered - d.Buffered() }

// Offset returns the stream offset of the next unconsumed byte, which is
// also the total number of bytes consumed.
func (d *Decoder) Offset() uint64 { return d.off }

// Feed appends a copy of p to the buffered input. It accepts all of p or
// none of it: if p does not fit in Free bytes it returns an *Error wrapping
// ErrInputLimit and buffers nothing, leaving the decoder usable. After a
// framing failure it returns that failure, and after CloseInput it returns
// ErrInputClosed.
func (d *Decoder) Feed(p []byte) error {
	if d.err != nil {
		return d.err
	}
	if d.eof {
		return ErrInputClosed
	}
	if len(p) > d.Free() {
		return newError(ErrInputLimit, d.off, "%d bytes offered, %d buffered, limit %d",
			len(p), d.Buffered(), d.lim.MaxBuffered)
	}
	if len(p) == 0 {
		return nil
	}
	if d.r > 0 {
		n := copy(d.buf, d.buf[d.r:])
		d.buf = d.buf[:n]
		d.r = 0
	}
	if need := len(d.buf) + len(p); need > cap(d.buf) {
		grown := make([]byte, len(d.buf), min(max(need, 2*cap(d.buf), 512), d.lim.MaxBuffered))
		copy(grown, d.buf)
		d.buf = grown
	}
	d.buf = append(d.buf, p...)
	return nil
}

// CloseInput records that no more input will arrive. From then on an
// incomplete line or frame is ErrTruncated instead of ErrNeedMore, and a
// read at a clean boundary with nothing buffered returns io.EOF.
func (d *Decoder) CloseInput() { d.eof = true }

// NextLine returns the next handshake line without its "\n" terminator or
// a "\r" immediately before it. The content is otherwise unchecked; parse
// it with the Parse*Line functions.
//
// It returns ErrNeedMore if no complete line is buffered, io.EOF at a clean
// end of input, ErrHandshakeDone once NextFrame has been called, and an
// *Error wrapping ErrHandshakeTooLarge or ErrTruncated on failure.
func (d *Decoder) NextLine() ([]byte, error) {
	if d.err != nil {
		return nil, d.err
	}
	if d.binary {
		return nil, ErrHandshakeDone
	}
	data := d.buf[d.r:]
	budget := d.lim.MaxHandshake - d.handshake
	i := bytes.IndexByte(data[:min(len(data), budget)], '\n')
	if i < 0 {
		// An exhausted budget with nothing buffered is not yet an
		// overlong line: the handshake may simply be over.
		if len(data) > 0 && len(data) >= budget {
			return nil, d.fail(newError(ErrHandshakeTooLarge, d.off,
				"no line end within the remaining %d of %d handshake bytes", budget, d.lim.MaxHandshake))
		}
		return nil, d.incomplete(len(data), "handshake line")
	}
	n := i + 1
	line := data[:i:i]
	if i > 0 && line[i-1] == '\r' {
		line = line[: i-1 : i-1]
	}
	d.consume(n)
	d.handshake += n
	return line, nil
}

// NextFrame returns the next binary message. The first call switches the
// decoder to binary framing for good.
//
// It returns ErrNeedMore if no complete frame is buffered and io.EOF at a
// clean end of input. On failure it returns an *Error wrapping
// ErrTruncated, ErrReservedClass, ErrLengthEncoding, or ErrMessageTooLarge;
// size failures are detected from the header alone, before the body is
// buffered.
func (d *Decoder) NextFrame() (Frame, error) {
	if d.err != nil {
		return Frame{}, d.err
	}
	d.binary = true
	data := d.buf[d.r:]
	if len(data) < HeaderLen {
		return Frame{}, d.incomplete(len(data), "message header")
	}
	class, typ := MessageClass(data[0]), MessageType(data[1])
	if class == ClassReserved {
		return Frame{}, d.fail(newError(ErrReservedClass, d.off, "type %#02x", uint8(typ)))
	}
	if !typ.Variable() {
		f := Frame{Class: class, Type: typ, Raw: data[:HeaderLen:HeaderLen], Offset: d.off}
		d.consume(HeaderLen)
		return f, nil
	}

	lenBytes := data[HeaderLen:min(len(data), HeaderLen+MaxLengthLen)]
	bodyLen, lenLen, err := DecodeUint(lenBytes)
	switch {
	case errors.Is(err, ErrIntTruncated) && len(lenBytes) < MaxLengthLen:
		return Frame{}, d.incomplete(len(data), "message length")
	case err != nil:
		return Frame{}, d.fail(newError(ErrLengthEncoding, d.off,
			"class %d type %#02x: length not terminated within %d bytes", class, uint8(typ), MaxLengthLen))
	case bodyLen > d.maxBody:
		return Frame{}, d.fail(newError(ErrMessageTooLarge, d.off,
			"class %d type %#02x: body of %d bytes exceeds limit %d", class, uint8(typ), bodyLen, d.lim.MaxMessage))
	}

	start := HeaderLen + lenLen
	end := start + int(bodyLen) //nolint:gosec // G115: bodyLen <= MaxMessage, an int.
	if len(data) < end {
		return Frame{}, d.incomplete(len(data), "message body")
	}
	f := Frame{
		Class:  class,
		Type:   typ,
		Body:   data[start:end:end],
		Raw:    data[:end:end],
		Offset: d.off,
	}
	d.consume(end)
	return f, nil
}

func (d *Decoder) consume(n int) {
	d.r += n
	d.off += uint64(n) //nolint:gosec // G115: n is a non-negative length within the buffer.
}

// incomplete reports a partial line or frame of have bytes: ErrNeedMore
// while input may still arrive, otherwise io.EOF at a clean boundary or a
// sticky ErrTruncated.
func (d *Decoder) incomplete(have int, what string) error {
	if !d.eof {
		return ErrNeedMore
	}
	if have == 0 {
		return io.EOF
	}
	return d.fail(newError(ErrTruncated, d.off, "input ended after %d bytes of a %s", have, what))
}

func (d *Decoder) fail(err *Error) error {
	d.err = err
	return err
}
