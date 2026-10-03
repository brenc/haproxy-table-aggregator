package peerwire_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// event is one decoded line or frame, copied out of the decoder's buffer.
type event struct {
	line   bool
	start  uint64 // stream offset of the first byte
	end    uint64 // stream offset just past the last byte
	data   []byte // line content, or frame Raw
	class  peerwire.MessageClass
	typ    peerwire.MessageType
	body   []byte
	isBody bool // body != nil
}

func (e event) String() string {
	if e.line {
		return fmt.Sprintf("line[%d:%d]%q", e.start, e.end, e.data)
	}
	return fmt.Sprintf("frame[%d:%d]%d/%#02x/%x", e.start, e.end, e.class, uint8(e.typ), e.data)
}

// outcome is everything a decoder run produced.
type outcome struct {
	events []event
	// err is io.EOF for a clean end, or the terminal framing failure.
	err error
}

// sentinel returns the sentinel inside a terminal error.
func (o outcome) sentinel() error {
	var pe *peerwire.Error
	if errors.As(o.err, &pe) {
		return pe.Err
	}
	return o.err
}

func (o outcome) equal(p outcome) bool {
	if !slices.EqualFunc(o.events, p.events, func(a, b event) bool {
		return a.line == b.line && a.start == b.start && a.end == b.end && bytes.Equal(a.data, b.data) &&
			a.class == b.class && a.typ == b.typ && bytes.Equal(a.body, b.body) && a.isBody == b.isBody
	}) {
		return false
	}
	if !errors.Is(o.sentinel(), p.sentinel()) {
		return false
	}
	var oe, pe *peerwire.Error
	if errors.As(o.err, &oe) != errors.As(p.err, &pe) {
		return false
	}
	return oe == nil || oe.Offset == pe.Offset
}

// decodeChunks feeds chunks to a new decoder the way a network reader
// would: never more than Free bytes at once, draining lines (the first
// lines items) and then frames until ErrNeedMore after every Feed, and
// closing input at the end. It returns an error if the decoder breaks one
// of its documented invariants.
func decodeChunks(lim peerwire.Limits, chunks [][]byte, lines int) (outcome, error) {
	d, err := peerwire.NewDecoder(lim)
	if err != nil {
		return outcome{}, err
	}
	var out outcome
	drain := func() (bool, error) {
		for {
			before := d.Offset()
			var ev event
			if len(out.events) < lines {
				line, lerr := d.NextLine()
				if lerr != nil {
					return finish(d, &out, lerr, before)
				}
				ev = event{line: true, data: bytes.Clone(line)}
			} else {
				f, ferr := d.NextFrame()
				if ferr != nil {
					return finish(d, &out, ferr, before)
				}
				if f.Offset != before || uint64(len(f.Raw)) != d.Offset()-before {
					return true, fmt.Errorf("frame %v does not match consumed range [%d:%d]", f, before, d.Offset())
				}
				if f.Body != nil && (cap(f.Body) != len(f.Body) || !bytes.HasSuffix(f.Raw, f.Body)) {
					return true, fmt.Errorf("frame %v body is not a capped suffix of raw", f)
				}
				ev = event{
					data: bytes.Clone(f.Raw), class: f.Class, typ: f.Type,
					body: bytes.Clone(f.Body), isBody: f.Body != nil,
				}
			}
			ev.start, ev.end = before, d.Offset()
			out.events = append(out.events, ev)
		}
	}
	for _, c := range chunks {
		for len(c) > 0 {
			n := min(len(c), d.Free())
			if n == 0 {
				return out, fmt.Errorf("decoder stalled: Free is 0 with %d bytes buffered", d.Buffered())
			}
			if ferr := d.Feed(c[:n]); ferr != nil {
				return out, fmt.Errorf("feed: %w", ferr)
			}
			c = c[n:]
			if bc := peerwire.BufferCap(d); bc > lim.MaxBuffered {
				return out, fmt.Errorf("buffer capacity %d exceeds MaxBuffered %d", bc, lim.MaxBuffered)
			}
			done, derr := drain()
			if derr != nil || done {
				return out, derr
			}
		}
	}
	d.CloseInput()
	_, err = drain()
	if err == nil && out.err == nil {
		err = errors.New("drain after CloseInput returned without a terminal result")
	}
	return out, err
}

// finish handles a non-nil result from NextLine/NextFrame. It reports
// whether decoding is over and checks the error-path invariants.
func finish(d *peerwire.Decoder, out *outcome, err error, before uint64) (bool, error) {
	if d.Offset() != before {
		return true, fmt.Errorf("failed call consumed input: offset %d -> %d (%w)", before, d.Offset(), err)
	}
	if errors.Is(err, peerwire.ErrNeedMore) {
		if d.Free() <= 0 {
			return true, fmt.Errorf("ErrNeedMore with Free %d", d.Free())
		}
		return false, nil
	}
	out.err = err
	if errors.Is(err, io.EOF) {
		if d.Buffered() != 0 {
			return true, fmt.Errorf("io.EOF with %d bytes buffered", d.Buffered())
		}
		return true, nil
	}
	var pe *peerwire.Error
	if !errors.As(err, &pe) {
		return true, fmt.Errorf("terminal error %w is not *peerwire.Error", err)
	}
	if pe.Offset != before {
		return true, fmt.Errorf("error offset %d, want %d", pe.Offset, before)
	}
	// Failures are sticky.
	if _, again := d.NextFrame(); !errors.Is(again, err) {
		return true, fmt.Errorf("error not sticky: %w then %w", err, again)
	}
	return true, nil
}

// checkPreserved verifies that the events tile input from offset 0 with
// no gaps: each frame's Raw is the input at its offset and each line is
// the input minus its "\n" or "\r\n".
func checkPreserved(input []byte, o outcome) error {
	var next uint64
	for _, e := range o.events {
		if e.start != next || e.end > uint64(len(input)) {
			return fmt.Errorf("%v does not start at %d", e, next)
		}
		got := input[e.start:e.end]
		want := e.data
		if e.line {
			if !bytes.Equal(got, append(bytes.Clone(e.data), '\n')) &&
				!bytes.Equal(got, append(bytes.Clone(e.data), '\r', '\n')) {
				return fmt.Errorf("%v: input is %q", e, got)
			}
		} else if !bytes.Equal(got, want) {
			return fmt.Errorf("%v: input is %x", e, got)
		}
		next = e.end
	}
	if errors.Is(o.err, io.EOF) && next != uint64(len(input)) {
		return fmt.Errorf("clean end at %d of %d bytes", next, len(input))
	}
	return nil
}

// splits returns the chunkings of input exercised by every stream test:
// whole, one byte at a time, and two chunks split at every offset.
func splits(input []byte) [][][]byte {
	out := [][][]byte{{input}}
	bytewise := make([][]byte, len(input))
	for i := range input {
		bytewise[i] = input[i : i+1]
	}
	out = append(out, bytewise)
	for k := 1; k < len(input); k++ {
		out = append(out, [][]byte{input[:k], input[k:]})
	}
	return out
}
