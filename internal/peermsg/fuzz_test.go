package peermsg_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire/peertest"
)

// FuzzInbound frames fuzzed binary peers traffic and decodes every frame
// through one Inbound. Decoding must not panic, every failure must carry
// a class sentinel, and every accepted definition, update, switch, and
// ack must re-encode to exactly the bytes received: the integer encoding
// is bijective and trailing bytes are rejected, so a lenient parse would
// show up as a mismatch.
func FuzzInbound(f *testing.F) {
	for _, s := range seedStreams(f) {
		f.Add(s)
	}
	for _, body := range []string{bodyDefIn, bodyDefOut} {
		f.Add(frame(f, peerwire.StickTableDefine, body).Raw)
	}
	f.Add(append(frame(f, peerwire.StickTableDefine, bodyDefOut).Raw,
		frame(f, peerwire.StickTableIncrementalUpdate, keyHex+" 01 02 03").Raw...))
	f.Fuzz(func(t *testing.T, stream []byte) {
		lim := peerwire.Limits{MaxHandshake: 64, MaxMessage: 4096, MaxBuffered: 2 * (4096 + 7)}
		d, err := peerwire.NewDecoder(lim)
		if err != nil {
			t.Fatal(err)
		}
		if len(stream) > d.Free() {
			stream = stream[:d.Free()]
		}
		if err := d.Feed(stream); err != nil {
			t.Fatal(err)
		}
		d.CloseInput()
		in := peermsg.NewInbound(4)
		defs := map[peermsg.RemoteTableID]peermsg.Definition{}
		for {
			fr, err := d.NextFrame()
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, peerwire.ErrTruncated) &&
					!errors.Is(err, peerwire.ErrReservedClass) && !errors.Is(err, peerwire.ErrLengthEncoding) &&
					!errors.Is(err, peerwire.ErrMessageTooLarge) {
					t.Fatalf("framing: %v", err)
				}
				return
			}
			m, err := in.Decode(fr, time.Time{})
			if err != nil {
				if m != nil {
					t.Fatalf("message %#v with error %v", m, err)
				}
				if !errors.Is(err, peermsg.ErrMalformed) && !errors.Is(err, peermsg.ErrSchema) &&
					!errors.Is(err, peermsg.ErrState) && !errors.Is(err, peermsg.ErrUnknownMessage) {
					t.Fatalf("unclassified error %v", err)
				}
				continue
			}
			var b []byte
			switch m := m.(type) {
			case peermsg.DefinitionMessage:
				defs[m.ID] = m.Definition
				b, err = peermsg.AppendDefinition(nil, peermsg.LocalTableID(m.ID), m.Definition)
			case peermsg.UpdateMessage:
				b, err = peermsg.AppendUpdate(nil, defs[m.ID], m.Update)
			case peermsg.SwitchMessage:
				b, err = peermsg.AppendSwitch(nil, peermsg.LocalTableID(m.ID))
			case peermsg.AckMessage:
				b, err = peermsg.AppendAck(nil, peermsg.RemoteTableID(m.ID), m.Update)
			case peermsg.Control, peermsg.ErrorMessage:
				continue
			}
			// A frame HAProxy would accept can still exceed what this
			// package encodes; anything else must round-trip.
			if errors.Is(err, peermsg.ErrTooLarge) {
				continue
			}
			if err != nil || string(b) != string(fr.Raw) {
				t.Fatalf("%v re-encodes as %x (%v)", fr, b, err)
			}
		}
	})
}

// seedStreams returns the binary part of every committed capture stream.
func seedStreams(tb testing.TB) [][]byte {
	tb.Helper()
	paths, err := filepath.Glob(filepath.Join(captureDir, "*", "*.txt"))
	if err != nil || len(paths) == 0 {
		tb.Fatalf("no capture fixtures: %v", err)
	}
	var out [][]byte
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			tb.Fatal(err)
		}
		c, err := peertest.Parse(f)
		_ = f.Close()
		if err != nil {
			tb.Fatalf("%s: %v", p, err)
		}
		for kind, lines := range map[string]int{peertest.KindRx: 1, peertest.KindTx: 3} {
			stream := c.Stream(kind)
			d, err := peerwire.NewDecoder(peerwire.DefaultLimits())
			if err != nil {
				tb.Fatal(err)
			}
			if err := d.Feed(stream); err != nil {
				tb.Fatal(err)
			}
			for range lines {
				if _, err := d.NextLine(); err != nil {
					tb.Fatalf("%s %s: %v", p, kind, err)
				}
			}
			out = append(out, stream[d.Offset():])
		}
	}
	return out
}
