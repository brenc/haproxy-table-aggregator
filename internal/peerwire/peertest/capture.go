// Package peertest records and replays peers-protocol exchanges with stock
// HAProxy for tests: the capture fixture format and a recorded connection
// driven through a peerwire.Decoder. It is test support, not part of the
// service.
package peertest

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// Capture fixtures record one TCP connection between stock HAProxy and a
// test peer. They are written only by live capture tests and are never
// edited by hand. Format, one item per line:
//
//	# comment                 ignored
//	@key value                metadata
//	rx <hex>                  bytes received from HAProxy, in order
//	tx <hex>                  bytes sent to HAProxy, in order
//	txz <n>                   n zero bytes sent to HAProxy
//	note <text>               an action taken at this point (runtime
//	                          command, runtime reply line, read
//	                          deadline, ...)
//
// The rx and tx streams are the concatenation of their lines.
const (
	// Format is the value of every fixture's @format metadata.
	Format     = "peerwire-capture/1"
	hexPerLine = 32
	zeroRunMin = 64
	maxZeroRun = 1 << 20
)

// Item kinds.
const (
	KindRx   = "rx"
	KindTx   = "tx"
	KindNote = "note"
)

// Item is one recorded event: received or sent bytes, or a note.
type Item struct {
	// Kind is KindRx, KindTx, or KindNote.
	Kind string
	// Data holds the bytes of an rx or tx item.
	Data []byte
	// Text holds a note.
	Text string
}

// Capture is one recorded connection with its metadata.
type Capture struct {
	// Meta holds key/value metadata in insertion order.
	Meta [][2]string
	// Items holds the events in order.
	Items []Item
}

// Set sets metadata key to value, replacing an earlier value.
func (c *Capture) Set(key, value string) {
	for i := range c.Meta {
		if c.Meta[i][0] == key {
			c.Meta[i][1] = value
			return
		}
	}
	c.Meta = append(c.Meta, [2]string{key, value})
}

// Get returns the metadata value for key, or "".
func (c *Capture) Get(key string) string {
	for _, kv := range c.Meta {
		if kv[0] == key {
			return kv[1]
		}
	}
	return ""
}

// Add records a copy of data as an item of kind KindRx or KindTx. Empty
// data is not recorded.
func (c *Capture) Add(kind string, data []byte) {
	if len(data) == 0 {
		return
	}
	c.Items = append(c.Items, Item{Kind: kind, Data: bytes.Clone(data)})
}

// Note records a note. Each line of the formatted text becomes one note,
// as the format has no multi-line items.
func (c *Capture) Note(format string, args ...any) {
	for line := range strings.SplitSeq(fmt.Sprintf(format, args...), "\n") {
		c.Items = append(c.Items, Item{Kind: KindNote, Text: line})
	}
}

// Notes returns every note in order.
func (c *Capture) Notes() []string {
	var out []string
	for _, it := range c.Items {
		if it.Kind == KindNote {
			out = append(out, it.Text)
		}
	}
	return out
}

// Stream returns the concatenation of every item of kind.
func (c *Capture) Stream(kind string) []byte {
	var b []byte
	for _, it := range c.Items {
		if it.Kind == kind {
			b = append(b, it.Data...)
		}
	}
	return b
}

// TrimRx drops received bytes past n, which a reader may have buffered
// beyond the last complete frame before closing, and returns how many
// bytes it dropped.
func (c *Capture) TrimRx(n uint64) int {
	total := len(c.Stream(KindRx))
	if uint64(total) <= n {
		return 0
	}
	excess := total - int(n) //nolint:gosec // G115: n < total, an int.
	dropped := excess
	for i := len(c.Items) - 1; i >= 0 && excess > 0; i-- {
		if c.Items[i].Kind != KindRx {
			continue
		}
		k := min(excess, len(c.Items[i].Data))
		c.Items[i].Data = c.Items[i].Data[:len(c.Items[i].Data)-k]
		excess -= k
	}
	c.Items = slices.DeleteFunc(c.Items, func(it Item) bool { return it.Kind != KindNote && len(it.Data) == 0 })
	return dropped
}

// Encode renders c in the fixture format, preceded by comment lines.
func (c *Capture) Encode(comments []string) []byte {
	var b bytes.Buffer
	for _, line := range comments {
		b.WriteString(strings.TrimRight("# "+line, " ") + "\n")
	}
	for _, kv := range c.Meta {
		fmt.Fprintf(&b, "@%s %s\n", kv[0], kv[1])
	}
	for _, it := range c.Items {
		if it.Kind == KindNote {
			fmt.Fprintf(&b, "note %s\n", it.Text)
			continue
		}
		data := it.Data
		for len(data) > 0 {
			if it.Kind == KindTx {
				z := 0
				for z < len(data) && data[z] == 0 {
					z++
				}
				if z >= zeroRunMin {
					fmt.Fprintf(&b, "txz %d\n", z)
					data = data[z:]
					continue
				}
			}
			n := min(len(data), hexPerLine)
			if it.Kind == KindTx {
				if i := bytes.Index(data[:n], make([]byte, zeroRunMin)); i > 0 {
					n = i
				}
			}
			fmt.Fprintf(&b, "%s %s\n", it.Kind, hex.EncodeToString(data[:n]))
			data = data[n:]
		}
	}
	return b.Bytes()
}

// Parse reads a fixture. It fails on unknown items, bad hex, or a format
// other than Format.
func Parse(r io.Reader) (*Capture, error) {
	c := &Capture{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kind, rest, _ := strings.Cut(line, " ")
		switch {
		case strings.HasPrefix(kind, "@"):
			c.Meta = append(c.Meta, [2]string{kind[1:], rest})
		case kind == KindRx || kind == KindTx:
			b, err := hex.DecodeString(rest)
			if err != nil || len(b) == 0 {
				return nil, fmt.Errorf("line %d: bad hex", n)
			}
			c.Items = append(c.Items, Item{Kind: kind, Data: b})
		case kind == "txz":
			z, err := strconv.Atoi(rest)
			if err != nil || z <= 0 || z > maxZeroRun {
				return nil, fmt.Errorf("line %d: bad zero run %q", n, rest)
			}
			c.Items = append(c.Items, Item{Kind: KindTx, Data: make([]byte, z)})
		case kind == KindNote:
			c.Items = append(c.Items, Item{Kind: KindNote, Text: rest})
		default:
			return nil, fmt.Errorf("line %d: unknown item %q", n, kind)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if got := c.Get("format"); got != Format {
		return nil, fmt.Errorf("format %q, want %q", got, Format)
	}
	return c, nil
}
