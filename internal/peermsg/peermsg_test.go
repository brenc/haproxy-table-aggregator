package peermsg_test

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// unhex decodes hex written with spaces between fields.
func unhex(tb testing.TB, s string) []byte {
	tb.Helper()
	b, err := hex.DecodeString(strings.Join(strings.Fields(s), ""))
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

// Hand-written bodies, field by field from the layout in doc/peers.txt
// and src/peers.c; integers in HAProxy's encoding (values below 0xf0 take
// one byte; 30000 is f0 c4 0d, 10000 is f0 e2 03, and the bitfield
// 1<<9|1<<10 = 0x600 is f0 51).
const (
	// t_in, ID 1: key type 5, length 16, http_req_cnt and
	// http_req_rate(10000), expiry 30000, then rate parameters.
	bodyDefIn = "01 04 745f696e 05 10 f051 f0c40d 0a f0e203"
	// t_out, ID 2: gpt(3) is bit 22 (1<<22 = 0x400000, encoded f0 f1 fe
	// 0e), then gpt parameters: type 22 (0x16), 3 elements.
	bodyDefOut = "02 05 745f6f7574 05 10 f0f1fe0e f0c40d 16 03"
)

func TestDecodeDefinitionFixtures(t *testing.T) {
	for _, tc := range []struct {
		body string
		id   peermsg.RemoteTableID
		want peermsg.Definition
	}{
		{bodyDefIn, 1, defIn},
		{bodyDefOut, 2, defOut},
	} {
		id, def, err := peermsg.DecodeDefinition(unhex(t, tc.body))
		if err != nil || id != tc.id || fmt.Sprint(def) != fmt.Sprint(tc.want) {
			t.Fatalf("%s: %d %+v %v, want %d %+v", tc.body, id, def, err, tc.id, tc.want)
		}
		got, err := peermsg.AppendDefinition(nil, peermsg.LocalTableID(tc.id), tc.want)
		want := append([]byte{0x0a, 0x82, byte(len(unhex(t, tc.body)))}, unhex(t, tc.body)...)
		if err != nil || string(got) != string(want) {
			t.Fatalf("encode %+v: %x %v, want %x", tc.want, got, err, want)
		}
	}
}

func TestDecodeDefinitionErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		body  string
		class error
		err   error
	}{
		{"empty", "", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"table ID 0", "00 04 745f696e 05 10 00 f0c40d", peermsg.ErrMalformed, peermsg.ErrTableID},
		{"table ID over 32 bits", "f0f1fefefe7e 04 745f696e 05 10 00 f0c40d", peermsg.ErrMalformed, peerwire.ErrIntRange},
		{"empty name", "01 00 05 10 00 f0c40d", peermsg.ErrMalformed, peermsg.ErrTableName},
		{"name with space", "01 04 745f2069 05 10 00 f0c40d", peermsg.ErrMalformed, peermsg.ErrTableName},
		{"name past end", "01 09 745f696e", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"missing expiry", "01 04 745f696e 05 10 00", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"expiry 0 (no expire)", "01 04 745f696e 05 10 00 00", peermsg.ErrSchema, peermsg.ErrExpiry},
		{"integer key type", "01 04 745f696e 02 04 00 f0c40d", peermsg.ErrSchema, peermsg.ErrKeyType},
		{"string key type", "01 04 745f696e 06 20 00 f0c40d", peermsg.ErrSchema, peermsg.ErrKeyType},
		{"ipv6 key length 4", "01 04 745f696e 05 04 00 f0c40d", peermsg.ErrSchema, peermsg.ErrKeyLength},
		{"ipv6 key length 17", "01 04 745f696e 05 11 00 f0c40d", peermsg.ErrSchema, peermsg.ErrKeyLength},
		{"gpc0", "01 04 745f696e 05 10 04 f0c40d", peermsg.ErrSchema, peermsg.ErrDataType},
		{"conn_rate", "01 04 745f696e 05 10 20 f0c40d 05 f0e203", peermsg.ErrSchema, peermsg.ErrDataType},
		{"gpc array", "01 04 745f696e 05 10 f0f1fe1e f0c40d 16 03 17 02", peermsg.ErrSchema, peermsg.ErrDataType},
		{
			"bit 63", "01 04 745f696e 05 10 " + hex.EncodeToString(peerwire.AppendUint(nil, 1<<63)) + " f0c40d",
			peermsg.ErrSchema, peermsg.ErrDataType,
		},
		{"rate params for wrong type", "01 04 745f696e 05 10 f051 f0c40d 16 f0e203", peermsg.ErrSchema, peermsg.ErrFieldOrder},
		{"gpt params before rate", "01 04 745f696e 05 10 f0f1fe0e f0c40d 0a 03", peermsg.ErrSchema, peermsg.ErrFieldOrder},
		{"missing rate params", "01 04 745f696e 05 10 f051 f0c40d", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"missing period", "01 04 745f696e 05 10 f051 f0c40d 0a", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"zero period", "01 04 745f696e 05 10 f051 f0c40d 0a 00", peermsg.ErrSchema, peermsg.ErrPeriod},
		{"zero array length", "01 05 745f6f7574 05 10 f0f1fe0e f0c40d 16 00", peermsg.ErrSchema, peermsg.ErrArrayLength},
		{"array length 101", "01 05 745f6f7574 05 10 f0f1fe0e f0c40d 16 65", peermsg.ErrSchema, peermsg.ErrArrayLength},
		{"missing array length", "01 05 745f6f7574 05 10 f0f1fe0e f0c40d 16", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"trailing byte", bodyDefIn + " 00", peermsg.ErrMalformed, peermsg.ErrTrailingData},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, def, err := peermsg.DecodeDefinition(unhex(t, tc.body))
			if !errors.Is(err, tc.class) || !errors.Is(err, tc.err) {
				t.Fatalf("got %v, want %v and %v", err, tc.class, tc.err)
			}
			if errors.Is(tc.class, peermsg.ErrSchema) && (id != 1 || !strings.HasPrefix(def.Name, "t_")) {
				t.Fatalf("schema error without table identity: %d %q", id, def.Name)
			}
		})
	}
}

func TestDefinitionValidate(t *testing.T) {
	gpt3 := peermsg.Field{Type: peermsg.DataGPT, ArrayLen: 3}
	cnt := peermsg.Field{Type: peermsg.DataHTTPReqCnt}
	rate := peermsg.Field{Type: peermsg.DataHTTPReqRate, Period: 10000}
	ok := peermsg.Definition{Name: "t", KeyType: peermsg.KeyTypeIPv6, Expiry: 30000, Fields: []peermsg.Field{cnt, rate, gpt3}}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*peermsg.Definition)
		err  error
	}{
		{"empty name", func(d *peermsg.Definition) { d.Name = "" }, peermsg.ErrTableName},
		{"key type", func(d *peermsg.Definition) { d.KeyType = 2 }, peermsg.ErrKeyType},
		{"no expiry", func(d *peermsg.Definition) { d.Expiry = 0 }, peermsg.ErrExpiry},
		{"order", func(d *peermsg.Definition) { d.Fields = []peermsg.Field{rate, cnt} }, peermsg.ErrFieldOrder},
		{"duplicate", func(d *peermsg.Definition) { d.Fields = []peermsg.Field{cnt, cnt} }, peermsg.ErrFieldOrder},
		{"unsupported", func(d *peermsg.Definition) { d.Fields = []peermsg.Field{{Type: 2}} }, peermsg.ErrDataType},
		{"scalar with length", func(d *peermsg.Definition) { d.Fields = []peermsg.Field{{Type: peermsg.DataHTTPReqCnt, ArrayLen: 1}} }, peermsg.ErrArrayLength},
		{"array too long", func(d *peermsg.Definition) { d.Fields = []peermsg.Field{{Type: peermsg.DataGPT, ArrayLen: 101}} }, peermsg.ErrArrayLength},
		{"period on count", func(d *peermsg.Definition) { d.Fields = []peermsg.Field{{Type: peermsg.DataHTTPReqCnt, Period: 1}} }, peermsg.ErrPeriod},
		{"no period", func(d *peermsg.Definition) { d.Fields = []peermsg.Field{{Type: peermsg.DataHTTPReqRate}} }, peermsg.ErrPeriod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := ok
			d.Fields = append([]peermsg.Field(nil), ok.Fields...)
			tc.edit(&d)
			err := d.Validate()
			if !errors.Is(err, peermsg.ErrSchema) || !errors.Is(err, tc.err) {
				t.Fatalf("got %v, want %v", err, tc.err)
			}
			if _, aerr := peermsg.AppendDefinition(nil, 1, d); !errors.Is(aerr, tc.err) {
				t.Fatalf("AppendDefinition: %v", aerr)
			}
			if _, aerr := peermsg.AppendUpdate(nil, d, peermsg.Update{}); !errors.Is(aerr, tc.err) {
				t.Fatalf("AppendUpdate: %v", aerr)
			}
		})
	}
	if _, err := peermsg.AppendDefinition(nil, 0, ok); !errors.Is(err, peermsg.ErrTableID) {
		t.Fatalf("local ID 0: %v", err)
	}
	long := ok
	long.Name = strings.Repeat("n", peermsg.MaxEncodedMessage)
	if _, err := peermsg.AppendDefinition(nil, 1, long); !errors.Is(err, peermsg.ErrTooLarge) {
		t.Fatalf("oversized definition: %v", err)
	}
}

// keyHex is the 16 key bytes of 2001:db8:1:1::.
const keyHex = "20010db8 00010001 00000000 00000000"

func TestUpdateFixtures(t *testing.T) {
	rx := time.Unix(1000, 0)
	for _, tc := range []struct {
		name string
		typ  peerwire.MessageType
		def  peermsg.Definition
		prev peermsg.UpdateID
		body string
		want peermsg.Update
	}{
		{
			// http_req_cnt 5; rate age 7 ms, curr 5, prev 0.
			name: "explicit", typ: peerwire.StickTableUpdate, def: defIn,
			body: "00000001 " + keyHex + " 05 07 05 00",
			want: peermsg.Update{ID: 1, ExplicitID: true, Key: keyNative, Values: inValues(5, 7, 5, 0)},
		},
		{
			name: "implicit", typ: peerwire.StickTableIncrementalUpdate, def: defIn, prev: 1,
			body: keyHex + " 05 07 05 00",
			want: peermsg.Update{ID: 2, Key: keyNative, Values: inValues(5, 7, 5, 0)},
		},
		{
			// Remaining 29997 ms = 0x752d.
			name: "timed explicit", typ: peerwire.StickTableTimedUpdate, def: defIn,
			body: "00000001 0000752d " + keyHex + " 05 07 05 00",
			want: peermsg.Update{ID: 1, ExplicitID: true, Timed: true, Remaining: 29997, Key: keyNative, Values: inValues(5, 7, 5, 0)},
		},
		{
			name: "timed implicit wraps", typ: peerwire.StickTableIncrementalTimedUpdate, def: defIn, prev: 0xffffffff,
			body: "00000fa0 " + keyHex + " 05 07 05 00",
			want: peermsg.Update{ID: 0, Timed: true, Remaining: 4000, Key: keyNative, Values: inValues(5, 7, 5, 0)},
		},
		{
			// gpt(3) = 7, 0, 239 (the largest one-byte encoding).
			name: "gpt", typ: peerwire.StickTableUpdate, def: defOut,
			body: "00000002 " + keyHex + " 07 00 ef",
			want: peermsg.Update{ID: 2, ExplicitID: true, Key: keyNative, Values: gpt(7, 0, 239)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := unhex(t, tc.body)
			u, err := peermsg.DecodeUpdate(tc.typ, body, tc.def, tc.prev, true, rx)
			tc.want.Received = rx
			if err != nil || fmt.Sprint(u) != fmt.Sprint(tc.want) {
				t.Fatalf("got %+v %v, want %+v", u, err, tc.want)
			}
			b, err := peermsg.AppendUpdate(nil, tc.def, tc.want)
			want, ferr := peerwire.AppendFrame(nil, peerwire.ClassStickTable, tc.typ, body)
			if err != nil || ferr != nil || string(b) != string(want) {
				t.Fatalf("encode: %x %v, want %x", b, err, want)
			}
		})
	}
}

func inValues(cnt, age, curr, prev uint32) []peermsg.Value {
	return []peermsg.Value{
		{Type: peermsg.DataHTTPReqCnt, Uint: cnt},
		{Type: peermsg.DataHTTPReqRate, Freq: peermsg.FreqCounter{Age: peermsg.Millis(age), Curr: curr, Prev: prev}},
	}
}

func TestUpdateErrors(t *testing.T) {
	u32max := hex.EncodeToString(peerwire.AppendUint(nil, 1<<32-1))
	over := hex.EncodeToString(peerwire.AppendUint(nil, 1<<32))
	for _, tc := range []struct {
		name     string
		typ      peerwire.MessageType
		def      peermsg.Definition
		havePrev bool
		body     string
		class    error
		err      error
	}{
		{"short ID", peerwire.StickTableUpdate, defIn, false, "000000", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"short remaining", peerwire.StickTableIncrementalTimedUpdate, defIn, true, "0000", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"key of 4 bytes", peerwire.StickTableUpdate, defOut, false, "00000001 c0000201 07 00 00", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"key of 15 bytes", peerwire.StickTableUpdate, defOut, false, "00000001 20010db8000100010000000000000000", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"array of 2", peerwire.StickTableUpdate, defOut, false, "00000001 " + keyHex + " 07 00", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"array of 4", peerwire.StickTableUpdate, defOut, false, "00000001 " + keyHex + " 07 00 00 01", peermsg.ErrMalformed, peermsg.ErrTrailingData},
		{"missing rate", peerwire.StickTableUpdate, defIn, false, "00000001 " + keyHex + " 05", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"rate without prev", peerwire.StickTableUpdate, defIn, false, "00000001 " + keyHex + " 05 07 05", peermsg.ErrMalformed, peerwire.ErrShortBody},
		{"count over 32 bits", peerwire.StickTableUpdate, defIn, false, "00000001 " + keyHex + " " + over + " 07 05 00", peermsg.ErrMalformed, peerwire.ErrIntRange},
		{"age over 32 bits", peerwire.StickTableUpdate, defIn, false, "00000001 " + keyHex + " 05 " + over + " 05 00", peermsg.ErrMalformed, peerwire.ErrIntRange},
		{"gpt over 32 bits", peerwire.StickTableUpdate, defOut, false, "00000001 " + keyHex + " " + u32max + " 00 " + over, peermsg.ErrMalformed, peerwire.ErrIntRange},
		{"implicit without previous", peerwire.StickTableIncrementalUpdate, defOut, false, keyHex + " 07 00 00", peermsg.ErrState, peermsg.ErrImplicitID},
		{"not an update", peerwire.StickTableDefine, defOut, true, bodyDefOut, peermsg.ErrUnknownMessage, peermsg.ErrUnknownMessage},
		{"invalid definition", peerwire.StickTableUpdate, peermsg.Definition{Name: "x", KeyType: 2}, true, "00000001", peermsg.ErrSchema, peermsg.ErrKeyType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := peermsg.DecodeUpdate(tc.typ, unhex(t, tc.body), tc.def, 0, tc.havePrev, time.Time{})
			if !errors.Is(err, tc.class) || !errors.Is(err, tc.err) {
				t.Fatalf("got %v, want %v and %v", err, tc.class, tc.err)
			}
		})
	}
	// 2^32-1 itself fits every 32-bit field.
	body := unhex(t, "00000001 "+keyHex+" "+u32max+" "+u32max+" "+u32max)
	u, err := peermsg.DecodeUpdate(peerwire.StickTableUpdate, body, defOut, 0, false, time.Time{})
	if err != nil || fmt.Sprint(u.Values) != fmt.Sprint(gpt(1<<32-1, 1<<32-1, 1<<32-1)) {
		t.Fatalf("32-bit maxima: %+v %v", u.Values, err)
	}
}

func TestAppendUpdateRejectsPartialValues(t *testing.T) {
	full := peermsg.Update{ID: 1, ExplicitID: true, Key: keyNative, Values: inValues(1, 2, 3, 4)}
	if _, err := peermsg.AppendUpdate(nil, defIn, full); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		def    peermsg.Definition
		values []peermsg.Value
	}{
		{"no values", defIn, nil},
		{"missing rate", defIn, inValues(1, 2, 3, 4)[:1]},
		{"extra value", defIn, append(inValues(1, 2, 3, 4), peermsg.Value{Type: peermsg.DataGPT})},
		{"swapped", defIn, []peermsg.Value{inValues(1, 2, 3, 4)[1], inValues(1, 2, 3, 4)[0]}},
		{"short array", defOut, gpt(1, 2)},
		{"long array", defOut, gpt(1, 2, 3, 4)},
		{"nil array", defOut, []peermsg.Value{{Type: peermsg.DataGPT}}},
		{"count in gpt", defOut, []peermsg.Value{{Type: peermsg.DataGPT, Uint: 1, Array: []uint32{1, 2, 3}}}},
		{"counter in count", defIn, []peermsg.Value{
			{Type: peermsg.DataHTTPReqCnt, Freq: peermsg.FreqCounter{Curr: 1}}, inValues(1, 2, 3, 4)[1],
		}},
		{"array in rate", defIn, []peermsg.Value{
			inValues(1, 2, 3, 4)[0], {Type: peermsg.DataHTTPReqRate, Array: []uint32{}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := full
			u.Values = tc.values
			dst := []byte{0xaa}
			got, err := peermsg.AppendUpdate(dst, tc.def, u)
			if !errors.Is(err, peermsg.ErrSchema) || !errors.Is(err, peermsg.ErrValues) {
				t.Fatalf("got %v, want ErrValues", err)
			}
			if string(got) != string(dst) {
				t.Fatalf("dst changed on error: %x", got)
			}
		})
	}
}

func TestLifetime(t *testing.T) {
	for _, tc := range []struct {
		u    peermsg.Update
		want peermsg.Millis
	}{
		{peermsg.Update{}, 30000},
		{peermsg.Update{Remaining: 5000}, 30000}, // ignored when not timed
		{peermsg.Update{Timed: true, Remaining: 5000}, 5000},
		{peermsg.Update{Timed: true, Remaining: 0}, 0},
		{peermsg.Update{Timed: true, Remaining: 40000}, 30000}, // capped at the table expiry
	} {
		if got := tc.u.Lifetime(30000); got != tc.want {
			t.Fatalf("%+v: lifetime %v, want %v", tc.u, got, tc.want)
		}
	}
	if d := peermsg.Millis(1500).Duration(); d != 1500*time.Millisecond {
		t.Fatalf("Millis.Duration: %v", d)
	}
}

// TestKeysDoNotCollide round-trips keys that embed the same IPv4 address
// in different IPv6 forms, and checks that they stay distinct.
func TestKeysDoNotCollide(t *testing.T) {
	addrs := []string{
		"::ffff:192.0.2.1",       // IPv4-mapped, as HAProxy stores IPv4
		"::192.0.2.1",            // IPv4-compatible
		"2001:db8::c000:201",     // native, IPv4 bits in the interface ID
		"64:ff9b::c000:201",      // NAT64 well-known prefix
		"2002:c000:201::",        // 6to4
		"::ffff:0:c000:201",      // SIIT IPv4-translated
		"2001:db8:1:1::",         // native /64
		"::",                     // all zero
		"ffff:ffff:ffff:ffff::1", // high bits
	}
	seen := map[peermsg.Key]string{}
	for _, s := range addrs {
		k, err := peermsg.KeyFromAddr(netip.MustParseAddr(s))
		if err != nil {
			t.Fatal(err)
		}
		if prev, dup := seen[k]; dup {
			t.Fatalf("%s and %s collide", s, prev)
		}
		seen[k] = s
		if k.Addr() != netip.MustParseAddr(s) || k.Addr().Is4() {
			t.Fatalf("%s: Addr() is %v", s, k.Addr())
		}
		u := peermsg.Update{ID: 9, ExplicitID: true, Key: k, Values: gpt(1, 2, 3)}
		b, err := peermsg.AppendUpdate(nil, defOut, u)
		if err != nil {
			t.Fatal(err)
		}
		if got := b[3+4 : 3+4+16]; string(got) != string(k[:]) {
			t.Fatalf("%s: wire key %x", s, got)
		}
		back, err := peermsg.DecodeUpdate(peerwire.StickTableUpdate, b[3:], defOut, 0, false, time.Time{})
		if err != nil || back.Key != k {
			t.Fatalf("%s: decoded key %v, %v", s, back.Key, err)
		}
	}
	// A 4-byte address keys as its IPv4-mapped form, exactly as HAProxy
	// stores IPv4 sources in an IPv6 table.
	k4, err := peermsg.KeyFromAddr(netip.MustParseAddr("192.0.2.1"))
	if err != nil || k4 != mustKey("::ffff:192.0.2.1") || k4.String() != "::ffff:192.0.2.1" {
		t.Fatalf("IPv4 key %v, %v", k4, err)
	}
	for _, bad := range []netip.Addr{{}, netip.MustParseAddr("fe80::1%eth0")} {
		if _, err := peermsg.KeyFromAddr(bad); err == nil {
			t.Fatalf("KeyFromAddr(%v) succeeded", bad)
		}
	}
}

func TestSwitchAndAck(t *testing.T) {
	b, err := peermsg.AppendSwitch(nil, 7)
	if err != nil || hex.EncodeToString(b) != "0a830107" {
		t.Fatalf("switch %x %v", b, err)
	}
	if id, serr := peermsg.DecodeSwitch(b[3:]); serr != nil || id != 7 {
		t.Fatalf("decode switch %d %v", id, serr)
	}
	b, err = peermsg.AppendAck(nil, 1, 0xfffffffe)
	if err != nil || hex.EncodeToString(b) != "0a840501fffffffe" {
		t.Fatalf("ack %x %v", b, err)
	}
	if id, upd, err := peermsg.DecodeAck(b[3:]); err != nil || id != 1 || upd != 0xfffffffe {
		t.Fatalf("decode ack %d %#x %v", id, uint32(upd), err)
	}
	for _, tc := range []struct {
		name   string
		decode func([]byte) error
		body   string
		err    error
	}{
		{"switch empty", decodeSwitch, "", peerwire.ErrShortBody},
		{"switch 0", decodeSwitch, "00", peermsg.ErrTableID},
		{"switch trailing", decodeSwitch, "0100", peermsg.ErrTrailingData},
		{"ack empty", decodeAck, "", peerwire.ErrShortBody},
		{"ack short ID", decodeAck, "01 000000", peerwire.ErrShortBody},
		{"ack table 0", decodeAck, "00 00000001", peermsg.ErrTableID},
		{"ack trailing", decodeAck, "01 00000001 00", peermsg.ErrTrailingData},
	} {
		if err := tc.decode(unhex(t, tc.body)); !errors.Is(err, peermsg.ErrMalformed) || !errors.Is(err, tc.err) {
			t.Fatalf("%s: %v, want %v", tc.name, err, tc.err)
		}
	}
	if _, err := peermsg.AppendSwitch(nil, 0); !errors.Is(err, peermsg.ErrTableID) {
		t.Fatalf("switch to 0: %v", err)
	}
	if _, err := peermsg.AppendAck(nil, 0, 1); !errors.Is(err, peermsg.ErrTableID) {
		t.Fatalf("ack for 0: %v", err)
	}
}

func decodeSwitch(b []byte) error { _, err := peermsg.DecodeSwitch(b); return err }

func decodeAck(b []byte) error { _, _, err := peermsg.DecodeAck(b); return err }

// frame builds a stick-table frame as the peer would send it.
func frame(tb testing.TB, typ peerwire.MessageType, body string) peerwire.Frame {
	tb.Helper()
	raw, err := peerwire.AppendFrame(nil, peerwire.ClassStickTable, typ, unhex(tb, body))
	if err != nil {
		tb.Fatal(err)
	}
	b := unhex(tb, body)
	return peerwire.Frame{Class: peerwire.ClassStickTable, Type: typ, Body: b, Raw: raw}
}

func mustDecode(t *testing.T, in *peermsg.Inbound, f peerwire.Frame) peermsg.Message {
	t.Helper()
	m, err := in.Decode(f, time.Time{})
	if err != nil {
		t.Fatalf("decode %v: %v", f, err)
	}
	return m
}

func updateID(t *testing.T, in *peermsg.Inbound, f peerwire.Frame) peermsg.UpdateID {
	t.Helper()
	m, ok := mustDecode(t, in, f).(peermsg.UpdateMessage)
	if !ok {
		t.Fatalf("%v did not decode as an update", f)
	}
	return m.Update.ID
}

func explicitOut(id string) string { return id + " " + keyHex + " 01 02 03" }

// TestUpdateIDSequence walks explicit and implicit IDs across the 32-bit
// wrap and back to a smaller explicit ID, as a resync replay does. IDs are
// derived and recorded, never ordered by magnitude.
func TestUpdateIDSequence(t *testing.T) {
	in := peermsg.NewInbound(4)
	mustDecode(t, in, frame(t, peerwire.StickTableDefine, bodyDefOut))
	implicit := frame(t, peerwire.StickTableIncrementalUpdate, keyHex+" 01 02 03")
	implicitTimed := frame(t, peerwire.StickTableIncrementalTimedUpdate, "00000064 "+keyHex+" 01 02 03")
	if _, err := in.Decode(implicit, time.Time{}); !errors.Is(err, peermsg.ErrImplicitID) {
		t.Fatalf("implicit first update: %v", err)
	}
	for i, step := range []struct {
		f    peerwire.Frame
		want peermsg.UpdateID
	}{
		{frame(t, peerwire.StickTableUpdate, explicitOut("fffffffe")), 0xfffffffe},
		{implicit, 0xffffffff},
		{implicitTimed, 0},
		{implicit, 1},
		{frame(t, peerwire.StickTableUpdate, explicitOut("00000005")), 5},
		{implicit, 6},
		{frame(t, peerwire.StickTableTimedUpdate, "00000003 00000064 "+keyHex+" 01 02 03"), 3},
		{implicit, 4},
		{frame(t, peerwire.StickTableUpdate, explicitOut("00000004")), 4}, // a repeated ID is accepted as is
	} {
		if got := updateID(t, in, step.f); got != step.want {
			t.Fatalf("step %d: ID %#x, want %#x", i, uint32(got), uint32(step.want))
		}
	}
	tab, _ := in.Table(2)
	if !tab.HaveUpdate || tab.LastUpdate != 4 {
		t.Fatalf("last update %+v", tab)
	}
	// A new definition starts a fresh binding, so the next implicit
	// update has nothing to derive from.
	mustDecode(t, in, frame(t, peerwire.StickTableDefine, bodyDefOut))
	if _, err := in.Decode(implicit, time.Time{}); !errors.Is(err, peermsg.ErrImplicitID) {
		t.Fatalf("implicit after redefinition: %v", err)
	}
}

// TestNamespacesAreIndependent decodes two sources that announce table ID
// 1 for different tables with different schemas.
func TestNamespacesAreIndependent(t *testing.T) {
	a, b := peermsg.NewInbound(4), peermsg.NewInbound(4)
	mustDecode(t, a, frame(t, peerwire.StickTableDefine, bodyDefIn))
	// Source B announces t_out as its table 1.
	mustDecode(t, b, frame(t, peerwire.StickTableDefine, "01 05 745f6f7574 05 10 f0f1fe0e f0c40d 16 03"))
	ua, ok := mustDecode(t, a, frame(t, peerwire.StickTableUpdate, "0000000a "+keyHex+" 0a 01 0a 00")).(peermsg.UpdateMessage)
	if !ok {
		t.Fatal("source A update")
	}
	ub, ok := mustDecode(t, b, frame(t, peerwire.StickTableUpdate, "0000000a "+keyHex+" 14 00 00")).(peermsg.UpdateMessage)
	if !ok {
		t.Fatal("source B update")
	}
	if ua.ID != 1 || ub.ID != 1 || ua.Table != tableIn || ub.Table != tableOut {
		t.Fatalf("A %d/%s, B %d/%s", ua.ID, ua.Table, ub.ID, ub.Table)
	}
	if cnt, _ := ua.Update.Value(peermsg.DataHTTPReqCnt); cnt.Uint != 10 {
		t.Fatalf("A count %d", cnt.Uint)
	}
	if v, _ := ub.Update.Value(peermsg.DataGPT); fmt.Sprint(v.Array) != "[20 0 0]" {
		t.Fatalf("B gpt %v", v.Array)
	}
	// B's update body does not fit A's schema for the same ID.
	if _, err := a.Decode(frame(t, peerwire.StickTableUpdate, "0000000b "+keyHex+" 14 00 00"), time.Time{}); !errors.Is(err, peermsg.ErrMalformed) {
		t.Fatalf("B's body in A's namespace: %v", err)
	}
}

// TestDefinitionRebinding follows HAProxy's rules: a definition unbinds
// whatever its ID and its name were bound to, and selects the table.
func TestDefinitionRebinding(t *testing.T) {
	in := peermsg.NewInbound(4)
	mustDecode(t, in, frame(t, peerwire.StickTableDefine, bodyDefIn))  // 1 = t_in
	mustDecode(t, in, frame(t, peerwire.StickTableDefine, bodyDefOut)) // 2 = t_out
	updOut := frame(t, peerwire.StickTableUpdate, explicitOut("00000001"))
	updIn := frame(t, peerwire.StickTableUpdate, "00000001 "+keyHex+" 01 02 03 04")
	if m, ok := mustDecode(t, in, updOut).(peermsg.UpdateMessage); !ok || m.Table != tableOut {
		t.Fatalf("update after t_out definition: %+v", m)
	}

	// Switching selects a defined table and keeps its last update ID.
	sw, ok := mustDecode(t, in, frame(t, peerwire.StickTableSwitch, "01")).(peermsg.SwitchMessage)
	if !ok || sw.ID != 1 || sw.Table != tableIn {
		t.Fatalf("switch: %+v", sw)
	}
	mustDecode(t, in, updIn)
	mustDecode(t, in, frame(t, peerwire.StickTableSwitch, "02"))
	if got := updateID(t, in, frame(t, peerwire.StickTableIncrementalUpdate, keyHex+" 01 02 03")); got != 2 {
		t.Fatalf("implicit after switch back: %d", got)
	}

	// Redefining ID 1 as t_out moves t_out off ID 2.
	mustDecode(t, in, frame(t, peerwire.StickTableDefine, "01 05 745f6f7574 05 10 f0f1fe0e f0c40d 16 03"))
	if _, ok := in.Table(2); ok {
		t.Fatal("ID 2 still bound after its table moved to ID 1")
	}
	if tab, ok := in.Table(1); !ok || tab.Definition.Name != tableOut {
		t.Fatalf("ID 1 is %+v", tab)
	}
	if _, err := in.Decode(frame(t, peerwire.StickTableSwitch, "02"), time.Time{}); !errors.Is(err, peermsg.ErrUnknownTable) {
		t.Fatalf("switch to unbound ID: %v", err)
	}
	if _, ok := in.Selected(); ok {
		t.Fatal("selection kept after switch to unbound ID")
	}
	if _, err := in.Decode(updOut, time.Time{}); !errors.Is(err, peermsg.ErrNoTable) {
		t.Fatalf("update with no selection: %v", err)
	}

	// A rejected definition binds its ID so its updates are not
	// attributed elsewhere, and does not decode them.
	if _, err := in.Decode(frame(t, peerwire.StickTableDefine, "03 05 745f677063 05 10 04 f0c40d"), time.Time{}); !errors.Is(err, peermsg.ErrDataType) {
		t.Fatalf("gpc0 definition: %v", err)
	}
	if tab, ok := in.Table(3); !ok || tab.Rejected == nil || tab.Definition.Name != "t_gpc" {
		t.Fatalf("rejected binding %+v", tab)
	}
	if _, err := in.Decode(updOut, time.Time{}); !errors.Is(err, peermsg.ErrRejectedTable) {
		t.Fatalf("update for rejected table: %v", err)
	}
	// A malformed definition clears the selection.
	mustDecode(t, in, frame(t, peerwire.StickTableSwitch, "01"))
	if _, err := in.Decode(frame(t, peerwire.StickTableDefine, "01"), time.Time{}); !errors.Is(err, peermsg.ErrMalformed) {
		t.Fatalf("truncated definition: %v", err)
	}
	if _, ok := in.Selected(); ok {
		t.Fatal("selection kept after malformed definition")
	}
}

func TestInboundLimitsAndClasses(t *testing.T) {
	in := peermsg.NewInbound(1)
	mustDecode(t, in, frame(t, peerwire.StickTableDefine, bodyDefIn))
	if _, err := in.Decode(frame(t, peerwire.StickTableDefine, bodyDefOut), time.Time{}); !errors.Is(err, peermsg.ErrTooManyTables) ||
		strings.Contains(err.Error(), "rejected") {
		t.Fatalf("second table over limit 1: %v", err)
	}
	// A definition that is both rejected and over the limit binds nothing:
	// its one class is ErrState, and it carries the schema reason without
	// the ErrSchema class.
	_, err := in.Decode(frame(t, peerwire.StickTableDefine, "02 05 745f677063 05 10 04 f0c40d"), time.Time{})
	for _, want := range []error{peermsg.ErrState, peermsg.ErrTooManyTables, peermsg.ErrDataType} {
		if !errors.Is(err, want) {
			t.Fatalf("rejected definition over the limit: %v, want %v", err, want)
		}
	}
	if errors.Is(err, peermsg.ErrSchema) {
		t.Fatalf("over-limit error %v also has class ErrSchema", err)
	}
	if !strings.Contains(err.Error(), "definition also rejected") || !strings.Contains(err.Error(), "data type 2") {
		t.Fatalf("over-limit error %q lacks the rejection detail", err)
	}
	// Redefining the bound table is within the limit.
	mustDecode(t, in, frame(t, peerwire.StickTableDefine, bodyDefIn))

	for _, tc := range []struct {
		f    peerwire.Frame
		want peermsg.Message
		err  error
	}{
		{peerwire.Frame{Class: peerwire.ClassControl, Type: peerwire.ControlHeartbeat}, peermsg.Control{Type: peerwire.ControlHeartbeat}, nil},
		{peerwire.Frame{Class: peerwire.ClassControl, Type: peerwire.ControlResyncPartial}, peermsg.Control{Type: peerwire.ControlResyncPartial}, nil},
		{peerwire.Frame{Class: peerwire.ClassError, Type: peerwire.ErrorTypeSizeLimit}, peermsg.ErrorMessage{Type: peerwire.ErrorTypeSizeLimit}, nil},
		{peerwire.Frame{Class: peerwire.ClassControl, Type: 9}, nil, peermsg.ErrUnknownMessage},
		{peerwire.Frame{Class: peerwire.ClassError, Type: 2}, nil, peermsg.ErrUnknownMessage},
		{peerwire.Frame{Class: 42, Type: 7}, nil, peermsg.ErrUnknownMessage},
		{frame(t, 0xff, ""), nil, peermsg.ErrUnknownMessage},
		{frame(t, peerwire.StickTableAck, "02 00000009"), peermsg.AckMessage{ID: 2, Update: 9}, nil},
	} {
		m, err := in.Decode(tc.f, time.Time{})
		if !errors.Is(err, tc.err) || m != tc.want {
			t.Fatalf("%v: %#v %v, want %#v %v", tc.f, m, err, tc.want, tc.err)
		}
	}
}

func TestLocalTables(t *testing.T) {
	l := peermsg.NewLocalTables(2)
	id, err := l.Register(defOut)
	if err != nil || id != 1 {
		t.Fatalf("first: %d %v", id, err)
	}
	if id2, err2 := l.Register(defIn); err2 != nil || id2 != 2 {
		t.Fatalf("second: %d %v", id2, err2)
	}
	if _, err = l.Register(defOut); !errors.Is(err, peermsg.ErrDuplicateTable) {
		t.Fatalf("duplicate: %v", err)
	}
	other := defOut
	other.Name = "t_other"
	if _, err = l.Register(other); !errors.Is(err, peermsg.ErrTooManyTables) {
		t.Fatalf("over limit: %v", err)
	}
	if _, err = peermsg.NewLocalTables(4).Register(peermsg.Definition{Name: "x"}); !errors.Is(err, peermsg.ErrKeyType) {
		t.Fatalf("invalid: %v", err)
	}
	d, err := l.Definition(2)
	if err != nil || d.Name != tableIn {
		t.Fatalf("definition 2: %+v %v", d, err)
	}
	d.Fields[0].Type = 0 // must not alias the namespace
	if d, _ := l.Definition(2); d.Fields[0].Type != peermsg.DataHTTPReqCnt {
		t.Fatal("Definition aliases stored fields")
	}
	for _, bad := range []peermsg.LocalTableID{0, 3} {
		if _, err := l.Definition(bad); !errors.Is(err, peermsg.ErrUnknownTable) {
			t.Fatalf("definition %d: %v", bad, err)
		}
	}
	if id, ok := l.ID(tableIn); !ok || id != 2 {
		t.Fatalf("ID(t_in) %d %v", id, ok)
	}
}

// TestRemainingBoundary checks the signed-int limit on a timed update's
// remaining lifetime: HAProxy reads the field as a signed int, so values
// from 0x80000000 are negative to it and fail closed here in both
// directions.
func TestRemainingBoundary(t *testing.T) {
	for _, tc := range []struct {
		remaining uint32
		ok        bool
	}{
		{0x7fffffff, true},
		{0x80000000, false},
		{0xffffffff, false},
	} {
		t.Run(fmt.Sprintf("%#x", tc.remaining), func(t *testing.T) {
			body := unhex(t, fmt.Sprintf("00000001 %08x %s 01 02 03", tc.remaining, keyHex))
			u, err := peermsg.DecodeUpdate(peerwire.StickTableTimedUpdate, body, defOut, 0, false, time.Time{})
			enc := peermsg.Update{
				ID: 1, ExplicitID: true, Timed: true, Remaining: peermsg.Millis(tc.remaining),
				Key: keyNative, Values: gpt(1, 2, 3),
			}
			dst := []byte{0xaa}
			b, aerr := peermsg.AppendUpdate(dst, defOut, enc)
			if tc.ok {
				if err != nil || aerr != nil || u.Remaining != peermsg.MaxRemaining {
					t.Fatalf("decode %+v %v, encode %v", u, err, aerr)
				}
				if got := u.Lifetime(30000); got != 30000 {
					t.Fatalf("lifetime %v, want the table expiry", got)
				}
				return
			}
			for _, e := range []error{err, aerr} {
				if !errors.Is(e, peermsg.ErrMalformed) || !errors.Is(e, peermsg.ErrRemaining) {
					t.Fatalf("got %v, want ErrRemaining", e)
				}
			}
			if string(b) != string(dst) {
				t.Fatalf("dst changed on error: %x", b)
			}
			if got := enc.Lifetime(30000); got != 0 {
				t.Fatalf("out-of-range lifetime %v, want 0", got)
			}
		})
	}
}
