package peerwire_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire/peertest"
)

// EnvCaptureDir, when set, makes TestLiveCapture write its captures as
// fixtures under <dir>/<haproxy version>/<case>.txt.
const EnvCaptureDir = "HTA_PEERWIRE_CAPTURE_DIR"

const (
	livePeer  = "hap" // HAProxy's local peer name
	agentPeer = "agg" // the test peer's name
	ioTimeout = 5 * time.Second
)

// captureConfig is the HAProxy configuration for live captures. HAProxy
// dials the test peer at agg and listens for it on its own peers bind.
// t_req has the production schema; t_u32 (gpc0, 32-bit) and t_u64
// (bytes_in_cnt, 64-bit) carry the boundary values. Each has a single
// data type so a value is the tail of its update body.
func captureConfig(aggAddr string) func(lab.CustomParams) ([]byte, error) {
	return func(p lab.CustomParams) ([]byte, error) {
		if len(p.Binds) != 1 {
			return nil, fmt.Errorf("want one bind, got %d", len(p.Binds))
		}
		return []byte(strings.Join([]string{
			"global",
			"    localpeer " + livePeer,
			"    nbthread 1",
			"    maxconn 64",
			"    stats socket " + p.Socket + " mode 600 level admin",
			"defaults",
			"    timeout connect 2s",
			"    timeout client 10s",
			"    timeout server 10s",
			"peers cap",
			"    bind " + p.Binds[0],
			"    server " + livePeer,
			"    server " + agentPeer + " " + aggAddr,
			"backend " + tableReq,
			"    stick-table type ipv6 size 1k expire 30s store http_req_cnt,http_req_rate(10s) peers cap",
			"backend " + tableU32,
			"    stick-table type integer size 1k expire 30s store gpc0 peers cap",
			"backend " + tableU64,
			"    stick-table type integer size 1k expire 30s store bytes_in_cnt peers cap",
			"",
		}, "\n")), nil
	}
}

// TestLiveCapture records fresh peers-protocol exchanges with the stock
// HAProxy named by HTA_HAPROXY, checks them exactly as committed fixtures
// are checked, and optionally writes them as fixtures.
func TestLiveCapture(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	cfg := captureConfig(ln.Addr().String())
	h := labtest.StartCustom(t, lab.CustomOptions{Name: livePeer, Listeners: 1, Config: cfg})
	rendered, err := cfg(lab.CustomParams{Socket: "<runtime socket>", Binds: []string{"fd@3"}})
	if err != nil {
		t.Fatal(err)
	}
	env := liveEnv{t: t, h: h, config: string(rendered)}

	captures := []*peertest.Capture{env.initiator(ln)}
	_ = ln.Close() // HAProxy's reconnects to agg are now refused.
	captures = append(captures, env.acceptor())
	for _, sc := range []struct {
		name  string
		hello string
	}{
		{"status-501", "HAProxyX 2.1\nhap\nagg 1 1\n"},
		{"status-502", "HAProxyS 3.0\nhap\nagg 1 1\n"},
		{"status-503", "HAProxyS 2.1\nnothap\nagg 1 1\n"},
		{"status-504", "HAProxyS 2.1\nhap\nnobody 1 1\n"},
	} {
		captures = append(captures, env.status(sc.name, sc.hello))
	}
	// HAProxy's default tune.bufsize is 16384: a message larger than that
	// in total is never processed, and a declared body larger than that is
	// refused from its header.
	atBufsize := unknownMessage(t, 16384-5)
	overBufsize := unknownMessage(t, 16384-4)
	tooLarge := peerwire.AppendUint([]byte{0x0a, 0xff}, 16384+1)
	reserved := []byte{0xff, 0x00}
	captures = append(captures,
		env.errorFrame("frame-at-bufsize", cat(atBufsize, reserved)),
		env.errorFrame("frame-over-bufsize", cat(overBufsize, reserved)),
		env.errorFrame("size-limit", tooLarge),
		env.errorFrame("reserved-class", reserved),
		// Unknown classes and types, fixed and variable, are skipped:
		// only the trailing reserved class draws an error.
		env.errorFrame("unknown-messages", cat(
			[]byte{0x2a, 0x07}, []byte{0x2a, 0xc3, 0x01, 'x'}, []byte{0x00, 0x09}, []byte{0x0a, 0xff, 0x00}, reserved)),
		env.errorFrame("length-encoding", []byte{0x0a, 0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}))

	dir := os.Getenv(EnvCaptureDir)
	version, _, _ := strings.Cut(h.Record.HAProxyVersion, "-")
	for _, c := range captures {
		t.Run(c.Get("case"), func(t *testing.T) {
			verifyCapture(t, c)
			if dir == "" {
				return
			}
			out := filepath.Join(dir, version, c.Get("case")+".txt")
			if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(out, c.Encode(env.comments()), 0o644); err != nil {
				t.Fatal(err)
			}
			t.Logf("wrote %s", out)
		})
	}
}

type liveEnv struct {
	t      *testing.T
	h      *lab.Custom
	config string
}

func (e liveEnv) newCapture(name, description string) *peertest.Capture {
	return peertest.NewCapture(name, description, e.h)
}

func (e liveEnv) comments() []string {
	return peertest.Comments([]string{
		"Fresh capture from stock HAProxy; generated by TestLiveCapture",
		"(internal/peerwire/capture_live_test.go) via `make peerwire-captures`.",
		"Do not edit. rx = HAProxy to test peer, tx = test peer to HAProxy.",
		"HAProxy configuration (listener inherited as fd@3):",
	}, e.config)
}

func (e liveEnv) wrap(conn net.Conn, c *peertest.Capture) *peertest.Conn {
	return peertest.Wrap(e.t, conn, c)
}

// unknownMessage is a stick-table class message of an unassigned variable
// type (HAProxy ignores it) with a zero body of n bytes.
func unknownMessage(t *testing.T, n int) []byte {
	t.Helper()
	b, err := peerwire.AppendFrame(nil, peerwire.ClassStickTable, 0xff, make([]byte, n))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func control(typ peerwire.MessageType) []byte { return []byte{byte(peerwire.ClassControl), byte(typ)} }

func ourHello(t *testing.T) []byte {
	t.Helper()
	b, err := peerwire.AppendHello(nil, peerwire.Hello{
		Version: peerwire.ProtocolVersion, RemotePeer: livePeer, LocalPeer: agentPeer,
		PID: uint32(os.Getpid()), RelativePID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (e liveEnv) runtime(c *peertest.Capture, cmd string) {
	e.t.Helper()
	peertest.RuntimeSilent(e.t, e.h, c, cmd)
}

// initiator accepts HAProxy's own connection to agg, answers its hello,
// completes the resync it requests, stores boundary values, and records
// the resulting definitions, updates, and a heartbeat.
func (e liveEnv) initiator(ln net.Listener) *peertest.Capture {
	t := e.t
	c := e.newCapture("initiator", "HAProxy dials the test peer; boundary values set over the runtime API")
	if tl, ok := ln.(*net.TCPListener); ok {
		_ = tl.SetDeadline(time.Now().Add(10 * time.Second))
	}
	conn, err := ln.Accept()
	if err != nil {
		t.Fatalf("HAProxy did not connect: %v", err)
	}
	l := e.wrap(conn, c)
	for range peerwire.HelloLines {
		l.Line()
	}
	status, err := peerwire.AppendStatus(nil, peerwire.StatusSucceeded)
	if err != nil {
		t.Fatal(err)
	}
	l.Send(status)
	l.UntilControl(peerwire.ControlResyncRequest)
	l.Send(control(peerwire.ControlResyncFinished))
	l.UntilControl(peerwire.ControlResyncConfirm)

	e.runtime(c, "set table "+tableReq+" key 2001:db8::1 data.http_req_cnt 10")
	e.runtime(c, "set table "+tableReq+" key ::ffff:192.0.2.1 data.http_req_cnt 20")
	for i, v := range boundaryU32 {
		e.runtime(c, fmt.Sprintf("set table %s key %d data.gpc0 %d", tableU32, i+1, v))
	}
	for i, v := range boundaryU64 {
		// The runtime API parses a signed 64-bit value.
		e.runtime(c, fmt.Sprintf("set table %s key %d data.bytes_in_cnt %d", tableU64, i+1, int64(v)))
	}
	want := 2 + len(boundaryU32) + len(boundaryU64)
	for got := 0; got < want; {
		f, err := l.Frame()
		if err != nil {
			t.Fatalf("waiting for updates (%d of %d): %v", got, want, err)
		}
		if f.Class == peerwire.ClassStickTable && f.Type != peerwire.StickTableDefine {
			got++
		}
	}
	l.Send(control(peerwire.ControlHeartbeat))
	c.Note("waiting for HAProxy's idle heartbeat")
	l.UntilControl(peerwire.ControlHeartbeat)
	return l.Finish()
}

// acceptor dials HAProxy's peers bind as agg once the tables hold the
// boundary values, and records the status line, the table contents that
// HAProxy pushes right behind it, and its idle heartbeat.
func (e liveEnv) acceptor() *peertest.Capture {
	t := e.t
	c := e.newCapture("acceptor", "the test peer dials HAProxy, which answers 200 and pushes its tables")
	l := e.dial(c)
	l.Send(ourHello(t))
	l.Line()
	l.Send(control(peerwire.ControlHeartbeat))
	c.Note("waiting for HAProxy's idle heartbeat")
	l.UntilControl(peerwire.ControlHeartbeat)
	return l.Finish()
}

// status sends a raw hello HAProxy must refuse and records its status
// line and close.
func (e liveEnv) status(name, hello string) *peertest.Capture {
	c := e.newCapture(name, "HAProxy refuses a malformed or mismatched hello")
	l := e.dial(c)
	l.Send([]byte(hello))
	l.Line()
	l.ExpectEOF()
	return l.Finish()
}

// errorFrame completes a handshake, sends bad, and records HAProxy's
// answer up to its close.
func (e liveEnv) errorFrame(name string, bad []byte) *peertest.Capture {
	c := e.newCapture(name, "the test peer sends an invalid or oversized message after the handshake")
	l := e.dial(c)
	l.Send(ourHello(e.t))
	l.Line()
	l.Send(bad)
	l.ExpectEOF()
	return l.Finish()
}

func (e liveEnv) dial(c *peertest.Capture) *peertest.Conn {
	e.t.Helper()
	return peertest.Dial(e.t, e.h.Node.Listeners[0], c)
}
