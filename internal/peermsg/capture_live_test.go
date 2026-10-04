package peermsg_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire/peertest"
)

// EnvCaptureDir, when set, makes TestLiveTableCapture write its captures
// as fixtures under <dir>/<haproxy version>/<case>.txt.
const EnvCaptureDir = "HTA_PEERMSG_CAPTURE_DIR"

const (
	livePeer  = "hap"
	agentPeer = "agg"
)

// tableConfig renders a node with the input and output tables in the
// given order (which fixes HAProxy's table IDs: 1, 2, ... in
// configuration order) and a lab frontend that tracks the synthetic
// client header with the production key rule.
func tableConfig(aggAddr string, tables []string) func(lab.CustomParams) ([]byte, error) {
	stores := map[string]string{
		tableIn:  "stick-table type ipv6 size 1k expire 30s store http_req_cnt,http_req_rate(10s) peers cap",
		tableOut: "stick-table type ipv6 size 1k expire 30s store gpt(3) peers cap",
	}
	return func(p lab.CustomParams) ([]byte, error) {
		if len(p.Binds) != 2 {
			return nil, fmt.Errorf("want two binds, got %d", len(p.Binds))
		}
		lines := []string{
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
		}
		for _, name := range tables {
			lines = append(lines, "backend "+name, "    "+stores[name])
		}
		lines = append(lines,
			"frontend lab",
			"    mode http",
			"    bind "+p.Binds[1],
			"    http-request track-sc0 req.hdr_ip("+lab.ClientHeader+"),ipmask(32,64) table "+tableIn,
			"    http-request return status 200",
			"")
		return []byte(strings.Join(lines, "\n")), nil
	}
}

// closedAddr returns a loopback address nothing listens on, for the
// configured address of the test peer: HAProxy's own connection attempts
// are refused, and the test peer dials HAProxy instead.
func closedAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

type liveNode struct {
	t      *testing.T
	h      *lab.Custom
	config string
	client *lab.Client
}

func startNode(t *testing.T, name string, tables []string) *liveNode {
	t.Helper()
	cfg := tableConfig(closedAddr(t), tables)
	h := labtest.StartCustom(t, lab.CustomOptions{Name: name, Listeners: 2, Config: cfg})
	rendered, err := cfg(lab.CustomParams{Socket: "<runtime socket>", Binds: []string{"fd@3", "fd@4"}})
	if err != nil {
		t.Fatal(err)
	}
	client, err := lab.NewClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdle)
	return &liveNode{t: t, h: h, config: string(rendered), client: client}
}

// traffic sends n requests with the synthetic client address ip.
func (n *liveNode) traffic(c *peertest.Capture, ip string, count int) {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c.Note("traffic: %d requests from %s", count, ip)
	if _, err := n.client.SendN(ctx, n.h.Node.Listeners[1], ip, count); err != nil {
		n.t.Fatalf("traffic from %s: %v", ip, err)
	}
}

// runtime runs cmd and records its reply as "reply: " notes.
func (n *liveNode) runtime(c *peertest.Capture, cmd string) {
	n.t.Helper()
	reply := peertest.Runtime(n.t, n.h, c, cmd)
	for line := range strings.SplitSeq(reply, "\n") {
		if strings.TrimSpace(line) != "" {
			c.Note("%s%s", replyPrefix, line)
		}
	}
}

// session is a test peer connected to a node's peers bind, with its own
// inbound table namespace.
type session struct {
	t  *testing.T
	l  *peertest.Conn
	in *peermsg.Inbound
}

func (n *liveNode) connect(c *peertest.Capture) *session {
	n.t.Helper()
	l := peertest.Dial(n.t, n.h.Node.Listeners[0], c)
	hello, err := peerwire.AppendHello(nil, peerwire.Hello{
		Version: peerwire.ProtocolVersion, RemotePeer: livePeer, LocalPeer: agentPeer,
		PID: uint32(os.Getpid()), RelativePID: 1,
	})
	if err != nil {
		n.t.Fatal(err)
	}
	l.Send(hello)
	status, err := peerwire.ParseStatusLine(l.Line())
	if err != nil || status != peerwire.StatusSucceeded {
		n.t.Fatalf("status %d, %v", status, err)
	}
	return &session{t: n.t, l: l, in: peermsg.NewInbound(8)}
}

// waitLimit bounds a whole wait in until. HAProxy's heartbeats would
// otherwise keep restarting a per-read timeout forever.
const waitLimit = 15 * time.Second

// next reads and decodes one message by deadline, failing with what was
// awaited. HAProxy's resync requests are answered at once with
// "finished", as an empty peer would.
func (s *session) next(what string, deadline time.Time) peermsg.Message {
	s.t.Helper()
	f, err := s.l.FrameBy(deadline)
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.t.Fatalf("waiting for %s: HAProxy closed the connection", what)
		}
		s.t.Fatalf("waiting for %s: %v", what, err)
	}
	m, err := s.in.Decode(f, time.Now())
	if err != nil {
		s.t.Fatalf("decode %v: %v", f, err)
	}
	if c, ok := m.(peermsg.Control); ok && c.Type == peerwire.ControlResyncRequest {
		s.l.Capture().Note("answering HAProxy's resync request")
		s.l.Send(control(peerwire.ControlResyncFinished))
	}
	return m
}

// until reads messages until done reports true.
func (s *session) until(what string, done func(peermsg.Message) bool) {
	s.t.Helper()
	s.l.Capture().Note("waiting for %s", what)
	deadline := time.Now().Add(waitLimit)
	for {
		if done(s.next(what, deadline)) {
			return
		}
	}
}

// updatesFor returns a predicate that is satisfied once every key in keys
// has had an update of kind timed in table.
func updatesFor(table string, timed bool, keys ...peermsg.Key) func(peermsg.Message) bool {
	pending := map[peermsg.Key]bool{}
	for _, k := range keys {
		pending[k] = true
	}
	return func(m peermsg.Message) bool {
		if u, ok := m.(peermsg.UpdateMessage); ok && u.Table == table && u.Update.Timed == timed {
			delete(pending, u.Update.Key)
		}
		return len(pending) == 0
	}
}

// allOf returns a predicate satisfied once each of preds has been.
func allOf(preds ...func(peermsg.Message) bool) func(peermsg.Message) bool {
	done := make([]bool, len(preds))
	return func(m peermsg.Message) bool {
		all := true
		for i, p := range preds {
			done[i] = done[i] || p(m)
			all = all && done[i]
		}
		return all
	}
}

func isControl(typ peerwire.MessageType) func(peermsg.Message) bool {
	return func(m peermsg.Message) bool {
		c, ok := m.(peermsg.Control)
		return ok && c.Type == typ
	}
}

func isAck(m peermsg.Message) bool {
	_, ok := m.(peermsg.AckMessage)
	return ok
}

func control(typ peerwire.MessageType) []byte {
	return []byte{byte(peerwire.ClassControl), byte(typ)}
}

func (s *session) send(b []byte, err error) {
	s.t.Helper()
	if err != nil {
		s.t.Fatal(err)
	}
	s.l.Send(b)
}

func (s *session) finish() *peertest.Capture {
	s.t.Helper()
	s.l.Send(control(peerwire.ControlHeartbeat))
	s.until("HAProxy's idle heartbeat", isControl(peerwire.ControlHeartbeat))
	return s.l.Finish()
}

// TestLiveTableCapture records fresh table exchanges with the stock
// HAProxy named by HTA_HAPROXY, checks them exactly as committed fixtures
// are checked, and optionally writes them as fixtures.
func TestLiveTableCapture(t *testing.T) {
	push, pushCfg := capturePush(t)
	a, b, collCfg := captureCollision(t)
	dir := os.Getenv(EnvCaptureDir)
	t.Run(push.Get("case"), func(t *testing.T) {
		verifyPush(t, push, wantAcks(push.Get("haproxy-version"), hostLittleEndian()))
	})
	t.Run("collision", func(t *testing.T) { verifyCollision(t, a, b) })
	if dir == "" {
		return
	}
	for _, w := range []struct {
		c   *peertest.Capture
		cfg string
	}{{push, pushCfg}, {a, collCfg[0]}, {b, collCfg[1]}} {
		version, _, _ := strings.Cut(w.c.Get("haproxy-version"), "-")
		out := filepath.Join(dir, version, w.c.Get("case")+".txt")
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(out, w.c.Encode(comments(w.cfg)), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", out)
	}
}

func comments(config string) []string {
	return peertest.Comments([]string{
		"Fresh capture from stock HAProxy; generated by TestLiveTableCapture",
		"(internal/peermsg/capture_live_test.go) via `make peerwire-captures`.",
		"Do not edit. rx = HAProxy to test peer, tx = test peer to HAProxy.",
		"HAProxy configuration (listeners inherited as fd@3 and fd@4):",
	}, config)
}

// capturePush records HAProxy's push of tables filled by real traffic,
// its timed resync, a live update, and its handling of output updates
// encoded by this package.
func capturePush(t *testing.T) (*peertest.Capture, string) {
	n := startNode(t, livePeer, []string{tableIn, tableOut})
	c := peertest.NewCapture(casePush, "real traffic, push, timed resync, live update, and encoded output", n.h)
	for _, tr := range pushTraffic {
		n.traffic(c, tr.ip, tr.count)
	}
	peertest.RuntimeSilent(t, n.h, c, fmt.Sprintf("set table %s key %s data.gpt[0] %d data.gpt[2] %d",
		tableOut, keyNative, presetGPT[0], presetGPT[2]))
	n.runtime(c, "show table "+tableIn)
	n.runtime(c, "show table "+tableOut)

	s := n.connect(c)
	s.until("the push of every entry", allOf(
		updatesFor(tableIn, false, keyNative, keyMapped, keyOther), updatesFor(tableOut, false, keyNative)))

	c.Note("requesting a full resync")
	s.l.Send(control(peerwire.ControlResyncRequest))
	s.until("timed resync of every entry", updatesFor(tableIn, true, keyNative, keyMapped, keyOther))
	s.until("resync finished", isControl(peerwire.ControlResyncFinished))
	s.l.Send(control(peerwire.ControlResyncConfirm))
	n.runtime(c, "show table "+tableIn)

	n.traffic(c, otherClient, 1)
	s.until("the live update", updatesFor(tableIn, false, keyOther))
	n.runtime(c, "show table "+tableIn)

	for _, step := range outputSteps(t) {
		c.Note("sending %s", step.what)
		s.send(step.msg, nil)
		if step.ack {
			s.until("ack", isAck)
			// Read the entry at once: the timed one lives only 4 s, so a
			// read after later waits could find it legitimately expired.
			n.runtime(c, outEntryRead(step.key))
		}
	}
	n.runtime(c, "show peers")
	return s.finish(), n.config
}

// captureCollision records two sources that both use table ID 1, for
// different tables, and give the same key different counts.
func captureCollision(t *testing.T) (a, b *peertest.Capture, cfgs [2]string) {
	var out [2]*peertest.Capture
	for i, src := range collisionSources {
		n := startNode(t, src.node, src.tables)
		c := peertest.NewCapture(src.caseName, "collision source "+src.node, n.h)
		n.traffic(c, nativeClient, src.count)
		peertest.RuntimeSilent(t, n.h, c, fmt.Sprintf("set table %s key %s data.gpt[1] %d", tableOut, keyNative, src.count))
		n.runtime(c, "show table "+tableIn)
		s := n.connect(c)
		s.until("the push of both tables", allOf(
			updatesFor(tableIn, false, keyNative), updatesFor(tableOut, false, keyNative)))
		out[i] = s.finish()
		cfgs[i] = n.config
	}
	return out[0], out[1], cfgs
}
