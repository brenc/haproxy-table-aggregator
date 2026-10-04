package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// envChild makes the test binary run main instead of the tests, so the
// daemon can be run as a real process and signalled.
const envChild = "HTAD_TEST_RUN_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(envChild) == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func writeConfig(t *testing.T, doc string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "htad.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestUsageErrors(t *testing.T) {
	good := writeConfig(t, `{"local_peer": "agg", "insecure_plaintext_loopback_lab": true,
		"listen": "127.0.0.1:0", "sources": [{"name": "a"}], "tables": [{"name": "t", "period": "1s"}]}`)
	plain := writeConfig(t, `{"local_peer": "agg", "listen": "127.0.0.1:0",
		"sources": [{"name": "a"}], "tables": [{"name": "t", "period": "1s"}]}`)
	for name, args := range map[string][]string{
		"no config":         nil,
		"extra argument":    {"-config", good, "x"},
		"bad level":         {"-config", good, "-log-level", "loud"},
		"missing file":      {"-config", filepath.Join(t.TempDir(), "none.json")},
		"plaintext not set": {"-config", plain},
	} {
		t.Run(name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			if err := run(context.Background(), args, &out, &errOut); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// lines parses the daemon's JSON event lines.
func lines(t *testing.T, out string) []map[string]any {
	t.Helper()
	var evs []map[string]any
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("event line %q: %v", sc.Text(), err)
		}
		evs = append(evs, m)
	}
	return evs
}

func types(evs []map[string]any) []string {
	var out []string
	for _, e := range evs {
		out = append(out, fmt.Sprint(e["type"]))
	}
	return out
}

// TestRunEvents drives the daemon in-process with a scripted source and
// checks the JSON event stream and a clean shutdown.
func TestRunEvents(t *testing.T) {
	cfg := writeConfig(t, `{"local_peer": "agg", "insecure_plaintext_loopback_lab": true,
		"listen": "127.0.0.1:0", "sources": [{"name": "a"}],
		"tables": [{"name": "t_in", "period": "10s"}]}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out, errOut syncBuffer
	addr := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- runWith(ctx, []string{"-config", cfg}, &out, &errOut, func(m *sources.Manager) {
			addr <- m.Addr().String()
		})
	}()
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", <-addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	hello, _ := peerwire.AppendHello(nil, peerwire.Hello{
		Version: peerwire.ProtocolVersion, RemotePeer: "agg", LocalPeer: "a", PID: 7, RelativePID: 1,
	})
	def := peermsg.Definition{
		Name: "t_in", KeyType: peermsg.KeyTypeIPv6, Expiry: 30000,
		Fields: []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: 10000}},
	}
	defMsg, err := peermsg.AppendDefinition(nil, 1, def)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := peermsg.KeyFromAddr(netip.MustParseAddr("2001:db8::"))
	upd, err := peermsg.AppendUpdate(nil, def, peermsg.Update{ID: 9, ExplicitID: true, Key: key, Values: []peermsg.Value{
		{Type: peermsg.DataHTTPReqCnt, Uint: 4},
		{Type: peermsg.DataHTTPReqRate, Freq: peermsg.FreqCounter{Age: 100, Curr: 4}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	finished, _ := peerwire.AppendFrame(nil, peerwire.ClassControl, peerwire.ControlResyncFinished, nil)
	msg := append(append(append(hello, defMsg...), upd...), finished...)
	if _, err := conn.Write(msg); err != nil {
		t.Fatal(err)
	}
	// Wait for the acknowledgement: status, resync request, confirm, ack.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	got := make([]byte, 0, 64)
	ackFrame, _ := peermsg.AppendAck(nil, 1, 9)
	for !bytes.Contains(got, ackFrame) {
		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("read %x: %v", got, err)
		}
		got = append(got, buf[:n]...)
	}
	stopAt := time.Now()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v\n%s", err, errOut.String())
	}
	t.Logf("shutdown took %v", time.Since(stopAt))
	evs := lines(t, out.String())
	want := []string{"session_up", "table_defined", "entry_updated", "sync_finished", "session_down"}
	if fmt.Sprint(types(evs)) != fmt.Sprint(want) {
		t.Fatalf("events %v, want %v\n%s", types(evs), want, out.String())
	}
	u, ok := evs[2]["update"].(map[string]any)
	if !ok {
		t.Fatalf("update line %v", evs[2])
	}
	if u["key"] != "2001:db8::" || u["http_req_cnt"] != 4.0 || u["id"] != 9.0 || u["lifetime_ms"] != 30000.0 {
		t.Fatalf("update line %v", evs[2])
	}
	if !strings.Contains(errOut.String(), "stopped") {
		t.Fatalf("log:\n%s", errOut.String())
	}
}

// TestLiveSIGTERM runs htad as a process against the two stock HAProxy lab
// nodes, one session in each direction, and checks that SIGTERM closes
// both sessions, reports their ends, and exits 0 promptly.
func TestLiveSIGTERM(t *testing.T) {
	// Reserve addresses nothing listens on. The daemon's is off
	// 127.0.0.1 so that no connection's ephemeral source port takes it
	// before the daemon binds it.
	free := func(ip string) string {
		var lc net.ListenConfig
		ln, err := lc.Listen(context.Background(), "tcp4", ip+":0")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ln.Close() }()
		return ln.Addr().String()
	}
	listenAddr, closedAddr := free("127.0.0.2"), free("127.0.0.1")
	l := labtest.Start(t, lab.Options{Aggregator: &lab.Aggregator{
		Name: "agg", NodeAddrs: map[string]string{"a": listenAddr, "b": closedAddr},
	}})
	a, b := l.Node("a"), l.Node("b")
	cfg := writeConfig(t, fmt.Sprintf(`{"local_peer": "agg", "insecure_plaintext_loopback_lab": true,
		"listen": %q, "sources": [{"name": "a"}, {"name": "b", "address": %q}],
		"tables": [{"name": %q, "period": "10s"}], "idle_timeout": "4s"}`, listenAddr, b.PeersAddr, lab.LabTable))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-config", cfg)
	cmd.Env = append(os.Environ(), envChild+"=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var errOut syncBuffer
	cmd.Stderr = &errOut
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	evCh := make(chan map[string]any, 64)
	go func() {
		defer close(evCh)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			var m map[string]any
			if json.Unmarshal(sc.Bytes(), &m) == nil {
				evCh <- m
			}
		}
	}()
	var evs []map[string]any
	upsFor := map[string]bool{}
	timeout := time.After(20 * time.Second)
	for len(upsFor) < 2 {
		select {
		case e, ok := <-evCh:
			if !ok {
				t.Fatalf("daemon exited early: %v\n%s", cmd.Wait(), errOut.String())
			}
			evs = append(evs, e)
			if e["type"] == "session_up" {
				upsFor[fmt.Sprint(e["source"])] = true
			}
		case <-timeout:
			t.Fatalf("sessions not up: %v\n%s", types(evs), errOut.String())
		}
	}
	for _, n := range []*lab.Node{a, b} {
		waitEstablished(t, n, true)
	}

	start := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	for e := range evCh {
		evs = append(evs, e)
	}
	werr := cmd.Wait()
	elapsed := time.Since(start)
	if werr != nil {
		t.Fatalf("exit after SIGTERM: %v\n%s", werr, errOut.String())
	}
	if elapsed > 3*time.Second {
		t.Fatalf("shutdown took %v", elapsed)
	}
	downs := map[string]bool{}
	for _, e := range evs {
		if e["type"] == "session_down" {
			downs[fmt.Sprint(e["source"])] = true
		}
	}
	if !downs["a"] || !downs["b"] || !strings.Contains(errOut.String(), "stopped") {
		t.Fatalf("after SIGTERM: events %v\n%s", types(evs), errOut.String())
	}
	t.Logf("SIGTERM to exit 0 in %v; events %v\n%s", elapsed.Round(time.Millisecond), types(evs), errOut.String())
	for _, n := range []*lab.Node{a, b} {
		waitEstablished(t, n, false)
	}
}

// waitEstablished waits until node n has (or no longer has) an
// established session with the aggregator. After a close HAProxy dials
// again at once, so the peer can show as active while connecting; only
// the ESTA status says a session is established.
func waitEstablished(t *testing.T, n *lab.Node, established bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		reply, err := n.Runtime(ctx, "show peers")
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		var line string
		for l := range strings.SplitSeq(reply, "\n") {
			if strings.Contains(l, " id=agg(") {
				line = l
			}
		}
		if strings.Contains(line, "last_status=ESTA") == established {
			t.Logf("node %s: %s", n.Name, strings.TrimSpace(line))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s: established=%v not reached:\n%s", n.Name, established, reply)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// blockingWriter blocks every Write until released.
type blockingWriter struct{ release chan struct{} }

func (w blockingWriter) Write(p []byte) (int, error) {
	<-w.release
	return len(p), nil
}

// TestRunBlockedOutput checks that shutdown is bounded when stdout stops
// accepting writes: run returns an error instead of waiting forever.
func TestRunBlockedOutput(t *testing.T) {
	old := outputDrainTimeout
	outputDrainTimeout = 200 * time.Millisecond
	t.Cleanup(func() { outputDrainTimeout = old })
	cfg := writeConfig(t, `{"local_peer": "agg", "insecure_plaintext_loopback_lab": true,
		"listen": "127.0.0.1:0", "sources": [{"name": "a"}],
		"tables": [{"name": "t_in", "period": "10s"}]}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := blockingWriter{release: make(chan struct{})}
	defer close(out.release)
	var errOut syncBuffer
	addr := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- runWith(ctx, []string{"-config", cfg}, out, &errOut, func(m *sources.Manager) {
			addr <- m.Addr().String()
		})
	}()
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", <-addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	hello, _ := peerwire.AppendHello(nil, peerwire.Hello{
		Version: peerwire.ProtocolVersion, RemotePeer: "agg", LocalPeer: "a", PID: 7, RelativePID: 1,
	})
	if _, err := conn.Write(hello); err != nil {
		t.Fatal(err)
	}
	// Wait for the status line: SessionUp is then queued, and the writer
	// blocks on it.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 4)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "event output blocked") {
			t.Fatalf("run returned %v", err)
		}
		t.Logf("run returned after %v: %v", time.Since(start).Round(time.Millisecond), err)
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown blocked on stdout")
	}
}

// TestSIGTERMBlockedStdout runs htad as a process whose stdout pipe is
// never read and fills it with events: SIGTERM must still end the process
// promptly (with an error, since events were lost).
func TestSIGTERMBlockedStdout(t *testing.T) {
	t.Run("one signal", func(t *testing.T) { blockedStdout(t, false) })
	t.Run("second signal", func(t *testing.T) { blockedStdout(t, true) })
}

// blockedStdout runs the scenario; with second, a second SIGTERM follows
// the first and must terminate the process at once.
func blockedStdout(t *testing.T, second bool) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	cfg := writeConfig(t, fmt.Sprintf(`{"local_peer": "agg", "insecure_plaintext_loopback_lab": true,
		"listen": %q, "sources": [{"name": "a"}], "tables": [{"name": "t_in", "period": "10s"}]}`, addr))
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pr.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-config", cfg)
	cmd.Env = append(os.Environ(), envChild+"=1")
	cmd.Stdout = pw
	var errOut syncBuffer
	cmd.Stderr = &errOut
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = pw.Close()
	var conn net.Conn
	for deadline := time.Now().Add(5 * time.Second); ; {
		if conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", addr); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon not listening: %v\n%s", err, errOut.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer func() { _ = conn.Close() }()
	def := peermsg.Definition{
		Name: "t_in", KeyType: peermsg.KeyTypeIPv6, Expiry: 30000,
		Fields: []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: 10000}},
	}
	msg, _ := peerwire.AppendHello(nil, peerwire.Hello{
		Version: peerwire.ProtocolVersion, RemotePeer: "agg", LocalPeer: "a", PID: 7, RelativePID: 1,
	})
	if msg, err = peermsg.AppendDefinition(msg, 1, def); err != nil {
		t.Fatal(err)
	}
	key, _ := peermsg.KeyFromAddr(netip.MustParseAddr("2001:db8::"))
	for i := range 2000 {
		msg, err = peermsg.AppendUpdate(msg, def, peermsg.Update{
			ID: peermsg.UpdateID(i), ExplicitID: true, Key: key, Values: []peermsg.Value{
				{Type: peermsg.DataHTTPReqCnt, Uint: uint32(i)},
				{Type: peermsg.DataHTTPReqRate, Freq: peermsg.FreqCounter{Curr: 1}},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	go func() { _, _ = conn.Write(msg) }()
	time.Sleep(2 * time.Second) // the pipe fills and the writer blocks
	start := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if second {
		time.Sleep(200 * time.Millisecond)
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		werr := cmd.Wait()
		var ee *exec.ExitError
		ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
		if !errors.As(werr, &ee) || !ok || !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
			t.Fatalf("second SIGTERM: %v\n%s", werr, errOut.String())
		}
		t.Logf("second SIGTERM killed the process %v after the first", time.Since(start).Round(time.Millisecond))
		return
	}
	werr := cmd.Wait()
	elapsed := time.Since(start)
	var ee *exec.ExitError
	if !errors.As(werr, &ee) || ee.ExitCode() != 1 || !strings.Contains(errOut.String(), "event output blocked") {
		t.Fatalf("exit %v after %v\n%s", werr, elapsed, errOut.String())
	}
	if elapsed > outputDrainTimeout+3*time.Second {
		t.Fatalf("SIGTERM took %v", elapsed)
	}
	t.Logf("blocked stdout: SIGTERM to exit 1 in %v", elapsed.Round(time.Millisecond))
}

// TestMalformedConfigExits runs htad as a process with a syntactically
// broken configuration: it must exit 1 promptly, not hang in parsing.
func TestMalformedConfigExits(t *testing.T) {
	cfg := writeConfig(t, `{"local_peer": "agg", "insecure_plaintext_loopback_lab": true,
		"listen": "127.0.0.1:0", "sources": [{"name": "a"},], "tables": [{"name": "t", "period": "1s"}]}`)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-config", cfg)
	cmd.Env = append(os.Environ(), envChild+"=1")
	var errOut syncBuffer
	cmd.Stderr = &errOut
	start := time.Now()
	err := cmd.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 || ctx.Err() != nil ||
		!strings.Contains(errOut.String(), "invalid configuration") {
		t.Fatalf("exit %v after %v\n%s", err, time.Since(start), errOut.String())
	}
	t.Logf("exit 1 after %v: %s", time.Since(start).Round(time.Millisecond), strings.TrimSpace(errOut.String()))
}

// TestSignalledDuringStartup checks that a signal that arrived before
// startup finished ends run at once, without starting any session.
func TestSignalledDuringStartup(t *testing.T) {
	cfg := writeConfig(t, `{"local_peer": "agg", "insecure_plaintext_loopback_lab": true,
		"listen": "127.0.0.1:0", "sources": [{"name": "a"}], "tables": [{"name": "t", "period": "1s"}]}`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := false
	var out, errOut syncBuffer
	err := runWith(ctx, []string{"-config", cfg}, &out, &errOut, func(*sources.Manager) { started = true })
	if err != nil || started || out.String() != "" {
		t.Fatalf("run: %v, started %v, output %q", err, started, out.String())
	}
}
