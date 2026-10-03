package lab_test

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
)

// Environment for the re-executed child test.
const (
	envChildMode  = "HTA_LAB_CHILD_MODE"
	envChildState = "HTA_LAB_CHILD_STATE"
	envChildBase  = "HTA_LAB_CHILD_BASE"
)

// childState is what the child reports about the lab it owns, so that the
// parent can prove every piece of it is gone afterwards.
type childState struct {
	Dir     string   `json:"dir"`
	HAProxy string   `json:"haproxy"`
	PIDs    []int    `json:"pids"`
	Addrs   []string `json:"addrs"`
	Sockets []string `json:"sockets"`
}

// TestCleanupChild is the body run in a child process by
// TestCleanupAfterMidTestFailure; it is skipped in a normal run.
func TestCleanupChild(t *testing.T) {
	mode := os.Getenv(envChildMode)
	if mode == "" {
		t.Skip("runs only as a child of TestCleanupAfterMidTestFailure")
	}
	// Cleanups run last-registered first, so this check runs after the
	// lab's Close and before the child exits. It proves Close itself
	// stopped everything; otherwise the parent-death signal would hide a
	// broken Close by killing HAProxy when the child exits.
	var st childState
	t.Cleanup(func() {
		if st.PIDs == nil {
			return
		}
		if left := leftovers(st); len(left) > 0 {
			killLeft(st)
			t.Errorf("%s %v", postCloseLeft, left)
			return
		}
		t.Log(postCloseOK)
	})
	l := labtest.Start(t, lab.Options{BaseDir: os.Getenv(envChildBase)})
	c := newClient(t, nil)
	for _, n := range l.Nodes {
		if _, err := c.SendN(t.Context(), n.LabAddr, "2001:db8::1", 1); err != nil {
			t.Fatalf("node %s not serving: %v", n.Name, err)
		}
	}
	st = labState(l)
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	path := os.Getenv(envChildState)
	if err := os.WriteFile(path+".tmp", b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "fatal":
		t.Fatal("deliberate mid-test failure")
	case "hang":
		// Wait to be SIGKILLed by the parent; no cleanup will run.
		time.Sleep(time.Minute)
		t.Fatal("parent never killed the child")
	default:
		t.Fatalf("unknown child mode %q", mode)
	}
}

// Markers the child prints from its post-Close check.
const (
	postCloseOK   = "post-close check: ok"
	postCloseLeft = "post-close leftovers:"
)

func labState(l *lab.Lab) childState {
	st := childState{Dir: l.Dir, HAProxy: l.Record.HAProxyPath, Addrs: []string{l.Responder.Addr()}}
	for _, n := range l.Nodes {
		st.PIDs = append(st.PIDs, n.PID())
		st.Addrs = append(st.Addrs, n.Addrs()...)
		st.Sockets = append(st.Sockets, n.Socket)
	}
	return st
}

// TestCloseStopsNodes checks in-process that Close itself, not process
// exit, stops every node and releases every listener and socket.
func TestCloseStopsNodes(t *testing.T) {
	l, err := lab.Start(t.Context(), lab.Options{HAProxy: labtest.HAProxy(t)})
	if err != nil {
		t.Fatal(err)
	}
	st := labState(l)
	if len(st.PIDs) != len(lab.DefaultNodes) || len(leftovers(st)) != len(st.PIDs)+len(st.Addrs)+len(st.Sockets) {
		_ = l.Close()
		t.Fatalf("lab not fully up before Close: %+v, live %v", st, leftovers(st))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if left := leftovers(st); len(left) > 0 {
		killLeft(st)
		t.Fatalf("resources alive after Close returned: %v", left)
	}
	for _, n := range l.Nodes {
		if !n.Exited() {
			t.Errorf("node %s not reported exited after Close", n.Name)
		}
	}
	if _, err := os.Stat(st.Dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("lab dir %s still exists after Close: %v", st.Dir, err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestCleanupAfterMidTestFailure proves that a lab leaves no owned
// processes, listeners, or sockets behind when a test fails mid-run
// (cleanup runs, and the child verifies the result before it exits) and
// when the test process is killed outright (cleanup cannot run; the
// parent-death signal must stop HAProxy).
func TestCleanupAfterMidTestFailure(t *testing.T) {
	bin := labtest.HAProxy(t)
	for _, mode := range []string{"fatal", "hang"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			base := t.TempDir()
			statePath := filepath.Join(base, "state.json")

			cmd := exec.CommandContext(t.Context(), os.Args[0],
				"-test.run=^TestCleanupChild$", "-test.count=1", "-test.v")
			cmd.Env = append(os.Environ(),
				envChildMode+"="+mode,
				envChildState+"="+statePath,
				envChildBase+"="+base,
				labtest.EnvHAProxy+"="+bin,
				labtest.EnvRecordDir+"=",
			)
			var out strings.Builder
			cmd.Stdout, cmd.Stderr = &out, &out
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}

			var st childState
			if mode == "hang" {
				st = waitState(t, statePath, cmd)
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				_ = cmd.Wait()
			} else {
				err := cmd.Wait()
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) || !strings.Contains(out.String(), "deliberate mid-test failure") {
					t.Fatalf("child did not fail as intended: %v\n%s", err, out.String())
				}
				o := out.String()
				if strings.Contains(o, "close lab:") || strings.Contains(o, postCloseLeft) ||
					!strings.Contains(o, postCloseOK) {
					t.Fatalf("lab cleanup in the child did not stop everything:\n%s", o)
				}
				st = readState(t, statePath)
			}
			if len(st.PIDs) != len(lab.DefaultNodes) {
				t.Fatalf("child reported pids %v\n%s", st.PIDs, out.String())
			}

			assertGone(t, st)
			if mode == "fatal" {
				if _, err := os.Stat(st.Dir); !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("lab dir %s still exists after cleanup: %v", st.Dir, err)
				}
			}
		})
	}
}

func readState(t *testing.T, path string) childState {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("child state: %v", err)
	}
	var st childState
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatalf("child state: %v", err)
	}
	return st
}

func waitState(t *testing.T, path string, cmd *exec.Cmd) childState {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return readState(t, path)
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	t.Fatal("child never reported lab state")
	return childState{}
}

func assertGone(t *testing.T, st childState) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		left := leftovers(st)
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			killLeft(st)
			t.Fatalf("owned resources survived: %v", left)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// killLeft kills surviving HAProxy processes so a failing run does not
// leak them into later runs.
func killLeft(st childState) {
	for _, pid := range st.PIDs {
		if running(pid, st.HAProxy) {
			if p, err := os.FindProcess(pid); err == nil {
				_ = p.Kill()
			}
		}
	}
}

func leftovers(st childState) []string {
	var left []string
	dial := func(network, addr string) bool {
		d := net.Dialer{Timeout: 200 * time.Millisecond}
		c, err := d.DialContext(context.Background(), network, addr)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}
	for _, pid := range st.PIDs {
		if running(pid, st.HAProxy) {
			left = append(left, "pid "+strconv.Itoa(pid))
		}
	}
	for _, addr := range st.Addrs {
		if dial("tcp", addr) {
			left = append(left, "listener "+addr)
		}
	}
	for _, sock := range st.Sockets {
		if dial("unix", sock) {
			left = append(left, "socket "+sock)
		}
	}
	return left
}

// running reports whether pid is a live (non-zombie) process executing the
// given binary. A reused pid running something else does not count.
func running(pid int, binary string) bool {
	proc := "/proc/" + strconv.Itoa(pid)
	stat, err := os.ReadFile(proc + "/stat")
	if err != nil {
		return false
	}
	// The state field follows the parenthesised command name.
	if i := strings.LastIndexByte(string(stat), ')'); i >= 0 && i+2 < len(stat) && stat[i+2] == 'Z' {
		return false
	}
	exe, err := os.Readlink(proc + "/exe")
	return err == nil && strings.TrimSuffix(exe, " (deleted)") == binary
}
