// Package lab runs a reproducible local HAProxy lab: isolated stock HAProxy
// processes on loopback, a counting HTTP responder, and runtime-socket
// inspection of each process's local stick tables.
//
// Every listener is a socket the harness opens itself and hands to HAProxy
// as an inherited descriptor (bind fd@N), so ports are never probed and
// released. The harness signals only processes it started, and on Linux a
// parent-death signal kills them even if the harness itself is killed.
package lab

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

// DefaultNodes are the node names Start uses when Options.Nodes is empty.
var DefaultNodes = []string{"a", "b"}

// DefaultPeriod is the rate period used when Options.Period is zero.
const DefaultPeriod = 10 * time.Second

// HAProxy binds a stats socket first as "<path>.<pid>.tmp", which must fit
// in sun_path (108 bytes on Linux) with its NUL terminator, then renames it.
// PIDs on Linux have at most 7 digits (PID_MAX_LIMIT is 4194304), so the
// longest path that binds whatever PID HAProxy gets is 95 bytes.
const (
	sunPathLen    = 108
	maxPIDDigits  = 7
	maxSocketPath = sunPathLen - 1 - len(".") - maxPIDDigits - len(".tmp")
)

const (
	readyTimeout = 10 * time.Second
	stopTimeout  = 5 * time.Second
)

// Options configures Start.
type Options struct {
	// HAProxy is the path of the stock haproxy executable. Required.
	HAProxy string
	// Period is the http_req_rate period. Tables expire entries after
	// ExpiryFactor periods. Zero means DefaultPeriod.
	Period time.Duration
	// BaseDir holds the lab's temporary directory. Empty means
	// os.TempDir(). Socket paths must stay within HAProxy's limit.
	BaseDir string
	// Nodes names the HAProxy processes. Empty means DefaultNodes.
	Nodes []string
}

// Lab is a running set of independent HAProxy nodes sharing one responder.
// Nodes are not peered with each other or with anything else.
type Lab struct {
	// Dir is the lab's private temporary directory, removed by Close.
	Dir string
	// Period is the configured rate period.
	Period time.Duration
	// Responder is the shared origin server and traffic ground truth.
	Responder *Responder
	// Nodes are the running HAProxy processes in Options.Nodes order.
	Nodes []*Node
	// Record identifies the toolchain and binary used for this run.
	Record RunRecord

	closeOnce sync.Once
	closeErr  error
}

// Node is one owned HAProxy process.
type Node struct {
	// Name is the node's lab name, also sent upstream in NodeHeader.
	Name string
	// LabAddr is the isolated lab listener (127.0.0.1:port).
	LabAddr string
	// ProdAddr4 is the production-style listener on 127.0.0.1.
	ProdAddr4 string
	// ProxyAddr is the PROXY-protocol listener (127.0.0.1:port). Every
	// connection must start with a PROXY header; see ProxyClient.
	ProxyAddr string
	// ProdAddr6 is the production-style listener on [::1], or empty when
	// IPv6 loopback is unavailable on this host.
	ProdAddr6 string
	// Socket is the admin-level runtime API socket.
	Socket string
	// ConfigPath is the generated configuration file.
	ConfigPath string
	// LogPath receives HAProxy's stdout and stderr.
	LogPath string

	cmd     *exec.Cmd
	exited  chan struct{}
	waitErr error
}

// Start creates the lab directory, starts the responder, and starts one
// HAProxy process per node, waiting until each answers on its runtime
// socket. On error everything already started is torn down.
func Start(ctx context.Context, opts Options) (_ *Lab, err error) {
	if opts.HAProxy == "" {
		return nil, errors.New("lab: no haproxy executable given")
	}
	if opts.Period == 0 {
		opts.Period = DefaultPeriod
	}
	if opts.Period < time.Second || opts.Period%time.Millisecond != 0 {
		return nil, fmt.Errorf("lab: period %v must be whole milliseconds and at least 1s", opts.Period)
	}
	nodes := opts.Nodes
	if len(nodes) == 0 {
		nodes = DefaultNodes
	}
	if err = validateNodeNames(nodes); err != nil {
		return nil, err
	}
	record, err := NewRunRecord(ctx, opts.HAProxy, opts.Period)
	if err != nil {
		return nil, err
	}

	base := opts.BaseDir
	if base == "" {
		base = os.TempDir()
	}
	dir, err := os.MkdirTemp(base, "htalab-")
	if err != nil {
		return nil, fmt.Errorf("lab: %w", err)
	}
	l := &Lab{Dir: dir, Period: opts.Period, Record: record}
	defer func() {
		if err != nil {
			err = errors.Join(err, l.Close())
		}
	}()

	if l.Responder, err = StartResponder(); err != nil {
		return nil, err
	}
	for _, name := range nodes {
		n, startErr := l.startNode(ctx, record.HAProxyPath, name)
		if n != nil {
			l.Nodes = append(l.Nodes, n)
		}
		if startErr != nil {
			return nil, startErr
		}
	}
	return l, nil
}

func validateNodeNames(names []string) error {
	for i, name := range names {
		if name == "" || strings.Trim(name, "abcdefghijklmnopqrstuvwxyz0123456789") != "" {
			return fmt.Errorf("lab: node name %q must be lowercase alphanumeric", name)
		}
		if slices.Contains(names[:i], name) {
			return fmt.Errorf("lab: duplicate node name %q", name)
		}
	}
	return nil
}

// Node returns the node with the given name, or nil.
func (l *Lab) Node(name string) *Node {
	for _, n := range l.Nodes {
		if n.Name == name {
			return n
		}
	}
	return nil
}

// Close stops every node (SIGTERM, then SIGKILL after a grace period),
// stops the responder, and removes the lab directory. It is safe to call
// more than once.
func (l *Lab) Close() error {
	l.closeOnce.Do(func() {
		var errs []error
		for _, n := range l.Nodes {
			errs = append(errs, n.stop())
		}
		if l.Responder != nil {
			errs = append(errs, l.Responder.Close())
		}
		errs = append(errs, os.RemoveAll(l.Dir))
		l.closeErr = errors.Join(errs...)
	})
	return l.closeErr
}

type inheritedListener struct {
	addr string
	file *os.File
}

func listenInherited(ctx context.Context, network, address string) (inheritedListener, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, network, address)
	if err != nil {
		return inheritedListener{}, err
	}
	defer func() { _ = ln.Close() }()
	tl, ok := ln.(*net.TCPListener)
	if !ok {
		return inheritedListener{}, fmt.Errorf("listen %s: unexpected listener %T", address, ln)
	}
	f, err := tl.File()
	if err != nil {
		return inheritedListener{}, fmt.Errorf("listen %s: %w", address, err)
	}
	return inheritedListener{addr: ln.Addr().String(), file: f}, nil
}

func (l *Lab) startNode(ctx context.Context, haproxy, name string) (*Node, error) {
	n := &Node{
		Name:       name,
		Socket:     filepath.Join(l.Dir, name+".sock"),
		ConfigPath: filepath.Join(l.Dir, name+".cfg"),
		LogPath:    filepath.Join(l.Dir, name+".log"),
	}
	if len(n.Socket) > maxSocketPath {
		return nil, fmt.Errorf("lab: runtime socket path %s exceeds HAProxy's %d-byte limit; use a shorter BaseDir or TMPDIR",
			n.Socket, maxSocketPath)
	}

	var files []*os.File
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()
	inherit := func(network, address string) (string, string, error) {
		il, err := listenInherited(ctx, network, address)
		if err != nil {
			return "", "", err
		}
		files = append(files, il.file)
		// ExtraFiles[i] becomes descriptor 3+i in the child.
		return il.addr, fmt.Sprintf("fd@%d", 2+len(files)), nil
	}

	labAddr, labBind, err := inherit("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("lab: node %s lab listener: %w", name, err)
	}
	prod4, prod4Bind, err := inherit("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("lab: node %s prod listener: %w", name, err)
	}
	proxyAddr, proxyBind, err := inherit("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("lab: node %s proxy listener: %w", name, err)
	}
	prodBinds := []string{prod4Bind}
	prod6, prod6Bind, err := inherit("tcp6", "[::1]:0")
	if err == nil {
		prodBinds = append(prodBinds, prod6Bind)
	} else {
		prod6 = ""
	}
	n.LabAddr, n.ProdAddr4, n.ProdAddr6, n.ProxyAddr = labAddr, prod4, prod6, proxyAddr

	cfg, err := renderConfig(configParams{
		Node:        name,
		Socket:      n.Socket,
		LabBind:     labBind,
		ProdBinds:   prodBinds,
		ProxyBind:   proxyBind,
		Responder:   l.Responder.Addr(),
		PeriodMS:    millis(l.Period),
		ExpireMS:    millis(ExpiryFactor * l.Period),
		LabTable:    LabTable,
		ProdTable:   ProdTable,
		ProxyTable:  ProxyTable,
		ClientHdr:   ClientHeader,
		NodeHdr:     NodeHeader,
		ListenerHdr: ListenerHeader,
		KeyHdr:      KeyHeader,
		LookupHdr:   LookupHeader,
	})
	if err != nil {
		return nil, err
	}
	if err = os.WriteFile(n.ConfigPath, cfg, 0o600); err != nil {
		return nil, fmt.Errorf("lab: %w", err)
	}
	logFile, err := os.OpenFile(n.LogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("lab: %w", err)
	}
	defer func() { _ = logFile.Close() }()

	//nolint:noctx,gosec // G204: running the operator-chosen haproxy is the
	// point; its lifetime is owned by Close rather than a context.
	cmd := exec.Command(haproxy, "-db", "-f", n.ConfigPath)
	cmd.Dir = l.Dir
	cmd.Env = []string{}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.ExtraFiles = files
	if err := startOwned(cmd); err != nil {
		return nil, fmt.Errorf("lab: start node %s: %w", name, err)
	}
	n.cmd = cmd
	n.exited = make(chan struct{})
	go func() {
		n.waitErr = cmd.Wait()
		close(n.exited)
	}()

	if err := n.waitReady(ctx); err != nil {
		return n, fmt.Errorf("lab: node %s: %w\n%s", name, err, n.logTail())
	}
	return n, nil
}

func (n *Node) waitReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-n.exited:
			return fmt.Errorf("haproxy exited during startup: %w", n.waitErr)
		case <-ctx.Done():
			return fmt.Errorf("haproxy not ready: %w", ctx.Err())
		case <-tick.C:
		}
		reply, err := RuntimeCommand(ctx, n.Socket, "show info")
		if err == nil && strings.Contains(reply, "Pid:") {
			return nil
		}
	}
}

func (n *Node) logTail() string {
	b, err := os.ReadFile(n.LogPath)
	if err != nil {
		return ""
	}
	const limit = 4096
	if len(b) > limit {
		b = b[len(b)-limit:]
	}
	return string(b)
}

// PID returns the HAProxy process ID, or 0 if it was never started.
func (n *Node) PID() int {
	if n.cmd == nil || n.cmd.Process == nil {
		return 0
	}
	return n.cmd.Process.Pid
}

// Exited reports whether the process has exited.
func (n *Node) Exited() bool {
	select {
	case <-n.exited:
		return true
	default:
		return false
	}
}

// Runtime sends one runtime API command to this node.
func (n *Node) Runtime(ctx context.Context, command string) (string, error) {
	return RuntimeCommand(ctx, n.Socket, command)
}

// ShowTable dumps and parses one of this node's stick tables.
func (n *Node) ShowTable(ctx context.Context, table string) (Table, error) {
	reply, err := n.Runtime(ctx, "show table "+table)
	if err != nil {
		return Table{}, err
	}
	return ParseTable(reply)
}

// Addrs returns every listener address the node owns.
func (n *Node) Addrs() []string {
	addrs := []string{n.LabAddr, n.ProdAddr4, n.ProxyAddr}
	if n.ProdAddr6 != "" {
		addrs = append(addrs, n.ProdAddr6)
	}
	return addrs
}

func (n *Node) stop() error {
	if n.cmd == nil || n.Exited() {
		return nil
	}
	_ = n.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-n.exited:
		return nil
	case <-time.After(stopTimeout):
	}
	if err := n.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("lab: kill node %s: %w", n.Name, err)
	}
	<-n.exited
	return nil
}
