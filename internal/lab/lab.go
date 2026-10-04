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
	"net/netip"
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
	// Aggregator, if non-nil, gives every node a peers section shared
	// only with the aggregator, attached to the three input tables and
	// the two output tables (OutputTable, MetaTable), and a probe
	// listener reading the output.
	Aggregator *Aggregator
}

// Aggregator describes the aggregator peer the lab nodes are configured
// with. Each node's own peer name is its node name.
type Aggregator struct {
	// Name is the aggregator's peer name.
	Name string
	// Addr is the address every node dials to reach the aggregator: a
	// loopback ip:port, since lab peers sessions are plaintext.
	Addr string
	// NodeAddrs overrides Addr per node name. A loopback address nothing
	// listens on keeps a node from ever dialing the aggregator itself.
	NodeAddrs map[string]string
	// OutputFirst names nodes whose configuration declares the output
	// tables before the input tables. HAProxy numbers a peers section's
	// tables in configuration order, so those nodes announce different
	// table IDs than the others.
	OutputFirst []string
}

func (a *Aggregator) addr(node string) string {
	if v, ok := a.NodeAddrs[node]; ok {
		return v
	}
	return a.Addr
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
	// PeersAddr is the node's peers bind (127.0.0.1:port) when the lab has
	// an aggregator, or empty.
	PeersAddr string
	// ProbeAddr is the output probe listener (127.0.0.1:port) when the lab
	// has an aggregator, or empty. See Client.Probe.
	ProbeAddr string
	// Listeners are the listener addresses of a custom node (see
	// StartCustom), in CustomOptions order. The lab listener fields above
	// are empty for a custom node.
	Listeners []string
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
	if agg := opts.Aggregator; agg != nil {
		if err = validateAggregator(agg, nodes); err != nil {
			return nil, err
		}
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
		n, startErr := l.startNode(ctx, record.HAProxyPath, name, opts.Aggregator)
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

func validateAggregator(agg *Aggregator, nodes []string) error {
	if agg.Name == "" || strings.ContainsAny(agg.Name, " \t\r\n#") {
		return fmt.Errorf("lab: aggregator peer name %q is not a plain word", agg.Name)
	}
	if slices.Contains(nodes, agg.Name) {
		return fmt.Errorf("lab: aggregator peer name %q is also a node name", agg.Name)
	}
	for _, name := range agg.OutputFirst {
		if !slices.Contains(nodes, name) {
			return fmt.Errorf("lab: aggregator OutputFirst names %q, which is not a node", name)
		}
	}
	for _, name := range nodes {
		// Lab peers sessions are plaintext, so they stay on loopback.
		ap, err := netip.ParseAddrPort(agg.addr(name))
		if err != nil || !ap.Addr().IsLoopback() || ap.Addr().Zone() != "" || ap.Port() == 0 {
			return fmt.Errorf("lab: aggregator address %q for node %s must be a loopback ip:port", agg.addr(name), name)
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

func (l *Lab) startNode(ctx context.Context, haproxy, name string, agg *Aggregator) (*Node, error) {
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
	var aggName, aggAddr, peersBind, probeBind string
	outputFirst := false
	if agg != nil {
		if n.PeersAddr, peersBind, err = inherit("tcp4", "127.0.0.1:0"); err != nil {
			return nil, fmt.Errorf("lab: node %s peers listener: %w", name, err)
		}
		if n.ProbeAddr, probeBind, err = inherit("tcp4", "127.0.0.1:0"); err != nil {
			return nil, fmt.Errorf("lab: node %s probe listener: %w", name, err)
		}
		aggName, aggAddr = agg.Name, agg.addr(name)
		outputFirst = slices.Contains(agg.OutputFirst, name)
	}

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
		AggName:     aggName,
		AggAddr:     aggAddr,
		PeersBind:   peersBind,
		OutputFirst: outputFirst,
		ProbeBind:   probeBind,
		OutTable:    OutputTable,
		MetaTable:   MetaTable,
		OutSlots:    OutputSlots,
		OutLimit:    OutputLimit,

		OutVersionHdr:  OutVersionHeader,
		OutRateHdr:     OutRateHeader,
		MetaVersionHdr: MetaVersionHeader,
	})
	if err != nil {
		return nil, err
	}
	if err := n.spawn(ctx, haproxy, l.Dir, cfg, files); err != nil {
		if n.cmd == nil {
			return nil, fmt.Errorf("lab: node %s: %w", name, err)
		}
		return n, fmt.Errorf("lab: node %s: %w", name, err)
	}
	return n, nil
}

// spawn writes cfg to n.ConfigPath, starts haproxy in the foreground with
// files as inherited descriptors 3, 4, ..., and waits until it answers on
// n.Socket. If the process started, n owns it even when spawn fails.
func (n *Node) spawn(ctx context.Context, haproxy, dir string, cfg []byte, files []*os.File) error {
	if err := os.WriteFile(n.ConfigPath, cfg, 0o600); err != nil {
		return err
	}
	logFile, err := os.OpenFile(n.LogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = logFile.Close() }()

	//nolint:noctx,gosec // G204: running the operator-chosen haproxy is the
	// point; its lifetime is owned by Close rather than a context.
	cmd := exec.Command(haproxy, "-db", "-f", n.ConfigPath)
	cmd.Dir = dir
	cmd.Env = []string{}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.ExtraFiles = files
	if err := startOwned(cmd); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	n.cmd = cmd
	n.exited = make(chan struct{})
	go func() {
		n.waitErr = cmd.Wait()
		close(n.exited)
	}()

	if err := n.waitReady(ctx); err != nil {
		return fmt.Errorf("%w\n%s", err, n.logTail())
	}
	return nil
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
	var addrs []string
	for _, a := range []string{n.LabAddr, n.ProdAddr4, n.ProxyAddr, n.ProdAddr6, n.PeersAddr, n.ProbeAddr} {
		if a != "" {
			addrs = append(addrs, a)
		}
	}
	return append(addrs, n.Listeners...)
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
