package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// CustomOptions configures StartCustom.
type CustomOptions struct {
	// HAProxy is the path of the stock haproxy executable. Required.
	HAProxy string
	// BaseDir holds the temporary directory. Empty means os.TempDir().
	// The runtime socket path must stay within HAProxy's limit.
	BaseDir string
	// Name is the node name: lowercase alphanumeric. Empty means "custom".
	Name string
	// Listeners is the number of loopback TCP listeners (127.0.0.1) the
	// harness opens and hands to HAProxy as inherited descriptors.
	Listeners int
	// Config renders the complete HAProxy configuration. It must declare
	// the admin runtime socket at CustomParams.Socket, which StartCustom
	// waits on, and bind every listener in CustomParams.Binds.
	Config func(CustomParams) ([]byte, error)
}

// CustomParams is passed to CustomOptions.Config.
type CustomParams struct {
	// Socket is the runtime API socket path to declare with
	// "stats socket <path> level admin".
	Socket string
	// Binds are bind addresses ("fd@N"), one per requested listener, in
	// the same order as Node.Listeners.
	Binds []string
}

// Custom is one owned HAProxy process running a caller-supplied
// configuration, for tests that need sections the two-node lab does not
// generate, such as peers. It shares the lab's process ownership and
// cleanup guarantees.
type Custom struct {
	// Dir is the private temporary directory, removed by Close.
	Dir string
	// Node is the running process. Its Listeners hold the addresses of
	// the inherited listeners; its lab listener fields are empty.
	Node *Node
	// Record identifies the toolchain and binary used for this run.
	Record RunRecord

	closeOnce sync.Once
	closeErr  error
}

// StartCustom starts one HAProxy process with opts.Config and waits until
// it answers on its runtime socket. On error everything already started is
// torn down.
func StartCustom(ctx context.Context, opts CustomOptions) (_ *Custom, err error) {
	if opts.HAProxy == "" {
		return nil, errors.New("lab: no haproxy executable given")
	}
	if opts.Config == nil {
		return nil, errors.New("lab: no config renderer given")
	}
	if opts.Listeners < 0 {
		return nil, fmt.Errorf("lab: negative listener count %d", opts.Listeners)
	}
	name := opts.Name
	if name == "" {
		name = "custom"
	}
	if err = validateNodeNames([]string{name}); err != nil {
		return nil, err
	}
	record, err := NewRunRecord(ctx, opts.HAProxy, 0)
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
	c := &Custom{Dir: dir, Record: record}
	defer func() {
		if err != nil {
			err = errors.Join(err, c.Close())
		}
	}()

	n := &Node{
		Name:       name,
		Socket:     filepath.Join(dir, name+".sock"),
		ConfigPath: filepath.Join(dir, name+".cfg"),
		LogPath:    filepath.Join(dir, name+".log"),
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
	params := CustomParams{Socket: n.Socket}
	for i := range opts.Listeners {
		il, lerr := listenInherited(ctx, "tcp4", "127.0.0.1:0")
		if lerr != nil {
			return nil, fmt.Errorf("lab: custom listener %d: %w", i, lerr)
		}
		files = append(files, il.file)
		n.Listeners = append(n.Listeners, il.addr)
		// ExtraFiles[i] becomes descriptor 3+i in the child.
		params.Binds = append(params.Binds, fmt.Sprintf("fd@%d", 2+len(files)))
	}
	cfg, err := opts.Config(params)
	if err != nil {
		return nil, fmt.Errorf("lab: render custom config: %w", err)
	}
	spawnErr := n.spawn(ctx, record.HAProxyPath, dir, cfg, files)
	if n.cmd != nil {
		c.Node = n
	}
	if spawnErr != nil {
		return nil, fmt.Errorf("lab: node %s: %w", name, spawnErr)
	}
	return c, nil
}

// Close stops the process (SIGTERM, then SIGKILL after a grace period) and
// removes the directory. It is safe to call more than once.
func (c *Custom) Close() error {
	c.closeOnce.Do(func() {
		var errs []error
		if c.Node != nil {
			errs = append(errs, c.Node.stop())
		}
		errs = append(errs, os.RemoveAll(c.Dir))
		c.closeErr = errors.Join(errs...)
	})
	return c.closeErr
}
