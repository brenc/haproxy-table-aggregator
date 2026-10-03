package lab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// RunRecord identifies exactly what a lab run executed: the Go toolchain
// that built the harness and the HAProxy binary, by resolved path, content
// digest, and self-reported version.
type RunRecord struct {
	StartedAt      time.Time `json:"started_at"`
	GoVersion      string    `json:"go_version"`
	GOOS           string    `json:"goos"`
	GOARCH         string    `json:"goarch"`
	Period         string    `json:"period"`
	HAProxyPath    string    `json:"haproxy_path"`
	HAProxySHA256  string    `json:"haproxy_sha256"`
	HAProxyVersion string    `json:"haproxy_version"`
	HAProxyVV      string    `json:"haproxy_vv"`
	// BuildInfo is the build-info.txt written next to the binary by
	// scripts/build-haproxy.sh, when present. Its binary_sha256 should
	// equal HAProxySHA256.
	BuildInfo string `json:"build_info,omitempty"`
}

var versionLine = regexp.MustCompile(`(?m)^HAProxy version (\S+)`)

// NewRunRecord resolves haproxy, hashes it, and captures `haproxy -vv`.
func NewRunRecord(ctx context.Context, haproxy string, period time.Duration) (RunRecord, error) {
	path, err := exec.LookPath(haproxy)
	if err != nil {
		return RunRecord{}, fmt.Errorf("lab: haproxy executable: %w", err)
	}
	if path, err = filepath.Abs(path); err != nil {
		return RunRecord{}, fmt.Errorf("lab: haproxy executable: %w", err)
	}
	if path, err = filepath.EvalSymlinks(path); err != nil {
		return RunRecord{}, fmt.Errorf("lab: haproxy executable: %w", err)
	}
	sum, err := fileSHA256(path)
	if err != nil {
		return RunRecord{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	vv, err := exec.CommandContext(ctx, path, "-vv").CombinedOutput() //nolint:gosec // G204: the operator-chosen haproxy
	if err != nil {
		return RunRecord{}, fmt.Errorf("lab: %s -vv: %w\n%s", path, err, vv)
	}
	m := versionLine.FindSubmatch(vv)
	if m == nil {
		return RunRecord{}, fmt.Errorf("lab: %s -vv: no version line", path)
	}
	r := RunRecord{
		StartedAt:      time.Now().UTC(),
		GoVersion:      runtime.Version(),
		GOOS:           runtime.GOOS,
		GOARCH:         runtime.GOARCH,
		Period:         period.String(),
		HAProxyPath:    path,
		HAProxySHA256:  sum,
		HAProxyVersion: string(m[1]),
		HAProxyVV:      string(vv),
	}
	info, err := os.ReadFile(filepath.Join(filepath.Dir(path), "build-info.txt"))
	switch {
	case err == nil:
		r.BuildInfo = string(info)
	case errors.Is(err, fs.ErrNotExist):
	default:
		return RunRecord{}, fmt.Errorf("lab: %w", err)
	}
	return r, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // G304: hashing the operator-chosen haproxy binary is the point
	if err != nil {
		return "", fmt.Errorf("lab: %w", err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("lab: hash %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// BuildInfoSHA256 returns the binary_sha256 recorded in BuildInfo, or "".
func (r RunRecord) BuildInfoSHA256() string {
	for line := range strings.Lines(r.BuildInfo) {
		if v, ok := strings.CutPrefix(line, "binary_sha256:"); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// Summary is a one-line identification suitable for logs.
func (r RunRecord) Summary() string {
	return fmt.Sprintf("go=%s %s/%s haproxy=%s sha256=%s path=%s period=%s",
		r.GoVersion, r.GOOS, r.GOARCH, r.HAProxyVersion, r.HAProxySHA256, r.HAProxyPath, r.Period)
}

// Write stores the record as JSON in dir under a name derived from the
// start time, HAProxy version, and label, and returns the file path.
func (r RunRecord) Write(dir, label string) (string, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("lab: %w", err)
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", fmt.Errorf("lab: %w", err)
	}
	name := fmt.Sprintf("%s-haproxy-%s-%s-%s.json",
		r.StartedAt.Format("20060102T150405.000000000Z"), r.HAProxyVersion, r.Period, sanitize(label))
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		return "", fmt.Errorf("lab: %w", err)
	}
	return path, nil
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
			return r
		default:
			return '_'
		}
	}, s)
}
