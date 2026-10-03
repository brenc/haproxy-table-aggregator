// Package labtest adapts the lab harness to Go tests: it finds the HAProxy
// binary from the environment, skips clearly when none is configured, and
// ties lab teardown to test cleanup.
//
// Environment:
//
//	HTA_HAPROXY          absolute path of a stock haproxy executable
//	                     (tests needing HAProxy skip when unset)
//	HTA_HAPROXY_VERSION  optional version prefix the binary must report,
//	                     e.g. "3.4.6"; guards against testing the wrong build
//	HTA_LAB_RECORD_DIR   optional directory receiving one JSON run record
//	                     per started lab
package labtest

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/brenc/haproxy-table-aggregator/internal/lab"
)

// Environment variable names read by this package.
const (
	EnvHAProxy        = "HTA_HAPROXY"
	EnvHAProxyVersion = "HTA_HAPROXY_VERSION"
	EnvRecordDir      = "HTA_LAB_RECORD_DIR"
)

// HAProxy returns the configured HAProxy path, or skips the test when
// HTA_HAPROXY is unset.
func HAProxy(tb testing.TB) string {
	tb.Helper()
	path := os.Getenv(EnvHAProxy)
	if path == "" {
		tb.Skipf("%s not set; set it to the absolute path of a stock haproxy "+
			"binary, or run `make haproxy lab-test`", EnvHAProxy)
	}
	return path
}

// Start starts a lab with opts (filling HAProxy from the environment when
// empty), registers its teardown with tb.Cleanup, verifies the expected
// HAProxy version if one is configured, and records the run identity.
func Start(tb testing.TB, opts lab.Options) *lab.Lab {
	tb.Helper()
	if opts.HAProxy == "" {
		opts.HAProxy = HAProxy(tb)
	}
	l, err := lab.Start(context.Background(), opts)
	if err != nil {
		tb.Fatalf("start lab: %v", err)
	}
	tb.Cleanup(func() {
		if err := l.Close(); err != nil {
			tb.Errorf("close lab: %v", err)
		}
	})
	rec := l.Record
	tb.Logf("lab run: %s", rec.Summary())
	if want := os.Getenv(EnvHAProxyVersion); want != "" &&
		rec.HAProxyVersion != want && !strings.HasPrefix(rec.HAProxyVersion, want+"-") {
		tb.Fatalf("haproxy reports version %s, %s wants %s", rec.HAProxyVersion, EnvHAProxyVersion, want)
	}
	if built := rec.BuildInfoSHA256(); built != "" && built != rec.HAProxySHA256 {
		tb.Fatalf("haproxy sha256 %s differs from build-info.txt %s", rec.HAProxySHA256, built)
	}
	if dir := os.Getenv(EnvRecordDir); dir != "" {
		path, err := rec.Write(dir, tb.Name())
		if err != nil {
			tb.Fatalf("write run record: %v", err)
		}
		tb.Logf("run record: %s", path)
	}
	return l
}
