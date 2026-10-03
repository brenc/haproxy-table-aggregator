package peerwire_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

func TestHelloRoundTrip(t *testing.T) {
	h := peerwire.Hello{
		Version:     peerwire.ProtocolVersion,
		RemotePeer:  "hap",
		LocalPeer:   "agg",
		PID:         4294967295,
		RelativePID: 1,
	}
	b, err := peerwire.AppendHello([]byte("x"), h)
	if err != nil {
		t.Fatal(err)
	}
	const want = "xHAProxyS 2.1\nhap\nagg 4294967295 1\n"
	if string(b) != want {
		t.Fatalf("AppendHello = %q, want %q", b, want)
	}
	lines := strings.Split(strings.TrimSuffix(want[1:], "\n"), "\n")
	got, err := peerwire.ParseHello([]byte(lines[0]), []byte(lines[1]), []byte(lines[2]))
	if err != nil || got != h {
		t.Fatalf("ParseHello = %+v, %v; want %+v", got, err, h)
	}
}

func TestAppendHelloRejectsBadNames(t *testing.T) {
	for _, name := range []string{"", "a b", "a\nb", "caf\xc3\xa9", strings.Repeat("n", 1025)} {
		b, err := peerwire.AppendHello([]byte("keep"), peerwire.Hello{RemotePeer: name, LocalPeer: "ok"})
		if !errors.Is(err, peerwire.ErrBadHandshake) || string(b) != "keep" {
			t.Errorf("AppendHello(remote %q) = %q, %v", name, b, err)
		}
	}
}

func TestParseVersionLine(t *testing.T) {
	good := map[string]peerwire.Version{
		"HAProxyS 2.1":                   {Major: 2, Minor: 1},
		"HAProxyS 2.0":                   {Major: 2, Minor: 0},
		"HAProxyS 3.07":                  {Major: 3, Minor: 7},
		"HAProxyS 4294967295.4294967295": {Major: 4294967295, Minor: 4294967295},
	}
	for line, want := range good {
		if got, err := peerwire.ParseVersionLine([]byte(line)); err != nil || got != want {
			t.Errorf("ParseVersionLine(%q) = %v, %v", line, got, err)
		}
	}
	for _, line := range []string{
		"", "HAProxyS", "HAProxyS ", "HAProxyX 2.1", "haproxys 2.1", "HAProxyS  2.1", "HAProxyS 2",
		"HAProxyS 2.", "HAProxyS .1", "HAProxyS 2.1.0", "HAProxyS +2.1", "HAProxyS 2.1 ", "HAProxyS 4294967296.1",
		"HAProxyS 2.1\x00",
	} {
		if _, err := peerwire.ParseVersionLine([]byte(line)); !errors.Is(err, peerwire.ErrBadHandshake) {
			t.Errorf("ParseVersionLine(%q) error = %v", line, err)
		}
	}
}

// TestHandshakeErrorsHaveNoOffset checks that line-level failures, which
// have no stream position, are not reported as positional *Error values.
func TestHandshakeErrorsHaveNoOffset(t *testing.T) {
	_, err := peerwire.ParseVersionLine([]byte("bad"))
	var pe *peerwire.Error
	if !errors.Is(err, peerwire.ErrBadHandshake) || errors.As(err, &pe) || strings.Contains(err.Error(), "offset") {
		t.Fatalf("ParseVersionLine error %q", err)
	}
	err = peerwire.Limits{}.Validate()
	if !errors.Is(err, peerwire.ErrInvalidLimits) || errors.As(err, &pe) || strings.Contains(err.Error(), "offset") {
		t.Fatalf("Validate error %q", err)
	}
}

func TestParseSenderLine(t *testing.T) {
	name, pid, rel, err := peerwire.ParseSenderLine([]byte("hap 1299972 1"))
	if err != nil || name != "hap" || pid != 1299972 || rel != 1 {
		t.Fatalf("ParseSenderLine = %q %d %d %v", name, pid, rel, err)
	}
	for _, line := range []string{
		"", "hap", "hap 1", "hap 1 1 1", "hap  1 1", " hap 1 1", "hap -1 1", "hap 1 x", "hap 4294967296 1", "hap\t1 1",
	} {
		if _, _, _, err := peerwire.ParseSenderLine([]byte(line)); !errors.Is(err, peerwire.ErrBadHandshake) {
			t.Errorf("ParseSenderLine(%q) error = %v", line, err)
		}
	}
}

func TestStatusLine(t *testing.T) {
	for _, code := range []peerwire.StatusCode{
		peerwire.StatusSucceeded, peerwire.StatusTryAgain, peerwire.StatusProtocolError,
		peerwire.StatusBadVersion, peerwire.StatusHostMismatch, peerwire.StatusUnknownPeer, 100, 999,
	} {
		b, err := peerwire.AppendStatus(nil, code)
		if err != nil || len(b) != 4 || b[3] != '\n' {
			t.Fatalf("AppendStatus(%d) = %q, %v", code, b, err)
		}
		if got, err := peerwire.ParseStatusLine(b[:3]); err != nil || got != code {
			t.Errorf("ParseStatusLine(%q) = %d, %v", b[:3], got, err)
		}
	}
	for _, code := range []peerwire.StatusCode{0, 99, 1000} {
		if _, err := peerwire.AppendStatus(nil, code); !errors.Is(err, peerwire.ErrBadHandshake) {
			t.Errorf("AppendStatus(%d) error = %v", code, err)
		}
	}
	for _, line := range []string{"", "20", "2000", "020", "20x", " 200", "200 ", "+20"} {
		if _, err := peerwire.ParseStatusLine([]byte(line)); !errors.Is(err, peerwire.ErrBadHandshake) {
			t.Errorf("ParseStatusLine(%q) error = %v", line, err)
		}
	}
}
