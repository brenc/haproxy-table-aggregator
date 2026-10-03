package peerwire

import (
	"bytes"
	"strconv"
)

// ProtocolName is the protocol identifier that starts every hello.
const ProtocolName = "HAProxyS"

// Version is a peers protocol version, "<major>.<minor>" on the wire.
type Version struct {
	Major uint32
	Minor uint32
}

// ProtocolVersion is the version the pinned releases announce. They also
// accept 2.0 from a peer, which signals a downgrade.
var ProtocolVersion = Version{Major: 2, Minor: 1}

// String formats v as it appears on the wire.
func (v Version) String() string {
	return strconv.FormatUint(uint64(v.Major), 10) + "." + strconv.FormatUint(uint64(v.Minor), 10)
}

// StatusCode is the three-digit code of a handshake status line.
type StatusCode uint16

// Status codes from doc/peers.txt. The comments note what triggers each
// one in stock HAProxy when it receives a hello.
const (
	// StatusSucceeded: handshake succeeded; binary messages may follow
	// immediately on the same connection.
	StatusSucceeded StatusCode = 200
	// StatusTryAgain: try again later.
	StatusTryAgain StatusCode = 300
	// StatusProtocolError: the first line does not start with
	// "HAProxyS ", or the third line has no space.
	StatusProtocolError StatusCode = 501
	// StatusBadVersion: the version is unparsable, has another major, or
	// a newer minor.
	StatusBadVersion StatusCode = 502
	// StatusHostMismatch: the second line is not the receiver's own peer
	// name ("local peer identifier mismatch" in the specification).
	StatusHostMismatch StatusCode = 503
	// StatusUnknownPeer: the sender's name on the third line is not a
	// configured peer ("remote peer identifier mismatch").
	StatusUnknownPeer StatusCode = 504
)

// Hello is the three-line message that opens a session.
type Hello struct {
	// Version is the protocol version the sender speaks.
	Version Version
	// RemotePeer is the name of the peer the hello is addressed to, as the
	// sender knows it; the receiver checks it against its own name.
	RemotePeer string
	// LocalPeer is the sender's own peer name.
	LocalPeer string
	// PID is the sender's process ID.
	PID uint32
	// RelativePID is the sender's relative process ID; stock HAProxy
	// sends 1.
	RelativePID uint32
}

// HelloLines is the number of lines in a hello message.
const HelloLines = 3

// maxPeerName bounds a peer name. The decoder's MaxHandshake already
// bounds it; this keeps encoded hellos within the default.
const maxPeerName = 1024

// AppendHello appends the encoded hello to dst. It returns an error
// wrapping ErrBadHandshake, and dst unchanged, if a peer name is not
// valid (see ParsePeerNameLine).
func AppendHello(dst []byte, h Hello) ([]byte, error) {
	if err := checkPeerName(h.RemotePeer); err != nil {
		return dst, err
	}
	if err := checkPeerName(h.LocalPeer); err != nil {
		return dst, err
	}
	dst = append(dst, ProtocolName...)
	dst = append(dst, ' ')
	dst = strconv.AppendUint(dst, uint64(h.Version.Major), 10)
	dst = append(dst, '.')
	dst = strconv.AppendUint(dst, uint64(h.Version.Minor), 10)
	dst = append(dst, '\n')
	dst = append(dst, h.RemotePeer...)
	dst = append(dst, '\n')
	dst = append(dst, h.LocalPeer...)
	dst = append(dst, ' ')
	dst = strconv.AppendUint(dst, uint64(h.PID), 10)
	dst = append(dst, ' ')
	dst = strconv.AppendUint(dst, uint64(h.RelativePID), 10)
	return append(dst, '\n'), nil
}

// AppendStatus appends the status line for code to dst. It returns an
// error wrapping ErrBadHandshake, and dst unchanged, unless code has
// exactly three digits.
func AppendStatus(dst []byte, code StatusCode) ([]byte, error) {
	if code < 100 || code > 999 {
		return dst, detailError(ErrBadHandshake, "status code %d is not three digits", code)
	}
	dst = strconv.AppendUint(dst, uint64(code), 10)
	return append(dst, '\n'), nil
}

// ParseHello parses the three lines of a hello as returned by NextLine.
// Errors wrap ErrBadHandshake.
func ParseHello(version, remote, sender []byte) (Hello, error) {
	v, err := ParseVersionLine(version)
	if err != nil {
		return Hello{}, err
	}
	r, err := ParsePeerNameLine(remote)
	if err != nil {
		return Hello{}, err
	}
	name, pid, rel, err := ParseSenderLine(sender)
	if err != nil {
		return Hello{}, err
	}
	return Hello{Version: v, RemotePeer: r, LocalPeer: name, PID: pid, RelativePID: rel}, nil
}

// ParseVersionLine parses a hello's first line, "HAProxyS <major>.<minor>".
// Each number is one or more ASCII digits fitting in 32 bits. It does not
// judge whether the version is supported. Errors wrap ErrBadHandshake.
func ParseVersionLine(line []byte) (Version, error) {
	rest, ok := bytes.CutPrefix(line, []byte(ProtocolName+" "))
	if !ok {
		return Version{}, detailError(ErrBadHandshake, "version line does not start with %q", ProtocolName+" ")
	}
	majS, minS, ok := bytes.Cut(rest, []byte{'.'})
	if !ok {
		return Version{}, detailError(ErrBadHandshake, "version has no '.'")
	}
	maj, ok := parseDecimal32(majS)
	if !ok {
		return Version{}, detailError(ErrBadHandshake, "major version is not a 32-bit decimal")
	}
	minor, ok := parseDecimal32(minS)
	if !ok {
		return Version{}, detailError(ErrBadHandshake, "minor version is not a 32-bit decimal")
	}
	return Version{Major: maj, Minor: minor}, nil
}

// ParsePeerNameLine parses a hello's second line: a peer name of 1 to
// 1024 printable ASCII bytes without spaces. Errors wrap ErrBadHandshake.
func ParsePeerNameLine(line []byte) (string, error) {
	if err := checkPeerName(string(line)); err != nil {
		return "", err
	}
	return string(line), nil
}

// ParseSenderLine parses a hello's third line, "<name> <pid> <relative
// pid>", with single spaces, a name as for ParsePeerNameLine, and 32-bit
// decimal numbers. Stock HAProxy itself only requires the first space;
// this parser is stricter because HAProxy always sends all three fields.
// Errors wrap ErrBadHandshake.
func ParseSenderLine(line []byte) (name string, pid, relativePID uint32, err error) {
	fields := bytes.Split(line, []byte{' '})
	if len(fields) != 3 {
		return "", 0, 0, detailError(ErrBadHandshake, "sender line has %d space-separated fields, want 3", len(fields))
	}
	if err := checkPeerName(string(fields[0])); err != nil {
		return "", 0, 0, err
	}
	pid, ok := parseDecimal32(fields[1])
	if !ok {
		return "", 0, 0, detailError(ErrBadHandshake, "process ID is not a 32-bit decimal")
	}
	relativePID, ok = parseDecimal32(fields[2])
	if !ok {
		return "", 0, 0, detailError(ErrBadHandshake, "relative process ID is not a 32-bit decimal")
	}
	return string(fields[0]), pid, relativePID, nil
}

// ParseStatusLine parses a status line: exactly three ASCII digits, the
// first non-zero. It does not judge whether the code is known. Errors wrap
// ErrBadHandshake.
func ParseStatusLine(line []byte) (StatusCode, error) {
	if len(line) != 3 || line[0] < '1' || line[0] > '9' || !isDigits(line) {
		return 0, detailError(ErrBadHandshake, "status line is not a three-digit code (%d bytes)", len(line))
	}
	code := StatusCode(line[0]-'0')*100 + StatusCode(line[1]-'0')*10 + StatusCode(line[2]-'0')
	return code, nil
}

func checkPeerName(name string) error {
	if name == "" || len(name) > maxPeerName {
		return detailError(ErrBadHandshake, "peer name length %d is outside 1..%d", len(name), maxPeerName)
	}
	for i := range len(name) {
		if c := name[i]; c <= ' ' || c > '~' {
			return detailError(ErrBadHandshake, "peer name byte %d is %#02x, not printable ASCII", i, c)
		}
	}
	return nil
}

func isDigits(b []byte) bool {
	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// parseDecimal32 accepts one or more ASCII digits (no sign) fitting in 32
// bits.
func parseDecimal32(b []byte) (uint32, bool) {
	if len(b) == 0 || len(b) > 10 || !isDigits(b) {
		return 0, false
	}
	v, err := strconv.ParseUint(string(b), 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(v), true
}
