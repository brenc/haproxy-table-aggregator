package lab

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// maxRuntimeResponse bounds a single runtime API reply. Lab tables are
// small; a larger reply means something other than a lab table was dumped.
const maxRuntimeResponse = 1 << 20

// RuntimeCommand sends one command to an HAProxy runtime API Unix socket
// and returns the complete reply. The socket is used in non-interactive
// mode, so HAProxy closes the connection after answering.
func RuntimeCommand(ctx context.Context, socket, command string) (string, error) {
	if strings.ContainsAny(command, "\r\n;") {
		return "", fmt.Errorf("runtime command %q: must be a single command", command)
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		return "", fmt.Errorf("runtime socket %s: %w", socket, err)
	}
	defer func() { _ = conn.Close() }()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	if err = conn.SetDeadline(deadline); err != nil {
		return "", fmt.Errorf("runtime socket %s: %w", socket, err)
	}
	if _, err = io.WriteString(conn, command+"\n"); err != nil {
		return "", fmt.Errorf("runtime command %q: %w", command, err)
	}
	reply, err := io.ReadAll(io.LimitReader(conn, maxRuntimeResponse+1))
	if err != nil {
		return "", fmt.Errorf("runtime command %q: %w", command, err)
	}
	if len(reply) > maxRuntimeResponse {
		return "", fmt.Errorf("runtime command %q: reply exceeds %d bytes", command, maxRuntimeResponse)
	}
	return string(reply), nil
}

// Table is a parsed `show table <name>` dump.
type Table struct {
	Name    string
	Type    string
	Size    int64
	Used    int64
	Entries []Entry
}

// Entry is one stick-table entry. Data maps each stored data type, written
// as HAProxy prints it (for example "http_req_rate(10000)"), to its value.
type Entry struct {
	Key  string
	Exp  time.Duration
	Data map[string]int64
}

// Entry returns the entry for key, if present.
func (t Table) Entry(key string) (Entry, bool) {
	for _, e := range t.Entries {
		if e.Key == key {
			return e, true
		}
	}
	return Entry{}, false
}

// Keys returns every entry key in dump order.
func (t Table) Keys() []string {
	keys := make([]string, 0, len(t.Entries))
	for _, e := range t.Entries {
		keys = append(keys, e.Key)
	}
	return keys
}

// ErrNoTable reports that HAProxy has no table with the requested name.
var ErrNoTable = errors.New("no such table")

// ParseTable parses the output of `show table <name>`. It accepts the
// format shared by HAProxy 3.2 and 3.4:
//
//	# table: lab_in, type: ipv6, size:1024, used:1
//	0x55d0c0de0000: key=::1 use=0 exp=29900 shard=0 http_req_cnt=2 ...
//
// Unknown entry fields are ignored so that additive upstream changes do not
// break the lab; missing headers, keys, or malformed numbers are errors.
func ParseTable(dump string) (Table, error) {
	var t Table
	sc := bufio.NewScanner(strings.NewReader(dump))
	sawHeader := false
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "":
			continue
		case strings.HasPrefix(line, "# table:"):
			if sawHeader {
				return Table{}, errors.New("show table: more than one table in dump")
			}
			var err error
			if t, err = parseTableHeader(line); err != nil {
				return Table{}, err
			}
			sawHeader = true
		case !sawHeader:
			if strings.Contains(line, "Unknown table") || strings.Contains(line, "No such table") {
				return Table{}, fmt.Errorf("show table: %w: %s", ErrNoTable, line)
			}
			return Table{}, fmt.Errorf("show table: unexpected line before header: %q", line)
		default:
			e, err := parseEntry(line)
			if err != nil {
				return Table{}, err
			}
			t.Entries = append(t.Entries, e)
		}
	}
	if err := sc.Err(); err != nil {
		return Table{}, fmt.Errorf("show table: %w", err)
	}
	if !sawHeader {
		return Table{}, errors.New("show table: missing table header")
	}
	return t, nil
}

func parseTableHeader(line string) (Table, error) {
	var t Table
	for field := range strings.SplitSeq(strings.TrimPrefix(line, "#"), ",") {
		name, value, ok := strings.Cut(field, ":")
		if !ok {
			return Table{}, fmt.Errorf("show table: malformed header field %q", field)
		}
		value = strings.TrimSpace(value)
		var err error
		switch strings.TrimSpace(name) {
		case "table":
			t.Name = value
		case "type":
			t.Type = value
		case "size":
			t.Size, err = strconv.ParseInt(value, 10, 64)
		case "used":
			t.Used, err = strconv.ParseInt(value, 10, 64)
		default:
		}
		if err != nil {
			return Table{}, fmt.Errorf("show table: header field %q: %w", field, err)
		}
	}
	if t.Name == "" || t.Type == "" {
		return Table{}, fmt.Errorf("show table: incomplete header %q", line)
	}
	return t, nil
}

func parseEntry(line string) (Entry, error) {
	_, rest, ok := strings.Cut(line, ": ")
	if !ok {
		return Entry{}, fmt.Errorf("show table: malformed entry %q", line)
	}
	e := Entry{Data: map[string]int64{}}
	hasKey := false
	for field := range strings.FieldsSeq(rest) {
		name, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch name {
		case "key":
			e.Key = value
			hasKey = true
		case "exp":
			ms, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return Entry{}, fmt.Errorf("show table: entry %q exp: %w", line, err)
			}
			e.Exp = time.Duration(ms) * time.Millisecond
		case "use", "shard":
		default:
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return Entry{}, fmt.Errorf("show table: entry %q field %s: %w", line, name, err)
			}
			e.Data[name] = n
		}
	}
	if !hasKey {
		return Entry{}, fmt.Errorf("show table: entry without key %q", line)
	}
	return e, nil
}
