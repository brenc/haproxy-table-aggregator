package lab

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync/atomic"
	"time"
)

// Client sends lab requests, one fresh TCP connection per request. Keep-
// alive is disabled so the HTTP transport never silently replays a request
// on a reused connection; every attempt is exactly one request.
type Client struct {
	http   *http.Client
	prefix string
	seq    atomic.Uint64
}

// Response is the outcome of one lab request.
type Response struct {
	// ID is the unique request ID sent in RequestIDHeader.
	ID string
	// Status is the HTTP status code returned by HAProxy.
	Status int
	// Key is the stick-table key HAProxy tracked, as it formats it.
	Key string
	// Lookup is the http_req_cnt HAProxy looked up for Key after tracking,
	// or -1 when the response carried no value.
	Lookup int64
}

// NewClient returns a Client whose connections originate from source, or
// from the kernel's choice when source is nil. Any 127.0.0.0/8 address is
// usable as an IPv4 source on Linux loopback without configuration.
func NewClient(source net.IP) (*Client, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	if source != nil {
		dialer.LocalAddr = &net.TCPAddr{IP: source}
	}
	return newClient(dialer.DialContext)
}

// NewProxyClient returns a Client for a node's ProxyAddr. Each connection
// begins with a PROXY protocol v1 header declaring source as the client
// address, so HAProxy's src fetch returns source without the harness
// needing that address locally. An IPv4-mapped source is sent as TCP6, as
// a dual-stack balancer would report it.
func NewProxyClient(source netip.Addr) (*Client, error) {
	if !source.IsValid() || source.Zone() != "" {
		return nil, fmt.Errorf("proxy client: invalid source %v", source)
	}
	family, dst := "TCP6", "2001:db8:ffff::1"
	if source.Is4() {
		family, dst = "TCP4", "192.0.2.254"
	}
	header := fmt.Sprintf("PROXY %s %s %s 40000 80\r\n", family, source, dst)
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return newClient(func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if _, err := io.WriteString(conn, header); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("proxy header: %w", err)
		}
		return conn, nil
	})
}

func newClient(dial func(context.Context, string, string) (net.Conn, error)) (*Client, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("request id prefix: %w", err)
	}
	transport := &http.Transport{
		Proxy:             nil,
		DialContext:       dial,
		DisableKeepAlives: true,
		MaxIdleConns:      -1,
	}
	return &Client{
		http: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		prefix: hex.EncodeToString(b[:]),
	}, nil
}

// Send issues one GET to addr (host:port). When clientIP is non-empty it is
// sent in ClientHeader; only the lab listener honours it. An error means
// the request may or may not have reached the responder; callers must
// consult the Responder rather than assume either outcome.
func (c *Client) Send(ctx context.Context, addr, clientIP string) (Response, error) {
	id := c.prefix + "-" + strconv.FormatUint(c.seq.Add(1), 10)
	resp := Response{ID: id, Lookup: -1}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/", http.NoBody)
	if err != nil {
		return resp, fmt.Errorf("request %s: %w", id, err)
	}
	req.Header.Set(RequestIDHeader, id)
	if clientIP != "" {
		req.Header.Set(ClientHeader, clientIP)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return resp, fmt.Errorf("request %s: %w", id, err)
	}
	defer func() { _ = res.Body.Close() }()
	if _, err := io.Copy(io.Discard, io.LimitReader(res.Body, 1<<16)); err != nil {
		return resp, fmt.Errorf("request %s body: %w", id, err)
	}
	resp.Status = res.StatusCode
	resp.Key = res.Header.Get(KeyHeader)
	if v := res.Header.Get(LookupHeader); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return resp, fmt.Errorf("request %s: bad %s %q: %w", id, LookupHeader, v, err)
		}
		resp.Lookup = n
	}
	return resp, nil
}

// ProbeResponse is the output probe listener's answer for one key: what
// stock HAProxy's table_gpt lookups and ACL saw in the output tables.
type ProbeResponse struct {
	// Status is 429 when the ACL matched (schema version 1 and a rate at
	// or above OutputLimit), 200 otherwise.
	Status int
	// Key is the key HAProxy looked up.
	Key string
	// Version and Rate are slots 0 and 1 of the OutputTable entry; both
	// read 0 for a missing entry.
	Version, Rate int64
	// MetaVersion is slot 0 of the MetaTable entry ::.
	MetaVersion int64
}

// Probe asks a node's probe listener (Node.ProbeAddr) what HAProxy's
// ordinary output lookups return for clientIP's key. Probing tracks
// nothing.
func (c *Client) Probe(ctx context.Context, addr, clientIP string) (ProbeResponse, error) {
	var out ProbeResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/", http.NoBody)
	if err != nil {
		return out, err
	}
	req.Header.Set(ClientHeader, clientIP)
	res, err := c.http.Do(req)
	if err != nil {
		return out, fmt.Errorf("probe: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if _, err := io.Copy(io.Discard, io.LimitReader(res.Body, 1<<16)); err != nil {
		return out, fmt.Errorf("probe body: %w", err)
	}
	out.Status, out.Key = res.StatusCode, res.Header.Get(KeyHeader)
	for _, f := range []struct {
		hdr string
		dst *int64
	}{{OutVersionHeader, &out.Version}, {OutRateHeader, &out.Rate}, {MetaVersionHeader, &out.MetaVersion}} {
		v := res.Header.Get(f.hdr)
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return out, fmt.Errorf("probe: status %d, bad %s %q: %w", res.StatusCode, f.hdr, v, err)
		}
		*f.dst = n
	}
	return out, nil
}

// SendN issues n sequential requests and returns the responses received.
// It stops at the first transport error or non-200 status.
func (c *Client) SendN(ctx context.Context, addr, clientIP string, n int) ([]Response, error) {
	out := make([]Response, 0, n)
	for range n {
		r, err := c.Send(ctx, addr, clientIP)
		if err != nil {
			return out, err
		}
		out = append(out, r)
		if r.Status != http.StatusOK {
			return out, fmt.Errorf("request %s: status %d", r.ID, r.Status)
		}
	}
	return out, nil
}

// CloseIdle releases any transport resources.
func (c *Client) CloseIdle() { c.http.CloseIdleConnections() }
