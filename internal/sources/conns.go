package sources

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
)

// acceptLoop accepts inbound connections until the listener closes.
func (m *Manager) acceptLoop() {
	delay := time.Duration(0)
	for {
		nc, err := m.ln.Accept()
		if err != nil {
			if m.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			// Resource errors such as EMFILE are retried after a pause
			// rather than spinning.
			delay = min(max(2*delay, 5*time.Millisecond), time.Second)
			m.log.Warn("accept failed", "err", err, "retry_in", delay)
			if !sleep(m.ctx, delay) {
				return
			}
			continue
		}
		delay = 0
		select {
		case m.pending <- struct{}{}:
		default:
			m.log.Warn("too many connections in handshake; closing", "remote", nc.RemoteAddr().String())
			_ = nc.Close()
			continue
		}
		m.wg.Go(func() {
			var once sync.Once
			done := func() { once.Do(func() { <-m.pending }) }
			defer done()
			m.serveInbound(nc, done)
		})
	}
}

// serveInbound validates a source's hello, makes the connection the
// source's session, and runs it. handshakeDone releases the connection's
// place among the connections in handshake.
func (m *Manager) serveInbound(nc net.Conn, handshakeDone func()) {
	defer func() { _ = nc.Close() }()
	remote := nc.RemoteAddr().String()
	pc, err := peersession.NewConn(nc, m.sessOpts)
	if err != nil {
		m.log.Error("session setup", "err", err)
		return
	}
	hello, err := pc.ReadHello(m.ctx, m.known)
	if err != nil {
		if m.ctx.Err() == nil {
			m.log.Warn("inbound hello refused", "remote", remote, "err", err)
		}
		return
	}
	sl := m.slots[hello.LocalPeer]
	a, err := m.claim(sl, peersession.Inbound, pc)
	if err != nil {
		if !errors.Is(err, errClosing) {
			m.log.Info("inbound connection refused", "source", sl.name, "remote", remote, "err", err)
		}
		if errors.Is(err, ErrOutboundCurrent) {
			_ = nc.Close()
			m.nudge(sl)
		}
		return
	}
	defer m.release(sl, a)
	if err := pc.AcceptHello(a.ctx); err != nil {
		m.log.Info("inbound handshake failed", "source", sl.name, "remote", remote, "err", err)
		return
	}
	handshakeDone()
	m.run(sl, a, peersession.SessionUp{
		Direction: peersession.Inbound, RemotePID: hello.PID, RemoteRelativePID: hello.RelativePID,
		RemoteAddr: remote, At: time.Now(),
	})
}

// dialLoop keeps one outbound session to a source while it has no other
// session, backing off between attempts.
func (m *Manager) dialLoop(sl *slot, address string) {
	backoff := m.cfg.ReconnectMin
	for {
		if !m.waitIdle(sl) {
			return
		}
		start := time.Now()
		up := m.dialOnce(sl, address)
		if up && time.Since(start) > m.cfg.ReconnectMax {
			backoff = m.cfg.ReconnectMin
		}
		if !sleep(m.ctx, jitter(backoff)) {
			return
		}
		backoff = min(2*backoff, m.cfg.ReconnectMax)
	}
}

// waitIdle waits until the source has no current session. It returns
// false when the manager closes.
func (m *Manager) waitIdle(sl *slot) bool {
	for {
		m.mu.Lock()
		cur, changed := sl.cur, sl.changed
		m.mu.Unlock()
		if cur == nil {
			return m.ctx.Err() == nil
		}
		select {
		case <-changed:
		case <-m.ctx.Done():
			return false
		}
	}
}

// dialOnce makes one outbound attempt and runs the session if it is
// established. It reports whether the session came up.
func (m *Manager) dialOnce(sl *slot, address string) bool {
	ctx, cancel := context.WithTimeout(m.ctx, m.cfg.HandshakeTimeout)
	defer cancel()
	nc, err := m.dial(ctx, address)
	if err != nil {
		if m.ctx.Err() == nil {
			m.log.Debug("dial failed", "source", sl.name, "address", address, "err", err)
			m.fault(sl, err)
		}
		return false
	}
	defer func() { _ = nc.Close() }()
	pc, err := peersession.NewConn(nc, m.sessOpts)
	if err != nil {
		m.log.Error("session setup", "err", err)
		return false
	}
	if gerr := pc.Greet(ctx, sl.name); gerr != nil {
		if m.ctx.Err() == nil {
			m.log.Warn("outbound handshake failed", "source", sl.name, "address", address, "err", gerr)
			m.fault(sl, gerr)
		}
		return false
	}
	a, err := m.claim(sl, peersession.Outbound, pc)
	if err != nil {
		return false
	}
	defer m.release(sl, a)
	m.run(sl, a, peersession.SessionUp{
		Direction: peersession.Outbound, RemoteAddr: nc.RemoteAddr().String(), At: time.Now(),
	})
	return true
}

// jitter returns a duration uniformly distributed in [d/2, d], so that
// sources and this daemon do not retry in lockstep.
func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	half := d / 2
	n, err := rand.Int(rand.Reader, big.NewInt(int64(d-half)+1))
	if err != nil {
		return d
	}
	return half + time.Duration(n.Int64())
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
