// Package ratetest is an independent reference for package rate, for
// tests only: a simulated HAProxy http_req_rate counter on a sender's
// wrapping millisecond clock, its reading as the specification in package
// rate states it, and the exact sliding-window count of the simulated
// events. It deliberately does not call package rate.
package ratetest

import (
	"math/big"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
)

// Source is one simulated counter. Times are the sender's now_ms, a
// 32-bit millisecond clock that wraps; differences are taken modulo 2^32.
// The zero value with a Period is a counter that never counted an event.
type Source struct {
	// Period is the counter's period in milliseconds.
	Period uint32

	start      uint32
	curr, prev uint32
	events     []uint32
}

// Hit counts n events at sender time now, rotating the counter first if
// its current period is over: by one period if the event falls within
// the next one, otherwise to a fresh period starting at now with nothing
// in the previous one. Period starts are kept even.
func (s *Source) Hit(now uint32, n int) {
	if now-s.start >= s.Period {
		next := s.start + s.Period
		if now-next < s.Period {
			s.prev, s.start = s.curr, next
		} else {
			s.prev, s.start = 0, now
		}
		s.start &^= 1
		s.curr = 0
	}
	for range n {
		s.curr++
		s.events = append(s.events, now)
	}
}

// Wire returns the counter as the sender encodes it at now.
func (s *Source) Wire(now uint32) peermsg.FreqCounter {
	return peermsg.FreqCounter{Age: peermsg.Millis(now - s.start), Curr: s.curr, Prev: s.prev}
}

// Read returns the sender's own reading at now (within the defined range:
// now less than 2^31 ms after the current period's start).
func (s *Source) Read(now uint32) uint64 {
	return Read(s.curr, s.prev, s.Period, uint64(now-s.start))
}

// Exact returns how many simulated events happened in the trailing
// window (now-Period, now]: the exact sliding-window count the estimate
// approximates.
func (s *Source) Exact(now uint32) uint64 {
	var n uint64
	for _, t := range s.events {
		if now-t < s.Period {
			n++
		}
	}
	return n
}

// Read is the specified reading of a counter with curr and prev events,
// age milliseconds after its current period started: the window of
// length period ending now overlaps the period holding curr entirely and
// the one before it in proportion, events are assumed uniform within a
// period, the result is truncated, and a counter whose newest non-empty
// period is the previous one with at most one event reads exactly that
// count.
func Read(curr, prev, period uint32, age uint64) uint64 {
	p := uint64(period)
	if p == 0 {
		return 0
	}
	// Which two consecutive periods the trailing window touches, and the
	// position of now within the later one.
	var newer, older, pos uint64
	switch {
	case age <= p:
		newer, older, pos = uint64(curr), uint64(prev), age
	case age <= 2*p:
		newer, older, pos = 0, uint64(curr), age-p
	default:
		return 0
	}
	if newer == 0 && older <= 1 {
		return older
	}
	overlap := new(big.Rat).SetFrac(new(big.Int).SetUint64(p-pos), new(big.Int).SetUint64(p))
	est := new(big.Rat).Mul(new(big.Rat).SetInt(new(big.Int).SetUint64(older)), overlap)
	est.Add(est, new(big.Rat).SetInt(new(big.Int).SetUint64(newer)))
	return new(big.Int).Quo(est.Num(), est.Denom()).Uint64()
}

// ReadReceived is the specified reading at local time now of counter fc
// received at local time received: its age advances by the whole
// milliseconds elapsed since reception, never backwards.
func ReadReceived(fc peermsg.FreqCounter, period uint32, received, now time.Time) uint64 {
	age := uint64(fc.Age)
	if d := now.Sub(received); d > 0 {
		age += uint64(d.Milliseconds()) //nolint:gosec // G115: d is positive.
	}
	return Read(fc.Curr, fc.Prev, period, age)
}
