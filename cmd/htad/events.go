package main

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// eventLine is one JSON line of output. Durations are milliseconds; time
// is the wall-clock reception or transition time (RFC 3339, local zone).
type eventLine struct {
	Time    time.Time `json:"time"`
	Source  string    `json:"source"`
	Session uint64    `json:"session"`
	Type    string    `json:"type"`

	Direction  string `json:"direction,omitempty"`
	RemoteAddr string `json:"remote_addr,omitempty"`
	RemotePID  uint32 `json:"remote_pid,omitempty"`
	Error      string `json:"error,omitempty"`

	Table    string      `json:"table,omitempty"`
	TableID  uint32      `json:"table_id,omitempty"`
	ExpiryMS uint32      `json:"expiry_ms,omitempty"`
	Fields   []string    `json:"fields,omitempty"`
	Update   *updateLine `json:"update,omitempty"`
	Partial  *bool       `json:"partial,omitempty"`
}

type updateLine struct {
	ID          uint32    `json:"id"`
	ExplicitID  bool      `json:"explicit_id"`
	Timed       bool      `json:"timed"`
	RemainingMS uint32    `json:"remaining_ms,omitempty"`
	LifetimeMS  uint32    `json:"lifetime_ms"`
	Key         string    `json:"key"`
	HTTPReqCnt  *uint32   `json:"http_req_cnt,omitempty"`
	HTTPReqRate *freqLine `json:"http_req_rate,omitempty"`
}

type freqLine struct {
	AgeMS uint32 `json:"age_ms"`
	Curr  uint32 `json:"curr"`
	Prev  uint32 `json:"prev"`
}

func writeEvent(w io.Writer, ev sources.Event) error {
	line := eventLine{Source: ev.Source, Session: ev.Session}
	switch b := ev.Body.(type) {
	case peersession.SessionUp:
		line.Type, line.Time = "session_up", b.At
		line.Direction, line.RemoteAddr, line.RemotePID = b.Direction.String(), b.RemoteAddr, b.RemotePID
	case peersession.SessionDown:
		line.Type, line.Time = "session_down", b.At
		line.Error = b.Err.Error()
	case peersession.TableDefined:
		line.Type, line.Time = "table_defined", b.Received
		line.Table, line.TableID, line.ExpiryMS = b.Definition.Name, uint32(b.ID), uint32(b.Definition.Expiry)
		for _, f := range b.Definition.Fields {
			line.Fields = append(line.Fields, f.String())
		}
	case peersession.TableRejected:
		line.Type, line.Time = "table_rejected", b.Received
		line.Table, line.TableID, line.Error = b.Table, uint32(b.ID), b.Err.Error()
	case peersession.EntryUpdated:
		line.Type, line.Time = "entry_updated", b.Update.Received
		line.Table, line.TableID = b.Table, uint32(b.ID)
		line.Update = newUpdateLine(b)
	case peersession.SyncFinished:
		line.Type, line.Time = "sync_finished", b.Received
		partial := b.Partial
		line.Partial = &partial
	default:
		return fmt.Errorf("unknown event %T", ev.Body)
	}
	b, err := json.Marshal(line)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

func newUpdateLine(e peersession.EntryUpdated) *updateLine {
	u := e.Update
	out := &updateLine{
		ID: uint32(u.ID), ExplicitID: u.ExplicitID, Timed: u.Timed, Key: u.Key.String(),
		LifetimeMS: uint32(u.Lifetime(e.Expiry)),
	}
	if u.Timed {
		out.RemainingMS = uint32(u.Remaining)
	}
	for _, v := range u.Values {
		switch v.Type {
		case peermsg.DataHTTPReqCnt:
			n := v.Uint
			out.HTTPReqCnt = &n
		case peermsg.DataHTTPReqRate:
			out.HTTPReqRate = &freqLine{AgeMS: uint32(v.Freq.Age), Curr: v.Freq.Curr, Prev: v.Freq.Prev}
		default:
			// Input tables store only the two types above.
		}
	}
	return out
}
