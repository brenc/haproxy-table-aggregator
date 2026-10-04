// Command htalab creates the two-node local HAProxy lab, and tears it down
// on SIGINT/SIGTERM or, with -smoke, after a scripted traffic check.
//
//	htalab -haproxy /path/to/haproxy [-period 10s] [-smoke] [-record-dir DIR]
//	       [-aggregator NAME@ADDR [-htad-config FILE]]
//
// -haproxy defaults to $HTA_HAPROXY. The lab runs two independent stock
// HAProxy processes on loopback; see internal/lab. With -aggregator, each
// node also gets a peers section shared only with the aggregator peer NAME,
// which the nodes dial at ADDR (a loopback ip:port), and -htad-config
// writes a matching htad configuration that listens on ADDR for the
// sessions both nodes open and also dials node b, so both connection
// directions (and their collisions) occur.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/lab"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "htalab:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) (err error) {
	fs := flag.NewFlagSet("htalab", flag.ContinueOnError)
	haproxy := fs.String("haproxy", os.Getenv("HTA_HAPROXY"), "stock haproxy executable (default $HTA_HAPROXY)")
	period := fs.Duration("period", lab.DefaultPeriod, "http_req_rate period")
	smoke := fs.Bool("smoke", false, "send 10 requests to node a and 20 to node b, print both tables, and exit")
	recordDir := fs.String("record-dir", "", "write the run record JSON to this directory")
	aggFlag := fs.String("aggregator", "", "aggregator peer as NAME@ip:port; adds a peers section to each node")
	htadConfig := fs.String("htad-config", "", "with -aggregator, write an htad configuration for this lab here")
	if err = fs.Parse(args); err != nil {
		return err
	}
	if *haproxy == "" {
		return errors.New("no haproxy executable: pass -haproxy or set HTA_HAPROXY")
	}
	var agg *lab.Aggregator
	if *aggFlag != "" {
		name, addr, ok := strings.Cut(*aggFlag, "@")
		if !ok {
			return fmt.Errorf("-aggregator %q: want NAME@ip:port", *aggFlag)
		}
		agg = &lab.Aggregator{Name: name, Addr: addr}
	} else if *htadConfig != "" {
		return errors.New("-htad-config needs -aggregator")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	l, err := lab.Start(ctx, lab.Options{HAProxy: *haproxy, Period: *period, Aggregator: agg})
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, l.Close())
		if err == nil {
			_, _ = fmt.Fprintln(out, "lab torn down")
		}
	}()

	_, _ = fmt.Fprintf(out, "lab up in %s\n%s\n", l.Dir, l.Record.Summary())
	_, _ = fmt.Fprintf(out, "responder %s\n", l.Responder.Addr())
	for _, n := range l.Nodes {
		_, _ = fmt.Fprintf(out, "node %s pid=%d lab=%s prod=%s prod6=%s proxy=%s runtime=%s",
			n.Name, n.PID(), n.LabAddr, n.ProdAddr4, n.ProdAddr6, n.ProxyAddr, n.Socket)
		if n.PeersAddr != "" {
			_, _ = fmt.Fprintf(out, " peers=%s probe=%s", n.PeersAddr, n.ProbeAddr)
		}
		_, _ = fmt.Fprintln(out)
	}
	if *htadConfig != "" {
		if err := writeHtadConfig(*htadConfig, l, agg); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "htad config %s\n", *htadConfig)
	}
	if *recordDir != "" {
		path, err := l.Record.Write(*recordDir, "htalab")
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "run record %s\n", path)
	}

	if *smoke {
		return runSmoke(ctx, l, out)
	}
	_, _ = fmt.Fprintln(out, "press Ctrl-C to tear down")
	<-ctx.Done()
	return nil
}

// writeHtadConfig writes an htad configuration for the lab: both nodes
// dial the daemon, and the daemon also dials node b. It names the lab's
// input table and both output tables.
func writeHtadConfig(path string, l *lab.Lab, agg *lab.Aggregator) error {
	a, b := l.Node("a"), l.Node("b")
	if a == nil || b == nil {
		return errors.New("-htad-config needs nodes a and b")
	}
	expire := (lab.ExpiryFactor * l.Period).String()
	doc := map[string]any{
		"local_peer":                      agg.Name,
		"insecure_plaintext_loopback_lab": true,
		"listen":                          agg.Addr,
		"sources": []map[string]string{
			{"name": a.Name},
			{"name": b.Name, "address": b.PeersAddr},
		},
		"tables": []map[string]string{{"name": lab.LabTable, "period": l.Period.String()}},
		"outputs": []map[string]string{
			{"name": lab.OutputTable, "kind": "aggregate", "expire": expire},
			{"name": lab.MetaTable, "kind": "metadata", "expire": expire},
		},
	}
	buf, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(buf, '\n'), 0o600)
}

func runSmoke(ctx context.Context, l *lab.Lab, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	c, err := lab.NewClient(nil)
	if err != nil {
		return err
	}
	defer c.CloseIdle()
	const client, key = "2001:db8:ab:cd::1", "2001:db8:ab:cd::"
	want := map[string]int64{"a": 10, "b": 20}
	var failed bool
	for _, n := range l.Nodes {
		if _, err := c.SendN(ctx, n.LabAddr, client, int(want[n.Name])); err != nil {
			return fmt.Errorf("node %s: %w", n.Name, err)
		}
	}
	for _, n := range l.Nodes {
		tbl, err := n.ShowTable(ctx, lab.LabTable)
		if err != nil {
			return err
		}
		e, _ := tbl.Entry(key)
		observed := l.Responder.Count(lab.Observation{Node: n.Name, Listener: "lab", Key: key})
		_, _ = fmt.Fprintf(out, "node %s %s key=%s http_req_cnt=%d responder_observed=%d want=%d\n",
			n.Name, lab.LabTable, key, e.Data["http_req_cnt"], observed, want[n.Name])
		if e.Data["http_req_cnt"] != want[n.Name] || int64(observed) != want[n.Name] {
			failed = true
		}
	}
	if dups, missing := l.Responder.Anomalies(); len(dups) > 0 || missing > 0 {
		return fmt.Errorf("responder anomalies: duplicates %v, missing ids %d", dups, missing)
	}
	if failed {
		return errors.New("smoke counts differ from expectations")
	}
	_, _ = fmt.Fprintln(out, "smoke ok")
	return nil
}
