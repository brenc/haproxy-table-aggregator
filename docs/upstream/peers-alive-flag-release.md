# Draft upstream report: releasing an abandoned peer connection kills the live session

Status: draft, not filed. Written for the HAProxy GitHub "Bug Report"
template (`.github/ISSUE_TEMPLATE/Bug.yml`); each `##` heading below is
one template field. Suggested title:

> peers: releasing an abandoned connection attempt clears PEER_F_ALIVE and
> gets the session that won the collision closed as dead

Checked before drafting (2026-10-04):

- Read in the pinned release tarballs only: `src/peers.c` of 3.4.6
  (`haproxy-3.4.6.tar.gz`) and 3.2.25 (`haproxy-3.2.25.tar.gz`), the
  same tarballs `docs/plans/02-wire-framing.md` records.
- Not checked: the master branch, the 3.3 stable branch, existing GitHub
  issues, and the haproxy@formilux.org archive. This draft was written
  offline; check all four before filing.

Our own context, not for the report: found in phase 04 while building the
aggregator's peer sessions; see `docs/plans/04-peer-sessions.md`. This
repository's automated reproduction is `TestLiveAbandonedAttempt`
(`internal/sources/live_test.go`). The aggregator works around the
behavior by sending two extra heartbeats, 10 ms and 250 ms after a
session starts and after it refuses a connection (`peersession.Conn.Nudge`,
`sources.nudgeDelays`). Without that mitigation the test failed 8 of 10
runs on each pinned build (16/20). With it, 30 of 30 runs passed. The
script and a trace are kept in `artifacts/upstream-peers-alive/`, which
is not committed. The raw outputs of the 20-trial runs below were not
kept.

Open review items (internal review R-7 to R-9, pending a fresh
investigation; do not file before they are resolved):

- R-7: the "heartbeat first" result (0/10 closed) is an artifact of the
  4 s observation window and is withdrawn below. The reviewer, using the
  published script with only the first message changed to a heartbeat
  and an 8 s watch on 3.4.6, saw 6 of 6 sessions closed, all at 5.003 s
  with `no_hbt=1`. A local unpublished variant of the script (8 s watch)
  agreed: 10/10 closed on each build with a heartbeat first, and 10/10
  with the published resync-first behavior. That variant is not part of
  this draft. The 4 s window also understates the resync-first rate,
  because a silent remote that survives the first check is checked again
  5 s later.
- R-8: the dead-peer branch has a precondition (no local updates to
  push); now stated in the analysis, but the reproduction and impact
  have not been re-examined with it in mind.
- R-9: the trace comes from an earlier variant of the script and is
  condensed; regenerate it with the published script and record the
  trace commands before filing.

---

## Detailed Description of the Problem

A remote peer dials HAProxy while HAProxy's own connection attempt to that
peer is still pending, and HAProxy accepts the incoming session (a
collision: `show peers` reports `coll=1`). The abandoned attempt can be
released later. Releasing it clears `PEER_F_ALIVE`, the liveness flag
that the session which won the collision relies on. If the sync task's
next liveness check runs before another message arrives from the remote
peer, HAProxy closes that live, healthy session as dead (`no_hbt` is
incremented) and both sides have to reconnect.

Observed, with stock 3.4.6 and 3.2.25 and the script below:

- HAProxy (`hap`) starts while the remote peer (`rem`) is not listening.
  Its first attempt to dial `rem` is refused, and the attempt stays
  pending.
- About 0.3 s later `rem` starts listening and dials `hap`. `hap` accepts
  the hello (`200`), and `show peers` shows `coll=1`.
- About 0.7 s after that, a TCP connection from `hap` arrives at `rem`:
  the attempt `hap` has already abandoned. The published script reads up
  to 256 bytes from it without printing them; our daemon received a
  complete hello on such connections.
  `rem` closes it without answering.
- In 12 of 20 trials on 3.4.6 and 11 of 20 on 3.2.25, `hap` then closed
  the established session, between 0.71 s and 1.94 s after the
  handshake. Every closed trial shows `no_hbt=1` (the "dead peer
  session" branch); every surviving trial shows `no_hbt=0`.
- If `rem` sends two heartbeats 10 ms and 250 ms after closing the late
  connection, 0 of 20 (3.4.6) and 1 of 20 (3.2.25) sessions were closed.
- Withdrawn, see open item R-7: a "heartbeat first" result of 0 of 10
  closed was measured with a 4 s window, which ends before the 5 s
  check that a heartbeat moves the liveness check to. With a longer
  watch, sessions were closed at 5.0 s.

A 3.4.6 trace of one closed trial. It comes from an earlier variant of
the script, in which the remote peer is named `agg`, not `rem`. Tracing
was enabled through the CLI (`trace peers sink stderr`, `trace peers
level developer`, `trace peers verbosity minimal`, `trace peers start
now`), and the output was filtered to session, resync, release and
dead-peer events. The excerpt below keeps 7 of those lines and removes
the `<mypeers/agg> peer=(` prefix and the learn, teach and handshake-time
fields. See open item R-9.

```
[00|peers|3|/peers.c:3115] release old session : [B,GETPEER] .fl=0x00000000, .app=STOPPED, status=HSHK, .reco=4s, .heart=<NEVER>
[00|peers|3|/peers.c:3170] connected, now wait for messages : [B,WAITMSG] .fl=0x00000030, .app=STARTING, status=ESTA, .reco=1s, .heart=3s
[00|peers|2|/peers.c:2610] Resync request message received : [B,WAITMSG] .fl=0x00000020, .app=RUNNING, status=ESTA, .reco=1s, .heart=2s
[00|peers|2|/peers.c:2625] Full resync finished message received : [B,WAITMSG] .fl=0x00100023, .app=RUNNING, status=ESTA, .reco=1s, .heart=2s
[00|peers|3|/peers.c:1175] peer session released : [F,END] .fl=0x00100003, .app=RUNNING, status=ESTA, .reco=0s, .heart=2s
[07|peers|3|/peers.c:3749] dead peer session, force shutdown : [B,WAITMSG] .fl=0x00100003, .app=RUNNING, status=ESTA, .reco=1s, .heart=<NEVER>
[00|peers|3|/peers.c:1175] peer session released : [B,END] .fl=0x00100000, .app=STOPPED, status=ESTA, .reco=1s, .heart=<NEVER>
```

Before the abandoned `[F]` appctx is released the flags are
`0x00100023`, with `PEER_F_ALIVE` (`0x20`) set. After the release they
are `0x00100003`. The next sync-task pass then closes the live `[B]`
session.

## Expected Behavior

Releasing a connection that is not the peer's current session
(`peer->appctx != appctx`) should not change the liveness of the session
that is current. A peer that answered and sent messages within the
current session should not be declared dead because of an abandoned
attempt.

## Steps to Reproduce the Behavior

The script below plays the remote peer `rem` with the Python 3 standard
library only. It starts `haproxy -db` with the configuration in the next
field (ports chosen at random), then for each trial:

1. Waits 0.3 s, so that HAProxy's first attempt to dial `rem` is refused
   and stays pending.
2. Listens as `rem` and dials HAProxy. It sends a resync request
   (`00 00`), not a heartbeat, and answers HAProxy's resync request with
   "finished" (`00 01`).
3. Accepts HAProxy's late connection, reads its hello and closes it
   without a status line.
4. Watches the established session for 4 s, below the 5 s reconnect
   period, and prints `show peers`.

`python3 peers_alive_repro.py /path/to/haproxy 20` reproduces the
problem. Add `--heartbeats-after-close` for the variant that sends two
heartbeats after step 3.

```python
#!/usr/bin/env python3
# Usage: peers_alive_repro.py HAPROXY TRIALS [--heartbeats-after-close]
import os, socket, subprocess, sys, tempfile, time

CFG = """global
    localpeer hap
    stats socket {sock} level admin
defaults
    timeout connect 5s
    timeout client 30s
    timeout server 30s
peers mypeers
    bind 127.0.0.1:{hap_port}
    server hap
    server rem 127.0.0.1:{rem_port}
backend st
    stick-table type ip size 1k expire 1m store gpc0 peers mypeers
"""

def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p

def show_peer(sock):
    c = socket.socket(socket.AF_UNIX); c.connect(sock); c.sendall(b"show peers\n")
    out = b""
    while (b := c.recv(65536)):
        out += b
    lines = out.decode().splitlines()
    i = next(i for i, l in enumerate(lines) if " id=rem(" in l)
    return lines[i].strip() + "\n    " + lines[i + 1].strip()

def trial(haproxy, heartbeats_after_close):
    d = tempfile.mkdtemp()
    hap_port, rem_port, sock = free_port(), free_port(), os.path.join(d, "hap.sock")
    with open(os.path.join(d, "hap.cfg"), "w") as f:
        f.write(CFG.format(sock=sock, hap_port=hap_port, rem_port=rem_port))
    hap = subprocess.Popen([haproxy, "-db", "-f", os.path.join(d, "hap.cfg")],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        while not os.path.exists(sock):
            time.sleep(0.01)
        # 1. "rem" is not listening yet: HAProxy's first attempt to dial it
        #    is refused and retried.
        time.sleep(0.3)
        ln = socket.socket(); ln.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ln.bind(("127.0.0.1", rem_port)); ln.listen(4)
        # 2. "rem" dials HAProxy; HAProxy accepts it while its own attempt
        #    is pending (a collision, show peers "coll=1").
        s = socket.create_connection(("127.0.0.1", hap_port))
        s.sendall(b"HAProxyS 2.1\nhap\nrem 1 1\n")
        assert s.recv(4) == b"200\n"
        s.sendall(b"\x00\x00")            # resync request (not a heartbeat)
        t0, answered, s_timeout = time.time(), False, s.settimeout(0.05)
        def pump():                       # False once HAProxy closed the session
            nonlocal answered
            try:
                b = s.recv(4096)
            except socket.timeout:
                return True
            if b and not answered and b"\x00\x00" in b:
                s.sendall(b"\x00\x01")    # answer HAProxy's resync request: finished
                answered = True
            return bool(b)
        while time.time() - t0 < 0.2:
            pump()
        # 3. HAProxy's abandoned attempt connects late; close it without a
        #    status line.
        ln.settimeout(5)
        late, _ = ln.accept()
        late.settimeout(2); late.recv(256); late.close()
        t_late = time.time() - t0
        if heartbeats_after_close:
            for delay in (0.01, 0.24):
                time.sleep(delay); s.sendall(b"\x00\x04")
        # 4. Watch the established session for 4 s (below the 5 s period).
        closed = None
        while time.time() - t0 < 4.0:
            if not pump():
                closed = time.time() - t0
                break
        return t_late, closed, show_peer(sock)
    finally:
        hap.terminate(); hap.wait()

if __name__ == "__main__":
    haproxy, n = sys.argv[1], int(sys.argv[2])
    hb = "--heartbeats-after-close" in sys.argv[3:]
    closed_count = 0
    for i in range(n):
        t_late, closed, peer = trial(haproxy, hb)
        closed_count += closed is not None
        print(f"trial {i}: late attempt closed at {t_late:.3f}s, live session "
              + (f"closed by HAProxy at {closed:.3f}s" if closed else "still up at 4s"))
        print("    " + peer)
    print(f"{closed_count}/{n} live sessions closed by HAProxy")
```

## Do you have any idea what may have caused this?

From the source only, not instrumented beyond the trace above. Line
numbers are for 3.4.6 / 3.2.25 `src/peers.c`. The relevant code is the
same in both releases apart from tracing.

1. Collision. `peer_io_handler()`, state `PEER_SESS_ST_GETPEER`
   (3.4.6:3106-3125, 3.2.25:3010-3027). When the accepted hello names a
   peer that already has an `appctx`, the collision code itself sets
   `curpeer->reconnect` to `now + 50 + ha_random() % 2000` ms
   (3.4.6:3120, 3.2.25:3022), calls `peer_session_forceshutdown(curpeer)`
   on the old appctx, sets `heartbeat` to `TICK_ETERNITY` (3.4.6:3122,
   3.2.25:3025), increments `coll`, and installs the new appctx with
   `PEER_F_ALIVE` set (3.4.6:3135, 3.2.25:3037).
2. Release. `peer_session_release()` (3.4.6:1160-1176,
   3.2.25:1105-1121) runs for any appctx at or past `PEER_SESS_ST_SENDSUCCESS`.
   It calls `__peer_session_deinit()` only if `peer->appctx == appctx`,
   but it clears `PEER_F_ALIVE` unconditionally (3.4.6:1173,
   3.2.25:1119). So releasing the old, abandoned appctx clears the flag
   of the new session.
3. Liveness check. `__process_running_peer_sync()`
   (3.4.6:3737-3753, 3.2.25:3608-3622). This branch is reached only
   when the local side has no updates to push to the peer
   (`if (!update_to_push)`, 3.4.6:3736, 3.2.25:3607); while updates are
   pending, an expired `reconnect` is simply re-armed (3.4.6:3722-3723,
   3.2.25:3594-3595). When `peer->reconnect` expires with nothing to
   push: if `PEER_F_ALIVE` is set, it clears the flag and re-arms 5 s;
   otherwise it calls `peer_session_forceshutdown()` and increments
   `no_hbt` (3.4.6 also emits the trace event "dead peer session, force
   shutdown"; 3.2.25 has no such event).
4. Messages set `PEER_F_ALIVE` (3.4.6:3296, 3.2.25:3188). A received
   heartbeat also pushes `reconnect` out by `PEER_RECONNECT_TIMEOUT`
   (5 s; 3.4.6:2667, 3.2.25:2590), as does the pending-updates path in
   step 3.

So after a collision, with no local updates pending, the check at step 3
runs 50 to 2050 ms later, unless a heartbeat from the remote moved it out
to 5 s. If the abandoned appctx is released after the remote's last
message and before the next check, the live session is closed. This
matches the measurements: closures at 0.71 to 1.94 s after the
handshake, and almost none when heartbeats follow the release. A
heartbeat as the first message only moves the check to 5 s; with a
silent remote the session was then closed at 5.0 s (open item R-7).
Inferred from source and not tested: without the unconditional clear,
the flag set by that heartbeat would survive and the session would pass
the 5 s check.

Inferred and not verified: why the abandoned appctx is released so late
(about 0.7 s after the collision) and still delivers its hello. Its
status was `HSHK` at the collision, so `peer_session_forceshutdown()`
did not skip it: the skip only applies to `PEER_SESS_ST_CONNECT`,
3.4.6:3413 and 3.2.25:3300. We assume its stream keeps retrying the
refused TCP connect, flushes the queued hello once it connects, and
releases the appctx only when that stream ends.

Also inferred and not tested: a stock HAProxy remote peer sends
heartbeats only after 3 s of silence, so HAProxy-to-HAProxy collisions
should be exposed in the same way.

## Do you have an idea how to solve the issue?

The release path already distinguishes the current appctx from a stale
one. Clearing `PEER_F_ALIVE` only for the current one looks like the
natural fix:

```c
if (peer) {
	HA_SPIN_LOCK(PEER_LOCK, &peer->lock);
	if (peer->appctx == appctx) {
		__peer_session_deinit(peer);
		peer->flags &= ~PEER_F_ALIVE;
	}
	HA_SPIN_UNLOCK(PEER_LOCK, &peer->lock);
	...
}
```

This is a suggestion only; it has not been built or tested. We have not
checked whether anything relies on the unconditional clear.

A remote peer can avoid the problem by sending a message after HAProxy's
extra connection has been released (in practice, shortly after the
remote closes that connection) and before HAProxy's next liveness check.
A message sent before the release does not help. That is a workaround
on the remote side, not a fix.

## What is your configuration?

```haproxy
global
    localpeer hap
    stats socket /tmp/x/hap.sock level admin

defaults
    timeout connect 5s
    timeout client 30s
    timeout server 30s

peers mypeers
    bind 127.0.0.1:HAP_PORT
    server hap
    server rem 127.0.0.1:REM_PORT

backend st
    stick-table type ip size 1k expire 1m store gpc0 peers mypeers
```

The script fills in the socket path and both ports. `rem` is played by
the script; HAProxy is run in the foreground (`-db`).

## Output of `haproxy -vv`

```
HAProxy version 3.4.6-56332c5 2026/09/28 - https://haproxy.org/
Status: long-term supported branch - will stop receiving fixes around Q2 2031.
Known bugs: http://www.haproxy.org/bugs/bugs-3.4.6.html
Running on: Linux 6.12.107+deb13-amd64 #1 SMP PREEMPT_DYNAMIC Debian 6.12.107-1 (2026-08-29) x86_64
Build options : 
  TARGET  = linux-glibc
  CC      = cc
  CFLAGS  = -O2 -g -fwrapv -fvect-cost-model=very-cheap -fstack-protector-strong -D_FORTIFY_SOURCE=3 -fPIE
  OPTIONS = USE_OPENSSL_AWSLC=1 USE_LUA=1 USE_QUIC=1 USE_PROMEX=1 USE_PCRE2=1 USE_PCRE2_JIT=1
  DEBUG   = -DDEBUG_STRICT -DDEBUG_STRICT_ACTION

Feature list : -51DEGREES +ACCEPT4 +ACME +BACKTRACE -CLOSEFROM +CPU_AFFINITY +CRYPT_H -DEVICEATLAS +DL -ECH -ENGINE +EPOLL -EVPORTS +GETADDRINFO +HAVE_TCP_MD5SIG -KQUEUE +KTLS -LIBATOMIC +LIBCRYPT +LINUX_CAP +LINUX_SPLICE +LINUX_TPROXY +LUA +MATH -MEMORY_PROFILING +NETFILTER +NS -OBSOLETE_LINKER +OPENSSL +OPENSSL_AWSLC -OPENSSL_WOLFSSL -OT -PCRE +PCRE2 +PCRE2_JIT -PCRE_JIT +POLL +PRCTL -PROCCTL +PROMEX -PTHREAD_EMULATION +QUIC -QUIC_OPENSSL_COMPAT +RT +SHM_OPEN +SLZ +SSL -STATIC_PCRE -STATIC_PCRE2 +TFO +THREAD +THREAD_DUMP +TPROXY +TRACE -WURFL -ZLIB
Detected feature list : +HAVE_WORKING_TCP_MD5SIG

Default settings :
  bufsize = 16384, maxrewrite = 1024, maxpollevents = 200

Built with multi-threading support (MAX_TGROUPS=32, MAX_THREADS=1024, default=8).
Built with SSL library version : OpenSSL 1.1.1 (compatible; AWS-LC 5.11.0)
Running on SSL library version : AWS-LC 5.11.0
SSL library supports TLS extensions : yes
SSL library supports SNI : yes
SSL library FIPS mode : no
SSL library default verify directory : /etc/ssl/certs
SSL library supports : TLSv1.0 TLSv1.1 TLSv1.2 TLSv1.3
QUIC: connection sock-per-conn mode support : yes
QUIC: GSO emission support : yes
Built with Lua version : Lua 5.4.7
Built with the Prometheus exporter as a service
Built with network namespace support.
Built with libslz for stateless compression.
Compression algorithms supported : identity("identity"), deflate("deflate"), raw-deflate("deflate"), gzip("gzip")
Built with transparent proxy support using: IP_TRANSPARENT IPV6_TRANSPARENT IP_FREEBIND
Built with PCRE2 version : 10.46 2025-08-27
PCRE2 library supports JIT : yes
Encrypted password support via crypt(3): yes
Built with gcc compiler version 14.2.0

Available polling systems :
      epoll : pref=300,  test result OK
       poll : pref=200,  test result OK
     select : pref=150,  test result OK
Total: 3 (3 usable), will use epoll.

Available multiplexer protocols :
(protocols marked as <default> cannot be specified using 'proto' keyword)
       qmux : mode=HTTP  side=FE|BE  mux=QMUX  flags=HTX|NO_UPG
       quic : mode=HTTP  side=FE|BE  mux=QUIC  flags=HTX|NO_UPG|FRAMED
         h2 : mode=HTTP  side=FE|BE  mux=H2    flags=HTX|HOL_RISK|NO_UPG
  <default> : mode=HTTP  side=FE|BE  mux=H1    flags=HTX
         h1 : mode=HTTP  side=FE|BE  mux=H1    flags=HTX|NO_UPG
       fcgi : mode=HTTP  side=BE     mux=FCGI  flags=HTX|HOL_RISK|NO_UPG
  <default> : mode=SPOP  side=BE     mux=SPOP  flags=HOL_RISK|NO_UPG
       spop : mode=SPOP  side=BE     mux=SPOP  flags=HOL_RISK|NO_UPG
  <default> : mode=TCP   side=FE|BE  mux=PASS  flags=
       none : mode=TCP   side=FE|BE  mux=PASS  flags=NO_UPG

Available services : prometheus-exporter
Available filters :
	[BWLIM] bwlim-in
	[BWLIM] bwlim-out
	[CACHE] cache
	[COMP] comp-req
	[COMP] comp-res
	[COMP] compression
	[FCGI] fcgi-app
	[SPOE] spoe
	[TRACE] trace
```

The 3.2.25 binary (`3.2.25-70469d3`) was built with the same `CFLAGS`
and `OPTIONS`.

## Last Outputs and Backtraces

No crash. A closed trial on 3.2.25:

```
trial 0: late attempt closed at 0.699s, live session closed by HAProxy at 1.893s
    0x56459092f050: id=rem(remote,inactive) addr=127.0.0.1:55073 app_state=STOPPED learn_state=NOTASSIGNED last_status=ESTA last_hdshk=1s
    reconnect=1s heartbeat=<NEVER> confirm=0 tx_hbt=0 rx_hbt=0 no_hbt=1 new_conn=1 proto_err=0 coll=1
```

A surviving trial on 3.4.6:

```
trial 0: late attempt closed at 0.693s, live session still up at 4s
    0x56316c5ed490: id=rem(remote,active) addr=127.0.0.1:39543 app_state=RUNNING learn_state=NOTASSIGNED last_status=ESTA last_hdshk=4s
    reconnect=1s heartbeat=1s confirm=0 tx_hbt=1 rx_hbt=0 no_hbt=0 new_conn=1 proto_err=0 coll=1
```

Summary of all runs (20 trials each unless noted):

| Build  | Resync request first | + two heartbeats after the close |
| ------ | -------------------- | -------------------------------- |
| 3.4.6  | 12/20 closed         | 0/20 closed                      |
| 3.2.25 | 11/20 closed         | 1/20 closed                      |

Both columns use the published script's 4 s window (see open item R-7).

## Additional Information

- Stock builds, no local patches. Debian 13, x86_64, loopback only.
- Impact: a peer session that has just won a collision can be torn down
  within about 2 s, although both ends are healthy and exchanging
  messages, provided the HAProxy side has no updates pending for that
  peer when the liveness check runs. Both sides then reconnect, which can collide again. Data is
  not lost; a resync follows the reconnect. But the churn shows up as
  `no_hbt` and as extra reconnects. Collisions are most likely when
  peers start or restart at about the same time.
- The heartbeat variant still lost 1 of 20 sessions on 3.2.25. We assume
  the liveness check fell between the release and the first heartbeat
  (10 ms later); not verified.
