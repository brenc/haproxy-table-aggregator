# Draft upstream report: implicit peer update IDs stored byte-swapped

Status: draft, not filed. Written for the HAProxy GitHub "Bug Report"
template (`.github/ISSUE_TEMPLATE/Bug.yml` on master); each `##` heading
below is one template field. Suggested title:

> peers: implicit update IDs are stored byte-swapped since 3.3 (wrong acks
> and `last_get`)

Checked before drafting (2026-10-03):

- No existing GitHub issue found (searched `last_get`, `last_acked`,
  `implicit update`, `peer_treat_updatemsg`, byte order/endian terms).
- Still present on master at `88576119c7ef` (2026-10-02).
- Introduced by `f12252c7a` ("BUG/MEDIUM: peers: Fix update message
  parsing during a full resync", 2025-11-07, first in v3.3-dev12), so
  3.3.0 and later are affected by source. Live-tested on 3.4.6 only;
  3.2.25 is the unaffected comparison.
- Not checked: the haproxy@formilux.org mailing list archive, and the
  3.3 stable branch head.

Our own context, not for the report: this was found in phase 03 while
building `internal/peermsg`; see `docs/plans/03-table-messages.md`.
Our senders always send explicit IDs, so the aggregator is unaffected.

---

## Detailed Description of the Problem

When a peer receives an update message without an explicit update ID
(types `0x81`/`0x83`, "incremental"), HAProxy 3.4.6 records the derived
ID byte-swapped on little-endian hosts. The swapped value is then
acknowledged to the sender and used to derive the next implicit ID.

With two stock 3.4.6 peers, writing five entries on peer `a` in one CLI
call (so they are pushed back to back, the first with an explicit ID and
the rest implicit) gives on peer `b`:

```
last_acked=33554435 last_pushed=0 last_get=33554435 teaching_origin=0 update=0
```

and on peer `a` the acknowledged value lands in its cursor:

```
last_acked=0 last_pushed=5 last_get=0 teaching_origin=0 update=33554435
```

`33554435` is `0x02000003`. With 3.2.25 and the same steps, both sides
show `5`. The stick-table entries themselves are applied correctly on
both versions (`gpc0=1..5` on `b`).

The value follows exactly from swapping each derived ID: the first update
is explicit (1); then 2 is stored as `0x02000000`, `0x02000001` as
`0x01000002`, `0x01000003` as `0x03000001`, and `0x03000002` as
`0x02000003`.

The same effect is visible with a single implicit update after an
explicit one: an implicit update following explicit ID 0 is acknowledged
as `0x01000000` instead of 1, and `show peers` reports
`last_get=16777216`.

## Expected Behavior

`doc/peers.txt` defines an update without an ID as carrying the previous
update ID plus one. The receiver should record and acknowledge 2, 3, 4,
5, as 3.2.25 does, so `last_get`/`last_acked` on the receiver and the
acknowledged cursor on the sender read `5`.

## Steps to Reproduce the Behavior

Two peers on loopback, `a.cfg` and `b.cfg` identical except for the
socket name (`a.sock`/`b.sock`); see the configuration field. Then, with
`socat`:

1. Start both: `haproxy -f a.cfg -L a -D -p a.pid` and
   `haproxy -f b.cfg -L b -D -p b.pid`, wait about 2 seconds.
2. Write five entries on `a` in one CLI call, so they are pushed
   together:

   ```sh
   echo "set table st key 192.0.2.1 data.gpc0 1; set table st key 192.0.2.2 data.gpc0 2; set table st key 192.0.2.3 data.gpc0 3; set table st key 192.0.2.4 data.gpc0 4; set table st key 192.0.2.5 data.gpc0 5" \
     | socat - UNIX-CONNECT:a.sock
   ```

3. Wait about 2 seconds, then run `show peers` on both sockets and
   `show table st` on `b.sock`.

Writing the entries one CLI call at a time does not reproduce it: each
is then pushed on its own with an explicit ID, and `last_get` is `5`.

## Do you have any idea what may have caused this?

`f12252c7a` moved the ID handling in `peer_treat_updatemsg()` out of the
`learnstate` condition and merged the two paths:

```c
if (updt) {
	...
	memcpy(&update, *msg_cur, sizeof(update));   /* network order */
	*msg_cur += sizeof(update);
}
else
	update = st->last_get + 1;                   /* host order */

if (p->learnstate != PEER_LR_ST_PROCESSING)
	st->last_get = htonl(update);
```

The `htonl()` is right for the explicit ID read from the wire, but the
implicit ID is already in host order, so it is swapped too. Before that
commit the implicit path was `st->last_get++`.

## Do you have an idea how to solve the issue?

Convert only the wire value, for example:

```c
if (updt) {
	...
	memcpy(&update, *msg_cur, sizeof(update));
	*msg_cur += sizeof(update);
	update = ntohl(update);
}
else
	update = st->last_get + 1;

if (p->learnstate != PEER_LR_ST_PROCESSING)
	st->last_get = update;
```

This is a suggestion only; it has not been built or tested.

## What is your configuration?

```haproxy
global
    stats socket unix@a.sock level admin
    log stderr local0 notice

defaults
    mode tcp
    timeout connect 5s
    timeout client 30s
    timeout server 30s

peers mypeers
    peer a 127.0.0.1:20001
    peer b 127.0.0.1:20002

backend st
    stick-table type ip size 1k expire 10m store gpc0 peers mypeers
```

`b.cfg` is identical with `unix@b.sock`.

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

## Last Outputs and Backtraces

No crash. Full output of the reproduction on 3.4.6:

```
## a: show peers (sender)
              last_acked=0 last_pushed=5 last_get=0 teaching_origin=0 update=33554435
              last_acked=0 last_pushed=0 last_get=0 teaching_origin=0 update=0
## b: show peers (receiver)
              last_acked=0 last_pushed=0 last_get=0 teaching_origin=0 update=0
              last_acked=33554435 last_pushed=0 last_get=33554435 teaching_origin=0 update=0
## b: show table st
# table: st, type: ip, size:1024, used:5
0x7fc4d8049e18: key=192.0.2.5 use=0 exp=598016 shard=0 gpc0=5
0x7fc4d8049ba8: key=192.0.2.2 use=0 exp=598016 shard=0 gpc0=2
0x7fc4d8049ad8: key=192.0.2.1 use=0 exp=598016 shard=0 gpc0=1
0x7fc4d8049c78: key=192.0.2.3 use=0 exp=598016 shard=0 gpc0=3
0x7fc4d8049d48: key=192.0.2.4 use=0 exp=598016 shard=0 gpc0=4
```

The same steps on 3.2.25-70469d3 show `last_get=5` and `last_acked=5` on
`b`, and `update=5` on `a`.

## Additional Information

- Stock builds, no local patches. Debian 13, x86_64 (little-endian).
  On a big-endian host `htonl()` is a no-op, so the bug should not show
  there (not tested).
- Possible impact, from reading the source and not reproduced: the
  sender keeps the acknowledged value in `st->update` and resumes
  teaching from it on the next session (`teaching_origin = last_pushed =
  st->update`). A swapped value that looks like it is in the future is
  reset to the oldest point, which causes a larger resend than needed. A
  swapped value that lies between the true ack and the local update
  counter would make the sender skip updates the peer never received.
  We have not observed either; a reconnect test without a full resync
  would be needed.
