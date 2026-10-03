#!/usr/bin/env bash
# Builds unmodified upstream HAProxy releases for the local lab, linked
# statically against a pinned AWS-LC.
#
# Usage: scripts/build-haproxy.sh [VERSION...]   (default: every pinned one)
#
# Every archive is downloaded once, verified against the SHA-256 pinned
# below (HAProxy digests match haproxy.org's published .sha256 files), and
# re-verified on reuse. No patches are applied and EXTRAVERSION is not set:
# the binaries are stock upstream. AWS-LC is built once and shared by every
# HAProxy build. Results land in $HAPROXY_ARTIFACTS (default
# artifacts/haproxy):
#
#   <version>/haproxy          the binary; never committed
#   <version>/build-info.txt   digests, toolchain versions, every flag
#   <version>/haproxy-vv.txt   full `haproxy -vv` output
#   dl/                        verified source archives
#   aws-lc-<v>/                AWS-LC build tree and install prefix
#
# Prerequisites (Debian package names): curl, cmake, make, gcc, g++, perl,
# liblua5.4-dev, libpcre2-dev, libcrypt-dev, and Go. AWS-LC's build runs
# Go; the recipe uses the toolchain pinned by this repository's go.mod (any
# Go 1.21+ with GOTOOLCHAIN=auto fetches it). LUA_INC and LUA_LIB may point
# at unpacked Lua development files. JOBS sets parallelism.
#
# Rebuilding with the same host toolchain and HAPROXY_ARTIFACTS path yields a
# bit-identical binary; other paths change embedded __FILE__ strings.
#
# The lab itself accepts any stock haproxy binary; this recipe is one
# reproducible way to obtain one.

set -euo pipefail

declare -A HAPROXY_SHA256=(
	[3.4.6]=791e1815f8af6e8b850a227a9a0a190f3d3478c9e8d38a0f51c98b7f4bfe368b
	[3.2.25]=d59a68d0daef7b5c596b019b742089788ff1748513ef96e71fe7b3943577866e
)
DEFAULT_VERSIONS=(3.4.6 3.2.25)
AWSLC_VERSION=5.11.0
AWSLC_SHA256=8cb24c6e6be1fa7ff05075c4560ca8b537a7ef48f9e6f465af4ea455794d74f4

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
out_root=${HAPROXY_ARTIFACTS:-$root/artifacts/haproxy}
mkdir -p "$out_root"
out_root=$(cd "$out_root" && pwd)
jobs=${JOBS:-$(nproc 2>/dev/null || echo 2)}
dl="$out_root/dl"
awslc_dir="$out_root/aws-lc-$AWSLC_VERSION"
awslc_prefix="$awslc_dir/install"

check_prereqs() {
	local missing=() tool
	for tool in curl tar cmake make gcc g++ perl go sha256sum; do
		command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
	done
	local header
	for header in lua5.4/lua.h pcre2.h crypt.h; do
		printf '#include <%s>\n' "$header" | gcc -E -DPCRE2_CODE_UNIT_WIDTH=8 -x c - >/dev/null 2>&1 ||
			missing+=("$header")
	done
	if [[ ${#missing[@]} -gt 0 ]]; then
		echo "build-haproxy: missing prerequisites: ${missing[*]}" >&2
		exit 1
	fi
}

# fetch URL DEST SHA256: download once, verify on every use.
fetch() {
	local url=$1 dest=$2 digest=$3
	if [[ -f $dest ]] && printf '%s  %s\n' "$digest" "$dest" | sha256sum --check --status; then
		return
	fi
	rm -f "$dest" "$dest.part"
	echo "download $url"
	curl -fsSL --retry 3 -o "$dest.part" "$url"
	if ! printf '%s  %s\n' "$digest" "$dest.part" | sha256sum --check --status; then
		echo "build-haproxy: digest mismatch for $url" >&2
		echo "  got  $(sha256sum "$dest.part" | cut -d' ' -f1)" >&2
		echo "  want $digest" >&2
		rm -f "$dest.part"
		exit 1
	fi
	mv "$dest.part" "$dest"
}

# AWS-LC's cmake runs `go`; point it at the repository's pinned toolchain
# and keep its caches inside the build tree.
setup_go() {
	local goroot
	goroot=$(cd "$root" && go env GOROOT)
	export PATH="$goroot/bin:$PATH"
	export GOTOOLCHAIN=local
	export GOCACHE="$awslc_dir/go-cache"
	export GOMODCACHE="$awslc_dir/go-mod"
	export GOFLAGS=-modcacherw
	go_version=$(go version)
}

build_awslc() {
	local stamp="$awslc_prefix/.built-$AWSLC_SHA256"
	[[ -f $stamp ]] && return
	local tarball="$dl/aws-lc-$AWSLC_VERSION.tar.gz"
	fetch "https://github.com/aws/aws-lc/archive/refs/tags/v$AWSLC_VERSION.tar.gz" \
		"$tarball" "$AWSLC_SHA256"
	rm -rf "$awslc_dir/src" "$awslc_dir/build" "$awslc_prefix"
	mkdir -p "$awslc_dir/src"
	tar -xzf "$tarball" -C "$awslc_dir/src" --strip-components=1
	echo "build aws-lc $AWSLC_VERSION"
	cmake -S "$awslc_dir/src" -B "$awslc_dir/build" \
		-DCMAKE_BUILD_TYPE=Release \
		-DCMAKE_INSTALL_PREFIX="$awslc_prefix" \
		-DCMAKE_POSITION_INDEPENDENT_CODE=ON \
		-DBUILD_SHARED_LIBS=OFF -DBUILD_TESTING=OFF -DBUILD_TOOL=OFF \
		>"$awslc_dir/cmake.log" 2>&1 || { tail -n 40 "$awslc_dir/cmake.log" >&2; exit 1; }
	cmake --build "$awslc_dir/build" -j "$jobs" >"$awslc_dir/build.log" 2>&1 ||
		{ tail -n 40 "$awslc_dir/build.log" >&2; exit 1; }
	cmake --install "$awslc_dir/build" >>"$awslc_dir/build.log" 2>&1
	touch "$stamp"
}

build_haproxy() {
	local version=$1
	local want=${HAPROXY_SHA256[$version]:-}
	if [[ -z $want ]]; then
		echo "build-haproxy: no pinned checksum for $version" >&2
		exit 1
	fi
	local url="https://www.haproxy.org/download/${version%.*}/src/haproxy-$version.tar.gz"
	local tarball="$dl/haproxy-$version.tar.gz"
	local dest="$out_root/$version"
	fetch "$url" "$tarball" "$want"

	# A fixed work path keeps __FILE__ strings (used by DEBUG_STRICT
	# assertions) identical between builds of the same inputs.
	local work="$out_root/build/$version"
	rm -rf "$work"
	mkdir -p "$work"
	tar -xzf "$tarball" -C "$work"
	local src="$work/haproxy-$version"

	local lua_args=(LUA_LIB_NAME=lua5.4)
	[[ -z ${LUA_INC:-} ]] || lua_args+=("LUA_INC=$LUA_INC")
	[[ -z ${LUA_LIB:-} ]] || lua_args+=("LUA_LIB=$LUA_LIB")
	local flags=(
		TARGET=linux-glibc
		USE_OPENSSL_AWSLC=1 USE_QUIC=1
		USE_LUA=1 USE_PCRE2=1 USE_PCRE2_JIT=1 USE_PROMEX=1
		"SSL_INC=$awslc_prefix/include"
		"SSL_LIB=$awslc_prefix/lib"
		"${lua_args[@]}"
		"DEBUG=-DDEBUG_STRICT -DDEBUG_STRICT_ACTION"
		"CFLAGS=-fstack-protector-strong -D_FORTIFY_SOURCE=3 -fPIE"
		"LDFLAGS=-pie -Wl,-z,relro,-z,now"
	)
	echo "build haproxy $version"
	if ! make -C "$src" -j "$jobs" "${flags[@]}" >"$work/build.log" 2>&1; then
		tail -n 40 "$work/build.log" >&2
		echo "build-haproxy: $version failed; full log in $work/build.log" >&2
		exit 1
	fi

	rm -rf "$dest"
	mkdir -p "$dest"
	install -m 0755 "$src/haproxy" "$dest/haproxy"
	cp "$work/build.log" "$dest/build.log"
	"$dest/haproxy" -vv >"$dest/haproxy-vv.txt"
	{
		echo "version: $version"
		echo "source_url: $url"
		echo "source_sha256: $want"
		echo "patches: none"
		echo "awslc_version: $AWSLC_VERSION"
		echo "awslc_url: https://github.com/aws/aws-lc/archive/refs/tags/v$AWSLC_VERSION.tar.gz"
		echo "awslc_sha256: $AWSLC_SHA256"
		echo "awslc_cmake_flags: -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_SHARED_LIBS=OFF -DBUILD_TESTING=OFF -DBUILD_TOOL=OFF"
		printf 'make_flags:'
		printf ' %q' "${flags[@]}"
		echo
		echo "ssl_built: $(sed -n 's/^Built with SSL library version : //p' "$dest/haproxy-vv.txt")"
		echo "ssl_running: $(sed -n 's/^Running on SSL library version : //p' "$dest/haproxy-vv.txt")"
		echo "compiler: $(gcc --version | head -n 1)"
		echo "cxx_compiler: $(g++ --version | head -n 1)"
		echo "cmake: $(cmake --version | head -n 1)"
		echo "perl: $(perl -e 'print $^V')"
		echo "go: $go_version"
		echo "host: $(uname -srm)"
		echo "binary_sha256: $(sha256sum "$dest/haproxy" | cut -d' ' -f1)"
		echo "built_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	} >"$dest/build-info.txt"
	rm -rf "$work"
	echo "built $dest/haproxy"
	cat "$dest/build-info.txt"
}

check_prereqs
mkdir -p "$dl"
setup_go
build_awslc

versions=("$@")
if [[ ${#versions[@]} -eq 0 ]]; then
	versions=("${DEFAULT_VERSIONS[@]}")
fi
for v in "${versions[@]}"; do
	build_haproxy "$v"
done
