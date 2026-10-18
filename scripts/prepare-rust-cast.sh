#!/bin/sh
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
build="$root/.build"
patch_file="$root/patches/rust_cast/compatibility.patch"
crate=rust_cast-0.21.0.crate
expected_sha=235d09067a35143d271fdeaf28298b4f729b5da50abdbf3b9e6395262d8bc7a9
url="https://static.crates.io/crates/rust_cast/$crate"

fail() {
    printf 'prepare-rust-cast: %s\n' "$*" >&2
    exit 1
}

mkdir -p "$build"
exec 9> "$build/prepare-rust-cast.lock"
flock --exclusive --timeout 120 9 || fail 'could not acquire preparation lock within 120 seconds'

[ -f "$patch_file" ] || fail "missing patch: $patch_file"
patch_sha=$(sha256sum "$patch_file")
patch_sha=${patch_sha%% *}
fingerprint="$expected_sha-$patch_sha"
generation="$build/.rust_cast-$fingerprint"
stamp=.prepare-rust-cast.stamp

is_prepared() {
    [ -f "$1/$stamp" ] &&
        [ "$(cat "$1/$stamp")" = "$fingerprint" ] &&
        [ -f "$1/Cargo.toml" ] && [ -f "$1/LICENSE" ]
}

if is_prepared "$build/rust_cast"; then
    exit 0
fi

# A symlink can be replaced atomically; a nonempty directory cannot.
if [ -e "$build/rust_cast" ] && [ ! -L "$build/rust_cast" ]; then
    fail "$build/rust_cast must be absent or a generated symlink"
fi

work=$(mktemp -d "$build/.prepare-rust-cast.XXXXXX")
trap 'rm -rf -- "$work"' 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

if ! is_prepared "$generation"; then
    [ ! -e "$generation" ] && [ ! -L "$generation" ] ||
        fail "incomplete generated directory; remove it before retrying: $generation"

    cargo_home=${CARGO_HOME:-${HOME:?set HOME or CARGO_HOME}/.cargo}
    archive=
    # Read the shared Cargo cache, but never write or extract into it.
    for candidate in "$cargo_home"/registry/cache/*/"$crate"; do
        [ -f "$candidate" ] || continue
        archive=$candidate
        break
    done

    if [ -z "$archive" ]; then
        archive="$work/$crate"
        curl --fail --location --connect-timeout 15 --max-time 120 \
            --output "$archive" "$url"
    fi
    printf '%s  %s\n' "$expected_sha" "$archive" | sha256sum --check --status ||
        fail "SHA-256 mismatch: $archive"

    tar -xzf "$archive" -C "$work"
    source_dir="$work/rust_cast-0.21.0"
    [ -f "$source_dir/LICENSE" ] || fail 'upstream archive is missing LICENSE'
    patch --batch --forward --fuzz=0 -p1 -d "$source_dir" < "$patch_file"
    [ -f "$source_dir/LICENSE" ] || fail 'patch removed upstream LICENSE'

    # Refuse to stamp a tree if the patch changed during preparation.
    applied_sha=$(sha256sum "$patch_file")
    applied_sha=${applied_sha%% *}
    [ "$applied_sha" = "$patch_sha" ] || fail 'patch changed during preparation; retry'
    printf '%s\n' "$fingerprint" > "$source_dir/$stamp"
    mv -- "$source_dir" "$generation"
fi

# Keep previous generations so preparation does not delete sources in use.
ln -s -- ".rust_cast-$fingerprint" "$work/publish"
mv -Tf -- "$work/publish" "$build/rust_cast"
printf 'Prepared rust_cast 0.21.0 at %s\n' "$build/rust_cast"
