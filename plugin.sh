#!/bin/sh
here="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
sys="$(uname -s)"
case "$sys" in
MINGW* | MSYS* | CYGWIN*) rel="x86_64-pc-windows-msvc/muse-sub.exe" ;;
Darwin*)
    case "$(uname -m)" in
    arm64) rel="aarch64-apple-darwin/muse-sub" ;;
    *) rel="x86_64-apple-darwin/muse-sub" ;;
    esac ;;
*)
    case "$(uname -m)" in
    aarch64 | arm64) rel="aarch64-unknown-linux-musl/muse-sub" ;;
    *) rel="x86_64-unknown-linux-musl/muse-sub" ;;
    esac ;;
esac
if [ -x "$here/$rel" ]; then exec "$here/$rel" "$@"; fi
for cand in "$here/target/release/muse-sub.exe" "$here/target/release/muse-sub" "$here/target/debug/muse-sub.exe" "$here/target/debug/muse-sub"; do
    if [ -x "$cand" ]; then exec "$cand" "$@"; fi
done
echo "muse-sub binary not found in $here" >&2
exit 1
