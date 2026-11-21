#!/bin/sh
# crosscheck.sh REF_BIN NEW_BIN DATASET... : runs both binaries on each dataset and diffs outputs.
set -eu
ref=$1; new=$2; shift 2
here=$(cd "$(dirname "$0")" && pwd)
py=${PYTHON:-python3}
tmp=$(mktemp -d)
for d in "$@"; do
  rm -rf "$tmp/a" "$tmp/b"
  DATA_DIR="$d" OUTPUT_DIR="$tmp/a" "$ref"
  DATA_DIR="$d" OUTPUT_DIR="$tmp/b" "$new"
  printf '%s: ' "$d"; (cd "$here" && "$py" compare.py "$tmp/a" "$tmp/b" | tail -1)
done
rm -rf "$tmp"
