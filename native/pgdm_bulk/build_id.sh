#!/usr/bin/env bash
# Hash every local C source/header, including the decoder include fragment.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
source_hash=$(find . -maxdepth 1 -type f \( -name '*.c' -o -name '*.h' -o -name '*.inc' \) -printf '%f\n' | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -d' ' -f1)
prefix=$(sed -n 's/^#define PGDM_BULK_BUILD_PREFIX "\(.*\)"/\1/p' pgdm_bulk.c)
[[ -n $prefix ]]
if [[ ${1:-} == --hash ]]; then printf '%s\n' "$source_hash";
else printf '%s%s\n' "$prefix" "$source_hash"; fi
