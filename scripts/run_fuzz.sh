#!/usr/bin/env bash
set -euo pipefail

go_cmd="${GO:-go}"
fuzztime="${FUZZTIME:-5s}"
found=0

for pkg in $("$go_cmd" list ./...); do
	targets=$("$go_cmd" test -run '^$' -list '^Fuzz' "$pkg" | sed -n '/^Fuzz/p')
	if [[ -z "$targets" ]]; then
		continue
	fi

	while IFS= read -r target; do
		[[ -z "$target" ]] && continue
		found=1
		echo "fuzzing $pkg $target for $fuzztime"
		"$go_cmd" test -run '^$' -fuzz "^${target}$" -fuzztime "$fuzztime" "$pkg"
	done <<< "$targets"
done

if [[ "$found" -eq 0 ]]; then
	echo "no fuzz targets found" >&2
	exit 1
fi
