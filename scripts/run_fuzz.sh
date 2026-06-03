#!/usr/bin/env bash
set -euo pipefail

go_cmd="${GO:-go}"
fuzztime="${FUZZTIME:-1000x}"
fuzzparallel="${FUZZPARALLEL:-}"
found=0

for pkg in $("$go_cmd" list ./...); do
	targets=$("$go_cmd" test -run '^$' -list '^Fuzz' "$pkg" | sed -n '/^Fuzz/p')
	if [[ -z "$targets" ]]; then
		continue
	fi

	while IFS= read -r target; do
		[[ -z "$target" ]] && continue
		found=1
		args=(test -run '^$' -fuzz "^${target}$" -fuzztime "$fuzztime")
		if [[ -n "$fuzzparallel" ]]; then
			args+=(-parallel "$fuzzparallel")
			echo "fuzzing $pkg $target for $fuzztime with $fuzzparallel workers"
		else
			echo "fuzzing $pkg $target for $fuzztime"
		fi
		args+=("$pkg")
		"$go_cmd" "${args[@]}"
	done <<< "$targets"
done

if [[ "$found" -eq 0 ]]; then
	echo "no fuzz targets found" >&2
	exit 1
fi
