#!/usr/bin/env bash
# Fresh scratch per invocation: historical C++ writes fixed-name sidecars.
set -euo pipefail
if [[ $# != 4 ]]; then
  echo 'usage: bash e2e.sh DATASET CONFIG CPP_EXE RUST_EXE' >&2
  exit 2
fi
dataset=$(readlink -f "$1")
config=$(readlink -f "$2")
cpp=$(readlink -f "$3")
rust=$(readlink -f "$4")
cpu=${BENCH_CPU:-2}
scratch=$(mktemp -d /tmp/blitzcrank-rust-acceptance.XXXXXX)
echo "Results: $scratch"
ln -s "$dataset" "$scratch/input.csv"
ln -s "$config" "$scratch/input.config"
printf 'mode,backend,run,phase,wall_s,user_s,system_s,max_rss_kib,total_compressed_bytes\n' > "$scratch/results.csv"
sha256sum "$dataset" "$config" "$cpp" "$rust" > "$scratch/inputs.sha256"
for mode in bulk record; do
  for run in 1 2 3; do
    order='cpp rust'
    if (( run % 2 == 0 )); then order='rust cpp'; fi
    for backend in $order; do
      work="$scratch/$mode-$backend-$run"
      mkdir "$work"
      (
        cd "$work"
        if [[ $backend == cpp ]]; then
          threshold=20000
          if [[ $mode == record ]]; then threshold=1; fi
          /usr/bin/time -f '%e,%U,%S,%M' -o encode.time taskset -c "$cpu" "$cpp" -c ../input.csv payload.bin ../input.config 0 1 "$threshold" > encode.log 2>&1
          /usr/bin/time -f '%e,%U,%S,%M' -o decode.time taskset -c "$cpu" "$cpp" -d payload.bin restored.csv ../input.config 0 "$threshold" > decode.log 2>&1
          total=$(stat -c '%s' payload.bin _enum.dat _temp.index | awk '{s+=$1} END {print s}')
          sha256sum payload.bin _enum.dat _temp.index > artifacts.sha256
        else
          rows=256
          lanes=1
          if [[ $mode == record ]]; then rows=1; lanes=1; fi
          /usr/bin/time -f '%e,%U,%S,%M' -o encode.time taskset -c "$cpu" "$rust" compress ../input.csv ../input.config payload.bin "$rows" "$lanes" > encode.log 2>&1
          /usr/bin/time -f '%e,%U,%S,%M' -o decode.time taskset -c "$cpu" "$rust" decompress payload.bin restored.csv > decode.log 2>&1
          total=$(stat -c '%s' payload.bin)
          sha256sum payload.bin > artifacts.sha256
        fi
        cmp ../input.csv restored.csv
        for phase in encode decode; do
          printf '%s,%s,%s,%s,%s,%s\n' "$mode" "$backend" "$run" "$phase" "$(<"$phase.time")" "$total" >> "$scratch/results.csv"
        done
      )
      echo "Verified mode=$mode backend=$backend run=$run"
      paste "$work/encode.time" "$work/decode.time"
    done
  done
done
for mode in bulk record; do
  for backend in cpp rust; do
    for run in 2 3; do
      diff "$scratch/$mode-$backend-1/artifacts.sha256" "$scratch/$mode-$backend-$run/artifacts.sha256"
    done
  done
done
echo "All 12 full-data roundtrips passed. Results: $scratch/results.csv"
