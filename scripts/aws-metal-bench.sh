#!/bin/bash
# RFC-0001 comparative benchmark matrix on the Linux reference platform (EBS
# gp3, GOMAXPROCS=1). Each (profile, impl, voters) cell runs independently with
# a timeout so a stuck baseline cell cannot block the matrix. /tmp is tmpfs on
# AL2023 — all cells use EBS-backed directories. BUCKET env is required.
set -x
export HOME=/root GOCACHE=/root/.cache/go-build GOPATH=/root/go
export PATH=$PATH:/usr/local/go/bin
exec > /tmp/metal-bench.log 2>&1
BUCKET="${BUCKET:?BUCKET env required}"
cd /root/raft/bench/compare
go build -o /root/compare-bin . || exit 1
go build -o /root/bboltprobe-bin ./bboltprobe || exit 1

mkdir -p /root/bench-out
# cell: profile impl voters archive-mb timeout
run_cell() {
  local p="$1" impl="$2" v="$3" mb="$4" tmo="$5"
  local tag="$p-$impl-v$v"
  [ "$mb" != "0" ] && tag="$p-$impl-v$v-${mb}m"
  local d=/root/bench-matrix/$tag
  rm -rf "$d"; mkdir -p "$d"
  local extra=""
  [ "$mb" != "0" ] && extra="-archive-mb $mb"
  timeout "$tmo" /root/compare-bin -profile "$p" -impl "$impl" -voters "$v" $extra -dir "$d" \
    > /root/bench-out/$tag.json 2>/root/bench-out/$tag.err
  echo "CELL $tag rc=$?"
  cat /root/bench-out/$tag.json
}

for impl in ours hashicorp etcd; do
  run_cell P1 "$impl" 1 0 180
  run_cell P1 "$impl" 3 0 180
  run_cell P2 "$impl" 1 0 180
  run_cell P2 "$impl" 3 0 180
  run_cell P3 "$impl" 3 0 180
  run_cell P4 "$impl" 1 8 240
  run_cell P4 "$impl" 3 8 240
  run_cell P4 "$impl" 1 32 300
  run_cell P4 "$impl" 3 32 300
  run_cell P5 "$impl" 3 0 240
  run_cell P6 "$impl" 3 0 300
  run_cell P7 "$impl" 1 0 300
  run_cell P8 "$impl" 1 0 180
  run_cell P8 "$impl" 3 0 180
done

# Native bbolt probe on Linux (reference; the Nanos run is in metal-qualify).
PROBE_DIR=/root/bench-out/bboltprobe-linux timeout 120 /root/bboltprobe-bin > /root/bench-out/bboltprobe-linux.log 2>&1
echo "BBOLTPROBE-LINUX rc=$?"

for f in /root/bench-out/*.json /root/bench-out/*.err /root/bench-out/*.log; do
  aws s3 cp "$f" s3://$BUCKET/$(basename "$f")
done
aws s3 cp /tmp/metal-bench.log s3://$BUCKET/metal-bench.log
echo "METAL-BENCH COMPLETE"
