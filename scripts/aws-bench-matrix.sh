#!/bin/bash
# Comparative benchmark matrix: each (profile, impl) cell runs independently
# with a timeout so a stuck baseline cell cannot block the rest. EBS-backed
# directories (/tmp is tmpfs on AL2023).
set -x
BUCKET="${BUCKET:?BUCKET env required (temporary S3 transfer bucket)}"
export HOME=/root GOCACHE=/root/.cache/go-build GOPATH=/root/go
export PATH=$PATH:/usr/local/go/bin
exec > /tmp/bench-matrix.log 2>&1
cd /root/raft/bench/compare
go build -o /root/compare-bin . || exit 1
for p in P1 P2 P7; do
  for impl in ours hashicorp etcd; do
    d=/root/bench-matrix/$p-$impl
    rm -rf "$d"; mkdir -p "$d"
    timeout 180 /root/compare-bin -profile $p -impl $impl -dir "$d" > /root/bench-$p-$impl.json 2>/root/bench-$p-$impl.err
    echo "CELL $p $impl rc=$?"
    cat /root/bench-$p-$impl.json
  done
done
for f in /root/bench-*.json /root/bench-*.err; do
  aws s3 cp "$f" s3://${BUCKET}/$(basename $f)
done
echo "BENCH-MATRIX COMPLETE"
