#!/bin/bash
# Nanos QEMU/TCG qualification on Linux (KVM-pending fallback receipt) plus
# comparative benchmarks on the Linux reference platform.
set -x
BUCKET="${BUCKET:?BUCKET env required (temporary S3 transfer bucket)}"
export HOME=/root
export GOCACHE=/root/.cache/go-build GOPATH=/root/go
export PATH=$PATH:/usr/local/go/bin:/usr/local/bin
exec > /tmp/tcg-bench.log 2>&1
cd /root/raft
ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then GOARCH=arm64; QEMU=qemu-system-aarch64; else GOARCH=amd64; QEMU=qemu-system-x86_64; fi

# Build Nanos images.
go build -o /root/qualify-bin ./cmd/qualify || exit 1
mkdir -p /root/nanos && cd /root/nanos
cp /root/qualify-bin qualify
cat > config.json <<'JSON'
{"Files":["qualify"],"Args":["qualify"],"Mounts":{"qualvol":"/data"},"RunConfig":{"Memory":"512M","CPUs":1}}
JSON
cat > crash-write.json <<'JSON'
{"Files":["qualify"],"Args":["qualify"],"Env":{"QUAL_MODE":"crashwrite"},"Mounts":{"qualvol":"/data"},"RunConfig":{"Memory":"512M","CPUs":1}}
JSON
cat > crash-verify.json <<'JSON'
{"Files":["qualify"],"Args":["qualify"],"Env":{"QUAL_MODE":"crashverify"},"Mounts":{"qualvol":"/data"},"RunConfig":{"Memory":"512M","CPUs":1}}
JSON
ops volume create qualvol -s 128M 2>/dev/null || true
ops build --arch $GOARCH -c config.json qualify && cp /root/.ops/images/qualify image-full
ops build --arch $GOARCH -c crash-write.json qualify && mv /root/.ops/images/qualify image-crashwrite
ops build --arch $GOARCH -c crash-verify.json qualify && mv /root/.ops/images/qualify image-crashverify
if [ "$GOARCH" = "arm64" ]; then KERNEL=$(find /root/.ops -path "*arm*" -name kernel.img | head -1); else KERNEL=$(find /root/.ops -name kernel.img | grep -v arm | head -1); fi
VOL=$(find /root/.ops/volumes -name "qualvol*" | head -1)
echo "KERNEL=$KERNEL VOL=$VOL"

# Full suite under TCG.
if [ "$GOARCH" = "arm64" ]; then
  timeout 300 $QEMU -machine virt -machine gic-version=2 -cpu cortex-a72 -accel tcg -m 512M -smp 1 \
    -kernel "$KERNEL" \
    -drive file=/root/nanos/image-full,format=raw,if=none,id=hd0 -device virtio-blk-pci,drive=hd0 \
    -drive file="$VOL",format=raw,if=none,id=hd1 -device virtio-blk-pci,drive=hd1 \
    -semihosting -device virtio-rng-pci \
    -nographic -no-reboot > /root/nanos/tcg-full.log 2>&1
else
  timeout 300 $QEMU -machine q35 -accel tcg -cpu qemu64 -m 512M -smp 1 \
    -kernel "$KERNEL" -device virtio-scsi-pci,id=scsi0 \
    -drive file=/root/nanos/image-full,format=raw,if=none,id=hd0 -device scsi-hd,bus=scsi0.0,drive=hd0 \
    -drive file="$VOL",format=raw,if=none,id=hd1 -device scsi-hd,bus=scsi0.0,drive=hd1 \
    -device virtio-rng-pci \
    -nographic -no-reboot -device isa-debug-exit > /root/nanos/tcg-full.log 2>&1
fi
cat /root/nanos/tcg-full.log

# Power-loss crash test under TCG.
cd /root/raft
if [ "$GOARCH" = "arm64" ]; then
  sh scripts/nanos-crash-test.sh "$KERNEL" /root/nanos/image-crashwrite /root/nanos/image-crashverify "$VOL" $QEMU tcg
else
  sh scripts/nanos-crash-test-x86.sh "$KERNEL" /root/nanos/image-crashwrite /root/nanos/image-crashverify "$VOL" $QEMU
fi

# Comparative benchmarks (Linux reference platform).
cd /root/raft/bench/compare
go mod download || exit 1
# EBS-backed directory (/tmp is tmpfs on AL2023 — not a durable substrate).
for p in P1 P2 P7; do
  rm -rf /root/bench-$p
  mkdir -p /root/bench-$p
  go run . -profile $p -impl all -dir /root/bench-$p 2>/root/nanos/bench-$p.err > /root/nanos/bench-$p.json
  cat /root/nanos/bench-$p.json
  tail -3 /root/nanos/bench-$p.err
done

{
  echo "=== PLATFORM PINS ==="
  go version; uname -a
  $QEMU --version | head -1
  ops version 2>/dev/null || true
  head -2 /etc/os-release
  ls /root/.ops
  nproc; free -m
} > /root/nanos/pins.txt
cat /root/nanos/pins.txt
aws s3 cp /tmp/tcg-bench.log s3://${BUCKET}/tcg-bench.log
for f in tcg-full.log pins.txt bench-P1.json bench-P2.json bench-P7.json bench-P1.err bench-P2.err bench-P7.err; do
  aws s3 cp /root/nanos/$f s3://${BUCKET}/$f
done
echo "TCG-BENCH COMPLETE"
