#!/bin/bash
# Full qualification on the AWS Linux instance: reference-platform tests,
# native qualify, Nanos QEMU/KVM qualification (both substrates + power loss).
set -x
export HOME=/root
export GOCACHE=/root/.cache/go-build GOPATH=/root/go
export PATH=$PATH:/usr/local/go/bin:/root/.ops/bin
exec > /tmp/qual-results.log 2>&1

ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then GOARCH=arm64; QEMU_PKG=qemu-system-aarch64-core; else GOARCH=amd64; QEMU_PKG=qemu-system-x86-core; fi
dnf install -y gcc git $QEMU_PKG tar gzip || exit 1
cd /root
curl -sL https://go.dev/dl/go1.27.4.linux-$GOARCH.tar.gz -o go.tar.gz
tar -C /usr/local -xzf go.tar.gz
go version
# Install ops directly (the get.sh installer insists on x86 QEMU even on arm64).
if [ "$GOARCH" = "arm64" ]; then OPS_ARCH=aarch64; else OPS_ARCH=x86_64; fi
curl -sSfL "https://storage.googleapis.com/cli/linux/$OPS_ARCH/ops" -o /usr/local/bin/ops || exit 1
chmod +x /usr/local/bin/ops
ops version || true

rm -rf /root/raft
mkdir -p /root/raft && cd /root/raft
aws s3 cp s3://raft-qual-1794435910-27473/raft-src.tar.gz . || exit 1
tar xzf raft-src.tar.gz
gofmt -l . ; go vet ./... && go build ./... || exit 1
go test ./... -count=1 -timeout 300s || exit 1
CGO_ENABLED=1 go test -race ./... -count=1 -timeout 400s || exit 1

# Linux reference receipt: native qualify.
mkdir -p /tmp/qual-data
go run ./cmd/qualify || exit 1
QUAL_DIR=/tmp/qual-data go run ./cmd/qualify || exit 1

# Nanos image build + KVM qualification (architecture matches the host).
ls -la /dev/kvm || { echo "NO KVM"; exit 1; }
go build -o /root/qualify-bin ./cmd/qualify
mkdir -p /root/nanos && cd /root/nanos
cp /root/qualify-bin qualify
cat > config.json <<'JSON'
{
  "Files": ["qualify"],
  "Args": ["qualify"],
  "Mounts": {"qualvol": "/data"},
  "RunConfig": {"Memory": "512M", "CPUs": 1}
}
JSON
ops volume create qualvol -s 128M
ops build --arch $GOARCH -c config.json qualify || exit 1
cp /root/.ops/images/qualify /root/nanos/image-full
cat > crash-write.json <<'JSON'
{
  "Files": ["qualify"],
  "Args": ["qualify"],
  "Env": {"QUAL_MODE": "crashwrite"},
  "Mounts": {"qualvol": "/data"},
  "RunConfig": {"Memory": "512M", "CPUs": 1}
}
JSON
cat > crash-verify.json <<'JSON'
{
  "Files": ["qualify"],
  "Args": ["qualify"],
  "Env": {"QUAL_MODE": "crashverify"},
  "Mounts": {"qualvol": "/data"},
  "RunConfig": {"Memory": "512M", "CPUs": 1}
}
JSON
ops build --arch $GOARCH -c crash-write.json qualify && mv /root/.ops/images/qualify image-crashwrite
ops build --arch $GOARCH -c crash-verify.json qualify && mv /root/.ops/images/qualify image-crashverify

if [ "$GOARCH" = "arm64" ]; then KERNEL=$(find /root/.ops -path "*arm*" -name kernel.img | head -1); else KERNEL=$(find /root/.ops -name kernel.img | grep -v arm | head -1); fi
VOL=$(find /root/.ops/volumes -name "qualvol*" | head -1)
echo "KERNEL=$KERNEL VOL=$VOL"

# Full suite under KVM.
if [ "$GOARCH" = "arm64" ]; then
  timeout 180 qemu-system-aarch64 -machine virt -machine gic-version=2 -accel kvm -cpu host -m 512M -smp 1 \
    -kernel "$KERNEL" \
    -drive file=/root/nanos/image-full,format=raw,if=none,id=hd0 -device virtio-blk-pci,drive=hd0 \
    -drive file="$VOL",format=raw,if=none,id=hd1 -device virtio-blk-pci,drive=hd1 \
    -semihosting -device virtio-rng-pci \
    -nographic -no-reboot > /root/nanos/kvm-full.log 2>&1
else
  timeout 180 qemu-system-x86_64 -machine q35 -accel kvm -cpu host -m 512M -smp 1 \
    -kernel "$KERNEL" \
    -device virtio-scsi-pci,id=scsi0 \
    -drive file=/root/nanos/image-full,format=raw,if=none,id=hd0 -device scsi-hd,bus=scsi0.0,drive=hd0 \
    -drive file="$VOL",format=raw,if=none,id=hd1 -device scsi-hd,bus=scsi0.0,drive=hd1 \
    -device virtio-rng-pci \
    -nographic -no-reboot -device isa-debug-exit > /root/nanos/kvm-full.log 2>&1
fi
cat /root/nanos/kvm-full.log

# Power-loss crash test under KVM.
cd /root/raft
if [ "$GOARCH" = "arm64" ]; then
  sh scripts/nanos-crash-test.sh "$KERNEL" /root/nanos/image-crashwrite /root/nanos/image-crashverify "$VOL" qemu-system-aarch64 kvm
else
  sh scripts/nanos-crash-test-x86.sh "$KERNEL" /root/nanos/image-crashwrite /root/nanos/image-crashverify "$VOL"
fi

# Record platform pins.
{
  echo "=== PLATFORM PINS ==="
  go version
  uname -a
  if [ "$GOARCH" = "arm64" ]; then qemu-system-aarch64 --version | head -1; else qemu-system-x86_64 --version | head -1; fi
  ops version 2>/dev/null || true
  cat /etc/os-release | head -2
  ls /root/.ops | head
  nproc
  free -m
} > /root/nanos/pins.txt
cat /root/nanos/pins.txt

aws s3 cp /tmp/qual-results.log s3://raft-qual-1794435910-27473/qual-results.log
aws s3 cp /root/nanos/kvm-full.log s3://raft-qual-1794435910-27473/kvm-full.log
aws s3 cp /root/nanos/pins.txt s3://raft-qual-1794435910-27473/pins.txt
echo "AWS-QUALIFY COMPLETE"
