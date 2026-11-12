#!/bin/bash
# RFC-0001 pinned QEMU/KVM unikernel qualification on bare metal (x86_64).
# Runs the Linux reference receipt, the Nanos KVM full suite (both substrates +
# filesystem-substrate primitives), the power-loss crash test, the in-guest
# benchmark profiles, idle-CPU, and the bbolt incumbent-storage probe. Captures
# host-side VMM RSS/CPU for guest-total memory and idle accounting.
# BUCKET env is required (temporary S3 transfer bucket).
set -x
export HOME=/root
export GOCACHE=/root/.cache/go-build GOPATH=/root/go
export PATH=$PATH:/usr/local/go/bin:/root/.ops/bin
exec > /tmp/metal-qual.log 2>&1
BUCKET="${BUCKET:?BUCKET env required}"
ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then GOARCH=arm64; QEMU=qemu-system-aarch64; else GOARCH=amd64; QEMU=qemu-system-x86_64; fi

# Full QEMU (the -core packages may lack features; AL2023 QEMU has no slirp,
# and the qualify suite needs no NIC — it uses the in-process LoopNetwork).
dnf install -y gcc git qemu-system-x86 tar gzip || exit 1
cd /root
curl -sL https://go.dev/dl/go1.27.4.linux-$GOARCH.tar.gz -o go.tar.gz
tar -C /usr/local -xzf go.tar.gz
go version
if [ "$GOARCH" = "arm64" ]; then OPS_ARCH=aarch64; else OPS_ARCH=x86_64; fi
curl -sSfL "https://storage.googleapis.com/cli/linux/$OPS_ARCH/ops" -o /usr/local/bin/ops || exit 1
chmod +x /usr/local/bin/ops
ops version || true

# KVM availability (bare metal guarantees vmx; load the module if needed).
modprobe kvm_intel 2>/dev/null || modprobe kvm 2>/dev/null || true
ls -la /dev/kvm || { echo "NO KVM"; exit 1; }
grep -m1 -o vmx /proc/cpuinfo || true

rm -rf /root/raft && mkdir -p /root/raft && cd /root/raft
aws s3 cp s3://$BUCKET/raft-src.tar.gz . || exit 1
tar xzf raft-src.tar.gz
gofmt -l . ; go vet ./... && go build ./... || exit 1
go test ./... -count=1 -timeout 300s || exit 1
CGO_ENABLED=1 go test -race ./... -count=1 -timeout 400s || exit 1

# Linux reference receipt: native qualify on EBS (not tmpfs).
mkdir -p /root/qual-data
QUAL_DIR=/root/qual-data go run ./cmd/qualify || exit 1

# Build the Nanos images.
go build -o /root/qualify-bin ./cmd/qualify || exit 1
(cd bench/compare && go build -o /root/bboltprobe-bin ./bboltprobe) || exit 1
mkdir -p /root/nanos && cd /root/nanos
cp /root/qualify-bin qualify
cp /root/bboltprobe-bin bboltprobe
mkjson() { # name mode-or-args
  cat > "$1" <<JSON
{"Files":["qualify"],"Args":["qualify"],"Env":{"QUAL_MODE":"$2"},"Mounts":{"qualvol":"/data"},"RunConfig":{"Memory":"512M","CPUs":1}}
JSON
}
cat > full.json <<'JSON'
{"Files":["qualify"],"Args":["qualify"],"Mounts":{"qualvol":"/data"},"RunConfig":{"Memory":"512M","CPUs":1}}
JSON
mkjson crashwrite.json crashwrite
mkjson crashverify.json crashverify
mkjson bench.json bench
mkjson idle.json idle
cat > bboltprobe.json <<'JSON'
{"Files":["bboltprobe"],"Args":["bboltprobe"],"Mounts":{"qualvol":"/data"},"RunConfig":{"Memory":"512M","CPUs":1}}
JSON
ops volume create qualvol -s 128M
# Larger volume for the benchmark profiles (archive-class logs + 64 MiB snapshot).
ops volume create benchvol -s 2G
ops build --arch $GOARCH -c full.json qualify && cp /root/.ops/images/qualify image-full
ops build --arch $GOARCH -c crashwrite.json qualify && mv /root/.ops/images/qualify image-crashwrite
ops build --arch $GOARCH -c crashverify.json qualify && mv /root/.ops/images/qualify image-crashverify
ops build --arch $GOARCH -c bench.json qualify && mv /root/.ops/images/qualify image-bench
ops build --arch $GOARCH -c idle.json qualify && mv /root/.ops/images/qualify image-idle
ops build --arch $GOARCH -c bboltprobe.json bboltprobe && mv /root/.ops/images/bboltprobe image-bboltprobe

if [ "$GOARCH" = "arm64" ]; then KERNEL=$(find /root/.ops -path "*arm*" -name kernel.img | head -1); else KERNEL=$(find /root/.ops -name kernel.img | grep -v arm | head -1); fi
VOL=$(find /root/.ops/volumes -name "qualvol*" | head -1)
BVOL=$(find /root/.ops/volumes -name "benchvol*" | head -1)
echo "KERNEL=$KERNEL VOL=$VOL BVOL=$BVOL"

# Run a guest image under KVM, sampling host VMM RSS (guest-total memory:
# heap + mappings + kernel cache all live in the QEMU address space). $1=image
# $2=volume $3=logfile $4=timeout. Writes peak RSS to <logfile>.rss.
run_kvm() {
  local img="$1" vol="$2" log="$3" tmo="$4"
  if [ "$GOARCH" = "arm64" ]; then
    timeout "$tmo" $QEMU -machine virt -machine gic-version=2 -accel kvm -cpu host -m 512M -smp 1 \
      -kernel "$KERNEL" \
      -drive file="$img",format=raw,if=none,id=hd0 -device virtio-blk-pci,drive=hd0 \
      -drive file="$vol",format=raw,if=none,id=hd1 -device virtio-blk-pci,drive=hd1 \
      -semihosting -device virtio-rng-pci -nographic -no-reboot > "$log" 2>&1 &
  else
    timeout "$tmo" $QEMU -machine q35 -accel kvm -cpu host -m 512M -smp 1 \
      -kernel "$KERNEL" -device virtio-scsi-pci,id=scsi0 \
      -drive file="$img",format=raw,if=none,id=hd0 -device scsi-hd,bus=scsi0.0,drive=hd0 \
      -drive file="$vol",format=raw,if=none,id=hd1 -device scsi-hd,bus=scsi0.0,drive=hd1 \
      -device virtio-rng-pci -nographic -no-reboot -device isa-debug-exit > "$log" 2>&1 &
  fi
  local qpid=$!
  local peak=0
  while kill -0 "$qpid" 2>/dev/null; do
    local rss=$(ps -o rss= -p "$qpid" 2>/dev/null | tr -d ' ')
    [ -n "$rss" ] && [ "$rss" -gt "$peak" ] && peak=$rss
    sleep 1
  done
  wait "$qpid"
  echo "peak_rss_kb=$peak" > "$log.rss"
}

# Full suite under KVM (both substrates + filesystem primitives).
run_kvm /root/nanos/image-full "$VOL" /root/nanos/kvm-full.log 240
cat /root/nanos/kvm-full.log /root/nanos/kvm-full.log.rss

# Power-loss crash test under KVM.
cd /root/raft
if [ "$GOARCH" = "arm64" ]; then
  sh scripts/nanos-crash-test.sh "$KERNEL" /root/nanos/image-crashwrite /root/nanos/image-crashverify "$VOL" $QEMU kvm
else
  sh scripts/nanos-crash-test-x86.sh "$KERNEL" /root/nanos/image-crashwrite /root/nanos/image-crashverify "$VOL" $QEMU
fi

# In-guest benchmark profiles under KVM (large volume).
run_kvm /root/nanos/image-bench "$BVOL" /root/nanos/kvm-bench.log 600
cat /root/nanos/kvm-bench.log /root/nanos/kvm-bench.log.rss

# bbolt incumbent-storage probe under KVM.
run_kvm /root/nanos/image-bboltprobe "$VOL" /root/nanos/kvm-bboltprobe.log 180
cat /root/nanos/kvm-bboltprobe.log

# Idle CPU: sample the VMM process CPU over the idle window (host-side).
if [ "$GOARCH" = "arm64" ]; then
  timeout 120 $QEMU -machine virt -machine gic-version=2 -accel kvm -cpu host -m 512M -smp 1 \
    -kernel "$KERNEL" \
    -drive file=/root/nanos/image-idle,format=raw,if=none,id=hd0 -device virtio-blk-pci,drive=hd0 \
    -drive file="$VOL",format=raw,if=none,id=hd1 -device virtio-blk-pci,drive=hd1 \
    -semihosting -device virtio-rng-pci -nographic -no-reboot > /root/nanos/kvm-idle.log 2>&1 &
else
  timeout 120 $QEMU -machine q35 -accel kvm -cpu host -m 512M -smp 1 \
    -kernel "$KERNEL" -device virtio-scsi-pci,id=scsi0 \
    -drive file=/root/nanos/image-idle,format=raw,if=none,id=hd0 -device scsi-hd,bus=scsi0.0,drive=hd0 \
    -drive file="$VOL",format=raw,if=none,id=hd1 -device scsi-hd,bus=scsi0.0,drive=hd1 \
    -device virtio-rng-pci -nographic -no-reboot -device isa-debug-exit > /root/nanos/kvm-idle.log 2>&1 &
fi
IPID=$!
# Wait for the idle window to begin, then sample utime+stime deltas over 50 s.
for i in $(seq 1 60); do grep -q "idle BEGIN" /root/nanos/kvm-idle.log 2>/dev/null && break; sleep 1; done
read u1 s1 < <(awk '{print $14, $15}' /proc/$IPID/stat 2>/dev/null)
sleep 50
read u2 s2 < <(awk '{print $14, $15}' /proc/$IPID/stat 2>/dev/null)
HZ=$(getconf CLK_TCK)
echo "idle_cpu_ticks=$(( (u2-u1)+(s2-s1) )) hz=$HZ window=50s" > /root/nanos/kvm-idle.cpu
wait "$IPID" 2>/dev/null
cat /root/nanos/kvm-idle.log /root/nanos/kvm-idle.cpu

# Platform pins.
{
  echo "=== PLATFORM PINS ==="
  go version; uname -a
  $QEMU --version | head -1
  ops version 2>/dev/null || true
  head -2 /etc/os-release
  ls /root/.ops | head
  nproc; free -m
  grep -m1 "model name" /proc/cpuinfo
} > /root/nanos/pins.txt
cat /root/nanos/pins.txt

# Upload evidence.
aws s3 cp /tmp/metal-qual.log s3://$BUCKET/metal-qual.log
for f in kvm-full.log kvm-full.log.rss kvm-bench.log kvm-bench.log.rss kvm-bboltprobe.log kvm-idle.log kvm-idle.cpu pins.txt; do
  aws s3 cp /root/nanos/$f s3://$BUCKET/$f
done
echo "METAL-QUALIFY COMPLETE"
