#!/bin/sh
# x86_64 QEMU/KVM power-loss qualification (RFC-0001 §6.3, §11).
# Usage: nanos-crash-test-x86.sh <kernel.img> <crashwrite-image> <crashverify-image> <volume.raw>
set -e
KERNEL="$1"; WRITE_IMG="$2"; VERIFY_IMG="$3"; VOL="$4"
QEMU="${5:-qemu-system-x86_64}"
DIR=$(mktemp -d)
trap 'rm -rf "$DIR"' EXIT

common_args() {
  echo "-machine q35 -accel kvm -cpu host \
    -m 512M -smp 1 -kernel $KERNEL \
    -device virtio-rng-pci \
    -netdev user,id=n0 -device virtio-net-pci,netdev=n0 \
    -no-reboot -display none -monitor none -device isa-debug-exit"
}

$QEMU $(common_args) \
  -device virtio-scsi-pci,id=scsi0 \
  -drive file="$WRITE_IMG",format=raw,if=none,id=hd0 -device scsi-hd,bus=scsi0.0,drive=hd0 \
  -drive file="$VOL",format=raw,if=none,id=hd1 -device scsi-hd,bus=scsi0.0,drive=hd1 \
  -serial file:"$DIR/write.log" -daemonize -pidfile "$DIR/qemu.pid"
i=0
while [ $i -lt 120 ]; do
  if grep -q "crashwrite READY" "$DIR/write.log" 2>/dev/null; then break; fi
  sleep 1; i=$((i+1))
done
if ! grep -q "crashwrite READY" "$DIR/write.log" 2>/dev/null; then
  echo "CRASHTEST FAIL write phase never became ready"; cat "$DIR/write.log"; exit 1
fi
kill -9 "$(cat "$DIR/qemu.pid")" 2>/dev/null || true
sleep 1
echo "CRASHTEST power-loss injected"

$QEMU $(common_args) \
  -device virtio-scsi-pci,id=scsi0 \
  -drive file="$VERIFY_IMG",format=raw,if=none,id=hd0 -device scsi-hd,bus=scsi0.0,drive=hd0 \
  -drive file="$VOL",format=raw,if=none,id=hd1 -device scsi-hd,bus=scsi0.0,drive=hd1 \
  -serial file:"$DIR/verify.log" -daemonize -pidfile "$DIR/qemu2.pid"
i=0
while [ $i -lt 120 ]; do
  if grep -q "QUAL DONE" "$DIR/verify.log" 2>/dev/null; then break; fi
  sleep 1; i=$((i+1))
done
kill -9 "$(cat "$DIR/qemu2.pid")" 2>/dev/null || true
cat "$DIR/verify.log"
if grep -q "QUAL crashverify PASS" "$DIR/verify.log"; then
  echo "CRASHTEST PASS"
else
  echo "CRASHTEST FAIL"; exit 1
fi
