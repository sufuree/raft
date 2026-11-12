#!/bin/sh
# Power-loss qualification for the Nanos unikernel (RFC-0001 §6.3, §11):
# boot the crashwrite image, kill QEMU after the durable+unsynced state is
# written, then boot crashverify on the SAME volume and check recovery.
#
# Usage: nanos-crash-test.sh <kernel.img> <crashwrite-image> <crashverify-image> <volume.raw> [qemu-binary] [accel]
set -e
KERNEL="$1"; WRITE_IMG="$2"; VERIFY_IMG="$3"; VOL="$4"
QEMU="${5:-qemu-system-aarch64}"
ACCEL="${6:-tcg}"
CPU="cortex-a72"
if [ "$ACCEL" = "kvm" ]; then CPU="host"; fi
DIR=$(mktemp -d)
trap 'rm -rf "$DIR"' EXIT

common_args() {
  echo "-machine virt -machine gic-version=2 -cpu $CPU -accel $ACCEL \
    -m 512M -smp 1 -kernel $KERNEL \
    -semihosting -device virtio-rng-pci \
      -no-reboot -display none -monitor none"
}

# Phase 1: write durable + unsynced state, then hang.
$QEMU $(common_args) \
  -drive file="$WRITE_IMG",format=raw,if=none,id=hd0 -device virtio-blk-pci,drive=hd0 \
  -drive file="$VOL",format=raw,if=none,id=hd1 -device virtio-blk-pci,drive=hd1 \
  -serial file:"$DIR/write.log" -daemonize -pidfile "$DIR/qemu.pid"
i=0
while [ $i -lt 120 ]; do
  if grep -q "crashwrite READY" "$DIR/write.log" 2>/dev/null; then break; fi
  sleep 1; i=$((i+1))
done
if ! grep -q "crashwrite READY" "$DIR/write.log" 2>/dev/null; then
  echo "CRASHTEST FAIL write phase never became ready"; cat "$DIR/write.log"; exit 1
fi
# Power loss: kill the guest without any clean shutdown.
kill -9 "$(cat "$DIR/qemu.pid")" 2>/dev/null || true
sleep 1
echo "CRASHTEST power-loss injected"

# Phase 2: recover on the same volume.
$QEMU $(common_args) \
  -drive file="$VERIFY_IMG",format=raw,if=none,id=hd0 -device virtio-blk-pci,drive=hd0 \
  -drive file="$VOL",format=raw,if=none,id=hd1 -device virtio-blk-pci,drive=hd1 \
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
