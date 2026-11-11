package wal

import (
	"errors"
	"os"
	"sync"
)

// Device is the storage substrate (§6.3): a fixed or growable byte space with
// explicit synchronization. Both file and raw block-device bindings implement
// it; protocol semantics must not depend on the choice.
type Device interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	// Sync is the durable receipt; a successful WriteAt alone is not durable.
	Sync() error
	Size() (int64, error)
	Truncate(size int64) error
}

// FileDevice binds the WAL to a file on a filesystem.
type FileDevice struct {
	f  *os.File
	mu sync.Mutex
}

// OpenFile opens or creates a WAL file.
func OpenFile(path string) (*FileDevice, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	return &FileDevice{f: f}, nil
}

func (d *FileDevice) ReadAt(p []byte, off int64) (int, error)  { return d.f.ReadAt(p, off) }
func (d *FileDevice) WriteAt(p []byte, off int64) (int, error) { return d.f.WriteAt(p, off) }
func (d *FileDevice) Sync() error                              { return d.f.Sync() }
func (d *FileDevice) Size() (int64, error) {
	fi, err := d.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
func (d *FileDevice) Truncate(size int64) error { return d.f.Truncate(size) }
func (d *FileDevice) Close() error              { return d.f.Close() }

// MemDevice is an in-memory device for crash simulation. It can inject torn
// writes and drop unsynchronized data on crash.
type MemDevice struct {
	mu       sync.Mutex
	data     []byte
	durable  []byte // last synced prefix
	TornTail int    // if >0, the next WriteAt truncates its last TornTail bytes
	FailSync bool
	// OnSync, when set, runs after each successful Sync with the cumulative
	// sync count; tests use it to crash at exact durability boundaries.
	OnSync    func(n int)
	SyncCount int
	crashed   bool
}

// NewMemDevice creates an empty memory device.
func NewMemDevice() *MemDevice { return &MemDevice{} }

func (d *MemDevice) ReadAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if off >= int64(len(d.data)) {
		return 0, errEOF
	}
	n := copy(p, d.data[off:])
	if n < len(p) {
		return n, errEOF
	}
	return n, nil
}

func (d *MemDevice) WriteAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.crashed {
		return 0, errors.New("wal: device crashed")
	}
	end := off + int64(len(p))
	if end > int64(len(d.data)) {
		grown := make([]byte, end)
		copy(grown, d.data)
		d.data = grown
	}
	n := copy(d.data[off:], p)
	if d.TornTail > 0 && n > d.TornTail {
		// Simulate a torn write: the tail bytes never reach the medium.
		d.data = d.data[:end-int64(d.TornTail)]
		d.TornTail = 0
		return n, nil
	}
	return n, nil
}

func (d *MemDevice) Sync() error {
	d.mu.Lock()
	if d.crashed {
		d.mu.Unlock()
		return errors.New("wal: device crashed")
	}
	if d.FailSync {
		d.mu.Unlock()
		return errors.New("wal: injected sync failure")
	}
	d.durable = append([]byte(nil), d.data...)
	d.SyncCount++
	n := d.SyncCount
	hook := d.OnSync
	d.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	return nil
}

func (d *MemDevice) Size() (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(len(d.data)), nil
}

func (d *MemDevice) Truncate(size int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if size <= int64(len(d.data)) {
		d.data = d.data[:size]
	}
	return nil
}

// Crash reverts the device to its last synchronized prefix, dropping any
// unsynchronized writes (the crash/recovery model, §11). Further writes fail
// until PowerOn, modelling a powered-off device.
func (d *MemDevice) Crash() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data = append([]byte(nil), d.durable...)
	d.crashed = true
}

// PowerOn restores write access after a simulated crash.
func (d *MemDevice) PowerOn() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.crashed = false
}

var errEOF = errors.New("wal: EOF")

// FixedRegionDevice binds the WAL to a fixed-size region with raw block-device
// semantics (§6.3): the size never changes, Truncate is a no-op (a block
// device cannot shrink; logical disposal is encoded in the recovery
// protocol), and writes past the region fail with ENOSPC. It wraps any
// Device, including a real block device or a fixed-size file.
type FixedRegionDevice struct {
	dev  Device
	size int64
}

// NewFixedRegion wraps dev with a fixed region of the given size. The size
// must not exceed the underlying device's size.
func NewFixedRegion(dev Device, size int64) (*FixedRegionDevice, error) {
	sz, err := dev.Size()
	if err != nil {
		return nil, err
	}
	if size <= 0 || size > sz {
		return nil, errors.New("wal: fixed region exceeds device size")
	}
	return &FixedRegionDevice{dev: dev, size: size}, nil
}

func (d *FixedRegionDevice) ReadAt(p []byte, off int64) (int, error) {
	if off >= d.size {
		return 0, errEOF
	}
	if off+int64(len(p)) > d.size {
		p = p[:d.size-off]
	}
	return d.dev.ReadAt(p, off)
}

func (d *FixedRegionDevice) WriteAt(p []byte, off int64) (int, error) {
	if off+int64(len(p)) > d.size {
		return 0, errENOSPC
	}
	return d.dev.WriteAt(p, off)
}

func (d *FixedRegionDevice) Sync() error { return d.dev.Sync() }

// Size returns the constant region size; a block device never grows.
func (d *FixedRegionDevice) Size() (int64, error) { return d.size, nil }

// Truncate is a no-op: disposal of dead space is logical, encoded in the
// checkpoint and the stale-tail recovery rule, never a device resize.
func (d *FixedRegionDevice) Truncate(size int64) error { return nil }

var errENOSPC = errors.New("wal: no space left on device")
