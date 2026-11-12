package main

// Filesystem-substrate qualification (RFC-0001 §6.3). The WAL's file binding
// relies on explicit write+fsync and file create/remove; rename, directory
// fsync, and advisory locking are consumer-facing primitives whose behavior
// must be qualified rather than assumed on a unikernel filesystem. Each
// primitive's observed behavior is recorded; the WAL does not depend on
// rename or directory fsync (it uses in-place checkpoint slots), but consumers
// that publish files atomically need these receipts.

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
)

func fsSubstrate() {
	dir := filepath.Join(workDir, "fssub")
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		report("fssub-mkdir", err, "")
		return
	}

	// 1. write + fsync + reopen: the durable receipt the WAL relies on.
	p := filepath.Join(dir, "flush")
	payload := bytes.Repeat([]byte("F"), 4096)
	err := func() error {
		f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
		if _, err := f.WriteAt(payload, 0); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		return f.Close()
	}()
	if err != nil {
		report("fssub-flush", err, "")
	} else {
		got, rerr := os.ReadFile(p)
		if rerr != nil || !bytes.Equal(got, payload) {
			report("fssub-flush", fmt.Errorf("reopen mismatch: %v", rerr), "")
		} else {
			report("fssub-flush", nil, "write+fsync+reopen verified (WAL durable receipt)")
		}
	}

	// 2. rename atomic publication: write tmp, fsync, rename over target.
	tmp := filepath.Join(dir, "pub.tmp")
	target := filepath.Join(dir, "pub")
	os.WriteFile(target, []byte("old"), 0o600)
	err = func() error {
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
		if _, err := f.WriteAt(payload, 0); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		f.Close()
		return os.Rename(tmp, target)
	}()
	if err != nil {
		report("fssub-rename", err, "")
	} else {
		got, _ := os.ReadFile(target)
		if !bytes.Equal(got, payload) {
			report("fssub-rename", fmt.Errorf("rename content mismatch"), "")
		} else {
			report("fssub-rename", nil, "write+fsync+rename-over verified (atomic publication)")
		}
	}

	// 3. directory fsync: record whether the substrate supports it (needed for
	// durable create/rename/delete on POSIX; a unikernel may no-op or reject).
	if d, err := os.Open(dir); err != nil {
		report("fssub-dirsync", err, "open dir")
	} else {
		err := d.Sync()
		d.Close()
		if err != nil {
			report("fssub-dirsync", err, "directory fsync not supported (recorded)")
		} else {
			report("fssub-dirsync", nil, "directory fsync accepted")
		}
	}

	// 4. file create/remove durability cycle (WAL segment/staging lifecycle).
	cyc := filepath.Join(dir, "cycle")
	err = func() error {
		if err := os.WriteFile(cyc, payload, 0o600); err != nil {
			return err
		}
		return os.Remove(cyc)
	}()
	if err != nil {
		report("fssub-create-remove", err, "")
	} else if _, err := os.Stat(cyc); !os.IsNotExist(err) {
		report("fssub-create-remove", fmt.Errorf("file still present after remove"), "")
	} else {
		report("fssub-create-remove", nil, "create+remove verified")
	}
}
