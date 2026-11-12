package driver_test

import (
	"os"
	"path/filepath"
)

func tempWALPath() string {
	dir, err := os.MkdirTemp("", "raft-example-")
	if err != nil {
		panic(err)
	}
	return filepath.Join(dir, "example.wal")
}
