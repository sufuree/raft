//go:build !linux

package main

import (
	"errors"
	"os"
)

func flock(f *os.File) error {
	return errors.New("flock not available on this platform")
}
