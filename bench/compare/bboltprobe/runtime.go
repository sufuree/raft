package main

import "runtime"

func runtimeVersion() string { return runtime.Version() }
func runtimeArch() string    { return runtime.GOOS + "/" + runtime.GOARCH }
