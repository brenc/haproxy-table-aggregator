//go:build !linux

package lab

import "os/exec"

// startOwned starts cmd. Without Linux's parent-death signal, a harness
// killed before cleanup can leak the process; Linux is the supported lab
// platform.
func startOwned(cmd *exec.Cmd) error {
	return cmd.Start()
}
