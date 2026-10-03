package lab

import (
	"os/exec"
	"runtime"
	"syscall"
)

// startOwned starts cmd so that the kernel kills it with SIGKILL if the
// harness dies without running cleanup (panic, os.Exit, SIGKILL). Linux
// delivers the parent-death signal when the forking OS thread exits, so the
// fork happens on a locked thread; Go keeps that thread alive afterwards
// because the goroutine unlocks it rather than exiting while locked.
func startOwned(cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGKILL,
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	return cmd.Start()
}
