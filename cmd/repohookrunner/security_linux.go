// +build linux

package main

import (
	"syscall"
)

func dropToUser(user int) error {
	return syscall.Setresuid(user, user, user)
}
