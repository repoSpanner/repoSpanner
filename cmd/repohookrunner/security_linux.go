// +build linux
package main

import (
	"syscall"
)

func dropToUser(user int) error {
	return syscall.Setresuid(request.User, request.User, request.User)
}
