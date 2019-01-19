// +build !linux

package main

import (
	"errors"
)

func dropToUser(user int) error {
	return errors.New("User changing is not supported")
}
