package cstx

import (
	"errors"
)

var (
	ErrTimeout   = errors.New("crossServiceTransaction timeout")
	ErrCancelled = errors.New("crossServiceTransaction cancelled")
)
