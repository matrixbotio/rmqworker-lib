package cstx

import (
	"sync"
)

var (
	IsCSTXAcksConsumerSet bool

	AcksMap     = make(map[string][]AckMessage, 0)
	AcksMapLock sync.RWMutex
)
