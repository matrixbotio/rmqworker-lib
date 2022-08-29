package cstx

import (
	"sync"
)

var (
	CSTXAcksMap             = make(map[string][]CSTXAck, 0)
	CSTXAcksMapLock         sync.RWMutex
	ACKSConsumerStartedLock sync.Mutex
)
