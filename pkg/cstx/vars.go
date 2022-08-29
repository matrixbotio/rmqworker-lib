package cstx

import (
	"sync"
)

var (
	IsCSTXAcksConsumerSet   bool
	CSTXAcksMap             = make(map[string][]CSTXAck, 0)
	CSTXAcksMapLock         sync.RWMutex
	ACKSConsumerStartedLock sync.Mutex
)
