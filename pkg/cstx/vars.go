package cstx

import (
	"sync"

	"github.com/matrixbotio/rmqworker-lib"
)

var (
	CSTXAcksConsumer        *rmqworker.RMQWorker
	CSTXAcksMap             = make(map[string][]CSTXAck, 0)
	CSTXAcksMapLock         sync.RWMutex
	ACKSConsumerStartedLock sync.Mutex
)
