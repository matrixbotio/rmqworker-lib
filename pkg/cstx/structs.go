package cstx

import "github.com/matrixbotio/rmqworker-lib"

type CrossServiceTransaction struct {
	ID        string
	AckNum    int32
	StartedAt int64
	Timeout   int32

	Handler *rmqworker.RMQHandler
}

type CSTXAck struct {
	TXID    string
	Type    string // ack or nack
	Time    int64
	Timeout int32
}
