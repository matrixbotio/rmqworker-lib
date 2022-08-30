package cstx

import "time"

type ackType string

const (
	successAckType ackType = "ack"
	failureAckType         = "nack"
)

const (
	ExchangeName = "cstx"

	HeaderID        = "CSTXID"
	HeaderAckNum    = "CSTXAckNum"
	HeaderTimeout   = "CSTXTimeout"
	HeaderStartedAt = "CSTXStartedAt"
)

const (
	StandardAckMessageLifetime = int64(time.Minute * 1)
)
