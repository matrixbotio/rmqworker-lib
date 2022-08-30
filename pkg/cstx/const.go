package cstx

import "time"

type ackType string

const (
	successAckType ackType = "ack"
	failureAckType         = "nack"
)

const (
	ExchangeName = "cstx"

	HeaderCSTXID        = "CSTXID"
	HeaderCSTXAckNum    = "CSTXAckNum"
	HeaderCSTXTimeout   = "CSTXTimeout"
	HeaderCSTXStartedAt = "CSTXStartedAt"
)

const (
	StandardAckMessageLifetime = int64(time.Minute * 1)
)
