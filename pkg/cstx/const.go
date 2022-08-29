package cstx

import "time"

const (
	CSTXExchangeName = "cstx"
	CSTXAckType      = "ack"
	CSTXNackType     = "nack"
)

const (
	HeaderCSTXID        = "CSTXID"
	HeaderCSTXAckNum    = "CSTXAckNum"
	HeaderCSTXTimeout   = "CSTXTimeout"
	HeaderCSTXStartedAt = "CSTXStartedAt"
)

const (
	StandardAckMessageLifetime = int64(time.Minute * 1)
)
