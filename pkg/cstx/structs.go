package cstx

type AckMessage struct {
	TXID    string
	Type    ackType
	Time    int64
	Timeout int32
}
