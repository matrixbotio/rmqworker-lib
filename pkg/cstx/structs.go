package cstx

type CrossServiceTransaction struct {
	ID        string
	AckNum    int32
	StartedAt int64
	Timeout   int32

	Handler handler
}

type AckMessage struct {
	TXID    string
	Type    ackType
	Time    int64
	Timeout int32
}
