package cstx

type CrossServiceTransaction struct {
	ID        string
	AckNum    int32
	StartedAt int64
	Timeout   int32

	Handler handler
}

type CSTXAck struct {
	TXID    string
	Type    string // ack or nack
	Time    int64
	Timeout int32
}
