package cstx

type AckMessage struct {
	TXID    string  `json:"txId"`
	Type    ackType `json:"type"`
	Time    int64   `json:"time"`
	Timeout int32   `json:"timeout"`
}
