package cstx

import (
	"time"

	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

func GetCSTXHeaders(tx CrossServiceTransaction) []structs.RMQHeader {
	return []structs.RMQHeader{
		{Name: HeaderID, Value: tx.ID},
		{Name: HeaderAckNum, Value: tx.AckNum},
		{Name: HeaderTimeout, Value: tx.Timeout},
		{Name: HeaderStartedAt, Value: tx.StartedAt},
	}
}

func StartAcksCleaner() {
	for {
		time.Sleep(time.Minute * 5)
		AcksMapLock.Lock()
		for txId, txAcks := range AcksMap {
			if time.Now().UnixMilli()-txAcks[0].Time > int64(txAcks[0].Timeout) {
				delete(AcksMap, txId)
			}
		}
		AcksMapLock.Unlock()
	}
}
