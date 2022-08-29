package cstx

import (
	"time"

	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

func GetCSTXHeaders(tx CrossServiceTransaction) []structs.RMQHeader {
	return []structs.RMQHeader{
		{Name: HeaderCSTXID, Value: tx.ID},
		{Name: HeaderCSTXAckNum, Value: tx.AckNum},
		{Name: HeaderCSTXTimeout, Value: tx.Timeout},
		{Name: HeaderCSTXStartedAt, Value: tx.StartedAt},
	}
}

func StartAcksCleaner() {
	for {
		time.Sleep(time.Minute * 5)
		CSTXAcksMapLock.Lock()
		for txId, txAcks := range CSTXAcksMap {
			if time.Now().UnixMilli()-txAcks[0].Time > int64(txAcks[0].Timeout) {
				delete(CSTXAcksMap, txId)
			}
		}
		CSTXAcksMapLock.Unlock()
	}
}