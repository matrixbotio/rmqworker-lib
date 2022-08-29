package cstx

import (
	"encoding/json"
	"time"

	"github.com/streadway/amqp"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
)

func SetCSTXHeaders(headers amqp.Table, CSTX CrossServiceTransaction) amqp.Table {
	if CSTX.ID != "" {
		headers[HeaderCSTXID] = CSTX.ID
		headers[HeaderCSTXAckNum] = CSTX.AckNum
		headers[HeaderCSTXTimeout] = CSTX.Timeout
		headers[HeaderCSTXStartedAt] = CSTX.StartedAt
	}
	return headers
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

func ACKSConsumerCallback() rmqworker.RMQDeliveryCallback {
	return func(worker *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
		var ack CSTXAck
		body := deliveryHandler.GetMessageBody()
		if len(body) > 0 {
			if err := json.Unmarshal(body, &ack); err != nil {
				worker.Logger.Error("unmarshal CrossServiceTransaction Ack message body", zap.Error(err))
				return
			}
		}
		CSTXAcksMapLock.Lock()
		CSTXAcksMap[ack.TXID] = append(CSTXAcksMap[ack.TXID], ack)
		CSTXAcksMapLock.Unlock()
	}
}
