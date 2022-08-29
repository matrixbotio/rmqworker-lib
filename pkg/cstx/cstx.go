package cstx

import (
	"time"

	"github.com/matrixbotio/constants-lib"

	"github.com/matrixbotio/rmqworker-lib"
)

func (tx CrossServiceTransaction) PublishToQueue(task rmqworker.RMQPublishRequestTask) rmqworker.APIError {
	task.CSTX = tx
	return tx.Handler.PublishToQueue(task)
}

func (tx CrossServiceTransaction) PublishToExchange(task rmqworker.PublishToExchangeTask) rmqworker.APIError {
	task.CSTX = tx
	return tx.Handler.PublishToExchange(task)
}

// Commit the CrossServiceTransaction and await the required number of acks from other participants
func (tx CrossServiceTransaction) Commit() error {
	if err := tx.sendCSTXAck(CSTXAckType); err != nil {
		return *err
	}
	return tx.awaitRequiredAcks()
}

func (tx CrossServiceTransaction) Rollback() rmqworker.APIError {
	return tx.sendCSTXAck(CSTXNackType)
}

func (tx CrossServiceTransaction) sendCSTXAck(ackType string) rmqworker.APIError {
	return tx.Handler.PublishToExchange(rmqworker.PublishToExchangeTask{
		Message: CSTXAck{
			TXID:    tx.ID,
			Type:    ackType,
			Time:    time.Now().UnixMilli(),
			Timeout: tx.Timeout,
		},
		ExchangeName: CSTXExchangeName,
	})
}

func (tx CrossServiceTransaction) awaitRequiredAcks() error {
	if CSTXAcksConsumer == nil {
		return constants.Error("BASE_INTERNAL_ERROR", "CSTX acks consumer not started")
	}
	for {
		CSTXAcksMapLock.RLock()
		if len(CSTXAcksMap[tx.ID]) >= int(tx.AckNum) {
			CSTXAcksMapLock.RUnlock()
			return nil
		}
		CSTXAcksMapLock.RUnlock()
		if time.Now().UnixMilli()-tx.StartedAt > int64(tx.Timeout) {
			return ErrCSTXTimeout
		}
		time.Sleep(time.Second * 1)
	}
}
