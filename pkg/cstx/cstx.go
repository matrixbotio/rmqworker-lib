package cstx

import (
	"time"

	"github.com/matrixbotio/constants-lib"

	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
	"github.com/matrixbotio/rmqworker-lib/pkg/tasks"
)

func (tx CrossServiceTransaction) PublishToQueue(task tasks.RMQPublishRequestTask) errs.APIError {
	task.CSTX = tx
	return tx.Handler.PublishToQueue(task)
}

func (tx CrossServiceTransaction) PublishToExchange(task tasks.PublishToExchangeTask) errs.APIError {
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

func (tx CrossServiceTransaction) Rollback() errs.APIError {
	return tx.sendCSTXAck(CSTXNackType)
}

func (tx CrossServiceTransaction) sendCSTXAck(ackType string) errs.APIError {
	return tx.Handler.PublishToExchange(tasks.PublishToExchangeTask{
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
