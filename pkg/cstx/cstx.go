package cstx

import (
	"time"

	"github.com/matrixbotio/constants-lib"

	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

func (tx CrossServiceTransaction) PublishToQueue(task structs.RMQPublishRequestTask) errs.APIError {
	return tx.Handler.PublishCSXTToQueue(task, tx)
}

func (tx CrossServiceTransaction) PublishToExchange(task structs.PublishToExchangeTask) errs.APIError {
	return tx.Handler.PublishCSXTToExchange(task, tx)
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
	return tx.Handler.PublishToExchange(structs.PublishToExchangeTask{
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
	if IsCSTXAcksConsumerSet == false {
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
