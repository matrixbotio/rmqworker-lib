package cstx

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

type CSTX interface {
	PublishToQueue(task structs.RMQPublishRequestTask) errs.APIError
	PublishToExchange(task structs.PublishToExchangeTask) errs.APIError

	Commit() error
	Rollback() error
	GetID() string
	Serialize() string
}

type CrossServiceTransaction struct {
	ID        string
	AckNum    int32
	StartedAt int64
	Timeout   int32

	Handler Handler `json:"-"`
}

func (tx CrossServiceTransaction) GetID() string {
	return tx.ID
}

func (tx CrossServiceTransaction) Serialize() string {
	res, err := json.Marshal(tx)
	if err != nil {
		panic("should not happened")
	}
	return string(res)
}

func (tx CrossServiceTransaction) PublishToQueue(task structs.RMQPublishRequestTask) errs.APIError {
	return tx.Handler.PublishCSXTToQueue(task, tx)
}

func (tx CrossServiceTransaction) PublishToExchange(task structs.PublishToExchangeTask) errs.APIError {
	return tx.Handler.PublishCSXTToExchange(task, tx)
}

// Commit the CrossServiceTransaction and await the required number of acks from other participants
func (tx CrossServiceTransaction) Commit() error {
	if err := tx.sendCSTXAck(successAckType); err != nil {
		return fmt.Errorf("sendCSTXAck: %w", err)
	}
	return tx.awaitRequiredAcks()
}

func (tx CrossServiceTransaction) Rollback() error {
	return tx.sendCSTXAck(failureAckType)
}

func (tx CrossServiceTransaction) sendCSTXAck(ackType ackType) error {
	err := tx.Handler.PublishToExchange(structs.PublishToExchangeTask{
		Message: AckMessage{
			TXID:    tx.ID,
			Type:    ackType,
			Time:    time.Now().UnixMilli(),
			Timeout: tx.Timeout,
		},
		ExchangeName: ExchangeName,
	})

	if err != nil {
		return fmt.Errorf("cstx send ack: publish: %s", err.Message)
	}

	return nil
}

func (tx CrossServiceTransaction) awaitRequiredAcks() error {
	if IsCSTXAcksConsumerSet == false {
		return errors.New("CSTX acks consumer not started")
	}

	for {
		AcksMapLock.RLock()

		for _, t := range AcksMap[tx.ID] {
			if t.Type == failureAckType {
				AcksMapLock.RUnlock()
				return ErrCancelled
			}
		}
		if len(AcksMap[tx.ID]) >= int(tx.AckNum) {
			AcksMapLock.RUnlock()
			return nil
		}

		AcksMapLock.RUnlock()

		if time.Now().UnixMilli()-tx.StartedAt > int64(tx.Timeout) {
			return ErrTimeout
		}

		time.Sleep(time.Second * 1)
	}
}
