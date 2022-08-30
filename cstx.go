package rmqworker

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib/pkg/cstx"
	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
)

var (
	cstxAcksConsumer            *RMQWorker
	cstxAcksConsumerStartedLock sync.Mutex
)

func (handler *RMQHandler) NewCSTX(ackNum, timeout int32) cstx.CrossServiceTransaction {
	return cstx.CrossServiceTransaction{
		Handler:   handler,
		ID:        uuid.NewString(),
		AckNum:    ackNum,
		StartedAt: time.Now().UnixMilli(),
		Timeout:   timeout,
	}
}

func (handler *RMQHandler) StartCSTXAcksConsumer() errs.APIError {
	cstxAcksConsumerStartedLock.Lock()
	defer cstxAcksConsumerStartedLock.Unlock()

	if cstxAcksConsumer != nil {
		return nil
	}

	err := handler.DeclareExchanges(map[string]string{cstx.ExchangeName: ExchangeTypeTopic})
	if err != nil {
		return err
	}

	queueName := cstx.ExchangeName + "-" + uuid.NewString()
	task := WorkerTask{
		QueueName:      queueName,
		ISQueueDurable: false,
		ISAutoDelete:   true,
		Callback: func(worker *RMQWorker, deliveryHandler RMQDeliveryHandler) {
			var ack cstx.AckMessage
			body := deliveryHandler.GetMessageBody()
			if len(body) > 0 {
				if err := json.Unmarshal(body, &ack); err != nil {
					worker.Logger.Error("unmarshal CrossServiceTransaction Ack message body", zap.Error(err))
					return
				}
			}

			cstx.AcksMapLock.Lock()
			cstx.AcksMap[ack.TXID] = append(cstx.AcksMap[ack.TXID], ack)
			cstx.AcksMapLock.Unlock()
		},
		ID:               queueName,
		FromExchange:     cstx.ExchangeName,
		ExchangeType:     ExchangeTypeTopic,
		ConsumersCount:   1,
		WorkerName:       queueName,
		QueueLength:      1000,
		MessagesLifetime: cstx.StandardAckMessageLifetime,
	}

	cstxAcksConsumer, err = handler.NewRMQWorker(task)
	if err != nil {
		return err
	}

	if err := cstxAcksConsumer.Serve(); err != nil {
		return err
	}

	cstx.IsCSTXAcksConsumerSet = true

	go cstx.StartAcksCleaner()

	return nil
}

func (deliveryHandler RMQDeliveryHandler) GetCSTX(handler *RMQHandler) cstx.CrossServiceTransaction {
	var CSTX cstx.CrossServiceTransaction
	ID, exists := deliveryHandler.GetHeader(cstx.HeaderID)
	if exists {
		CSTX.ID = ID.(string)
	}
	ackNum, exists := deliveryHandler.GetHeader(cstx.HeaderAckNum)
	if exists {
		CSTX.AckNum = ackNum.(int32)
	}
	timeout, exists := deliveryHandler.GetHeader(cstx.HeaderTimeout)
	if exists {
		CSTX.Timeout = timeout.(int32)
	}
	startedAt, exists := deliveryHandler.GetHeader(cstx.HeaderStartedAt)
	if exists {
		CSTX.StartedAt = startedAt.(int64)
	}
	CSTX.Handler = handler
	return CSTX
}
