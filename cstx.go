package rmqworker

import (
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib/pkg/cstx"
	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
)

var CSTXAcksConsumer *RMQWorker

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
	cstx.ACKSConsumerStartedLock.Lock()
	defer cstx.ACKSConsumerStartedLock.Unlock()

	if CSTXAcksConsumer != nil {
		return nil
	}

	err := handler.DeclareExchanges(map[string]string{cstx.CSTXExchangeName: ExchangeTypeTopic})
	if err != nil {
		return err
	}

	queueName := cstx.CSTXExchangeName + "-" + uuid.NewString()
	task := WorkerTask{
		QueueName:      queueName,
		ISQueueDurable: false,
		ISAutoDelete:   true,
		Callback: func(worker *RMQWorker, deliveryHandler RMQDeliveryHandler) {
			var ack cstx.CSTXAck
			body := deliveryHandler.GetMessageBody()
			if len(body) > 0 {
				if err := json.Unmarshal(body, &ack); err != nil {
					worker.Logger.Error("unmarshal CrossServiceTransaction Ack message body", zap.Error(err))
					return
				}
			}
			cstx.CSTXAcksMapLock.Lock()
			cstx.CSTXAcksMap[ack.TXID] = append(cstx.CSTXAcksMap[ack.TXID], ack)
			cstx.CSTXAcksMapLock.Unlock()
		},
		ID:               queueName,
		FromExchange:     cstx.CSTXExchangeName,
		ExchangeType:     ExchangeTypeTopic,
		ConsumersCount:   1,
		WorkerName:       queueName,
		QueueLength:      1000,
		MessagesLifetime: cstx.StandardAckMessageLifetime,
	}
	CSTXAcksConsumer, err = handler.NewRMQWorker(task)
	if err != nil {
		return err
	}

	err = CSTXAcksConsumer.Serve()
	if err != nil {
		return err
	}

	go cstx.StartAcksCleaner()

	return nil
}

func (deliveryHandler RMQDeliveryHandler) GetCSTX(handler *RMQHandler) cstx.CrossServiceTransaction {
	var CSTX cstx.CrossServiceTransaction
	ID, exists := deliveryHandler.GetHeader(cstx.HeaderCSTXID)
	if exists {
		CSTX.ID = ID.(string)
	}
	ackNum, exists := deliveryHandler.GetHeader(cstx.HeaderCSTXAckNum)
	if exists {
		CSTX.AckNum = ackNum.(int32)
	}
	timeout, exists := deliveryHandler.GetHeader(cstx.HeaderCSTXTimeout)
	if exists {
		CSTX.Timeout = timeout.(int32)
	}
	startedAt, exists := deliveryHandler.GetHeader(cstx.HeaderCSTXStartedAt)
	if exists {
		CSTX.StartedAt = startedAt.(int64)
	}
	CSTX.Handler = handler
	return CSTX
}
