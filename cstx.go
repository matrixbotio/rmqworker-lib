package rmqworker

import (
	"sync"
	"time"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

const cstxExchangeName = "cstx"
const cstxAck = "ack"
const cstxNack = "nack"

const headerCSTXID = "CSTXID"
const headerCSTXAckNum = "CSTXAckNum"
const headerCSTXTimeout = "CSTXTimeout"
const headerCSTXStartedAt = "CSTXStartedAt"

const standardAckMessageLifetime = int64(time.Minute * 1)

var acksConsumerStartedLock sync.Mutex
var cstxAcksConsumer *RMQWorker
var cstxAcksMap = make(map[string][]CSTXAck, 0)
var cstxAcksMapLock sync.RWMutex

// BeginCSTX starts a new cross-service transaction
func (handler *RMQHandler) BeginCSTX(ackNum, timeout int32) (*CrossServiceTransaction, APIError) {
	CSTX := CrossServiceTransaction{
		handler:   handler,
		ID:        getUUID(),
		AckNum:    ackNum,
		StartedAt: time.Now().UnixMilli(),
		Timeout:   timeout,
	}
	return &CSTX, nil
}

func (CSTX CrossServiceTransaction) PublishToQueue(task RMQPublishRequestTask) APIError {
	task.CSTX = CSTX
	return CSTX.handler.PublishToQueue(task)
}

func (CSTX CrossServiceTransaction) PublishToExchange(task PublishToExchangeTask) APIError {
	task.cstx = CSTX
	return CSTX.handler.PublishToExchange(task)
}

// Commit the CrossServiceTransaction and await the required number of acks from other participants
func (CSTX CrossServiceTransaction) Commit() (bool, APIError) {
	err := CSTX.sendCSTXAck(cstxAck)
	if err != nil {
		return false, err
	}
	return CSTX.awaitRequiredAcks()
}

func (CSTX CrossServiceTransaction) Rollback() APIError {
	return CSTX.sendCSTXAck(cstxNack)
}

func (handler *RMQHandler) StartCSTXAcksConsumer() APIError {
	acksConsumerStartedLock.Lock()
	defer acksConsumerStartedLock.Unlock()

	if cstxAcksConsumer != nil {
		return nil
	}

	err := handler.DeclareExchanges(map[string]string{cstxExchangeName: ExchangeTypeTopic})
	if err != nil {
		return err
	}

	queueName := cstxExchangeName + "-" + getUUID()
	task := WorkerTask{
		QueueName:        queueName,
		ISQueueDurable:   false,
		ISAutoDelete:     true,
		Callback:         acksConsumerCallback(),
		ID:               queueName,
		FromExchange:     cstxExchangeName,
		ExchangeType:     ExchangeTypeTopic,
		ConsumersCount:   1,
		WorkerName:       queueName,
		QueueLength:      1000,
		MessagesLifetime: standardAckMessageLifetime,
	}
	cstxAcksConsumer, err = handler.NewRMQWorker(task)
	if err != nil {
		return err
	}

	err = cstxAcksConsumer.Serve()
	if err != nil {
		return err
	}

	startAcksCleaner()

	return nil
}

func (CSTX CrossServiceTransaction) sendCSTXAck(ackType string) APIError {
	return CSTX.handler.PublishToExchange(PublishToExchangeTask{
		Message: CSTXAck{
			TXID:    CSTX.ID,
			Type:    ackType,
			Time:    time.Now().UnixMilli(),
			Timeout: CSTX.Timeout,
		},
		ExchangeName: cstxExchangeName,
	})
}

func (CSTX CrossServiceTransaction) awaitRequiredAcks() (bool, APIError) {
	if cstxAcksConsumer == nil {
		return false, constants.Error("BASE_INTERNAL_ERROR", "CSTX acks consumer not started")
	}
	for {
		cstxAcksMapLock.RLock()
		if len(cstxAcksMap[CSTX.ID]) >= int(CSTX.AckNum) {
			cstxAcksMapLock.RUnlock()
			return true, nil
		}
		cstxAcksMapLock.RUnlock()
		if time.Now().UnixMilli()-CSTX.StartedAt > int64(CSTX.Timeout) {
			return false, constants.Error("CSTX_TIMEOUT", "CrossServiceTransaction timeout: "+CSTX.ID)
		}
		time.Sleep(time.Second * 1)
	}
}

// GetCSTXID - check CSTX available.
// returns CSTX ID, available status
func (deliveryHandler RMQDeliveryHandler) GetCSTXID() (string, bool) {
	IDraw, isAvailable := deliveryHandler.GetHeader(headerCSTXID)
	if isAvailable {
		return IDraw.(string), isAvailable
	}
	return "", isAvailable
}

func (deliveryHandler RMQDeliveryHandler) GetCSTX(handler *RMQHandler) CrossServiceTransaction {
	var CSTX CrossServiceTransaction
	CSTX.ID, _ = deliveryHandler.GetCSTXID()

	ackNum, exists := deliveryHandler.GetHeader(headerCSTXAckNum)
	if exists {
		CSTX.AckNum = ackNum.(int32)
	}
	timeout, exists := deliveryHandler.GetHeader(headerCSTXTimeout)
	if exists {
		CSTX.Timeout = timeout.(int32)
	}
	startedAt, exists := deliveryHandler.GetHeader(headerCSTXStartedAt)
	if exists {
		CSTX.StartedAt = startedAt.(int64)
	}
	CSTX.handler = handler
	return CSTX
}

func setCSTXHeaders(headers amqp.Table, CSTX CrossServiceTransaction) amqp.Table {
	if CSTX.ID != "" {
		headers[headerCSTXID] = CSTX.ID
		headers[headerCSTXAckNum] = CSTX.AckNum
		headers[headerCSTXTimeout] = CSTX.Timeout
		headers[headerCSTXStartedAt] = CSTX.StartedAt
	}
	return headers
}

func acksConsumerCallback() RMQDeliveryCallback {
	return func(worker *RMQWorker, deliveryHandler RMQDeliveryHandler) {
		var ack CSTXAck
		body := deliveryHandler.GetMessageBody()
		if len(body) > 0 {
			err := json.Unmarshal(body, &ack)
			if err != nil {
				worker.logger.Error("unmarshal CrossServiceTransaction Ack message body", zap.Error(err))
				return
			}
		}
		cstxAcksMapLock.Lock()
		cstxAcksMap[ack.TXID] = append(cstxAcksMap[ack.TXID], ack)
		cstxAcksMapLock.Unlock()
	}
}

func startAcksCleaner() {
	go func() {
		for {
			time.Sleep(time.Minute * 5)
			cstxAcksMapLock.Lock()
			for txId, txAcks := range cstxAcksMap {
				if time.Now().UnixMilli()-txAcks[0].Time > int64(txAcks[0].Timeout) {
					delete(cstxAcksMap, txId)
				}
			}
			cstxAcksMapLock.Unlock()
		}
	}()
}
