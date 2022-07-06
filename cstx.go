package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

const cstxExchangeName = "cstx"
const cstxAck = "ack"
const cstxNack = "nack"

const headerCSTXID = "CSTXID"
const headerCSTXAckNum = "CSTXAckNum"
const headerCSTXTimeout = "CSTXTimeout"
const headerCSTXStartedAt = "CSTXStartedAt"

var cstxExchangeDeclared = false
var cstxAcksConsumer *RMQWorker
var cstxAcksMap map[string][]CSTXAck
var cstxAcksMapLock = sync.RWMutex{}

// BeginCSTX starts a new cross-service transaction
func (handler *RMQHandler) BeginCSTX(ackNum int, timeout int) (*CrossServiceTransaction, APIError) {
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
	if cstxAcksConsumer == nil {
		err := CSTX.startAcksConsumer()
		if err != nil {
			return false, err
		}
		startAcksCleaner()
	}
	err := CSTX.sendCSTXAck(cstxAck)
	if err != nil {
		return false, err
	}
	return CSTX.awaitRequiredAcks()
}

func (CSTX CrossServiceTransaction) Rollback() APIError {
	return CSTX.sendCSTXAck(cstxNack)
}

func (CSTX CrossServiceTransaction) startAcksConsumer() APIError {
	if !cstxExchangeDeclared {
		err := CSTX.handler.DeclareExchanges(map[string]string{cstxExchangeName: ExchangeTypeTopic})
		if err != nil {
			return err
		}
		cstxExchangeDeclared = true
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
		MessagesLifetime: int64(CSTX.Timeout),
	}
	worker, err := CSTX.handler.NewRMQWorker(task)
	if err != nil {
		return err
	}

	err = worker.Serve()
	if err != nil {
		return err
	}

	return nil
}

func (CSTX CrossServiceTransaction) sendCSTXAck(ackType string) APIError {
	return CSTX.handler.PublishToExchange(PublishToExchangeTask{
		Message: CSTXAck{
			TxId:    CSTX.ID,
			Type:    ackType,
			Time:    time.Now().UnixMilli(),
			Timeout: CSTX.Timeout,
		},
		ExchangeName: cstxExchangeName,
	})
}

func (CSTX CrossServiceTransaction) awaitRequiredAcks() (bool, APIError) {
	for {
		cstxAcksMapLock.RLock()
		if len(cstxAcksMap[CSTX.ID]) >= CSTX.AckNum {
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

func (deliveryHandler RMQDeliveryHandler) GetCSTX(handler *RMQHandler) CrossServiceTransaction {
	var CSTX CrossServiceTransaction
	ID, exists := deliveryHandler.GetHeader(headerCSTXID)
	if exists {
		CSTX.ID = ID.(string)
	}
	ackNum, exists := deliveryHandler.GetHeader(headerCSTXAckNum)
	if exists {
		CSTX.AckNum = ackNum.(int)
	}
	timeout, exists := deliveryHandler.GetHeader(headerCSTXTimeout)
	if exists {
		CSTX.Timeout = timeout.(int)
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
				worker.logger.Error("Failed to unmarshal CrossServiceTransaction Ack message body: " + err.Error())
				return
			}
		}
		cstxAcksMapLock.Lock()
		cstxAcksMap[ack.TxId] = append(cstxAcksMap[ack.TxId], ack)
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
