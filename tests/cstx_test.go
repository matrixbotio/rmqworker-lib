package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixbotio/rmqworker-lib"
)

const (
	cstxTestTimeout          = 5 * time.Second
	cstxTransactionTimeoutMS = 2000
)

func TestIntegration_CSTX_OneConsumerSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	queueName1 := "service1-" + uuid.NewString()
	const (
		messageBody1 = "body-body-body-service1"
		ackCount     = 1
	)

	var wg sync.WaitGroup
	wg.Add(ackCount)
	var cstx rmqworker.CrossServiceTransaction
	rmqHandler := getHandler(t)

	// start service
	rmqHandler.StartCSTXAcksConsumer()
	workerTask1 := rmqworker.WorkerTask{
		QueueName:  queueName1,
		RoutingKey: queueName1,
		Callback: func(w *rmqworker.RMQWorker, h rmqworker.RMQDeliveryHandler) {
			transaction := h.GetCSTX(rmqHandler)
			if transaction.ID == "" {
				return
			}

			// test
			assert.Equal(t, cstx.ID, transaction.ID)
			assert.Equal(t, []byte(`"`+messageBody1+`"`), h.GetMessageBody()) // todo что за прикол с кавычками?

			err := transaction.Commit()
			assert.NoError(t, err)

			// finished
			wg.Done()
		},
	}
	worker1, err := rmqHandler.NewRMQWorker(workerTask1)
	require.NoError(t, rmqworker.ConvertAPIError(err))
	err = worker1.Serve()
	require.NoError(t, rmqworker.ConvertAPIError(err))
	go worker1.AwaitFinish()

	// run transaction
	cstx = rmqHandler.NewCSTX(ackCount, cstxTransactionTimeoutMS)

	err = cstx.PublishToQueue(rmqworker.RMQPublishRequestTask{
		QueueName:   queueName1,
		MessageBody: messageBody1,
		CSTX:        cstx,
	})
	require.NoError(t, rmqworker.ConvertAPIError(err))

	// wait
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-time.After(cstxTestTimeout):
		t.Fatal("TIMEOUT")
	case <-doneCh:
	}
}

func TestIntegration_CSTX_TwoConsumersSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	queueName1 := "service1-" + uuid.NewString()
	queueName2 := "service2-" + uuid.NewString()
	const (
		messageBody1 = "body-body-body-service1"
		messageBody2 = "body-body-body-service2"
		ackCount     = 2
	)

	var wg sync.WaitGroup
	wg.Add(ackCount)
	var cstx rmqworker.CrossServiceTransaction
	rmqHandler := getHandler(t)

	// start service1
	rmqHandler.StartCSTXAcksConsumer()
	workerTask1 := rmqworker.WorkerTask{
		QueueName:  queueName1,
		RoutingKey: queueName1,
		Callback: func(w *rmqworker.RMQWorker, h rmqworker.RMQDeliveryHandler) {
			transaction := h.GetCSTX(rmqHandler)
			if transaction.ID == "" {
				return
			}

			// test
			assert.Equal(t, cstx.ID, transaction.ID)
			assert.Equal(t, []byte(`"`+messageBody1+`"`), h.GetMessageBody()) // todo что за прикол с кавычками?

			err := transaction.Commit()
			assert.NoError(t, err)

			// finished
			wg.Done()
		},
	}
	worker1, err := rmqHandler.NewRMQWorker(workerTask1)
	require.NoError(t, rmqworker.ConvertAPIError(err))
	err = worker1.Serve()
	require.NoError(t, rmqworker.ConvertAPIError(err))
	go worker1.AwaitFinish()

	// start service2
	rmqHandler.StartCSTXAcksConsumer()
	workerTask2 := rmqworker.WorkerTask{
		QueueName:  queueName2,
		RoutingKey: queueName2,
		Callback: func(w *rmqworker.RMQWorker, h rmqworker.RMQDeliveryHandler) {
			transaction := h.GetCSTX(rmqHandler)
			if transaction.ID == "" {
				return
			}

			// test
			assert.Equal(t, cstx.ID, transaction.ID)
			assert.Equal(t, []byte(`"`+messageBody2+`"`), h.GetMessageBody()) // todo что за прикол с кавычками?

			err := transaction.Commit()
			assert.NoError(t, err)

			// finished
			wg.Done()
		},
	}
	worker2, err := rmqHandler.NewRMQWorker(workerTask2)
	require.NoError(t, rmqworker.ConvertAPIError(err))
	err = worker2.Serve()
	require.NoError(t, rmqworker.ConvertAPIError(err))
	go worker2.AwaitFinish()

	// run transaction
	cstx = rmqHandler.NewCSTX(ackCount, cstxTransactionTimeoutMS)

	err = cstx.PublishToQueue(rmqworker.RMQPublishRequestTask{
		QueueName:   queueName1,
		MessageBody: messageBody1,
		CSTX:        cstx,
	})
	require.NoError(t, rmqworker.ConvertAPIError(err))

	err = cstx.PublishToQueue(rmqworker.RMQPublishRequestTask{
		QueueName:   queueName2,
		MessageBody: messageBody2,
		CSTX:        cstx,
	})
	require.NoError(t, rmqworker.ConvertAPIError(err))

	// wait
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-time.After(cstxTestTimeout):
		t.Fatal("TIMEOUT")
	case <-doneCh:
	}
}
