package cstx

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/pkg/cstx"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
	"github.com/matrixbotio/rmqworker-lib/tests"
)

func TestIntegration_OneConsumerTimeoutError(t *testing.T) {
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
	var tx cstx.CrossServiceTransaction
	rmqHandler := tests.GetHandler(t)

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

			// do delay
			time.Sleep(cstxConsumerProcessDelay)

			// test
			err := transaction.Commit()
			assert.ErrorIs(t, err, cstx.ErrTimeout)

			// finished
			wg.Done()
		},
	}
	worker1, err := rmqHandler.NewRMQWorker(workerTask1)
	require.NoError(t, tests.ConvertAPIError(err))
	err = worker1.Serve()
	require.NoError(t, tests.ConvertAPIError(err))
	go worker1.AwaitFinish()

	// run transaction
	tx = rmqHandler.NewCSTX(ackCount, cstxTransactionTimeoutMS)

	err = tx.PublishToQueue(structs.RMQPublishRequestTask{
		QueueName:   queueName1,
		MessageBody: messageBody1,
	})
	require.NoError(t, tests.ConvertAPIError(err))

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

func TestIntegration_NotEnoughConfirmationsTimeoutError(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	queueName1 := "service1-" + uuid.NewString()
	const (
		messageBody1   = "body-body-body-service1"
		ackCountWanted = 2
		ackCountToSent = 1
	)

	var wg sync.WaitGroup
	wg.Add(ackCountToSent)
	var tx cstx.CrossServiceTransaction
	rmqHandler := tests.GetHandler(t)

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
			err := transaction.Commit()
			assert.ErrorIs(t, err, cstx.ErrTimeout)

			// finished
			wg.Done()
		},
	}
	worker1, err := rmqHandler.NewRMQWorker(workerTask1)
	require.NoError(t, tests.ConvertAPIError(err))
	err = worker1.Serve()
	require.NoError(t, tests.ConvertAPIError(err))
	go worker1.AwaitFinish()

	// run transaction
	tx = rmqHandler.NewCSTX(ackCountWanted, cstxTransactionTimeoutMS)

	err = tx.PublishToQueue(structs.RMQPublishRequestTask{
		QueueName:   queueName1,
		MessageBody: messageBody1,
	})
	require.NoError(t, tests.ConvertAPIError(err))

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

func TestIntegration_TwoConsumersTimeoutWhenFirstDelayedError(t *testing.T) {
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
	var tx cstx.CrossServiceTransaction
	rmqHandler := tests.GetHandler(t)

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

			// do delay
			time.Sleep(cstxConsumerProcessDelay)

			// test
			err := transaction.Commit()
			assert.ErrorIs(t, err, cstx.ErrTimeout)

			// finished
			wg.Done()
		},
	}
	worker1, err := rmqHandler.NewRMQWorker(workerTask1)
	require.NoError(t, tests.ConvertAPIError(err))
	err = worker1.Serve()
	require.NoError(t, tests.ConvertAPIError(err))
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
			err := transaction.Commit()
			assert.ErrorIs(t, err, cstx.ErrTimeout)

			// finished
			wg.Done()
		},
	}
	worker2, err := rmqHandler.NewRMQWorker(workerTask2)
	require.NoError(t, tests.ConvertAPIError(err))
	err = worker2.Serve()
	require.NoError(t, tests.ConvertAPIError(err))
	go worker2.AwaitFinish()

	// run transaction
	tx = rmqHandler.NewCSTX(ackCount, cstxTransactionTimeoutMS)

	err = tx.PublishToQueue(structs.RMQPublishRequestTask{
		QueueName:   queueName1,
		MessageBody: messageBody1,
	})
	require.NoError(t, tests.ConvertAPIError(err))

	err = tx.PublishToQueue(structs.RMQPublishRequestTask{
		QueueName:   queueName2,
		MessageBody: messageBody2,
	})
	require.NoError(t, tests.ConvertAPIError(err))

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

func TestIntegration_TwoConsumersTimeoutWhenSecondDelayedError(t *testing.T) {
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
	var tx cstx.CrossServiceTransaction
	rmqHandler := tests.GetHandler(t)

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
			err := transaction.Commit()
			assert.ErrorIs(t, err, cstx.ErrTimeout)

			// finished
			wg.Done()
		},
	}
	worker1, err := rmqHandler.NewRMQWorker(workerTask1)
	require.NoError(t, tests.ConvertAPIError(err))
	err = worker1.Serve()
	require.NoError(t, tests.ConvertAPIError(err))
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

			// do delay
			time.Sleep(cstxConsumerProcessDelay)

			// test
			err := transaction.Commit()
			assert.ErrorIs(t, err, cstx.ErrTimeout)

			// finished
			wg.Done()
		},
	}
	worker2, err := rmqHandler.NewRMQWorker(workerTask2)
	require.NoError(t, tests.ConvertAPIError(err))
	err = worker2.Serve()
	require.NoError(t, tests.ConvertAPIError(err))
	go worker2.AwaitFinish()

	// run transaction
	tx = rmqHandler.NewCSTX(ackCount, cstxTransactionTimeoutMS)

	err = tx.PublishToQueue(structs.RMQPublishRequestTask{
		QueueName:   queueName1,
		MessageBody: messageBody1,
	})
	require.NoError(t, tests.ConvertAPIError(err))

	err = tx.PublishToQueue(structs.RMQPublishRequestTask{
		QueueName:   queueName2,
		MessageBody: messageBody2,
	})
	require.NoError(t, tests.ConvertAPIError(err))

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
