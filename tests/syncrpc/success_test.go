package syncrpc

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/matrixbotio/constants-lib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
	"github.com/matrixbotio/rmqworker-lib/pkg/syncrpc"
	"github.com/matrixbotio/rmqworker-lib/tests"
)

func TestIntegration_Success(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	type requestResponseData struct {
		UniqNumber int
	}
	rmqHandler := tests.GetHandler(t)

	// run simulator of an external service that gets our requests
	// responses to us back with the same data as form request
	w, apiErr := rmqHandler.NewRMQWorker(rmqworker.WorkerTask{
		QueueName:  requestsExchange,
		RoutingKey: requestsExchange,

		ISQueueDurable: true,
		ISAutoDelete:   false,
		Callback: func(_ *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
			apiErr := deliveryHandler.CheckResponseError()
			require.Nil(t, apiErr)

			var result requestResponseData
			err := json.Unmarshal(deliveryHandler.GetMessageBody(), &result)
			require.NoError(t, err)

			responseRoutingKeyHeader, apiErr := deliveryHandler.GetResponseRoutingKeyHeader()
			require.Nil(t, apiErr)

			apiErr = rmqHandler.PublishToExchange(structs.PublishToExchangeTask{
				CorrelationID: deliveryHandler.GetCorrelationID(),
				Message:       result,
				ExchangeName:  responsesExchange,
				RoutingKey:    responseRoutingKeyHeader,
			})
			require.Nil(t, apiErr)
		},
		FromExchange:     requestsExchange,
		ExchangeType:     "topic",
		ConsumersCount:   1,
		MessagesLifetime: int64(1 * 60 * 1000),
		UseErrorCallback: true,
		ErrorCallback: func(*rmqworker.RMQWorker, *constants.APIError) {
			t.Fatal("should not happened")
		},
		DisableCheckResponseErrors: true,
	})
	require.Nil(t, apiErr)
	apiErr = w.Serve()
	require.Nil(t, apiErr)

	// our 2 rpc-handlers
	logger := zaptest.NewLogger(t)
	handlerProps := syncrpc.HandlerProps{
		RequestsExchange:  requestsExchange,
		ResponsesExchange: responsesExchange,
	}

	handlerProps.ServiceTag = "service-1"
	handler1, err := syncrpc.NewHandler(rmqHandler, logger, handlerProps)
	require.NoError(t, err)

	handlerProps.ServiceTag = "service-2"
	handler2, err := syncrpc.NewHandler(rmqHandler, logger, handlerProps)
	require.NoError(t, err)

	// test
	var wg sync.WaitGroup
	wg.Add(50 + 50)

	j := 0
	for _, h := range []*syncrpc.Handler{handler1, handler2} {
		for i := 0; i < 50; i++ {
			go func(h *syncrpc.Handler, j int) {
				defer wg.Done()

				data, err := h.ExecuteRequest(context.Background(), requestResponseData{j})
				require.NoError(t, err)

				var result requestResponseData
				err = json.Unmarshal(data, &result)
				require.NoError(t, err)

				// get the same unique data back
				assert.Equal(t, j, result.UniqNumber)
			}(h, j)
			j++
		}
	}

	// wait
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-time.After(testTimeout):
		t.Fatal("TIMEOUT")
	case <-doneCh:
	}
}
