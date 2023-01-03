package syncrpc

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/matrixbotio/constants-lib"
	"github.com/stretchr/testify/require"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

var startCSTXAcksConsumerOnce sync.Once

type requestResponseData struct {
	UniqNumber int
}

// run simulator of an external service that gets our requests
// responses to us back with the same data as form request
func runWorker(t *testing.T, rmqHandler *rmqworker.RMQHandler, requestsExchange, responsesExchange string) {
	startCSTXAcksConsumerOnce.Do(func() {
		apiErr := rmqHandler.StartCSTXAcksConsumer()
		require.Nil(t, apiErr)
	})

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

			// logic in case of cstx - always do Commit()
			if transaction := deliveryHandler.GetCSTX(rmqHandler); transaction.ID != "" {
				go func() {
					err := transaction.Commit()
					require.NoError(t, err)
				}()
			}
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
}
