package syncrpc

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/matrixbotio/rmqworker-lib/pkg/cstx"
	"github.com/matrixbotio/rmqworker-lib/pkg/syncrpc"
	"github.com/matrixbotio/rmqworker-lib/tests"
)

func TestIntegration_SuccessWithCstx(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	const (
		cstxAckCount             = 2
		cstxTransactionTimeoutMS = 2000
	)

	requestsExchange := requestsExchange + t.Name()
	responsesExchange := responsesExchange + t.Name()

	rmqHandler := tests.GetHandler(t)
	runWorker(t, rmqHandler, requestsExchange, responsesExchange)

	service, err := syncrpc.NewService(
		rmqHandler,
		zaptest.NewLogger(t),
		syncrpc.ServiceProps{
			RequestsExchange:           requestsExchange,
			RequestsExchangeRoutingKey: requestsExchange + "-r-key",
			ResponsesExchange:          responsesExchange,
			ServiceTag:                 "cstx-service",
		},
	)
	require.NoError(t, err)

	// test
	const parallelMessagesCount = 50
	var txErrsCount int32
	txErrs := make(chan error, parallelMessagesCount)

	for i := 1; i <= parallelMessagesCount; i++ {
		go func(i int) {
			// run transaction
			tx := cstx.New(cstxAckCount, cstxTransactionTimeoutMS, rmqHandler)
			ctx := cstx.WithCstx(context.Background(), tx)

			data, err := service.ExecuteRequest(ctx, requestResponseData{i})
			require.NoError(t, err)

			var result requestResponseData
			err = json.Unmarshal(data, &result)
			require.NoError(t, err)

			// get the same unique data back
			assert.Equal(t, i, result.UniqNumber)

			go func() {
				txErrs <- tx.Commit()
				atomic.AddInt32(&txErrsCount, 1)
				if atomic.LoadInt32(&txErrsCount) == parallelMessagesCount {
					close(txErrs)
				}
			}()
		}(i)
	}

	timer := time.NewTimer(testTimeout)
L:
	for {
		select {
		case <-timer.C:
			t.Fatal("TIMEOUT")
		case err, ok := <-txErrs:
			if !ok {
				break L
			}
			assert.NoError(t, err)
		}
	}
}
