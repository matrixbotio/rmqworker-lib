package syncrpc

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/matrixbotio/rmqworker-lib/pkg/syncrpc"
	"github.com/matrixbotio/rmqworker-lib/tests"
)

func TestIntegration_Success(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	requestsExchange := requestsExchange + t.Name()
	responsesExchange := responsesExchange + t.Name()

	rmqHandler := tests.GetHandler(t)
	runWorker(t, rmqHandler, requestsExchange, responsesExchange)

	// our 3 rpc-handlers
	logger := zaptest.NewLogger(t)
	serviceProps := syncrpc.ServiceProps{
		RequestsExchange:           requestsExchange,
		RequestsExchangeRoutingKey: requestsExchange + "-r-key",
		ResponsesExchange:          responsesExchange,
	}

	serviceProps.ServiceTag = "service-1"
	service1, err := syncrpc.NewService(rmqHandler, logger, serviceProps)
	require.NoError(t, err)

	serviceProps.ServiceTag = "service-2"
	service2, err := syncrpc.NewService(rmqHandler, logger, serviceProps)
	require.NoError(t, err)
	service3, err := syncrpc.NewService(rmqHandler, logger, serviceProps)
	require.NoError(t, err)

	// test
	var wg sync.WaitGroup
	wg.Add(50 + 50 + 50)

	j := 0
	for _, h := range []*syncrpc.Service{service1, service2, service3} {
		for i := 0; i < 50; i++ {
			go func(h *syncrpc.Service, j int) {
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
