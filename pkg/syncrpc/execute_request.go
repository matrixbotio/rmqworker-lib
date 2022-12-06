package syncrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

const executeRequestCallMaxTime = time.Minute

func (h *Handler) ExecuteRequest(ctx context.Context, requestData any) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, executeRequestCallMaxTime)
	defer cancel()

	requestID := uuid.NewString()

	responseCh := make(chan consumerResponse, 1)
	h.consumerResponses.Store(requestID, responseCh)
	defer h.consumerResponses.Delete(requestID)

	apiErr := h.rmqHandler.PublishToExchange(structs.PublishToExchangeTask{
		ResponseRoutingKey: h.queueName,
		CorrelationID:      requestID,
		Message:            requestData,
		ExchangeName:       h.props.RequestsExchange,
		RoutingKey:         h.props.RequestsExchange,
	})
	if apiErr != nil {
		return nil, fmt.Errorf("publish request to exchange: %w", *apiErr)
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context: %w", ctx.Err())
	case response := <-responseCh:
		if response.error != nil {
			return nil, fmt.Errorf("response has error: %w", response.error)
		}
		return response.data, nil
	}
}
