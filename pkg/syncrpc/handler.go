package syncrpc

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
)

type HandlerProps struct {
	RequestsExchange  string
	ResponsesExchange string

	ServiceTag string
}

type Handler struct {
	rmqHandler *rmqworker.RMQHandler
	logger     *zap.Logger
	props      HandlerProps

	consumerResponses sync.Map
	queueName         string
}

func NewHandler(rmqHandler *rmqworker.RMQHandler, logger *zap.Logger, props HandlerProps) (*Handler, error) {
	if props.RequestsExchange == "" {
		return nil, fmt.Errorf("RequestsExchange is empty")
	}
	if props.ResponsesExchange == "" {
		return nil, fmt.Errorf("ResponsesExchange is empty")
	}
	if props.ServiceTag == "" {
		return nil, fmt.Errorf("ServiceTag is empty")
	}

	h := &Handler{
		rmqHandler: rmqHandler,
		logger:     logger,
		props:      props,
		queueName:  fmt.Sprintf("%s-%s", props.ServiceTag, uuid.NewString()),
	}

	if err := h.startResponsesConsumer(props); err != nil {
		return nil, fmt.Errorf("startResponsesConsumer: %w", err)
	}

	return h, nil
}
