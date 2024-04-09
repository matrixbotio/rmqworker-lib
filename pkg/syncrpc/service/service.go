package service

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
)

type ServiceProps struct {
	RequestsExchange           string
	RequestsExchangeRoutingKey string
	ResponsesExchange          string
	ErrorCallback              rmqworker.RMQErrorCallback
	ISQueueDurable             *bool
	ISAutoDelete               *bool
	ConsumersCount             *int

	ServiceTag string
}

type Service struct {
	rmqHandler *rmqworker.RMQHandler
	logger     *zap.Logger
	props      ServiceProps

	consumerResponses sync.Map
	queueName         string
}

func New(rmqHandler *rmqworker.RMQHandler, logger *zap.Logger, props ServiceProps) (*Service, error) {
	if props.RequestsExchange == "" {
		return nil, fmt.Errorf("RequestsExchange is empty")
	}
	if props.ResponsesExchange == "" {
		return nil, fmt.Errorf("ResponsesExchange is empty")
	}
	if props.ServiceTag == "" {
		return nil, fmt.Errorf("ServiceTag is empty")
	}

	h := &Service{
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
