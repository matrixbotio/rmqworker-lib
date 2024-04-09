package service

import (
	"errors"
	"fmt"

	"github.com/matrixbotio/constants-lib"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
)

const (
	exchangeType       = "topic"
	messagesLifetimeMS = int64(2 * 60 * 1000)
)

type consumerResponse struct {
	data  []byte
	error error
}

func (s *Service) responsesConsumerCallback(_ *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
	var responseCh chan consumerResponse
	if ch, found := s.consumerResponses.Load(deliveryHandler.GetCorrelationID()); !found {
		return
	} else {
		responseCh = ch.(chan consumerResponse)
	}

	response := consumerResponse{
		data: deliveryHandler.GetMessageBody(),
	}
	if apiErr := deliveryHandler.CheckResponseError(); apiErr != nil {
		response.error = errors.New(apiErr.Message)
	}

	responseCh <- response
	close(responseCh)
}

func (s *Service) responsesConsumerErrorCallback(_ *rmqworker.RMQWorker, err *constants.APIError) {
	if err != nil {
		s.logger.Error(
			"syncrpc.service responsesConsumerErrorCallback",
			zap.Error(errors.New(err.Message)),
			zap.String("queue", s.queueName),
			zap.String("serviceTag", s.props.ServiceTag),
		)
	}
}

func (s *Service) startResponsesConsumer(props ServiceProps) error {
	errorCallback := s.responsesConsumerErrorCallback
	if props.ErrorCallback != nil {
		errorCallback = props.ErrorCallback
	}

	isQueueDurable := false
	if props.ISQueueDurable != nil {
		isQueueDurable = *props.ISQueueDurable
	}

	isAutoDelete := true
	if props.ISAutoDelete != nil {
		isAutoDelete = *props.ISAutoDelete
	}

	consumersCount := 1
	if props.ConsumersCount != nil {
		consumersCount = *props.ConsumersCount
	}

	spec := rmqworker.WorkerTask{
		QueueName:        s.queueName,
		ISQueueDurable:   isQueueDurable,
		ISAutoDelete:     isAutoDelete,
		Callback:         s.responsesConsumerCallback,
		FromExchange:     props.ResponsesExchange,
		ExchangeType:     exchangeType,
		ConsumersCount:   consumersCount,
		WorkerName:       "worker:" + s.queueName,
		MessagesLifetime: messagesLifetimeMS,
		RoutingKey:       s.queueName,
		UseErrorCallback: true,
		ErrorCallback:    errorCallback,
	}

	w, apiErr := s.rmqHandler.NewRMQWorker(spec)
	if apiErr != nil {
		return fmt.Errorf("create rmqWorker: %w", *apiErr)
	}

	if apiErr = w.Serve(); apiErr != nil {
		return fmt.Errorf("serve rmqWorker: %w", *apiErr)
	}

	return nil
}
