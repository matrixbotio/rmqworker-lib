package syncrpc

import (
	"fmt"

	"github.com/matrixbotio/constants-lib"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
)

const (
	exchangeType       = "topic"
	consumersCount     = 1
	messagesLifetimeMS = int64(2 * 60 * 1000)
)

type consumerResponse struct {
	data  []byte
	error error
}

func (h *Handler) responsesConsumerCallback(_ *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
	var responseCh chan consumerResponse
	if ch, found := h.consumerResponses.Load(deliveryHandler.GetCorrelationID()); !found {
		return
	} else {
		responseCh = ch.(chan consumerResponse)
	}

	response := consumerResponse{
		data: deliveryHandler.GetMessageBody(),
	}
	if apiErr := deliveryHandler.CheckResponseError(); apiErr != nil {
		response.error = *apiErr
	}

	responseCh <- response
	close(responseCh)
}

func (h *Handler) responsesConsumerErrorCallback(_ *rmqworker.RMQWorker, err *constants.APIError) {
	if err != nil {
		h.logger.Error(
			"syncrpc.handler responsesConsumerErrorCallback",
			zap.Error(*err),
			zap.String("queue", h.queueName),
			zap.String("serviceTag", h.props.ServiceTag),
		)
	}
}

func (h *Handler) startResponsesConsumer(props HandlerProps) error {
	spec := rmqworker.WorkerTask{
		QueueName:        h.queueName,
		ISQueueDurable:   true,
		ISAutoDelete:     false,
		Callback:         h.responsesConsumerCallback,
		FromExchange:     props.ResponsesExchange,
		ExchangeType:     exchangeType,
		ConsumersCount:   consumersCount,
		WorkerName:       "worker:" + h.queueName,
		MessagesLifetime: messagesLifetimeMS,
		RoutingKey:       h.queueName,
		UseErrorCallback: true,
		ErrorCallback:    h.responsesConsumerErrorCallback,
	}

	w, apiErr := h.rmqHandler.NewRMQWorker(spec)
	if apiErr != nil {
		return fmt.Errorf("create rmqWorker: %w", *apiErr)
	}

	if apiErr = w.Serve(); apiErr != nil {
		return fmt.Errorf("serve rmqWorker: %w", *apiErr)
	}

	return nil
}
