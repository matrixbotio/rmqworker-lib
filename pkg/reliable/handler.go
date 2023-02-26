package reliable

import (
	"fmt"
	"time"

	"github.com/matrixbotio/constants-lib"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/pkg/utils"
)

type Handler[T any] struct {
	rmqHandler *rmqworker.RMQHandler
	props      Props
	callback   MessageHandler

	worker *rmqworker.RMQWorker
}

type Props struct {
	Exchange         string
	RoutingKey       string
	ConsumersCount   int           // default: 1
	MessagesLifetime time.Duration // default 10 minutes
}

const defaultMessagesLifetime = time.Minute * 10

func New[T any](rmqHandler *rmqworker.RMQHandler, props Props, callback MessageHandler) *Handler[T] {
	handler := Handler[T]{
		rmqHandler: rmqHandler,
		props:      props,
		callback:   callback,
	}

	if handler.props.MessagesLifetime < 1 {
		handler.props.MessagesLifetime = defaultMessagesLifetime
	}

	return &handler
}

func (h *Handler[T]) Start() error {
	queue := utils.GetQueueName(h.props.Exchange, h.props.RoutingKey)

	workerTask := rmqworker.WorkerTask{
		QueueName:      queue,
		ISQueueDurable: true,
		ISAutoDelete:   false,
		Callback: func(w *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
			h.handleCallback(&deliveryHandler)
		},
		ID:               queue,
		FromExchange:     h.props.Exchange,
		ExchangeType:     rmqworker.ExchangeTypeTopic,
		ConsumersCount:   h.props.ConsumersCount,
		WorkerName:       queue,
		MessagesLifetime: h.props.MessagesLifetime.Milliseconds(),
	}

	var err *constants.APIError
	h.worker, err = h.rmqHandler.NewRMQWorker(workerTask)
	if err != nil {
		return fmt.Errorf("syncrpc handler start new rmq worker: %s", err.Error())
	}

	if err := h.worker.Serve(); err != nil {
		return fmt.Errorf("syncrpc handler rmq worker serve: %s", err.Message)
	}

	return nil
}

func (h *Handler[T]) Stop() {
	h.worker.Stop()
}

func (h *Handler[T]) handleCallback(deliveryHandler *rmqworker.RMQDeliveryHandler) {
	request, err := utils.GetRequest[T](deliveryHandler)
	if err != nil {
		h.reject(deliveryHandler, err)
	}

	if err := h.callback.HandleMessage(h.worker, deliveryHandler, request); err != nil {
		h.reject(deliveryHandler, err)
	}

	deliveryHandler.Accept()
}

func (h *Handler[T]) reject(deliveryHandler *rmqworker.RMQDeliveryHandler, err error) {
	zap.L().Error(
		"Error handling reliable async RMQ message, rejecting",
		zap.Error(err),
		zap.String("rmqExchange", h.props.Exchange),
		zap.String("routingKey", h.props.RoutingKey),
	)

	deliveryHandler.Reject(true)
}
