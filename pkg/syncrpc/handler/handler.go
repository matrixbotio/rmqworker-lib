package handler

import (
	"fmt"

	"github.com/matrixbotio/constants-lib"

	"github.com/matrixbotio/rmqworker-lib"
)

type Props struct {
	Exchange         string
	RoutingKey       string
	ConsumersCount   int   // default: 1
	MessagesLifetime int64 // milliseconds. 0 to disable limit
}

type Handler struct {
	rmqHandler RMQHandler
	props      Props
	callback   Callback

	worker *rmqworker.RMQWorker
}

func New(rmqHandler RMQHandler, props Props, callback Callback) *Handler {
	return &Handler{
		rmqHandler: rmqHandler,
		props:      props,
		callback:   callback,
	}
}

func (h *Handler) Start() error {
	queue := h.props.Exchange
	if h.props.RoutingKey != "" {
		queue = queue + "-" + h.props.RoutingKey
	}

	workerTask := rmqworker.WorkerTask{
		QueueName:      queue,
		ISQueueDurable: true,
		ISAutoDelete:   false,
		Callback: func(w *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
			h.callbackWithResponse(&deliveryHandler)
		},
		ID:               queue,
		FromExchange:     h.props.Exchange,
		ExchangeType:     rmqworker.ExchangeTypeTopic,
		ConsumersCount:   h.props.ConsumersCount,
		WorkerName:       queue,
		MessagesLifetime: h.props.MessagesLifetime,
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

func (h *Handler) Stop() {
	h.worker.Stop()
}
