package reliable

import "github.com/matrixbotio/rmqworker-lib"

type MessageHandler interface {
	HandleMessage(w *rmqworker.RMQWorker, deliveryHandler *rmqworker.RMQDeliveryHandler, request any) error
}
