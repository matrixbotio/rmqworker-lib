package rmqworker

import "github.com/streadway/amqp"

// RMQHandler - RMQ connection handler
type RMQHandler struct {
	ConnectionData rmqConnectionData
	RMQChannel     *amqp.Channel
}

// NewRMQHandler - create new RMQHandler
func NewRMQHandler(connData rmqConnectionData) *RMQHandler {
	return &RMQHandler{
		ConnectionData: connData,
	}
}
