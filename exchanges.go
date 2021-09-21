package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// RMQExchangeDeclare - declare RMQ exchange
func RMQExchangeDeclare(RMQChannel *amqp.Channel, exchangeName, exchangeType string) APIError {
	err := RMQChannel.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to declare "+exchangeName+" rmq exchange: "+err.Error(),
		)
	}
	return nil
}
