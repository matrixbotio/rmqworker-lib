package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// rmqExchangeDeclare - declare RMQ exchange
func (r *RMQHandler) rmqExchangeDeclare(RMQChannel *amqp.Channel, exchangeName, exchangeType string) APIError {
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

// DeclareExchanges - declare RMQ exchanges list
func (r *RMQHandler) DeclareExchanges(exchangeTypes map[string]string) APIError {
	for exchangeName, exchangeType := range exchangeTypes {
		err := r.rmqExchangeDeclare(r.RMQChannel, exchangeName, exchangeType)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeclareQueues - declare RMQ exchanges list
func (r *RMQHandler) DeclareQueues(queues []string) APIError {
	for _, queueName := range queues {
		err := r.rmqQueueDeclare(
			r.RMQChannel, // RMQ channel
			queueName,    // queue name
			true,         // durable
			false,        // auto-delete
		)
		if err != nil {
			return err
		}
	}
	return nil
}
