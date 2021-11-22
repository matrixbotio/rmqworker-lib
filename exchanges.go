package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// rmqExchangeDeclare - declare RMQ exchange
func (r *RMQHandler) rmqExchangeDeclare(RMQChannel *amqp.Channel, exchangeName, exchangeType string) APIError {
	args := amqp.Table{}
	if r.limitMessagesLifetime {
		args["x-message-ttl"] = r.messagesLifetime
	}

	err := RMQChannel.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		args,         // arguments
	)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to declare "+exchangeName+" rmq exchange: "+err.Error(),
		)
	}
	return nil
}

// DeclareExchanges - declare RMQ exchanges list.
// exchange name -> exchange type
func (r *RMQHandler) DeclareExchanges(exchangeTypes map[string]string) APIError {
	r.Connections.Publish.rwMutex.RLock()
	defer r.Connections.Publish.rwMutex.RUnlock()
	for exchangeName, exchangeType := range exchangeTypes {
		err := r.rmqExchangeDeclare(r.Connections.Publish.Channel, exchangeName, exchangeType)
		if err != nil {
			return err
		}
	}
	return nil
}
