package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// rmqExchangeDeclare - declare RMQ exchange
func (r *RMQHandler) rmqExchangeDeclare(RMQChannel *amqp.Channel, task RMQExchangeDeclareTask) APIError {
	args := amqp.Table{}
	if task.MessagesLifetime > 0 {
		args["x-message-ttl"] = task.MessagesLifetime
	}

	err := r.checkConnection()
	if err != nil {
		return err
	}

	rmqErr := RMQChannel.ExchangeDeclare(
		task.ExchangeName, // name
		task.ExchangeType, // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		args,              // arguments
	)
	if rmqErr != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to declare "+task.ExchangeName+" rmq exchange: "+rmqErr.Error(),
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
		err := r.rmqExchangeDeclare(r.Connections.Publish.Channel, RMQExchangeDeclareTask{
			ExchangeName: exchangeName,
			ExchangeType: exchangeType,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
