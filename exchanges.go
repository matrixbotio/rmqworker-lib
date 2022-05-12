package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// rmqExchangeDeclare - declare RMQ exchange
func (r *RMQHandler) rmqExchangeDeclare(RMQChannel *amqp.Channel, task RMQExchangeDeclareTask) APIError {
	r.rlock()
	defer r.runlock()

	args := amqp.Table{}
	if task.MessagesLifetime > 0 {
		args["x-message-ttl"] = task.MessagesLifetime
	}

	err := r.channelKeeper.Channel().ExchangeDeclare(
		task.ExchangeName, // name
		task.ExchangeType, // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		args,              // arguments
	)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to declare "+task.ExchangeName+" rmq exchange: "+err.Error(),
		)
	}
	return nil
}
