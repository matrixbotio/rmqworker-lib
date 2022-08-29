package rmqworker

import (
	"strings"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"

	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
)

// rmqExchangeDeclare - declare RMQ exchange
func (r *RMQHandler) rmqExchangeDeclare(RMQChannel *amqp.Channel, task RMQExchangeDeclareTask) errs.APIError {
	r.rlock()
	defer r.runlock()

	args := amqp.Table{}
	if task.MessagesLifetime > 0 {
		args["x-message-ttl"] = task.MessagesLifetime
	}

	err := RMQChannel.ExchangeDeclare(
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

// DeclareExchanges - declare RMQ exchanges list.
// exchange name -> exchange type
func (r *RMQHandler) DeclareExchanges(exchangeTypes map[string]string) errs.APIError {
	ch, err := r.getChannel()
	if err != nil {
		return err
	}

	for exchangeName, exchangeType := range exchangeTypes {
		err := r.rmqExchangeDeclare(ch, RMQExchangeDeclareTask{
			ExchangeName: exchangeName,
			ExchangeType: exchangeType,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// IsQueueExists - is queue exists? /ᐠ｡ꞈ｡ᐟ\
func (r *RMQHandler) IsQueueExists(name string) (bool, errs.APIError) {
	ch, err := r.getChannel()
	if err != nil {
		return false, err
	}

	_, rmqErr := ch.QueueDeclarePassive(name, true, false, false, false, nil)
	if rmqErr != nil {
		if strings.Contains(rmqErr.Error(), "NOT_FOUND - no queue") {
			return false, nil
		}
		return false, constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to get queue `"+name+"` state: "+rmqErr.Error(),
		)
	}

	return true, nil
}
