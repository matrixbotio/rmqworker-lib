package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// RMQQueueDeclare - declare RMQ queue
func RMQQueueDeclare(RMQChannel *amqp.Channel, queueName string, durable bool, autodelete bool) APIError {
	_, err := RMQChannel.QueueDeclare(
		queueName,  // name
		durable,    // durable
		autodelete, // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to declare '"+queueName+"' queue: "+err.Error(),
		)
	}
	return nil
}

// RMQQueueBind - bind queue to exchange
func RMQQueueBind(RMQChannel *amqp.Channel, fromExchangeName, toQueueName, routingKey string) APIError {
	err := RMQChannel.QueueBind(
		toQueueName,      // queue name
		routingKey,       // routing key
		fromExchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to bind '"+toQueueName+"' queue to '"+
				fromExchangeName+"' exchange: "+err.Error(),
		)
	}
	return nil
}

// RMQQueueDeclareAndBind - declare queue & bind to exchange
func RMQQueueDeclareAndBind(task RMQQueueDeclareTask) APIError {
	// declare
	err := RMQQueueDeclare(task.RMQChannel, task.QueueName, task.Durable, task.AutoDelete)
	if err != nil {
		return err
	}

	// bind
	return RMQQueueBind(task.RMQChannel, task.FromExchangeName, task.QueueName, task.RoutingKey)
}
