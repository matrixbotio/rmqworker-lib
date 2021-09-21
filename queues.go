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
