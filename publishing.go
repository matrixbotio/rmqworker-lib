package rmqworker

import (
	"encoding/json"
	"errors"

	"github.com/streadway/amqp"
)

// RMQPublishInterfaceToQueue - another version of rmqPublishToQueue. use `message` instead of `task.MessageBody`
func RMQPublishInterfaceToQueue(task RMQPublishRequestTask, message interface{}) error {
	var err error
	task.MessageBody, err = json.Marshal(message)
	if err != nil {
		return errors.New("failed to encode message to json: " + err.Error())
	}
	return rmqPublishToQueue(task)
}

// send request to rmq queue
func rmqPublishToQueue(task RMQPublishRequestTask) error {
	headers := amqp.Table{}
	if task.ResponseRoutingKey != "" {
		headers["responseRoutingKey"] = task.ResponseRoutingKey
	}

	err := task.RMQChannel.Publish(
		"",             // exchange
		task.QueueName, // queue
		false,          // mandatory
		false,
		amqp.Publishing{
			Headers:      headers,
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         task.MessageBody,
		},
	)
	if err != nil {
		return errors.New("failed to push event to rmq queue: " + err.Error())
	}
	return nil
}
