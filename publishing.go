package rmqworker

import (
	"encoding/json"
	"errors"

	"github.com/matrixbotio/constants-lib"
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

// RMQCheckResponseError - check RMQ response error
func RMQCheckResponseError(rmqDelivery amqp.Delivery) APIError {
	responseCodeRaw, isErrorFound := rmqDelivery.Headers["code"]
	if isErrorFound {
		responseCode, isConvertable := responseCodeRaw.(int64)
		if !isConvertable {
			return constants.Error(
				"DATA_PARSE_ERR",
				"failed to parse rmq response code",
			)
		}
		if responseCode == 0 {
			// no errors
			return nil
		}
		var errName string = "UNKNOWN"
		errNameRaw, isErrorNameFound := rmqDelivery.Headers["name"]
		if isErrorNameFound {
			errName, isConvertable = errNameRaw.(string)
			if !isConvertable {
				return constants.Error(
					"DATA_PARSE_ERR",
					"failed to parse rmq error name",
				)
			}
		}

		errMessage := string(rmqDelivery.Body)
		return constants.Error(errName, errMessage)
	}
	return nil
}
