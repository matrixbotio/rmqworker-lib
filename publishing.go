package rmqworker

import (
	"encoding/json"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// RMQPublishInterfaceToQueue - another version of rmqPublishToQueue. use `message` instead of `task.MessageBody`
func (r *RMQHandler) RMQPublishInterfaceToQueue(task RMQPublishRequestTask, message interface{}) APIError {
	var convErr error
	task.MessageBody, convErr = json.Marshal(message)
	if convErr != nil {
		return constants.Error(
			"DATA_ENCODE_ERR",
			"failed to encode message to json: "+convErr.Error(),
		)
	}
	return r.RMQPublishToQueue(task)
}

// RMQPublishToQueue - send request to rmq queue
func (r *RMQHandler) RMQPublishToQueue(task RMQPublishRequestTask) APIError {
	headers := amqp.Table{}
	if task.ResponseRoutingKey != "" {
		headers["responseRoutingKey"] = task.ResponseRoutingKey
	}

	err := r.RMQChannel.Publish(
		"",             // exchange
		task.QueueName, // queue
		false,          // mandatory
		false,
		amqp.Publishing{
			Headers:      headers,
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         task.MessageBody,
		},
	)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to push event to rmq queue: "+err.Error(),
		)
	}
	return nil
}

// SendRMQResponse - publish message to RMQ exchange
func (r *RMQHandler) SendRMQResponse(
	task *RMQPublishResponseTask,
	errorMsg ...*constants.APIError,
) APIError {
	headers := amqp.Table{}
	var responseBody []byte
	var responseToEncode interface{}
	contentType := "application/json"

	if len(errorMsg) == 0 {
		// no errors
		headers["code"] = 0
		responseToEncode = task.MessageBody
	} else {
		// add error to header & body
		headers["code"] = errorMsg[0].Code
		headers["name"] = errorMsg[0].Name
		responseToEncode = errorMsg[0].Message
		contentType = "text/plain"
	}

	// encode response to json
	responseBody, marshalErr := json.Marshal(responseToEncode)
	if marshalErr != nil {
		e := constants.Error(
			"DATA_ENCODE_ERR",
			"failed to marshal response to json: "+marshalErr.Error(),
		)
		return e
	}

	// check RMQ connection
	newChannel, err := checkRMQConnection(r.RMQConn, r.ConnectionData)
	if err != nil {
		// check connection is open
		if err.Name != "DATA_EXISTS" {
			return constants.Error(
				"SERVICE_REQ_FAILED",
				"failed to check RMQ connection: "+err.Message,
			)
		}
	}
	if newChannel != nil {
		// channel updated
		r.RMQChannel = newChannel
	}

	// push result to rmq
	rmqErr := r.RMQChannel.Publish(
		task.ExchangeName,       // exchange
		task.ResponseRoutingKey, // routing key
		false,                   // mandatory
		false,                   // immediate
		amqp.Publishing{
			Headers:       headers,
			ContentType:   contentType,
			Body:          responseBody,
			CorrelationId: task.CorrelationID,
		})
	if rmqErr != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to push rmq response: "+rmqErr.Error(),
		)
	}
	return nil
}
