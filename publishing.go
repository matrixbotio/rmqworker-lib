package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// RMQPublishInterfaceToQueue - another version of rmqPublishToQueue. use `message` instead of `task.MessageBody`
func (r *RMQHandler) RMQPublishInterfaceToQueue(task RMQPublishRequestTask, message interface{}) APIError {
	var err APIError
	task.MessageBody, err = encodeMessage(message)
	if err != nil {
		return err
	}
	return r.RMQPublishToQueue(task)
}

// RMQPublishToQueue - send request to rmq queue
func (r *RMQHandler) RMQPublishToQueue(task RMQPublishRequestTask) APIError {
	headers := amqp.Table{}
	if task.ResponseRoutingKey != "" {
		headers["responseRoutingKey"] = task.ResponseRoutingKey
	}

	// encode message
	body, err := encodeMessage(task.MessageBody)
	if err != nil {
		return err
	}

	rmqErr := r.RMQChannel.Publish(
		"",             // exchange
		task.QueueName, // queue
		false,          // mandatory
		false,
		amqp.Publishing{
			CorrelationId: getUUID(),
			Headers:       headers,
			DeliveryMode:  amqp.Persistent,
			ContentType:   "application/json",
			Body:          body,
		},
	)
	if rmqErr != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to push event to rmq queue: "+rmqErr.Error(),
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
	var responseToEncode interface{}
	contentType := "application/json"

	var isErrorFound bool
	if len(errorMsg) == 0 {
		isErrorFound = false
	} else {
		isErrorFound = errorMsg[0] != nil
	}

	if !isErrorFound {
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

	// encode message
	responseBody, err := encodeMessage(responseToEncode)
	if err != nil {
		return err
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

// RMQPublishToExchange - publish message to exchange
func (r *RMQHandler) RMQPublishToExchange(message interface{}, exchangeName, routingKey string) APIError {
	// encode message
	jsonBytes, err := encodeMessage(message)
	if err != nil {
		return err
	}

	// publish message
	rmqErr := r.RMQChannel.Publish(
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonBytes,
		})
	if rmqErr != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to push message to exchange: "+rmqErr.Error(),
		)
	}
	return nil
}
