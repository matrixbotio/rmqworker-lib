package rmqworker

import (
	"strconv"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

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

	rmqErr := r.Connections.Publish.Channel.Publish(
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
	var responseBody []byte
	contentType := "application/json"

	r.Logger.Verbose(errorMsg) // TEMP

	var isErrorFound bool
	if len(errorMsg) == 0 {
		isErrorFound = false
	} else {
		isErrorFound = errorMsg[0] != nil
	}
	r.Logger.Verbose("RMQ Handler: isErrorFound = " + strconv.FormatBool(isErrorFound)) // // TEMP

	if !isErrorFound {
		r.Logger.Verbose("RMQ Handler: no errors found") // TEMP
		// no errors
		headers["code"] = 0
		// encode message
		var err APIError
		responseBody, err = encodeMessage(task.MessageBody)
		if err != nil {
			return err
		}
	} else {
		r.Logger.Verbose("RMQ Handler: add error info to respone: " + errorMsg[0].Name) // TEMP
		// add error to header & body
		headers["code"] = errorMsg[0].Code
		headers["name"] = errorMsg[0].Name
		responseBody = []byte(errorMsg[0].Message)
		contentType = "text/plain"
	}

	// push result to rmq
	rmqErr := r.Connections.Publish.Channel.Publish(
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
	rmqErr := r.Connections.Publish.Channel.Publish(
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
