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
		r.Connections.Publish.Channel, err = openRMQChannel(r.Connections.Publish.Conn)
		if err != nil {
			r.Logger.Warn(convertRMQError(err))
		}
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

	var isErrorFound bool
	if len(errorMsg) == 0 {
		isErrorFound = false
	} else {
		isErrorFound = errorMsg[0] != nil
	}

	if !isErrorFound {
		// no errors
		headers["code"] = 0
		// encode message
		var err APIError
		responseBody, err = encodeMessage(task.MessageBody)
		if err != nil {
			return err
		}
	} else {
		// add error to header & body
		headers["code"] = errorMsg[0].Code
		headers["name"] = errorMsg[0].Name
		responseBody = []byte(errorMsg[0].Message)
		contentType = "text/plain"
	}

	// check RMQ connection
	err := checkRMQConnection(
		r.Connections.Publish.Conn,    // connection
		r.Connections.Data,            // data for new connections
		r.Connections.Publish.Channel, // channel
		r.Logger,                      // logger
	)
	if err != nil {
		return err
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
		r.Connections.Publish.Channel, err = openRMQChannel(r.Connections.Publish.Conn)
		if err != nil {
			r.Logger.Warn(convertRMQError(err))
		}
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
		r.Connections.Publish.Channel, err = openRMQChannel(r.Connections.Publish.Conn)
		if err != nil {
			r.Logger.Warn(convertRMQError(err))
		}
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to push message to exchange: "+rmqErr.Error(),
		)
	}
	return nil
}
