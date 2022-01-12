package rmqworker

import (
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

	err = r.publishMessage(
		"",
		task.QueueName,
		false,
		false,
		amqp.Publishing{
			CorrelationId: getUUID(),
			Headers:       headers,
			DeliveryMode:  amqp.Persistent,
			ContentType:   "application/json",
			Body:          body,
		})
	if err != nil {
		return err
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

	// push result to rmq
	err := r.publishMessage(
		task.ExchangeName,
		task.ResponseRoutingKey,
		false,
		false,
		amqp.Publishing{
			Headers:       headers,
			ContentType:   contentType,
			Body:          responseBody,
			CorrelationId: task.CorrelationID,
		})
	if err != nil {
		return err
	}
	return nil
}

// RMQPublishToExchange - publish message to exchangeю
// responseRoutingKey is optional to send requests to exchange
func (r *RMQHandler) RMQPublishToExchange(
	message interface{},
	exchangeName,
	routingKey string,
	responseRoutingKey ...string,
) APIError {
	headers := amqp.Table{}
	if len(responseRoutingKey) > 0 {
		headers["responseRoutingKey"] = responseRoutingKey[0]
	}

	jsonBytes, err := encodeMessage(message)
	if err != nil {
		return err
	}

	err = r.publishMessage(
		exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Headers:     headers,
			ContentType: "application/json",
			Body:        jsonBytes,
		})
	if err != nil {
		return err
	}
	return nil
}

func (r *RMQHandler) publishMessage(exchangeName string, key string, mandatory bool, immediate bool,
	publishing amqp.Publishing) APIError {
	r.Connections.Publish.mutex.Lock()
	defer r.Connections.Publish.mutex.Unlock()
	r.Connections.Publish.rwMutex.RLock()
	defer r.Connections.Publish.rwMutex.RUnlock()
	rmqErr := r.Connections.Publish.Channel.Publish(exchangeName, key, mandatory, immediate, publishing)
	if rmqErr != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to push message to exchange: "+rmqErr.Error(),
		)
	}
	return nil
}
