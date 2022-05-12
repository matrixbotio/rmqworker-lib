package rmqworker

import (
	"context"

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

	return r.publishMessage(publishTask{
		exchangeName: "",
		key:          task.QueueName,
		publishing: amqp.Publishing{
			CorrelationId: getUUID(),
			Headers:       headers,
			DeliveryMode:  amqp.Persistent,
			ContentType:   defaultContentType,
			Body:          body,
		},
		ensureDelivered: false,
	})
}

// SendRMQResponse - publish message to RMQ exchange
func (r *RMQHandler) SendRMQResponse(
	task *RMQPublishResponseTask,
	errorMsg ...*constants.APIError,
) APIError {
	headers := amqp.Table{}
	var responseBody []byte
	contentType := defaultContentType

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
		headers["stack"] = errorMsg[0].Stack
		responseBody = []byte(errorMsg[0].Message)
		contentType = "text/plain"
	}

	// push result to rmq
	return r.publishMessage(publishTask{
		exchangeName: task.ExchangeName,
		key:          task.ResponseRoutingKey,
		publishing: amqp.Publishing{
			Headers:       headers,
			ContentType:   contentType,
			Body:          responseBody,
			CorrelationId: task.CorrelationID,
		},
		ensureDelivered: true,
	})
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

	return r.publishMessage(publishTask{
		exchangeName: exchangeName,
		key:          routingKey,
		publishing: amqp.Publishing{
			Headers:     headers,
			ContentType: defaultContentType,
			Body:        jsonBytes,
		},
		ensureDelivered: false,
	})
}

type publishTask struct {
	exchangeName    string
	key             string
	publishing      amqp.Publishing
	ensureDelivered bool
}

func (r *RMQHandler) publishMessage(task publishTask) APIError {
	ctx, ctxCancel := context.WithTimeout(context.Background(), publishTimeout)
	defer ctxCancel()

	var err error
	if task.ensureDelivered {
		err = r.ensurePublisher.Publish(ctx, task.exchangeName, task.key, task.publishing)
	} else {
		err = r.firePublisher.Publish(ctx, task.exchangeName, task.key, task.publishing)
	}

	if err != nil {
		if checkContextDeadlineErr(err) {
			return publishDeadlineError
		}
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to push message to exchange: "+err.Error(),
		)
	}
	return nil
}
