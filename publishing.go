package rmqworker

import (
	"context"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// RMQPublishToQueue - send request to rmq queue.
// NOTE: should be replaced by PublishToExchange in later lib versions
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
	})
}

// PublishToQueue - send request to rmq queue
func (r *RMQHandler) PublishToQueue(task RMQPublishRequestTask) APIError {
	headers := amqp.Table{}
	if task.ResponseRoutingKey != "" {
		headers["responseRoutingKey"] = task.ResponseRoutingKey
	}

	// encode message
	body, err := encodeMessage(task.MessageBody)
	if err != nil {
		return err
	}

	correlationID := task.CorrelationID
	if correlationID == "" {
		correlationID = getUUID()
	}

	headers = setCSTXHeaders(headers, task.CSTX)

	return r.publishMessage(publishTask{
		exchangeName: "",
		key:          task.QueueName,
		publishing: amqp.Publishing{
			Headers:       headers,
			CorrelationId: correlationID,
			DeliveryMode:  amqp.Persistent,
			ContentType:   defaultContentType,
			Body:          body,
		},
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
	})
}

// RMQPublishToExchange - publish message to exchange.
// responseRoutingKey is optional to send requests to exchange.
// NOTE: should be replaced by PublishToExchange in later lib versions
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
	})
}

// PublishToExchange - publish message to exchangeю
// responseRoutingKey is optional to send requests to exchange
func (r *RMQHandler) PublishToExchange(task PublishToExchangeTask) APIError {
	headers := amqp.Table{}
	if task.ResponseRoutingKey != "" {
		headers["responseRoutingKey"] = task.ResponseRoutingKey
	}

	jsonBytes, err := encodeMessage(task.Message)
	if err != nil {
		return err
	}

	correlationID := task.CorrelationID
	if correlationID == "" {
		correlationID = getUUID()
	}

	headers = setCSTXHeaders(headers, task.cstx)

	return r.publishMessage(publishTask{
		exchangeName: task.ExchangeName,
		key:          task.RoutingKey,
		publishing: amqp.Publishing{
			Headers:       headers,
			ContentType:   defaultContentType,
			CorrelationId: correlationID,
			Body:          jsonBytes,
		},
	})
}

type publishTask struct {
	exchangeName string
	key          string
	publishing   amqp.Publishing
}

func (r *RMQHandler) publishMessage(task publishTask) APIError {
	r.rlock()
	defer r.runlock()

	ctx, ctxCancel := context.WithTimeout(context.Background(), publishTimeout)
	defer ctxCancel()

	err := r.publisher.Publish(ctx, task.exchangeName, task.key, task.publishing)
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
