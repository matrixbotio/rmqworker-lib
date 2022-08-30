package rmqworker

import (
	"context"

	"github.com/google/uuid"
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"

	"github.com/matrixbotio/rmqworker-lib/pkg/cstx"
	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

// RMQPublishToQueue - send request to rmq queue.
// NOTE: should be replaced by PublishToExchange in later lib versions
func (r *RMQHandler) RMQPublishToQueue(task structs.RMQPublishRequestTask) errs.APIError {
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
			CorrelationId: uuid.NewString(),
			Headers:       headers,
			DeliveryMode:  amqp.Persistent,
			ContentType:   defaultContentType,
			Body:          body,
		},
	})
}

// PublishToQueue - send request to rmq queue
func (r *RMQHandler) PublishToQueue(task structs.RMQPublishRequestTask, additionalHeaders ...structs.RMQHeader) errs.APIError {
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
		correlationID = uuid.NewString()
	}

	for _, h := range additionalHeaders {
		headers[h.Name] = h.Value
	}

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
) errs.APIError {
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
		var err errs.APIError
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
) errs.APIError {
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
func (r *RMQHandler) PublishToExchange(task structs.PublishToExchangeTask, additionalHeaders ...structs.RMQHeader) errs.APIError {
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
		correlationID = uuid.NewString()
	}

	for _, h := range additionalHeaders {
		headers[h.Name] = h.Value
	}

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

func (r *RMQHandler) PublishCSXTToQueue(task structs.RMQPublishRequestTask, tx cstx.CrossServiceTransaction) errs.APIError {
	headers := cstx.GetCSTXHeaders(tx)
	return r.PublishToQueue(task, headers...)
}

func (r *RMQHandler) PublishCSXTToExchange(task structs.PublishToExchangeTask, tx cstx.CrossServiceTransaction) errs.APIError {
	headers := cstx.GetCSTXHeaders(tx)
	return r.PublishToExchange(task, headers...)
}

type publishTask struct {
	exchangeName string
	key          string
	publishing   amqp.Publishing
}

func (r *RMQHandler) publishMessage(task publishTask) errs.APIError {
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
