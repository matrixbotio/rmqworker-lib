package rmqworker

import (
	"encoding/json"
	"errors"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// rmqPublishInterfaceToQueue - another version of rmqPublishToQueue. use `message` instead of `task.MessageBody`
func (r *RMQHandler) rmqPublishInterfaceToQueue(task RMQPublishRequestTask, message interface{}) error {
	var err error
	task.MessageBody, err = json.Marshal(message)
	if err != nil {
		return errors.New("failed to encode message to json: " + err.Error())
	}
	return r.rmqPublishToQueue(task)
}

// rmqPublishToQueue - send request to rmq queue
func (r *RMQHandler) rmqPublishToQueue(task RMQPublishRequestTask) error {
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

// rmqCheckResponseError - check RMQ response error
func (r *RMQHandler) rmqCheckResponseError(rmqDelivery amqp.Delivery) APIError {
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

// sendRMQResponse - publish message to RMQ exchange
func (r *RMQHandler) sendRMQResponse(task *RMQPublishResponseTask, errorMsg ...*constants.APIError) APIError {
	headers := amqp.Table{}
	var responseBody []byte
	var responseToEncode interface{}
	if len(errorMsg) == 0 {
		// no errors
		headers["code"] = 0
		responseToEncode = task.MessageBody
	} else {
		// add error to header & body
		headers["code"] = errorMsg[0].Code
		headers["name"] = errorMsg[0].Name
		responseToEncode = errorMsg[0].Message
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
	newChannel, err := r.checkRMQConnection(task.RMQConn, r.ConnectionData)
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
		task.RMQChannel = newChannel
	}

	// push result to rmq
	rmqErr := task.RMQChannel.Publish(
		task.ExchangeName,       // exchange
		task.ResponseRoutingKey, // routing key
		false,                   // mandatory
		false,                   // immediate
		amqp.Publishing{
			Headers:     headers,
			ContentType: "application/json",
			Body:        responseBody,
		})
	if rmqErr != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to push rmq response: "+rmqErr.Error(),
		)
	}
	return nil
}
