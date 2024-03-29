package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"

	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
)

// RMQDeliveryHandler - RMQ delivery data container
type RMQDeliveryHandler struct {
	rmqDelivery amqp.Delivery
	isAccepted  bool
}

// NewRMQDeliveryHandler - create new RMQ delivery handler
func NewRMQDeliveryHandler(delivery amqp.Delivery) RMQDeliveryHandler {
	return RMQDeliveryHandler{
		rmqDelivery: delivery,
	}
}

// GetMessageBody from RMQ delivery
func (d *RMQDeliveryHandler) GetMessageBody() []byte {
	return d.rmqDelivery.Body
}

// GetHeader from RMQ delivery headers.
// returns header value, is header exists (bool)
func (d *RMQDeliveryHandler) GetHeader(headerName string) (interface{}, bool) {
	headerValue, isHeaderExists := d.rmqDelivery.Headers[headerName]
	return headerValue, isHeaderExists
}

// GetResponseRoutingKeyHeader - get response routing key from delivery headers
func (d *RMQDeliveryHandler) GetResponseRoutingKeyHeader() (string, errs.APIError) {
	// get header
	headerRaw, exists := d.GetHeader("responseRoutingKey")
	if !exists {
		return "", nil
	}

	// convert to string
	header, isConvertable := headerRaw.(string)
	if !isConvertable {
		return "", constants.Error(
			"DATA_PARSE_ERR",
			"failed to parse header in from RMQ delivery to string",
		)
	}
	return header, nil
}

// GetCorrelationID from RMQ delivery
func (d *RMQDeliveryHandler) GetCorrelationID() string {
	return d.rmqDelivery.CorrelationId
}

// GetRoutingKey from RMQ delivery
func (d *RMQDeliveryHandler) GetRoutingKey() string {
	return d.rmqDelivery.RoutingKey
}

// Accept RMQ message delivery
func (d *RMQDeliveryHandler) Accept() errs.APIError {
	if d.isAccepted {
		return constants.Error(
			"DATA_REQ_ERR",
			"message has already been accepted",
		)
	}

	err := d.simpleAccept()
	if err != nil {
		return err
	}

	d.isAccepted = true
	return nil
}

func (d *RMQDeliveryHandler) simpleAccept() errs.APIError {
	err := d.rmqDelivery.Ack(false)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to accept msg: "+err.Error(),
		)
	}
	return nil
}

// Reject RMQ message delivery
func (d *RMQDeliveryHandler) Reject(requeue bool) errs.APIError {
	err := d.rmqDelivery.Reject(requeue)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to reject delivery: "+err.Error(),
		)
	}
	return nil
}

// CheckResponseError - check RMQ response error
func (d *RMQDeliveryHandler) CheckResponseError() errs.APIError {
	responseCodeRaw, isFieldFound := d.GetHeader("code")
	if !isFieldFound {
		return nil
	}

	responseCode, isConvertable := responseCodeRaw.(int32)
	if !isConvertable {
		errMessage := "failed to parse rmq response code"
		headers := d.rmqDelivery.Headers
		headersBytes, err := json.Marshal(headers)
		if err == nil {
			errMessage += ". headers: " + string(headersBytes)
		}

		return constants.Error(
			"DATA_PARSE_ERR",
			errMessage,
		)
	}
	if int64(responseCode) == 0 {
		// no errors
		return nil
	}

	var errName string = "UNKNOWN"
	errNameRaw, isErrorNameFound := d.GetHeader("name")
	if isErrorNameFound {
		errName, isConvertable = errNameRaw.(string)
		if !isConvertable {
			return constants.Error(
				"DATA_PARSE_ERR",
				"failed to parse rmq error name",
			)
		}
	}

	var errorStack string
	errorStackRaw, _ := d.GetHeader("stack")
	errorStack, isConvertable = errorStackRaw.(string)
	if !isConvertable {
		errorStack = ""
	}
	errMessage := string(d.GetMessageBody())

	if errorStack != "" {
		return &constants.APIError{
			Message: errMessage,
			Code:    int(responseCode),
			Name:    errName,
			Stack:   errorStack,
		}
	}

	return constants.Error(errName, errMessage)
}
