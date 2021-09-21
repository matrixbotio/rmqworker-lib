package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// RMQDeliveryHandler - RMQ delivery data container
type RMQDeliveryHandler struct {
	rmqDelivery amqp.Delivery
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

// GetCorrelationID from RMQ delivery
func (d *RMQDeliveryHandler) GetCorrelationID() string {
	return d.rmqDelivery.CorrelationId
}

// GetRoutingKey from RMQ delivery
func (d *RMQDeliveryHandler) GetRoutingKey() string {
	return d.rmqDelivery.RoutingKey
}

// Accept RMQ message delivery
func (d *RMQDeliveryHandler) Accept() APIError {
	err := d.rmqDelivery.Acknowledger.Ack(d.rmqDelivery.DeliveryTag, false)
	if err != nil {
		return constants.Error(
			"DATA_HANDLE_ERR",
			"failed to ack task: "+err.Error(),
		)
	}
	return nil
}

// CheckResponseError - check RMQ response error
func (d *RMQDeliveryHandler) CheckResponseError() APIError {
	responseCodeRaw, isErrorFound := d.rmqDelivery.Headers["code"]
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

		errMessage := string(d.GetMessageBody())
		return constants.Error(errName, errMessage)
	}
	return nil
}
