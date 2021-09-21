package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// RMQDeliveryHandler - RMQ delivery data container
type RMQDeliveryHandler struct {
	rmqDelivery amqp.Delivery
}

func newRMQDeliveryHandler(delivery amqp.Delivery) RMQDeliveryHandler {
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
