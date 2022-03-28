package rmqworker

import (
	"testing"

	"github.com/streadway/amqp"
)

func TestDeliveryHandler(t *testing.T) {
	message := "failed to generate joke"
	correlationID := "9b5472cd-cfe0-4cbc-8735-2ec105240bc3"
	responseRoutingKey := "054e5b61-7147-4fde-842f-af1409ca36fc"
	var errorCode int32 = -31000
	errorName := "DATA_HANDLE_ERR"

	delivery := NewRMQDeliveryHandler(amqp.Delivery{
		Headers: amqp.Table{
			"responseRoutingKey": responseRoutingKey,
			"code":               errorCode,
			"name":               errorName,
			"stack":              "main.go:8020, solution.go:5040, workers.go:34",
		},
		CorrelationId: correlationID,
		DeliveryTag:   5040,
		RoutingKey:    "binance-spot",
		Body:          []byte(message),
	})

	// test headers
	_, isHeaderFound := delivery.GetHeader("responseRoutingKey")
	if !isHeaderFound {
		t.Fatal("failed to find header field")
	}

	// test correlation ID
	testCorrelationID := delivery.GetCorrelationID()
	if testCorrelationID != correlationID {
		t.Fatal("correlation ID `" + correlationID + "` is not equal to `" + testCorrelationID + "`")
	}

	// test response error
	err := delivery.CheckResponseError()
	if err == nil {
		t.Fatal("error should be set")
	}
}
