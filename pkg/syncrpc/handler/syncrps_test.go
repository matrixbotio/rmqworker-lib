package handler

import (
	"errors"
	"testing"

	"github.com/matrixbotio/constants-lib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/matrixbotio/rmqworker-lib"
)

func TestCallbackWithResponseSuccessful(t *testing.T) {
	// given
	deliveryHandler := NewMockRMQDeliveryHandler(t)
	rmqHandler := NewMockRMQHandler(t)

	deliveryHandler.EXPECT().GetMessageBody().Return([]byte("\"test\""))
	deliveryHandler.EXPECT().GetResponseRoutingKeyHeader().Return("mockedResponseRoutingKey", nil)
	deliveryHandler.EXPECT().GetCorrelationID().Return("mockedCorrelationID")

	actualResult := ""
	rmqHandler.EXPECT().SendRMQResponse(mock.Anything, (*constants.APIError)(nil)).
		Run(func(task *rmqworker.RMQPublishResponseTask, errorMsg ...*constants.APIError) {
			actualResult = task.MessageBody.(string)
		}).Return(nil)

	expectedResult := "ok"
	var parsedRequest string

	callback := func(w *rmqworker.RMQWorker, deliveryHandler RMQDeliveryHandler, request any) (any, error) {
		parsedRequest = request.(string)
		return expectedResult, nil
	}

	handler := Handler[string]{
		rmqHandler: rmqHandler,
		callback:   callback,
	}

	// when
	handler.callbackWithResponse(deliveryHandler)

	// then
	assert.Equal(t, "test", parsedRequest)
	assert.Equal(t, expectedResult, actualResult)
}

func TestCallbackWithResponseErrorResult(t *testing.T) {
	// given
	deliveryHandler := NewMockRMQDeliveryHandler(t)
	rmqHandler := NewMockRMQHandler(t)

	deliveryHandler.EXPECT().GetMessageBody().Return([]byte("\"test\""))
	deliveryHandler.EXPECT().GetResponseRoutingKeyHeader().Return("mockedResponseRoutingKey", nil)
	deliveryHandler.EXPECT().GetCorrelationID().Return("mockedCorrelationID")

	var responseErr *constants.APIError
	rmqHandler.EXPECT().SendRMQResponse(mock.Anything, mock.Anything).
		Run(func(task *rmqworker.RMQPublishResponseTask, errorMsg ...*constants.APIError) {
			responseErr = errorMsg[0]
		}).Return(nil)

	errMsg := "mocked error"

	callback := func(w *rmqworker.RMQWorker, deliveryHandler RMQDeliveryHandler, request any) (any, error) {
		return nil, errors.New(errMsg)
	}

	handler := Handler[string]{
		rmqHandler: rmqHandler,
		callback:   callback,
	}

	// when then
	assert.NotPanics(t, func() {
		handler.callbackWithResponse(deliveryHandler)
	})
	assert.Equal(t, errMsg, responseErr.Message)
}
