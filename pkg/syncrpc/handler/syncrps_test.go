package handler

import (
	"errors"
	"testing"

	"github.com/matrixbotio/constants-lib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/pkg/syncrpc/handler/dependencies"
)

func TestCallbackWithResponseSuccessful(t *testing.T) {
	// given
	deliveryHandler := dependencies.NewMockRMQDeliveryHandler(t)
	rmqHandler := dependencies.NewMockRMQHandler(t)

	deliveryHandler.EXPECT().GetResponseRoutingKeyHeader().Return("mockedResponseRoutingKey", nil)
	deliveryHandler.EXPECT().GetCorrelationID().Return("mockedCorrelationID")

	expectedResult := "ok"

	actualResult := ""
	rmqHandler.EXPECT().SendRMQResponse(mock.Anything, (*constants.APIError)(nil)).
		Run(func(task *rmqworker.RMQPublishResponseTask, errorMsg ...*constants.APIError) {
			actualResult = task.MessageBody.(string)
		}).Return(nil)

	callback := func(w *rmqworker.RMQWorker, deliveryHandler dependencies.RMQDeliveryHandler) (any, error) {
		return expectedResult, nil
	}

	handler := Handler{
		rmqHandler: rmqHandler,
		callback:   callback,
	}

	// when
	handler.callbackWithResponse(deliveryHandler)

	// then
	assert.Equal(t, expectedResult, actualResult)
}

func TestCallbackWithResponseErrorResult(t *testing.T) {
	// given
	deliveryHandler := dependencies.NewMockRMQDeliveryHandler(t)
	rmqHandler := dependencies.NewMockRMQHandler(t)

	deliveryHandler.EXPECT().GetResponseRoutingKeyHeader().Return("mockedResponseRoutingKey", nil)
	deliveryHandler.EXPECT().GetCorrelationID().Return("mockedCorrelationID")

	var responseErr *constants.APIError
	rmqHandler.EXPECT().SendRMQResponse(mock.Anything, mock.Anything).
		Run(func(task *rmqworker.RMQPublishResponseTask, errorMsg ...*constants.APIError) {
			responseErr = errorMsg[0]
		}).Return(nil)

	errMsg := "mocked error"

	callback := func(w *rmqworker.RMQWorker, deliveryHandler dependencies.RMQDeliveryHandler) (any, error) {
		return nil, errors.New(errMsg)
	}

	handler := Handler{
		rmqHandler: rmqHandler,
		callback:   callback,
	}

	// when then
	assert.NotPanics(t, func() {
		handler.callbackWithResponse(deliveryHandler)
	})
	assert.Equal(t, errMsg, responseErr.Message)
}
