package handler

import (
	"encoding/json"

	"github.com/matrixbotio/constants-lib"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
)

type Callback func(w *rmqworker.RMQWorker, deliveryHandler RMQDeliveryHandler, request any) (any, error)

func (h *Handler[T]) callbackWithResponse(deliveryHandler RMQDeliveryHandler) {
	bodyBytes := deliveryHandler.GetMessageBody()

	var request T
	var response any
	var err error

	if len(bodyBytes) > 0 {
		err = json.Unmarshal(bodyBytes, &request)
	}

	if err == nil {
		response, err = h.callback(h.worker, deliveryHandler, request)
	}

	responseRoutingKey, _ := deliveryHandler.GetResponseRoutingKeyHeader()

	if responseRoutingKey != "" {
		exchange := h.props.Exchange
		responseExchangeName := exchange + ".response"

		errRespTask := rmqworker.RMQPublishResponseTask{
			ExchangeName:       responseExchangeName,
			ResponseRoutingKey: responseRoutingKey,
			CorrelationID:      deliveryHandler.GetCorrelationID(),
			MessageBody:        response,
		}
		var responseErr *constants.APIError
		if err != nil {
			responseErr = constants.Error("BASE_INTERNAL_ERROR", err.Error())
		}

		if err := h.rmqHandler.SendRMQResponse(&errRespTask, responseErr); err != nil {
			zap.L().Error("SendRMQResponse", zap.Error(*err), zap.String("method_name", exchange))
		}
	}
}
