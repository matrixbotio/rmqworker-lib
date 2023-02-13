package syncrpc

import (
	"github.com/matrixbotio/constants-lib"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/pkg/syncrpc/dependencies"
)

type Callback func(w *rmqworker.RMQWorker, deliveryHandler dependencies.RMQDeliveryHandler) (any, error)

func (h *Handler) callbackWithResponse(deliveryHandler dependencies.RMQDeliveryHandler, callback Callback) {
	response, err := callback(h.worker, deliveryHandler)

	exchange := h.props.Exchange
	responseRoutingKey, _ := deliveryHandler.GetResponseRoutingKeyHeader()

	if responseRoutingKey != "" {
		responseExchangeName := h.props.Exchange + ".response"

		errRespTask := rmqworker.RMQPublishResponseTask{
			ExchangeName:       responseExchangeName,
			ResponseRoutingKey: responseRoutingKey,
			CorrelationID:      deliveryHandler.GetCorrelationID(),
			MessageBody:        response,
		}
		var responseErr *constants.APIError = nil
		if err != nil {
			responseErr = constants.Error("BASE_INTERNAL_ERROR", err.Error())
		}

		if err := h.rmqHandler.SendRMQResponse(
			&errRespTask,
			responseErr,
		); err != nil {
			zap.L().Error("SendRMQResponse", zap.Error(*err), zap.String("method_name", exchange))
		}
	}
}
