package rmqserver

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/matrixbotio/constants-lib"
	"github.com/matrixbotio/rmqworker-lib"
	"go.uber.org/zap"
)

type Message struct {
	Method string           `json:"method"`
	Data   *json.RawMessage `json:"data"`
}

func (s *Server) callback(_ *rmqworker.RMQWorker, deliveryHandler rmqworker.RMQDeliveryHandler) {
	go func() {
		var msg Message
		if err := json.Unmarshal(deliveryHandler.GetMessageBody(), &msg); err != nil {
			fmt.Printf("rmqserver.callback error unmarshalling method: %v", err)
			return
		}

		res, err := s.manager(msg.Method, msg.Data)

		ResponseRoutingKeyHeader, _ := deliveryHandler.GetResponseRoutingKeyHeader()
		task := &rmqworker.RMQPublishResponseTask{
			CorrelationID:      deliveryHandler.GetCorrelationID(),
			MessageBody:        res,
			ExchangeName:       "",
			ResponseRoutingKey: ResponseRoutingKeyHeader,
		}
		// no need to send response if no routing key
		if ResponseRoutingKeyHeader == "" {
			return
		}

		if err != nil {
			errorMsg := &constants.APIError{
				Message: err.Error(),
				Code:    -32603,
				Name:    "BASE_INTERNAL_ERROR",
			}
			apiErr := s.rmqHandler.SendRMQResponse(task, errorMsg)
			if apiErr != nil {
				fmt.Printf("send rmq response: %v", apiErr)
			}
			return
		}

		apiErr := s.rmqHandler.SendRMQResponse(task)
		if apiErr != nil {
			fmt.Printf("send rmq response: %v", apiErr)
		}
	}()
}

func (s *Server) errorCallback(_ *rmqworker.RMQWorker, err *constants.APIError) {
	if err != nil {
		zap.L().Error(
			"rmqserver.errorCallback",
			zap.Error(errors.New(err.Message)),
		)
	}
}
