package main

import (
	"fmt"

	"github.com/matrixbotio/go-common-lib/zes"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/cmd"
)

func main() {
	logger, loggerErr := zes.Init(false)
	if loggerErr != nil {
		panic(loggerErr)
	}
	defer logger.Close()
	zap.ReplaceGlobals(logger.New(zap.DebugLevel))

	h := cmd.GetHandler()

	h.StartCSTXAcksConsumer()

	transaction, err := h.BeginCSTX(2, 3000)
	if err != nil {
		panic(err)
	}

	transaction.PublishToQueue(rmqworker.RMQPublishRequestTask{
		QueueName:          "service1",
		MessageBody:        "body-body-body-service1",
		ResponseRoutingKey: "",
		CorrelationID:      "",
		CSTX:               *transaction,
	})

	transaction.PublishToQueue(rmqworker.RMQPublishRequestTask{
		QueueName:          "service2",
		MessageBody:        "body-body-body-service2",
		ResponseRoutingKey: "",
		CorrelationID:      "",
		CSTX:               *transaction,
	})

	res, err := transaction.Commit()
	if err != nil {
		zap.L().Debug(fmt.Sprintf("commit error: %#v", err))
	} else {
		zap.L().Debug(fmt.Sprintf("commit result: %v", res))
	}
}
