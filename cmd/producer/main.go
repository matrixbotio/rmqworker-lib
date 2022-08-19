package main

import (
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

	h := cmd.GetHandler(logger.New(zap.DebugLevel))

	err := h.PublishToQueue(rmqworker.RMQPublishRequestTask{
		QueueName:          "alex",
		MessageBody:        "body-body-body",
		ResponseRoutingKey: "",
		CorrelationID:      "",
		CSTX:               rmqworker.CrossServiceTransaction{},
	})
	if err != nil {
		panic(err)
	}
}
