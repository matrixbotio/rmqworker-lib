package main

import (
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib/cmd"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

func main() {
	logger, loggerErr := zap.NewDevelopment()
	if loggerErr != nil {
		panic(loggerErr)
	}
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	h := cmd.GetHandler()

	err := h.PublishToQueue(structs.RMQPublishRequestTask{
		QueueName:          "alex",
		MessageBody:        "body-body-body",
		ResponseRoutingKey: "",
		CorrelationID:      "",
	})
	if err != nil {
		panic(err)
	}
}
