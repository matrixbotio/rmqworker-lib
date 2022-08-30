package main

import (
	"github.com/matrixbotio/go-common-lib/zes"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib/cmd"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

func main() {
	logger, loggerErr := zes.Init(false)
	if loggerErr != nil {
		panic(loggerErr)
	}
	defer logger.Close()
	zap.ReplaceGlobals(logger.New(zap.DebugLevel))

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
