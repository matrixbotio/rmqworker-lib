package main

import (
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib/cmd"
	"github.com/matrixbotio/rmqworker-lib/pkg/cstx"
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

	//h.StartCSTXAcksConsumer()

	transaction := cstx.New(1, 3000, h)

	zap.L().Info("transaction started", zap.String("id", transaction.ID))

	transaction.PublishToQueue(structs.RMQPublishRequestTask{
		QueueName:          "service1",
		MessageBody:        "body-body-body-service1",
		ResponseRoutingKey: "",
		CorrelationID:      "",
	})

	transaction.PublishToQueue(structs.RMQPublishRequestTask{
		QueueName:          "service2",
		MessageBody:        "body-body-body-service2",
		ResponseRoutingKey: "",
		CorrelationID:      "",
	})

	//res, err := transaction.Commit()
	//if err != nil {
	//	zap.L().Debug(fmt.Sprintf("commit error: %#v", err))
	//} else {
	//	zap.L().Debug(fmt.Sprintf("commit result: %v", res))
	//}
}
