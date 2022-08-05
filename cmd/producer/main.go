package main

import (
	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/cmd"
)

func main() {
	h := cmd.GetHandler()

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
