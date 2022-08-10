package main

import (
	"log"

	"github.com/matrixbotio/rmqworker-lib"
	"github.com/matrixbotio/rmqworker-lib/cmd"
)

func main() {
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

	res, err := transaction.Commit()
	if err != nil {
		log.Printf("commit error: %#v", err)
	}
	log.Printf("commit result: %v", res)
}
