package tasks

import "github.com/matrixbotio/rmqworker-lib/pkg/cstx"

// RMQPublishRequestTask - publish message to RMQ task data container
type RMQPublishRequestTask struct {
	// required
	QueueName   string
	MessageBody interface{}

	// optional
	ResponseRoutingKey string
	CorrelationID      string
	CSTX               cstx.CrossServiceTransaction
}

type PublishToExchangeTask struct {
	// required
	Message      interface{}
	ExchangeName string

	// optional
	RoutingKey         string
	ResponseRoutingKey string
	CorrelationID      string

	CSTX cstx.CrossServiceTransaction
}
