package structs

// RMQPublishRequestTask - publish message to RMQ task data container
type RMQPublishRequestTask struct {
	// required
	QueueName   string
	MessageBody interface{}

	// optional
	ResponseRoutingKey string
	CorrelationID      string
}

type PublishToExchangeTask struct {
	// required
	Message      interface{}
	ExchangeName string

	// optional
	RoutingKey         string
	ResponseRoutingKey string
	CorrelationID      string
}
