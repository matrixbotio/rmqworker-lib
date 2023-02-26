package utils

func GetQueueName(exchange, routingKey string) string {
	queue := exchange
	if routingKey != "" {
		queue = queue + "-" + routingKey
	}
	return queue
}
