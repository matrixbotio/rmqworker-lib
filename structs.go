package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// APIError - error data container
type APIError *constants.APIError

type rmqConnectionData struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	UseTLS   string `json:"tls"`
}

// RMQQueueDeclareTask - queue declare task data container
type RMQQueueDeclareTask struct {
	RMQChannel       *amqp.Channel
	QueueName        string
	Durable          bool
	AutoDelete       bool
	FromExchangeName string
	RoutingKey       string
}
