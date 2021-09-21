package rmqworker

import (
	"time"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// APIError - error data container
type APIError *constants.APIError

// RMQConnectionData - rmq connection data container
type RMQConnectionData struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	UseTLS   string `json:"tls"`
}

// rmqQueueDeclareTask - queue declare task data container
type rmqQueueDeclareTask struct {
	RMQChannel       *amqp.Channel
	QueueName        string
	Durable          bool
	AutoDelete       bool
	FromExchangeName string
	RoutingKey       string
}

// rmqPublishRequestTask - publish message to RMQ task data container
type rmqPublishRequestTask struct {
	RMQChannel         *amqp.Channel
	QueueName          string
	ResponseRoutingKey string
	MessageBody        []byte
}

// rmqPublishResponseTask - response for publish message to RMQ request
type rmqPublishResponseTask struct {
	RMQConn            *amqp.Connection
	RMQChannel         *amqp.Channel
	ExchangeName       string
	ResponseRoutingKey string
	MessageBody        []byte
}

// rmqMonitoringWorkerTask - new RMQ request->response monitoring worker data
type rmqMonitoringWorkerTask struct {
	// required
	QueueName        string
	ISQueueDurable   bool
	ISAutoDelete     bool
	FromExchangeName string
	RoutingKey       string
	RMQConn          *amqp.Connection
	RMQChannel       *amqp.Channel
	Callback         RMQDeliveryCallback

	// optional
	ID              string
	Timeout         time.Duration
	TimeoutCallback RMQTimeoutCallback
}

// RMQDeliveryCallback - RMQ delivery callback function
type RMQDeliveryCallback func(w *RMQWorker, rmqDelivery amqp.Delivery)

// RMQTimeoutCallback - RMQ response timeout callback function
type RMQTimeoutCallback func(w *RMQWorker)

type rmqWorkerData struct {
	Name                string // worker name
	QueueName           string
	AutoAck             bool
	CheckResponseErrors bool

	// if only one response is expected,
	// then a timeout can be applied
	UseResponseTimeout  bool
	WaitResponseTimeout time.Duration

	// optional params
	ID string
}

type rmqWorkerConnections struct {
	RMQConn    *amqp.Connection
	RMQChannel *amqp.Channel //channel for worker
}

type rmqWorkerChannels struct {
	RMQMessages <-chan amqp.Delivery
	OnFinished  chan struct{}
	StopCh      chan struct{}
}
