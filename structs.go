package rmqworker

import (
	"time"

	"github.com/matrixbotio/constants-lib"
	simplecron "github.com/sagleft/simple-cron"
	"github.com/streadway/amqp"
)

// APIError - error data container
type APIError *constants.APIError

type openConnectionNChannelTask struct {
	connectionPair     *connectionPair
	connData           RMQConnectionData
	logger             *constants.Logger
	consume            consumeFunc
	skipChannelOpening bool

	errorData error
}

// RMQConnectionData - rmq connection data container
type RMQConnectionData struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	UseTLS   string `json:"tls"`
}

// RMQWorker - just RMQ worker
type RMQWorker struct {
	data          rmqWorkerData
	connections   *handlerConnections
	channels      rmqWorkerChannels
	paused        bool
	awaitMessages bool

	deliveryCallback RMQDeliveryCallback
	errorCallback    RMQErrorCallback
	timeoutCallback  RMQTimeoutCallback
	cronHandler      *simplecron.CronObject

	logger *constants.Logger
}

// RMQMonitoringWorker - rmq extended worker
type RMQMonitoringWorker struct {
	Worker *RMQWorker
}

// RMQQueueDeclareSimpleTask - queue declare task data container
type RMQQueueDeclareSimpleTask struct {
	QueueName  string
	Durable    bool
	AutoDelete bool

	// optional
	MessagesLifetime int64
	QueueLength      int64
}

// RMQQueueDeclareTask - queue declare task data container
type RMQQueueDeclareTask struct {
	QueueName        string
	Durable          bool
	AutoDelete       bool
	FromExchangeName string
	RoutingKey       string

	// optional
	MessagesLifetime int64
	QueueLength      int64
}

// RMQExchangeDeclareTask - exchange declare task data container
type RMQExchangeDeclareTask struct {
	ExchangeName     string
	ExchangeType     string
	MessagesLifetime int64
}

// RMQPublishRequestTask - publish message to RMQ task data container
type RMQPublishRequestTask struct {
	QueueName          string
	ResponseRoutingKey string
	MessageBody        interface{}
}

// RMQPublishResponseTask - response for publish message to RMQ request
type RMQPublishResponseTask struct {
	ExchangeName       string
	ResponseRoutingKey string
	CorrelationID      string
	MessageBody        interface{}
}

// WorkerTask - new RMQ worker data
type WorkerTask struct {
	QueueName     string
	Callback      RMQDeliveryCallback
	WorkerName    string
	ReuseChannels bool
	ErrorCallback RMQErrorCallback
}

// RMQMonitoringWorkerTask - new RMQ request->response monitoring worker data
type RMQMonitoringWorkerTask struct {
	// required
	QueueName        string
	ISQueueDurable   bool
	ISAutoDelete     bool
	FromExchangeName string
	RoutingKey       string // to bind queue to response exchange
	Callback         RMQDeliveryCallback
	ReuseChannels    bool // open new channel for worker?

	// optional
	ID               string
	Timeout          time.Duration
	TimeoutCallback  RMQTimeoutCallback
	WorkerName       string
	MessagesLifetime int64            // milliseconds. 0 to disable limit
	QueueLength      int64            // how many maximum messages to keep in the queue
	ErrorCallback    RMQErrorCallback // error handler func for RMQ-Worker errors
}

// RMQDeliveryCallback - RMQ delivery callback function
type RMQDeliveryCallback func(w *RMQWorker, deliveryHandler RMQDeliveryHandler)

// RMQErrorCallback - RMQ error callback function
type RMQErrorCallback func(w *RMQWorker, err *constants.APIError)

// RMQTimeoutCallback - RMQ response timeout callback function
type RMQTimeoutCallback func(w *RMQWorker)

type rmqWorkerData struct {
	Name                string // worker name
	QueueName           string // the name of the queue from which to receive messages
	AutoAckByLib        bool   // whether or not the worker will accept the message as soon as he receives it
	CheckResponseErrors bool   // whether to check the error code in the messages

	// if only one response is expected,
	// then a timeout can be applied
	UseResponseTimeout  bool
	WaitResponseTimeout time.Duration

	// optional params
	ID          string // worker ID for logs
	ConsumerTag string // empty -> random ID
	ConsumerId  string // empty -> random ID
}

type rmqWorkerChannels struct {
	RMQMessages <-chan amqp.Delivery
	OnFinished  chan struct{}
	StopCh      chan struct{}
}

type consumeTask struct {
	consume consumeFunc

	connData       RMQConnectionData
	connectionPair *connectionPair // to recreate connection
}
