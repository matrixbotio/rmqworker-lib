package rmqworker

import (
	"time"

	"github.com/beefsack/go-rate"
	"github.com/matrixbotio/constants-lib"
	darkmq "github.com/sagleft/darkrmq"
	simplecron "github.com/sagleft/simple-cron"
	"github.com/streadway/amqp"
)

// APIError - error data container
type APIError *constants.APIError

/*type openConnectionNChannelTask struct {
	connectionPair     *connectionPair
	connData           RMQConnectionData
	logger             *constants.Logger
	consume            consumeFunc
	reconsumeAll       bool
	skipChannelOpening bool

	errorData error
}*/

// RMQConnectionData - rmq connection data container
type RMQConnectionData struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	UseTLS   string `json:"tls"`
}

// consumer implement darkmq.Consumer interface
type consumer struct {
	Tag string

	QueueData DeclareQueueTask
	Binding   exchandeBindData

	msgHandler    func(delivery RMQDeliveryHandler)
	errorCallback func(err *constants.APIError)
}

type exchandeBindData struct {
	ExchangeName string
	RoutingKey   string
}

// RMQWorker - just RMQ worker
type RMQWorker struct {
	data           rmqWorkerData
	conn           *darkmq.Connector
	rmqConsumer    *consumer
	channels       rmqWorkerChannels
	consumersCount int

	deliveryCallback RMQDeliveryCallback
	useErrorCallback bool
	errorCallback    RMQErrorCallback
	timeoutCallback  RMQTimeoutCallback
	cronHandler      *simplecron.CronObject

	logger      *constants.Logger
	rateLimiter *rate.RateLimiter

	stopCh chan struct{}
}

// DeclareQueueTask - queue declare task data container
type DeclareQueueTask struct {
	Name       string
	Durable    bool
	AutoDelete bool

	// optional
	MessagesLifetime int64
	MaxLength        int64
	DisableOverflow  bool
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
	DisableOverflow  bool
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
	// required
	QueueName      string
	RoutingKey     string
	ISQueueDurable bool
	ISAutoDelete   bool
	Callback       RMQDeliveryCallback // callback to handle RMQ delivery

	// optional
	ID                 string             // worker ID
	FromExchange       string             // exchange name to bind queue
	ConsumersCount     int                // default: 1
	WorkerName         string             // worker name. default name when empty
	EnableRateLimiter  bool               // limit handle rmq messages rate
	MaxEventsPerSecond int                // for limiter
	QueueLength        int64              // how many maximum messages to keep in the queue
	MessagesLifetime   int64              // milliseconds. 0 to disable limit
	DisableOverflow    bool               // disable queue overflow
	UseErrorCallback   bool               // handle worker errors with error handler
	ErrorCallback      RMQErrorCallback   // error handler callback
	Timeout            time.Duration      // timeout to limit worker time
	TimeoutCallback    RMQTimeoutCallback // timeout callback
}

// RMQDeliveryCallback - RMQ delivery callback function
type RMQDeliveryCallback func(w *RMQWorker, deliveryHandler RMQDeliveryHandler)

// RMQErrorCallback - RMQ error callback function
type RMQErrorCallback func(w *RMQWorker, err *constants.APIError)

// RMQTimeoutCallback - RMQ response timeout callback function
type RMQTimeoutCallback func(w *RMQWorker)

type rmqWorkerData struct {
	Name                string // worker name
	CheckResponseErrors bool   // whether to check the error code in the messages

	// if only one response is expected,
	// then a timeout can be applied
	UseResponseTimeout  bool
	WaitResponseTimeout time.Duration

	// optional params
	ID         string // worker ID for logs
	ConsumerId string // empty -> random ID
}

type rmqWorkerChannels struct {
	RMQMessages <-chan amqp.Delivery
	OnFinished  chan struct{}
	StopCh      chan struct{}

	msgChanOpened bool
}

type CreateRMQHandlerTask struct {
	Data                    RMQConnectionData
	UseErrorCallback        bool
	ConnectionErrorCallback func(err APIError)
	Logger                  *constants.Logger
}

// RMQHandler - RMQ connection handler
type RMQHandler struct {
	task CreateRMQHandlerTask
	conn *darkmq.Connector

	connPool        *darkmq.Pool
	ensurePublisher *darkmq.EnsurePublisher // the publisher of the messages, who verifies that they were received

	connPoolLightning *darkmq.LightningPool
	firePublisher     *darkmq.FireForgetPublisher // the publisher of the messages, who does not care if the messages are received

	channelKeeper darkmq.ChannelKeeper // rmq channel handler. for different requests
}
