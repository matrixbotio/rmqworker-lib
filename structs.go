package rmqworker

import (
	"sync"
	"time"

	"github.com/beefsack/go-rate"
	"github.com/matrixbotio/constants-lib"
	darkmq "github.com/sagleft/darkrmq"
	simplecron "github.com/sagleft/simple-cron"
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

// consumer implement darkmq.Consumer interface
type consumer struct {
	Tag string

	QueueData DeclareQueueTask
	Binding   exchandeBindData

	msgHandler    func(delivery RMQDeliveryHandler)
	errorCallback func(err *constants.APIError)
}

type exchandeBindData struct {
	// required
	ExchangeName string
	RoutingKey   string

	// optional
	ExchangeType string // direct (by default), topic, etc
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
	rateLimiter      *rate.RateLimiter
	logs             LogCallbacks

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
	// required
	QueueName   string
	MessageBody interface{}

	// optional
	ResponseRoutingKey string
	CorrelationID      string
	CSTX               CrossServiceTransaction
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
	ID                         string             // worker ID
	FromExchange               string             // exchange name to bind queue
	ExchangeType               string             // direct, topic, etc
	ConsumersCount             int                // default: 1
	WorkerName                 string             // worker name. default name when empty
	EnableRateLimiter          bool               // limit handle rmq messages rate
	MaxEventsPerSecond         int                // for limiter
	QueueLength                int64              // how many maximum messages to keep in the queue
	MessagesLifetime           int64              // milliseconds. 0 to disable limit
	DisableOverflow            bool               // disable queue overflow
	DisableCheckResponseErrors bool               // if it is necessary to handle an error at the service level, not at the library level
	UseErrorCallback           bool               // handle worker errors with error handler
	ErrorCallback              RMQErrorCallback   // error handler callback
	Timeout                    time.Duration      // timeout to limit worker time
	TimeoutCallback            RMQTimeoutCallback // timeout callback
	DoNotStopOnTimeout         bool
	Logs                       LogCallbacks
}

type LogCallbacks struct {
	UseLogs    bool
	LogVerbose func(info string)
	LogError   func(errInfo string)
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
	DoNotStopOnTimeout  bool

	// optional params
	ID         string // worker ID for logs
	ConsumerId string // empty -> random ID
}

type rmqWorkerChannels struct {
	RMQMessages <-chan amqp.Delivery
	OnFinished  chan struct{}
	StopCh      chan struct{}
}

type CreateRMQHandlerTask struct {
	// required
	Data                    RMQConnectionData
	UseErrorCallback        bool
	ConnectionErrorCallback func(err APIError)

	// optional
	Logs LogCallbacks
}

// RMQHandler - RMQ connection handler
type RMQHandler struct {
	task  CreateRMQHandlerTask
	conn  *darkmq.Connector
	locks rmqHandlerLocks

	connPool          *darkmq.Pool
	publisher         *darkmq.ConstantPublisher
	connPoolLightning *darkmq.LightningPool
}

type rmqHandlerLocks struct {
	rwLock sync.RWMutex
}

// RequestHandler - periodic request handler
type RequestHandler struct {
	RMQH   *RMQHandler
	Task   RequestHandlerTask
	Worker *RMQWorker

	WorkerID  string
	Response  *RequestHandlerResponse
	LastError *constants.APIError

	Finished chan struct{}
	IsPaused bool
}

// RequestHandlerTask data
type RequestHandlerTask struct {
	// required
	ResponseFromExchangeName string
	RequestToQueueName       string
	TempQueueName            string
	AttemptsNumber           int
	Timeout                  time.Duration

	// optional
	ExchangeInsteadOfQueue bool
	WorkerName             string
	ForceQueueToDurable    bool
	MethodFriendlyName     string // the name of the operation performed by the vorker for the logs and errors
	Logs                   LogCallbacks
}

type PublishToExchangeTask struct {
	// required
	Message      interface{}
	ExchangeName string

	// optional
	RoutingKey         string
	ResponseRoutingKey string
	CorrelationID      string

	cstx CrossServiceTransaction
}

type CrossServiceTransaction struct {
	ID        string
	AckNum    int
	StartedAt int64
	Timeout   int

	handler *RMQHandler
}

type CSTXAck struct {
	TXID    string
	Type    string // ack or nack
	Time    int64
	Timeout int
}
