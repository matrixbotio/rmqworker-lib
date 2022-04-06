package rmqworker

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/matrixbotio/constants-lib"
	simplecron "github.com/sagleft/simple-cron"
	"github.com/streadway/amqp"
)

/*
   ,    ,    /\   /\
  /( /\ )\  _\ \_/ /_
  |\_||_/| < \_   _/ >      ____  __  __  ___    _   _                 _ _
  \______/  \|0   0|/      |  _ \|  \/  |/ _ \  | | | | __ _ _ __   __| | | ___ _ __
    _\/_   _(_  ^  _)_     | |_) | |\/| | | | | | |_| |/ _` | '_ \ / _` | |/ _ \ '__|
   ( () ) /`\|V"""V|/`\    |  _ <| |  | | |_| | |  _  | (_| | | | | (_| | |  __/ |
     {}   \  \_____/  /    |_| \_\_|  |_|\__\_\ |_| |_|\__,_|_| |_|\__,_|_|\___|_|
     ()   /\   )=(   /\
jgs  {}  /  \_/\=/\_/  \
*/

// RMQHandler - RMQ connection handler
type RMQHandler struct {
	Connections handlerConnections
	Logger      *constants.Logger
	Cron        *simplecron.CronObject
}

type handlerConnections struct {
	Data RMQConnectionData

	Publish connectionPair // connection & channel for publish messages
	Consume connectionPair // connection & channel for consume messages
}

type connectionPair struct {
	mutex    sync.Mutex
	rwMutex  sync.RWMutex
	Conn     *amqp.Connection
	Channel  *amqp.Channel
	consumes map[string]consumeFunc
}

// NewRMQHandler - create new RMQHandler
func NewRMQHandler(connData RMQConnectionData, logger ...*constants.Logger) (*RMQHandler, APIError) {
	// create handler
	r := RMQHandler{
		Connections: handlerConnections{
			Data: connData,
		},
	}

	// assign logger
	if len(logger) > 0 {
		if logger[0] != nil {
			r.Logger = logger[0]
		}
	}

	err := r.openConnectionsAndChannels()
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (r *RMQHandler) checkConnection() APIError {
	if r.Connections.Publish.Conn == nil || r.Connections.Publish.Conn.IsClosed() {
		// open new connection
		err := r.openConnectionsAndChannels()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RMQHandler) openConnectionsAndChannels() APIError {
	var err APIError
	err = openConnectionNChannel(openConnectionNChannelTask{
		connectionPair: &r.Connections.Publish,
		connData:       r.Connections.Data,
		logger:         r.Logger,
		consume:        nil,
	})
	if err != nil {
		return err
	}

	return openConnectionNChannel(openConnectionNChannelTask{
		connectionPair: &r.Connections.Consume,
		connData:       r.Connections.Data,
		logger:         r.Logger,
		consume:        nil,
	})
}

// NewRMQHandler - clone handler & open new RMQ channel
func (r *RMQHandler) NewRMQHandler() (*RMQHandler, APIError) {
	handlerRoot := *r
	newHandler := handlerRoot
	return &newHandler, r.openConnectionsAndChannels()
}

// Close channels
func (r *RMQHandler) Close() {
	err := r.Connections.Consume.Channel.Close()
	if err != nil {
		r.Logger.Error(constants.Error(baseInternalError,
			"Error when closing consumer channel",
			err.Error()))
		err = nil
	}
	err = r.Connections.Publish.Channel.Close()
	if err != nil {
		r.Logger.Error(constants.Error(baseInternalError,
			"Error when closing publisher channel",
			err.Error()))
	}
}

/*
                \||/
                |  @___oo
      /\  /\   / (__,,,,|
     ) /^\) ^\/ _)                                       _     _                     _ _
     )   /^\/   _)        _ __ ___  __ _ _   _  ___  ___| |_  | |__   __ _ _ __   __| | | ___ _ __
     )   _ /  / _)       | '__/ _ \/ _` | | | |/ _ \/ __| __| | '_ \ / _` | '_ \ / _` | |/ _ \ '__|
 /\  )/\/ ||  | )_)      | | |  __/ (_| | |_| |  __/\__ \ |_  | | | | (_| | | | | (_| | |  __/ |
<  >      |(,,) )__)     |_|  \___|\__, |\__,_|\___||___/\__| |_| |_|\__,_|_| |_|\__,_|_|\___|_|
 ||      /    \)___)\                 |_|
 | \____(      )___) )___
  \______(_______;;; __;;;

*/

// RequestHandler used for one-time requests.
// using RMQ-M worker
type RequestHandler struct {
	RMQH *RMQHandler
	Task RequestHandlerTask

	WorkerID  string
	Response  *RequestHandlerResponse
	LastError *constants.APIError
}

// RequestHandlerTask data
type RequestHandlerTask struct {
	// required
	ResponseFromExchangeName string
	RequestToQueueName       string
	TempQueueName            string
	AttemptsNumber           int
	MessageBody              interface{}

	// optional
	ExchangeInsteadOfQueue bool
	WorkerName             string
	ResponseTimeout        time.Duration
	ForceQueueToDurable    bool
}

// NewRequestHandler - create new handler for one-time request
func (r *RMQHandler) NewRequestHandler(task RequestHandlerTask) (*RequestHandler, APIError) {
	return &RequestHandler{
		RMQH: r,
		Task: task,
	}, nil
}

// Close channels
func (r *RequestHandler) Close() {
	r.RMQH.Close()
}

// SetID for worker
func (r *RequestHandler) SetID(id string) *RequestHandler {
	r.WorkerID = id
	return r
}

func (r *RequestHandler) resetLastError() *RequestHandler {
	r.LastError = nil
	return r
}

func (r *RequestHandler) sendRequest() APIError {
	if r.Task.ExchangeInsteadOfQueue {
		return r.RMQH.RMQPublishToExchange(
			r.Task.MessageBody,        // request message
			r.Task.RequestToQueueName, // exchange name
			"",                        // routing key
			r.Task.TempQueueName,      // response routing key
		)
	}
	return r.RMQH.RMQPublishToQueue(RMQPublishRequestTask{
		QueueName:          r.Task.RequestToQueueName,
		ResponseRoutingKey: r.Task.TempQueueName,
		MessageBody:        r.Task.MessageBody,
	})
}

// Send request (sync)
func (r *RequestHandler) Send() (*RequestHandlerResponse, APIError) {
	r.resetLastError()
	// create RMQ-M worker
	w, err := r.RMQH.NewRMQMonitoringWorker(RMQMonitoringWorkerTask{
		QueueName:        r.Task.TempQueueName,
		ISQueueDurable:   r.Task.ForceQueueToDurable,
		ISAutoDelete:     false,
		FromExchangeName: r.Task.ResponseFromExchangeName,
		RoutingKey:       r.Task.TempQueueName,
		Callback:         r.handleMessage,
		ID:               r.WorkerID,
		Timeout:          r.Task.ResponseTimeout,
		TimeoutCallback:  r.handleTimeout,
		ErrorCallback:    r.onError,
	})
	if err != nil {
		return nil, err
	}

	if r.Task.AttemptsNumber == 0 {
		// value is not set
		r.Task.AttemptsNumber = 1
	}
	for i := 1; i <= r.Task.AttemptsNumber; i++ {
		if i > 1 {
			w.Reset()
		}
		// send request
		err := r.sendRequest()
		if err != nil {
			return nil, err
		}

		// await response
		w.AwaitFinish()

		if r.LastError == nil {
			break
		}
	}

	// delete temp queue
	err = r.RMQH.DeleteQueues(map[string][]string{
		"reqHandler": {r.Task.TempQueueName},
	})
	if err != nil {
		return nil, err
	}

	// return result
	return r.Response, r.LastError
}

// request callback
func (r *RequestHandler) handleMessage(w *RMQWorker, deliveryHandler RMQDeliveryHandler) {
	r.Response = &RequestHandlerResponse{
		ResponseBody: deliveryHandler.GetMessageBody(),
	}
	w.Stop()
}

func (r *RequestHandler) onError(w *RMQWorker, err *constants.APIError) {
	r.LastError = err
	w.Stop()
}

func (r *RequestHandler) handleTimeout(_ *RMQWorker) {
	message := "send RMQ request timeout"
	if r.Task.WorkerName != "" {
		message += " for " + r.Task.WorkerName + " worker"
	}

	r.LastError = constants.Error(
		"SERVICE_REQ_TIMEOUT",
		message,
	)
}

// RequestHandlerResponse - raw RMQ response data
type RequestHandlerResponse struct {
	ResponseBody []byte
}

// Decode response from JSON. pass pointer to struct or map in `destination`
func (s *RequestHandlerResponse) Decode(destination interface{}) APIError {
	err := json.Unmarshal(s.ResponseBody, destination)
	if err != nil {
		return constants.Error(
			"DATA_PARSE_ERR",
			"failed to decode rmq response as json: "+err.Error(),
		)
	}
	return nil
}
