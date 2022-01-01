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
	mutex   sync.Mutex
	rwMutex sync.RWMutex
	Conn    *amqp.Connection
	Channel *amqp.Channel
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

func (r *RMQHandler) openConnectionsAndChannels() APIError {
	var err APIError
	err = openConnectionNChannel(&r.Connections.Publish, r.Connections.Data, r.Logger, nil)
	if err != nil {
		return err
	}

	return openConnectionNChannel(&r.Connections.Consume, r.Connections.Data, r.Logger, nil)
}

// NewRMQHandler - clone handler & open new RMQ channel
func (r *RMQHandler) NewRMQHandler() (*RMQHandler, APIError) {
	handlerRoot := *r
	newHandler := handlerRoot

	// open new channel for publish
	var err APIError
	err = openConnectionNChannel(&newHandler.Connections.Publish, r.Connections.Data, r.Logger, nil)
	if err != nil {
		return nil, err
	}

	// & consume messages
	err = openConnectionNChannel(&newHandler.Connections.Consume, r.Connections.Data, r.Logger, nil)
	if err != nil {
		return nil, err
	}

	return &newHandler, nil
}

// Close channels
func (r *RMQHandler) Close() {
	r.Connections.Consume.Channel.Close()
	r.Connections.Publish.Channel.Close()
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
	ResponseFromExchangeName string
	RequestToQueueName       string
	TempQueueName            string
	AttemptsNumber           int

	WorkerName      string
	ResponseTimeout time.Duration
	MessageBody     interface{}
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

// Send request (sync)
func (r *RequestHandler) Send() (*RequestHandlerResponse, APIError) {
	r.resetLastError()
	// create RMQ-M worker
	w, err := r.RMQH.NewRMQMonitoringWorker(RMQMonitoringWorkerTask{
		QueueName:        r.Task.TempQueueName,
		ISQueueDurable:   false,
		ISAutoDelete:     true,
		FromExchangeName: r.Task.ResponseFromExchangeName,
		RoutingKey:       r.Task.TempQueueName,
		Callback:         r.handleMessage,
		ReuseChannels:    true,
		ID:               r.WorkerID,
		Timeout:          r.Task.ResponseTimeout,
		TimeoutCallback:  r.handleTimeout,
	})
	if err != nil {
		return nil, err
	}

	if r.Task.AttemptsNumber == 0 {
		// value is not set
		r.Task.AttemptsNumber = 1
	}
	for i := 1; i <= r.Task.AttemptsNumber; i++ {
		// send request
		err := r.RMQH.RMQPublishToQueue(RMQPublishRequestTask{
			QueueName:          r.Task.RequestToQueueName,
			ResponseRoutingKey: r.Task.TempQueueName,
			MessageBody:        r.Task.MessageBody,
		})
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

func (r *RequestHandler) handleTimeout(w *RMQWorker) {
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
