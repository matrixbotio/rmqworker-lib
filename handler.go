package rmqworker

import (
	"encoding/json"
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
	ConnectionData RMQConnectionData
	RMQConn        *amqp.Connection
	RMQChannel     *amqp.Channel
	Logger         *constants.Logger
	Cron           *simplecron.CronObject
}

// NewRMQHandler - create new RMQHandler
func NewRMQHandler(connData RMQConnectionData, logger ...*constants.Logger) (*RMQHandler, APIError) {
	// create handler
	r := RMQHandler{
		ConnectionData: connData,
	}

	// assign logger
	if len(logger) > 0 {
		if logger[0] != nil {
			r.Logger = logger[0]
		}
	}

	// open connection & channel
	err := r.recreateConnection()
	if err != nil {
		return nil, err
	}

	// run cron for check connection & channel
	r.Cron = simplecron.NewCronHandler(
		r.checkConnection,
		time.Minute*cronConnectionCheckTimeout,
	)
	go r.Cron.Run()

	return &r, nil
}

func (r *RMQHandler) recreateConnection() APIError {
	var err APIError
	r.RMQConn, r.RMQChannel, err = r.openConnectionNChannel()
	return err
}

// NewRMQHandler - clone handler & open new RMQ channel
func (r *RMQHandler) NewRMQHandler() (*RMQHandler, APIError) {
	handlerRoot := *r
	newHandler := handlerRoot

	// open new channel
	var err APIError
	newHandler.RMQChannel, err = openRMQChannel(newHandler.RMQConn)
	if err != nil {
		return nil, err
	}

	return &newHandler, nil
}

func (r *RMQHandler) checkConnection() {
	if r.RMQConn.IsClosed() {
		r.Logger.Verbose("connection is closed, open new..")
		r.recreateConnection()
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
	ResponseFromExchangeName string
	RequestToQueueName       string
	TempQueueName            string

	WorkerName      string
	ResponseTimeout time.Duration
	MessageBody     interface{}
}

// NewRequestHandler - create new handler for one-time request
func (r *RMQHandler) NewRequestHandler(task RequestHandlerTask) *RequestHandler {
	return &RequestHandler{
		RMQH: r,
		Task: task,
	}
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
		ID:               r.WorkerID,
		Timeout:          r.Task.ResponseTimeout,
		TimeoutCallback:  r.handleTimeout,
	})
	if err != nil {
		return nil, err
	}

	// send request
	err = r.RMQH.RMQPublishToQueue(RMQPublishRequestTask{
		QueueName:          r.Task.RequestToQueueName,
		ResponseRoutingKey: getUUID(),
		MessageBody:        r.Task.MessageBody,
	})
	if err != nil {
		return nil, err
	}

	// await response
	w.AwaitFinish()

	// delete temp queue
	err = r.RMQH.DeleteQueues(map[string][]string{
		"reqHandler": []string{r.Task.TempQueueName},
	})
	if err != nil {
		return nil, err
	}

	// stop connections
	w.StopConnections()

	// return result
	return r.Response, nil
}

// request callback
func (r *RequestHandler) handleMessage(w *RMQWorker, deliveryHandler RMQDeliveryHandler) {
	r.Response = &RequestHandlerResponse{
		ResponseBody: deliveryHandler.GetMessageBody(),
	}
	w.Stop()
}

func (r *RequestHandler) handleTimeout(w *RMQWorker) {
	r.LastError = constants.Error(
		"SERVICE_REQ_TIMEOUT",
		"send RMQ request timeout",
	)
	w.Stop()
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
