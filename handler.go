package rmqworker

import (
	"context"
	"log"
	"time"

	"github.com/matrixbotio/constants-lib"
	darkmq "github.com/sagleft/darkrmq"
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

// NewRMQHandler - create new RMQHandler
func NewRMQHandler(task CreateRMQHandlerTask, logger ...*constants.Logger) (*RMQHandler, APIError) {
	// create handler
	r := RMQHandler{
		task: task,
	}
	r.rmqInit()

	return &r, nil
}

func (r *RMQHandler) rmqInit() APIError {
	// init rmq connector
	r.conn = darkmq.NewConnector(darkmq.Config{
		Wait: waitBetweenReconnect,
	})

	// init channel pools
	r.connPool = darkmq.NewPool(r.conn)
	r.connPoolLightning = darkmq.NewLightningPool(r.conn)

	// init publishers
	r.ensurePublisher = darkmq.NewEnsurePublisher(r.connPool)
	r.firePublisher = darkmq.NewFireForgetPublisher(r.connPoolLightning)

	go r.rmqConnect()

	var err error
	r.channelKeeper, err = r.connPool.ChannelWithConfirm(context.Background())
	if err != nil {
		return constants.Error("DATA_HANDLE_ERR", "failed to get channel from pool: "+err.Error())
	}
	return nil
}

func (r *RMQHandler) rmqConnect() {
	dsn := getRMQConnectionURL(r.task.Data)

	err := r.conn.Dial(context.Background(), dsn)
	if err == nil {
		return
	}

	if !r.task.UseErrorCallback {
		log.Println("[rmq handler error callback is not set] error: " + err.Error())
		return
	}

	r.task.ConnectionErrorCallback(constants.Error(
		"SERVICE_CONN_ERR", "failed to connect: "+err.Error(),
	))
}

// NewRMQHandler - clone handler & open new RMQ channel
func (r *RMQHandler) NewRMQHandler() *RMQHandler {
	handlerRoot := *r
	newHandler := handlerRoot
	newHandler.locks = rmqHandlerLocks{}
	newHandler.rmqInit()
	return &newHandler
}

func (r *RMQHandler) rlock() {
	r.locks.rwLock.RLock()
}

func (r *RMQHandler) runlock() {
	r.locks.rwLock.RUnlock()
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
	w, err := r.RMQH.NewRMQWorker(WorkerTask{
		QueueName:      r.Task.TempQueueName,
		RoutingKey:     r.Task.TempQueueName,
		ISQueueDurable: r.Task.ForceQueueToDurable,
		ISAutoDelete:   false,
		Callback:       r.handleMessage,

		ID:               r.WorkerID,
		FromExchange:     r.Task.ResponseFromExchangeName,
		ConsumersCount:   1,
		MessagesLifetime: requestHandlerDefaultMessageLifetime,
		UseErrorCallback: true,
		ErrorCallback:    r.onError,
		Timeout:          r.Task.ResponseTimeout,
		TimeoutCallback:  r.handleTimeout,
	})
	if err != nil {
		return nil, err
	}

	// run worker
	err = w.Serve()
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
