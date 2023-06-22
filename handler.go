package rmqworker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/matrixbotio/constants-lib"
	darkmq "github.com/sagleft/darkrmq"

	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
	"github.com/matrixbotio/rmqworker-lib/pkg/structs"
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
func NewRMQHandler(task CreateRMQHandlerTask) (*RMQHandler, errs.APIError) {
	// create handler
	r := RMQHandler{
		task: task,
	}

	if err := r.rmqInit(); err != nil {
		return nil, constants.Error("BASE_INTERNAL_ERROR", err.Error()) // TBD: return error
	}

	return &r, nil
}

func (r *RMQHandler) rmqInit() error {
	// init rmq connector
	r.conn = darkmq.NewConnector(darkmq.Config{
		Wait: waitBetweenReconnect,
	})

	// init channel pools
	r.connPool = darkmq.NewPool(r.conn)
	r.connPoolLightning = darkmq.NewLightningPool(r.conn)

	if err := r.rmqConnect(); err != nil {
		return fmt.Errorf("rmq init handler connect: %w", err)
	}

	// init publishers
	var err error
	r.publisher, err = darkmq.NewConstantPublisher(r.connPoolLightning)
	if err != nil {
		return fmt.Errorf("rmq init handler publisher: %w", err)
	}

	return nil
}

func (r *RMQHandler) rmqConnect() error {
	var dsn = getRMQConnectionURL(r.task.Data)
	var err = make(chan error)

	r.conn.AddDialedListener(func(_ darkmq.Dialed) {
		err <- nil
	})

	go func() {
		if connErr := r.conn.Dial(context.Background(), dsn); connErr != nil {
			err <- fmt.Errorf("rmq handler connect dial: %w", connErr)
		} else {
			err <- nil
		}
	}()

	select {
	case e := <-err:
		return e
	case <-time.After(handlerFirstConnTimeout):
		return errors.New("rmq handler connect dial: timeout")
	}
}

// NewRMQHandler - clone handler & open new RMQ channel
func (r *RMQHandler) NewRMQHandler() *RMQHandler {
	handlerRoot := *r
	newHandler := handlerRoot
	newHandler.locks = rmqHandlerLocks{}
	return &newHandler
}

// IsConnectionAlive - check connection
func (r *RMQHandler) IsConnectionAlive() bool {
	return r.conn.IsConnectionAlive()
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

// NewRequestHandler - create new handler for one-time request
func (h *RMQHandler) NewRequestHandler(task RequestHandlerTask) (*RequestHandler, errs.APIError) {
	r := &RequestHandler{
		RMQH:     h,
		Task:     task,
		IsPaused: true,
	}
	r.remakeFinishedChannel()

	// create RMQ-M worker
	var err errs.APIError
	r.Worker, err = r.RMQH.NewRMQWorker(WorkerTask{
		QueueName:          r.Task.TempQueueName,
		RoutingKey:         r.Task.TempQueueName,
		ISQueueDurable:     r.Task.ForceQueueToDurable,
		ISAutoDelete:       false,
		Callback:           r.handleMessage,
		ID:                 r.WorkerID,
		FromExchange:       r.Task.ResponseFromExchangeName,
		ConsumersCount:     1,
		WorkerName:         r.Task.WorkerName,
		MessagesLifetime:   requestHandlerDefaultMessageLifetime,
		UseErrorCallback:   true,
		ErrorCallback:      r.onError,
		Timeout:            task.Timeout,
		TimeoutCallback:    r.handleTimeout,
		DoNotStopOnTimeout: true,
	})
	if err != nil {
		return nil, err
	}

	// run worker
	err = r.Worker.Serve()
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *RequestHandler) remakeFinishedChannel() {
	r.Finished = make(chan struct{}, 1)
}

// SetID for worker
func (r *RequestHandler) SetID(id string) *RequestHandler {
	r.WorkerID = id
	return r
}

func (r *RequestHandler) sendRequest(messageBody interface{}, responseRoutingKey string) errs.APIError {
	if r.Task.ExchangeInsteadOfQueue {
		return r.RMQH.RMQPublishToExchange(
			messageBody,               // request message
			r.Task.RequestToQueueName, // exchange name
			"",                        // routing key
			responseRoutingKey,        // response routing key
		)
	}

	return r.RMQH.RMQPublishToQueue(structs.RMQPublishRequestTask{
		QueueName:          r.Task.RequestToQueueName,
		ResponseRoutingKey: responseRoutingKey,
		MessageBody:        messageBody,
	})
}

func (r *RequestHandler) reset() {
	r.LastError = nil
	r.Worker.Reset()
}

func (r *RequestHandler) waitResponse() {
	<-r.Finished
}

func (r *RequestHandler) pause() {
	r.IsPaused = true
}

func (r *RequestHandler) resume() {
	r.IsPaused = false
}

// Send request (sync)
func (r *RequestHandler) Send(messageBody interface{}, responseRoutingKey string) (*RequestHandlerResponse, errs.APIError) {
	// init
	r.resume()
	if r.Task.AttemptsNumber == 0 {
		// value is not set
		r.Task.AttemptsNumber = 1
	}

	for i := 1; i <= r.Task.AttemptsNumber; i++ {
		// reset worker
		r.remakeFinishedChannel()
		r.reset()

		// send request
		err := r.sendRequest(messageBody, responseRoutingKey)
		if err != nil {
			return nil, err
		}

		// await response
		r.waitResponse()

		if r.LastError == nil {
			break
		}
	}
	r.pause()

	// return result
	return r.Response, r.LastError
}

func (r *RequestHandler) handleTimeout(_ *RMQWorker) {
	var message string
	if r.Task.MethodFriendlyName == "" {
		message = "send RMQ request timeout"
		if r.Task.WorkerName != "" {
			message += " for " + r.Task.WorkerName + " worker"
		}
	} else {
		message = r.Task.MethodFriendlyName + " timeout"
	}

	r.LastError = constants.Error(
		"SERVICE_REQ_TIMEOUT",
		message,
	)
	r.markFinished()
}

func (r *RequestHandler) markFinished() {
	if len(r.Finished) == 0 {
		r.Finished <- struct{}{}
	}
}

func (r *RequestHandler) DeleteQueues() errs.APIError {
	return r.RMQH.DeleteQueues(map[string][]string{
		"reqHandler": {r.Task.TempQueueName},
	})
}

// request callback
func (r *RequestHandler) handleMessage(_ *RMQWorker, deliveryHandler RMQDeliveryHandler) {
	defer r.markFinished()

	if r.IsPaused {
		return
	}

	r.Response = &RequestHandlerResponse{
		ResponseBody: deliveryHandler.GetMessageBody(),
	}
}

func (r *RequestHandler) onError(_ *RMQWorker, err *constants.APIError) {
	r.LastError = err
	r.markFinished()
}

// RequestHandlerResponse - raw RMQ response data
type RequestHandlerResponse struct {
	ResponseBody []byte
}

// Decode response from JSON. pass pointer to struct or map in `destination`
func (s *RequestHandlerResponse) Decode(destination interface{}) errs.APIError {
	err := json.Unmarshal(s.ResponseBody, destination)
	if err != nil {
		return constants.Error(
			"DATA_PARSE_ERR",
			"failed to decode rmq response as json: "+err.Error(),
		)
	}
	return nil
}

// Stop handler, cancel consumer
func (r *RequestHandler) Stop() {
	r.markFinished()
	r.Worker.Stop()
}
