package rmqworker

import (
	"context"
	"log"
	"time"

	"github.com/beefsack/go-rate"
	"github.com/matrixbotio/constants-lib"
	simplecron "github.com/sagleft/simple-cron"
	"github.com/streadway/amqp"
)

/*
             __.-/|
             \`o_O'  +---------------------------+
              =( )=  |        RMQ Worker         |
                U|   | I accept msgs from queues |
      /\  /\   / |   +---------------------------+
     ) /^\) ^\/ _)\     |
     )   /^\/   _) \    |
     )   _ /  / _)  \___|_
 /\  )/\/ ||  | )_)\___,|))
<  >      |(,,) )__)    |
 ||      /    \)___)\
 | \____(      )___) )____
  \______(_______;;;)__;;;)

*/

// NewRMQWorker - create new RMQ worker to receive messages
func (r *RMQHandler) NewRMQWorker(task WorkerTask) (*RMQWorker, APIError) {
	// set worker name
	if task.WorkerName == "" {
		task.WorkerName = "RMQ-W"
	}
	if task.ConsumersCount == 0 {
		task.ConsumersCount = 1
	}

	// create worker
	w := RMQWorker{
		data: rmqWorkerData{
			Name:                task.WorkerName,
			CheckResponseErrors: true,
			UseResponseTimeout:  task.Timeout > 0,
			WaitResponseTimeout: task.Timeout,
			ID:                  task.ID,
		},
		conn: r.conn,
		rmqConsumer: &consumer{ // consumer
			QueueData: DeclareQueueTask{
				Name:             task.QueueName,
				Durable:          task.ISQueueDurable,
				AutoDelete:       task.ISAutoDelete,
				MessagesLifetime: task.MessagesLifetime,
				MaxLength:        task.QueueLength,
				DisableOverflow:  task.DisableOverflow,
			},
			Binding: exchandeBindData{
				ExchangeName: task.FromExchange,
				RoutingKey:   task.RoutingKey,
			},
		},
		channels: rmqWorkerChannels{
			RMQMessages: make(<-chan amqp.Delivery),
			OnFinished:  make(chan struct{}, 1),
			StopCh:      make(chan struct{}, 1),
		},
		consumersCount:   task.ConsumersCount,
		deliveryCallback: task.Callback,
		timeoutCallback:  task.TimeoutCallback,
		logger:           r.task.Logger,
	}

	// setup error handler
	w.rmqConsumer.errorCallback = w.handleError
	if task.UseErrorCallback {
		w.useErrorCallback = true
		w.errorCallback = task.ErrorCallback
	}

	// setup messages handler
	w.rmqConsumer.msgHandler = w.handleRMQMessage

	w.remakeStopChannel()

	if task.EnableRateLimiter && task.MaxEventsPerSecond > 0 {
		w.rateLimiter = rate.New(task.MaxEventsPerSecond, time.Second)
	}

	return &w, nil
}

func (w *RMQWorker) logWarn(err *constants.APIError) {
	err.Message = w.getLogWorkerName() + err.Message
	if w.logger != nil {
		w.logger.Warn(err)
	} else {
		log.Println(err.Message)
	}
}

func (w *RMQWorker) logVerbose(message string) {
	if w.logger != nil {
		w.logger.Verbose(w.getLogWorkerName() + message)
	} else {
		log.Println(w.getLogWorkerName() + message)
	}
}

func (w *RMQWorker) logError(err *constants.APIError) {
	if w.logger != nil {
		err.Message = w.getLogWorkerName() + err.Message
		w.logger.Error(err)
	} else {
		log.Println(err.Message)
	}
}

// IsConnAlive - check that the connection is established
/*func (w *RMQWorker) IsConnAlive() bool {
	if w.connections == nil || w.connections.Consume.Conn == nil {
		return false
	}

	return !w.connections.Consume.Conn.IsClosed()
}*/

// SetName - set RMQ worker name for logs
func (w *RMQWorker) SetName(name string) *RMQWorker {
	w.data.Name = name
	return w
}

// GetName - get worker name
func (w *RMQWorker) GetName() string {
	return w.data.Name
}

// SetID - set RMQ worker ID
func (w *RMQWorker) SetID(id string) *RMQWorker {
	w.data.ID = id
	return w
}

// GetID - get worker ID
func (w *RMQWorker) GetID() string {
	return w.data.ID
}

// IsActive - return worker paused state
/*func (w *RMQWorker) IsActive() bool {
	return w.awaitMessages
}*/

// SetCheckResponseErrors - determines whether the errors in the answers passed to headers will be checked
func (w *RMQWorker) SetCheckResponseErrors(check bool) *RMQWorker {
	w.data.CheckResponseErrors = check
	return w
}

func (w *RMQWorker) getLogWorkerName() string {
	return "RMQ Worker " + w.data.Name + ": "
}

// Serve - start consumer(s)
func (w *RMQWorker) Serve() {
	err := w.conn.StartMultipleConsumers(context.Background(), w.rmqConsumer, w.consumersCount, w.stopCh)
	if err != nil {
		w.handleError(constants.Error(
			"SERVICE_REQ_FAILED", "failed to start consumer(s): "+err.Error(),
		))
	}
}

// Stop RMQ messages listen
func (w *RMQWorker) Stop() {
	w.stopCh <- struct{}{}
}

// Reset worker channels
func (w *RMQWorker) Reset() {
	w.channels.OnFinished = make(chan struct{}, 1)
	w.channels.StopCh = make(chan struct{}, 1)

	w.remakeStopChannel()
}

func (w *RMQWorker) remakeStopChannel() {
	w.stopCh = make(chan struct{}, 1)
}

func (w *RMQWorker) runCron() {
	w.cronHandler = simplecron.NewCronHandler(w.timeIsUp, w.data.WaitResponseTimeout)
	go w.cronHandler.Run()
}

// SetConsumerTag - set worker unique consumer tag
func (w *RMQWorker) SetConsumerTag(uniqueTag string) *RMQWorker {
	w.rmqConsumer.Tag = uniqueTag
	return w
}

// SetConsumerTagFromName - assign a consumer tag to the worker based on its name and random ID
func (w *RMQWorker) SetConsumerTagFromName() *RMQWorker {
	tag := w.data.Name + "-" + getUUID()
	if w.data.Name == "" {
		tag = "worker" + w.rmqConsumer.Tag
	}
	return w.SetConsumerTag(tag)
}

func (w *RMQWorker) stopCron() {
	if w.cronHandler != nil {
		w.cronHandler.Stop()
	}
}

func (w *RMQWorker) limitHandleRate() {
	if w.rateLimiter != nil {
		w.rateLimiter.Wait()
	}
}

func (w *RMQWorker) handleError(err *constants.APIError) {
	if !w.useErrorCallback {
		w.logError(err)
		return
	}

	w.errorCallback(w, err)
}

func (w *RMQWorker) handleRMQMessage(delivery RMQDeliveryHandler) {
	w.logVerbose("new rmq message found")

	// check response error
	if w.data.CheckResponseErrors {
		w.logVerbose("check message errors")
		aErr := delivery.CheckResponseError()
		if aErr != nil {
			w.handleError(aErr)
			return
		}
	}

	// callback
	w.logVerbose("run callback..")
	if w.deliveryCallback == nil {
		w.handleError(constants.Error("DATA_HANDLE_ERR", "rmq worker message callback is nil"))
		return
	}

	// run callback
	w.deliveryCallback(w, delivery)
}

func (w *RMQWorker) timeIsUp() {
	w.logVerbose("worker cron: response time is up")
	w.cronHandler.Stop()
	w.timeoutCallback(w)
	w.Stop()
}

// AwaitFinish - wait for worker finished
func (w *RMQWorker) AwaitFinish() {
	<-w.channels.OnFinished
}