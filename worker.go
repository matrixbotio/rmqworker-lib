package rmqworker

import (
	"context"
	"errors"
	"time"

	"github.com/beefsack/go-rate"
	"github.com/google/uuid"
	"github.com/matrixbotio/constants-lib"
	darkmq "github.com/sagleft/darkrmq"
	"github.com/streadway/amqp"
	"go.uber.org/zap"

	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
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
func (r *RMQHandler) NewRMQWorker(task WorkerTask) (*RMQWorker, errs.APIError) {
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
			CheckResponseErrors: !task.DisableCheckResponseErrors,
			ID:                  task.ID,
		},
		conn: r.conn,
		rmqConsumer: &consumer{ // consumer
			Tag:       task.WorkerName + "-" + uuid.NewString(),
			ManualAck: task.ManualAck,
			QueueData: DeclareQueueTask{
				Name:               task.QueueName,
				Durable:            task.ISQueueDurable,
				AutoDelete:         task.ISAutoDelete,
				MessagesLifetime:   task.MessagesLifetime,
				MaxLength:          task.QueueLength,
				DisableOverflow:    task.DisableOverflow,
				DeadLetterExchange: task.DeadLetterExchange,
			},
			Binding: exchandeBindData{
				ExchangeName: task.FromExchange,
				ExchangeType: task.ExchangeType,
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
		Logger:           r.task.Logger,
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

func (w *RMQWorker) logVerbose(message string) {
	w.Logger.Debug(message, zap.String("worker_name", w.data.Name))
}

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

// SetCheckResponseErrors - determines whether the errors in the answers passed to headers will be checked
func (w *RMQWorker) SetCheckResponseErrors(check bool) *RMQWorker {
	w.data.CheckResponseErrors = check
	return w
}

// Serve - start consumer(s)
func (w *RMQWorker) Serve() errs.APIError {
	err := w.conn.StartMultipleConsumers(darkmq.StartConsumersTask{
		Ctx:      context.Background(),
		Consumer: w.rmqConsumer,
		Count:    w.consumersCount,
		Stop:     w.stopCh,
	})
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to start consumer(s): "+err.Error(),
		)
	}
	return nil
}

// Stop RMQ messages listen
func (w *RMQWorker) Stop() {
	if len(w.stopCh) == 0 {
		w.stopCh <- struct{}{}
	}

	w.Finish()
	w.logVerbose("worker stopped")
}

// Finish worker but continue listen messages
func (w *RMQWorker) Finish() {
	if len(w.channels.OnFinished) == 0 {
		w.channels.OnFinished <- struct{}{}
	}
}

// Reset worker channels
func (w *RMQWorker) Reset() {
	w.channels.OnFinished = make(chan struct{}, 1)
	w.channels.StopCh = make(chan struct{}, 1)
}

func (w *RMQWorker) remakeStopChannel() {
	w.stopCh = make(chan struct{}, 1)
}

// SetConsumerTag - set worker unique consumer tag
func (w *RMQWorker) SetConsumerTag(uniqueTag string) *RMQWorker {
	w.rmqConsumer.Tag = uniqueTag
	return w
}

// SetConsumerTagFromName - assign a consumer tag to the worker based on its name and random ID
func (w *RMQWorker) SetConsumerTagFromName() *RMQWorker {
	tag := w.data.Name + "-" + uuid.NewString()
	if w.data.Name == "" {
		tag = "worker" + w.rmqConsumer.Tag
	}
	return w.SetConsumerTag(tag)
}

func (w *RMQWorker) handleError(err *constants.APIError) {
	if !w.useErrorCallback {
		w.Logger.Error("handleError", zap.Error(errors.New(err.Message)))
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

// AwaitFinish - wait for worker finished
func (w *RMQWorker) AwaitFinish() {
	<-w.channels.OnFinished
}
