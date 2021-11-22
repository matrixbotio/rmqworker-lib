package rmqworker

import (
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/matrixbotio/constants-lib"
	simplecron "github.com/sagleft/simple-cron"
	"github.com/streadway/amqp"
)

/*
      ,  ,  , , ,
     <(__)> | | |
     | \/ | \_|_/  I AM RMQ Worker.
     \^  ^/   |    I hurt in a hundred ways
     /\--/\  /|
jgs /  \/  \/ |
*/

// NewRMQWorker - create new RMQ worker to receive messages
func (r *RMQHandler) NewRMQWorker(
	QueueName string,
	callback RMQDeliveryCallback,
	workerName ...string,
) (*RMQWorker, APIError) {
	if r.Connections.Publish.Conn.IsClosed() {
		// open new connection
		err := r.openConnectionsAndChannels()
		if err != nil {
			return nil, err
		}
	}

	// open channel for worker
	var wChannel *amqp.Channel
	var err APIError
	err = openConnectionNChannel(&r.Connections.Consume, r.Connections.Data, r.Logger, nil)
	if err != nil {
		return nil, err
	}

	// set worker name
	name := "rmq worker"
	if len(workerName) > 0 {
		name = workerName[0]
	}

	w := RMQWorker{
		data: rmqWorkerData{
			Name:                name,
			QueueName:           QueueName,
			AutoAckByLib:        true,
			CheckResponseErrors: true,
		},
		consumeChannel: wChannel,
		connections:    &r.Connections,
		channels: rmqWorkerChannels{
			RMQMessages: make(<-chan amqp.Delivery),
			OnFinished:  make(chan struct{}, 1),
			StopCh:      make(chan struct{}, 1),
		},
		syncMode:         true,
		deliveryCallback: callback,
		logger:           r.Logger,
		awaitMessages:    true,
	}

	return &w, nil
}

// CloseChannels - force close worker's channels
func (w *RMQWorker) CloseChannels() {
	err := w.connections.Consume.Channel.Close()
	if err != nil {
		w.logWarn(constants.Error("BASE_INTERNAL_ERROR", "Exception closing consumer channel"+err.Error()))
	}
	err = w.connections.Publish.Channel.Close()
	if err != nil {
		w.logWarn(constants.Error("BASE_INTERNAL_ERROR", "Exception closing publisher channel"+err.Error()))
	}
}

func (w *RMQWorker) logWarn(err *constants.APIError) {
	if w.logger != nil {
		err.Message = w.getLogWorkerName() + err.Message
		w.logger.Warn(err)
	}
}

func (w *RMQWorker) logVerbose(message string) {
	if w.logger != nil {
		w.logger.Verbose(w.getLogWorkerName() + message)
	} else {
		log.Println()
	}
}

func (w *RMQWorker) logError(err *constants.APIError) {
	if w.logger != nil {
		err.Message = w.getLogWorkerName() + err.Message
		w.logger.Error(err)
	} else {
		log.Println(err)
	}
}

// SetName - set RMQ worker name for logs
func (w *RMQWorker) SetName(name string) *RMQWorker {
	w.data.Name = name
	return w.SetConsumerTagFromName()
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

// SetAutoAck - auto accept messages.
// This will also change the auto-acceptance of messages by the library (!autoAck)
func (w *RMQWorker) SetAutoAck(autoAck bool) *RMQWorker {
	w.data.AutoAckByLib = autoAck
	return w
}

// SetCheckResponseErrors - determines whether the errors in the answers passed to headers will be checked
func (w *RMQWorker) SetCheckResponseErrors(check bool) *RMQWorker {
	w.data.CheckResponseErrors = check
	return w
}

// SetTimeout - set RMQ response timeout. When the timer goes out, the callback will be called
func (w *RMQWorker) SetTimeout(timeout time.Duration, callback RMQTimeoutCallback) *RMQWorker {
	w.data.UseResponseTimeout = true
	w.data.WaitResponseTimeout = timeout
	w.timeoutCallback = callback
	return w
}

func (w *RMQWorker) getLogWorkerName() string {
	return "RMQ Worker " + w.data.Name + ": "
}

// Serve - listen RMQ messages
func (w *RMQWorker) Serve() {
	w.logVerbose("subscribe..")
	err := w.Subscribe()
	if err != nil {
		w.logError(err)
	}

	w.logVerbose("listen..")
	w.Listen()
}

// Subscribe to RMQ messages
func (w *RMQWorker) Subscribe() APIError {
	var aErr APIError

	var consumeFunc = func(channel *amqp.Channel) {
		var err error
		w.channels.RMQMessages, err = channel.Consume(
			w.data.QueueName, // queue
			"",               // consumer. "" > generate random ID
			false,            // auto-ack by RMQ service
			false,            // exclusive
			false,            // no-local
			false,            // no-wait
			nil,              // args
		)
		if err != nil {
			e := constants.Error(
				"SERVICE_REQ_FAILED",
				"failed to consume rmq worker messages: "+err.Error(),
			)
			w.logError(e)
		}
	}

	// channel not created but connection is active
	// create new channel
	aErr = openConnectionNChannel(&w.connections.Consume, w.connections.Data, w.logger, consumeFunc)
	if aErr != nil {
		return aErr
	}
	return nil
}

// Stop RMQ messages listen
func (w *RMQWorker) Stop() {
	if w.cronHandler != nil {
		go w.cronHandler.Stop()
	}
	w.channels.StopCh <- struct{}{}
	w.awaitMessages = false
	w.channels.OnFinished <- struct{}{}
	w.logVerbose("worker stopped")
}

// Pause RMQ Worker (ignore messages)
func (w *RMQWorker) Pause() {
	if w.cronHandler != nil {
		w.cronHandler.Pause()
	}
	w.paused = true
}

// Resume RMQ Worker (continue listen messages)
func (w *RMQWorker) Resume() {
	if w.cronHandler != nil {
		w.cronHandler.Resume()
	}
	w.paused = false
}

// Reset worker channels
func (w *RMQWorker) Reset() {
	w.awaitMessages = true
	w.channels.OnFinished = make(chan struct{}, 1)
	w.channels.StopCh = make(chan struct{}, 1)
	if w.data.UseResponseTimeout {
		w.runCron()
	}
}

func (w *RMQWorker) runCron() {
	w.cronHandler = simplecron.NewCronHandler(w.timeIsUp, w.data.WaitResponseTimeout)
	go w.cronHandler.Run()
}

// SetConsumerTag - set worker unique consumer tag
func (w *RMQWorker) SetConsumerTag(uniqueTag string) *RMQWorker {
	w.data.ConsumerTag = uniqueTag
	return w
}

// SetConsumerTagFromName - assign a consumer tag to the worker based on its name and random ID
func (w *RMQWorker) SetConsumerTagFromName() *RMQWorker {
	tag := w.data.Name + "-" + uuid.New().String()
	if w.data.Name == "" {
		tag = "worker" + w.data.ConsumerTag
	}
	return w.SetConsumerTag(tag)
}

// Listen RMQ messages
func (w *RMQWorker) Listen() {
	w.awaitMessages = true

	if w.data.UseResponseTimeout {
		w.runCron()
	}

	for w.awaitMessages {
		for rmqDelivery := range w.channels.RMQMessages {
			if !w.awaitMessages {
				break
			}

			if w.paused {
				continue // ignore message
			}

			if w.cronHandler != nil {
				w.cronHandler.Stop()
			}
			w.handleRMQMessage(rmqDelivery)
		}
		w.logVerbose("The message cycle has ended. Params: `await messages` = " + strconv.FormatBool(w.awaitMessages))
		w.logVerbose("Sleep " + strconv.Itoa(waitingBetweenMsgSubscription) + " seconds to subscription to new messages")
		time.Sleep(waitingBetweenMsgSubscription * time.Second)
	}
}

func (w *RMQWorker) handleRMQMessage(rmqDelivery amqp.Delivery) {
	w.logVerbose("new rmq message found")
	// create delivery handler
	delivery := NewRMQDeliveryHandler(rmqDelivery)

	// auto accept message if needed
	if w.data.AutoAckByLib {
		err := delivery.Accept()
		if err != nil {
			w.logVerbose("error: " + err.Message)
			w.logError(err)
			return
		}
	}

	// check response error
	w.logVerbose("check message errors")
	if w.data.CheckResponseErrors {
		aErr := delivery.CheckResponseError()
		if aErr != nil {
			w.logVerbose("message error: " + aErr.Message)
			w.logError(aErr)
			return
		}
	}

	// callback
	w.logVerbose("run callback..")
	if w.deliveryCallback == nil {
		w.logVerbose("callback is not set")
		w.logError(constants.Error("DATA_HANDLE_ERR", "rmq worker message callback is nil"))
	}

	// run callback
	if w.syncMode {
		w.deliveryCallback(w, delivery)
	} else {
		go w.deliveryCallback(w, delivery)
	}
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

// SetSyncMode - whether to run the callback of task processing synchronously
func (w *RMQWorker) SetSyncMode(sync bool) *RMQWorker {
	w.syncMode = sync
	return w
}

/*

             __.-/|
             \`o_O'  +------------------------------------------------------+
              =( )=  |        RMQ Monitoring Worker (RMQ-M Worker)          |
                U|   | I accept responses from exchange to the binded queue |
      /\  /\   / |   +------------------------------------------------------+
     ) /^\) ^\/ _)\     |
     )   /^\/   _) \    |
     )   _ /  / _)  \___|_
 /\  )/\/ ||  | )_)\___,|))
<  >      |(,,) )__)    |
 ||      /    \)___)\
 | \____(      )___) )____
  \______(_______;;;)__;;;)

*/

// NewRMQMonitoringWorker - declare queue, bind to exchange, create worker & run.
// monitoring worker used for create a queue and receive messages from exchange into it
func (r *RMQHandler) NewRMQMonitoringWorker(task RMQMonitoringWorkerTask) (*RMQMonitoringWorker, APIError) {
	// declare queue & bind
	err := r.RMQQueueDeclareAndBind(RMQQueueDeclareTask{
		QueueName:        task.QueueName,
		Durable:          task.ISQueueDurable,
		AutoDelete:       task.ISAutoDelete,
		FromExchangeName: task.FromExchangeName,
		RoutingKey:       task.RoutingKey,
		MessagesLifetime: task.MessagesLifetime,
	})
	if err != nil {
		return nil, err
	}

	// create worker
	w := RMQMonitoringWorker{}
	w.Worker, err = r.NewRMQWorker(task.QueueName, task.Callback)
	if err != nil {
		return nil, err
	}

	// add optional params
	if task.ID != "" {
		w.Worker.SetID(task.ID)
	}
	if task.Timeout > 0 {
		w.Worker.SetTimeout(task.Timeout, task.TimeoutCallback)
	}
	if task.WorkerName != "" {
		w.Worker.SetName(task.WorkerName)
	}

	// setup worker
	if task.WorkerName != "" {
		w.Worker.SetName(task.WorkerName)
	}

	// run worker
	go w.Worker.Serve()
	return &w, nil
}

// Stop listen rmq messages
func (w *RMQMonitoringWorker) Stop() {
	w.Worker.Stop()
}

// Pause handle rmq messages
func (w *RMQMonitoringWorker) Pause() {
	w.Worker.Pause()
}

// Resume handle rmq messages
func (w *RMQMonitoringWorker) Resume() {
	w.Worker.Resume()
}

// AwaitFinish - await worker finished
func (w *RMQMonitoringWorker) AwaitFinish() {
	w.Worker.AwaitFinish()
}

// Reset worker channels
func (w *RMQMonitoringWorker) Reset() {
	w.Worker.Reset()
}

// GetName - get worker name
func (w *RMQMonitoringWorker) GetName() string {
	return w.Worker.GetName()
}

// GetID - get worker ID
func (w *RMQMonitoringWorker) GetID() string {
	return w.Worker.GetID()
}

// StopConnections - force stop worker connections
func (w *RMQMonitoringWorker) StopConnections() {
	w.Worker.CloseChannels()
}
