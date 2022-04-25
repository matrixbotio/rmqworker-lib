package rmqworker

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/beefsack/go-rate"
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
func (r *RMQHandler) NewRMQWorker(task WorkerTask) (*RMQWorker, APIError) {
	err := r.checkConnection()
	if err != nil {
		return nil, err
	}

	// set worker name
	if task.WorkerName == "" {
		task.WorkerName = "rmq worker"
	}

	w := RMQWorker{
		data: rmqWorkerData{
			Name:                task.WorkerName,
			QueueName:           task.QueueName,
			AutoAckByLib:        true,
			CheckResponseErrors: true,
		},
		connections: &r.Connections,
		channels: rmqWorkerChannels{
			RMQMessages: make(<-chan amqp.Delivery),
			OnFinished:  make(chan struct{}, 1),
			StopCh:      make(chan struct{}, 1),
		},
		deliveryCallback:      task.Callback,
		errorCallback:         task.ErrorCallback,
		logger:                r.Logger,
		awaitMessages:         true,
		rejectDeliveryOnPause: task.RejectDeliveryOnPause,
	}
	if task.EnableRateLimiter && task.MaxEventsPerSecond > 0 {
		w.rateLimiter = rate.New(task.MaxEventsPerSecond, time.Second)
	}

	return &w, nil
}

// CloseChannels - force close worker's channels
func (w *RMQWorker) CloseChannels() {
	err := w.connections.Consume.Channel.Close()
	if err != nil {
		w.logWarn(constants.Error(baseInternalError, "Exception closing consumer channel"+err.Error()))
	}
	err = w.connections.Publish.Channel.Close()
	if err != nil {
		w.logWarn(constants.Error(baseInternalError, "Exception closing publisher channel"+err.Error()))
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

// IsConnAlive - check that the connection is established
func (w *RMQWorker) IsConnAlive() bool {
	if w.connections == nil || w.connections.Consume.Conn == nil {
		return false
	}

	return !w.connections.Consume.Conn.IsClosed()
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

// IsPaused - return worker paused state
func (w *RMQWorker) IsPaused() bool {
	return w.paused
}

// IsActive - return worker paused state
func (w *RMQWorker) IsActive() bool {
	return w.awaitMessages
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

func (w *RMQWorker) setupConsume(channel *amqp.Channel) {
	var err error
	if w.channels.RMQMessages == nil || !w.channels.msgChanOpened {
		w.data.ConsumerId = getUUID()
		w.channels.RMQMessages, err = channel.Consume(
			w.data.QueueName,  // queue
			w.data.ConsumerId, // consumer. "" > generate random ID
			false,             // auto-ack by RMQ service
			false,             // exclusive
			false,             // no-local
			false,             // no-wait
			nil,               // args
		)
		if err != nil {
			e := constants.Error(
				"SERVICE_REQ_FAILED",
				"failed to consume rmq worker messages: "+err.Error(),
			)
			w.logError(e)
		}
		w.channels.msgChanOpened = true
	}
}

// Subscribe to RMQ messages
func (w *RMQWorker) Subscribe() APIError {
	var aErr APIError
	// channel not created but connection is active
	// create new channel
	consume := w.setupConsume
	consumerConnectionPair := &w.connections.Consume
	if consumerConnectionPair.consumes == nil {
		consumerConnectionPair.consumes = make(map[string]consumeFunc)
	}
	if w.data.ConsumerTag == "" {
		w.data.ConsumerTag = getUUID()
	}
	consumerConnectionPair.consumes[w.data.ConsumerTag] = consume
	aErr = openConnectionNChannel(openConnectionNChannelTask{
		connectionPair:     consumerConnectionPair,
		connData:           w.connections.Data,
		logger:             w.logger,
		consume:            w.setupConsume,
		skipChannelOpening: true, // channel already set in worker constructor
	})
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

	if w.connections.Consume.Channel != nil {
		w.connections.Consume.rwMutex.RLock()
		err := w.connections.Consume.Channel.Cancel(w.data.ConsumerId, false)
		if err != nil {
			if !strings.Contains(err.Error(), "channel/connection is not open") {
				w.logError(constants.Error(baseInternalError, "Exception stopping consumer: "+err.Error()))
			}
		}
		delete(w.connections.Consume.consumes, w.data.ConsumerTag)
		w.connections.Consume.rwMutex.RUnlock()
	}

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
	w.channels.OnFinished = make(chan struct{}, 1)
	w.channels.StopCh = make(chan struct{}, 1)

	// re-consume
	err := startConsumer(consumeTask{
		consume:        w.setupConsume,
		connData:       w.connections.Data,
		connectionPair: &w.connections.Consume,
	})
	if err != nil {
		w.logError(err)
		return
	}

	go w.Listen()
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

// Listen RMQ messages
func (w *RMQWorker) Listen() {
	w.awaitMessages = true

	if w.data.UseResponseTimeout {
		w.runCron()
	}

	for w.awaitMessages {
		for rmqDelivery := range w.channels.RMQMessages {
			// create delivery handler
			delivery := NewRMQDeliveryHandler(rmqDelivery)

			if !w.awaitMessages {
				w.handleDeliveryOnPause(delivery)
				return
			}

			if w.paused {
				w.handleDeliveryOnPause(delivery)
				continue // ignore message
			}

			w.limitHandleRate()
			w.stopCron()
			w.handleRMQMessage(delivery)
		}
		w.channels.msgChanOpened = false
		w.logVerbose("Sleep " + strconv.Itoa(waitingBetweenMsgSubscription) + " seconds before re-consuming")
		time.Sleep(waitingBetweenMsgSubscription * time.Second)
	}
}

func (w *RMQWorker) handleDeliveryOnPause(delivery RMQDeliveryHandler) {
	var err APIError
	if w.rejectDeliveryOnPause {
		err = delivery.Reject(true)
	} else {
		err = delivery.Accept()
	}
	if err != nil {
		w.logWarn(err)
	}
}

func (w *RMQWorker) handleError(err *constants.APIError) {
	if w.errorCallback == nil {
		w.logError(err)
		return
	}
	w.errorCallback(w, err)
}

func (w *RMQWorker) handleRMQMessage(delivery RMQDeliveryHandler) {
	w.logVerbose("new rmq message found")

	// auto accept message if needed
	if w.data.AutoAckByLib {
		err := delivery.Accept()
		if err != nil {
			w.handleError(err)
			return
		}
	}

	// check response error
	w.logVerbose("check message errors")
	if w.data.CheckResponseErrors {
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
		QueueLength:      task.QueueLength,
		DisableOverflow:  task.DisableOverflow,
	})
	if err != nil {
		return nil, err
	}

	// create worker
	w := RMQMonitoringWorker{}
	w.Worker, err = r.NewRMQWorker(WorkerTask{
		QueueName:             task.QueueName,
		Callback:              task.Callback,
		WorkerName:            task.WorkerName,
		ErrorCallback:         task.ErrorCallback,
		EnableRateLimiter:     task.EnableRateLimiter,
		MaxEventsPerSecond:    task.MaxEventsPerSecond,
		RejectDeliveryOnPause: task.RejectDeliveryOnPause,
	})
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

// IsPaused - return worker paused state
func (w *RMQMonitoringWorker) IsPaused() bool {
	return w.Worker.IsPaused()
}

// IsActive - return worker paused state
func (w *RMQMonitoringWorker) IsActive() bool {
	return w.Worker.IsActive()
}

// IsConnAlive - check that the connection is established
func (w *RMQMonitoringWorker) IsConnAlive() bool {
	return w.Worker.IsConnAlive()
}
