package rmqworker

import (
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/matrixbotio/constants-lib"
	simplecron "github.com/sagleft/simple-cron"
	"github.com/streadway/amqp"
)

// NewRMQWorker - create new RMQ worker to receive messages
func (r *RMQHandler) NewRMQWorker(
	QueueName string,
	callback RMQDeliveryCallback,
) (*RMQWorker, APIError) {
	var err APIError
	var wChannel *amqp.Channel
	if r.RMQConn.IsClosed() {
		// open new connection
		wChannel, err = openRMQChannel(r.RMQConn)
		if err != nil {
			return nil, err
		}
	} else {
		wChannel = r.RMQChannel
	}

	w := RMQWorker{
		connectionData: r.ConnectionData,
		data: rmqWorkerData{
			Name:                "rmq worker",
			QueueName:           QueueName,
			AutoAckByLib:        true,
			CheckResponseErrors: true,
		},
		connections: rmqWorkerConnections{
			RMQConn:    r.RMQConn,
			RMQChannel: wChannel,
		},
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

func (w *RMQWorker) logWarn(err *constants.APIError) {
	if w.logger != nil {
		err.Message = w.getLogWorkerName() + err.Message
		w.logger.Warn(err)
	}
}

func (w *RMQWorker) logInfo(message string) {
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
	w.HandleReconnect()
}

// HandleReconnect - reconnect to RMQ delivery (messages)
func (w *RMQWorker) HandleReconnect() {
	w.logger.Verbose("reconnect..")
	w.reconnect(w.openConnection, w.data.Name)

	w.logger.Verbose("subscribe..")
	err := w.Subscribe()
	if err != nil {
		w.logError(err)
	}

	w.logger.Verbose("listen..")
	w.Listen()
}

func (w *RMQWorker) openConnection() APIError {
	w.logger.Verbose("open RMQ connection..")
	return checkRMQConnection(
		w.connections.RMQConn,
		w.connectionData,
		w.connections.RMQChannel,
		w.logger,
	)
}

// Subscribe to RMQ messages
func (w *RMQWorker) Subscribe() APIError {
	var aErr APIError
	if w.connections.RMQChannel == nil {
		// channel not created but connection is active
		// create new channel
		w.connections.RMQChannel, aErr = openRMQChannel(w.connections.RMQConn)
		if aErr != nil {
			return aErr
		}
	}

	var err error
	w.channels.RMQMessages, err = w.connections.RMQChannel.Consume(
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
			"failed to consume rmq worker messages: "+err.Error()+". reopen channel...",
		)
		w.logError(e)
		// reopen channel
		var rErr APIError
		w.connections.RMQChannel, rErr = openRMQChannel(w.connections.RMQConn)
		if rErr != nil {
			return rErr
		}
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
	w.logInfo("worker stopped")
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
	}
}

func (w *RMQWorker) handleRMQMessage(rmqDelivery amqp.Delivery) {
	w.logger.Verbose("new rmq message found")
	// create delivery handler
	delivery := NewRMQDeliveryHandler(rmqDelivery)

	// auto accept message if needed
	if w.data.AutoAckByLib {
		w.logger.Verbose("ack by lib..")
		err := delivery.Accept()
		if err != nil {
			w.logger.Verbose("error: " + err.Message)
			w.logError(err)
			return
		}
	}

	// check response error
	w.logger.Verbose("check message errors")
	if w.data.CheckResponseErrors {
		aErr := delivery.CheckResponseError()
		if aErr != nil {
			w.logger.Verbose("message error: " + aErr.Message)
			w.logError(aErr)
			return
		}
	}

	// callback
	w.logger.Verbose("run callback..")
	if w.deliveryCallback == nil {
		w.logInfo("callback is not set")
		w.logError(constants.Error("DATA_HANDLE_ERR", "rmq worker message callback is nil"))
	}

	// run callback
	if w.syncMode {
		w.logger.Verbose("sync callback...")
		w.deliveryCallback(w, delivery)
	} else {
		w.logger.Verbose("async callback...")
		go w.deliveryCallback(w, delivery)
	}
}

func (w *RMQWorker) timeIsUp() {
	w.logger.Verbose("time is up")
	w.Stop()
	w.cronHandler.Stop()
	w.timeoutCallback(w)
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
