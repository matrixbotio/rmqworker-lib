package rmqworker

import (
	"time"

	"github.com/matrixbotio/constants-lib"
	simplecron "github.com/sagleft/simple-cron"
	"github.com/streadway/amqp"
)

// RMQWorker - just RMQ worker
type RMQWorker struct {
	ConnectionData RMQConnectionData
	Data           rmqWorkerData
	Connections    rmqWorkerConnections
	Channels       rmqWorkerChannels
	Paused         bool
	SyncMode       bool

	DeliveryCallback RMQDeliveryCallback
	TimeoutCallback  RMQTimeoutCallback
	cronHandler      *simplecron.CronObject

	Logger *constants.Logger
}

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
		ConnectionData: r.ConnectionData,
		Data: rmqWorkerData{
			Name:                "rmq worker",
			QueueName:           QueueName,
			AutoAckForQueue:     false,
			AutoAckByLib:        false,
			CheckResponseErrors: true,
		},
		Connections: rmqWorkerConnections{
			RMQConn:    r.RMQConn,
			RMQChannel: wChannel,
		},
		Channels: rmqWorkerChannels{
			RMQMessages: make(<-chan amqp.Delivery),
			OnFinished:  make(chan struct{}),
			StopCh:      make(chan struct{}),
		},
		DeliveryCallback: callback,
		Logger:           r.Logger,
	}

	return &w, nil
}

func (w *RMQWorker) logWarn(err APIError) {
	if w.Logger != nil {
		err.Message = w.getLogWorkerName() + err.Message
		w.Logger.Warn(err)
	}
}

func (w *RMQWorker) logInfo(message string) {
	if w.Logger != nil {
		w.Logger.Log(w.getLogWorkerName() + message)
	}
}

func (w *RMQWorker) logError(err APIError) {
	if w.Logger != nil {
		err.Message = w.getLogWorkerName() + err.Message
		w.Logger.Error(err)
	}
}

// SetName - set RMQ worker name for logs
func (w *RMQWorker) SetName(name string) *RMQWorker {
	w.Data.Name = name
	return w
}

// SetID - set RMQ worker ID
func (w *RMQWorker) SetID(id string) *RMQWorker {
	w.Data.ID = id
	return w
}

// SetAutoAck - auto accept messages
func (w *RMQWorker) SetAutoAck(autoAck bool) *RMQWorker {
	w.Data.AutoAckByLib = autoAck
	return w
}

// SetCheckResponseErrors - determines whether the errors in the answers passed to headers will be checked
func (w *RMQWorker) SetCheckResponseErrors(check bool) *RMQWorker {
	w.Data.CheckResponseErrors = check
	return w
}

// SetTimeout - set RMQ response timeout. When the timer goes out, the callback will be called
func (w *RMQWorker) SetTimeout(timeout time.Duration, callback RMQTimeoutCallback) *RMQWorker {
	w.Data.UseResponseTimeout = true
	w.Data.WaitResponseTimeout = timeout
	w.TimeoutCallback = callback
	return w
}

func (w *RMQWorker) getLogWorkerName() string {
	return "RMQ Worker " + w.Data.Name + ": "
}

// Serve - listen RMQ messages
func (w *RMQWorker) Serve() {
	w.HandleReconnect()
}

// HandleReconnect - reconnect to RMQ delivery (messages)
func (w *RMQWorker) HandleReconnect() {
	w.reconnect(w.Subscribe, w.Data.Name)
	w.Listen()
}

// Subscribe to RMQ messages
func (w *RMQWorker) Subscribe() APIError {
	var err error
	var aErr APIError
	w.Connections.RMQChannel, aErr = checkRMQConnection(w.Connections.RMQConn, w.ConnectionData)
	if aErr != nil {
		// check connection is open
		if aErr.Name != "DATA_EXISTS" {
			return aErr
		}
	}

	if w.Connections.RMQChannel == nil {
		// channel not created but connection is active
		// create new channel
		w.Connections.RMQChannel, aErr = openRMQChannel(w.Connections.RMQConn)
		if aErr != nil {
			return aErr
		}
	}
	w.Channels.RMQMessages, err = w.Connections.RMQChannel.Consume(
		w.Data.QueueName,       // queue
		"",                     // consumer. "" > generate random ID
		w.Data.AutoAckForQueue, // auto-ack
		false,                  // exclusive
		false,                  // no-local
		false,                  // no-wait
		nil,                    // args
	)
	if err != nil {
		e := constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to consume rmq worker messages: "+err.Error(),
		)
		return e
	}
	return nil
}

// Stop RMQ messages listen
func (w *RMQWorker) Stop() {
	if w.cronHandler != nil {
		w.cronHandler.Stop()
	}
	w.Channels.StopCh <- struct{}{}
}

// Pause RMQ Worker (ignore messages)
func (w *RMQWorker) Pause() {
	if w.cronHandler != nil {
		w.cronHandler.Pause()
	}
	w.Paused = true
}

// Resume RMQ Worker (continue listen messages)
func (w *RMQWorker) Resume() {
	if w.cronHandler != nil {
		w.cronHandler.Resume()
	}
	w.Paused = false
}

// Reset worker channels
func (w *RMQWorker) Reset() {
	w.Channels.OnFinished = make(chan struct{})
	w.Channels.StopCh = make(chan struct{})
}

// Listen RMQ messages
func (w *RMQWorker) Listen() {
	awaitMessages := true

	go func() {
		<-w.Channels.StopCh
		awaitMessages = false
	}()

	if w.Data.UseResponseTimeout {
		w.logInfo("run response timeout cron")
		w.cronHandler = simplecron.NewCronHandler(w.timeIsUp, w.Data.WaitResponseTimeout)
		w.cronHandler.Run()
	}

	for awaitMessages {
		for rmqDelivery := range w.Channels.RMQMessages {
			if !awaitMessages {
				w.logInfo("break")
				break
			}

			if w.Paused {
				continue // ignore message
			}

			w.logInfo("new message found. run handler..")
			w.handleRMQMessage(rmqDelivery)
		}
	}

	w.logInfo("work finished")
	w.Channels.OnFinished <- struct{}{}
}

func (w *RMQWorker) handleRMQMessage(rmqDelivery amqp.Delivery) {
	// create delivery handler
	delivery := NewRMQDeliveryHandler(rmqDelivery)

	// auto accept message if needed
	if w.Data.AutoAckByLib {
		w.logInfo("ack message")
		err := delivery.Accept()
		if err != nil {
			w.logError(err)
			return
		}
	}

	// check response error
	w.logInfo("check response error..")
	if w.Data.CheckResponseErrors {
		aErr := delivery.CheckResponseError()
		if aErr != nil {
			w.logError(aErr)
			return
		}
	}

	// callback
	if w.DeliveryCallback == nil {
		w.logError(constants.Error("DATA_HANDLE_ERR", "rmq worker message callback is nil"))
	}

	// run callback
	w.logInfo("run callback")
	go w.DeliveryCallback(w, delivery)
}

func (w *RMQWorker) timeIsUp() {
	w.Stop()
	w.TimeoutCallback(w)
}

// AwaitFinish - wait for worker finished
func (w *RMQWorker) AwaitFinish() {
	<-w.Channels.OnFinished
}

// SetSyncMode - whether to run the callback of task processing synchronously
func (w *RMQWorker) SetSyncMode(sync bool) *RMQWorker {
	w.SyncMode = sync
	return w
}

// RMQMonitoringWorker - rmq extended worker
type RMQMonitoringWorker struct {
	Worker *RMQWorker
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
