package rmqworker

import (
	"fmt"
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

	DeliveryCallback RMQDeliveryCallback
	TimeoutCallback  RMQTimeoutCallback

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
			AutoAck:             false,
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
	w.Data.AutoAck = autoAck
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
	w.logInfo("check rmq connection...")
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
		w.logInfo("rmq channel is nil. creating new channel...")
		w.Connections.RMQChannel, aErr = openRMQChannel(w.Connections.RMQConn)
		if aErr != nil {
			return aErr
		}
	}
	w.logInfo("consume rmq messages...")
	w.Channels.RMQMessages, err = w.Connections.RMQChannel.Consume(
		w.Data.QueueName, // queue
		"",               // consumer. "" > generate random ID
		w.Data.AutoAck,   // auto-ack
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
		return e
	}
	return nil
}

// Stop RMQ messages listen
func (w *RMQWorker) Stop() {
	w.Channels.StopCh <- struct{}{}
}

// Pause RMQ Worker (ignore messages)
func (w *RMQWorker) Pause() {
	w.Paused = true
}

// Resume RMQ Worker (continue listen messages)
func (w *RMQWorker) Resume() {
	w.Paused = false
}

// Listen RMQ messages
func (w *RMQWorker) Listen() {
	w.logInfo("listen messages...")
	awaitMessages := true

	go func() {
		<-w.Channels.StopCh
		awaitMessages = false
	}()

	if w.Data.UseResponseTimeout {
		w.logInfo("run response timeout cron")
		simplecron.NewCronHandler(w.timeIsUp, w.Data.WaitResponseTimeout).Run()
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

			go w.handleRMQMessage(rmqDelivery)
		}
	}

	w.logInfo("work finished")
	w.Channels.OnFinished <- struct{}{}
}

func (w *RMQWorker) handleRMQMessage(rmqDelivery amqp.Delivery) {
	// auto accept message if needed
	if !w.Data.AutoAck {
		w.logInfo("ack message")
		err := rmqDelivery.Acknowledger.Ack(rmqDelivery.DeliveryTag, false)
		if err != nil {
			w.logError(constants.Error(
				"DATA_HANDLE_ERR",
				"failed to ack task: "+err.Error(),
			))

			return
		}
	}

	// check response error
	if w.Data.CheckResponseErrors {
		aErr := rmqCheckResponseError(rmqDelivery)
		if aErr != nil {
			fmt.Println("check response error: " + aErr.Message)
			w.logError(aErr)
			return
		}
	}

	// callback
	if w.DeliveryCallback == nil {
		w.logError(constants.Error("DATA_HANDLE_ERR", "rmq worker message callback is nil"))
	}
	w.DeliveryCallback(w, rmqDelivery)
}

func (w *RMQWorker) timeIsUp() {
	w.Stop()
	w.TimeoutCallback(w)
}

// AwaitFinish - wait for worker finished
func (w *RMQWorker) AwaitFinish() {
	<-w.Channels.OnFinished
}

// RMQMonitoringWorker - rmq extended worker
type RMQMonitoringWorker struct {
	Worker *RMQWorker
}

// NewRMQMonitoringWorker - declare queue, bind to exchange, create worker & run.
// monitoring worker used for send request & get response.
func (r *RMQHandler) NewRMQMonitoringWorker(task rmqMonitoringWorkerTask) (*RMQMonitoringWorker, APIError) {
	// declare queue & bind
	err := r.rmqQueueDeclareAndBind(rmqQueueDeclareTask{
		RMQChannel:       task.RMQChannel,
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
