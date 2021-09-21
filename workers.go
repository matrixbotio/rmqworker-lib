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

func (w *RMQWorker) setName(name string) *RMQWorker {
	w.Data.Name = name
	return w
}

func (w *RMQWorker) setID(id string) *RMQWorker {
	w.Data.ID = id
	return w
}

func (w *RMQWorker) setAutoAck(autoAck bool) *RMQWorker {
	w.Data.AutoAck = autoAck
	return w
}

func (w *RMQWorker) setCheckResponseErrors(check bool) *RMQWorker {
	w.Data.CheckResponseErrors = check
	return w
}

func (w *RMQWorker) setTimeout(timeout time.Duration, callback RMQTimeoutCallback) *RMQWorker {
	w.Data.UseResponseTimeout = true
	w.Data.WaitResponseTimeout = timeout
	w.TimeoutCallback = callback
	return w
}

func (w *RMQWorker) getLogWorkerName() string {
	return "RMQ Worker " + w.Data.Name + ": "
}

func (w *RMQWorker) serve() {
	w.handleReconnect()
}

func (w *RMQWorker) handleReconnect() {
	w.reconnect(w.subscribe, w.Data.Name)
	w.listen()
}

func (w *RMQWorker) subscribe() APIError {
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

func (w *RMQWorker) stop() {
	w.Channels.StopCh <- struct{}{}
}

func (w *RMQWorker) pause() {
	w.Paused = true
}

func (w *RMQWorker) resume() {
	w.Paused = false
}

func (w *RMQWorker) listen() {
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
	w.stop()
	w.TimeoutCallback(w)
}

func (w *RMQWorker) awaitFinish() {
	<-w.Channels.OnFinished
}
