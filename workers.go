package rmqworker

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/matrixbotio/constants-lib"
	simplecron "github.com/sagleft/simple-cron"
	"github.com/streadway/amqp"
)

// RMQWorker - just RMQ worker
type RMQWorker struct {
	Data        rmqWorkerData
	Connections rmqWorkerConnections
	Channels    rmqWorkerChannels
	Paused      bool

	DeliveryCallback RMQDeliveryCallback
	TimeoutCallback  RMQTimeoutCallback

	LoggerInfoCallback  LoggerInfoFunc
	LoggerErrorCallback LoggerErrorFunc
}

// NewRMQWorker - create new RMQ worker to receive messages
func NewRMQWorker(
	QueueName string,
	RMQConn *amqp.Connection,
	callback RMQDeliveryCallback,
) (*RMQWorker, error) {
	wChannel, err := RMQConn.Channel()
	if err != nil {
		return nil, errors.New("failed to open channel for rmq worker (constructor): " +
			err.Error())
	}

	w := RMQWorker{
		Data: rmqWorkerData{
			Name:                "rmq worker",
			QueueName:           QueueName,
			AutoAck:             false,
			CheckResponseErrors: true,
		},
		Connections: rmqWorkerConnections{
			RMQConn:    RMQConn,
			RMQChannel: wChannel,
		},
		Channels: rmqWorkerChannels{
			RMQMessages: make(<-chan amqp.Delivery),
			OnFinished:  make(chan struct{}),
			StopCh:      make(chan struct{}),
		},
		DeliveryCallback: callback,
	}
	w.SetInfoLogger(w.defaultLogger).SetErrorLogger(w.defaultLogger)

	return &w, nil
}

// LoggerInfoFunc - logger callback function (info)
type LoggerInfoFunc func(i interface{})

// LoggerErrorFunc - logger callback function (error)
type LoggerErrorFunc func(i interface{})

func (w *RMQWorker) defaultLogger(i interface{}) {
	log.Println(i)
}

// SetInfoLogger - set RMQ worker logger callback (info)
func (w *RMQWorker) SetInfoLogger(logger LoggerInfoFunc) *RMQWorker {
	w.LoggerInfoCallback = logger
	return w
}

// SetErrorLogger - set RMQ worker logger callback (error)
func (w *RMQWorker) SetErrorLogger(logger LoggerErrorFunc) *RMQWorker {
	w.LoggerErrorCallback = logger
	return w
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

func (w *RMQWorker) logInfo(message string) {
	w.LoggerCallback(w.getLogWorkerName() + message)
}

func (w *RMQWorker) logError(err APIError) {
	w.LoggerCallback(err)
}

func (w *RMQWorker) serve() {
	w.handleReconnect()
}

func (w *RMQWorker) handleReconnect() {
	reconnect(w.subscribe, w.Data.Name)
	w.listen()
}

func (w *RMQWorker) subscribe() apiError {
	var err error
	var aErr apiError
	w.log("check rmq connection...")
	w.Connections.RMQChannel, aErr = checkRMQConnection(w.Connections.RMQConn)
	if aErr != nil {
		// check connection is open
		if aErr.Name != "DATA_EXISTS" {
			return aErr
		}
	}

	if w.Connections.RMQChannel == nil {
		// channel not created but connection is active
		// create new channel
		w.log("rmq channel is nil. creating new channel...")
		w.Connections.RMQChannel, aErr = getRMQChannel(w.Connections.RMQConn)
		if aErr != nil {
			return aErr
		}
	}
	w.log("consume rmq messages...")
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
	w.log("listen messages...")
	awaitMessages := true

	go func() {
		<-w.Channels.StopCh
		awaitMessages = false
	}()

	if w.Data.UseResponseTimeout {
		w.log("run response timeout cron")
		simplecron.NewCronHandler(w.timeIsUp, w.Data.WaitResponseTimeout).Run()
	}

	for awaitMessages {
		for rmqDelivery := range w.Channels.RMQMessages {
			if !awaitMessages {
				w.log("break")
				break
			}

			if w.Paused {
				continue // ignore message
			}

			go w.handleRMQMessage(rmqDelivery)
		}
	}

	w.log("work finished")
	w.Channels.OnFinished <- struct{}{}
}

func (w *RMQWorker) handleRMQMessage(rmqDelivery amqp.Delivery) {
	// auto accept message if needed
	if !w.Data.AutoAck {
		w.log("ack message")
		err := rmqDelivery.Acknowledger.Ack(rmqDelivery.DeliveryTag, false)
		//err := rmqDelivery.Ack(false) // accept multiple = false
		if err != nil {
			Logger.Error(constants.Error(
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
			Logger.Error(aErr)
			return
		}
	}

	// callback
	if w.DeliveryCallback == nil {
		Logger.Error(constants.Error("DATA_HANDLE_ERR", "rmq worker message callback is nil"))
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
