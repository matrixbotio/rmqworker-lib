package rmqworker

import (
	"time"

	"github.com/matrixbotio/constants-lib"
	simplecron "github.com/sagleft/simple-cron"
	"github.com/streadway/amqp"
)

// RMQHandler - RMQ connection handler
type RMQHandler struct {
	Connections handlerConnections
	Logger      *constants.Logger
	Cron        *simplecron.CronObject
}

type handlerConnections struct {
	Data RMQConnectionData

	Publish connectionPair // connection & channel for publish messages
	Consume connectionPair // connection & channel for consume messages
}

type connectionPair struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
}

// NewRMQHandler - create new RMQHandler
func NewRMQHandler(connData RMQConnectionData, logger ...*constants.Logger) (*RMQHandler, APIError) {
	// create handler
	r := RMQHandler{
		Connections: handlerConnections{
			Data: connData,
		},
	}

	// assign logger
	if len(logger) > 0 {
		if logger[0] != nil {
			r.Logger = logger[0]
		}
	}

	// open connection & channel
	err := r.recreateConnection()
	if err != nil {
		return nil, err
	}

	// run cron for check connection & channel
	r.Cron = simplecron.NewCronHandler(
		r.checkConnections,
		time.Minute*cronConnectionCheckTimeout,
	)
	go r.Cron.Run()

	return &r, nil
}

func (r *RMQHandler) recreateConnection() APIError {
	var err APIError
	// set connections for publish messages
	r.Connections.Publish.Conn, r.Connections.Publish.Channel, err = r.openConnectionNChannel()
	if err != nil {
		return err
	}

	// set connections for consume messages
	r.Connections.Consume.Conn, r.Connections.Consume.Channel, err = r.openConnectionNChannel()
	return err
}

// NewRMQHandler - clone handler & open new RMQ channel
func (r *RMQHandler) NewRMQHandler() (*RMQHandler, APIError) {
	handlerRoot := *r
	newHandler := handlerRoot

	// open new channel for publish
	var err APIError
	newHandler.Connections.Publish.Channel, err = openRMQChannel(newHandler.Connections.Publish.Conn)
	if err != nil {
		return nil, err
	}

	// & consume messages
	newHandler.Connections.Consume.Channel, err = openRMQChannel(newHandler.Connections.Publish.Conn)
	if err != nil {
		return nil, err
	}

	return &newHandler, nil
}

func (r *RMQHandler) checkConnections() {
	if r.Connections.Publish.Conn.IsClosed() {
		r.Logger.Verbose("connection (publish conn) is closed, open new..")
		r.recreateConnection()
	}
	if r.Connections.Consume.Conn.IsClosed() {
		r.Logger.Verbose("connection (consume conn) is closed, open new..")
		r.recreateConnection()
	}
}
