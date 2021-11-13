package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	simplecron "github.com/sagleft/simple-cron"
	"github.com/streadway/amqp"
	"sync"
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
	sync.Mutex
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

	err := r.openConnectionsAndChannels()
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (r *RMQHandler) openConnectionsAndChannels() APIError {
	var err APIError
	r.Connections.Publish.Conn, r.Connections.Publish.Channel, err =
		openConnectionNChannel(nil, r.Connections.Data, r.Logger, nil)
	if err != nil {
		return err
	}

	r.Connections.Consume.Conn, r.Connections.Consume.Channel, err =
		openConnectionNChannel(nil, r.Connections.Data, r.Logger, nil)
	return err
}

// NewRMQHandler - clone handler & open new RMQ channel
func (r *RMQHandler) NewRMQHandler() (*RMQHandler, APIError) {
	handlerRoot := *r
	newHandler := handlerRoot

	// open new channel for publish
	var err APIError
	newHandler.Connections.Publish.Conn, newHandler.Connections.Publish.Channel, err =
		openConnectionNChannel(newHandler.Connections.Publish.Conn, r.Connections.Data, r.Logger, nil)
	if err != nil {
		return nil, err
	}

	// & consume messages
	newHandler.Connections.Consume.Conn, newHandler.Connections.Consume.Channel, err =
		openConnectionNChannel(newHandler.Connections.Consume.Conn, r.Connections.Data, r.Logger, nil)
	if err != nil {
		return nil, err
	}

	return &newHandler, nil
}
