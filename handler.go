package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// RMQHandler - RMQ connection handler
type RMQHandler struct {
	ConnectionData RMQConnectionData
	RMQConn        *amqp.Connection
	RMQChannel     *amqp.Channel
	Logger         *constants.Logger
}

// NewRMQHandler - create new RMQHandler
func NewRMQHandler(connData RMQConnectionData, logger ...*constants.Logger) (*RMQHandler, APIError) {
	// create handler
	r := RMQHandler{
		ConnectionData: connData,
	}

	// assign logger
	if len(logger) > 0 {
		if logger[0] != nil {
			r.Logger = logger[0]
		}
	}

	// open connection & channel
	var err APIError
	r.RMQConn, r.RMQChannel, err = r.openConnectionNChannel()
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// NewRMQHandler - clone handler & open new RMQ channel
func (r *RMQHandler) NewRMQHandler() (*RMQHandler, APIError) {
	handlerRoot := *r
	newHandler := handlerRoot

	// open new channel
	var err APIError
	newHandler.RMQChannel, err = openRMQChannel(newHandler.RMQConn)
	if err != nil {
		return nil, err
	}

	return &newHandler, nil
}
