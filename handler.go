package rmqworker

import (
	"time"

	"github.com/matrixbotio/constants-lib"
	simplecron "github.com/sagleft/simple-cron"
	"github.com/streadway/amqp"
)

// RMQHandler - RMQ connection handler
type RMQHandler struct {
	ConnectionData RMQConnectionData
	RMQConn        *amqp.Connection
	RMQChannel     *amqp.Channel
	Logger         *constants.Logger
	Cron           *simplecron.CronObject
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
	r.recreateConnection()

	// run cron for check connection & channel
	r.Cron = simplecron.NewCronHandler(
		r.checkConnection,
		time.Minute*cronConnectionCheckTimeout,
	)
	go r.Cron.Run()

	return &r, nil
}

func (r *RMQHandler) recreateConnection() APIError {
	var err APIError
	r.RMQConn, r.RMQChannel, err = r.openConnectionNChannel()
	return err
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

func (r *RMQHandler) checkConnection() {
	if r.RMQConn.IsClosed() {
		r.Logger.Verbose("connection is closed, open new..")
		r.recreateConnection()
	}
}
