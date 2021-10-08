package rmqworker

import (
	"crypto/tls"
	"strconv"
	"time"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

func rmqConnect(connData RMQConnectionData) (*amqp.Connection, APIError) {
	var conn *amqp.Connection
	var err error
	var useTLS bool = true

	if connData.UseTLS == "0" {
		useTLS = false
	}

	dsn := "amqp"
	if useTLS {
		dsn += "s"
	}
	dsn += "://" + connData.User + ":" + connData.Password +
		"@" + connData.Host + ":" + connData.Port + "/"

	if useTLS {
		tls := &tls.Config{MinVersion: tls.VersionTLS12}
		conn, err = amqp.DialTLS(dsn, tls)
	} else {
		conn, err = amqp.Dial(dsn)
	}

	if err != nil {
		return nil, constants.Error(
			"SERVICE_CONN_ERR",
			"Failed to connect to RabbitMQ: "+err.Error(),
		)
	}
	return conn, nil
}

// openConnectionNChannel - open new RMQ connection & channel
func (r *RMQHandler) openConnectionNChannel() (*amqp.Connection, *amqp.Channel, APIError) {
	// get connection
	conn, err := rmqConnect(r.ConnectionData)
	if err != nil {
		return nil, nil, err
	}

	// get channel
	channel, err := openRMQChannel(conn)
	if err != nil {
		return nil, nil, err
	}

	return conn, channel, nil
}

// openRMQChannel - open new RMQ channel
func openRMQChannel(conn *amqp.Connection) (*amqp.Channel, APIError) {
	channel, rmqErr := conn.Channel()
	if rmqErr != nil {
		return nil, constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to get amqp channel: "+rmqErr.Error(),
		)
	}
	return channel, nil
}

// checkRMQConnection - check RMQ connection is active. open new connection if inactive
func checkRMQConnection(RMQConn *amqp.Connection, connData RMQConnectionData) (*amqp.Channel, APIError) {
	if !RMQConn.IsClosed() {
		return nil, constants.Error(
			"DATA_EXISTS",
			"rmq connection is active",
		)
	}
	conn, err := rmqConnect(connData)
	if err != nil {
		return nil, err
	}

	RMQConn = conn
	RMQChannel, err := openRMQChannel(conn)
	if err != nil {
		return nil, err
	}

	if RMQChannel == nil {
		return nil, constants.Error(
			"BASE_INTERNAL_ERROR",
			"failed to open new rmq channel, new channel is nil",
		)
	}
	return RMQChannel, nil
}

type connFunc func() APIError

func (r *RMQWorker) reconnect(callback connFunc, connectionDescription string) {
	isConnected := false
	for !isConnected {
		for i := 0; i < reconnectionAttemptsNumber; i++ {
			err := callback()
			if err == nil {
				// connection established
				return
			}
			if err.Name == "DATA_EXISTS" {
				// connection is open
				return
			}
			r.logger.Warn(err)
			time.Sleep(reconnectAfterSeconds * time.Second)
		}
		errMsg := "failed to connect to " +
			connectionDescription + " after " +
			strconv.Itoa(reconnectionAttemptsNumber) + " attempts"

		r.logger.Error(constants.Error("SERVICE_CONN_ERR", errMsg))
		time.Sleep(time.Second * waitingBetweenAttempts)
	}
}
