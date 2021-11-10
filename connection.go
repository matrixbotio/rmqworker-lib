package rmqworker

import (
	"crypto/tls"
	"time"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// openConnectionNChannel - open new RMQ connection & channel
func openConnectionNChannel(conn *amqp.Connection, conData RMQConnectionData, logger *constants.Logger, consumeFunc func(channel *amqp.Channel)) (*amqp.Connection, *amqp.Channel, APIError) {
	// get connection
	if conn == nil || conn.IsClosed() {
		var err APIError
		conn, err = rmqConnect(conData, logger)
		if err != nil {
			return nil, nil, err
		}
		connCloseReceiver := make(chan *amqp.Error)
		conn.NotifyClose(connCloseReceiver)
		go handleNotifyClose(connCloseReceiver, conn, conData, logger, consumeFunc)
	}

	// get channel
	channel, err := openRMQChannel(conn, consumeFunc)
	if err != nil {
		return nil, nil, err
	}

	channelCloseReceiver := make(chan *amqp.Error)
	channel.NotifyClose(channelCloseReceiver)
	go handleNotifyClose(channelCloseReceiver, conn, conData, logger, consumeFunc)

	return conn, channel, nil
}

// rmqConnect - open new RMQ connection
func rmqConnect(connData RMQConnectionData, logger *constants.Logger) (*amqp.Connection, APIError) {
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

// openRMQChannel - open new RMQ channel
func openRMQChannel(conn *amqp.Connection, consumeFunc func(channel *amqp.Channel)) (*amqp.Channel, APIError) {
	channel, rmqErr := conn.Channel()
	if rmqErr != nil {
		return nil, constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to get amqp channel: "+rmqErr.Error(),
		)
	}
	err := channel.Qos(1, 0, false)
	if err != nil {
		return nil, constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to set up QOS: "+err.Error(),
		)
	}

	if consumeFunc != nil {
		consumeFunc(channel)
	}

	return channel, nil
}

// handleNotifyClose - reconnect when connection is closed
func handleNotifyClose(receiver chan *amqp.Error, conn *amqp.Connection, connData RMQConnectionData, logger *constants.Logger, consumeFunc func(channel *amqp.Channel)) {
	for closeError := range receiver {
		logger.Error("RMQ connection closed: " + closeError.Error())
		for {
			var err APIError
			// TODO: synchronize
			conn, _, err = openConnectionNChannel(conn, connData, logger, consumeFunc)
			if err == nil {
				logger.Log("RMQ connection/channel recovered")
				break
			} else {
				logger.Error("Exception while trying to recover RMQ connection/channel: " + err.Message)
				time.Sleep(5 * time.Second)
			}
		}
	}
}
