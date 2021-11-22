package rmqworker

import (
	"crypto/tls"
	"time"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// openConnectionNChannel - open new RMQ connection & channel
func openConnectionNChannel(connectionPair *connectionPair, conData RMQConnectionData, logger *constants.Logger,
	consumeFunc func(channel *amqp.Channel)) APIError {
	var err APIError

	// get connection
	if connectionPair.Conn == nil || connectionPair.Conn.IsClosed() {
		connectionPair.Conn, err = rmqConnect(conData)
		if err != nil {
			return err
		}
		connCloseReceiver := make(chan *amqp.Error)
		connectionPair.Conn.NotifyClose(connCloseReceiver)
		go func() {
			for closeErr := range connCloseReceiver {
				handleNotifyClose(closeErr, connectionPair, conData, logger, consumeFunc)
			}
		}()
	}

	// get channel
	connectionPair.Channel, err = openRMQChannel(connectionPair.Conn, consumeFunc)
	if err != nil {
		return err
	}
	channelCloseReceiver := make(chan *amqp.Error)
	connectionPair.Channel.NotifyClose(channelCloseReceiver)
	go func() {
		for closeErr := range channelCloseReceiver {
			handleNotifyClose(closeErr, connectionPair, conData, logger, consumeFunc)
		}
	}()

	return nil
}

// rmqConnect - open new RMQ connection
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
func handleNotifyClose(closeError *amqp.Error, connectionPair *connectionPair, connData RMQConnectionData,
	logger *constants.Logger, consumeFunc func(channel *amqp.Channel)) {
	// Lock all interactions with the connection/channel unless it will be reopened
	connectionPair.rwMutex.Lock()
	defer connectionPair.rwMutex.Unlock()

	logger.Error("RMQ connection/channel closed: " + closeError.Error())
	for {
		var err APIError
		// TODO: synchronize
		err = openConnectionNChannel(connectionPair, connData, logger, consumeFunc)
		if err == nil {
			logger.Log("RMQ connection/channel recovered")
			break
		} else {
			logger.Error("Exception while trying to recover RMQ connection/channel: " + err.Message)
			time.Sleep(5 * time.Second)
		}
	}
}
