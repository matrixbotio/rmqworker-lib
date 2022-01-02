package rmqworker

import (
	"crypto/tls"
	"time"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

type consumeFunc func(channel *amqp.Channel)

// openConnectionNChannel - open new RMQ connection & channel
func openConnectionNChannel(task openConnectionNChannelTask) APIError {
	var err APIError

	// get connection
	if task.connectionPair.Conn == nil || task.connectionPair.Conn.IsClosed() {
		task.connectionPair.Conn, err = rmqConnect(task.connData)
		if err != nil {
			return err
		}
		connCloseReceiver := make(chan *amqp.Error)
		task.connectionPair.Conn.NotifyClose(connCloseReceiver)
		go func() {
			for task.errorData = range connCloseReceiver {
				onConnClosed(task)
			}
		}()
	}

	if task.skipChannelOpening {
		// use existing channel, setup messages consume
		err := setupConsume(task.connectionPair.Channel, task.consume)
		if err != nil {
			return err
		}
	} else {
		// get new channel
		task.connectionPair.Channel, err = openRMQChannel(task.connectionPair.Conn, task.consume)
		if err != nil {
			return err
		}
	}

	// setup channel reconnection
	channelCloseReceiver := make(chan *amqp.Error)
	task.connectionPair.Channel.NotifyClose(channelCloseReceiver)
	go func() {
		for task.errorData = range channelCloseReceiver {
			onConnClosed(task)
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
func openRMQChannel(conn *amqp.Connection, consume consumeFunc) (*amqp.Channel, APIError) {
	channel, rmqErr := conn.Channel()
	if rmqErr != nil {
		return nil, constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to get amqp channel: "+rmqErr.Error(),
		)
	}

	return channel, setupConsume(channel, consume)
}

func setupConsume(channel *amqp.Channel, consume consumeFunc) APIError {
	err := channel.Qos(1, 0, false)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to set up QOS: "+err.Error(),
		)
	}

	if consume != nil {
		consume(channel)
	}
	return nil
}

// onConnClosed - reconnect when connection is closed
func onConnClosed(task openConnectionNChannelTask) {
	// Lock all interactions with the connection/channel unless it will be reopened
	task.connectionPair.rwMutex.Lock()
	defer task.connectionPair.rwMutex.Unlock()
	task.skipChannelOpening = false

	if task.errorData != nil {
		task.logger.Error("RMQ connection/channel closed: " + task.errorData.Error())
	}
	for {
		var err APIError

		err = openConnectionNChannel(task)
		if err == nil {
			task.logger.Log("RMQ connection/channel recovered")
			break
		} else {
			task.logger.Error("Exception while trying to recover RMQ connection/channel: " + err.Message)
			time.Sleep(5 * time.Second)
		}
	}
}
