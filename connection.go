package rmqworker

import (
	"crypto/tls"
	"strconv"
	"strings"
	"time"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

type consumeFunc func(channel *amqp.Channel)

// openConnectionNChannel - open new RMQ connection & channel
func openConnectionNChannel(task openConnectionNChannelTask, forceHandleNotifyCLose bool) APIError {
	var err APIError

	handleConnectionNotifyClose := forceHandleNotifyCLose
	// get connection
	if task.connectionPair.Conn == nil || task.connectionPair.Conn.IsClosed() {
		task.connectionPair.Conn, err = rmqConnect(task.connData)
		if err != nil {
			return err
		}
		handleConnectionNotifyClose = true
	}

	if handleConnectionNotifyClose {
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
		err := setupConsume(consumeTask{
			consume:        task.consume,
			connData:       task.connData,
			connectionPair: task.connectionPair,
		})
		if err != nil {
			return err
		}
	} else {
		// get new channel
		task.connectionPair.Channel, err = openRMQChannel(task.connectionPair.Conn, task.connData, task.consume)
		if err != nil {
			return err
		}
		// setup channel reconnection
		channelCloseReceiver := make(chan *amqp.Error)
		task.connectionPair.Channel.NotifyClose(channelCloseReceiver)
		go func() {
			for task.errorData = range channelCloseReceiver {
				onConnClosed(task)
			}
		}()
	}
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
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		conn, err = amqp.DialTLS(dsn, tlsConfig)
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
func openRMQChannel(conn *amqp.Connection, connData RMQConnectionData, consume consumeFunc) (*amqp.Channel, APIError) {
	channel, rmqErr := conn.Channel()
	if rmqErr != nil {
		return nil, constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to get amqp channel: "+rmqErr.Error(),
		)
	}

	return channel, setupConsume(consumeTask{
		consume:  consume,
		connData: connData,
		connectionPair: &connectionPair{
			Conn:    conn,
			Channel: channel,
		},
	})
}

func setupConsume(task consumeTask) APIError {
	task.connectionPair.rwMutex.Lock()
	defer task.connectionPair.rwMutex.Unlock()

	err := task.connectionPair.Channel.Qos(1, 0, false)
	if err != nil {
		if strings.Contains(err.Error(), "channel/connection is not open") {
			// reopen connection
			conn, err := rmqConnect(task.connData)
			if err != nil {
				return err
			}
			task.connectionPair.Conn = conn
			// open channel
			task.connectionPair.Channel, err = openRMQChannel(task.connectionPair.Conn, task.connData, task.consume)
			if err != nil {
				return err
			}
		} else {
			return constants.Error(
				"SERVICE_REQ_FAILED",
				"failed to set up QOS: "+err.Error()+
					", conn active: "+strconv.FormatBool(task.connectionPair.Conn.IsClosed()),
			)
		}
	}

	if task.consume != nil {
		task.consume(task.connectionPair.Channel)
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
		task.logger.Error("RMQ connection/channel closed: " + task.errorData.Error() +
			", conn active: " + strconv.FormatBool(task.connectionPair.Conn.IsClosed()))
	}
	for {
		var err APIError

		err = openConnectionNChannel(task, false)
		if err == nil {
			task.logger.Log("RMQ connection/channel recovered")
			break
		} else {
			task.logger.Error("Exception while trying to recover RMQ connection/channel: " + err.Message)
			time.Sleep(5 * time.Second)
		}
	}
}
