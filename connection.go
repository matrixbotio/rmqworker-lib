package rmqworker

import (
	"crypto/tls"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

type consumeFunc func(channel *amqp.Channel)

// openConnectionNChannel - open new RMQ connection & channel
func openConnectionNChannel(task openConnectionNChannelTask) APIError {
	var err APIError

	if task.connectionPair == nil {
		task.connectionPair = &connectionPair{
			mutex:   sync.Mutex{},
			rwMutex: sync.RWMutex{},
		}
	}
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
		err := startConsumer(consumeTask{
			consume:        task.consume,
			connData:       task.connData,
			connectionPair: task.connectionPair,
			reconsumeAll:   task.reconsumeAll,
		})
		if err != nil {
			return err
		}
	} else {
		// get new channel
		err = openRMQChannel(task.connectionPair, task.connData, task.consume, task.reconsumeAll)
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
func openRMQChannel(connectionPair *connectionPair, connData RMQConnectionData, consume consumeFunc, reconsumeAll bool,
) APIError {
	channel, rmqErr := connectionPair.Conn.Channel()
	if rmqErr != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to get amqp channel: "+rmqErr.Error(),
		)
	}

	connectionPair.Channel = channel

	err := startConsumer(consumeTask{
		consume:        consume,
		connData:       connData,
		connectionPair: connectionPair,
		reconsumeAll:   reconsumeAll,
	})
	if err != nil {
		return err
	}

	return nil
}

func startConsumer(task consumeTask) APIError {
	task.connectionPair.mutex.Lock()
	defer task.connectionPair.mutex.Unlock()

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
			err = openRMQChannel(task.connectionPair, task.connData, task.consume, false)
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

	if task.reconsumeAll {
		for _, consume := range task.connectionPair.consumes {
			consume(task.connectionPair.Channel)
		}
	} else if task.consume != nil {
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
	task.reconsumeAll = true

	if task.errorData != nil {
		task.logger.Error(constants.Error(
			"SERVICE_CONN_ERR",
			"RMQ connection/channel closed: "+task.errorData.Error()+
				", conn active: "+strconv.FormatBool(task.connectionPair.Conn.IsClosed()),
		))
	}
	for {
		var err APIError

		err = openConnectionNChannel(task)
		if err == nil {
			task.logger.Log("RMQ connection/channel recovered")
			break
		} else {
			task.logger.Error(constants.Error(
				"SERVICE_REQ_FAILED",
				"Exception while trying to recover RMQ connection/channel: "+err.Message,
			))
			time.Sleep(5 * time.Second)
		}
	}
}
