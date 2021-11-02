package rmqworker

import (
	"crypto/tls"
	"strconv"
	"time"

	"github.com/matrixbotio/constants-lib"
	simplecron "github.com/sagleft/simple-cron"
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
	err := channel.Qos(0, 0, false)
	if err != nil {
		return nil, constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to set up QOS: "+err.Error(),
		)
	}
	return channel, nil
}

// checkRMQConnection - check RMQ connection is active. open new connection if inactive
func checkRMQConnection(RMQConn *amqp.Connection, connData RMQConnectionData, channel *amqp.Channel, logger *constants.Logger) APIError {
	if !RMQConn.IsClosed() {
		return nil
	}
	conn, err := rmqConnect(connData)
	if err != nil {
		return err
	}
	var receiver chan *amqp.Error
	conn.NotifyClose(receiver)
	go handleNotifyClose(receiver, RMQConn, connData, channel, logger)

	RMQConn = conn
	channel, err = openRMQChannel(conn)
	if err != nil {
		return err
	}

	if channel == nil {
		return constants.Error(
			"BASE_INTERNAL_ERROR",
			"failed to open new rmq channel, new channel is nil",
		)
	}
	return nil
}

func handleNotifyClose(receiver chan *amqp.Error, conn *amqp.Connection, connData RMQConnectionData, channel *amqp.Channel, logger *constants.Logger) {
	for closeError := range receiver {
		logger.Error("RMQ connection/channel close: " + closeError.Error())
		if conn.IsClosed() {
			err := checkRMQConnection(conn, connData, channel, logger)
			if err != nil {
				logger.Error("Error checking RMQ connection on close notification: " + err.Message)
			}
		} else {
			newChannel, err := openRMQChannel(conn)
			if err != nil {
				logger.Error("Error opening new channel on close notification" + err.Message)
			}
			channel = newChannel
		}
	}
}

type connFunc func() APIError

func (r *RMQWorker) reconnect(callback connFunc, connectionDescription string) {
	isConnected := false
	for !isConnected {
		for i := 0; i < reconnectionAttemptsNumber; i++ {

			isTimeIsUP := simplecron.NewRuntimeLimitHandler(
				reconnectionTimeout*time.Second,
				func() {
					err := callback()
					if err == nil {
						// connection established
						isConnected = true
						return
					}
					if err.Name == "DATA_EXISTS" {
						// connection is open
						isConnected = true
						return
					}
					r.logger.Warn(convertRMQError(err))
				},
			).Run()
			if isTimeIsUP {
				r.logger.Warn(constants.Error(
					"SERVICE_CONN_ERR",
					r.getLogWorkerName()+" connection timeout",
				))
			}
			if isConnected {
				return
			}
			time.Sleep(reconnectAfterSeconds * time.Second)

		}

		errMsg := "failed to connect to " +
			connectionDescription + " after " +
			strconv.Itoa(reconnectionAttemptsNumber) + " attempts"

		r.logger.Error(constants.Error("SERVICE_CONN_ERR", errMsg))

		sleepDuration := time.Second * waitingBetweenAttempts
		r.logger.Verbose("wait " + sleepDuration.String() + " between attempts...")
		time.Sleep(sleepDuration)
	}
}
