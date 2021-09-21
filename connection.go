package rmqworker

import (
	"crypto/tls"
	"net/url"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

func rmqConnect(connData rmqConnectionData) (*amqp.Connection, APIError) {
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
	dsn += "://" + connData.User + ":" + url.QueryEscape(connData.Password) +
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

// OpenConnectionNChannel - open new RMQ connection & channel
func OpenConnectionNChannel(connData rmqConnectionData) (*amqp.Connection, *amqp.Channel, APIError) {
	// get connection
	conn, err := rmqConnect(connData)
	if err != nil {
		return nil, nil, err
	}

	// get channel
	channel, err := OpenRMQChannel(conn)
	if err != nil {
		return nil, nil, err
	}

	return conn, channel, nil
}

// OpenRMQChannel - open new RMQ channel
func OpenRMQChannel(conn *amqp.Connection) (*amqp.Channel, APIError) {
	channel, rmqErr := conn.Channel()
	if rmqErr != nil {
		return nil, constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to get amqp channel: "+rmqErr.Error(),
		)
	}
	return channel, nil
}
