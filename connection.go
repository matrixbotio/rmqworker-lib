package rmqworker

import (
	"crypto/tls"
	"net/url"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

func rmqConnect(connData rmqConnectionData) (*amqp.Connection, apiError) {
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
